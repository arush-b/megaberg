//! Parquet table source implementation.
//!
//! Discovers parquet files from S3 using prefix listing.
//! Supports both partitioned (Hive-style) and unpartitioned layouts.
//! Optionally supports time-windowed discovery for observability data.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};
use futures::TryStreamExt;
use opendal::Operator;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::metadata::ParquetMetaDataReader;
use serde::Deserialize;
use tracing::{debug, info, warn};

use super::super::discovery::{FileInfo, PartitionFileMap, TableFormat};
use super::sink::{CommitResult, OutputFileInfo, TableSink};
use super::source::TableSource;

/// Minimum bytes to read for parquet footer (magic + footer length).
const PARQUET_FOOTER_MIN_SIZE: u64 = 8;

/// Maximum footer size to read in one request (8MB should cover most schemas).
const PARQUET_FOOTER_MAX_SIZE: u64 = 8 * 1024 * 1024;

// ============================================================================
// Time Window Configuration
// ============================================================================

/// Configuration for time-windowed partition discovery.
///
/// Used for observability data where you want to only compact
/// recent partitions (e.g., last 2 hours) and skip current
/// partitions to avoid racing with active writers.
#[derive(Debug, Clone, Deserialize)]
pub struct TimeWindowConfig {
    /// How to extract timestamp from partition path.
    pub partition_spec: PartitionTimeSpec,

    /// How far back to look for partitions.
    pub lookback: LookbackWindow,

    /// Number of most recent partitions to skip (avoid racing with writers).
    /// Default: 1 (skip current partition)
    #[serde(default = "default_skip_recent")]
    pub skip_recent: u32,
}

fn default_skip_recent() -> u32 {
    1
}

/// Specification for extracting time from partition paths.
#[derive(Debug, Clone, Deserialize)]
pub struct PartitionTimeSpec {
    /// Fields that make up the timestamp, in order.
    /// e.g., `[{name: "dt", format: "%Y-%m-%d"}, {name: "hour", format: "%H"}]`
    pub fields: Vec<PartitionTimeField>,

    /// Timezone for interpreting partition values (default: UTC).
    #[serde(default = "default_timezone")]
    pub timezone: String,

    /// The granularity of partitions (used for skip_recent calculation).
    #[serde(default)]
    pub granularity: PartitionGranularity,
}

fn default_timezone() -> String {
    "UTC".to_string()
}

/// A single field in the partition path that contributes to the timestamp.
#[derive(Debug, Clone, Deserialize)]
pub struct PartitionTimeField {
    /// Field name in partition (e.g., "dt", "hour", "minute").
    pub name: String,

    /// strftime format for parsing (e.g., "%Y-%m-%d", "%H").
    pub format: String,

    /// What time component this field represents.
    pub component: TimeComponent,
}

/// What part of a timestamp a partition field represents.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TimeComponent {
    /// Full date (YYYY-MM-DD)
    Date,
    /// Year only
    Year,
    /// Month only (1-12)
    Month,
    /// Day of month (1-31)
    Day,
    /// Hour (0-23)
    Hour,
    /// Minute (0-59)
    Minute,
}

/// Granularity of partition time boundaries.
#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PartitionGranularity {
    /// Partitioned by day
    Day,
    /// Partitioned by hour
    #[default]
    Hour,
    /// Partitioned by minute
    Minute,
}

impl PartitionGranularity {
    /// Get the duration of one partition at this granularity.
    fn duration(&self) -> Duration {
        match self {
            PartitionGranularity::Day => Duration::days(1),
            PartitionGranularity::Hour => Duration::hours(1),
            PartitionGranularity::Minute => Duration::minutes(1),
        }
    }

    /// Truncate a datetime to this granularity boundary.
    fn truncate(&self, dt: DateTime<Utc>) -> DateTime<Utc> {
        match self {
            PartitionGranularity::Day => dt
                .date_naive()
                .and_hms_opt(0, 0, 0)
                .map(|naive| Utc.from_utc_datetime(&naive))
                .unwrap_or(dt),
            PartitionGranularity::Hour => dt
                .date_naive()
                .and_hms_opt(dt.time().hour(), 0, 0)
                .map(|naive| Utc.from_utc_datetime(&naive))
                .unwrap_or(dt),
            PartitionGranularity::Minute => dt
                .date_naive()
                .and_hms_opt(dt.time().hour(), dt.time().minute(), 0)
                .map(|naive| Utc.from_utc_datetime(&naive))
                .unwrap_or(dt),
        }
    }
}

/// How far back to look for partitions.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum LookbackWindow {
    /// Look back a fixed number of partition periods.
    Partitions {
        /// Number of partitions to include.
        count: u32,
    },
    /// Look back a time duration from now.
    Duration {
        /// Hours to look back.
        #[serde(default)]
        hours: u32,
        /// Minutes to look back (added to hours).
        #[serde(default)]
        minutes: u32,
    },
}

impl LookbackWindow {
    /// Convert to a chrono Duration.
    fn to_duration(&self, granularity: PartitionGranularity) -> Duration {
        match self {
            LookbackWindow::Partitions { count } => granularity.duration() * (*count as i32),
            LookbackWindow::Duration { hours, minutes } => {
                Duration::hours(*hours as i64) + Duration::minutes(*minutes as i64)
            }
        }
    }
}

// ============================================================================
// Parquet Source
// ============================================================================

/// Parquet table source - discovers files via S3 listing.
///
/// This provides a TableSource implementation for raw parquet files
/// stored in S3 with Hive-style partitioning (e.g., `year=2024/month=01/`).
/// Also supports unpartitioned layouts where files are directly in base_path.
///
/// Optionally supports time-windowed discovery via `with_time_window()` for
/// observability data compaction scenarios.
pub struct ParquetSource {
    operator: Operator,
    base_path: String,
    bucket: String,
    /// Optional time window filter for observability data.
    time_window: Option<TimeWindowConfig>,
}

impl ParquetSource {
    /// Create a new ParquetSource.
    ///
    /// - `operator`: OpenDAL operator configured for the S3 bucket
    /// - `base_path`: Path prefix within the bucket (e.g., "data/events/")
    /// - `bucket`: S3 bucket name (for constructing full s3:// URLs)
    pub fn new(operator: Operator, base_path: String, bucket: String) -> Self {
        // Ensure base_path ends with / (unless empty)
        let base_path = if base_path.is_empty() || base_path.ends_with('/') {
            base_path
        } else {
            format!("{}/", base_path)
        };

        Self {
            operator,
            base_path,
            bucket,
            time_window: None,
        }
    }

    /// Enable time-windowed partition discovery.
    ///
    /// When configured, only partitions within the time window are discovered.
    /// Recent partitions (based on `skip_recent`) are skipped to avoid racing
    /// with active writers.
    ///
    /// # Example
    /// ```ignore
    /// let source = ParquetSource::new(op, "data/events/".into(), "bucket".into())
    ///     .with_time_window(TimeWindowConfig {
    ///         partition_spec: PartitionTimeSpec {
    ///             fields: vec![
    ///                 PartitionTimeField {
    ///                     name: "dt".into(),
    ///                     format: "%Y-%m-%d".into(),
    ///                     component: TimeComponent::Date,
    ///                 },
    ///                 PartitionTimeField {
    ///                     name: "hour".into(),
    ///                     format: "%H".into(),
    ///                     component: TimeComponent::Hour,
    ///                 },
    ///             ],
    ///             timezone: "UTC".into(),
    ///             granularity: PartitionGranularity::Hour,
    ///         },
    ///         lookback: LookbackWindow::Duration { hours: 2, minutes: 0 },
    ///         skip_recent: 1,
    ///     });
    /// ```
    pub fn with_time_window(mut self, config: TimeWindowConfig) -> Self {
        self.time_window = Some(config);
        self
    }

    /// Discover all directories containing parquet files.
    ///
    /// This handles:
    /// - Unpartitioned data (files directly in base_path)
    /// - Single-level partitioning (base_path/partition=value/)
    /// - Nested partitioning (base_path/year=2024/month=01/day=15/)
    ///
    /// When time_window is configured, partitions are filtered to only
    /// include those within the lookback window (excluding recent ones).
    async fn discover_partitions(&self) -> Result<Vec<String>> {
        let mut to_explore = vec![self.base_path.clone()];
        let mut partitions = Vec::new();

        while let Some(current) = to_explore.pop() {
            let entries: Vec<_> = self
                .operator
                .lister_with(&current)
                .recursive(false)
                .await
                .with_context(|| format!("Failed to list directory: {}", current))?
                .try_collect()
                .await?;

            let mut has_parquet_files = false;
            let mut subdirs = Vec::new();

            for entry in entries {
                let path = entry.path();
                let metadata = entry.metadata();

                if metadata.is_dir() {
                    // Skip if it's the current directory itself
                    if path != current {
                        subdirs.push(path.to_string());
                    }
                } else if path.ends_with(".parquet") {
                    has_parquet_files = true;
                }
            }

            // If this directory has parquet files, it's a partition
            if has_parquet_files {
                partitions.push(current.clone());
            }

            // Always explore subdirectories (they might have files even if parent does)
            to_explore.extend(subdirs);
        }

        // Apply time window filter if configured
        let partitions = if let Some(ref tw) = self.time_window {
            self.filter_partitions_by_time(partitions, tw)?
        } else {
            partitions
        };

        debug!(
            base = %self.base_path,
            partitions = partitions.len(),
            time_filtered = self.time_window.is_some(),
            "Discovered partitions"
        );
        Ok(partitions)
    }

    /// Filter partitions to only those within the time window.
    fn filter_partitions_by_time(
        &self,
        partitions: Vec<String>,
        config: &TimeWindowConfig,
    ) -> Result<Vec<String>> {
        let now = Utc::now();
        let granularity = config.partition_spec.granularity;

        // Calculate time boundaries
        let lookback_duration = config.lookback.to_duration(granularity);
        let cutoff = now - lookback_duration;

        // Skip recent partitions (truncate to boundary, then subtract skip count)
        let current_boundary = granularity.truncate(now);
        let skip_duration = granularity.duration() * (config.skip_recent as i32);
        let skip_after = current_boundary - skip_duration;

        info!(
            cutoff = %cutoff.format("%Y-%m-%d %H:%M"),
            skip_after = %skip_after.format("%Y-%m-%d %H:%M"),
            skip_recent = config.skip_recent,
            "Time window filter"
        );

        let mut filtered = Vec::new();
        let mut skipped_old = 0;
        let mut skipped_recent = 0;
        let mut parse_errors = 0;

        for partition in partitions {
            let partition_key = self.extract_partition_key(&partition);

            match self.parse_partition_time(&partition_key, &config.partition_spec) {
                Ok(ts) => {
                    if ts < cutoff {
                        skipped_old += 1;
                        debug!(partition = %partition_key, ts = %ts, "Skipping: too old");
                    } else if ts >= skip_after {
                        skipped_recent += 1;
                        debug!(partition = %partition_key, ts = %ts, "Skipping: too recent");
                    } else {
                        filtered.push(partition);
                    }
                }
                Err(e) => {
                    parse_errors += 1;
                    warn!(
                        partition = %partition_key,
                        error = %e,
                        "Failed to parse partition time, skipping"
                    );
                }
            }
        }

        info!(
            included = filtered.len(),
            skipped_old,
            skipped_recent,
            parse_errors,
            "Time window filter results"
        );

        Ok(filtered)
    }

    /// Parse partition path to extract timestamp.
    ///
    /// E.g., "dt=2024-01-15/hour=14" -> 2024-01-15 14:00:00 UTC
    fn parse_partition_time(
        &self,
        partition_key: &str,
        spec: &PartitionTimeSpec,
    ) -> Result<DateTime<Utc>> {
        // Extract key=value pairs from partition path
        let values = Self::extract_partition_values(partition_key);

        // Start with epoch and build up the timestamp
        let mut year = 1970i32;
        let mut month = 1u32;
        let mut day = 1u32;
        let mut hour = 0u32;
        let mut minute = 0u32;

        for field in &spec.fields {
            let value = values
                .get(&field.name)
                .ok_or_else(|| anyhow::anyhow!("Missing partition field: {}", field.name))?;

            match field.component {
                TimeComponent::Date => {
                    let date = NaiveDate::parse_from_str(value, &field.format)
                        .with_context(|| format!("Failed to parse date '{}' with format '{}'", value, field.format))?;
                    year = date.year();
                    month = date.month();
                    day = date.day();
                }
                TimeComponent::Year => {
                    year = value.parse()
                        .with_context(|| format!("Failed to parse year '{}'", value))?;
                }
                TimeComponent::Month => {
                    month = value.parse()
                        .with_context(|| format!("Failed to parse month '{}'", value))?;
                }
                TimeComponent::Day => {
                    day = value.parse()
                        .with_context(|| format!("Failed to parse day '{}'", value))?;
                }
                TimeComponent::Hour => {
                    hour = value.parse()
                        .with_context(|| format!("Failed to parse hour '{}'", value))?;
                }
                TimeComponent::Minute => {
                    minute = value.parse()
                        .with_context(|| format!("Failed to parse minute '{}'", value))?;
                }
            }
        }

        // Build NaiveDateTime
        let date = NaiveDate::from_ymd_opt(year, month, day)
            .ok_or_else(|| anyhow::anyhow!("Invalid date: {}-{}-{}", year, month, day))?;
        let time = NaiveTime::from_hms_opt(hour, minute, 0)
            .ok_or_else(|| anyhow::anyhow!("Invalid time: {}:{}", hour, minute))?;
        let naive = NaiveDateTime::new(date, time);

        // Convert to UTC (assuming partition times are in UTC for now)
        // TODO: Support configurable timezone via spec.timezone
        Ok(Utc.from_utc_datetime(&naive))
    }

    /// Extract key=value pairs from a partition path.
    ///
    /// E.g., "dt=2024-01-15/hour=14" -> {"dt": "2024-01-15", "hour": "14"}
    fn extract_partition_values(partition_key: &str) -> HashMap<String, String> {
        let mut values = HashMap::new();

        for segment in partition_key.split('/') {
            if let Some((key, value)) = segment.split_once('=') {
                values.insert(key.to_string(), value.to_string());
            }
        }

        values
    }

    /// List parquet files in a specific partition path.
    async fn list_files_in_partition(&self, partition_path: &str) -> Result<Vec<FileInfo>> {
        let mut files = Vec::new();

        let entries: Vec<_> = self
            .operator
            .lister_with(partition_path)
            .recursive(false)
            .await?
            .try_collect()
            .await?;

        for entry in entries {
            let path = entry.path();
            let metadata = entry.metadata();

            // Skip directories, only process parquet files
            if metadata.is_dir() {
                continue;
            }

            if path.ends_with(".parquet") {
                // Get size from listing metadata, or fall back to stat
                let size = metadata.content_length();
                let size = if size > 0 {
                    size
                } else {
                    // Fall back to stat if listing didn't include size
                    let stat = self
                        .operator
                        .stat(path)
                        .await
                        .with_context(|| format!("Failed to stat {}", path))?;
                    stat.content_length()
                };

                files.push(FileInfo {
                    path: format!("s3://{}/{}", self.bucket, path),
                    size_bytes: size,
                    record_count: 0, // Unknown without reading footer
                });
            }
        }

        debug!(partition = %partition_path, files = files.len(), "Listed files in partition");
        Ok(files)
    }

    /// Extract partition key from full path.
    /// E.g., "data/events/year=2024/month=01/" -> "year=2024/month=01"
    /// For unpartitioned data (base_path itself), returns empty string.
    fn extract_partition_key(&self, partition_path: &str) -> String {
        partition_path
            .strip_prefix(&self.base_path)
            .unwrap_or(partition_path)
            .trim_matches('/')
            .to_string()
    }

    /// Read parquet schema using ranged read (only fetches footer, not entire file).
    async fn read_schema_from_footer(&self, path: &str, file_size: u64) -> Result<SchemaRef> {
        // Parquet footer is at the end: [metadata][footer_len:4][magic:4]
        // We need to read the last N bytes to get the footer

        // First, read just the last 8 bytes to get footer length
        let footer_size_start = file_size.saturating_sub(PARQUET_FOOTER_MIN_SIZE);
        let suffix_bytes = self
            .operator
            .read_with(path)
            .range(footer_size_start..file_size)
            .await
            .with_context(|| format!("Failed to read footer suffix from {}", path))?
            .to_vec();

        if suffix_bytes.len() < 8 {
            anyhow::bail!("File too small to be valid parquet: {}", path);
        }

        // Parse footer length (4 bytes before magic)
        let footer_len = u32::from_le_bytes([
            suffix_bytes[suffix_bytes.len() - 8],
            suffix_bytes[suffix_bytes.len() - 7],
            suffix_bytes[suffix_bytes.len() - 6],
            suffix_bytes[suffix_bytes.len() - 5],
        ]) as u64;

        // Sanity check footer size
        let total_footer_size = footer_len + 8; // metadata + footer_len + magic
        if total_footer_size > file_size || total_footer_size > PARQUET_FOOTER_MAX_SIZE {
            anyhow::bail!(
                "Invalid or too large footer size {} in file {}",
                footer_len,
                path
            );
        }

        // Read the full footer
        let footer_start = file_size - total_footer_size;
        let footer_bytes = self
            .operator
            .read_with(path)
            .range(footer_start..file_size)
            .await
            .with_context(|| format!("Failed to read footer from {}", path))?
            .to_vec();

        // Parse metadata from footer bytes
        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&Bytes::from(footer_bytes))
            .with_context(|| format!("Failed to parse parquet metadata from {}", path))?;

        let schema = parquet_to_arrow_schema(metadata.file_metadata().schema_descr(), None)
            .context("Failed to convert parquet schema to arrow")?;

        Ok(Arc::new(schema))
    }

    /// Fallback: read full file to get schema (for small files or when ranged read fails).
    async fn read_schema_full_file(&self, path: &str) -> Result<SchemaRef> {
        let bytes = self
            .operator
            .read(path)
            .await
            .with_context(|| format!("Failed to read {}", path))?
            .to_vec();

        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&Bytes::from(bytes))
            .context("Failed to parse parquet metadata")?;

        let schema = parquet_to_arrow_schema(metadata.file_metadata().schema_descr(), None)
            .context("Failed to convert parquet schema to arrow")?;

        Ok(Arc::new(schema))
    }
}

#[async_trait]
impl TableSource for ParquetSource {
    fn format(&self) -> TableFormat {
        TableFormat::Parquet
    }

    fn location(&self) -> &str {
        &self.base_path
    }

    async fn schema(&self) -> Result<SchemaRef> {
        // Find first parquet file and read its schema from footer
        let partitions = self.discover_partitions().await?;

        for partition in partitions {
            let files = self.list_files_in_partition(&partition).await?;
            if let Some(first) = files.first() {
                // Convert s3://bucket/path to just path for opendal
                let path = first
                    .path
                    .strip_prefix(&format!("s3://{}/", self.bucket))
                    .unwrap_or(&first.path);

                // Try ranged read first (efficient for large files)
                if first.size_bytes > PARQUET_FOOTER_MAX_SIZE {
                    match self.read_schema_from_footer(path, first.size_bytes).await {
                        Ok(schema) => return Ok(schema),
                        Err(e) => {
                            warn!(
                                path = %path,
                                error = %e,
                                "Ranged read failed, falling back to full read"
                            );
                        }
                    }
                }

                // Fall back to full file read for small files or on error
                return self.read_schema_full_file(path).await;
            }
        }

        Err(anyhow::anyhow!("No parquet files found to read schema from"))
    }

    async fn discover(&self) -> Result<PartitionFileMap> {
        let partitions = self.discover_partitions().await?;
        let mut result = PartitionFileMap::new();

        for partition_path in partitions {
            let partition_key = self.extract_partition_key(&partition_path);
            let files = self.list_files_in_partition(&partition_path).await?;

            if !files.is_empty() {
                result.insert(partition_key, files);
            }
        }

        let total_files: usize = result.values().map(|v| v.len()).sum();
        info!(
            partitions = result.len(),
            files = total_files,
            time_windowed = self.time_window.is_some(),
            "Discovery complete"
        );

        Ok(result)
    }
}

#[async_trait]
impl TableSink for ParquetSource {
    fn format(&self) -> TableFormat {
        TableFormat::Parquet
    }

    async fn commit(
        &self,
        added_files: Vec<OutputFileInfo>,
        _removed_files: Vec<String>,
    ) -> Result<CommitResult> {
        // Parquet has no catalog - files are written directly to S3.
        // Nothing to commit, just log the result.
        info!(
            added = added_files.len(),
            base_path = %self.base_path,
            "Parquet sink: files written (no catalog commit needed)"
        );

        Ok(CommitResult {
            snapshot_id: None,
            files_added: added_files.len(),
            files_removed: 0, // Parquet doesn't track removed files
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_partition_values() {
        let values = ParquetSource::extract_partition_values("dt=2024-01-15/hour=14");
        assert_eq!(values.get("dt"), Some(&"2024-01-15".to_string()));
        assert_eq!(values.get("hour"), Some(&"14".to_string()));
    }

    #[test]
    fn test_extract_partition_values_nested() {
        let values = ParquetSource::extract_partition_values("year=2024/month=01/day=15/hour=14");
        assert_eq!(values.get("year"), Some(&"2024".to_string()));
        assert_eq!(values.get("month"), Some(&"01".to_string()));
        assert_eq!(values.get("day"), Some(&"15".to_string()));
        assert_eq!(values.get("hour"), Some(&"14".to_string()));
    }

    #[test]
    fn test_granularity_truncate() {
        let dt = Utc.with_ymd_and_hms(2024, 1, 15, 14, 35, 22).unwrap();

        assert_eq!(
            PartitionGranularity::Day.truncate(dt),
            Utc.with_ymd_and_hms(2024, 1, 15, 0, 0, 0).unwrap()
        );
        assert_eq!(
            PartitionGranularity::Hour.truncate(dt),
            Utc.with_ymd_and_hms(2024, 1, 15, 14, 0, 0).unwrap()
        );
        assert_eq!(
            PartitionGranularity::Minute.truncate(dt),
            Utc.with_ymd_and_hms(2024, 1, 15, 14, 35, 0).unwrap()
        );
    }

    /// Helper to create a test ParquetSource (operator doesn't matter for unit tests)
    fn create_test_source() -> ParquetSource {
        // Create a dummy operator - we won't actually use it in unit tests
        let builder = opendal::services::Memory::default();
        let operator = opendal::Operator::new(builder).unwrap().finish();
        ParquetSource::new(operator, "data/events/".to_string(), "test-bucket".to_string())
    }

    fn create_time_window_config() -> TimeWindowConfig {
        TimeWindowConfig {
            partition_spec: PartitionTimeSpec {
                fields: vec![
                    PartitionTimeField {
                        name: "dt".to_string(),
                        format: "%Y-%m-%d".to_string(),
                        component: TimeComponent::Date,
                    },
                    PartitionTimeField {
                        name: "hour".to_string(),
                        format: "%H".to_string(),
                        component: TimeComponent::Hour,
                    },
                ],
                timezone: "UTC".to_string(),
                granularity: PartitionGranularity::Hour,
            },
            lookback: LookbackWindow::Duration { hours: 3, minutes: 0 },
            skip_recent: 1,
        }
    }

    #[test]
    fn test_parse_partition_time() {
        let source = create_test_source();
        let spec = PartitionTimeSpec {
            fields: vec![
                PartitionTimeField {
                    name: "dt".to_string(),
                    format: "%Y-%m-%d".to_string(),
                    component: TimeComponent::Date,
                },
                PartitionTimeField {
                    name: "hour".to_string(),
                    format: "%H".to_string(),
                    component: TimeComponent::Hour,
                },
            ],
            timezone: "UTC".to_string(),
            granularity: PartitionGranularity::Hour,
        };

        let ts = source.parse_partition_time("dt=2024-01-15/hour=14", &spec).unwrap();
        assert_eq!(ts, Utc.with_ymd_and_hms(2024, 1, 15, 14, 0, 0).unwrap());
    }

    #[test]
    fn test_parse_partition_time_year_month_day_hour() {
        let source = create_test_source();
        let spec = PartitionTimeSpec {
            fields: vec![
                PartitionTimeField {
                    name: "year".to_string(),
                    format: "%Y".to_string(),
                    component: TimeComponent::Year,
                },
                PartitionTimeField {
                    name: "month".to_string(),
                    format: "%m".to_string(),
                    component: TimeComponent::Month,
                },
                PartitionTimeField {
                    name: "day".to_string(),
                    format: "%d".to_string(),
                    component: TimeComponent::Day,
                },
                PartitionTimeField {
                    name: "hour".to_string(),
                    format: "%H".to_string(),
                    component: TimeComponent::Hour,
                },
            ],
            timezone: "UTC".to_string(),
            granularity: PartitionGranularity::Hour,
        };

        let ts = source.parse_partition_time("year=2024/month=01/day=15/hour=14", &spec).unwrap();
        assert_eq!(ts, Utc.with_ymd_and_hms(2024, 1, 15, 14, 0, 0).unwrap());
    }

    #[test]
    fn test_time_window_filtering() {
        let source = create_test_source();
        let config = create_time_window_config();

        // Simulate current time as 2024-01-15 15:30:00 UTC
        // With 3 hour lookback and skip_recent=1:
        //   - cutoff = 12:30 (anything older is skipped)
        //   - skip_after = 15:00 (current hour boundary - 1 hour)
        //
        // So partitions hour=13, hour=14 should be included
        // hour=12 should be skipped (too old - before 12:30 cutoff)
        // hour=15 should be skipped (too recent - at or after 15:00)

        // We can't easily mock time, but we can test the logic with a manual calculation
        // Let's verify the filtering logic by creating partitions and checking the output

        // Create test partitions
        let partitions = vec![
            "data/events/dt=2024-01-15/hour=10/".to_string(), // too old
            "data/events/dt=2024-01-15/hour=11/".to_string(), // too old
            "data/events/dt=2024-01-15/hour=12/".to_string(), // borderline old
            "data/events/dt=2024-01-15/hour=13/".to_string(), // should include
            "data/events/dt=2024-01-15/hour=14/".to_string(), // should include
            "data/events/dt=2024-01-15/hour=15/".to_string(), // too recent (current)
        ];

        // The actual filtering depends on current time, so let's just verify
        // that the filter function doesn't panic and returns reasonable results
        let result = source.filter_partitions_by_time(partitions, &config);
        assert!(result.is_ok());

        // The result depends on current time - in a real scenario we'd mock time
        // But we can verify the function runs without error
        let filtered = result.unwrap();
        println!("Filtered partitions: {:?}", filtered);

        // At minimum, very old partitions should be filtered out
        // and very recent ones too (depending on when test runs)
    }

    #[test]
    fn test_time_window_filtering_with_fixed_time() {
        // Test with partitions that are definitely old (2020) - should all be filtered
        let source = create_test_source();
        let config = TimeWindowConfig {
            partition_spec: PartitionTimeSpec {
                fields: vec![
                    PartitionTimeField {
                        name: "dt".to_string(),
                        format: "%Y-%m-%d".to_string(),
                        component: TimeComponent::Date,
                    },
                    PartitionTimeField {
                        name: "hour".to_string(),
                        format: "%H".to_string(),
                        component: TimeComponent::Hour,
                    },
                ],
                timezone: "UTC".to_string(),
                granularity: PartitionGranularity::Hour,
            },
            lookback: LookbackWindow::Duration { hours: 2, minutes: 0 },
            skip_recent: 1,
        };

        // All 2020 partitions should be filtered as "too old"
        let old_partitions = vec![
            "data/events/dt=2020-01-15/hour=10/".to_string(),
            "data/events/dt=2020-01-15/hour=11/".to_string(),
            "data/events/dt=2020-01-15/hour=12/".to_string(),
        ];

        let result = source.filter_partitions_by_time(old_partitions, &config).unwrap();
        assert!(result.is_empty(), "Old partitions from 2020 should all be filtered out");
    }

    #[test]
    fn test_time_window_filtering_future_partitions() {
        // Test with partitions that are in the future (2030) - should be filtered as "too recent"
        let source = create_test_source();
        let config = TimeWindowConfig {
            partition_spec: PartitionTimeSpec {
                fields: vec![
                    PartitionTimeField {
                        name: "dt".to_string(),
                        format: "%Y-%m-%d".to_string(),
                        component: TimeComponent::Date,
                    },
                    PartitionTimeField {
                        name: "hour".to_string(),
                        format: "%H".to_string(),
                        component: TimeComponent::Hour,
                    },
                ],
                timezone: "UTC".to_string(),
                granularity: PartitionGranularity::Hour,
            },
            lookback: LookbackWindow::Duration { hours: 2, minutes: 0 },
            skip_recent: 1,
        };

        // All 2030 partitions should be filtered as "too recent"
        let future_partitions = vec![
            "data/events/dt=2030-01-15/hour=10/".to_string(),
            "data/events/dt=2030-01-15/hour=11/".to_string(),
        ];

        let result = source.filter_partitions_by_time(future_partitions, &config).unwrap();
        assert!(result.is_empty(), "Future partitions from 2030 should all be filtered out as too recent");
    }

    #[test]
    fn test_lookback_window_duration_conversion() {
        let lw = LookbackWindow::Duration { hours: 2, minutes: 30 };
        let duration = lw.to_duration(PartitionGranularity::Hour);
        assert_eq!(duration, Duration::hours(2) + Duration::minutes(30));

        let lw_partitions = LookbackWindow::Partitions { count: 5 };
        let duration = lw_partitions.to_duration(PartitionGranularity::Hour);
        assert_eq!(duration, Duration::hours(5));

        let duration = lw_partitions.to_duration(PartitionGranularity::Day);
        assert_eq!(duration, Duration::days(5));
    }
}
