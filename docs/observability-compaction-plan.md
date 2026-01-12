# Observability Data Compaction Plan

## Overview

Extend the planner to support time-windowed compaction for observability data, where:
- Multiple writers continuously write small parquet files
- A compaction job runs periodically (e.g., every 5 minutes)
- Only recent partitions are compacted (configurable lookback window)
- Results are committed to Iceberg

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Observability Compaction Flow                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  S3 Bucket (raw parquet files)                                      │
│  └── data/events/                                                   │
│      ├── dt=2024-01-15/hour=12/  ← skip (too old)                  │
│      ├── dt=2024-01-15/hour=13/  ← compact this                    │
│      ├── dt=2024-01-15/hour=14/  ← compact this                    │
│      └── dt=2024-01-15/hour=15/  ← skip (current, writers active)  │
│                                                                      │
│  ┌──────────────────┐    ┌──────────────────┐                       │
│  │ TimeWindowedSource│───▶│ Partition Filter │                       │
│  │ (ParquetSource++)│    │ (lookback window)│                       │
│  └──────────────────┘    └────────┬─────────┘                       │
│                                   │                                  │
│                                   ▼                                  │
│                          ┌──────────────────┐                       │
│                          │  pack_files()    │                       │
│                          │  build_tasks()   │                       │
│                          └────────┬─────────┘                       │
│                                   │                                  │
│                                   ▼                                  │
│                          ┌──────────────────┐                       │
│                          │CompactionEngine  │                       │
│                          └────────┬─────────┘                       │
│                                   │                                  │
│                                   ▼                                  │
│                          ┌──────────────────┐                       │
│                          │ Iceberg Commit   │                       │
│                          │ (append files)   │                       │
│                          └──────────────────┘                       │
└─────────────────────────────────────────────────────────────────────┘
```

## Configuration

### New Config Struct: `ObservabilityConfig`

```rust
/// Configuration for observability data compaction
#[derive(Debug, Clone, Deserialize)]
pub struct ObservabilityConfig {
    /// Enable observability mode (time-windowed compaction)
    pub enabled: bool,

    /// Partition time specification
    pub partition_time: PartitionTimeSpec,

    /// Lookback window configuration
    pub lookback: LookbackWindow,

    /// Number of recent partitions to skip (avoid racing with writers)
    /// Default: 1 (skip current partition)
    #[serde(default = "default_skip_recent")]
    pub skip_recent_partitions: u32,

    /// Output mode: "in_place" or "iceberg"
    #[serde(default)]
    pub output_mode: OutputMode,
}

/// How to extract timestamp from partition values
#[derive(Debug, Clone, Deserialize)]
pub struct PartitionTimeSpec {
    /// Partition fields that make up the timestamp
    /// e.g., ["dt", "hour"] for dt=2024-01-15/hour=14
    pub fields: Vec<PartitionTimeField>,

    /// Timezone for interpreting partition values (default: UTC)
    #[serde(default = "default_timezone")]
    pub timezone: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PartitionTimeField {
    /// Field name in partition (e.g., "dt", "hour", "minute")
    pub name: String,

    /// Format for parsing (e.g., "%Y-%m-%d", "%H", "%M")
    pub format: String,

    /// What this field represents
    pub component: TimeComponent,
}

#[derive(Debug, Clone, Deserialize)]
pub enum TimeComponent {
    Date,      // Full date (YYYY-MM-DD)
    Year,      // Just year
    Month,     // Just month
    Day,       // Just day
    Hour,      // Hour (0-23)
    Minute,    // Minute (0-59)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum LookbackWindow {
    /// Process last N partitions
    #[serde(rename = "partitions")]
    Partitions { count: u32 },

    /// Process partitions within duration from now
    #[serde(rename = "duration")]
    Duration {
        hours: Option<u32>,
        minutes: Option<u32>,
    },
}

#[derive(Debug, Clone, Deserialize, Default)]
pub enum OutputMode {
    /// Write compacted files to same S3 location, then commit to Iceberg
    #[default]
    InPlace,

    /// Write to Iceberg table location
    Iceberg {
        /// Iceberg table to commit to
        namespace: String,
        table: String,
    },
}
```

### Example Config JSON

```json
{
  "s3": {
    "endpoint": "https://s3.us-east-1.amazonaws.com",
    "bucket": "observability-data",
    "region": "us-east-1"
  },
  "source": {
    "type": "parquet",
    "base_path": "data/events/"
  },
  "time_window": {
    "partition_spec": {
      "fields": [
        { "name": "dt", "format": "%Y-%m-%d", "component": "date" },
        { "name": "hour", "format": "%H", "component": "hour" }
      ],
      "timezone": "UTC",
      "granularity": "hour"
    },
    "lookback": {
      "type": "duration",
      "hours": 2
    },
    "skip_recent": 1
  },
  "catalog": {
    "type": "glue",
    "warehouse": "s3://observability-data/warehouse"
  },
  "table": {
    "namespace": "observability",
    "name": "events"
  },
  "compaction": {
    "target_file_size_mb": 256,
    "min_files_per_partition": 2
  }
}
```

## Implementation

### 1. New File: `crates/engine/src/planner/table/observability.rs`

```rust
//! Time-windowed parquet source for observability data.

use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};

/// Parquet source with time-based partition filtering.
pub struct TimeWindowedParquetSource {
    /// Underlying parquet source
    inner: ParquetSource,

    /// Time window configuration
    config: ObservabilityConfig,
}

impl TimeWindowedParquetSource {
    pub fn new(
        operator: Operator,
        base_path: String,
        bucket: String,
        config: ObservabilityConfig,
    ) -> Self { ... }

    /// Filter partitions to only those within the time window.
    fn filter_partitions(&self, partitions: Vec<String>) -> Result<Vec<String>> {
        let now = Utc::now();
        let cutoff = self.calculate_cutoff(now);
        let skip_after = self.calculate_skip_threshold(now);

        partitions
            .into_iter()
            .filter(|p| {
                match self.parse_partition_time(p) {
                    Ok(ts) => ts >= cutoff && ts < skip_after,
                    Err(_) => false, // Skip unparseable partitions
                }
            })
            .collect()
    }

    /// Parse partition path to extract timestamp.
    /// e.g., "data/events/dt=2024-01-15/hour=14/" -> 2024-01-15 14:00:00 UTC
    fn parse_partition_time(&self, partition_path: &str) -> Result<DateTime<Utc>> {
        // Extract key=value pairs from path
        let values = self.extract_partition_values(partition_path)?;

        // Build timestamp from configured fields
        let mut date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let mut time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();

        for field in &self.config.partition_time.fields {
            let value = values.get(&field.name)
                .ok_or_else(|| anyhow!("Missing partition field: {}", field.name))?;

            match field.component {
                TimeComponent::Date => {
                    date = NaiveDate::parse_from_str(value, &field.format)?;
                }
                TimeComponent::Hour => {
                    let hour: u32 = value.parse()?;
                    time = time.with_hour(hour).unwrap();
                }
                // ... handle other components
            }
        }

        let naive = NaiveDateTime::new(date, time);
        Ok(DateTime::from_naive_utc_and_offset(naive, Utc))
    }

    fn calculate_cutoff(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        match &self.config.lookback {
            LookbackWindow::Duration { hours, minutes } => {
                let mut duration = Duration::zero();
                if let Some(h) = hours {
                    duration = duration + Duration::hours(*h as i64);
                }
                if let Some(m) = minutes {
                    duration = duration + Duration::minutes(*m as i64);
                }
                now - duration
            }
            LookbackWindow::Partitions { count } => {
                // For partition-based, we'll filter after sorting
                DateTime::UNIX_EPOCH
            }
        }
    }

    fn calculate_skip_threshold(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        // Skip current partition to avoid racing with writers
        // Truncate to partition boundary and subtract skip count
        let truncated = self.truncate_to_partition_boundary(now);
        truncated - Duration::hours(self.config.skip_recent_partitions as i64)
    }
}

#[async_trait]
impl TableSource for TimeWindowedParquetSource {
    fn format(&self) -> TableFormat {
        TableFormat::Parquet
    }

    fn location(&self) -> &str {
        self.inner.location()
    }

    async fn schema(&self) -> Result<SchemaRef> {
        self.inner.schema().await
    }

    async fn discover(&self) -> Result<PartitionFileMap> {
        // Get all partitions from inner source
        let all_partitions = self.inner.discover().await?;

        // Filter by time window
        let filtered_keys: HashSet<_> = self.filter_partitions(
            all_partitions.keys().cloned().collect()
        )?.into_iter().collect();

        // Return only matching partitions
        Ok(all_partitions
            .into_iter()
            .filter(|(k, _)| filtered_keys.contains(k))
            .collect())
    }
}
```

### 2. Update `crates/megaberg/src/configs.rs`

Add `ObservabilityConfig` to the main config:

```rust
#[derive(Debug, Deserialize)]
pub struct Config {
    pub s3: S3Config,
    pub catalog: CatalogConfigJson,
    pub table: TableConfig,
    pub compaction: Option<CompactionConfigJson>,

    /// Observability mode configuration (optional)
    #[serde(default)]
    pub observability: Option<ObservabilityConfig>,

    /// Source configuration (for parquet-based discovery)
    #[serde(default)]
    pub source: Option<SourceConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum SourceConfig {
    #[serde(rename = "iceberg")]
    Iceberg,

    #[serde(rename = "parquet")]
    Parquet {
        base_path: String,
    },
}
```

### 3. Update `crates/megaberg/src/main.rs`

Add logic to choose source based on config:

```rust
// Determine source type
let source: Box<dyn TableSource> = match &config.source {
    Some(SourceConfig::Parquet { base_path }) => {
        if let Some(obs_config) = &config.observability {
            if obs_config.enabled {
                Box::new(TimeWindowedParquetSource::new(
                    operator.clone(),
                    base_path.clone(),
                    config.s3.bucket.clone(),
                    obs_config.clone(),
                ))
            } else {
                Box::new(ParquetSource::new(
                    operator.clone(),
                    base_path.clone(),
                    config.s3.bucket.clone(),
                ))
            }
        } else {
            Box::new(ParquetSource::new(...))
        }
    }
    _ => {
        // Default: use IcebergSource
        Box::new(IcebergSource::new(...).await?)
    }
};
```

## File Changes Summary

| File | Change |
|------|--------|
| `crates/engine/src/planner/table/mod.rs` | Add `observability` module, re-export types |
| `crates/engine/src/planner/table/observability.rs` | **New file** - TimeWindowedParquetSource |
| `crates/engine/src/planner/mod.rs` | Re-export observability types |
| `crates/megaberg/src/configs.rs` | Add ObservabilityConfig, SourceConfig |
| `crates/megaberg/src/main.rs` | Add source selection logic |
| `crates/engine/Cargo.toml` | Already has chrono (may need to re-add) |

## Workflow Example

1. **Cron job runs every 5 minutes**
   ```bash
   megaberg --config observability-config.json
   ```

2. **TimeWindowedParquetSource discovers partitions**
   - Lists all partitions: `dt=2024-01-15/hour=12`, `hour=13`, `hour=14`, `hour=15`
   - Current time: 15:30 UTC
   - Lookback: 2 hours → cutoff = 13:30
   - Skip recent: 1 → skip threshold = 15:00
   - **Result**: Only `hour=14` is processed

3. **Compaction runs**
   - Packs files in `hour=14` partition
   - Writes compacted parquet files

4. **Commit to Iceberg**
   - Appends new data files to Iceberg table
   - Old raw files can be cleaned up separately

## Edge Cases

1. **Partition boundary races**: Handled by `skip_recent_partitions`
2. **Empty partitions**: Filtered out naturally
3. **Invalid partition paths**: Logged and skipped
4. **Timezone handling**: All times normalized to UTC
5. **Reprocessing same partition**: Iceberg handles deduplication via manifest tracking

## Future Enhancements

1. **Incremental tracking**: Remember last processed partition to avoid reprocessing
2. **Parallel partition compaction**: Process multiple time partitions concurrently
3. **Cleanup job**: Delete raw files after successful Iceberg commit
4. **Metrics**: Emit compaction metrics (files processed, bytes written, latency)
