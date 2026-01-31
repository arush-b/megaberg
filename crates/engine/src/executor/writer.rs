//! Rolling Parquet writer with multipart upload support and Iceberg metadata extraction.
use std::collections::HashMap;
use std::sync::Arc;

use super::config::CompactionConfig;
use arrow::datatypes::{Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;
use opendal::Operator;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use parquet::format::FileMetaData;

/// Metadata key for Iceberg field IDs in Arrow schema.
pub const PARQUET_FIELD_ID_META_KEY: &str = "PARQUET:field_id";

pub struct OpendalAsyncWriter {
    inner: opendal::Writer,
}

impl OpendalAsyncWriter {
    pub fn new(writer: opendal::Writer) -> Self {
        Self { inner: writer }
    }
}

impl AsyncFileWriter for OpendalAsyncWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        async move {
            self.inner
                .write(bs)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            Ok(())
        }
        .boxed()
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        async move {
            self.inner
                .close()
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            Ok(())
        }
        .boxed()
    }
}

/// Column-level statistics extracted from Parquet metadata.
#[derive(Debug, Clone, Default)]
pub struct ColumnStats {
    pub column_sizes: HashMap<i32, u64>,
    pub value_counts: HashMap<i32, u64>,
    pub null_value_counts: HashMap<i32, u64>,
    pub lower_bounds: HashMap<i32, Vec<u8>>,
    pub upper_bounds: HashMap<i32, Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct WrittenFile {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
    pub split_offsets: Vec<i64>,
    pub column_stats: ColumnStats,
}

/// Assigns sequential field IDs to an Arrow schema.
pub fn add_field_ids_to_schema(schema: &Schema, start_id: i32) -> (Schema, i32) {
    let (fields, next_id) = add_field_ids_to_fields(schema.fields(), start_id);
    (
        Schema::new_with_metadata(fields, schema.metadata().clone()),
        next_id,
    )
}

/// Recursively adds field IDs to a list of fields.
fn add_field_ids_to_fields(fields: &Fields, mut next_id: i32) -> (Fields, i32) {
    let new_fields: Vec<Field> = fields
        .iter()
        .map(|field| {
            let (new_field, new_next_id) = add_field_id_to_field(field.as_ref(), next_id);
            next_id = new_next_id;
            new_field
        })
        .collect();
    (Fields::from(new_fields), next_id)
}

/// Adds a field ID to a single field, recursively handling nested types.
fn add_field_id_to_field(field: &Field, mut next_id: i32) -> (Field, i32) {
    let field_id = next_id;
    next_id += 1;

    // Handle nested types recursively
    let new_data_type = match field.data_type() {
        arrow::datatypes::DataType::Struct(nested_fields) => {
            let (new_fields, new_next_id) = add_field_ids_to_fields(nested_fields, next_id);
            next_id = new_next_id;
            arrow::datatypes::DataType::Struct(new_fields)
        }
        arrow::datatypes::DataType::List(inner) => {
            let (new_inner, new_next_id) = add_field_id_to_field(inner.as_ref(), next_id);
            next_id = new_next_id;
            arrow::datatypes::DataType::List(Arc::new(new_inner))
        }
        arrow::datatypes::DataType::LargeList(inner) => {
            let (new_inner, new_next_id) = add_field_id_to_field(inner.as_ref(), next_id);
            next_id = new_next_id;
            arrow::datatypes::DataType::LargeList(Arc::new(new_inner))
        }
        arrow::datatypes::DataType::Map(entries, sorted) => {
            let (new_entries, new_next_id) = add_field_id_to_field(entries.as_ref(), next_id);
            next_id = new_next_id;
            arrow::datatypes::DataType::Map(Arc::new(new_entries), *sorted)
        }
        other => other.clone(),
    };

    // Add field ID metadata
    let mut metadata = field.metadata().clone();
    metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());

    let new_field =
        Field::new(field.name(), new_data_type, field.is_nullable()).with_metadata(metadata);

    (new_field, next_id)
}

/// Builds a mapping from Parquet column path to field ID.
fn build_path_to_field_id_map(schema: &Schema) -> HashMap<String, i32> {
    let mut map = HashMap::new();
    for field in schema.fields() {
        collect_field_ids(field, "", &mut map);
    }
    map
}

/// Recursively collects field IDs from a field and its children.
fn collect_field_ids(field: &Field, parent_path: &str, map: &mut HashMap<String, i32>) {
    let current_path = if parent_path.is_empty() {
        field.name().to_string()
    } else {
        format!("{}.{}", parent_path, field.name())
    };

    if let Some(id_str) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) {
        if let Ok(id) = id_str.parse::<i32>() {
            map.insert(current_path.clone(), id);
        }
    }

    match field.data_type() {
        arrow::datatypes::DataType::Struct(nested_fields) => {
            for nested_field in nested_fields {
                collect_field_ids(nested_field, &current_path, map);
            }
        }
        arrow::datatypes::DataType::List(inner) | arrow::datatypes::DataType::LargeList(inner) => {
            // Parquet uses "list.element" for list elements
            let list_path = format!("{current_path}.list");
            collect_field_ids(inner, &list_path, map);
        }
        arrow::datatypes::DataType::Map(entries, _) => {
            // Parquet uses "key_value.key" and "key_value.value" for map entries
            let kv_path = format!("{current_path}.key_value");
            collect_field_ids(entries, &kv_path, map);
        }
        _ => {}
    }
}

/// Extracts column statistics from Parquet FileMetaData.
fn extract_column_stats(
    file_metadata: &FileMetaData,
    path_to_field_id: &HashMap<String, i32>,
) -> ColumnStats {
    let mut stats = ColumnStats::default();

    for row_group in &file_metadata.row_groups {
        for column_chunk in &row_group.columns {
            if let Some(meta) = &column_chunk.meta_data {
                // Build column path from path_in_schema
                let column_path = meta.path_in_schema.join(".");

                // Look up field ID
                let Some(&field_id) = path_to_field_id.get(&column_path) else {
                    continue;
                };

                // Accumulate column sizes (compressed)
                *stats.column_sizes.entry(field_id).or_insert(0) +=
                    meta.total_compressed_size as u64;

                // Accumulate value counts
                *stats.value_counts.entry(field_id).or_insert(0) += meta.num_values as u64;

                // Extract statistics if present
                if let Some(col_stats) = &meta.statistics {
                    // Null count
                    if let Some(null_count) = col_stats.null_count {
                        *stats.null_value_counts.entry(field_id).or_insert(0) += null_count as u64;
                    }

                    // Lower bound (prefer min_value over deprecated min)
                    if let Some(min_val) = col_stats.min_value.as_ref().or(col_stats.min.as_ref()) {
                        // For bounds, we take the minimum across row groups
                        stats
                            .lower_bounds
                            .entry(field_id)
                            .and_modify(|existing| {
                                if min_val < existing {
                                    *existing = min_val.clone();
                                }
                            })
                            .or_insert_with(|| min_val.clone());
                    }

                    // Upper bound (prefer max_value over deprecated max)
                    if let Some(max_val) = col_stats.max_value.as_ref().or(col_stats.max.as_ref()) {
                        // For bounds, we take the maximum across row groups
                        stats
                            .upper_bounds
                            .entry(field_id)
                            .and_modify(|existing| {
                                if max_val > existing {
                                    *existing = max_val.clone();
                                }
                            })
                            .or_insert_with(|| max_val.clone());
                    }
                }
            }
        }
    }

    stats
}

/// Extracts split offsets (row group file offsets) from FileMetaData.
fn extract_split_offsets(file_metadata: &FileMetaData) -> Vec<i64> {
    file_metadata
        .row_groups
        .iter()
        .filter_map(|rg| rg.file_offset)
        .collect()
}

/// Calculates total file size from row group metadata.
fn calculate_file_size(file_metadata: &FileMetaData) -> u64 {
    file_metadata
        .row_groups
        .iter()
        .map(|rg| {
            rg.total_compressed_size
                .map(|s| s as u64)
                .unwrap_or_else(|| rg.total_byte_size as u64)
        })
        .sum()
}

pub struct RollingParquetWriter {
    operator: Operator,
    schema: SchemaRef,
    output_prefix: String,
    file_prefix: String,
    writer_props: Arc<WriterProperties>,
    multipart_chunk_bytes: usize,
    target_file_size_bytes: u64,

    // Path to field ID mapping for statistics extraction
    path_to_field_id: HashMap<String, i32>,

    // Current file state
    current_writer: Option<AsyncArrowWriter<OpendalAsyncWriter>>,
    current_path: Option<String>,
    current_rows: u64,
    file_index: u64,

    // Completed files
    completed: Vec<WrittenFile>,
}

impl RollingParquetWriter {
    /// Creates a new RollingParquetWriter.
    ///
    /// The schema should have `PARQUET:field_id` metadata on each field for
    /// Iceberg compatibility. Use `add_field_ids_to_schema` to add them if needed.
    pub fn new(
        operator: Operator,
        schema: SchemaRef,
        output_prefix: String,
        file_prefix: String,
        config: &CompactionConfig,
    ) -> Self {
        let writer_props = Arc::new(
            WriterProperties::builder()
                .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_1_0)
                .set_compression(config.compression)
                .set_max_row_group_size(config.row_group_size)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
                .build(),
        );

        // Build path to field ID mapping for stats extraction
        let path_to_field_id = build_path_to_field_id_map(&schema);

        Self {
            operator,
            schema,
            output_prefix,
            file_prefix,
            writer_props,
            multipart_chunk_bytes: config.multipart_chunk_bytes,
            target_file_size_bytes: config.target_file_size_bytes,
            path_to_field_id,
            current_writer: None,
            current_path: None,
            current_rows: 0,
            file_index: 0,
            completed: Vec::new(),
        }
    }

    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), ParquetError> {
        if self.current_writer.is_none() {
            self.start_new_file().await?;
        }

        let writer = self.current_writer.as_mut().unwrap();
        writer.write(batch).await?;
        self.current_rows += batch.num_rows() as u64;

        // Check if we need to roll to a new file
        if writer.bytes_written() as u64 >= self.target_file_size_bytes {
            self.finish_current_file().await?;
        }

        Ok(())
    }

    /// Finish writing and return statistics for all completed files.
    pub async fn finish(mut self) -> Result<Vec<WrittenFile>, ParquetError> {
        self.finish_current_file().await?;
        Ok(self.completed)
    }

    /// Start a new output file.
    async fn start_new_file(&mut self) -> Result<(), ParquetError> {
        let filename = format!(
            "{}-{:06}-{}.parquet",
            self.file_prefix,
            self.file_index,
            uuid_short()
        );
        self.file_index += 1;

        let path = if self.output_prefix.is_empty() {
            filename
        } else {
            format!("{}/{}", self.output_prefix.trim_end_matches('/'), filename)
        };

        let opendal_writer = self
            .operator
            .writer_with(&path)
            .chunk(self.multipart_chunk_bytes)
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?;

        let async_writer = OpendalAsyncWriter::new(opendal_writer);
        let parquet_writer = AsyncArrowWriter::try_new(
            async_writer,
            self.schema.clone(),
            Some((*self.writer_props).clone()),
        )?;

        self.current_writer = Some(parquet_writer);
        self.current_path = Some(path);
        self.current_rows = 0;

        Ok(())
    }

    async fn finish_current_file(&mut self) -> Result<(), ParquetError> {
        if let Some(writer) = self.current_writer.take() {
            // close() returns FileMetaData (Thrift format) with all statistics
            let file_metadata = writer.close().await?;

            if let Some(path) = self.current_path.take() {
                // Extract all metadata from FileMetaData
                let column_stats = extract_column_stats(&file_metadata, &self.path_to_field_id);
                let split_offsets = extract_split_offsets(&file_metadata);

                // Get actual file size from storage (includes header, footer, and magic bytes)
                let size_bytes = self
                    .operator
                    .stat(&path)
                    .await
                    .map(|s| s.content_length())
                    .unwrap_or_else(|_| calculate_file_size(&file_metadata));

                self.completed.push(WrittenFile {
                    path,
                    size_bytes,
                    record_count: self.current_rows,
                    split_offsets,
                    column_stats,
                });
            }
        }
        Ok(())
    }
}

/// Generate a short UUID-like string for unique file naming.
fn uuid_short() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    // Combine timestamp with a random component
    let ts = now.as_nanos();
    format!("{:x}", ts & 0xFFFFFFFF)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn test_add_field_ids_simple() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let (new_schema, next_id) = add_field_ids_to_schema(&schema, 1);

        assert_eq!(next_id, 3);

        let id_field = new_schema.field(0);
        assert_eq!(
            id_field.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"1".to_string())
        );

        let name_field = new_schema.field(1);
        assert_eq!(
            name_field.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"2".to_string())
        );
    }

    #[test]
    fn test_add_field_ids_nested() {
        let schema = Schema::new(vec![Field::new(
            "user",
            DataType::Struct(Fields::from(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("email", DataType::Utf8, true),
            ])),
            false,
        )]);

        let (new_schema, next_id) = add_field_ids_to_schema(&schema, 1);

        // user=1, id=2, email=3
        assert_eq!(next_id, 4);

        let user_field = new_schema.field(0);
        assert_eq!(
            user_field.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"1".to_string())
        );

        if let DataType::Struct(fields) = user_field.data_type() {
            assert_eq!(
                fields[0].metadata().get(PARQUET_FIELD_ID_META_KEY),
                Some(&"2".to_string())
            );
            assert_eq!(
                fields[1].metadata().get(PARQUET_FIELD_ID_META_KEY),
                Some(&"3".to_string())
            );
        } else {
            panic!("Expected struct type");
        }
    }

    #[test]
    fn test_build_path_to_field_id_map() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]);

        let map = build_path_to_field_id_map(&schema);

        assert_eq!(map.get("id"), Some(&1));
        assert_eq!(map.get("name"), Some(&2));
    }
}
