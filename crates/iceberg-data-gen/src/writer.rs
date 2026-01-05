use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::future::BoxFuture;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, PartitionSpec,
    PrimitiveLiteral, Struct,
};
use opendal::Operator;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

struct OpendalWriter(opendal::Writer);

impl AsyncFileWriter for OpendalWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            self.0
                .write(bs)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            self.0
                .close()
                .await
                .map(|_| ())
                .map_err(|e| ParquetError::External(Box::new(e)))
        })
    }
}

pub async fn write_parquet_file(
    operator: &Operator,
    table_location: &str,
    bucket: &str,
    batch: RecordBatch,
    partition_spec: &PartitionSpec,
    compression: Compression,
) -> Result<DataFile> {
    // 1. Extract partition values from the first row
    let alias = batch
        .column_by_name("alias")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .value(0);
    let ws_id = batch
        .column_by_name("workspace_id")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .value(0);
    let cluster = batch
        .column_by_name("cluster_name")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .value(0);
    // start_time is timestamptz (micros)
    let start_time_micros = batch
        .column_by_name("start_time")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
        .unwrap()
        .value(0);

    // Compute hours from epoch for Iceberg partition value (int)
    let hours_since_epoch = (start_time_micros / 1_000_000) / 3600;

    // Compute formatted hour for Hive path usage
    let dt = Utc.timestamp_micros(start_time_micros).unwrap();
    let formatted_hour = dt.format("%Y-%m-%d-%H").to_string();

    // 2. Construct path
    let table_prefix = table_location
        .trim_start_matches(&format!("s3://{}/", bucket))
        .trim_start_matches(&format!("s3a://{}/", bucket))
        .trim_start_matches("/");

    let file_name = format!("{}.parquet", Uuid::new_v4());
    let partition_path = format!(
        "alias={}/workspace_id={}/cluster_name={}/added_on_hour={}",
        alias, ws_id, cluster, formatted_hour
    );
    let file_path = format!("{}/data/{}/{}", table_prefix, partition_path, file_name);

    // 3. Write
    let mut writer = operator
        .writer(&file_path)
        .await
        .context("Failed to create writer")?;
    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();

    let mut arrow_writer =
        AsyncArrowWriter::try_new(OpendalWriter(writer), batch.schema(), Some(props))?;
    arrow_writer.write(&batch).await?;
    arrow_writer.close().await?;

    // 4. Stat for size
    let meta = operator.stat(&file_path).await?;
    let file_size = meta.content_length();
    let row_count = batch.num_rows() as u64;

    // 5. Build DataFile
    let partition_values = vec![
        Some(Literal::Primitive(PrimitiveLiteral::String(
            alias.to_string(),
        ))),
        Some(Literal::Primitive(PrimitiveLiteral::String(
            ws_id.to_string(),
        ))),
        Some(Literal::Primitive(PrimitiveLiteral::String(
            cluster.to_string(),
        ))),
        Some(Literal::Primitive(PrimitiveLiteral::Int(
            hours_since_epoch as i32,
        ))),
    ];

    let partition_struct = Struct::from_iter(partition_values);

    let full_path = format!("s3://{}/{}", bucket, file_path);

    let data_file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(full_path)
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(file_size)
        .record_count(row_count)
        .partition(partition_struct)
        .partition_spec_id(partition_spec.spec_id())
        .build()
        .context("Failed to build data file")?;

    Ok(data_file)
}
