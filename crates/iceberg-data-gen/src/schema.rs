use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use iceberg::spec::{PartitionSpec, Transform};
use std::sync::Arc;

pub fn get_arrow_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("cluster_name", DataType::Utf8, false),
        Field::new("query_id", DataType::Utf8, false),
        Field::new("catalog", DataType::Utf8, true),
        Field::new("query", DataType::Utf8, true),
        Field::new("end_time", DataType::Int64, true),
        Field::new("is_cached", DataType::Boolean, true),
        Field::new("explain_analyse_output", DataType::Utf8, true),
        Field::new("execution_time", DataType::Float64, true),
        Field::new("workspace_id", DataType::Utf8, false),
        Field::new("query_hash", DataType::Utf8, true),
        Field::new(
            "start_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("database", DataType::Utf8, true),
        Field::new("client_perceived_time", DataType::Float64, true),
        Field::new("parsing_time", DataType::Float64, true),
        Field::new("cluster_uuid", DataType::Utf8, true),
        Field::new("queueing_time", DataType::Float64, true),
        Field::new("alias", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
    ]))
}

// Helper to construct partition spec
// logic: alias, ws, cluster, start_time (hour)
pub fn get_partition_spec(schema: &iceberg::spec::Schema) -> PartitionSpec {
    // 1: alias (17)
    // 2: workspace_id (9)
    // 3: cluster_name (1)
    // 4: added_on_hour (start_time 11 -> hour)

    // Builder expects impl Into<SchemaRef> (Arc<Schema>)
    let schema_ref = std::sync::Arc::new(schema.clone());

    PartitionSpec::builder(schema_ref)
        .with_spec_id(0)
        .add_partition_field("alias", "alias", Transform::Identity)
        .unwrap()
        .add_partition_field("workspace_id", "workspace_id", Transform::Identity)
        .unwrap()
        .add_partition_field("cluster_name", "cluster_name", Transform::Identity)
        .unwrap()
        .add_partition_field("start_time", "added_on_hour", Transform::Hour)
        .unwrap()
        .build()
        .expect("Failed to build partition spec")
}

pub fn get_iceberg_schema() -> iceberg::spec::Schema {
    use iceberg::spec::{NestedField, PrimitiveType, Type};

    iceberg::spec::Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "cluster_name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(2, "query_id", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "catalog", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(4, "query", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(5, "end_time", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(6, "is_cached", Type::Primitive(PrimitiveType::Boolean)).into(),
            NestedField::optional(
                7,
                "explain_analyse_output",
                Type::Primitive(PrimitiveType::String),
            )
            .into(),
            NestedField::optional(8, "execution_time", Type::Primitive(PrimitiveType::Double))
                .into(),
            NestedField::required(9, "workspace_id", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(10, "query_hash", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(11, "start_time", Type::Primitive(PrimitiveType::Timestamp))
                .into(),
            NestedField::optional(12, "database", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(
                13,
                "client_perceived_time",
                Type::Primitive(PrimitiveType::Double),
            )
            .into(),
            NestedField::optional(14, "parsing_time", Type::Primitive(PrimitiveType::Double))
                .into(),
            NestedField::optional(15, "cluster_uuid", Type::Primitive(PrimitiveType::String))
                .into(),
            NestedField::optional(16, "queueing_time", Type::Primitive(PrimitiveType::Double))
                .into(),
            NestedField::required(17, "alias", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(18, "email", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(19, "status", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("Failed to build iceberg schema")
}
