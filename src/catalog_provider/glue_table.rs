use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::{any::Any, sync::Arc};
use crate::error;
use crate::glue_data_type_parser::*;

use datafusion::common::GetExt;
use futures::{
    future,
    stream::{self, BoxStream},
    StreamExt, TryStreamExt,
};
use object_store::{ObjectMeta, ObjectStore};

use async_trait::async_trait;
use aws_sdk_glue::{types::Table, Client};
use aws_sdk_glue::types::{Column, StorageDescriptor};

use datafusion::{
    arrow::{
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
        record_batch::RecordBatch,
    },
    common::{DataFusionError, ToDFSchema},
    datasource::{
        listing::{ListingOptions, ListingTableUrl, PartitionedFile},
        physical_plan::FileScanConfig,
        TableProvider,
        file_format::{
            avro::AvroFormat, csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat, FileFormat,
        },
    },
    error::Result,
    execution::{context::SessionState, TaskContext},
    logical_expr::{utils::conjunction, Expr, TableProviderFilterPushDown, TableType},
    optimizer::OptimizerConfig,
    physical_expr::{planner::create_physical_expr, PhysicalSortExpr},
    physical_plan::{
        common::compute_record_batch_statistics, internal_err, memory::MemoryStream, DisplayAs,
        DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
    },
    scalar::ScalarValue,
};

pub struct GlueTable {
    pub table: Table,
    pub client: Client,
    pub schema: SchemaRef,
    pub listing_options: ListingOptions,
}

fn scalar_to_string(scalar: &ScalarValue) -> String {
    match scalar {
        ScalarValue::Utf8(Some(v)) => format!("'{}'", v),
        _ => scalar.to_string(),
    }
}

impl GlueTable {
    pub fn new(table: Table, client: Client) -> error::Result<Self> {
        let sd = Self::get_storage_descriptor(&table)?;
        let database_name = table.database_name().unwrap();
        let table_name = table.name();
        let mut listing_options = Self::get_listing_options(database_name, table_name, &sd)?;
        let partitions = table.partition_keys();
        let schema = Self::derive_schema(database_name, table_name, &table)?;
        let fields = schema.fields().iter().as_slice();
        let part_cols = fields[(fields.len() - partitions.len())..]
            .iter()
            .map(|f| (f.name().to_string(), f.data_type().clone()))
            .collect::<Vec<_>>();

        listing_options = listing_options.with_table_partition_cols(part_cols);


        Ok(Self { table, client, schema: Arc::new(schema), listing_options })
    }
    
    fn get_glue_expr(&self, filters: &[Expr]) -> Option<String> {
        let mut glue_expr: Vec<String> = vec![];

        let partition_keys = self
            .table
            .partition_keys()
            .iter()
            .map(|c| c.name.clone())
            .collect::<HashSet<_>>();

        for f in filters.iter() {
            match f {
                Expr::BinaryExpr(b) => {
                    if let (Expr::Column(column), Expr::Literal(scalar)) =
                        (b.left.as_ref(), b.right.as_ref())
                    {
                        if partition_keys.contains(&column.name) {
                            glue_expr.push(format!(
                                "{} {} {}",
                                column.name,
                                b.op,
                                scalar_to_string(scalar)
                            ));
                        }
                    }
                }
                Expr::InList(b) => {
                    let all_literals = b
                        .list
                        .iter()
                        .map(|v| match v {
                            Expr::Literal(lit) => Some(scalar_to_string(lit)),
                            _ => None,
                        })
                        .collect::<Option<Vec<_>>>();
                    if let Some(literals) = all_literals {
                        if let Expr::Column(column) = b.expr.as_ref() {
                            if partition_keys.contains(&column.name) {
                                glue_expr.push(format!(
                                    "{} in ({:?})",
                                    column.name,
                                    literals.join(",")
                                ))
                            }
                        }
                    }
                }
                _ => continue,
            }
        }
        if glue_expr.len() > 0 {
            return Some(glue_expr.join(" AND "));
        }
        None
    }

    fn get_listing_options(
        database_name: &str,
        table_name: &str,
        sd: &StorageDescriptor,
    ) -> error::Result<ListingOptions> {
        Self::calculate_options(sd)
            .map_err(|e| Self::wrap_error_with_table_info(database_name, table_name, e))
    }

    fn derive_schema(database_name: &str, table_name: &str, table: &Table) -> error::Result<Schema> {
        let sd = Self::get_storage_descriptor(table)?;
        let mut columns = Self::get_columns(&sd)?.clone();
        if let Some(part_keys) = &table.partition_keys {
            columns.extend(part_keys.iter().cloned());
        }
        Self::map_glue_columns_to_arrow_schema(&columns)
            .map_err(|e| Self::wrap_error_with_table_info(database_name, table_name, e))
    }

    fn get_columns(sd: &StorageDescriptor) -> error::Result<&Vec<Column>> {
        sd.columns.as_ref().ok_or_else(|| {
            error::GlueError::AWS(
                "Failed to find columns in storage descriptor for glue table".to_string(),
            )
        })
    }

    fn get_storage_location(sd: &StorageDescriptor) -> error::Result<&str> {
        sd.location.as_deref().ok_or_else(|| {
            error::GlueError::AWS("Failed to find uri in storage descriptor for glue table".to_string())
        })
    }

    fn get_storage_descriptor(glue_table: &Table) -> error::Result<StorageDescriptor> {
        glue_table.storage_descriptor.clone().ok_or_else(|| {
            error::GlueError::AWS("Failed to find storage descriptor for glue table".to_string())
        })
    }

    fn get_database_name(glue_table: &Table) -> error::Result<&str> {
        glue_table
            .database_name
            .as_deref()
            .ok_or_else(|| error::GlueError::AWS("Failed to find name for glue database".to_string()))
    }

    fn wrap_error_with_table_info(
        database_name: &str,
        table_name: &str,
        e: error::GlueError,
    ) -> error::GlueError {
        match e {
            error::GlueError::NotImplemented(msg) => {
                error::GlueError::NotImplemented(format!("{}.{}: {}", database_name, table_name, msg))
            }
            _ => e,
        }
    }

    fn calculate_options(sd: &StorageDescriptor) -> error::Result<ListingOptions> {
        let empty_str = String::from("");
        let input_format = sd.input_format.as_ref().unwrap_or(&empty_str);
        let output_format = sd.output_format.as_ref().unwrap_or(&empty_str);
        let serde_info = sd.serde_info.as_ref().ok_or_else(|| {
            error::GlueError::AWS(
                "Failed to find serde_info in storage descriptor for glue table".to_string(),
            )
        })?;
        let serialization_library = serde_info
            .serialization_library
            .as_ref()
            .unwrap_or(&empty_str);
        let serde_info_parameters = serde_info
            .parameters
            .as_ref()
            .ok_or_else(|| {
                error::GlueError::AWS(
                    "Failed to find parameters of serde_info in storage descriptor for glue table"
                        .to_string(),
                )
            })?
            .clone();
        let sd_parameters = match &sd.parameters {
            Some(x) => x.clone(),
            None => HashMap::new(),
        };

        let item: (&str, &str, &str) = (input_format, output_format, serialization_library);
        let format_result: error::Result<Box<dyn FileFormat>> = match item {
            (
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            ) => Ok(Box::new(ParquetFormat::default())),
            (
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            ) => {
                let mut format = CsvFormat::default();
                let delim = serde_info_parameters
                    .get("field.delim")
                    .ok_or_else(|| {
                        error::GlueError::AWS(
                            "Failed to find field.delim in serde_info parameters".to_string(),
                        )
                    })?
                    .as_bytes();
                let delim_char = delim[0];
                format = format.with_delimiter(delim_char);
                let has_header = sd_parameters
                    .get("skip.header.line.count")
                    .unwrap_or(&empty_str)
                    .eq("1");
                format = format.with_has_header(has_header);
                Ok(Box::new(format))
            }
            (
                "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
                "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
                "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
            ) => Ok(Box::new(AvroFormat::default())),
            (
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "org.apache.hive.hcatalog.data.JsonSerDe",
            ) => Ok(Box::new(JsonFormat::default())),
            (
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "org.openx.data.jsonserde.JsonSerDe",
            ) => Ok(Box::new(JsonFormat::default())),
            (
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "com.amazon.ionhiveserde.IonHiveSerDe",
            ) => Ok(Box::new(JsonFormat::default())),
            _ => Err(error::GlueError::NotImplemented(format!(
                "No support for: {}, {}, {:?} yet.",
                input_format, output_format, sd
            ))),
        };
        let format = format_result?;

        let listing_options = ListingOptions {
            // empty extension doesn't work, as it fails on empty files, like spark _SUCCESS flags
            // TODO: find a way how to support compressed files, like *.csv.gz
            file_extension: format.file_type().get_ext(),
            format: Arc::from(format),
            table_partition_cols: vec![],
            collect_stat: true,
            target_partitions: 1,
            file_sort_order: vec![],
            file_type_write_options: None,
        };

        Ok(listing_options)
    }

    fn map_glue_data_type_to_arrow_data_type(glue_data_type: &GlueDataType) -> error::Result<DataType> {
        match glue_data_type {
            GlueDataType::TinyInt => Ok(DataType::Int8),
            GlueDataType::SmallInt => Ok(DataType::Int16),
            GlueDataType::Int => Ok(DataType::Int32),
            GlueDataType::Boolean => Ok(DataType::Boolean),
            GlueDataType::BigInt => Ok(DataType::Int64),
            GlueDataType::Float => Ok(DataType::Float32),
            GlueDataType::Double => Ok(DataType::Float64),
            GlueDataType::Binary => Ok(DataType::Binary),
            GlueDataType::Timestamp => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            GlueDataType::String => Ok(DataType::Utf8),
            GlueDataType::Char => Ok(DataType::Utf8),
            GlueDataType::Varchar => Ok(DataType::Utf8),
            GlueDataType::Date => Ok(DataType::Date32),
            GlueDataType::Decimal(precision, scale) => {
                Ok(DataType::Decimal256(*precision as u8, *scale as i8))
            }
            GlueDataType::Array(inner_data_type) => {
                let array_arrow_data_type =
                    Self::map_glue_data_type_to_arrow_data_type(inner_data_type)?;
                Ok(DataType::List(Arc::new(Field::new(
                    "item",
                    array_arrow_data_type,
                    true,
                ))))
            }
            GlueDataType::Map(key_glue_data_type, value_glue_data_type) => {
                let key_arrow_data_type =
                    Self::map_glue_data_type_to_arrow_data_type(key_glue_data_type)?;
                let value_arrow_data_type =
                    Self::map_glue_data_type_to_arrow_data_type(value_glue_data_type)?;
                Ok(DataType::Map(
                    Arc::new(Field::new(
                        "key_value",
                        DataType::Struct(
                            vec![
                                Field::new("key", key_arrow_data_type, true),
                                Field::new("value", value_arrow_data_type, true),
                            ]
                            .into(),
                        ),
                        true,
                    )),
                    true,
                ))
            }
            GlueDataType::Struct(glue_fields) => {
                let mut fields = Vec::new();
                for glue_field in glue_fields {
                    let field_arrow_data_type =
                        Self::map_glue_data_type_to_arrow_data_type(&glue_field.data_type)?;
                    fields.push(Field::new(&glue_field.name, field_arrow_data_type, true));
                }
                Ok(DataType::Struct(fields.into()))
            }
        }
    }

    fn map_glue_data_type(glue_data_type: &str) -> error::Result<DataType> {
        let parsed_glue_data_type = parse_glue_data_type(glue_data_type).map_err(|e| {
            error::GlueError::GlueDataTypeMapping(format!(
                "Error while parsing {}: {:?}",
                glue_data_type, e
            ))
        })?;

        Self::map_glue_data_type_to_arrow_data_type(&parsed_glue_data_type)
    }

    fn map_glue_column_to_arrow_field(glue_column: &Column) -> error::Result<Field> {
        let name = glue_column.name();
        let glue_type = glue_column
            .r#type
            .as_ref()
            .ok_or_else(|| error::GlueError::AWS("Failed to find type in glue column".to_string()))?
            .clone();
        Self::map_to_arrow_field(&name, &glue_type)
    }

    fn map_to_arrow_field(glue_name: &str, glue_type: &str) -> error::Result<Field> {
        let arrow_data_type = Self::map_glue_data_type(glue_type)?;
        Ok(Field::new(glue_name, arrow_data_type, true))
    }

    fn map_glue_columns_to_arrow_schema(glue_columns: &Vec<Column>) -> error::Result<Schema> {
        let mut arrow_fields = Vec::new();
        for column in glue_columns {
            let arrow_field = Self::map_glue_column_to_arrow_field(column)?;
            arrow_fields.push(arrow_field);
        }
        Ok(Schema::new(arrow_fields))
    }


}

async fn list_all_files<'a>(
    listing_table_url: &'a ListingTableUrl,
    ctx: &'a SessionState,
    store: &'a dyn ObjectStore,
    file_extension: &'a str,
) -> Result<BoxStream<'a, Result<ObjectMeta>>> {
    let exec_options = &ctx.options().execution;
    let ignore_subdirectory = exec_options.listing_table_ignore_subdirectory;

    let list = match listing_table_url.is_collection() {
        true => store.list(Some(&listing_table_url.prefix())),
        false => futures::stream::once(store.head(listing_table_url.prefix())).boxed(),
    };

    // If the prefix is a file, use a head request, otherwise list
    // let list = 
    Ok(list
        .try_filter(move |meta| {
            let path = &meta.location;
            let extension_match = path.as_ref().ends_with(file_extension);
            let glob_match = listing_table_url.contains(path, ignore_subdirectory);
            futures::future::ready(extension_match && glob_match)
        })
        .map_err(DataFusionError::ObjectStore)
        .boxed())
}

pub async fn partition_file_list<'a>(
    ctx: &'a SessionState,
    part: &'a Option<&'a aws_sdk_glue::types::Partition>,
    store: &'a dyn ObjectStore,
    table_path: &'a ListingTableUrl,
    file_extension: &'a str,
) -> Result<BoxStream<'a, Result<PartitionedFile>>> {
    return Ok(Box::pin(
        list_all_files(table_path, ctx, store, file_extension)
            .await?
            .map_ok(move |object_meta| {
                let mut x: PartitionedFile = object_meta.into();
                if let Some(part) = part {
                    x.partition_values = part
                    .values()
                    .iter()
                    .map(|v| ScalarValue::new_utf8(v))
                    .collect();
                }
                x
            }),
    ));
}

fn with_trailing_slash(url: &str) -> String {
    format!("{}/", url.strip_suffix("/").unwrap_or(url))
}

#[async_trait]
impl TableProvider for GlueTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let table_url = self
            .table
            .storage_descriptor()
            .and_then(|sd| sd.location())
            .unwrap();
        let listing_table_url = ListingTableUrl::parse(table_url)?;
        let object_store_url = listing_table_url.object_store();
        let store = state.runtime_env().object_store(listing_table_url.clone())?;
        let mut partitions = vec![];

        let part_keys = self.table.partition_keys();

        let table_paths = if part_keys.len() > 0 {
            let mut builder = self.client.get_partitions();
            if let Some(expr) = self.get_glue_expr(filters) {
                log::info!("GLUE EXPR: {}", expr);
                builder = builder.expression(expr);
            }
            let mut resp = builder
                .database_name(self.table.database_name().unwrap())
                .table_name(self.table.name())
                .into_paginator()
                .send();
            while let Some(resp) = resp.next().await {
                if let Ok(resp) = resp {
                    partitions.extend(resp.partitions().iter().cloned());
                };
            }
            partitions
            .iter()
            .flat_map(|p| {
                p.storage_descriptor().and_then(|sd| {
                    sd.location().and_then(|loc| {
                        ListingTableUrl::parse(with_trailing_slash(loc))
                            .ok()
                            .map(|x| (Some(p), x))
                    })
                })
            })
            .collect::<Vec<_>>()
        } else {
            vec![(None, listing_table_url)]
        };

        let file_list = future::try_join_all(table_paths.iter().map(|(p, table_path)| {
            partition_file_list(
                state,
                p,
                store.as_ref(),
                table_path,
                &self.listing_options.file_extension,
            )
        }))
        .await?;

        let file_list = stream::iter(file_list).flatten();
        let mut file_list = file_list.map(|f| f).boxed();

        let mut partitioned_files = vec![];

        while let Some(p) = file_list.next().await {
            if let Ok(p) = p {
                partitioned_files.push(p);
            }
        }

        let partitioned_file_lists = vec![partitioned_files];



        // println!("partitioned_file_lists: {:?}", partitioned_file_lists);

        let statistics = Statistics::new_unknown(&self.schema);
        // extract types of partition columns
        let table_partition_cols = self
            .listing_options
            .table_partition_cols
            .iter()
            .cloned()
            .map(|col| Ok(Field::new(col.0, col.1, false)))
            .collect::<Result<Vec<_>>>()?;
        let output_ordering = vec![];

        let filters = if let Some(expr) = conjunction(filters.to_vec()) {
            // NOTE: Use the table schema (NOT file schema) here because `expr` may contain references to partition columns.
            let table_df_schema = self.schema.as_ref().clone().to_dfschema()?;
            let filters = create_physical_expr(&expr, &table_df_schema, state.execution_props())?;
            Some(filters)
        } else {
            None
        };

        let n_fields = self.schema().fields().len() - table_partition_cols.len();
        let schema = Schema::new(&self.schema().fields[..n_fields]);

        self.listing_options
            .format
            .create_physical_plan(
                state,
                FileScanConfig {
                    object_store_url,
                    file_schema: Arc::new(schema),
                    file_groups: partitioned_file_lists,
                    statistics,
                    projection: projection.cloned(),
                    limit,
                    output_ordering,
                    table_partition_cols,
                },
                filters.as_ref(),
            )
            .await

        // Ok(Arc::new(EmptyExec::new(self.schema())))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }
}

/// Execution plan for empty relation with produce_one_row=false
#[derive(Debug)]
pub struct EmptyExec {
    /// The schema for the produced row
    schema: SchemaRef,
    /// Number of partitions
    partitions: usize,
}

impl EmptyExec {
    /// Create a new EmptyExec
    pub fn new(schema: SchemaRef) -> Self {
        EmptyExec {
            schema,
            partitions: 1,
        }
    }

    /// Create a new EmptyExec with specified partition number
    pub fn with_partitions(mut self, partitions: usize) -> Self {
        self.partitions = partitions;
        self
    }

    fn data(&self) -> datafusion::error::Result<Vec<RecordBatch>> {
        Ok(vec![])
    }
}

impl DisplayAs for EmptyExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "EmptyExec")
            }
        }
    }
}

impl ExecutionPlan for EmptyExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(self.schema.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        log::trace!(
            "Start EmptyExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );

        if partition >= self.partitions {
            return internal_err!(
                "EmptyExec invalid partition {} (expected less than {})",
                partition,
                self.partitions
            );
        }

        Ok(Box::pin(MemoryStream::try_new(
            self.data()?,
            self.schema.clone(),
            None,
        )?))
    }

    fn statistics(&self) -> Result<Statistics> {
        let batch = self
            .data()
            .expect("Create empty RecordBatch should not fail");
        Ok(compute_record_batch_statistics(
            &[batch],
            &self.schema,
            None,
        ))
    }
}
