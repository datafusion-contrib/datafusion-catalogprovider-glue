use datafusion::error::DataFusionError;
// SPDX-License-Identifier: Apache-2.0
use log;

use crate::catalog_provider::glue;
use crate::error::*;
use crate::glue_data_type_parser::*;
use async_trait::async_trait;
use aws_sdk_glue::types::{Column, StorageDescriptor, Table};
use aws_sdk_glue::Client;
use aws_types::SdkConfig;
use datafusion::datasource::{
    file_format::{
        avro::AvroFormat, csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat, FileFormat,
    },
    listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    TableProvider,
};
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    catalog::{schema::SchemaProvider, CatalogProvider},
    common::GetExt,
    execution::object_store::ObjectStoreRegistry,
};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Options to register a table
pub enum TableRegistrationOptions {
    /// Derive schema from Glue table
    DeriveSchemaFromGlueTable,
    /// Infer schema from data
    InferSchemaFromData,
}

/// `SchemaProvider` implementation for Amazon Glue API
#[allow(dead_code)]
pub struct GlueSchemaProvider {
    client: Client,
    tables: Vec<Table>,
    object_store_registry: Option<Arc<dyn ObjectStoreRegistry>>,
}

/// `CatalogProvider` implementation for the Amazon Glue API
pub struct GlueCatalogProvider {
    schema_provider_by_database: HashMap<String, Arc<dyn SchemaProvider>>,
}

/// Configuration
#[derive(Default)]
pub struct GlueCatalogConfig {
    databases: Vec<String>,
    object_store_registry: Option<Arc<dyn ObjectStoreRegistry>>,
    sdk_config: Option<SdkConfig>,
}

impl GlueCatalogConfig {
    /// Limit to specified databases
    pub fn with_databases(mut self, databases: &[&str]) -> Self {
        self.databases = databases.iter().map(|db| db.to_string()).collect();
        self
    }

    /// Provide sdk_config
    pub fn with_sdk_config(mut self, sdk_config: SdkConfig) -> Self {
        self.sdk_config = Some(sdk_config);
        self
    }

    /// Provide object store registry
    pub fn with_object_store_registry(
        mut self,
        object_store_registry: Arc<dyn ObjectStoreRegistry>,
    ) -> Self {
        self.object_store_registry = Some(object_store_registry);
        self
    }
}

impl GlueCatalogProvider {
    /// Convenience wrapper for creating a new `GlueCatalogProvider` using default configuration options.  Only works with AWS.
    pub async fn default() -> Result<Self> {
        GlueCatalogProvider::new(GlueCatalogConfig::default()).await
    }

    /// Create a new Glue CatalogProvider
    pub async fn new(config: GlueCatalogConfig) -> Result<Self> {
        let client = match &config.sdk_config {
            Some(sdk_config) => Client::new(sdk_config),
            None => {
                Client::new(&aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await)
            }
        };
        let mut schema_provider_by_database = HashMap::new();

        let glue_databases = client
            .get_databases()
            .send()
            .await?
            .database_list
            .iter()
            .filter(|db| config.databases.is_empty() || config.databases.contains(&db.name))
            .cloned()
            .collect::<Vec<_>>();

        for glue_database in glue_databases {
            let database = &glue_database.name;
            let tables = client
                .get_tables()
                .database_name(database)
                .send()
                .await?
                .table_list
                .ok_or_else(|| {
                    GlueError::AWS(format!("Did not find table list in database {}", database))
                })?;
            schema_provider_by_database.insert(
                database.to_string(),
                Arc::new(GlueSchemaProvider {
                    client: client.clone(),
                    tables,
                    object_store_registry: config.object_store_registry.clone(),
                }) as Arc<dyn SchemaProvider>,
            );
        }

        Ok(GlueCatalogProvider {
            schema_provider_by_database,
        })
    }
}

impl GlueSchemaProvider {
    async fn create_table(&self, glue_table: &Table) -> Result<Arc<dyn TableProvider>> {
        log::info!(
            "glue db: {:?}, table: {:?}",
            glue_table.database_name(),
            glue_table.name()
        );
        log::debug!("{:#?}", glue_table);
        // return Ok(());

        let database_name = Self::get_database_name(glue_table)?;
        let table_name = glue_table.name();

        let provider = glue_table
            .parameters()
            .and_then(|f| f.get("spark.sql.sources.provider"))
            .map(|s| s.as_str());

        if let Some("delta") = provider {
            #[cfg(feature = "deltalake")]
            return crate::catalog_provider::delta_table::create_delta_table(
                glue_table,
                &self.object_store_registry,
            )
            .await;
            #[cfg(not(feature = "deltalake"))]
            return Err(GlueError::DeltaLake(
                "DeltaLake support is not enabled".to_string(),
            ));
        }

        let sd = Self::get_storage_descriptor(glue_table)?;
        let listing_options = Self::get_listing_options(database_name, table_name, &sd)?;

        if let Some(partitions) = &glue_table.partition_keys {
            let builder = self.client.get_partitions();
            let resp = builder
                .database_name(glue_table.database_name().unwrap())
                .table_name(glue_table.name())
                .send()
                .await?;
            let partition_locations = resp
                .partitions()
                .iter()
                .flat_map(|p| p.storage_descriptor())
                .flat_map(|x| x.location())
                .collect::<Vec<_>>();

            let table_urls = partition_locations
                .iter()
                .map(|loc| {
                    let mut loc = loc.to_string();
                    if !loc.ends_with("/") && !loc.ends_with(&listing_options.file_extension) {
                        loc.push('/');
                    }
                    ListingTableUrl::parse(loc)
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let ltc = ListingTableConfig::new_with_multi_paths(table_urls);
            let ltc_with_lo = ltc.with_listing_options(listing_options);
            let schema = Self::derive_schema(database_name, table_name, &sd)?;

            let ltc_with_lo_and_schema = ltc_with_lo.with_schema(SchemaRef::new(schema));

            let listing_table = ListingTable::try_new(ltc_with_lo_and_schema)?;
            return Ok(Arc::new(listing_table));
            // if !partitions.is_empty() {
            //     return Err(GlueError::NotImplemented("partitioned tables are not implemented yet".into()))
            // }
        }

        let mut storage_location_uri = Self::get_storage_location(&sd)?.to_string();
        if !storage_location_uri.ends_with("/")
            && !storage_location_uri.ends_with(&listing_options.file_extension)
        {
            storage_location_uri.push('/');
        }

        let ltu = ListingTableUrl::parse(storage_location_uri)?;
        let ltc = ListingTableConfig::new(ltu);
        let ltc_with_lo = ltc.with_listing_options(listing_options);

        let schema = Self::derive_schema(database_name, table_name, &sd)?;

        let ltc_with_lo_and_schema = ltc_with_lo.with_schema(SchemaRef::new(schema));

        let listing_table = ListingTable::try_new(ltc_with_lo_and_schema)?;

        Ok(Arc::new(listing_table))
    }

    fn get_listing_options(
        database_name: &str,
        table_name: &str,
        sd: &StorageDescriptor,
    ) -> Result<ListingOptions> {
        Self::calculate_options(sd)
            .map_err(|e| Self::wrap_error_with_table_info(database_name, table_name, e))
    }

    fn derive_schema(
        database_name: &str,
        table_name: &str,
        sd: &StorageDescriptor,
    ) -> Result<Schema> {
        let columns = Self::get_columns(sd)?;
        Self::map_glue_columns_to_arrow_schema(columns)
            .map_err(|e| Self::wrap_error_with_table_info(database_name, table_name, e))
    }

    fn get_columns(sd: &StorageDescriptor) -> Result<&Vec<Column>> {
        sd.columns.as_ref().ok_or_else(|| {
            GlueError::AWS(
                "Failed to find columns in storage descriptor for glue table".to_string(),
            )
        })
    }

    fn get_storage_location(sd: &StorageDescriptor) -> Result<&str> {
        sd.location.as_deref().ok_or_else(|| {
            GlueError::AWS("Failed to find uri in storage descriptor for glue table".to_string())
        })
    }

    fn get_storage_descriptor(glue_table: &Table) -> Result<StorageDescriptor> {
        glue_table.storage_descriptor.clone().ok_or_else(|| {
            GlueError::AWS("Failed to find storage descriptor for glue table".to_string())
        })
    }

    fn get_database_name(glue_table: &Table) -> Result<&str> {
        glue_table
            .database_name
            .as_deref()
            .ok_or_else(|| GlueError::AWS("Failed to find name for glue database".to_string()))
    }

    fn wrap_error_with_table_info(
        database_name: &str,
        table_name: &str,
        e: GlueError,
    ) -> GlueError {
        match e {
            GlueError::NotImplemented(msg) => {
                GlueError::NotImplemented(format!("{}.{}: {}", database_name, table_name, msg))
            }
            _ => e,
        }
    }

    fn calculate_options(sd: &StorageDescriptor) -> Result<ListingOptions> {
        let empty_str = String::from("");
        let input_format = sd.input_format.as_ref().unwrap_or(&empty_str);
        let output_format = sd.output_format.as_ref().unwrap_or(&empty_str);
        let serde_info = sd.serde_info.as_ref().ok_or_else(|| {
            GlueError::AWS(
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
                GlueError::AWS(
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
        let format_result: Result<Box<dyn FileFormat>> = match item {
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
                        GlueError::AWS(
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
            _ => Err(GlueError::NotImplemented(format!(
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

    fn map_glue_data_type_to_arrow_data_type(glue_data_type: &GlueDataType) -> Result<DataType> {
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

    fn map_glue_data_type(glue_data_type: &str) -> Result<DataType> {
        let parsed_glue_data_type = parse_glue_data_type(glue_data_type).map_err(|e| {
            GlueError::GlueDataTypeMapping(format!(
                "Error while parsing {}: {:?}",
                glue_data_type, e
            ))
        })?;

        Self::map_glue_data_type_to_arrow_data_type(&parsed_glue_data_type)
    }

    fn map_glue_column_to_arrow_field(glue_column: &Column) -> Result<Field> {
        let name = glue_column.name();
        let glue_type = glue_column
            .r#type
            .as_ref()
            .ok_or_else(|| GlueError::AWS("Failed to find type in glue column".to_string()))?
            .clone();
        Self::map_to_arrow_field(&name, &glue_type)
    }

    fn map_to_arrow_field(glue_name: &str, glue_type: &str) -> Result<Field> {
        let arrow_data_type = Self::map_glue_data_type(glue_type)?;
        Ok(Field::new(glue_name, arrow_data_type, true))
    }

    fn map_glue_columns_to_arrow_schema(glue_columns: &Vec<Column>) -> Result<Schema> {
        let mut arrow_fields = Vec::new();
        for column in glue_columns {
            let arrow_field = Self::map_glue_column_to_arrow_field(column)?;
            arrow_fields.push(arrow_field);
        }
        Ok(Schema::new(arrow_fields))
    }
}

impl CatalogProvider for GlueCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schema_provider_by_database.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schema_provider_by_database.get(name).cloned()
    }

    fn register_schema(
        &self,
        _: &str,
        _: Arc<dyn SchemaProvider>,
    ) -> datafusion::error::Result<Option<Arc<dyn SchemaProvider>>> {
        unimplemented!()
    }
}

#[async_trait]
impl SchemaProvider for GlueSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.iter().map(|t| t.name().to_string()).collect()
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn datafusion::datasource::TableProvider>> {
        let table = self.tables.iter().find(|t| t.name() == name)?;
        self.create_table(table)
            .await
            .map_err(|err| {
                log::warn!("{:?}", err);
                err
            })
            .ok()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.iter().find(|t| t.name() == name).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_glue::types::Column;
    use datafusion::arrow::datatypes::TimeUnit;

    #[test]
    fn test_map_tinyint_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("tinyint")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new("id", DataType::Int8, true)
        );
        Ok(())
    }

    #[test]
    fn test_map_smallint_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("smallint")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new("id", DataType::Int16, true)
        );
        Ok(())
    }

    #[test]
    fn test_map_int_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder().name("id").r#type("int").build().unwrap()
            )
            .unwrap(),
            Field::new("id", DataType::Int32, true)
        );
        Ok(())
    }

    #[test]
    fn test_map_integer_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("integer")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new("id", DataType::Int32, true)
        );
        Ok(())
    }

    #[test]
    fn test_map_bigint_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("bigint")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new("id", DataType::Int64, true)
        );
        Ok(())
    }

    #[test]
    fn test_map_float_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("float")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new("id", DataType::Float32, true)
        );
        Ok(())
    }

    #[test]
    fn test_map_double_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("double")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new("id", DataType::Float64, true)
        );
        Ok(())
    }

    #[test]
    fn test_map_boolean_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("boolean")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new("id", DataType::Boolean, true)
        );
        Ok(())
    }

    #[test]
    fn test_map_binary_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("binary")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new("id", DataType::Binary, true)
        );
        Ok(())
    }

    #[test]
    fn test_map_date_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder().name("id").r#type("date").build().unwrap()
            )
            .unwrap(),
            Field::new("id", DataType::Date32, true)
        );
        Ok(())
    }

    #[test]
    fn test_map_timestamp_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("timestamp")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new("id", DataType::Timestamp(TimeUnit::Nanosecond, None), true)
        );
        Ok(())
    }

    #[test]
    fn test_map_string_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("string")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new("id", DataType::Utf8, true)
        );
        Ok(())
    }

    #[test]
    fn test_map_char_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder().name("id").r#type("char").build().unwrap()
            )
            .unwrap(),
            Field::new("id", DataType::Utf8, true)
        );
        Ok(())
    }

    #[test]
    fn test_map_varchar_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("varchar")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new("id", DataType::Utf8, true)
        );
        Ok(())
    }

    #[test]
    fn test_map_decimal_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("decimal(12,9)")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new("id", DataType::Decimal256(12, 9), true)
        );
        Ok(())
    }

    #[test]
    fn test_map_array_of_bigint_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("array<bigint>")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new(
                "id",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true
            )
        );
        Ok(())
    }

    #[test]
    fn test_map_array_of_int_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("array<int>")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new(
                "id",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true
            )
        );
        Ok(())
    }

    #[test]
    fn test_map_array_of_array_of_string_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("array<array<string>>")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new(
                "id",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true
                ),)),
                true
            )
        );
        Ok(())
    }

    #[test]
    fn test_map_struct_of_int_and_int_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("struct<reply_id:int,next_id:int>")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new(
                "id",
                DataType::Struct(
                    vec![
                        Field::new("reply_id", DataType::Int32, true),
                        Field::new("next_id", DataType::Int32, true),
                    ]
                    .into()
                ),
                true
            )
        );
        Ok(())
    }

    //struct<reply:struct<reply_id:int>>
    //array<struct<reply:struct<reply_id:int,next_id:int>,blog_id:bigint>>

    #[test]
    fn test_map_struct_of_struct_of_int_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("struct<reply:struct<reply_id:int>>")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new(
                "id",
                DataType::Struct(
                    vec![Field::new(
                        "reply",
                        DataType::Struct(
                            vec![Field::new("reply_id", DataType::Int32, true),].into()
                        ),
                        true
                    ),]
                    .into()
                ),
                true
            )
        );
        Ok(())
    }

    #[test]
    fn test_map_map_of_string_and_boolean_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("map<string,boolean>")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new(
                "id",
                DataType::Map(
                    Arc::new(Field::new(
                        "key_value",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, true),
                                Field::new("value", DataType::Boolean, true),
                            ]
                            .into()
                        ),
                        true
                    ),),
                    true
                ),
                true
            )
        );
        Ok(())
    }

    #[test]
    fn test_map_map_of_string_and_map_of_string_and_boolean_glue_column_to_arrow_field(
    ) -> Result<()> {
        assert_eq!(
            GlueSchemaProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("map<string,map<string,boolean>>")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new(
                "id",
                DataType::Map(
                    Arc::new(Field::new(
                        "key_value",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, true),
                                Field::new(
                                    "value",
                                    DataType::Map(
                                        Arc::new(Field::new(
                                            "key_value",
                                            DataType::Struct(
                                                vec![
                                                    Field::new("key", DataType::Utf8, true),
                                                    Field::new("value", DataType::Boolean, true),
                                                ]
                                                .into()
                                            ),
                                            true
                                        ),),
                                        true
                                    ),
                                    true
                                ),
                            ]
                            .into()
                        ),
                        true
                    ),),
                    true
                ),
                true
            )
        );

        Ok(())
    }

    #[test]
    fn test_map_glue_data_type() -> Result<()> {
        // simple types
        assert_eq!(
            GlueSchemaProvider::map_glue_data_type("int").unwrap(),
            DataType::Int32
        );
        assert_eq!(
            GlueSchemaProvider::map_glue_data_type("boolean").unwrap(),
            DataType::Boolean
        );
        assert_eq!(
            GlueSchemaProvider::map_glue_data_type("bigint").unwrap(),
            DataType::Int64
        );
        assert_eq!(
            GlueSchemaProvider::map_glue_data_type("float").unwrap(),
            DataType::Float32
        );
        assert_eq!(
            GlueSchemaProvider::map_glue_data_type("double").unwrap(),
            DataType::Float64
        );
        assert_eq!(
            GlueSchemaProvider::map_glue_data_type("binary").unwrap(),
            DataType::Binary
        );
        assert_eq!(
            GlueSchemaProvider::map_glue_data_type("timestamp").unwrap(),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            GlueSchemaProvider::map_glue_data_type("string").unwrap(),
            DataType::Utf8
        );

        let list_of_string = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));

        // array type
        assert_eq!(
            GlueSchemaProvider::map_glue_data_type("array<string>").unwrap(),
            list_of_string
        );
        assert_eq!(
            GlueSchemaProvider::map_glue_data_type("array<array<string>>").unwrap(),
            DataType::List(Arc::new(Field::new("item", list_of_string.clone(), true)))
        );

        let map_of_string_and_boolean = DataType::Map(
            Arc::new(Field::new(
                "key_value",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, true),
                        Field::new("value", DataType::Boolean, true),
                    ]
                    .into(),
                ),
                true,
            )),
            true,
        );

        // map type
        assert_eq!(
            GlueSchemaProvider::map_glue_data_type("map<string,boolean>").unwrap(),
            map_of_string_and_boolean
        );
        assert_eq!(
            GlueSchemaProvider::map_glue_data_type("map<map<string,boolean>,array<string>>")
                .unwrap(),
            DataType::Map(
                Arc::new(Field::new(
                    "key_value",
                    DataType::Struct(
                        vec![
                            Field::new("key", map_of_string_and_boolean, true),
                            Field::new("value", list_of_string, true),
                        ]
                        .into()
                    ),
                    true
                )),
                true
            )
        );

        let struct_of_int =
            DataType::Struct(vec![Field::new("reply_id", DataType::Int32, true)].into());

        // struct type
        assert_eq!(
            GlueSchemaProvider::map_glue_data_type("struct<reply_id:int>").unwrap(),
            struct_of_int
        );
        assert_eq!(
            GlueSchemaProvider::map_glue_data_type("struct<reply:struct<reply_id:int>>").unwrap(),
            DataType::Struct(vec![Field::new("reply", struct_of_int, true)].into())
        );

        assert_eq!(
            GlueSchemaProvider::map_glue_data_type("decimal(12,9)").unwrap(),
            DataType::Decimal256(12, 9)
        );

        Ok(())
    }
}
