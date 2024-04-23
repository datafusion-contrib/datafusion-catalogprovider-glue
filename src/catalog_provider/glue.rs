// SPDX-License-Identifier: Apache-2.0

use crate::error::*;
use crate::glue_data_type_parser::*;
use aws_config::BehaviorVersion;
use aws_sdk_glue::types::{Column, StorageDescriptor, Table};
use aws_sdk_glue::Client;
use aws_types::SdkConfig;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::schema::{MemorySchemaProvider, SchemaProvider};
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use std::any::Any;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use datafusion::datasource::object_store::ObjectStoreRegistry;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::error::Error::TableMetadataBuilder;
use iceberg_rust::spec::table_metadata::TableMetadata;
use iceberg_rust::spec::tabular::TabularMetadata;
use iceberg_rust::table::table_builder::TableBuilder;
use iceberg_rust::util;
use iceberg_rust::util::strip_prefix;
use object_store::ObjectStore;
use object_store::path::Path;

/// Options to register a table
pub enum TableRegistrationOptions {
    /// Derive schema from Glue table
    DeriveSchemaFromGlueTable,
    /// Infer schema from data
    InferSchemaFromData,
}

/// `CatalogProvider` implementation for the Amazon Glue API
pub struct GlueCatalogProvider {
    client: Client,
    schema_provider_by_database: HashMap<String, Arc<MemorySchemaProvider>>,
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
}

impl GlueCatalogProvider {

    /// Create a new Glue CatalogProvider
    pub fn new(sdk_config: &SdkConfig, object_store_registry: Arc<dyn ObjectStoreRegistry>) -> Self {
        let client = Client::new(sdk_config);
        let schema_provider_by_database = HashMap::new();
        GlueCatalogProvider {
            client,
            schema_provider_by_database,
            object_store_registry,
        }
    }

    /// Register the table with the given database and table name
    pub async fn register_table(
        &mut self,
        database: &str,
        table: &str,
        ctx: &SessionState,
    ) -> Result<()> {
        self.register_table_with_options(
            database,
            table,
            &TableRegistrationOptions::DeriveSchemaFromGlueTable,
            ctx,
        )
        .await
    }

    /// Register the table with the given database name, table name and options
    pub async fn register_table_with_options(
        &mut self,
        database: &str,
        table: &str,
        table_registration_options: &TableRegistrationOptions,
        ctx: &SessionState,
    ) -> Result<()> {
        let glue_table = self
            .client
            .get_table()
            .set_database_name(Some(database.to_string()))
            .set_name(Some(table.to_string()))
            .send()
            .await?
            .table
            .ok_or_else(|| GlueError::AWS(format!("Did not find table {}.{}", database, table)))?;

        self.register_glue_table(&glue_table, table_registration_options, ctx)
            .await
    }

    /// Register all tables in the given glue database
    pub async fn register_tables(
        &mut self,
        database: &str,
        ctx: &SessionState,
    ) -> Result<Vec<Result<()>>> {
        self.register_tables_with_options(
            database,
            &TableRegistrationOptions::DeriveSchemaFromGlueTable,
            ctx,
        )
        .await
    }

    /// Register all tables in the given glue database
    pub async fn register_tables_with_options(
        &mut self,
        database: &str,
        table_registration_options: &TableRegistrationOptions,
        ctx: &SessionState,
    ) -> Result<Vec<Result<()>>> {
        let glue_tables = self
            .client
            .get_tables()
            .set_database_name(Some(database.to_string()))
            .send()
            .await?
            .table_list
            .ok_or_else(|| {
                GlueError::AWS(format!("Did not find table list in database {}", database))
            })?;

        let mut results = Vec::new();
        for glue_table in glue_tables {
            let result = self
                .register_glue_table(&glue_table, table_registration_options, ctx)
                .await;
            results.push(result);
        }

        Ok(results)
    }

    /// Register all tables in all glue databases
    pub async fn register_all(&mut self, ctx: &SessionState) -> Result<Vec<Result<()>>> {
        self.register_all_with_options(&TableRegistrationOptions::DeriveSchemaFromGlueTable, ctx)
            .await
    }

    /// Register all tables in all glue databases
    pub async fn register_all_with_options(
        &mut self,
        table_registration_options: &TableRegistrationOptions,
        ctx: &SessionState,
    ) -> Result<Vec<Result<()>>> {
        let glue_databases = self.client.get_databases().send().await?.database_list;

        let mut results = Vec::new();
        for glue_database in glue_databases {
            let database = glue_database.name;
            let glue_tables = self
                .client
                .get_tables()
                .database_name(&database)
                .send()
                .await?
                .table_list
                .ok_or_else(|| {
                    GlueError::AWS(format!("Did not find table list in database {}", database))
                })?;

            for glue_table in glue_tables {
                let result = self
                    .register_glue_table(&glue_table, table_registration_options, ctx)
                    .await;
                results.push(result);
            }
        }

        Ok(results)
    }

    async fn register_glue_table(
        &mut self,
        glue_table: &Table,
        table_registration_options: &TableRegistrationOptions,
        ctx: &SessionState,
    ) -> Result<()> {
        let database_name = Self::get_database_name(glue_table)?;
        let table_name = &glue_table.name;

        let sd = Self::get_storage_descriptor(glue_table)?;
        let storage_location_uri = Self::get_storage_location(&sd)?;

        let table_parameters = match &glue_table.parameters {
            Some(x) => x.clone(),
            None => HashMap::new(),
        };

        let table_type = table_parameters
            .get("table_type")
            .map(|x| x.to_lowercase())
            .unwrap_or("".to_string());
        let table = if table_type == "delta" {
            //self.register_delta_table(database_name, table_name, storage_location_uri)
            //    .await?;
            todo!();
        } else if table_type == "iceberg" {
            self.register_iceberg_table(
                glue_table,
                table_registration_options,
                ctx,
                database_name,
                table_name,
                &sd,
                storage_location_uri,
                table_parameters,
            )
                .await?
        } else {
            self.register_listing_table(
                glue_table,
                table_registration_options,
                ctx,
                database_name,
                table_name,
                &sd,
                storage_location_uri,
            )
            .await?
        };

        let schema_provider_for_database = self.ensure_schema_provider_for_database(database_name);
        schema_provider_for_database.register_table(table_name.to_string(), table)?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn register_iceberg_table(
        &mut self,
        glue_table: &Table,
        table_registration_options: &TableRegistrationOptions,
        ctx: &SessionState,
        database_name: &str,
        table_name: &str,
        sd: &StorageDescriptor,
        storage_location_uri: &str,
        table_parameters: HashMap<String, String>,
    ) -> Result<Arc<dyn TableProvider>> {

        let url = url::Url::parse(storage_location_uri).map_err(|_| {
            GlueError::Other(format!("Failed to parse {storage_location_uri} as url"))
        })?;

        let object_store = self.object_store_registry.get_store(&url)?;

        let metadata_location = table_parameters.get("metadata_location").ok_or(GlueError::AWS(format!("Did not find metadata_location property in glue catalog")))?;
        let path = Path::parse(&strip_prefix(metadata_location)).map_err(|_| GlueError::Other(format!("Failed to parse {} as path", metadata_location)))?;

        let bytes = object_store.get(&path)
            .await.map_err(|e| GlueError::Other(format!("Failed to fetch {e:?} at {path}")))?
            .bytes()
            .await.map_err(|e|GlueError::Other(format!("Failed to get bytes from {e:?}  at {path}")))?;
        //println!("bytes are as following: {bytes:?}");

        let metadata: TableMetadata = serde_json::from_slice(&bytes)
            .map_err(|e|GlueError::Other(format!("Failed to read metadata from {e:?}  at {path}")))?;


        println!("metadata: {metadata:?}");


        //let catalog: Arc<dyn Catalog> = todo!();

        //TableMetadataBuilder::
        //let x = TableBuilder::new()

        /*
        let listing_options = Self::get_listing_options(database_name, table_name, sd, glue_table)?;

        let ltu = ListingTableUrl::parse(storage_location_uri)?;
        let ltc = ListingTableConfig::new(ltu);
        let ltc_with_lo = ltc.with_listing_options(listing_options);

        let ltc_with_lo_and_schema = match table_registration_options {
            TableRegistrationOptions::DeriveSchemaFromGlueTable => {
                let schema = Self::derive_schema(database_name, table_name, sd)?;
                ltc_with_lo.with_schema(SchemaRef::new(schema))
            }
            TableRegistrationOptions::InferSchemaFromData => ltc_with_lo.infer_schema(ctx).await?,
        };

        let listing_table = ListingTable::try_new(ltc_with_lo_and_schema)?;
        let result = Arc::new(listing_table);
        Ok(result)*/
        todo!();
    }

    #[allow(clippy::too_many_arguments)]
    async fn register_listing_table(
        &mut self,
        glue_table: &Table,
        table_registration_options: &TableRegistrationOptions,
        ctx: &SessionState,
        database_name: &str,
        table_name: &str,
        sd: &StorageDescriptor,
        storage_location_uri: &str,
    ) -> Result<Arc<dyn TableProvider>> {
        let listing_options = Self::get_listing_options(database_name, table_name, sd, glue_table)?;

        let ltu = ListingTableUrl::parse(storage_location_uri)?;
        let ltc = ListingTableConfig::new(ltu);
        let ltc_with_lo = ltc.with_listing_options(listing_options);

        let ltc_with_lo_and_schema = match table_registration_options {
            TableRegistrationOptions::DeriveSchemaFromGlueTable => {
                let schema = Self::derive_schema(database_name, table_name, sd)?;
                ltc_with_lo.with_schema(SchemaRef::new(schema))
            }
            TableRegistrationOptions::InferSchemaFromData => ltc_with_lo.infer_schema(ctx).await?,
        };

        let listing_table = ListingTable::try_new(ltc_with_lo_and_schema)?;
        let result = Arc::new(listing_table);
        Ok(result)
    }

    fn get_listing_options(
        database_name: &str,
        table_name: &str,
        sd: &StorageDescriptor,
        glue_table: &Table,
    ) -> Result<ListingOptions> {
        Self::calculate_options(glue_table, sd)
            .map_err(|e| Self::wrap_error_with_table_info(database_name, table_name, e))
    }

    fn ensure_schema_provider_for_database(
        &mut self,
        database_name: &str,
    ) -> &mut Arc<MemorySchemaProvider> {
        self.schema_provider_by_database
            .entry(database_name.to_string())
            .or_insert_with(|| {
                let instance = MemorySchemaProvider::new();
                Arc::new(instance)
            })
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

    fn calculate_options(glue_table: &Table, sd: &StorageDescriptor) -> Result<ListingOptions> {
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
            ) => Ok(Box::<ParquetFormat>::default()),
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
            ) => Ok(Box::<AvroFormat>::default()),
            (
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "org.apache.hive.hcatalog.data.JsonSerDe",
            ) => Ok(Box::<JsonFormat>::default()),
            (
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "org.openx.data.jsonserde.JsonSerDe",
            ) => Ok(Box::<JsonFormat>::default()),
            (
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "com.amazon.ionhiveserde.IonHiveSerDe",
            ) => Ok(Box::<JsonFormat>::default()),
            _ => Err(GlueError::NotImplemented(format!(
                "No support for: {}, {}, {:?} yet.",
                input_format, output_format, sd
            ))),
        };
        let format = format_result?;

        let partition_cols = glue_table
            .partition_keys()
            .iter()
            .map(|c| {
                (
                    c.name().to_owned(),
                    Self::map_glue_data_type(&c.r#type.clone().unwrap()).unwrap(),
                )
            })
            .collect::<Vec<(String, DataType)>>();

        let listing_options = ListingOptions {
            file_extension: String::new(),
            format: Arc::from(format),
            table_partition_cols: partition_cols,
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
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", key_arrow_data_type, true),
                            Field::new("value", value_arrow_data_type, true),
                        ])),
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
                Ok(DataType::Struct(Fields::from(fields)))
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
        let name = glue_column.name.clone();
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
        self.schema_provider_by_database
            .get(name)
            .cloned()
            .map(|x| x as Arc<dyn SchemaProvider>)
    }

    fn register_schema(
        &self,
        _: &str,
        _: Arc<dyn SchemaProvider>,
    ) -> datafusion::error::Result<Option<Arc<dyn SchemaProvider>>> {
        unimplemented!()
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("struct<reply_id:int,next_id:int>")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new(
                "id",
                DataType::Struct(Fields::from(vec![
                    Field::new("reply_id", DataType::Int32, true),
                    Field::new("next_id", DataType::Int32, true),
                ])),
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
                &Column::builder()
                    .name("id")
                    .r#type("struct<reply:struct<reply_id:int>>")
                    .build()
                    .unwrap()
            )
            .unwrap(),
            Field::new(
                "id",
                DataType::Struct(Fields::from(vec![Field::new(
                    "reply",
                    DataType::Struct(Fields::from(vec![Field::new(
                        "reply_id",
                        DataType::Int32,
                        true
                    ),])),
                    true
                ),])),
                true
            )
        );
        Ok(())
    }

    #[test]
    fn test_map_map_of_string_and_boolean_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, true),
                            Field::new("value", DataType::Boolean, true),
                        ])),
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
            GlueCatalogProvider::map_glue_column_to_arrow_field(
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
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, true),
                            Field::new(
                                "value",
                                DataType::Map(
                                    Arc::new(Field::new(
                                        "key_value",
                                        DataType::Struct(Fields::from(vec![
                                            Field::new("key", DataType::Utf8, true),
                                            Field::new("value", DataType::Boolean, true),
                                        ])),
                                        true
                                    ),),
                                    true
                                ),
                                true
                            ),
                        ])),
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
            GlueCatalogProvider::map_glue_data_type("int").unwrap(),
            DataType::Int32
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_data_type("boolean").unwrap(),
            DataType::Boolean
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_data_type("bigint").unwrap(),
            DataType::Int64
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_data_type("float").unwrap(),
            DataType::Float32
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_data_type("double").unwrap(),
            DataType::Float64
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_data_type("binary").unwrap(),
            DataType::Binary
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_data_type("timestamp").unwrap(),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_data_type("string").unwrap(),
            DataType::Utf8
        );

        let list_of_string = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));

        // array type
        assert_eq!(
            GlueCatalogProvider::map_glue_data_type("array<string>").unwrap(),
            list_of_string
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_data_type("array<array<string>>").unwrap(),
            DataType::List(Arc::new(Field::new("item", list_of_string.clone(), true)))
        );

        let map_of_string_and_boolean = DataType::Map(
            Arc::new(Field::new(
                "key_value",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, true),
                    Field::new("value", DataType::Boolean, true),
                ])),
                true,
            )),
            true,
        );

        // map type
        assert_eq!(
            GlueCatalogProvider::map_glue_data_type("map<string,boolean>").unwrap(),
            map_of_string_and_boolean
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_data_type("map<map<string,boolean>,array<string>>")
                .unwrap(),
            DataType::Map(
                Arc::new(Field::new(
                    "key_value",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", map_of_string_and_boolean, true),
                        Field::new("value", list_of_string, true),
                    ])),
                    true
                )),
                true
            )
        );

        let struct_of_int = DataType::Struct(Fields::from(vec![Field::new(
            "reply_id",
            DataType::Int32,
            true,
        )]));

        // struct type
        assert_eq!(
            GlueCatalogProvider::map_glue_data_type("struct<reply_id:int>").unwrap(),
            struct_of_int
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_data_type("struct<reply:struct<reply_id:int>>").unwrap(),
            DataType::Struct(Fields::from(vec![Field::new("reply", struct_of_int, true)]))
        );

        assert_eq!(
            GlueCatalogProvider::map_glue_data_type("decimal(12,9)").unwrap(),
            DataType::Decimal256(12, 9)
        );

        Ok(())
    }
}
