// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::catalog_provider::glue_data_type_parser::*;
use crate::error::{GlueError, Result};
use aws_sdk_glue::model::{Column, StorageDescriptor, Table};
use aws_sdk_glue::Client;
use aws_types::SdkConfig;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::catalog::CatalogProvider;
use datafusion::catalog::schema::{ObjectStoreSchemaProvider, SchemaProvider};
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableConfig};
use datafusion_objectstore_s3::object_store::s3::S3FileSystem;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// `CatalogProvider` implementation for the Amazon Glue API
pub struct GlueCatalogProvider {
    client: Client,
    s3_fs: Arc<S3FileSystem>,
    schema_provider_by_database: HashMap<String, Arc<ObjectStoreSchemaProvider>>,
}

impl GlueCatalogProvider {
    /// Convenience wrapper for creating a new `GlueCatalogProvider` using default configuration options.  Only works with AWS.
    pub async fn default() -> Self {
        let shared_config = aws_config::load_from_env().await;
        let s3_fs = Arc::new(S3FileSystem::default().await);
        GlueCatalogProvider::new(&shared_config, &s3_fs)
    }

    /// Create a new Glue CatalogProvider
    pub fn new(sdk_config: &SdkConfig, s3_fs: &Arc<S3FileSystem>) -> Self {
        let client = Client::new(sdk_config);
        let schema_provider_by_database = HashMap::new();
        GlueCatalogProvider {
            client,
            s3_fs: s3_fs.clone(),
            schema_provider_by_database,
        }
    }

    /// Register the table with the given database and table name
    pub async fn register_table(&mut self, database: &str, table: &str) -> Result<()> {
        let glue_table = self
            .client
            .get_table()
            .set_database_name(Some(database.to_string()))
            .set_name(Some(table.to_string()))
            .send()
            .await?
            .table
            .ok_or_else(|| GlueError::AWS(format!("Did not find table {}.{}", database, table)))?;

        self.register_glue_table(&glue_table).await
    }

    /// Register all tables in the given glue database
    pub async fn register_tables(&mut self, database: &str) -> Result<Vec<Result<()>>> {
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
            let result = self.register_glue_table(&glue_table).await;
            results.push(result);
        }

        Ok(results)
    }

    /// Register all tables in all glue databases
    pub async fn register_all(&mut self) -> Result<Vec<Result<()>>> {
        let glue_databases = self
            .client
            .get_databases()
            .send()
            .await?
            .database_list
            .ok_or_else(|| GlueError::AWS("Did not find database list".to_string()))?;

        let mut results = Vec::new();
        for glue_database in glue_databases {
            let database = glue_database
                .name
                .ok_or_else(|| GlueError::AWS("Failed to find name for database".to_string()))?;
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
                let result = self.register_glue_table(&glue_table).await;
                results.push(result);
            }
        }

        Ok(results)
    }

    async fn register_glue_table(&mut self, glue_table: &Table) -> Result<()> {
        let database_name = glue_table
            .database_name
            .as_ref()
            .ok_or_else(|| GlueError::AWS("Failed to find name for glue database".to_string()))?
            .clone();

        let schema_provider_for_database = self
            .schema_provider_by_database
            .entry(database_name.to_string())
            .or_insert_with(|| {
                let instance = ObjectStoreSchemaProvider::default();
                instance.register_object_store("s3", self.s3_fs.clone());
                Arc::new(instance)
            });

        let table_name = glue_table
            .name
            .as_deref()
            .ok_or_else(|| GlueError::AWS("Failed to find name for glue table".to_string()))?;
        let sd = glue_table.storage_descriptor.as_ref().ok_or_else(|| {
            GlueError::AWS("Failed to find storage descriptor for glue table".to_string())
        })?;
        let uri = sd.location.as_ref().ok_or_else(|| {
            GlueError::AWS("Failed to find uri in storage descriptor for glue table".to_string())
        })?;
        let (object_store, path) = schema_provider_for_database.object_store(uri)?;
        let mut ltc = ListingTableConfig::new(object_store, path);
        let schema =
            Self::map_glue_columns_to_arrow_schema(sd.columns.as_ref().ok_or_else(|| {
                GlueError::AWS(
                    "Failed to find columns in storage descriptor for glue table".to_string(),
                )
            })?)
            .map_err(|e| Self::wrap_error_with_table_info(&database_name, table_name, e))?;
        let schema_ref = SchemaRef::new(schema);
        ltc = ltc.with_schema(schema_ref);
        let listing_options = Self::calculate_options(sd)
            .map_err(|e| Self::wrap_error_with_table_info(&database_name, table_name, e))?;
        ltc = ltc.with_listing_options(listing_options);
        schema_provider_for_database
            .register_listing_table(table_name, uri, Some(ltc))
            .await?;
        Ok(())
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
            _ => Err(GlueError::NotImplemented(format!(
                "No support for: {}, {}, {:?} yet.",
                input_format, output_format, sd
            ))),
        };
        let format = format_result?;

        let listing_options = ListingOptions {
            file_extension: String::new(),
            format: Arc::from(format),
            table_partition_cols: vec![],
            collect_stat: true,
            target_partitions: 1,
        };

        Ok(listing_options)
    }

    /*
    https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-tables.html#aws-glue-api-catalog-tables-Column
    https://docs.aws.amazon.com/athena/latest/ug/data-types.html
     */

    fn map_glue_data_type(glue_data_type: &str) -> Result<DataType> {
        use pest::Parser;

        let mut pairs = GlueDataTypeParser::parse(Rule::DataType, glue_data_type).map_err(|e| {
            GlueError::GlueDataTypeMapping(format!(
                "Error while parsing {}: {:?}",
                glue_data_type, e
            ))
        })?;

        let pair = pairs.next().ok_or_else(|| {
            GlueError::GlueDataTypeMapping("Did not find actual type in DataType".to_string())
        })?;

        match pair.as_rule() {
            Rule::TinyInt => Ok(DataType::Int8),
            Rule::SmallInt => Ok(DataType::Int16),
            Rule::Int => Ok(DataType::Int32),
            Rule::Boolean => Ok(DataType::Boolean),
            Rule::BigInt => Ok(DataType::Int64),
            Rule::Float => Ok(DataType::Float32),
            Rule::Double => Ok(DataType::Float64),
            Rule::Binary => Ok(DataType::Binary),
            Rule::Timestamp => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            Rule::String => Ok(DataType::Utf8),
            Rule::Char => Ok(DataType::Utf8),
            Rule::Varchar => Ok(DataType::Utf8),
            Rule::Decimal => {
                let mut inner = pair.into_inner();
                let precision = inner
                    .next()
                    .ok_or_else(|| {
                        GlueError::GlueDataTypeMapping(format!(
                            "Did not find precision in {:?}",
                            glue_data_type
                        ))
                    })?
                    .as_str()
                    .parse()
                    .map_err(|_| {
                        GlueError::GlueDataTypeMapping(format!(
                            "Failed to parse precision as usize in {:?}",
                            glue_data_type
                        ))
                    })?;
                let scale = inner
                    .next()
                    .ok_or_else(|| {
                        GlueError::GlueDataTypeMapping(format!(
                            "Did not find scale in {:?}",
                            glue_data_type
                        ))
                    })?
                    .as_str()
                    .parse()
                    .map_err(|_| {
                        GlueError::GlueDataTypeMapping(format!(
                            "Failed to parse scale as usize in {:?}",
                            glue_data_type
                        ))
                    })?;
                Ok(DataType::Decimal(precision, scale))
            }
            Rule::ArrayType => {
                let array_glue_data_type = pair
                    .into_inner()
                    .next()
                    .ok_or_else(|| {
                        GlueError::GlueDataTypeMapping(format!(
                            "Did not find array type in {:?}",
                            glue_data_type
                        ))
                    })?
                    .as_str();
                let array_arrow_data_type = Self::map_glue_data_type(array_glue_data_type)?;
                Ok(DataType::List(Box::new(Field::new(
                    "id",
                    array_arrow_data_type,
                    true,
                ))))
            }
            Rule::MapType => {
                let mut inner = pair.into_inner();
                let key_glue_data_type = inner
                    .next()
                    .ok_or_else(|| {
                        GlueError::GlueDataTypeMapping(format!(
                            "Did not key data type in {:?}",
                            glue_data_type
                        ))
                    })?
                    .as_str();
                let value_glue_data_type = inner
                    .next()
                    .ok_or_else(|| {
                        GlueError::GlueDataTypeMapping(format!(
                            "Did not find value data type in {:?}",
                            glue_data_type
                        ))
                    })?
                    .as_str();

                let key_arrow_data_type = Self::map_glue_data_type(key_glue_data_type)?;
                let value_arrow_data_type = Self::map_glue_data_type(value_glue_data_type)?;

                Ok(DataType::Map(
                    Box::new(Field::new(
                        "entries",
                        DataType::Struct(vec![
                            Field::new("key", key_arrow_data_type, true),
                            Field::new("value", value_arrow_data_type, true),
                        ]),
                        true,
                    )),
                    true,
                ))
            }
            Rule::StructType => {
                let inner = pair.into_inner();
                let mut fields = Vec::new();
                for field in inner {
                    let mut struct_field_inner = field.into_inner();
                    let field_name = struct_field_inner
                        .next()
                        .ok_or_else(|| {
                            GlueError::GlueDataTypeMapping(format!(
                                "Did not find field name in {:?}",
                                glue_data_type
                            ))
                        })?
                        .as_str();
                    let field_glue_data_type = struct_field_inner
                        .next()
                        .ok_or_else(|| {
                            GlueError::GlueDataTypeMapping(format!(
                                "Did not find field data type in {:?}",
                                glue_data_type
                            ))
                        })?
                        .as_str();
                    let field_arrow_data_type = Self::map_glue_data_type(field_glue_data_type)?;
                    fields.push(Field::new(field_name, field_arrow_data_type, true));
                }
                Ok(DataType::Struct(fields))
            }
            _ => Err(GlueError::NotImplemented(format!(
                "No arrow type for glue_data_type: {}",
                &glue_data_type
            ))),
        }
    }

    fn map_glue_column_to_arrow_field(glue_column: &Column) -> Result<Field> {
        let name = glue_column
            .name
            .as_ref()
            .ok_or_else(|| GlueError::AWS("Failed to find name in glue column".to_string()))?
            .clone();
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
    use aws_sdk_glue::model::Column;
    use datafusion::arrow::datatypes::TimeUnit;

    #[test]
    fn test_map_tinyint_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueCatalogProvider::map_glue_column_to_arrow_field(
                &Column::builder().name("id").r#type("tinyint").build()
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
                &Column::builder().name("id").r#type("smallint").build()
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
                &Column::builder().name("id").r#type("int").build()
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
                &Column::builder().name("id").r#type("integer").build()
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
                &Column::builder().name("id").r#type("bigint").build()
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
                &Column::builder().name("id").r#type("float").build()
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
                &Column::builder().name("id").r#type("double").build()
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
                &Column::builder().name("id").r#type("boolean").build()
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
                &Column::builder().name("id").r#type("binary").build()
            )
            .unwrap(),
            Field::new("id", DataType::Binary, true)
        );
        Ok(())
    }

    #[test]
    fn test_map_timestamp_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueCatalogProvider::map_glue_column_to_arrow_field(
                &Column::builder().name("id").r#type("timestamp").build()
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
                &Column::builder().name("id").r#type("string").build()
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
                &Column::builder().name("id").r#type("char").build()
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
                &Column::builder().name("id").r#type("varchar").build()
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
                &Column::builder().name("id").r#type("decimal(12,9)").build()
            )
            .unwrap(),
            Field::new("id", DataType::Decimal(12, 9), true)
        );
        Ok(())
    }

    #[test]
    fn test_map_array_of_bigint_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueCatalogProvider::map_glue_column_to_arrow_field(
                &Column::builder().name("id").r#type("array<bigint>").build()
            )
            .unwrap(),
            Field::new(
                "id",
                DataType::List(Box::new(Field::new("id", DataType::Int64, true))),
                true
            )
        );
        Ok(())
    }

    #[test]
    fn test_map_array_of_int_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueCatalogProvider::map_glue_column_to_arrow_field(
                &Column::builder().name("id").r#type("array<int>").build()
            )
            .unwrap(),
            Field::new(
                "id",
                DataType::List(Box::new(Field::new("id", DataType::Int32, true))),
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
            )
            .unwrap(),
            Field::new(
                "id",
                DataType::List(Box::new(Field::new(
                    "id",
                    DataType::List(Box::new(Field::new("id", DataType::Utf8, true))),
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
            )
            .unwrap(),
            Field::new(
                "id",
                DataType::Struct(vec![
                    Field::new("reply_id", DataType::Int32, true),
                    Field::new("next_id", DataType::Int32, true),
                ]),
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
            )
            .unwrap(),
            Field::new(
                "id",
                DataType::Struct(vec![Field::new(
                    "reply",
                    DataType::Struct(vec![Field::new("reply_id", DataType::Int32, true),]),
                    true
                ),]),
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
            )
            .unwrap(),
            Field::new(
                "id",
                DataType::Map(
                    Box::new(Field::new(
                        "entries",
                        DataType::Struct(vec![
                            Field::new("key", DataType::Utf8, true),
                            Field::new("value", DataType::Boolean, true),
                        ]),
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
            )
            .unwrap(),
            Field::new(
                "id",
                DataType::Map(
                    Box::new(Field::new(
                        "entries",
                        DataType::Struct(vec![
                            Field::new("key", DataType::Utf8, true),
                            Field::new(
                                "value",
                                DataType::Map(
                                    Box::new(Field::new(
                                        "entries",
                                        DataType::Struct(vec![
                                            Field::new("key", DataType::Utf8, true),
                                            Field::new("value", DataType::Boolean, true),
                                        ]),
                                        true
                                    ),),
                                    true
                                ),
                                true
                            ),
                        ]),
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

        let list_of_string = DataType::List(Box::new(Field::new("id", DataType::Utf8, true)));

        // array type
        assert_eq!(
            GlueCatalogProvider::map_glue_data_type("array<string>").unwrap(),
            list_of_string
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_data_type("array<array<string>>").unwrap(),
            DataType::List(Box::new(Field::new("id", list_of_string.clone(), true)))
        );

        let map_of_string_and_boolean = DataType::Map(
            Box::new(Field::new(
                "entries",
                DataType::Struct(vec![
                    Field::new("key", DataType::Utf8, true),
                    Field::new("value", DataType::Boolean, true),
                ]),
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
                Box::new(Field::new(
                    "entries",
                    DataType::Struct(vec![
                        Field::new("key", map_of_string_and_boolean, true),
                        Field::new("value", list_of_string, true),
                    ]),
                    true
                )),
                true
            )
        );

        let struct_of_int = DataType::Struct(vec![Field::new("reply_id", DataType::Int32, true)]);

        // struct type
        assert_eq!(
            GlueCatalogProvider::map_glue_data_type("struct<reply_id:int>").unwrap(),
            struct_of_int
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_data_type("struct<reply:struct<reply_id:int>>").unwrap(),
            DataType::Struct(vec![Field::new("reply", struct_of_int, true)])
        );

        assert_eq!(
            GlueCatalogProvider::map_glue_data_type("decimal(12,9)").unwrap(),
            DataType::Decimal(12, 9)
        );

        Ok(())
    }
}
