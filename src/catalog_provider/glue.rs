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

use crate::error::{GlueError, Result};
use aws_sdk_glue::model::{Column, StorageDescriptor, Table};
use aws_sdk_glue::Client;
use aws_types::SdkConfig;
use datafusion::arrow::datatypes::TimeUnit::Nanosecond;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::catalog::CatalogProvider;
use datafusion::catalog::schema::{ObjectStoreSchemaProvider, SchemaProvider};
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableConfig};
use datafusion_objectstore_s3::object_store::s3::S3FileSystem;
use lazy_static::lazy_static;
use regex::Regex;
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
            .unwrap();

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
            .unwrap();

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
            .unwrap();

        let mut results = Vec::new();
        for glue_database in glue_databases {
            let glue_tables = self
                .client
                .get_tables()
                .database_name(glue_database.name.unwrap())
                .send()
                .await?
                .table_list
                .unwrap();

            for glue_table in glue_tables {
                let result = self.register_glue_table(&glue_table).await;
                results.push(result);
            }
        }

        Ok(results)
    }

    async fn register_glue_table(&mut self, glue_table: &Table) -> Result<()> {
        let database_name = glue_table.database_name.as_ref().unwrap().clone();

        //let mut schema_provider_by_database = self.schema_provider_by_database;

        let schema_provider_for_database = self
            .schema_provider_by_database
            .entry(database_name.to_string())
            .or_insert_with(|| {
                let instance = ObjectStoreSchemaProvider::default();
                instance.register_object_store("s3", self.s3_fs.clone());
                Arc::new(instance)
            });

        let table_name = glue_table.name.as_deref().unwrap();
        let sd = glue_table.storage_descriptor.as_ref().unwrap();
        let uri = sd.location.as_ref().unwrap();
        let (object_store, path) = schema_provider_for_database.object_store(uri)?;
        let mut ltc = ListingTableConfig::new(object_store, path);
        let schema = Self::map_glue_columns_to_arrow_schema(sd.columns.as_ref().unwrap())
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
        //println!("need to calculate options for {:?}", sd);

        let empty_str = String::from("");
        let input_format = sd.input_format.as_ref().unwrap_or(&empty_str);
        let output_format = sd.output_format.as_ref().unwrap_or(&empty_str);
        let serde_info = sd.serde_info.as_ref().unwrap();
        let serialization_library = serde_info.serialization_library().unwrap_or_default();
        let serde_info_parameters = serde_info.parameters.as_ref().unwrap().clone();
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
                let delim = serde_info_parameters.get("field.delim").unwrap().as_bytes();
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
     */
    fn map_glue_type_to_arrow_data_type(glue_name: &str, glue_type: &str) -> Result<DataType> {
        lazy_static! {
            static ref DECIMAL_RE: Regex =
                Regex::new("^decimal\\((?P<precision>\\d+)\\s*,\\s*(?P<scale>\\d+)\\)$").unwrap();
            static ref ARRAY_RE: Regex = Regex::new("^array<(?P<array_type>.+)>$").unwrap();
            static ref STRUCT_RE: Regex = Regex::new("^struct<(?P<field_list>.+)>$").unwrap();
            static ref MAP_RE: Regex =
                Regex::new("^map<\\s*(?P<key_type>[^\\s,]+)\\s*,\\s*(?P<value_type>[^\\s]+)>$")
                    .unwrap();
        }

        match glue_type {
            "int" => Ok(DataType::Int32),
            "boolean" => Ok(DataType::Boolean),
            "bigint" => Ok(DataType::Int64),
            "float" => Ok(DataType::Float32),
            "double" => Ok(DataType::Float64),
            "binary" => Ok(DataType::Binary),
            "timestamp" => Ok(DataType::Timestamp(Nanosecond, None)),
            "string" => Ok(DataType::Utf8),
            _ => {
                if let Some(decimal_cg) = DECIMAL_RE.captures(glue_type) {
                    let precision = decimal_cg
                        .name("precision")
                        .unwrap()
                        .as_str()
                        .parse()
                        .unwrap();
                    let scale = decimal_cg.name("scale").unwrap().as_str().parse().unwrap();
                    Ok(DataType::Decimal(precision, scale))
                } else if let Some(array_cg) = ARRAY_RE.captures(glue_type) {
                    let array_type = array_cg.name("array_type").unwrap().as_str();
                    //println!("array type: {}", array_type);
                    let field = Self::map_to_arrow_field(glue_name, array_type)?;
                    Ok(DataType::List(Box::new(field)))
                } else if let Some(struct_cg) = STRUCT_RE.captures(glue_type) {
                    let field_list = struct_cg.name("field_list").unwrap().as_str();
                    //println!("field list: {}", field_list);
                    let mut fields = Vec::new();
                    let pairs = field_list.split(',');
                    for pair in pairs {
                        let items: Vec<&str> = pair.split(':').collect();
                        let item_name = items[0];
                        let item_type = items[1];
                        let item_field = Self::map_to_arrow_field(item_name, item_type)?;
                        fields.push(item_field)
                    }
                    Ok(DataType::Struct(fields))
                } else if let Some(map_cg) = MAP_RE.captures(glue_type) {
                    let key_type = map_cg.name("key_type").unwrap().as_str();
                    let value_type = map_cg.name("value_type").unwrap().as_str();
                    //println!("key type: {}, value type: {}", key_type, value_type);
                    let key_field = Self::map_to_arrow_field("key", key_type)?;
                    let value_field = Self::map_to_arrow_field("value", value_type)?;
                    Ok(DataType::Map(
                        Box::new(Field::new(
                            "entries",
                            DataType::Struct(vec![key_field, value_field]),
                            true,
                        )),
                        true,
                    ))
                } else {
                    Err(GlueError::NotImplemented(format!(
                        "No arrow type for gluetype: {}",
                        &glue_type
                    )))
                }
            }
        }
    }

    fn map_glue_column_to_arrow_field(glue_column: &Column) -> Result<Field> {
        let name = glue_column.name.as_ref().unwrap().clone();
        let glue_type = glue_column.r#type.as_ref().unwrap().clone();
        Self::map_to_arrow_field(&name, &glue_type)
    }

    fn map_to_arrow_field(glue_name: &str, glue_type: &str) -> Result<Field> {
        let arrow_data_type = Self::map_glue_type_to_arrow_data_type(glue_name, glue_type)?;
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

    #[test]
    fn test_map_glue_column_to_arrow_field() -> Result<()> {
        assert_eq!(
            GlueCatalogProvider::map_glue_column_to_arrow_field(
                &Column::builder().name("id").r#type("int").build()
            )
            .unwrap(),
            Field::new("id", DataType::Int32, true)
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_column_to_arrow_field(
                &Column::builder().name("id").r#type("bigint").build()
            )
            .unwrap(),
            Field::new("id", DataType::Int64, true)
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_column_to_arrow_field(
                &Column::builder().name("id").r#type("float").build()
            )
            .unwrap(),
            Field::new("id", DataType::Float32, true)
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_column_to_arrow_field(
                &Column::builder().name("id").r#type("double").build()
            )
            .unwrap(),
            Field::new("id", DataType::Float64, true)
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_column_to_arrow_field(
                &Column::builder().name("id").r#type("boolean").build()
            )
            .unwrap(),
            Field::new("id", DataType::Boolean, true)
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_column_to_arrow_field(
                &Column::builder().name("id").r#type("binary").build()
            )
            .unwrap(),
            Field::new("id", DataType::Binary, true)
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_column_to_arrow_field(
                &Column::builder().name("id").r#type("timestamp").build()
            )
            .unwrap(),
            Field::new("id", DataType::Timestamp(Nanosecond, None), true)
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_column_to_arrow_field(
                &Column::builder().name("id").r#type("string").build()
            )
            .unwrap(),
            Field::new("id", DataType::Utf8, true)
        );
        assert_eq!(
            GlueCatalogProvider::map_glue_column_to_arrow_field(
                &Column::builder().name("id").r#type("decimal(12,9)").build()
            )
            .unwrap(),
            Field::new("id", DataType::Decimal(12, 9), true)
        );
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
}
