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

use datafusion::arrow::array::StringArray;
use datafusion::common::DataFusionError;
use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::error::Result;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::*;
use datafusion_catalogprovider_glue::catalog_provider::glue::{
    GlueCatalogProvider, TableRegistrationOptions,
};
use std::sync::Arc;
use url::Url;
use s3_object_store_provider::DemoS3ObjectStoreProvider;

mod s3_object_store_provider;

#[tokio::main]
async fn main() -> Result<()> {

    // Load an aws sdk config from the environment
    let sdk_config = aws_config::load_from_env().await;

    // Register an object store provider which creates instances for each requested s3://bucket using the sdk_config credentials
    // As an alternative you can also manually register the required object_store(s)
    let object_store_provider = Arc::new(DemoS3ObjectStoreProvider::new(&sdk_config).await?);
    let object_store_registry = Arc::new(ObjectStoreRegistry::new_with_provider(Some(
        object_store_provider,
    )));
    let runtime_config = RuntimeConfig::default().with_object_store_registry(object_store_registry);
    let config = SessionConfig::new().with_information_schema(true);
    let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
    let ctx = SessionContext::with_config_rt(config, runtime);

    let mut glue_catalog_provider = GlueCatalogProvider::new(&sdk_config);

    let register_results = glue_catalog_provider
        .register_all_with_options(&TableRegistrationOptions::InferSchemaFromData, &ctx.state())
        .await?;
    for result in register_results {
        if result.is_err() {
            // only output tables which were not registered...
            println!("{:?}", result);
        }
    }

    ctx.register_catalog("glue", Arc::new(glue_catalog_provider));

    ctx.sql("select * from information_schema.tables")
        .await?
        .show()
        .await?;

    let tables = ctx
        .sql(
            r#"
    select table_schema, table_name
    from information_schema.tables
    where table_catalog='glue'
    and table_schema <> 'information_schema'
    and table_schema <> 'datafusion_delta'
    and table_name <> 'parquet_testing_nullable_impala_parquet'
    and table_name <> 'parquet_testing_nonnullable_impala_parquet'
    and table_name <> 'parquet_testing_nested_lists_snappy_parquet'
    and table_name <> 'parquet_testing_null_list_parquet'
    order by table_catalog asc, table_schema asc, table_name asc
    "#,
        )
        .await?
        .collect()
        .await?;

    for batch in tables {
        let schema_column = batch.column(0);
        let table_column = batch.column(1);
        let schema_array = schema_column
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let table_array = table_column.as_any().downcast_ref::<StringArray>().unwrap();

        for row in 00..batch.num_rows() {
            let schema = schema_array.value(row);
            let table = table_array.value(row);
            match sample(ctx.clone(), schema, table, 10).await {
                Ok(_) => {}
                Err(e) => {
                    println!("failed to sample {}.{} due to {}", schema, table, e)
                }
            };
        }
    }

    Ok(())
}

async fn sample(ctx: SessionContext, schema: &str, table: &str, limit: usize) -> Result<()> {
    println!("sampling glue.{}.{}", schema, table);
    ctx.sql(&format!("select * from glue.{}.{}", schema, table))
        .await?
        .show_limit(limit)
        .await?;
    Ok(())
}

fn get_host_name(url: &Url) -> Result<&str> {
    url.host_str().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Not able to parse hostname from url, {}",
            url.as_str()
        ))
    })
}
