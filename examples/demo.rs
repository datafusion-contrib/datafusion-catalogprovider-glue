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

use aws_config::BehaviorVersion;
use aws_sdk_glue::config::{Credentials, ProvideCredentials};
use aws_types::SdkConfig;
use dashmap::DashMap;
use datafusion::arrow::array::StringArray;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::*;
use datafusion_catalogprovider_glue::catalog_provider::glue::{
    GlueCatalogProvider, TableRegistrationOptions,
};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use std::fmt::Debug;
use std::sync::Arc;
use url::Url;

#[tokio::main]
async fn main() -> Result<()> {
    // Load an aws sdk config from the environment
    let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;

    // Register an object store provider which creates instances for each requested s3://bucket using the sdk_config credentials
    // As an alternative you can also manually register the required object_store(s)
    let object_store_provider = DemoS3ObjectStoreProvider::new(&sdk_config).await?;
    let runtime_config =
        RuntimeConfig::default().with_object_store_registry(Arc::new(object_store_provider));
    let config = SessionConfig::new().with_information_schema(true);
    let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
    let ctx = SessionContext::new_with_config_rt(config, runtime);

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

#[derive(Debug)]
pub struct DemoS3ObjectStoreProvider {
    credentials: Credentials,
    region: String,
    object_stores: tokio::sync::RwLock<DashMap<Url, Arc<dyn ObjectStore>>>,
}

impl DemoS3ObjectStoreProvider {
    pub async fn new(sdk_config: &SdkConfig) -> crate::Result<DemoS3ObjectStoreProvider> {
        let credentials_provider = sdk_config
            .credentials_provider()
            .expect("could not find credentials provider");
        let credentials = credentials_provider
            .provide_credentials()
            .await
            .expect("could not load credentials");
        let region = sdk_config
            .region()
            .map(|r| r.to_string())
            .unwrap_or_else(|| "eu-central-1".to_string());

        let object_stores: tokio::sync::RwLock<DashMap<Url, Arc<dyn ObjectStore>>> =
            tokio::sync::RwLock::new(DashMap::new());

        Ok(DemoS3ObjectStoreProvider {
            credentials,
            region,
            object_stores,
        })
    }

    fn build_s3_object_store(&self, url: &Url) -> crate::Result<Arc<dyn ObjectStore>> {
        let bucket_name = get_host_name(url)?;

        let s3_builder = AmazonS3Builder::new()
            .with_bucket_name(bucket_name)
            .with_region(&self.region)
            .with_access_key_id(self.credentials.access_key_id())
            .with_secret_access_key(self.credentials.secret_access_key());

        let s3 = match self.credentials.session_token() {
            Some(session_token) => s3_builder.with_token(session_token),
            None => s3_builder,
        }
        .build()?;

        Ok(Arc::new(s3))
    }
}

impl ObjectStoreRegistry for DemoS3ObjectStoreProvider {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        {
            let guard = self.object_stores.blocking_write();
            guard.insert(url.clone(), store.clone());
        }
        Some(store.clone())
    }

    fn get_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        self.build_s3_object_store(url)
    }
}

fn get_host_name(url: &Url) -> Result<&str> {
    url.host_str().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Not able to parse hostname from url, {}",
            url.as_str()
        ))
    })
}
