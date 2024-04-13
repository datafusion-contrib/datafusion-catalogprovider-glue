// SPDX-License-Identifier: Apache-2.0
use crate::error::GlueError;

use aws_sdk_glue::Client;
use aws_types::SdkConfig;
use datafusion::{
    catalog::{schema::SchemaProvider, CatalogProvider},
    execution::object_store::ObjectStoreRegistry,
};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use super::glue_schema_provider::GlueSchemaProvider;

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
    pub async fn default() -> crate::error::Result<Self> {
        GlueCatalogProvider::new(GlueCatalogConfig::default()).await
    }

    /// Create a new Glue CatalogProvider
    pub async fn new(config: GlueCatalogConfig) -> crate::error::Result<Self> {
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
                Arc::new(GlueSchemaProvider::new(
                    client.clone(),
                    tables,
                    config.object_store_registry.clone(),
                )) as Arc<dyn SchemaProvider>,
            );
        }

        Ok(GlueCatalogProvider {
            schema_provider_by_database,
        })
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
