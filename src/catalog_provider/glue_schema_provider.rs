// SPDX-License-Identifier: Apache-2.0
use log;

use crate::{catalog_provider::glue_table, error::GlueError};

use async_trait::async_trait;
use aws_sdk_glue::types::Table;
use aws_sdk_glue::Client;
use datafusion::{
    catalog::schema::SchemaProvider, datasource::TableProvider, error::Result,
    execution::object_store::ObjectStoreRegistry,
};
use std::any::Any;
use std::sync::Arc;

use super::glue_table::GlueTable;

/// `SchemaProvider` implementation for Amazon Glue API
#[allow(dead_code)]
pub struct GlueSchemaProvider {
    client: Client,
    tables: Vec<Table>,
    object_store_registry: Option<Arc<dyn ObjectStoreRegistry>>,
}

impl GlueSchemaProvider {
    pub fn new(
        client: Client,
        tables: Vec<Table>,
        object_store_registry: Option<Arc<dyn ObjectStoreRegistry>>,
    ) -> Self {
        Self {
            client,
            tables,
            object_store_registry,
        }
    }

    fn is_supported(&self, glue_table: &Table) -> bool {
        let table_type = Self::get_table_type(glue_table);
        if let Some("delta") = table_type {
            #[cfg(not(feature = "deltalake"))]
            return false;
            #[cfg(feature = "deltalake")]
            return crate::catalog_provider::delta_table::is_supported(
                &self.object_store_registry,
                glue_table,
            );
        }
        return GlueTable::is_supported(glue_table);
    }

    fn get_table_type<'a>(glue_table: &'a Table) -> Option<&'a str> {
        glue_table
            .parameters()
            .and_then(|f| f.get("table_type").or(f.get("spark.sql.sources.provider")))
            .map(|s| s.as_str())
    }

    async fn create_table(&self, glue_table: &Table) -> Result<Arc<dyn TableProvider>> {
        log::debug!("glue table: {:#?}", glue_table);

        let provider = glue_table
            .parameters()
            .and_then(|f| f.get("spark.sql.sources.provider"))
            .map(|s| s.as_str());

        if let Some("delta") = provider {
            #[cfg(feature = "deltalake")]
            return Ok(crate::catalog_provider::delta_table::create_delta_table(
                glue_table,
                &self.object_store_registry,
            )
            .await?);
            #[cfg(not(feature = "deltalake"))]
            Err(GlueError::DeltaLake(
                "DeltaLake support is not enabled".to_string(),
            ))?;
        }

        return Ok(Arc::new(glue_table::GlueTable::new(
            glue_table.clone(),
            self.client.clone(),
        )?));
    }
}

#[async_trait]
impl SchemaProvider for GlueSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .iter()
            .filter(|t| self.is_supported(t))
            .map(|t| t.name().to_string())
            .collect()
    }

    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn datafusion::datasource::TableProvider>>> {
        let table = self
            .tables
            .iter()
            .find(|t| t.name() == name)
            .ok_or_else(|| GlueError::AWS(format!("table {name} not found")))?;
        self.create_table(table).await.map(|t| Some(t))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.iter().find(|t| t.name() == name).is_some()
    }
}
