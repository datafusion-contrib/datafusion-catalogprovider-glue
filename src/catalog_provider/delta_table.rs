use std::sync::Arc;

use crate::error::{GlueError, Result};
use aws_sdk_glue::types::Table;
use datafusion::{datasource::TableProvider, execution::object_store::ObjectStoreRegistry};
use deltalake::{DeltaTableBuilder, ObjectStore};

fn table_location(table: &Table) -> Result<&String> {
    let location = table
        .storage_descriptor()
        .and_then(|sd| sd.serde_info())
        .and_then(|si| si.parameters())
        .and_then(|params| params.get("path"))
        .ok_or_else(|| GlueError::DeltaLake("no delta table location".to_string()))?;
    Ok(location)
}

fn get_object_store(
    object_store_registry: &Option<Arc<dyn ObjectStoreRegistry>>,
    location: &str,
) -> crate::error::Result<Arc<dyn ObjectStore>> {
    let registry = object_store_registry
        .as_ref()
        .ok_or_else(|| GlueError::DeltaLake(format!("no object store")))?;

    let url = url::Url::parse(location)?;

    let object_store = registry.get_store(&url)?;
    Ok(object_store)
}

pub(crate) fn is_supported(
    object_store_registry: &Option<Arc<dyn ObjectStoreRegistry>>,
    table: &Table,
) -> bool {
    match table_location(table) {
        Ok(location) => get_object_store(object_store_registry, &location).is_ok(),
        _ => false,
    }
}

pub(crate) async fn create_delta_table(
    table: &Table,
    object_store_registry: &Option<Arc<dyn ObjectStoreRegistry>>,
) -> Result<Arc<dyn TableProvider>> {
    let location = table_location(table)?;
    let url = url::Url::parse(&location)?;
    let mut builder = DeltaTableBuilder::from_uri(&location);

    if let Ok(object_store) = get_object_store(object_store_registry, &location) {
        builder = builder.with_storage_backend(object_store, url);
    }

    builder
        .with_storage_options(std::env::vars().collect())
        .load()
        .await
        .map(|t| Arc::new(t) as Arc<dyn TableProvider>)
        .map_err(|err| GlueError::DeltaLake(err.to_string()))
}
