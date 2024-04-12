use std::sync::Arc;

use crate::error::{GlueError, Result};
use aws_sdk_glue::types::Table;
use datafusion::{datasource::TableProvider, execution::object_store::ObjectStoreRegistry};
use deltalake::DeltaTableBuilder;

fn table_location(table: &Table) -> Result<String> {
    let location = table
        .storage_descriptor()
        .and_then(|sd| sd.serde_info())
        .and_then(|si| si.parameters())
        .and_then(|params| params.get("path"))
        .cloned()
        .map(|loc| loc.replace("s3a", "s3"))
        .ok_or_else(|| GlueError::DeltaLake("no delta table location".to_string()))?;

    if location.starts_with("dbfs:") {
        return Err(GlueError::DeltaLake(format!(
            "unsupported schema: {:?}",
            location
        )));
    }
    Ok(location)
}

pub async fn create_delta_table(
    table: &Table,
    object_store_registry: &Option<Arc<dyn ObjectStoreRegistry>>,
) -> Result<Arc<dyn TableProvider>> {
    let location = table_location(table)?;
    log::info!("  location: {}", location);

    let mut builder = DeltaTableBuilder::from_uri(&location);
    if let Some(registry) = object_store_registry {
        let url =
            url::Url::parse(&location).map_err(|err| GlueError::DeltaLake(err.to_string()))?;

        let object_store = registry
            .get_store(&url)
            .map_err(|err| GlueError::DeltaLake(err.to_string()))?;

        builder = builder.with_storage_backend(object_store, url);
    }
    builder
        .with_storage_options(std::env::vars().collect())
        .load()
        .await
        .map(|t| Arc::new(t) as Arc<dyn TableProvider>)
        .map_err(|err| GlueError::DeltaLake(err.to_string()))
}
