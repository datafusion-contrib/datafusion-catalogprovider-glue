// SPDX-License-Identifier: Apache-2.0

/// Module with Datafusion `CatalogProvider` implementation for Amazon AWS Glue
pub mod glue;

#[cfg(feature = "deltalake")]
mod delta_table;

mod glue_schema_provider;
mod glue_catalog_provider;
mod glue_table;
