// SPDX-License-Identifier: Apache-2.0

/// Module with Datafusion `CatalogProvider` implementation for Amazon AWS Glue
pub mod glue;

#[cfg(feature = "deltalake")]
pub mod delta_table;

pub mod glue_table;
