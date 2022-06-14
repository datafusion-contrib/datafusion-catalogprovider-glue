// SPDX-License-Identifier: Apache-2.0

#![warn(missing_docs)]

//! [DataFusion-CatalogProvider-Glue](https://github.com/datafusion-contrib/datafusion-catalogprovider-glue)
//! provides a `CatalogProvider` interface for using `Datafusion` to query data in S3 via Glue.
//!
//! ## Examples
//! See [demo.rs](https://github.com/datafusion-contrib/datafusion-catalogprovider-glue/blob/main/examples/demo.rs).

/// Module which contains error definitions
pub mod error;

/// Module which contains the Glue CatalogProvider
pub mod catalog_provider;

/// Module which contains parser logic for Glue data types
mod glue_data_type_parser;
