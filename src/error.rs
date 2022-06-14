// SPDX-License-Identifier: Apache-2.0

//! Custom error type for `DataFusion-CatalogProvider-Glue`
use aws_sdk_glue::types::SdkError;
use datafusion::common::DataFusionError;
use std::error::Error;
use std::fmt::{Display, Formatter};

/// Enum with all errors in this crate.
/// PartialEq is to enable testing for specific error types
#[derive(Debug)]
pub enum GlueError {
    /// Returned when functionaly is not yet available.
    NotImplemented(String),
    /// Wrapper for AWS errors
    AWS(String),
    /// Wrapper for datafusion errors
    DataFusion(DataFusionError),
    /// Error during mapping of GlueDataType
    GlueDataTypeMapping(String),
}

impl Display for GlueError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GlueError::NotImplemented(desc) => write!(f, "Not yet implemented: {}", desc),
            GlueError::AWS(desc) => write!(f, "AWS error: {}", desc),
            GlueError::DataFusion(e) => e.fmt(f),
            GlueError::GlueDataTypeMapping(desc) => {
                write!(f, "Could not map glue data type: {}", desc)
            }
        }
    }
}

impl Error for GlueError {}

impl From<GlueError> for DataFusionError {
    fn from(glue_error: GlueError) -> Self {
        DataFusionError::External(Box::new(glue_error))
    }
}

impl From<DataFusionError> for GlueError {
    fn from(error: DataFusionError) -> Self {
        GlueError::DataFusion(error)
    }
}

impl<T: Error + 'static + Send + Sync> From<SdkError<T>> for GlueError {
    fn from(sdk_error: SdkError<T>) -> Self {
        GlueError::AWS(sdk_error.to_string())
    }
}

/// Result type for operations that can result in a GlueError
pub type Result<T> = std::result::Result<T, GlueError>;
