//! Result type for numeric parsing functions.

use core::result;

use crate::error;

/// A specialized [`Result`][`result::Result`] type for lexical operations.
pub type Result<T> = result::Result<T, error::Error>;
