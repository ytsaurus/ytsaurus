//! Pre-computed large value tables for writing float strings.

#![doc(hidden)]

// Re-export all the feature-specific files.
#[cfg(feature = "compact")]
pub use crate::table_bellerophon_decimal::*;
#[cfg(feature = "radix")]
pub use crate::table_bellerophon_radix::*;
#[cfg(not(feature = "compact"))]
pub use crate::table_lemire::*;
