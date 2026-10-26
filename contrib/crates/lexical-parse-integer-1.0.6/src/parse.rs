//! Shared trait and methods for parsing integers.

#![doc(hidden)]

// Select the correct back-end.
use lexical_util::num::Integer;
use lexical_util::result::Result;

use crate::algorithm::{algorithm_complete, algorithm_partial};
use crate::Options;

/// Parse integer trait, implemented in terms of the optimized back-end.
pub trait ParseInteger: Integer {
    /// Forward complete parser parameters to the backend.
    #[cfg_attr(not(feature = "compact"), inline(always))]
    fn parse_complete<const FORMAT: u128>(bytes: &[u8], options: &Options) -> Result<Self> {
        algorithm_complete::<_, { FORMAT }>(bytes, options)
    }

    /// Forward partial parser parameters to the backend.
    #[cfg_attr(not(feature = "compact"), inline(always))]
    fn parse_partial<const FORMAT: u128>(bytes: &[u8], options: &Options) -> Result<(Self, usize)> {
        algorithm_partial::<_, { FORMAT }>(bytes, options)
    }
}

macro_rules! parse_integer_impl {
    ($($t:ty)*) => ($(
        impl ParseInteger for $t {}
    )*)
}

parse_integer_impl! { u8 u16 u32 u64 u128 usize }
parse_integer_impl! { i8 i16 i32 i64 i128 isize }
