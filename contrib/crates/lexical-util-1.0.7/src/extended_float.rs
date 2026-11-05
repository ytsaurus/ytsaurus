//! Extended precision floating-point type.
//!
//! Also contains helpers to convert to and from native rust floats.
//! This representation stores the mantissa as a 64-bit unsigned integer,
//! and the exponent as a 32-bit unsigned integer, allowed ~80 bits of
//! precision (only 16 bits of the 32-bit integer are used, u32 is used
//! for performance). Since there is no storage for the sign bit,
//! this only works for positive floats.

#![cfg(any(feature = "parse-floats", feature = "write-floats"))]

use crate::num::UnsignedInteger;

/// Extended precision floating-point type.
///
/// This doesn't have any methods because it's used for **very** different
/// things for the Lemire, Bellepheron, and other algorithms. In Grisu,
/// it's an unbiased representation, for Lemire, it's a biased representation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExtendedFloat<M: UnsignedInteger> {
    /// Mantissa for the extended-precision float.
    pub mant: M,
    /// Binary exponent for the extended-precision float.
    pub exp: i32,
}

impl<M: UnsignedInteger> ExtendedFloat<M> {
    /// Get the mantissa component.
    #[inline(always)]
    pub fn mantissa(&self) -> M {
        self.mant
    }

    /// Get the exponent component.
    #[inline(always)]
    pub fn exponent(&self) -> i32 {
        self.exp
    }
}
