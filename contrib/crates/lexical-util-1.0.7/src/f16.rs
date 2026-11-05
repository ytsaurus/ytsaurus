//! Half-precision IEEE-754 floating point implementation.
//!
//! f16 is meant as an interchange format, and therefore there may be
//! rounding error in using it for fast-path algorithms. Since there
//! are no native operations using `f16`, this is of minimal concern.

#![cfg(feature = "f16")]

pub use float16::f16;
