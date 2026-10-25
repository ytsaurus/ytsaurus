//! Brain Floating Point implementation, a 16-bit type used in machine learning.
//!
//! bf16 is meant as an interchange format, and therefore there may be
//! rounding error in using it for fast-path algorithms. Since there
//! are no native operations using `bf16`, this is of minimal concern.

#![cfg(feature = "f16")]

pub use float16::bf16;
