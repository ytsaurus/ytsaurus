//! Shared utilities for lexical conversion routines.
//!
//! These are not meant to be used publicly for any numeric
//! conversion routines, but provide optimized math routines,
//! format packed struct definitions, and custom iterators
//! for all workspaces.
//!
//! # Features
//!
//! * `power-of-two` - Add support for parsing and writing power-of-two integer
//!   strings.
//! * `radix` - Add support for parsing and writing strings of any radix.
//! * `format` - Add support for custom number formats.
//! * `write-integers` - Add support for writing integers (used for
//!   [`lexical-write-integer`]).
//! * `write-floats` - Add support for writing floats (used for
//!   [`lexical-write-float`]).
//! * `parse-integers` - Add support for parsing integers (used for
//!   [`lexical-parse-integer`]).
//! * `parse-floats` - Add support for parsing floats (used for
//!   [`lexical-write-float`]).
//! * `compact` - Reduce code size at the cost of performance.
//! * `f16` - Enable support for half-precision [`f16`][`ieee-f16`] and
//!   [`bf16`][`brain-float`] floats.
//! * `std` (Default) - Disable to allow use in a [`no_std`] environment.
//!
//! [`no_std`]: https://docs.rust-embedded.org/book/intro/no-std.html
//! [`ieee-f16`]: https://en.wikipedia.org/wiki/Half-precision_floating-point_format
//! [`brain-float`]: https://en.wikipedia.org/wiki/Bfloat16_floating-point_format
//!
//! # Public API
//!
//! [`lexical-util`] mainly exists as an implementation detail for
//! the other lexical crates, although its API is mostly stable. If you would
//! like to use a high-level API that writes to and parses from [`String`] and
//! [`str`], respectively, please look at [`lexical`] instead. If you would like
//! an API that supports multiple numeric conversions without a dependency on
//! [`alloc`], please look at [`lexical-core`] instead.
//!
//! <div class="warning">
//!
//! Any undocumented, implementation details may change release-to-release
//! without major or minor version changes. Use internal implementation details
//! at your own risk. Any changes other than to [`NumberFormatBuilder`],
//! [`NumberFormat`], [`mod@format`], and [`mod@options`] will not be considered
//! a breaking change.
//!
//! </div>
//!
//! # Version Support
//!
//! The minimum, standard, required version is [`1.63.0`][`rust-1.63.0`], for
//! const generic support. Older versions of lexical support older Rust
//! versions.
//!
//! # Safety Guarantees
//!
//! For a detailed breakdown on the use of [`unsafe`], how and why our traits
//! are implemented safely, and how to verify this, see [`Safety`].
//!
//! [`lexical`]: https://crates.io/crates/lexical
//! [`lexical-parse-float`]: https://crates.io/crates/lexical-parse-float
//! [`lexical-parse-integer`]: https://crates.io/crates/lexical-parse-integer
//! [`lexical-write-float`]: https://crates.io/crates/lexical-write-float
//! [`lexical-write-integer`]: https://crates.io/crates/lexical-write-integer
//! [`lexical-core`]: https://crates.io/crates/lexical-core
//! [`lexical-util`]: https://crates.io/crates/lexical-util
//! [`rust-1.63.0`]: https://blog.rust-lang.org/2022/08/11/Rust-1.63.0.html
//! [`alloc`]: https://doc.rust-lang.org/alloc/
//! [`String`]: https://doc.rust-lang.org/alloc/string/struct.String.html
//! [`Safety`]: https://github.com/Alexhuszagh/rust-lexical/blob/main/lexical-util/docs/Safety.md
//! [`unsafe`]: https://doc.rust-lang.org/book/ch19-01-unsafe-rust.html

// FIXME: Implement clippy/allow reasons once we drop support for 1.80.0 and below
// Clippy reasons were stabilized in 1.81.0.

// We want to have the same safety guarantees as Rust core,
// so we allow unused unsafe to clearly document safety guarantees.
#![allow(unused_unsafe)]
#![cfg_attr(feature = "lint", warn(unsafe_op_in_unsafe_fn))]
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![deny(
    clippy::doc_markdown,
    clippy::unnecessary_safety_comment,
    clippy::semicolon_if_nothing_returned,
    clippy::unwrap_used,
    clippy::as_underscore
)]
#![allow(
    // used when concepts are logically separate
    clippy::match_same_arms,
    // loss of precision is intentional
    clippy::integer_division,
    // mathematical names use 1-character identifiers
    clippy::min_ident_chars,
    // these are not cryptographically secure contexts
    clippy::integer_division_remainder_used,
    // this can be intentional
    clippy::module_name_repetitions,
    // this is intentional: already passing a pointer and need performance
    clippy::needless_pass_by_value,
    // we use this for inline formatting for unsafe blocks
    clippy::semicolon_inside_block,
)]

pub mod algorithm;
pub mod ascii;
pub mod assert;
pub mod bf16;
pub mod constants;
pub mod digit;
pub mod div128;
pub mod error;
pub mod extended_float;
pub mod f16;
pub mod format;
pub mod iterator;
pub mod mul;
pub mod num;
pub mod options;
pub mod result;
pub mod step;

mod api;
mod feature_format;
mod format_builder;
mod format_flags;
mod libm;
mod noskip;
mod not_feature_format;
mod prebuilt_formats;
mod skip;

#[cfg(any(feature = "write-floats", feature = "write-integers"))]
pub use constants::{FormattedSize, BUFFER_SIZE};
pub use error::Error;
pub use format::{NumberFormat, NumberFormatBuilder};
#[cfg(any(feature = "parse-floats", feature = "parse-integers"))]
pub use options::ParseOptions;
#[cfg(any(feature = "write-floats", feature = "write-integers"))]
pub use options::WriteOptions;
pub use result::Result;
