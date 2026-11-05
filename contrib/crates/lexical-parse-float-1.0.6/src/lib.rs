//! Fast lexical string-to-float conversion routines.
//!
//! This contains high-performance methods to parse floats from bytes.
//! Using [`from_lexical`] is analogous to [`parse`][`core-parse`],
//! while enabling parsing from bytes as well as [`str`].
//!
//!
//! [`from_lexical`]: FromLexical::from_lexical
//! [`core-parse`]: core::str::FromStr
//!
//! # Getting Started
//!
//! To parse a number from bytes, use [`from_lexical`]:
//!
//! ```rust
//! # #[no_std]
//! # use core::str;
//! use lexical_parse_float::{Error, FromLexical};
//!
//! let value = f64::from_lexical("1234.5".as_bytes());
//! assert_eq!(value, Ok(1234.5));
//!
//! let value = f64::from_lexical("1.2345e325".as_bytes());
//! assert_eq!(value, Ok(f64::INFINITY));
//!
//! let value = f64::from_lexical("1234.5 }, {\"Key\", \"Value\"}}".as_bytes());
//! assert_eq!(value, Err(Error::InvalidDigit(6)));
//! ```
//!
//! If wishing to incrementally parse a string from bytes, that is, parse as
//! many characters until an invalid digit is found, you can use the partial
//! parsers. This is useful in parsing data where the type is known, such as
//! JSON, but where the end of the number is not yet known.
//!
//! ```rust
//! # #[no_std]
//! # use core::str;
//! use lexical_parse_float::{Error, FromLexical};
//!
//! let value = f64::from_lexical_partial("1234.5 }, {\"Key\", \"Value\"}}".as_bytes());
//! assert_eq!(value, Ok((1234.5, 6)));
//!
//! let value = f64::from_lexical_partial("1.2345e325".as_bytes());
//! assert_eq!(value, Ok((f64::INFINITY, 10)));
//! ```
//!
//! # Options/Formatting API
//!
//! Each float parser contains extensive formatting control through
//! [`mod@options`] and [`mod@format`], including digit [`separator`]
//! support (that is, floats such as `1_2__3.4_5`), if integral,
//! fractional, or any significant digits are required, if to disable
//! parsing non-finite values, if `+` signs are invalid or required,
//! and much more. For more comprehensive examples, see the
//! [`format`](#format) and [Comprehensive Configuration] sections
//! below.
//!
//! [`separator`]: NumberFormat::digit_separator
//! [Comprehensive Configuration]: #comprehensive-configuration
//!
//! ```rust
//! # #[cfg(feature = "radix")] {
//! # use core::str;
//! use lexical_parse_float::{Error, FromLexicalWithOptions, NumberFormatBuilder, Options};
//!
//! const FORMAT: u128 = NumberFormatBuilder::new()
//!     // require a `+` or `-` sign before the number
//!     .required_mantissa_sign(true)
//!     // require a `+` or `-` sign before the exponent digits
//!     .required_exponent_sign(true)
//!     // build the format, panicking if the format is invalid
//!     .build_strict();
//! const OPTIONS: Options = Options::new();
//!
//! let value = "+1.234e+300";
//! let result = f64::from_lexical_with_options::<FORMAT>(value.as_bytes(), &OPTIONS);
//! assert_eq!(result, Ok(1.234e+300));
//!
//! let value = "1.234e+300";
//! let result = f64::from_lexical_with_options::<FORMAT>(value.as_bytes(), &OPTIONS);
//! assert_eq!(result, Err(Error::MissingSign(0)));
//! # }
//! ```
//!
//! # Features
//!
//! * `format` - Add support for parsing custom integer formats.
//! * `power-of-two` - Add support for parsing power-of-two integer strings.
//! * `radix` - Add support for strings of any radix.
//! * `compact` - Reduce code size at the cost of performance.
//! * `f16` - Enable support for half-precision [`f16`][`ieee-f16`] and
//!   [`bf16`][`brain-float`] floats.
//! * `std` (Default) - Disable to allow use in a [`no_std`] environment.
//!
//! [`no_std`]: https://docs.rust-embedded.org/book/intro/no-std.html
//! [`ieee-f16`]: https://en.wikipedia.org/wiki/Half-precision_floating-point_format
//! [`brain-float`]: https://en.wikipedia.org/wiki/Bfloat16_floating-point_format
//!
//! A complete description of supported features includes:
//!
//! #### format
//!
//! Add support custom float parsing specifications. This should be used in
//! conjunction with [`Options`] for extensible float parsing.
//!
//! ##### JSON
//!
//! For example, in JSON, the following floats are valid or invalid:
//!
//! ```text
//! -1          // valid
//! +1          // invalid
//! 1           // valid
//! 1.          // invalid
//! .1          // invalid
//! 0.1         // valid
//! nan         // invalid
//! inf         // invalid
//! Infinity    // invalid
//! ```
//!
//! All of the finite numbers are valid in Rust, and Rust provides constants
//! for non-finite floats. In order to parse standard-conforming JSON floats
//! using lexical, you may use the following approach:
//!
//! ```rust
//! # #[cfg(feature = "format")] {
//! use lexical_parse_float::{format, options, Error, FromLexicalWithOptions, Result};
//!
//! fn parse_json_float(bytes: &[u8]) -> Result<f64> {
//!     f64::from_lexical_with_options::<{ format::JSON }>(bytes, &options::JSON)
//! }
//!
//! assert_eq!(parse_json_float(b"-1"), Ok(-1.0));
//! assert_eq!(parse_json_float(b"+1"), Err(Error::InvalidPositiveSign(0)));
//! assert_eq!(parse_json_float(b"1"), Ok(1.0));
//! assert_eq!(parse_json_float(b"1."), Err(Error::EmptyFraction(2)));
//! assert_eq!(parse_json_float(b"0.1"), Ok(0.1));
//! assert_eq!(parse_json_float(b"nan"), Err(Error::EmptyInteger(0)));
//! assert_eq!(parse_json_float(b"inf"), Err(Error::EmptyInteger(0)));
//! assert_eq!(parse_json_float(b"Infinity"), Err(Error::EmptyInteger(0)));
//! # }
//! ```
//!
//! ##### Custom Format
//!
//! An example building and using a custom format, with many of the available
//! options is:
//!
//! ```rust
//! # #[cfg(feature = "format")] {
//! # use core::{num, str};
//! use lexical_parse_float::{Error, NumberFormatBuilder, Options, FromLexicalWithOptions};
//!
//! const FORMAT: u128 = NumberFormatBuilder::new()
//!     // enable the use of digit separators with `_`
//!     .digit_separator(num::NonZeroU8::new(b'_'))
//!     // require digits before and after the decimal point,
//!     // if the decimal point is present.
//!     .required_integer_digits(true)
//!     .required_fraction_digits(true)
//!     // do not allow a leading `+` sign, so `+123` is invalid
//!     .no_positive_mantissa_sign(true)
//!     // do not allow `0` before an integer, so `01.1` is invalid.
//!     // however, `0.1` is valid.
//!     .no_integer_leading_zeros(true)
//!     // allow digit separators anywhere, including consecutive ones
//!     .leading_digit_separator(true)
//!     .trailing_digit_separator(true)
//!     .internal_digit_separator(true)
//!     .consecutive_digit_separator(true)
//!     // make it so the exponent character, `e`, is case-sensitive
//!     // that is, `E` is not considered a valid exponent character
//!     .case_sensitive_exponent(true)
//!     .build_strict();
//! const OPTIONS: Options = Options::builder()
//!     // change the string representation of NaN from `NaN` to `nan`
//!     .nan_string(Some(b"nan"))
//!     // disable a short infinity: long infinity is still allowed
//!     .inf_string(None)
//!     .build_strict();
//!
//! let value = f64::from_lexical_with_options::<FORMAT>(b"1_2.3_4", &OPTIONS);
//! assert_eq!(value, Ok(12.34));
//!
//! let value = f64::from_lexical_with_options::<FORMAT>(b"-inf", &OPTIONS);
//! assert_eq!(value, Err(Error::EmptyInteger(1)));
//!
//! let value = f64::from_lexical_with_options::<FORMAT>(b"Infinity", &OPTIONS);
//! assert_eq!(value, Ok(f64::INFINITY));
//!
//! let value = f64::from_lexical_with_options::<FORMAT>(b"nan", &OPTIONS);
//! assert_eq!(value.map(|x| x.is_nan()), Ok(true));
//!
//! let value = f64::from_lexical_with_options::<FORMAT>(b"+1_2.3_4", &OPTIONS);
//! assert_eq!(value, Err(Error::InvalidPositiveSign(0)));
//!
//! let value = f64::from_lexical_with_options::<FORMAT>(b"0.3_4", &OPTIONS);
//! assert_eq!(value, Ok(0.34));
//!
//! let value = f64::from_lexical_with_options::<FORMAT>(b"12", &OPTIONS);
//! assert_eq!(value, Ok(12.0));
//!
//! let value = f64::from_lexical_with_options::<FORMAT>(b"12.", &OPTIONS);
//! assert_eq!(value, Err(Error::EmptyFraction(3)));
//!
//! let value = f64::from_lexical_with_options::<FORMAT>(b"1.234e5", &OPTIONS);
//! assert_eq!(value, Ok(1.234e5));
//!
//! let value = f64::from_lexical_with_options::<FORMAT>(b"1.234E5", &OPTIONS);
//! assert_eq!(value, Err(Error::InvalidDigit(5)));
//! # }
//! ```
//!
//! Enabling the [`format`](crate#format) API significantly increases compile
//! times, however, it enables a large amount of customization in how floats are
//! written.
//!
//! #### power-of-two
//!
//! Enable parsing numbers with radixes that are powers of two, that is, `2`,
//! `4`, `8`, `16`, and `32`.
//!
//! ```rust
//! # #[cfg(feature = "power-of-two")] {
//! use lexical_parse_float::{NumberFormatBuilder, Options, FromLexicalWithOptions};
//!
//! const BINARY: u128 = NumberFormatBuilder::binary();
//! const OPTIONS: Options = Options::new();
//! let value = "1.0011101111100111011011001000101101000011100101011";
//! let result = f64::from_lexical_with_options::<BINARY>(value.as_bytes(), &OPTIONS);
//! assert_eq!(result, Ok(1.234f64));
//! # }
//! ```
//!
//! #### radix
//!
//! Enable parsing numbers using all radixes from `2` to `36`. This requires
//! more static storage than [`power-of-two`][crate#power-of-two], and increases
//! compile times, but can be quite useful for esoteric programming languages
//! which use duodecimal floats, for example.
//!
//! ```rust
//! # #[cfg(feature = "radix")] {
//! use lexical_parse_float::{NumberFormatBuilder, Options, FromLexicalWithOptions};
//!
//! const FORMAT: u128 = NumberFormatBuilder::from_radix(12);
//! const OPTIONS: Options = Options::new();
//! let value = "1.29842830A44BAA2";
//! let result = f64::from_lexical_with_options::<FORMAT>(value.as_bytes(), &OPTIONS);
//! assert_eq!(result, Ok(1.234f64));
//! # }
//! ```
//!
//! #### compact
//!
//! Reduce the generated code size at the cost of performance. This minimizes
//! the number of static tables, inlining, and generics used, drastically
//! reducing the size of the generated binaries. However, this resulting
//! performance of the generated code is much lower.
//!
//! #### f16
//!
//! This enables the use of the half-precision floats [`f16`][`ieee-f16`] and
//! [`bf16`][`brain-float`]. However, since these have limited hardware support
//! and are primarily used for vectorized operations, they are parsed as if
//! they were an [`f32`]. Due to the low precision of 16-bit floats, the results
//! may appear to have significant rounding error.
//!
//! ```rust
//! # #[cfg(feature = "f16")] {
//! # use core::str;
//! use lexical_parse_float::{f16, FromLexical};
//!
//! let value = "1.234375";
//! let result = f16::from_lexical(value.as_bytes());
//! assert_eq!(result, Ok(f16::from_f64_const(1.234f64)));
//! # }
//! ```
//!
//! #### std
//!
//! Enable use of the standard library. Currently, the standard library
//! is not used, and may be disabled without any change in functionality
//! on stable.
//!
//! # Comprehensive Configuration
//!
//! `lexical-parse-float` provides two main levels of configuration:
//! - The [`NumberFormatBuilder`], creating a packed struct with custom
//!   formatting options.
//! - The [`Options`] API.
//!
//! ## Number Format
//!
//! The number format class provides numerous flags to specify number writing.
//! When the [`power-of-two`](#power-of-two) feature is enabled, additional
//! flags are added:
//! - The radix for the significant digits (default `10`).
//! - The radix for the exponent base (default `10`).
//! - The radix for the exponent digits (default `10`).
//!
//! When the [`format`](#format) feature is enabled, numerous other syntax and
//! digit separator flags are enabled, including:
//! - A digit separator character, to group digits for increased legibility.
//! - Whether leading, trailing, internal, and consecutive digit separators are
//!   allowed.
//! - Toggling required float components, such as digits before the decimal
//!   point.
//! - Toggling whether special floats are allowed or are case-sensitive.
//!
//! Many pre-defined constants therefore exist to simplify common use-cases,
//! including:
//! - [`JSON`], [`XML`], [`TOML`], [`YAML`], [`SQLite`], and many more.
//! - [`Rust`], [`Python`], [`C#`], [`FORTRAN`], [`COBOL`] literals and strings,
//!   and many more.
//!
//! For a list of all supported fields, see [Parse
//! Float Fields][NumberFormatBuilder#parse-float-fields].
//!
//! <!-- Spacer for rustfmt -->
#![cfg_attr(
    feature = "format",
    doc = "
[`JSON`]: format::JSON
[`XML`]: format::XML
[`TOML`]: format::TOML
[`YAML`]: format::YAML
[`SQLite`]: format::SQLITE
[`Rust`]: format::RUST_LITERAL
[`Python`]: format::PYTHON_LITERAL
[`C#`]: format::CSHARP_LITERAL
[`FORTRAN`]: format::FORTRAN_LITERAL
[`COBOL`]: format::COBOL_LITERAL
"
)]
#![cfg_attr(
    not(feature = "format"),
    doc = "
[`JSON`]: https://docs.rs/lexical-parse-float/latest/lexical_parse_float/format/constant.JSON.html
[`XML`]: https://docs.rs/lexical-parse-float/latest/lexical_parse_float/format/constant.XML.html
[`TOML`]: https://docs.rs/lexical-parse-float/latest/lexical_parse_float/format/constant.TOML.html
[`YAML`]: https://docs.rs/lexical-parse-float/latest/lexical_parse_float/format/constant.YAML.html
[`SQLite`]: https://docs.rs/lexical-parse-float/latest/lexical_parse_float/format/constant.SQLITE.html
[`Rust`]: https://docs.rs/lexical-parse-float/latest/lexical_parse_float/format/constant.RUST_LITERAL.html
[`Python`]: https://docs.rs/lexical-parse-float/latest/lexical_parse_float/format/constant.PYTHON_LITERAL.html
[`C#`]: https://docs.rs/lexical-parse-float/latest/lexical_parse_float/format/constant.CSHARP_LITERAL.html
[`FORTRAN`]: https://docs.rs/lexical-parse-float/latest/lexical_parse_float/format/constant.FORTRAN_LITERAL.html
[`COBOL`]: https://docs.rs/lexical-parse-float/latest/lexical_parse_float/format/constant.COBOL_LITERAL.html
"
)]
//!
//! ## Options API
//!
//! The Options API provides high-level options to specify number parsing
//! or writing, options not intrinsically tied to a number format.
//! For example, the Options API provides:
//! - The [`exponent`][OptionsBuilder::exponent] character (defaults to `b'e'`
//!   or `b'^'`, depending on the radix).
//! - The [`decimal point`][OptionsBuilder::decimal_point] character (defaults
//!   to `b'.'`).
//! - Custom [`NaN`][f64::NAN] and [`Infinity`][f64::INFINITY] string
//!   [`representations`][Options::nan_string].
//!
//!
//! In addition, pre-defined constants for each category of options may
//! be found in their respective modules, for example, [`JSON`][`JSON-OPTS`].
//!
//! [`JSON-OPTS`]: options::JSON
//!
//! ## Examples
//!
//! An example of creating your own options to parse European-style
//! numbers (which use commas as decimal points, and periods as digit
//! separators) is as follows:
//!
//! ```
//! # #[cfg(feature = "format")] {
//! # use core::num;
//! use lexical_parse_float::{format, FromLexicalWithOptions, NumberFormatBuilder, Options};
//!
//! // This creates a format to parse a European-style float number.
//! // The decimal point is a comma, and the digit separators (optional)
//! // are periods.
//! const EUROPEAN: u128 = NumberFormatBuilder::new()
//!     .digit_separator(num::NonZeroU8::new(b'.'))
//!     .build_strict();
//! const COMMA_OPTIONS: Options = Options::builder()
//!     .decimal_point(b',')
//!     .build_strict();
//! assert_eq!(
//!     f32::from_lexical_with_options::<EUROPEAN>(b"300,10", &COMMA_OPTIONS),
//!     Ok(300.10)
//! );
//!
//! // Another example, using a pre-defined constant for JSON.
//! const JSON: u128 = format::JSON;
//! const JSON_OPTIONS: Options = Options::new();
//! assert_eq!(
//!     f32::from_lexical_with_options::<JSON>(b"0e1", &JSON_OPTIONS),
//!     Ok(0.0)
//! );
//! assert_eq!(
//!     f32::from_lexical_with_options::<JSON>(b"1E+2", &JSON_OPTIONS),
//!     Ok(100.0)
//! );
//! # }
//! ```
//!
//! # Higher-Level APIs
//!
//! If you would like an API that supports multiple numeric conversions rather
//! than just writing integers, use [`lexical`] or [`lexical-core`] instead.
//!
//! [`lexical`]: https://crates.io/crates/lexical
//! [`lexical-core`]: https://crates.io/crates/lexical-core
//!
//! # Version Support
//!
//! The minimum, standard, required version is [`1.63.0`][`rust-1.63.0`], for
//! const generic support. Older versions of lexical support older Rust
//! versions.
//!
//! # Algorithm
//!
//! The default implementations are highly optimized both for simple
//! strings, as well as input with large numbers of digits. In order to
//! keep performance optimal for simple strings, we avoid overly branching
//! to minimize the number of branches (and therefore optimization checks).
//! Most of the branches in the code are resolved at compile-time, and
//! the resulting ASM as well as comprehensive benchmarks are monitored
//! to ensure there are no regressions.
//!
//! For simple floats, we use an optimized digit parser with multiple-digit
//! optimizations (parsing 8 digits in 3 multiplication instructions),
//! and then use machine floats to create an exact representation with
//! high throughput. In more complex cases, we use the Eisel-Lemire
//! algorithm, described in "Number Parsing at a Gigabyte per Second",
//! available online [here](https://arxiv.org/abs/2101.11408). The
//! Eisel-Lemire algorithm creates an extended representation using a
//! 128-bit (or a fallback 192-bit representation) of the significant
//! digits of the float, scaled to the proper exponent using pre-computed
//! powers-of-5.
//!
//! If the Eisel-Lemire algorithm is unable to unambiguously round the float,
//! we fallback to using optimized, big-integer algorithms, which are
//! described in [Algorithm Approach](#algorithm-approach) below.
//!
//! ## Machine Float-Only Algorithm
//!
//! We also support an algorithm that uses only machine floats for the
//! fast-path algorithm, however, this may be slower for floats with large
//! exponents since it uses an iterative algorithm. A code sample
//! using this is:
//!
//! ```rust
//! use lexical_parse_float::Options;
//! use lexical_parse_float::format::STANDARD;
//! use lexical_parse_float::parse::ParseFloat;
//!
//! const OPTIONS: Options = Options::new();
//! let result = f64::fast_path_complete::<{ STANDARD }>(b"1.34000", &OPTIONS);
//! assert_eq!(result, Ok(1.34000));
//! ```
//!
//! # Design
//!
//! - [Algorithm Approach](https://github.com/Alexhuszagh/rust-lexical/blob/main/lexical-parse-float/docs/Algorithm.md)
//! - [Benchmarks](https://github.com/Alexhuszagh/rust-lexical/blob/main/lexical-parse-float/docs/Benchmarks.md)
//! - [Comprehensive Benchmarks](https://github.com/Alexhuszagh/lexical-benchmarks)
//! - [Big Integer Implementation](https://github.com/Alexhuszagh/rust-lexical/blob/main/lexical-parse-float/docs/BigInteger.md)
//!
//! # Safety Guarantees
//!
//! <div class="warning info-warning">
//! <style>
//! .info-warning::before {
//!   color: #87CEFAb0 !important;
//! }
//! .info-warning {
//!   border-left: 2px solid #87CEFAb0 !important;
//! }
//! </style>
//!
//! This module uses some unsafe code to achieve accept acceptable performance.
//! The safety guarantees and logic are described below.
//!
//! </div>
//!
//! The primary use of unsafe code is in the big integer implementation, which
//! for performance reasons requires unchecked indexing at certain points, where
//! rust cannot elide the index check. The use of unsafe code can be found in
//! the calculation of the [hi] bits, however, every invocation requires the
//! buffer to be of sufficient [length][longbits]. The other major source is the
//! implementation of methods such as [push_unchecked], however, the safety
//! invariants for each caller to create a safe API are documented and has
//! similar safety guarantees to a regular vector. All other invocations of
//! unsafe code are indexing a buffer where the index is proven to be within
//! bounds within a few lines of code of the unsafe index.
//!
//! [hi]: <https://github.com/Alexhuszagh/rust-lexical/blob/15d4c8c92d70b1fb9bd6d33f582ffe27e0e74f99/lexical-parse-float/src/bigint.rs#L266>
//! [longbits]: <https://github.com/Alexhuszagh/rust-lexical/blob/15d4c8c92d70b1fb9bd6d33f582ffe27e0e74f99/lexical-parse-float/src/bigint.rs#L550-L557>
//! [push_unchecked]: <https://github.com/Alexhuszagh/rust-lexical/blob/15d4c8c92d70b1fb9bd6d33f582ffe27e0e74f99/lexical-parse-float/src/bigint.rs#L377-L386>
//! [`rust-1.63.0`]: https://blog.rust-lang.org/2022/08/11/Rust-1.63.0.html

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

#[macro_use]
pub mod shared;

pub mod bellerophon;
pub mod bigint;
pub mod binary;
pub mod float;
pub mod fpu;
pub mod lemire;
pub mod libm;
pub mod limits;
pub mod mask;
pub mod number;
pub mod options;
pub mod parse;
pub mod slow;
pub mod table;

mod api;
mod table_bellerophon_decimal;
mod table_bellerophon_radix;
mod table_binary;
mod table_decimal;
mod table_large;
mod table_lemire;
mod table_radix;
mod table_small;

#[macro_use(parse_sign)]
extern crate lexical_parse_integer;

// Re-exports
#[cfg(feature = "f16")]
pub use lexical_util::bf16::bf16;
pub use lexical_util::error::Error;
#[cfg(feature = "f16")]
pub use lexical_util::f16::f16;
pub use lexical_util::format::{self, NumberFormat, NumberFormatBuilder};
pub use lexical_util::options::ParseOptions;
pub use lexical_util::result::Result;

pub use self::api::{FromLexical, FromLexicalWithOptions};
#[doc(inline)]
pub use self::options::{Options, OptionsBuilder};
