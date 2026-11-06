//! Fast lexical string-to-integer conversion routines.
//!
//! This contains high-performance methods to parse integers from bytes.
//! Using [`from_lexical`] is analogous to [`parse`][`core-parse`],
//! while enabling parsing from bytes as well as [`str`].
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
//! use lexical_parse_integer::{Error, FromLexical};
//!
//! let value = u64::from_lexical("1234".as_bytes());
//! assert_eq!(value, Ok(1234));
//!
//! let value = u64::from_lexical("18446744073709551616".as_bytes());
//! assert_eq!(value, Err(Error::Overflow(19)));
//!
//! let value = u64::from_lexical("1234 }, {\"Key\", \"Value\"}}".as_bytes());
//! assert_eq!(value, Err(Error::InvalidDigit(4)));
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
//! use lexical_parse_integer::{Error, FromLexical};
//!
//! let value = u64::from_lexical_partial("1234 }, {\"Key\", \"Value\"}}".as_bytes());
//! assert_eq!(value, Ok((1234, 4)));
//!
//! let value = u64::from_lexical_partial("18446744073709551616 }, {\"Key\", \"Value\"}}".as_bytes());
//! assert_eq!(value, Err(Error::Overflow(19)));
//! ```
//!
//! # Options/Formatting API
//!
//! Each integer parser contains extensive formatting control through
//! [`mod@format`], particularly digit [`separator`] support (that is,
//! integers such as `1_2__3`). For options, we have custom formats
//! optimized for both [`small`] and [`large`] integers.
//!
//! [`small`]: crate::options::SMALL_NUMBERS
//! [`large`]: crate::options::LARGE_NUMBERS
//! [`separator`]: NumberFormat::digit_separator
//!
//! To optimize for smaller integers at the expense of performance of larger
//! ones, you can use [`OptionsBuilder::no_multi_digit`] (defaults to [`true`]).
//!
//! ```rust
//! # use core::{num, str};
//! use lexical_parse_integer::{options, NumberFormatBuilder, FromLexicalWithOptions};
//!
//! const FORMAT: u128 = NumberFormatBuilder::new().build_strict();
//!
//! // a bit faster
//! let value = u64::from_lexical_with_options::<FORMAT>(b"12", &options::SMALL_NUMBERS);
//! assert_eq!(value, Ok(12));
//!
//! // a lot slower
//! let value = u64::from_lexical_with_options::<FORMAT>(b"18446744073709551615", &options::SMALL_NUMBERS);
//! assert_eq!(value, Ok(0xffffffffffffffff));
//! ```
//!
//! # Features
//!
//! * `format` - Add support for parsing custom integer formats.
//! * `power-of-two` - Add support for parsing power-of-two integer strings.
//! * `radix` - Add support for strings of any radix.
//! * `compact` - Reduce code size at the cost of performance.
//! * `std` (Default) - Disable to allow use in a [`no_std`] environment.
//!
//! [`no_std`]: https://docs.rust-embedded.org/book/intro/no-std.html
//!
//! A complete description of supported features includes:
//!
//! #### format
//!
//! Add support custom float formatting specifications. This should be used in
//! conjunction with [`Options`] for extensible integer parsing. This allows
//! changing the use of digit separators, requiring or not allowing signs, and
//! more.
//!
//! ##### JSON
//!
//! For example, in JSON, the following integers are valid or invalid:
//!
//! ```text
//! -1          // valid
//! +1          // invalid
//! 1           // valid
//! ```
//!
//! All of these are valid in our default format (the format of Rust strings),
//! so we must use a custom format to parse JSON strings:
//!
//! ```rust
//! # #[cfg(feature = "format")] {
//! # use core::str;
//! use lexical_parse_integer::{format, Error, FromLexicalWithOptions, Options};
//!
//! const OPTIONS: Options = Options::new();
//! let value = u64::from_lexical_with_options::<{ format::JSON }>("1234".as_bytes(), &OPTIONS);
//! assert_eq!(value, Ok(1234));
//!
//! let value = u64::from_lexical_with_options::<{ format::JSON }>("+1234".as_bytes(), &OPTIONS);
//! assert_eq!(value, Err(Error::InvalidPositiveSign(0)));
//! # }
//! ```
//!
//! ##### Custom Format
//!
//! An example of building a custom format to with digit separator support is:
//!
//! ```rust
//! # #[cfg(all(feature = "format", feature = "power-of-two"))] {
//! # use core::{num, str};
//! use lexical_parse_integer::{NumberFormatBuilder, Options, FromLexicalWithOptions};
//!
//! const FORMAT: u128 = NumberFormatBuilder::new()
//!     // require that a `+` or `-` preceeds the number
//!     .required_mantissa_sign(true)
//!     // allow internal digit separators, that is, a special character between digits
//!     .integer_internal_digit_separator(true)
//!     // use `_` as the digit separator
//!     .digit_separator(num::NonZeroU8::new(b'_'))
//!     // allow an optional `0d` prefix to the number
//!     .base_prefix(num::NonZeroU8::new(b'd'))
//!     // build the number format, panicking on error
//!     .build_strict();
//! const OPTIONS: Options = Options::new();
//!
//! let value = u64::from_lexical_with_options::<FORMAT>("+12_3_4".as_bytes(), &OPTIONS);
//! assert_eq!(value, Ok(1234));
//!
//! let value = u64::from_lexical_with_options::<FORMAT>("+0d12_3_4".as_bytes(), &OPTIONS);
//! assert_eq!(value, Ok(1234));
//! # }
//! ```
//!
//! For a list of all supported fields, see [Parse Integer
//! Fields][NumberFormatBuilder#parse-integer-fields].
//!
//! Enabling the [`format`](crate#format) API significantly increases compile
//! times, however, it enables a large amount of customization in how integers
//! are parsed.
//!
//! #### power-of-two
//!
//! Enable parsing numbers that are powers of two, that is, `2`, `4`, `8`, `16`,
//! and `32`.
//!
//! ```rust
//! # #[no_std]
//! # #[cfg(feature = "power-of-two")] {
//! # use core::str;
//! use lexical_parse_integer::{FromLexicalWithOptions, NumberFormatBuilder, Options};
//!
//! const BINARY: u128 = NumberFormatBuilder::binary();
//! const OPTIONS: Options = Options::new();
//! let value = u64::from_lexical_with_options::<BINARY>("10011010010".as_bytes(), &OPTIONS);
//! assert_eq!(value, Ok(1234));
//! # }
//! ```
//!
//! #### radix
//!
//! Enable parsing numbers using all radixes from `2` to `36`. This requires
//! more static storage than [`power-of-two`][crate#power-of-two], and increases
//! compile times, but can be quite useful for esoteric programming languages
//! which use duodecimal integers.
//!
//! ```rust
//! # #[no_std]
//! # #[cfg(feature = "radix")] {
//! # use core::str;
//! use lexical_parse_integer::{FromLexicalWithOptions, NumberFormatBuilder, Options};
//!
//! const BINARY: u128 = NumberFormatBuilder::from_radix(12);
//! const OPTIONS: Options = Options::new();
//! let value = u64::from_lexical_with_options::<BINARY>("86A".as_bytes(), &OPTIONS);
//! assert_eq!(value, Ok(1234));
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
//! #### std
//!
//! Enable use of the standard library. Currently, the standard library
//! is not used, and may be disabled without any change in functionality
//! on stable.
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
//! the resulting ASM is monitored to ensure there are no regressions. For
//! larger strings, a limited number of optimization checks are included
//! to try faster, multi-digit parsing algorithms. For 32-bit integers,
//! we try to parse 4 digits at a time, and for 64-bit and larger integers,
//! we try to parse 8 digits at a time. Attempting both checks leads to
//! significant performance penalties for simple strings, so only 1
//! optimization is used at at a time.
//!
//! In addition, a compact, fallback algorithm uses a naive, simple
//! algorithm, parsing only a single digit at a time. This avoid any
//! unnecessary branching and produces smaller binaries, but comes
//! at a significant performance penalty for integers with more digits.
//!
//! # Design
//!
//! - [Algorithm Approach](https://github.com/Alexhuszagh/rust-lexical/blob/main/lexical-parse-integer/docs/Algorithm.md)
//! - [Benchmarks](https://github.com/Alexhuszagh/rust-lexical/blob/main/lexical-parse-integer/docs/Benchmarks.md)
//! - [Comprehensive Benchmarks](https://github.com/Alexhuszagh/lexical-benchmarks)
//!
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

pub mod algorithm;
pub mod options;
pub mod parse;

mod api;

// Re-exports
pub use lexical_util::error::Error;
pub use lexical_util::format::{self, NumberFormat, NumberFormatBuilder};
pub use lexical_util::options::ParseOptions;
pub use lexical_util::result::Result;

pub use self::api::{FromLexical, FromLexicalWithOptions};
#[doc(inline)]
pub use self::options::{Options, OptionsBuilder};
