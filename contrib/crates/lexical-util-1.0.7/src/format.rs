//! The creation and processing of number format packed structs.
//!
//! This creates the format specification as a 128-bit packed struct,
//! represented as a [`u128`] through the [`NumberFormatBuilder`] and
//! with helpers to access format options through [`NumberFormat`].
//!
//! This has a consistent API whether or not the [`format`][crate#features]
//! feature is enabled, however, most functionality will be disabled if the
//! feature is not enabled.
//!
//! # Creating Formats
//!
//! Formats can be created through [`NumberFormatBuilder`]:
//!
//! ```rust
//! # #[cfg(feature = "format")] {
//! use core::num;
//!
//! use lexical_util::{NumberFormat, NumberFormatBuilder};
//!
//! // create the format for literal Rustt floats
//! const RUST: u128 = NumberFormatBuilder::new()
//!    .digit_separator(num::NonZeroU8::new(b'_'))
//!    .required_digits(true)
//!    .no_positive_mantissa_sign(true)
//!    .no_special(true)
//!    .internal_digit_separator(true)
//!    .trailing_digit_separator(true)
//!    .consecutive_digit_separator(true)
//!    .build_strict();
//!
//! // then, access the formats's properties
//! let format = NumberFormat::<{ RUST }> {};
//! assert!(format.no_positive_mantissa_sign());
//! assert!(format.no_special());
//! assert!(format.internal_digit_separator());
//! assert!(format.trailing_digit_separator());
//! assert!(format.consecutive_digit_separator());
//! assert!(!format.no_exponent_notation());
//! # }
//! ```
//!
//! These pre-built formats can then be used for [`FromLexicalWithOptions`]
//! and [`ToLexicalWithOptions`] conversions.
//!
//! # Pre-Defined Formats
//!
//! These are the pre-defined formats for parsing numbers from various
//! programming, markup, and data languages.
//!
//! - [`STANDARD`]: Standard number format. This is identical to the Rust string
//!   format.
#![cfg_attr(
    feature = "format",
    doc = "
- [`RUST_LITERAL`]: Number format for a [`Rust`] literal floating-point number.
- [`RUST_STRING`]: Number format to parse a [`Rust`] float from string.
- [`PYTHON_LITERAL`]: Number format for a [`Python`] literal floating-point number.
- [`PYTHON_STRING`]: Number format to parse a [`Python`] float from string.
- [`PYTHON3_LITERAL`]: Number format for a [`Python3`] literal floating-point number.
- [`PYTHON3_STRING`]: Number format to parse a [`Python3`] float from string.
- [`PYTHON36_LITERAL`]: Number format for a [`Python3.6`] or higher literal floating-point number.
- [`PYTHON35_LITERAL`]: Number format for a [`Python3.5`] or lower literal floating-point number.
- [`PYTHON2_LITERAL`]: Number format for a [`Python2`] literal floating-point number.
- [`PYTHON2_STRING`]: Number format to parse a [`Python2`] float from string.
- [`CXX_LITERAL`]: Number format for a [`C++`] literal floating-point number.
- [`CXX_STRING`]: Number format to parse a [`C++`] float from string.
"
)]
#![cfg_attr(
    all(feature = "format", feature = "power-of-two"),
    doc = "
- [`CXX_HEX_LITERAL`]: Number format for a [`C++`] literal hexadecimal floating-point number.
- [`CXX_HEX_STRING`]: Number format to parse a [`C++`] hexadecimal float from string.
"
)]
#![cfg_attr(
    feature = "format",
    doc = "
- [`CXX20_LITERAL`]: Number format for a [`C++20`] literal floating-point number.
- [`CXX20_STRING`]: Number format for a [`C++20`] string floating-point number.
"
)]
#![cfg_attr(
    all(feature = "format", feature = "power-of-two"),
    doc = "
- [`CXX20_HEX_LITERAL`]: Number format for a [`C++20`] literal hexadecimal floating-point number.
- [`CXX20_HEX_STRING`]: Number format for a [`C++20`] string hexadecimal floating-point number.
"
)]
#![cfg_attr(
    feature = "format",
    doc = "
- [`CXX17_LITERAL`]: Number format for a [`C++17`] literal floating-point number.
- [`CXX17_STRING`]: Number format for a [`C++17`] string floating-point number.
"
)]
#![cfg_attr(
    all(feature = "format", feature = "power-of-two"),
    doc = "
- [`CXX17_HEX_LITERAL`]: Number format for a [`C++17`] literal hexadecimal floating-point number.
- [`CXX17_HEX_STRING`]: Number format for a [`C++17`] string hexadecimal floating-point number.
"
)]
#![cfg_attr(
    feature = "format",
    doc = "
- [`CXX14_LITERAL`]: Number format for a [`C++14`] literal floating-point number.
- [`CXX14_STRING`]: Number format for a [`C++14`] string floating-point number.
"
)]
#![cfg_attr(
    all(feature = "format", feature = "power-of-two"),
    doc = "
- [`CXX14_HEX_STRING`]: Number format for a [`C++14`] string hexadecimal floating-point number.
"
)]
#![cfg_attr(
    feature = "format",
    doc = "
- [`CXX11_LITERAL`]: Number format for a [`C++11`] literal floating-point number.
- [`CXX11_STRING`]: Number format for a [`C++11`] string floating-point number.
"
)]
#![cfg_attr(
    all(feature = "format", feature = "power-of-two"),
    doc = "
- [`CXX11_HEX_STRING`]: Number format for a [`C++11`] string hexadecimal floating-point number.
"
)]
#![cfg_attr(
    feature = "format",
    doc = "
- [`CXX03_LITERAL`]: Number format for a [`C++03`] literal floating-point number.
- [`CXX03_STRING`]: Number format for a [`C++03`] string floating-point number.
- [`CXX98_LITERAL`]: Number format for a [`C++98`] literal floating-point number.
- [`CXX98_STRING`]: Number format for a [`C++98`] string floating-point number.
- [`C_LITERAL`]: Number format for a [`C`] literal floating-point number.
- [`C_STRING`]: Number format for a [`C`] string floating-point number.
"
)]
#![cfg_attr(
    all(feature = "format", feature = "power-of-two"),
    doc = "
- [`C_HEX_LITERAL`]: Number format for a [`C`] literal hexadecimal floating-point number.
- [`C_HEX_STRING`]: Number format for a [`C`] string hexadecimal floating-point number.
"
)]
#![cfg_attr(
    feature = "format",
    doc = "
- [`C18_LITERAL`]: Number format for a [`C18`] literal floating-point number.
- [`C18_STRING`]: Number format for a [`C18`] string floating-point number.
"
)]
#![cfg_attr(
    all(feature = "format", feature = "power-of-two"),
    doc = "
- [`C18_HEX_LITERAL`]: Number format for a [`C18`] literal hexadecimal floating-point number.
- [`C18_HEX_STRING`]: Number format for a [`C18`] string hexadecimal floating-point number.
"
)]
#![cfg_attr(
    feature = "format",
    doc = "
- [`C11_LITERAL`]: Number format for a [`C11`] literal floating-point number.
- [`C11_STRING`]: Number format for a [`C11`] string floating-point number.
"
)]
#![cfg_attr(
    all(feature = "format", feature = "power-of-two"),
    doc = "
- [`C11_HEX_LITERAL`]: Number format for a [`C11`] literal hexadecimal floating-point number.
- [`C11_HEX_STRING`]: Number format for a [`C11`] string hexadecimal floating-point number.
"
)]
#![cfg_attr(
    feature = "format",
    doc = "
- [`C99_LITERAL`]: Number format for a [`C99`] literal floating-point number.
- [`C99_STRING`]: Number format for a [`C99`] string floating-point number.
"
)]
#![cfg_attr(
    all(feature = "format", feature = "power-of-two"),
    doc = "
- [`C99_HEX_LITERAL`]: Number format for a [`C99`] literal hexadecimal floating-point number.
- [`C99_HEX_STRING`]: Number format for a [`C99`] string hexadecimal floating-point number.
"
)]
#![cfg_attr(
    feature = "format",
    doc = "
- [`C90_LITERAL`]: Number format for a [`C90`] literal floating-point number.
- [`C90_STRING`]: Number format for a [`C90`] string floating-point number.
"
)]
#![cfg_attr(
    all(feature = "format", feature = "power-of-two"),
    doc = "
- [`C90_HEX_STRING`]: Number format for a [`C90`] string hexadecimal floating-point number.
"
)]
#![cfg_attr(
    feature = "format",
    doc = "
- [`C89_LITERAL`]: Number format for a [`C89`] literal floating-point number.
- [`C89_STRING`]: Number format for a [`C89`] string floating-point number.
"
)]
#![cfg_attr(
    all(feature = "format", feature = "power-of-two"),
    doc = "
- [`C89_HEX_STRING`]: Number format for a [`C89`] string hexadecimal floating-point number.
"
)]
#![cfg_attr(
    feature = "format",
    doc = "
- [`RUBY_LITERAL`]: Number format for a [`Ruby`] literal floating-point number.
"
)]
#![cfg_attr(
    all(feature = "format", feature = "power-of-two"),
    doc = "
- [`RUBY_OCTAL_LITERAL`]: Number format for an octal [`Ruby`] literal floating-point number.
"
)]
#![cfg_attr(
    feature = "format",
    doc = "
- [`RUBY_STRING`]: Number format to parse a [`Ruby`] float from string.
- [`SWIFT_LITERAL`]: Number format for a [`Swift`] literal floating-point number.
- [`SWIFT_STRING`]: Number format to parse a [`Swift`] float from string.
- [`GO_LITERAL`]: Number format for a [`Golang`] literal floating-point number.
- [`GO_STRING`]: Number format to parse a [`Golang`] float from string.
- [`HASKELL_LITERAL`]: Number format for a [`Haskell`] literal floating-point number.
- [`HASKELL_STRING`]: Number format to parse a [`Haskell`] float from string.
- [`JAVASCRIPT_LITERAL`]: Number format for a [`Javascript`] literal floating-point number.
- [`JAVASCRIPT_STRING`]: Number format to parse a [`Javascript`] float from string.
- [`PERL_LITERAL`]: Number format for a [`Perl`] literal floating-point number.
- [`PERL_STRING`]: Number format to parse a [`Perl`] float from string.
- [`PHP_LITERAL`]: Number format for a [`PHP`] literal floating-point number.
- [`PHP_STRING`]: Number format to parse a [`PHP`] float from string.
- [`JAVA_LITERAL`]: Number format for a [`Java`] literal floating-point number.
- [`JAVA_STRING`]: Number format to parse a [`Java`] float from string.
- [`R_LITERAL`]: Number format for an [`R`] literal floating-point number.
- [`R_STRING`]: Number format to parse an [`R`] float from string.
- [`KOTLIN_LITERAL`]: Number format for a [`Kotlin`] literal floating-point number.
- [`KOTLIN_STRING`]: Number format to parse a [`Kotlin`] float from string.
- [`JULIA_LITERAL`]: Number format for a [`Julia`] literal floating-point number.
- [`JULIA_STRING`]: Number format to parse a [`Julia`] float from string.
"
)]
#![cfg_attr(
    all(feature = "format", feature = "power-of-two"),
    doc = "
- [`JULIA_HEX_LITERAL`]: Number format for a [`Julia`] literal floating-point number.
- [`JULIA_HEX_STRING`]: Number format to parse a [`Julia`] float from string.
"
)]
#![cfg_attr(
    feature = "format",
    doc = "
- [`CSHARP_LITERAL`]: Number format for a [`C#`] literal floating-point number.
- [`CSHARP_STRING`]: Number format to parse a [`C#`] float from string.
- [`CSHARP7_LITERAL`]: Number format for a [`C#7`] literal floating-point number.
- [`CSHARP7_STRING`]: Number format to parse a [`C#7`] float from string.
- [`CSHARP6_LITERAL`]: Number format for a [`C#6`] literal floating-point number.
- [`CSHARP6_STRING`]: Number format to parse a [`C#6`] float from string.
- [`CSHARP5_LITERAL`]: Number format for a [`C#5`] literal floating-point number.
- [`CSHARP5_STRING`]: Number format to parse a [`C#5`] float from string.
- [`CSHARP4_LITERAL`]: Number format for a [`C#4`] literal floating-point number.
- [`CSHARP4_STRING`]: Number format to parse a [`C#4`] float from string.
- [`CSHARP3_LITERAL`]: Number format for a [`C#3`] literal floating-point number.
- [`CSHARP3_STRING`]: Number format to parse a [`C#3`] float from string.
- [`CSHARP2_LITERAL`]: Number format for a [`C#2`] literal floating-point number.
- [`CSHARP2_STRING`]: Number format to parse a [`C#2`] float from string.
- [`CSHARP1_LITERAL`]: Number format for a [`C#1`] literal floating-point number.
- [`CSHARP1_STRING`]: Number format to parse a [`C#1`] float from string.
- [`KAWA_LITERAL`]: Number format for a [`Kawa`] literal floating-point number.
- [`KAWA_STRING`]: Number format to parse a [`Kawa`] float from string.
- [`GAMBITC_LITERAL`]: Number format for a [`Gambit-C`] literal floating-point number.
- [`GAMBITC_STRING`]: Number format to parse a [`Gambit-C`] float from string.
- [`GUILE_LITERAL`]: Number format for a [`Guile`] literal floating-point number.
- [`GUILE_STRING`]: Number format to parse a [`Guile`] float from string.
- [`CLOJURE_LITERAL`]: Number format for a [`Clojure`] literal floating-point number.
- [`CLOJURE_STRING`]: Number format to parse a [`Clojure`] float from string.
- [`ERLANG_LITERAL`]: Number format for an [`Erlang`] literal floating-point number.
- [`ERLANG_STRING`]: Number format to parse an [`Erlang`] float from string.
- [`ELM_LITERAL`]: Number format for an [`Elm`] literal floating-point number.
- [`ELM_STRING`]: Number format to parse an [`Elm`] float from string.
- [`SCALA_LITERAL`]: Number format for a [`Scala`] literal floating-point number.
- [`SCALA_STRING`]: Number format to parse a [`Scala`] float from string.
- [`ELIXIR_LITERAL`]: Number format for an [`Elixir`] literal floating-point number.
- [`ELIXIR_STRING`]: Number format to parse an [`Elixir`] float from string.
- [`FORTRAN_LITERAL`]: Number format for a [`FORTRAN`] literal floating-point number.
- [`FORTRAN_STRING`]: Number format to parse a [`FORTRAN`] float from string.
- [`D_LITERAL`]: Number format for a [`D`] literal floating-point number.
- [`D_STRING`]: Number format to parse a [`D`] float from string.
- [`COFFEESCRIPT_LITERAL`]: Number format for a [`Coffeescript`] literal floating-point number.
- [`COFFEESCRIPT_STRING`]: Number format to parse a [`Coffeescript`] float from string.
- [`COBOL_LITERAL`]: Number format for a [`Cobol`] literal floating-point number.
- [`COBOL_STRING`]: Number format to parse a [`Cobol`] float from string.
- [`FSHARP_LITERAL`]: Number format for a [`F#`] literal floating-point number.
- [`FSHARP_STRING`]: Number format to parse a [`F#`] float from string.
- [`VB_LITERAL`]: Number format for a [`Visual Basic`] literal floating-point number.
- [`VB_STRING`]: Number format to parse a [`Visual Basic`] float from string.
- [`OCAML_LITERAL`]: Number format for an [`OCaml`] literal floating-point number.
- [`OCAML_STRING`]: Number format to parse an [`OCaml`] float from string.
- [`OBJECTIVEC_LITERAL`]: Number format for an [`Objective-C`] literal floating-point number.
- [`OBJECTIVEC_STRING`]: Number format to parse an [`Objective-C`] float from string.
- [`REASONML_LITERAL`]: Number format for a [`ReasonML`] literal floating-point number.
- [`REASONML_STRING`]: Number format to parse a [`ReasonML`] float from string.
- [`OCTAVE_LITERAL`]: Number format for an [`Octave`] literal floating-point number.
- [`OCTAVE_STRING`]: Number format to parse an [`Octave`] float from string.
- [`MATLAB_LITERAL`]: Number format for an [`Matlab`] literal floating-point number.
- [`MATLAB_STRING`]: Number format to parse an [`Matlab`] float from string.
- [`ZIG_LITERAL`]: Number format for a [`Zig`] literal floating-point number.
- [`ZIG_STRING`]: Number format to parse a [`Zig`] float from string.
- [`SAGE_LITERAL`]: Number format for a [`Sage`] literal floating-point number.
- [`SAGE_STRING`]: Number format to parse a [`Sage`] float from string.
- [`JSON`]: Number format for a [`JSON`][`JSON-REF`] literal floating-point number.
- [`TOML`]: Number format for a [`TOML`][`TOML-REF`] literal floating-point number.
- [`YAML`]: Number format for a [`YAML`][`YAML-REF`] literal floating-point number.
- [`XML`]: Number format for an [`XML`][`XML-REF`] literal floating-point number.
- [`SQLITE`]: Number format for a [`SQLite`] literal floating-point number.
- [`POSTGRESQL`]: Number format for a [`PostgreSQL`] literal floating-point number.
- [`MYSQL`]: Number format for a [`MySQL`] literal floating-point number.
- [`MONGODB`]: Number format for a [`MongoDB`] literal floating-point number.
"
)]
//!
#![cfg_attr(
    any(feature = "parse-floats", feature = "parse-integers"),
    doc = "[`FromLexicalWithOptions`]: crate::from_lexical_with_options"
)]
#![cfg_attr(
    not(any(feature = "parse-floats", feature = "parse-integers")),
    doc = "[`FromLexicalWithOptions`]: https://github.com/Alexhuszagh/rust-lexical/blob/c6c5052/lexical-util/src/api.rs#L45"
)]
#![cfg_attr(
    any(feature = "write-floats", feature = "write-integers"),
    doc = "[`ToLexicalWithOptions`]: crate::to_lexical_with_options"
)]
#![cfg_attr(
    not(any(feature = "write-floats", feature = "write-integers")),
    doc = "[`ToLexicalWithOptions`]: https://github.com/Alexhuszagh/rust-lexical/blob/c6c5052/lexical-util/src/api.rs#L151"
)]
//!
//! # Low-Level Schema
//!
//! This describes how to directly get and set flags from the [`NumberFormat`]
//! packed struct. It is not recommended to use these directly, but for example,
//! the following can be done:
//!
//! ```rust
//! # #[cfg(feature = "format")] {
//! use lexical_util::format;
//!
//! assert_eq!(
//!     format::NumberFormatBuilder::new()
//!         .required_integer_digits(true)
//!         .build_strict(),
//!     format::STANDARD | format::REQUIRED_INTEGER_DIGITS
//! );
//! # }
//! ```
//!
//! ## Syntax Flags
//!
//! Bitflags to get and set syntax flags for the format packed struct.
//!
//! - [`REQUIRED_INTEGER_DIGITS`]: If digits are required before the decimal
//!   point.
//! - [`REQUIRED_FRACTION_DIGITS`]: If digits are required after the decimal
//!   point.
//! - [`REQUIRED_EXPONENT_DIGITS`]: If digits are required after the exponent
//!   character.
//! - [`REQUIRED_MANTISSA_DIGITS`]: If significant digits are required.
//! - [`REQUIRED_DIGITS`]: If at least 1 digit in the number is required.
//! - [`NO_POSITIVE_MANTISSA_SIGN`]: If a positive sign before the mantissa is
//!   not allowed.
//! - [`REQUIRED_MANTISSA_SIGN`]: If a sign symbol before the mantissa is
//!   required.
//! - [`NO_EXPONENT_NOTATION`]: If exponent notation is not allowed.
//! - [`NO_POSITIVE_EXPONENT_SIGN`]: If a positive sign before the exponent is
//!   not allowed.
//! - [`REQUIRED_EXPONENT_SIGN`]: If a sign symbol before the exponent is
//!   required.
//! - [`NO_EXPONENT_WITHOUT_FRACTION`]: If an exponent without fraction is not
//!   allowed.
//! - [`NO_SPECIAL`]: If special (non-finite) values are not allowed.
//! - [`CASE_SENSITIVE_SPECIAL`]: If special (non-finite) values are
//!   case-sensitive.
//! - [`NO_INTEGER_LEADING_ZEROS`]: If leading zeros before an integer are not
//!   allowed.
//! - [`NO_FLOAT_LEADING_ZEROS`]: If leading zeros before a float are not
//!   allowed.
//! - [`REQUIRED_EXPONENT_NOTATION`]: If exponent notation is required.
//! - [`CASE_SENSITIVE_EXPONENT`]: If exponent characters are case-sensitive.
//! - [`CASE_SENSITIVE_BASE_PREFIX`]: If base prefixes are case-sensitive.
//! - [`CASE_SENSITIVE_BASE_SUFFIX`]: If base suffixes are case-sensitive.
//!
//! [`REQUIRED_INTEGER_DIGITS`]: NumberFormat::REQUIRED_INTEGER_DIGITS
//! [`REQUIRED_FRACTION_DIGITS`]: NumberFormat::REQUIRED_FRACTION_DIGITS
//! [`REQUIRED_EXPONENT_DIGITS`]: NumberFormat::REQUIRED_EXPONENT_DIGITS
//! [`REQUIRED_MANTISSA_DIGITS`]: NumberFormat::REQUIRED_MANTISSA_DIGITS
//! [`REQUIRED_DIGITS`]: NumberFormat::REQUIRED_DIGITS
//! [`NO_POSITIVE_MANTISSA_SIGN`]: NumberFormat::NO_POSITIVE_MANTISSA_SIGN
//! [`REQUIRED_MANTISSA_SIGN`]: NumberFormat::REQUIRED_MANTISSA_SIGN
//! [`NO_EXPONENT_NOTATION`]: NumberFormat::NO_EXPONENT_NOTATION
//! [`NO_POSITIVE_EXPONENT_SIGN`]: NumberFormat::NO_POSITIVE_EXPONENT_SIGN
//! [`REQUIRED_EXPONENT_SIGN`]: NumberFormat::REQUIRED_EXPONENT_SIGN
//! [`NO_EXPONENT_WITHOUT_FRACTION`]: NumberFormat::NO_EXPONENT_WITHOUT_FRACTION
//! [`NO_SPECIAL`]: NumberFormat::NO_SPECIAL
//! [`CASE_SENSITIVE_SPECIAL`]: NumberFormat::CASE_SENSITIVE_SPECIAL
//! [`NO_INTEGER_LEADING_ZEROS`]: NumberFormat::NO_INTEGER_LEADING_ZEROS
//! [`NO_FLOAT_LEADING_ZEROS`]: NumberFormat::NO_FLOAT_LEADING_ZEROS
//! [`REQUIRED_EXPONENT_NOTATION`]: NumberFormat::REQUIRED_EXPONENT_NOTATION
//! [`CASE_SENSITIVE_EXPONENT`]: NumberFormat::CASE_SENSITIVE_EXPONENT
//! [`CASE_SENSITIVE_BASE_PREFIX`]: NumberFormat::CASE_SENSITIVE_BASE_PREFIX
//! [`CASE_SENSITIVE_BASE_SUFFIX`]: NumberFormat::CASE_SENSITIVE_BASE_SUFFIX
//!
//! ## Digit Separator Flags
//!
//! Bitflags to get and set digit separators flags for the format
//! packed struct.
//!
//! - [`INTEGER_INTERNAL_DIGIT_SEPARATOR`]: If digit separators are allowed
//!   between integer digits.
//! - [`FRACTION_INTERNAL_DIGIT_SEPARATOR`]: If digit separators are allowed
//!   between fraction digits.
//! - [`EXPONENT_INTERNAL_DIGIT_SEPARATOR`]: If digit separators are allowed
//!   between exponent digits.
//! - [`INTEGER_LEADING_DIGIT_SEPARATOR`]: If a digit separator is allowed
//!   before any integer digits.
//! - [`FRACTION_LEADING_DIGIT_SEPARATOR`]: If a digit separator is allowed
//!   before any integer digits.
//! - [`EXPONENT_LEADING_DIGIT_SEPARATOR`]: If a digit separator is allowed
//!   before any exponent digits.
//! - [`INTEGER_TRAILING_DIGIT_SEPARATOR`]: If a digit separator is allowed
//!   after any integer digits.
//! - [`FRACTION_TRAILING_DIGIT_SEPARATOR`]: If a digit separator is allowed
//!   after any fraction digits.
//! - [`EXPONENT_TRAILING_DIGIT_SEPARATOR`]: If a digit separator is allowed
//!   after any exponent digits.
//! - [`INTEGER_CONSECUTIVE_DIGIT_SEPARATOR`]: If multiple consecutive integer
//!   digit separators are allowed.
//! - [`FRACTION_CONSECUTIVE_DIGIT_SEPARATOR`]: If multiple consecutive fraction
//!   digit separators are allowed.
//! - [`EXPONENT_CONSECUTIVE_DIGIT_SEPARATOR`]: If multiple consecutive exponent
//!   digit separators are allowed.
//! - [`INTERNAL_DIGIT_SEPARATOR`]: If digit separators are allowed between
//!   digits.
//! - [`LEADING_DIGIT_SEPARATOR`]: Get if a digit separator is allowed before
//!   any digits.
//! - [`TRAILING_DIGIT_SEPARATOR`]: If a digit separator is allowed after any
//!   digits.
//! - [`CONSECUTIVE_DIGIT_SEPARATOR`]: If multiple consecutive digit separators
//!   are allowed.
//! - [`SPECIAL_DIGIT_SEPARATOR`]: If any digit separators are allowed in
//!   special (non-finite) values.
//!
//! [`INTEGER_INTERNAL_DIGIT_SEPARATOR`]: NumberFormat::INTEGER_INTERNAL_DIGIT_SEPARATOR
//! [`FRACTION_INTERNAL_DIGIT_SEPARATOR`]: NumberFormat::FRACTION_INTERNAL_DIGIT_SEPARATOR
//! [`EXPONENT_INTERNAL_DIGIT_SEPARATOR`]: NumberFormat::EXPONENT_INTERNAL_DIGIT_SEPARATOR
//! [`INTEGER_LEADING_DIGIT_SEPARATOR`]: NumberFormat::INTEGER_LEADING_DIGIT_SEPARATOR
//! [`FRACTION_LEADING_DIGIT_SEPARATOR`]: NumberFormat::FRACTION_LEADING_DIGIT_SEPARATOR
//! [`EXPONENT_LEADING_DIGIT_SEPARATOR`]: NumberFormat::EXPONENT_LEADING_DIGIT_SEPARATOR
//! [`INTEGER_TRAILING_DIGIT_SEPARATOR`]: NumberFormat::INTEGER_TRAILING_DIGIT_SEPARATOR
//! [`FRACTION_TRAILING_DIGIT_SEPARATOR`]: NumberFormat::FRACTION_TRAILING_DIGIT_SEPARATOR
//! [`EXPONENT_TRAILING_DIGIT_SEPARATOR`]: NumberFormat::EXPONENT_TRAILING_DIGIT_SEPARATOR
//! [`INTEGER_CONSECUTIVE_DIGIT_SEPARATOR`]: NumberFormat::INTEGER_CONSECUTIVE_DIGIT_SEPARATOR
//! [`FRACTION_CONSECUTIVE_DIGIT_SEPARATOR`]: NumberFormat::FRACTION_CONSECUTIVE_DIGIT_SEPARATOR
//! [`EXPONENT_CONSECUTIVE_DIGIT_SEPARATOR`]: NumberFormat::EXPONENT_CONSECUTIVE_DIGIT_SEPARATOR
//! [`INTERNAL_DIGIT_SEPARATOR`]: NumberFormat::INTERNAL_DIGIT_SEPARATOR
//! [`LEADING_DIGIT_SEPARATOR`]: NumberFormat::LEADING_DIGIT_SEPARATOR
//! [`TRAILING_DIGIT_SEPARATOR`]: NumberFormat::TRAILING_DIGIT_SEPARATOR
//! [`CONSECUTIVE_DIGIT_SEPARATOR`]: NumberFormat::CONSECUTIVE_DIGIT_SEPARATOR
//! [`SPECIAL_DIGIT_SEPARATOR`]: NumberFormat::SPECIAL_DIGIT_SEPARATOR
//!
//! ## Character Shifts and Masks
//!
//! Bitmasks and bit shifts to get and set control characters for the format
//! packed struct.
//!
//! - [`DIGIT_SEPARATOR_SHIFT`]: Shift to convert to and from a digit separator
//!   as a `u8`.
//! - [`DIGIT_SEPARATOR`]: Mask to extract the digit separator character.
//! - [`BASE_PREFIX_SHIFT`]: Shift to convert to and from a base prefix as a
//!   `u8`.
//! - [`BASE_PREFIX`]: Mask to extract the base prefix character.
//! - [`BASE_SUFFIX_SHIFT`]: Shift to convert to and from a base suffix as a
//!   `u8`.
//! - [`BASE_SUFFIX`]: Mask to extract the base suffix character.
//! - [`MANTISSA_RADIX_SHIFT`]: Shift to convert to and from a mantissa radix as
//!   a `u32`.
//! - [`MANTISSA_RADIX`]: Mask to extract the mantissa radix: the radix for the
//!   significant digits.
//! - [`RADIX_SHIFT`]: Alias for [`MANTISSA_RADIX_SHIFT`].
//! - [`RADIX`]: Alias for [`MANTISSA_RADIX`].
//! - [`EXPONENT_BASE_SHIFT`]: Shift to convert to and from an exponent base as
//!   a `u32`.
//! - [`EXPONENT_BASE`]: Mask to extract the exponent base: the base the
//!   exponent is raised to.
//! - [`EXPONENT_RADIX_SHIFT`]: Shift to convert to and from an exponent radix
//!   as a `u32`.
//! - [`EXPONENT_RADIX`]: Mask to extract the exponent radix: the radix for the
//!   exponent digits.
//!
//! [`DIGIT_SEPARATOR_SHIFT`]: DIGIT_SEPARATOR_SHIFT
//! [`DIGIT_SEPARATOR`]: NumberFormat::DIGIT_SEPARATOR
//! [`BASE_PREFIX_SHIFT`]: BASE_PREFIX_SHIFT
//! [`BASE_PREFIX`]: NumberFormat::BASE_PREFIX
//! [`BASE_SUFFIX_SHIFT`]: BASE_SUFFIX_SHIFT
//! [`BASE_SUFFIX`]: NumberFormat::BASE_SUFFIX
//! [`MANTISSA_RADIX_SHIFT`]: MANTISSA_RADIX_SHIFT
//! [`MANTISSA_RADIX`]: NumberFormat::MANTISSA_RADIX
//! [`RADIX_SHIFT`]: RADIX_SHIFT
//! [`RADIX`]: NumberFormat::RADIX
//! [`EXPONENT_BASE_SHIFT`]: EXPONENT_BASE_SHIFT
//! [`EXPONENT_BASE`]: NumberFormat::EXPONENT_BASE
//! [`EXPONENT_RADIX_SHIFT`]: EXPONENT_RADIX_SHIFT
//! [`EXPONENT_RADIX`]: crate::NumberFormat::EXPONENT_RADIX
//!
//! ## Character Functions
//!
//! Functions to get control characters from the format packed struct.
//!
//! - [`digit_separator`]: Extract the digit separator from the format packed
//!   struct.
//! - [`base_prefix`]: Extract the base prefix character from the format packed
//!   struct.
//! - [`base_suffix`]: Extract the base suffix character from the format packed
//!   struct.
//! - [`mantissa_radix`]: Extract the mantissa radix from the format packed
//!   struct.
//! - [`exponent_base`]: Extract the exponent base from the format packed
//!   struct.
//! - [`exponent_radix`]: Extract the exponent radix from the format packed
//!   struct.
//!
//! ## Validators
//!
//! Functions to validate control characters for the format packed struct.
//!
//! - [`is_valid_exponent_flags`]: Determine if the provided exponent flags are
//!   valid.
//! - [`is_valid_digit_separator`]: Determine if the digit separator is valid.
//! - [`is_valid_base_prefix`]: Determine if the base prefix character is valid.
//! - [`is_valid_base_suffix`]: Determine if the base suffix character is valid.
//! - [`is_valid_punctuation`]: Determine if all of the "punctuation" characters
//!   are valid.
//! - [`is_valid_radix`]: Determine if the radix is valid.
//!
//! <!-- References -->
#![cfg_attr(
    feature = "format",
    doc = "
[`Rust`]: https://www.rust-lang.org/
[`Python`]: https://www.python.org/
[`Python3`]: https://www.python.org/
[`Python3.6`]: https://www.python.org/downloads/release/python-360/
[`Python3.5`]: https://www.python.org/downloads/release/python-350/
[`Python2`]: https://www.python.org/downloads/release/python-270/
[`C++`]: https://en.cppreference.com/w/
[`C++20`]: https://en.cppreference.com/w/cpp/20
[`C++17`]: https://en.cppreference.com/w/cpp/17
[`C++14`]: https://en.cppreference.com/w/cpp/14
[`C++11`]: https://en.cppreference.com/w/cpp/11
[`C++03`]: https://en.wikipedia.org/wiki/C%2B%2B03
[`C++98`]: https://en.cppreference.com/w/
[`C`]: https://en.cppreference.com/w/c
[`C18`]: https://en.cppreference.com/w/c/17
[`C11`]: https://en.cppreference.com/w/c/11
[`C99`]: https://en.cppreference.com/w/c/99
[`C90`]: https://en.cppreference.com/w/c
[`C89`]: https://en.cppreference.com/w/c
[`Ruby`]: https://www.ruby-lang.org/en/
[`Swift`]: https://developer.apple.com/swift/
[`Golang`]: https://go.dev/
[`Haskell`]: https://www.haskell.org/
[`Javascript`]: https://developer.mozilla.org/en-US/docs/Web/JavaScript
[`Perl`]: https://www.perl.org/
[`PHP`]: https://www.php.net/
[`Java`]: https://www.java.com/en/
[`R`]: https://www.r-project.org/
[`Kotlin`]: https://kotlinlang.org/
[`Julia`]: https://julialang.org/
[`C#`]: https://learn.microsoft.com/en-us/dotnet/csharp/
[`C#7`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-73
[`C#6`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-60
[`C#5`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-50
[`C#4`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-40
[`C#3`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-30
[`C#2`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-20
[`C#1`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-12-1
[`Kawa`]: https://www.gnu.org/software/kawa/
[`Gambit-C`]: https://gambitscheme.org/
[`Guile`]: https://www.gnu.org/software/guile/
[`Clojure`]: https://clojure.org/
[`Erlang`]: https://www.erlang.org/
[`Elm`]: https://elm-lang.org/
[`Scala`]: https://www.scala-lang.org/
[`Elixir`]: https://elixir-lang.org/
[`FORTRAN`]: https://fortran-lang.org/
[`D`]: https://dlang.org/
[`Coffeescript`]: https://coffeescript.org/
[`Cobol`]: https://www.ibm.com/think/topics/cobol
[`F#`]: https://fsharp.org/
[`Visual Basic`]: https://learn.microsoft.com/en-us/dotnet/visual-basic/
[`OCaml`]: https://ocaml.org/
[`Objective-C`]: https://en.wikipedia.org/wiki/Objective-C
[`ReasonML`]: https://reasonml.github.io/
[`Octave`]: https://octave.org/
[`Matlab`]: https://www.mathworks.com/products/matlab.html
[`Zig`]: https://ziglang.org/
[`Sage`]: https://www.sagemath.org/
[`JSON-REF`]: https://www.json.org/json-en.html
[`TOML-REF`]: https://toml.io/en/
[`YAML-REF`]: https://yaml.org/
[`XML-REF`]: https://en.wikipedia.org/wiki/XML
[`SQLite`]: https://www.sqlite.org/
[`PostgreSQL`]: https://www.postgresql.org/
[`MySQL`]: https://www.mysql.com/
[`MongoDB`]: https://www.mongodb.com/
"
)]

use crate::error::Error;
#[cfg(feature = "format")]
pub use crate::feature_format::*;
pub use crate::format_builder::*;
pub use crate::format_flags::*;
#[cfg(not(feature = "format"))]
pub use crate::not_feature_format::*;
#[cfg(feature = "format")]
pub use crate::prebuilt_formats::*;

/// Determine if the format packed struct is valid.
#[inline(always)]
pub const fn format_is_valid<const FORMAT: u128>() -> bool {
    NumberFormat::<FORMAT> {}.is_valid()
}

/// Get the error type from the format packed struct.
///
/// An error type of `Error::Success` means the format is valid, any
/// other error signifies an invalid format.
#[inline(always)]
pub const fn format_error<const FORMAT: u128>() -> Error {
    NumberFormat::<FORMAT> {}.error()
}

/// Standard number format. This is identical to the Rust string format.
pub const STANDARD: u128 = NumberFormatBuilder::new().build_strict();
