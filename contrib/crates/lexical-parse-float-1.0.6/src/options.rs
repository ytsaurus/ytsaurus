//! Configuration options for parsing floats.
//!
//! This enables extensive control over how the float is parsed, from
//! control characters like the decimal point and the valid non-finite
//! float representations.
//!
//! # Examples
//!
//! For example, to parse a European-style float:
//!
//! ```rust
//! use lexical_parse_float::{FromLexicalWithOptions, Options};
//! use lexical_parse_float::format::STANDARD;
//!
//! const OPTIONS: Options = Options::builder()
//!     .decimal_point(b',')
//!     .build_strict();
//! let value = "1,2345e300";
//! let result = f64::from_lexical_with_options::<STANDARD>(value.as_bytes(), &OPTIONS);
//! assert_eq!(result, Ok(1.2345e300));
//! ```
//!
//! # Pre-Defined Formats
//!
//! These are the pre-defined formats for parsing numbers from various
//! programming, markup, and data languages.
//!
//! - [`STANDARD`]: Standard number format.
//! - [`DECIMAL_COMMA`]: Numerical format with a decimal comma.
//! - [`HEX_FLOAT`]: Numerical format for hexadecimal floats, which use a `p`
//!   exponent.
//! - [`RUST_LITERAL`]: Number format for a [`Rust`] literal floating-point
//!   number.
//! - [`PYTHON_LITERAL`]: Number format for a [`Python`] literal floating-point
//!   number.
//! - [`CXX_LITERAL`]: Number format for a [`C++`] literal floating-point
//!   number.
//! - [`C_LITERAL`]: Number format for a [`C`] literal floating-point number.
//! - [`RUBY_LITERAL`]: Number format for a [`Ruby`] literal floating-point
//!   number.
//! - [`RUBY_STRING`]: Number format to parse a [`Ruby`] float from string.
//! - [`SWIFT_LITERAL`]: Number format for a [`Swift`] literal floating-point
//!   number.
//! - [`GO_LITERAL`]: Number format for a [`Golang`] literal floating-point
//!   number.
//! - [`HASKELL_LITERAL`]: Number format for a [`Haskell`] literal
//!   floating-point number.
//! - [`HASKELL_STRING`]: Number format to parse a [`Haskell`] float from
//!   string.
//! - [`JAVASCRIPT_LITERAL`]: Number format for a [`Javascript`] literal
//!   floating-point number.
//! - [`JAVASCRIPT_STRING`]: Number format to parse a [`Javascript`] float from
//!   string.
//! - [`PERL_LITERAL`]: Number format for a [`Perl`] literal floating-point
//!   number.
//! - [`PHP_LITERAL`]: Number format for a [`PHP`] literal floating-point
//!   number.
//! - [`JAVA_LITERAL`]: Number format for a [`Java`] literal floating-point
//!   number.
//! - [`JAVA_STRING`]: Number format to parse a [`Java`] float from string.
//! - [`R_LITERAL`]: Number format for an [`R`] literal floating-point number.
//! - [`KOTLIN_LITERAL`]: Number format for a [`Kotlin`] literal floating-point
//!   number.
//! - [`KOTLIN_STRING`]: Number format to parse a [`Kotlin`] float from string.
//! - [`JULIA_LITERAL`]: Number format for a [`Julia`] literal floating-point
//!   number.
//! - [`CSHARP_LITERAL`]: Number format for a [`C#`] literal floating-point
//!   number.
//! - [`CSHARP_STRING`]: Number format to parse a [`C#`] float from string.
//! - [`KAWA_LITERAL`]: Number format for a [`Kawa`] literal floating-point
//!   number.
//! - [`KAWA_STRING`]: Number format to parse a [`Kawa`] float from string.
//! - [`GAMBITC_LITERAL`]: Number format for a [`Gambit-C`] literal
//!   floating-point number.
//! - [`GAMBITC_STRING`]: Number format to parse a [`Gambit-C`] float from
//!   string.
//! - [`GUILE_LITERAL`]: Number format for a [`Guile`] literal floating-point
//!   number.
//! - [`GUILE_STRING`]: Number format to parse a [`Guile`] float from string.
//! - [`CLOJURE_LITERAL`]: Number format for a [`Clojure`] literal
//!   floating-point number.
//! - [`CLOJURE_STRING`]: Number format to parse a [`Clojure`] float from
//!   string.
//! - [`ERLANG_LITERAL`]: Number format for an [`Erlang`] literal floating-point
//!   number.
//! - [`ERLANG_STRING`]: Number format to parse an [`Erlang`] float from string.
//! - [`ELM_LITERAL`]: Number format for an [`Elm`] literal floating-point
//!   number.
//! - [`ELM_STRING`]: Number format to parse an [`Elm`] float from string.
//! - [`SCALA_LITERAL`]: Number format for a [`Scala`] literal floating-point
//!   number.
//! - [`SCALA_STRING`]: Number format to parse a [`Scala`] float from string.
//! - [`ELIXIR_LITERAL`]: Number format for an [`Elixir`] literal floating-point
//!   number.
//! - [`ELIXIR_STRING`]: Number format to parse an [`Elixir`] float from string.
//! - [`FORTRAN_LITERAL`]: Number format for a [`FORTRAN`] literal
//!   floating-point number.
//! - [`D_LITERAL`]: Number format for a [`D`] literal floating-point number.
//! - [`COFFEESCRIPT_LITERAL`]: Number format for a [`Coffeescript`] literal
//!   floating-point number.
//! - [`COFFEESCRIPT_STRING`]: Number format to parse a [`Coffeescript`] float
//!   from string.
//! - [`COBOL_LITERAL`]: Number format for a [`COBOL`] literal floating-point
//!   number.
//! - [`COBOL_STRING`]: Number format to parse a [`COBOL`] float from string.
//! - [`FSHARP_LITERAL`]: Number format for an [`F#`] literal floating-point
//!   number.
//! - [`VB_LITERAL`]: Number format for a [`Visual Basic`] literal
//!   floating-point number.
//! - [`VB_STRING`]: Number format to parse a [`Visual Basic`] float from
//!   string.
//! - [`OCAML_LITERAL`]: Number format for an [`OCaml`] literal floating-point
//!   number.
//! - [`OBJECTIVEC_LITERAL`]: Number format for an [`Objective-C`] literal
//!   floating-point number.
//! - [`OBJECTIVEC_STRING`]: Number format to parse an [`Objective-C`] float
//!   from string.
//! - [`REASONML_LITERAL`]: Number format for an [`ReasonML`] literal
//!   floating-point number.
//! - [`MATLAB_LITERAL`]: Number format for a [`MATLAB`] literal floating-point
//!   number.
//! - [`ZIG_LITERAL`]: Number format for a [`Zig`] literal floating-point
//!   number.
//! - [`SAGE_LITERAL`]: Number format for a [`Sage`] literal floating-point
//!   number.
//! - [`JSON`]: Number format for a [`JSON`][`JSON-REF`] literal floating-point
//!   number.
//! - [`TOML`]: Number format for a [`TOML`][`TOML-REF`] literal floating-point
//!   number.
//! - [`YAML`]: Number format for a [`YAML`][`YAML-REF`] literal floating-point
//!   number.
//! - [`XML`]: Number format for an [`XML`][`XML-REF`] literal floating-point
//!   number.
//! - [`SQLITE`]: Number format for a [`SQLite`] literal floating-point number.
//! - [`POSTGRESQL`]: Number format for a [`PostgreSQL`] literal floating-point
//!   number.
//! - [`MYSQL`]: Number format for a [`MySQL`] literal floating-point number.
//! - [`MONGODB`]: Number format for a [`MongoDB`] literal floating-point
//!   number.
//!
//! <!-- References -->
//!
//! [`Rust`]: https://www.rust-lang.org/
//! [`Python`]: https://www.python.org/
//! [`C++`]: https://en.cppreference.com/w/
//! [`C`]: https://en.cppreference.com/w/c
//! [`Ruby`]: https://www.ruby-lang.org/en/
//! [`Swift`]: https://developer.apple.com/swift/
//! [`Golang`]: https://go.dev/
//! [`Haskell`]: https://www.haskell.org/
//! [`Javascript`]: https://developer.mozilla.org/en-US/docs/Web/JavaScript
//! [`Perl`]: https://www.perl.org/
//! [`PHP`]: https://www.php.net/
//! [`Java`]: https://www.java.com/en/
//! [`R`]: https://www.r-project.org/
//! [`Kotlin`]: https://kotlinlang.org/
//! [`Julia`]: https://julialang.org/
//! [`C#`]: https://learn.microsoft.com/en-us/dotnet/csharp/
//! [`Kawa`]: https://www.gnu.org/software/kawa/
//! [`Gambit-C`]: https://gambitscheme.org/
//! [`Guile`]: https://www.gnu.org/software/guile/
//! [`Clojure`]: https://clojure.org/
//! [`Erlang`]: https://www.erlang.org/
//! [`Elm`]: https://elm-lang.org/
//! [`Scala`]: https://www.scala-lang.org/
//! [`Elixir`]: https://elixir-lang.org/
//! [`FORTRAN`]: https://fortran-lang.org/
//! [`D`]: https://dlang.org/
//! [`Coffeescript`]: https://coffeescript.org/
//! [`Cobol`]: https://www.ibm.com/think/topics/cobol
//! [`F#`]: https://fsharp.org/
//! [`Visual Basic`]: https://learn.microsoft.com/en-us/dotnet/visual-basic/
//! [`OCaml`]: https://ocaml.org/
//! [`Objective-C`]: https://en.wikipedia.org/wiki/Objective-C
//! [`ReasonML`]: https://reasonml.github.io/
//! [`Matlab`]: https://www.mathworks.com/products/matlab.html
//! [`Zig`]: https://ziglang.org/
//! [`Sage`]: https://www.sagemath.org/
//! [`JSON-REF`]: https://www.json.org/json-en.html
//! [`TOML-REF`]: https://toml.io/en/
//! [`YAML-REF`]: https://yaml.org/
//! [`XML-REF`]: https://en.wikipedia.org/wiki/XML
//! [`SQLite`]: https://www.sqlite.org/
//! [`PostgreSQL`]: https://www.postgresql.org/
//! [`MySQL`]: https://www.mysql.com/
//! [`MongoDB`]: https://www.mongodb.com/

#![allow(clippy::must_use_candidate)]

use lexical_util::ascii::{is_valid_ascii, is_valid_letter_slice};
use lexical_util::error::Error;
use lexical_util::options::{self, ParseOptions};
use lexical_util::result::Result;

/// Maximum length for a special string.
pub const MAX_SPECIAL_STRING_LENGTH: usize = 50;

/// Builder for [`Options`].
///
/// This enables extensive control over how the float is parsed, from
/// control characters like the decimal point and the valid non-finite
/// float representations.
///
/// # Examples
///
/// ```rust
/// use lexical_parse_float::{FromLexicalWithOptions, Options};
/// use lexical_parse_float::format::STANDARD;
///
/// const OPTIONS: Options = Options::builder()
///     .decimal_point(b',')
///     .build_strict();
/// let value = "1,2345e300";
/// let result = f64::from_lexical_with_options::<STANDARD>(value.as_bytes(), &OPTIONS);
/// assert_eq!(result, Ok(1.2345e300));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct OptionsBuilder {
    /// Disable the use of arbitrary-precision arithmetic, and always
    /// return the results from the fast or intermediate path algorithms.
    lossy: bool,
    /// Character to designate the exponent component of a float.
    exponent: u8,
    /// Character to separate the integer from the fraction components.
    decimal_point: u8,
    /// String representation of Not A Number, aka `NaN`.
    nan_string: Option<&'static [u8]>,
    /// Short string representation of `Infinity`.
    inf_string: Option<&'static [u8]>,
    /// Long string representation of `Infinity`.
    infinity_string: Option<&'static [u8]>,
}

impl OptionsBuilder {
    /// Create new options builder with default options.
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            lossy: false,
            exponent: b'e',
            decimal_point: b'.',
            nan_string: Some(b"NaN"),
            inf_string: Some(b"inf"),
            infinity_string: Some(b"infinity"),
        }
    }

    // GETTERS

    /// Get if we disable the use of arbitrary-precision arithmetic.
    ///
    /// Lossy algorithms never use the fallback, slow algorithm. Defaults to
    /// [`false`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_float::options::Options;
    ///
    /// assert_eq!(Options::builder().get_lossy(), false);
    /// ```
    #[inline(always)]
    pub const fn get_lossy(&self) -> bool {
        self.lossy
    }

    /// Get the character to designate the exponent component of a float.
    ///
    /// Any non-control character is valid, but `\t` to `\r` are also valid.
    /// The full range is `[0x09, 0x0D]` and `[0x20, 0x7F]`. Defaults to `e`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_float::options::Options;
    ///
    /// assert_eq!(Options::builder().get_exponent(), b'e');
    /// ```
    #[inline(always)]
    pub const fn get_exponent(&self) -> u8 {
        self.exponent
    }

    /// Get the character to separate the integer from the fraction components.
    ///
    /// Any non-control character is valid, but `\t` to `\r` are also valid.
    /// The full range is `[0x09, 0x0D]` and `[0x20, 0x7F]`. Defaults to `.`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_float::options::Options;
    ///
    /// assert_eq!(Options::builder().get_decimal_point(), b'.');
    /// ```
    #[inline(always)]
    pub const fn get_decimal_point(&self) -> u8 {
        self.decimal_point
    }

    /// Get the string representation for `NaN`.
    ///
    /// The first character must start with `N` or `n` and all characters must
    /// be valid ASCII letters (`A-Z` or `a-z`). Defaults to `NaN`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_float::Options;
    ///
    /// let builder = Options::builder();
    /// assert_eq!(builder.get_nan_string(), Some("NaN".as_bytes()));
    /// ```
    #[inline(always)]
    pub const fn get_nan_string(&self) -> Option<&'static [u8]> {
        self.nan_string
    }

    /// Get the short string representation for `Infinity`.
    ///
    /// The first character must start with `I` or `i` and all characters must
    /// be valid ASCII letters (`A-Z` or `a-z`). Defaults to `inf`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_float::Options;
    ///
    /// let builder = Options::builder();
    /// assert_eq!(builder.get_inf_string(), Some("inf".as_bytes()));
    /// ```
    #[inline(always)]
    pub const fn get_inf_string(&self) -> Option<&'static [u8]> {
        self.inf_string
    }

    /// Get the long string representation for `Infinity`.
    ///
    /// The first character must start with `I` or `i` and all characters must
    /// be valid ASCII letters (`A-Z` or `a-z`). Defaults to `infinity`.
    ///
    /// [`get_inf_string`]: Self::get_inf_string
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_float::Options;
    ///
    /// let builder = Options::builder();
    /// assert_eq!(builder.get_infinity_string(), Some("infinity".as_bytes()));
    /// ```
    #[inline(always)]
    pub const fn get_infinity_string(&self) -> Option<&'static [u8]> {
        self.infinity_string
    }

    // SETTERS

    /// Set if we disable the use of arbitrary-precision arithmetic.
    ///
    /// Lossy algorithms never use the fallback, slow algorithm. Defaults to
    /// [`false`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_float::options::Options;
    ///
    /// const OPTIONS: Options = Options::builder()
    ///     .lossy(true)
    ///     .build_strict();
    /// assert_eq!(OPTIONS.lossy(), true);
    /// ```
    #[must_use]
    #[inline(always)]
    pub const fn lossy(mut self, lossy: bool) -> Self {
        self.lossy = lossy;
        self
    }

    /// Set the character to designate the exponent component of a float.
    ///
    /// Any non-control character is valid, but `\t` to `\r` are also valid.
    /// The full range is `[0x09, 0x0D]` and `[0x20, 0x7F]`. Defaults to `e`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_float::options::Options;
    ///
    /// const OPTIONS: Options = Options::builder()
    ///     .exponent(b'^')
    ///     .build_strict();
    /// assert_eq!(OPTIONS.exponent(), b'^');
    /// ```
    #[must_use]
    #[inline(always)]
    pub const fn exponent(mut self, exponent: u8) -> Self {
        self.exponent = exponent;
        self
    }

    /// Set the character to separate the integer from the fraction components.
    ///
    /// Any non-control character is valid, but `\t` to `\r` are also valid.
    /// The full range is `[0x09, 0x0D]` and `[0x20, 0x7F]`. Defaults to `.`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_float::options::Options;
    ///
    /// const OPTIONS: Options = Options::builder()
    ///     .exponent(b',')
    ///     .build_strict();
    /// assert_eq!(OPTIONS.exponent(), b',');
    /// ```
    #[must_use]
    #[inline(always)]
    pub const fn decimal_point(mut self, decimal_point: u8) -> Self {
        self.decimal_point = decimal_point;
        self
    }

    /// Set the string representation for `NaN`.
    ///
    /// The first character must start with `N` or `n` and all characters must
    /// be valid ASCII letters (`A-Z` or `a-z`). If set to `None`, then parsing
    /// [`NaN`][f64::NAN] returns an error. Defaults to `NaN`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_float::Options;
    ///
    /// const OPTIONS: Options = Options::builder()
    ///     .nan_string(Some(b"nan"))
    ///     .build_strict();
    /// assert_eq!(OPTIONS.nan_string(), Some(b"nan".as_ref()));
    /// ```
    ///
    /// Panics
    ///
    /// Setting a value with more than 50 elements will panic at runtime. You
    /// should always build the format using [`build_strict`] or checking
    /// [`is_valid`] prior to using the format, to avoid unexpected panics.
    ///
    /// [`build_strict`]: Self::build_strict
    /// [`is_valid`]: Self::is_valid
    #[must_use]
    #[inline(always)]
    pub const fn nan_string(mut self, nan_string: Option<&'static [u8]>) -> Self {
        self.nan_string = nan_string;
        self
    }

    /// Set the short string representation for `Infinity`.
    ///
    /// The first character must start with `I` or `i` and all characters must
    /// be valid ASCII letters (`A-Z` or `a-z`). If set to `None`, then parsing
    /// [`Infinity`][f64::INFINITY] returns an error. Defaults to `inf`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_float::Options;
    ///
    /// const OPTIONS: Options = Options::builder()
    ///     .inf_string(Some(b"Infinity"))
    ///     .build_strict();
    /// assert_eq!(OPTIONS.inf_string(), Some(b"Infinity".as_ref()));
    /// ```
    ///
    /// Panics
    ///
    /// Setting a value with more than 50 elements or one that is longer than
    /// [`infinity_string`] will panic at runtime. You should always build
    /// the format using [`build_strict`] or checking [`is_valid`] prior to
    /// using the format, to avoid unexpected panics.
    ///
    /// [`infinity_string`]: Self::infinity_string
    /// [`build_strict`]: Self::build_strict
    /// [`is_valid`]: Self::is_valid
    #[must_use]
    #[inline(always)]
    pub const fn inf_string(mut self, inf_string: Option<&'static [u8]>) -> Self {
        self.inf_string = inf_string;
        self
    }

    /// Set the long string representation for `Infinity`.
    ///
    /// The first character must start with `I` or `i` and all characters must
    /// be valid ASCII letters (`A-Z` or `a-z`). If set to `None`, then parsing
    /// [`Infinity`][f64::INFINITY] returns an error. Defaults to `infinity`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_float::Options;
    ///
    /// const OPTIONS: Options = Options::builder()
    ///     .infinity_string(Some(b"Infinity"))
    ///     .build_strict();
    /// assert_eq!(OPTIONS.infinity_string(), Some(b"Infinity".as_ref()));
    /// ```
    ///
    /// Panics
    ///
    /// Setting a value with more than 50 elements or one that is shorter than
    /// [`inf_string`] will panic at runtime. You should always build the format
    /// using [`build_strict`] or checking [`is_valid`] prior to
    /// using the format, to avoid unexpected panics.
    ///
    /// [`inf_string`]: Self::inf_string
    /// [`build_strict`]: Self::build_strict
    /// [`is_valid`]: Self::is_valid
    #[must_use]
    #[inline(always)]
    pub const fn infinity_string(mut self, infinity_string: Option<&'static [u8]>) -> Self {
        self.infinity_string = infinity_string;
        self
    }

    // BUILDERS

    /// Determine if [`nan_string`] is valid.
    ///
    /// [`nan_string`]: Self::nan_string
    #[doc(hidden)]
    #[inline(always)]
    #[allow(clippy::if_same_then_else, clippy::needless_bool)] // reason = "more idiomatic"
    pub const fn nan_str_is_valid(&self) -> bool {
        if self.nan_string.is_none() {
            return true;
        }

        let nan = unwrap_str(self.nan_string);
        let length = nan.len();
        if length == 0 || length > MAX_SPECIAL_STRING_LENGTH {
            false
        } else if !matches!(nan[0], b'N' | b'n') {
            false
        } else if !is_valid_letter_slice(nan) {
            false
        } else {
            true
        }
    }

    /// Determine if [`inf_string`] is valid.
    ///
    /// [`inf_string`]: Self::inf_string
    #[doc(hidden)]
    #[inline(always)]
    #[allow(clippy::if_same_then_else, clippy::needless_bool)] // reason = "more idiomatic"
    pub const fn inf_str_is_valid(&self) -> bool {
        if self.infinity_string.is_none() && self.inf_string.is_some() {
            return false;
        } else if self.inf_string.is_none() {
            return true;
        }

        let inf = unwrap_str(self.inf_string);
        let length = inf.len();
        let infinity = unwrap_str(self.infinity_string);
        if length == 0 || length > MAX_SPECIAL_STRING_LENGTH {
            false
        } else if !matches!(inf[0], b'I' | b'i') {
            false
        } else if length > infinity.len() {
            false
        } else if !is_valid_letter_slice(inf) {
            false
        } else {
            true
        }
    }

    /// Determine if [`infinity_string`] is valid.
    ///
    /// [`infinity_string`]: Self::infinity_string
    #[doc(hidden)]
    #[inline(always)]
    #[allow(clippy::if_same_then_else, clippy::needless_bool)] // reason = "more idiomatic"
    pub const fn infinity_string_is_valid(&self) -> bool {
        if self.infinity_string.is_none() && self.inf_string.is_some() {
            return false;
        } else if self.infinity_string.is_none() {
            return true;
        }
        let inf = unwrap_str(self.inf_string);
        let infinity = unwrap_str(self.infinity_string);
        let length = infinity.len();
        if length == 0 || length > MAX_SPECIAL_STRING_LENGTH {
            false
        } else if !matches!(infinity[0], b'I' | b'i') {
            false
        } else if length < inf.len() {
            false
        } else if !is_valid_letter_slice(infinity) {
            false
        } else {
            true
        }
    }

    /// Check if the builder state is valid.
    #[inline(always)]
    #[allow(clippy::if_same_then_else, clippy::needless_bool)] // reason = "more idiomatic"
    pub const fn is_valid(&self) -> bool {
        if !is_valid_ascii(self.exponent) {
            false
        } else if !is_valid_ascii(self.decimal_point) {
            false
        } else if !self.nan_str_is_valid() {
            false
        } else if !self.inf_str_is_valid() {
            false
        } else if !self.infinity_string_is_valid() {
            false
        } else {
            true
        }
    }

    /// Build the [`Options`] struct without validation.
    ///
    /// # Panics
    ///
    /// This is completely safe, however, misusing this, especially
    /// the [`nan_string`], [`inf_string`], and [`infinity_string`] could
    /// panic at runtime. Always use [`is_valid`] prior to using the built
    /// options.
    ///
    /// [`inf_string`]: Self::inf_string
    /// [`nan_string`]: Self::nan_string
    /// [`infinity_string`]: Self::infinity_string
    /// [`is_valid`]: Self::is_valid
    #[inline(always)]
    pub const fn build_unchecked(&self) -> Options {
        Options {
            lossy: self.lossy,
            exponent: self.exponent,
            decimal_point: self.decimal_point,
            nan_string: self.nan_string,
            inf_string: self.inf_string,
            infinity_string: self.infinity_string,
        }
    }

    /// Build the [`Options`] struct, panicking if the builder is invalid.
    ///
    /// # Panics
    ///
    /// If the built options are not valid. This should always
    /// be used within a const context to avoid panics at runtime.
    #[inline(always)]
    pub const fn build_strict(&self) -> Options {
        match self.build() {
            Ok(value) => value,
            Err(error) => core::panic!("{}", error.description()),
        }
    }

    /// Build the [`Options`] struct.
    ///
    /// # Errors
    ///
    /// If the NaN, Inf, or Infinity strings are too long or invalid
    /// digits/characters are provided for some numerical formats.
    #[inline(always)]
    #[allow(clippy::if_same_then_else)] // reason = "more idiomatic"
    pub const fn build(&self) -> Result<Options> {
        if !is_valid_ascii(self.exponent) {
            return Err(Error::InvalidExponentSymbol);
        } else if !is_valid_ascii(self.decimal_point) {
            return Err(Error::InvalidDecimalPoint);
        }

        if self.nan_string.is_some() {
            let nan = unwrap_str(self.nan_string);
            if nan.is_empty() || !matches!(nan[0], b'N' | b'n') {
                return Err(Error::InvalidNanString);
            } else if !is_valid_letter_slice(nan) {
                return Err(Error::InvalidNanString);
            } else if nan.len() > MAX_SPECIAL_STRING_LENGTH {
                return Err(Error::NanStringTooLong);
            }
        }

        if self.inf_string.is_some() && self.infinity_string.is_none() {
            return Err(Error::InfinityStringTooShort);
        }

        if self.inf_string.is_some() {
            let inf = unwrap_str(self.inf_string);
            if inf.is_empty() || !matches!(inf[0], b'I' | b'i') {
                return Err(Error::InvalidInfString);
            } else if !is_valid_letter_slice(inf) {
                return Err(Error::InvalidInfString);
            } else if inf.len() > MAX_SPECIAL_STRING_LENGTH {
                return Err(Error::InfStringTooLong);
            }
        }

        if self.infinity_string.is_some() {
            let inf = unwrap_str(self.inf_string);
            let infinity = unwrap_str(self.infinity_string);
            if infinity.is_empty() || !matches!(infinity[0], b'I' | b'i') {
                return Err(Error::InvalidInfinityString);
            } else if !is_valid_letter_slice(infinity) {
                return Err(Error::InvalidInfinityString);
            } else if infinity.len() > MAX_SPECIAL_STRING_LENGTH {
                return Err(Error::InfinityStringTooLong);
            } else if infinity.len() < inf.len() {
                return Err(Error::InfinityStringTooShort);
            }
        }

        Ok(self.build_unchecked())
    }
}

impl Default for OptionsBuilder {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

/// Options to customize parsing floats.
///
/// This enables extensive control over how the float is parsed, from
/// control characters like the decimal point and the valid non-finite
/// float representations.
///
/// # Examples
///
/// ```rust
/// use lexical_parse_float::{FromLexicalWithOptions, Options};
/// use lexical_parse_float::format::STANDARD;
///
/// const OPTIONS: Options = Options::builder()
///     .lossy(true)
///     .nan_string(Some(b"NaN"))
///     .inf_string(Some(b"Inf"))
///     .infinity_string(Some(b"Infinity"))
///     .build_strict();
///
/// let value = "1.2345e300";
/// let result = f64::from_lexical_with_options::<STANDARD>(value.as_bytes(), &OPTIONS);
/// assert_eq!(result, Ok(1.2345e300));
///
/// let value = "NaN";
/// let result = f64::from_lexical_with_options::<STANDARD>(value.as_bytes(), &OPTIONS);
/// assert_eq!(result.map(|x| x.is_nan()), Ok(true));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Options {
    /// Disable the use of arbitrary-precision arithmetic, and always
    /// return the results from the fast or intermediate path algorithms.
    lossy: bool,
    /// Character to designate the exponent component of a float.
    exponent: u8,
    /// Character to separate the integer from the fraction components.
    decimal_point: u8,
    /// String representation of Not A Number, aka `NaN`.
    nan_string: Option<&'static [u8]>,
    /// Short string representation of `Infinity`.
    inf_string: Option<&'static [u8]>,
    /// Long string representation of `Infinity`.
    infinity_string: Option<&'static [u8]>,
}

impl Options {
    // CONSTRUCTORS

    /// Create options with default values.
    #[inline(always)]
    pub const fn new() -> Self {
        Self::builder().build_unchecked()
    }

    /// Create the default options for a given radix.
    ///
    /// This sets the exponent to `^` for any radix where `e`
    /// would be a valid digit.
    #[inline(always)]
    #[cfg(feature = "power-of-two")]
    pub const fn from_radix(radix: u8) -> Self {
        // Need to determine the correct exponent character ('e' or '^'),
        // since the default character is `e` normally, but this is a valid
        // digit for radix >= 15.
        let mut builder = Self::builder();
        if radix >= 15 {
            builder = builder.exponent(b'^');
        }
        builder.build_unchecked()
    }

    // GETTERS

    /// Check if the options state is valid.
    #[inline(always)]
    pub const fn is_valid(&self) -> bool {
        self.rebuild().is_valid()
    }

    /// Get if we disable the use of arbitrary-precision arithmetic.
    ///
    /// Lossy algorithms never use the fallback, slow algorithm. Defaults to
    /// [`false`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_float::options::Options;
    ///
    /// assert_eq!(Options::new().lossy(), false);
    /// ```
    #[inline(always)]
    pub const fn lossy(&self) -> bool {
        self.lossy
    }

    /// Get the character to designate the exponent component of a float.
    ///
    /// Any non-control character is valid, but `\t` to `\r` are also valid.
    /// The full range is `[0x09, 0x0D]` and `[0x20, 0x7F]`. Defaults to `e`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_float::options::Options;
    ///
    /// assert_eq!(Options::new().exponent(), b'e');
    /// ```
    #[inline(always)]
    pub const fn exponent(&self) -> u8 {
        self.exponent
    }

    /// Get the character to separate the integer from the fraction components.
    ///
    /// Any non-control character is valid, but `\t` to `\r` are also valid.
    /// The full range is `[0x09, 0x0D]` and `[0x20, 0x7F]`. Defaults to `.`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_float::options::Options;
    ///
    /// assert_eq!(Options::new().decimal_point(), b'.');
    /// ```
    #[inline(always)]
    pub const fn decimal_point(&self) -> u8 {
        self.decimal_point
    }

    /// Get the string representation for `NaN`.
    ///
    /// The first character must start with `N` or `n` and all characters must
    /// be valid ASCII letters (`A-Z` or `a-z`). If set to `None`, then parsing
    /// [`NaN`][f64::NAN] returns an error. Defaults to `NaN`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_float::options::Options;
    ///
    /// assert_eq!(Options::new().nan_string(), Some(b"NaN".as_ref()));
    /// ```
    #[inline(always)]
    pub const fn nan_string(&self) -> Option<&'static [u8]> {
        self.nan_string
    }

    /// Get the short string representation for `Infinity`.
    ///
    /// The first character must start with `I` or `i` and all characters must
    /// be valid ASCII letters (`A-Z` or `a-z`). If set to `None`, then parsing
    /// [`Infinity`][f64::INFINITY] returns an error. Defaults to `inf`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_float::options::Options;
    ///
    /// assert_eq!(Options::new().inf_string(), Some(b"inf".as_ref()));
    /// ```
    #[inline(always)]
    pub const fn inf_string(&self) -> Option<&'static [u8]> {
        self.inf_string
    }

    /// Get the long string representation for `Infinity`.
    ///
    /// The first character must start with `I` or `i` and all characters must
    /// be valid ASCII letters (`A-Z` or `a-z`). If set to `None`, then parsing
    /// [`Infinity`][f64::INFINITY] returns an error. Defaults to `infinity`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_float::options::Options;
    ///
    /// assert_eq!(Options::new().infinity_string(), Some(b"infinity".as_ref()));
    /// ```
    #[inline(always)]
    pub const fn infinity_string(&self) -> Option<&'static [u8]> {
        self.infinity_string
    }

    // SETTERS

    /// Set if we disable the use of arbitrary-precision arithmetic.
    #[inline(always)]
    #[deprecated = "Options should be treated as immutable, use `OptionsBuilder` instead. Will be removed in 2.0."]
    pub fn set_lossy(&mut self, lossy: bool) {
        self.lossy = lossy;
    }

    /// Set the character to designate the exponent component of a float.
    #[inline(always)]
    #[deprecated = "Options should be treated as immutable, use `OptionsBuilder` instead. Will be removed in 2.0."]
    pub fn set_exponent(&mut self, exponent: u8) {
        self.exponent = exponent;
    }

    /// Set the character to separate the integer from the fraction components.
    #[inline(always)]
    #[deprecated = "Options should be treated as immutable, use `OptionsBuilder` instead. Will be removed in 2.0."]
    pub fn set_decimal_point(&mut self, decimal_point: u8) {
        self.decimal_point = decimal_point;
    }

    /// Set the string representation for `NaN`.
    #[inline(always)]
    #[deprecated = "Options should be treated as immutable, use `OptionsBuilder` instead. Will be removed in 2.0."]
    pub fn set_nan_string(&mut self, nan_string: Option<&'static [u8]>) {
        self.nan_string = nan_string;
    }

    /// Set the short string representation for `Infinity`
    #[inline(always)]
    #[deprecated = "Options should be treated as immutable, use `OptionsBuilder` instead. Will be removed in 2.0."]
    pub fn set_inf_string(&mut self, inf_string: Option<&'static [u8]>) {
        self.inf_string = inf_string;
    }

    /// Set the long string representation for `Infinity`
    #[inline(always)]
    #[deprecated = "Options should be treated as immutable, use `OptionsBuilder` instead. Will be removed in 2.0."]
    pub fn set_infinity_string(&mut self, infinity_string: Option<&'static [u8]>) {
        self.infinity_string = infinity_string;
    }

    // BUILDERS

    /// Get [`OptionsBuilder`] as a static function.
    #[must_use]
    #[inline(always)]
    pub const fn builder() -> OptionsBuilder {
        OptionsBuilder::new()
    }

    /// Create [`OptionsBuilder`] using existing values.
    #[must_use]
    #[inline(always)]
    pub const fn rebuild(&self) -> OptionsBuilder {
        OptionsBuilder {
            lossy: self.lossy,
            exponent: self.exponent,
            decimal_point: self.decimal_point,
            nan_string: self.nan_string,
            inf_string: self.inf_string,
            infinity_string: self.infinity_string,
        }
    }
}

impl Default for Options {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

impl ParseOptions for Options {
    #[inline(always)]
    fn is_valid(&self) -> bool {
        Self::is_valid(self)
    }
}

/// Unwrap `Option` as a const fn.
#[inline(always)]
const fn unwrap_str(option: Option<&'static [u8]>) -> &'static [u8] {
    match option {
        Some(x) => x,
        None => &[],
    }
}

// PRE-DEFINED CONSTANTS
// ---------------------

// Only constants that differ from the standard version are included.

/// Standard number format.
#[rustfmt::skip]
pub const STANDARD: Options = Options::new();

/// Numerical format with a decimal comma.
/// This is the standard numerical format for most of the world.
#[rustfmt::skip]
pub const DECIMAL_COMMA: Options = Options::builder()
    .decimal_point(b',')
    .build_strict();

/// Numerical format for hexadecimal floats, which use a `p` exponent.
#[rustfmt::skip]
pub const HEX_FLOAT: Options = Options::builder()
    .exponent(b'p')
    .build_strict();

/// Numerical format where `^` is used as the exponent notation character.
/// This isn't very common, but is useful when `e` or `p` are valid digits.
#[rustfmt::skip]
pub const CARAT_EXPONENT: Options = Options::builder()
    .exponent(b'^')
    .build_strict();

/// Number format for a `Rust` literal floating-point number.
#[rustfmt::skip]
pub const RUST_LITERAL: Options = Options::builder()
    .nan_string(options::RUST_LITERAL)
    .inf_string(options::RUST_LITERAL)
    .infinity_string(options::RUST_LITERAL)
    .build_strict();

/// Number format for a `Python` literal floating-point number.
#[rustfmt::skip]
pub const PYTHON_LITERAL: Options = Options::builder()
    .nan_string(options::PYTHON_LITERAL)
    .inf_string(options::PYTHON_LITERAL)
    .infinity_string(options::PYTHON_LITERAL)
    .build_strict();

/// Number format for a `C++` literal floating-point number.
#[rustfmt::skip]
pub const CXX_LITERAL: Options = Options::builder()
    .nan_string(options::CXX_LITERAL_NAN)
    .inf_string(options::CXX_LITERAL_INF)
    .infinity_string(options::CXX_LITERAL_INFINITY)
    .build_strict();

/// Number format for a `C` literal floating-point number.
#[rustfmt::skip]
pub const C_LITERAL: Options = Options::builder()
    .nan_string(options::C_LITERAL_NAN)
    .inf_string(options::C_LITERAL_INF)
    .infinity_string(options::C_LITERAL_INFINITY)
    .build_strict();

/// Number format for a `Ruby` literal floating-point number.
#[rustfmt::skip]
pub const RUBY_LITERAL: Options = Options::builder()
    .nan_string(options::RUBY_LITERAL_NAN)
    .inf_string(options::RUBY_LITERAL_INF)
    .infinity_string(options::RUBY_LITERAL_INF)
    .build_strict();

/// Number format to parse a `Ruby` float from string.
/// `Ruby` can write NaN and Infinity as strings, but won't round-trip them back to floats.
#[rustfmt::skip]
pub const RUBY_STRING: Options = Options::builder()
    .nan_string(options::RUBY_STRING_NONE)
    .inf_string(options::RUBY_STRING_NONE)
    .infinity_string(options::RUBY_STRING_NONE)
    .build_strict();

/// Number format for a `Swift` literal floating-point number.
#[rustfmt::skip]
pub const SWIFT_LITERAL: Options = Options::builder()
    .nan_string(options::SWIFT_LITERAL)
    .inf_string(options::SWIFT_LITERAL)
    .infinity_string(options::SWIFT_LITERAL)
    .build_strict();

/// Number format for a `Go` literal floating-point number.
#[rustfmt::skip]
pub const GO_LITERAL: Options = Options::builder()
    .nan_string(options::GO_LITERAL)
    .inf_string(options::GO_LITERAL)
    .infinity_string(options::GO_LITERAL)
    .build_strict();

/// Number format for a `Haskell` literal floating-point number.
#[rustfmt::skip]
pub const HASKELL_LITERAL: Options = Options::builder()
    .nan_string(options::HASKELL_LITERAL)
    .inf_string(options::HASKELL_LITERAL)
    .infinity_string(options::HASKELL_LITERAL)
    .build_strict();

/// Number format to parse a `Haskell` float from string.
#[rustfmt::skip]
pub const HASKELL_STRING: Options = Options::builder()
    .inf_string(options::HASKELL_STRING_INF)
    .infinity_string(options::HASKELL_STRING_INFINITY)
    .build_strict();

/// Number format for a `Javascript` literal floating-point number.
#[rustfmt::skip]
pub const JAVASCRIPT_LITERAL: Options = Options::builder()
    .inf_string(options::JAVASCRIPT_INF)
    .infinity_string(options::JAVASCRIPT_INFINITY)
    .build_strict();

/// Number format to parse a `Javascript` float from string.
#[rustfmt::skip]
pub const JAVASCRIPT_STRING: Options = Options::builder()
    .inf_string(options::JAVASCRIPT_INF)
    .infinity_string(options::JAVASCRIPT_INFINITY)
    .build_strict();

/// Number format for a `Perl` literal floating-point number.
#[rustfmt::skip]
pub const PERL_LITERAL: Options = Options::builder()
    .nan_string(options::PERL_LITERAL)
    .inf_string(options::PERL_LITERAL)
    .infinity_string(options::PERL_LITERAL)
    .build_strict();

/// Number format for a `PHP` literal floating-point number.
#[rustfmt::skip]
pub const PHP_LITERAL: Options = Options::builder()
    .nan_string(options::PHP_LITERAL_NAN)
    .inf_string(options::PHP_LITERAL_INF)
    .infinity_string(options::PHP_LITERAL_INFINITY)
    .build_strict();

/// Number format for a `Java` literal floating-point number.
#[rustfmt::skip]
pub const JAVA_LITERAL: Options = Options::builder()
    .nan_string(options::JAVA_LITERAL)
    .inf_string(options::JAVA_LITERAL)
    .infinity_string(options::JAVA_LITERAL)
    .build_strict();

/// Number format to parse a `Java` float from string.
#[rustfmt::skip]
pub const JAVA_STRING: Options = Options::builder()
    .inf_string(options::JAVA_STRING_INF)
    .infinity_string(options::JAVA_STRING_INFINITY)
    .build_strict();

/// Number format for an `R` literal floating-point number.
#[rustfmt::skip]
pub const R_LITERAL: Options = Options::builder()
    .inf_string(options::R_LITERAL_INF)
    .infinity_string(options::R_LITERAL_INFINITY)
    .build_strict();

/// Number format for a `Kotlin` literal floating-point number.
#[rustfmt::skip]
pub const KOTLIN_LITERAL: Options = Options::builder()
    .nan_string(options::KOTLIN_LITERAL)
    .inf_string(options::KOTLIN_LITERAL)
    .infinity_string(options::KOTLIN_LITERAL)
    .build_strict();

/// Number format to parse a `Kotlin` float from string.
#[rustfmt::skip]
pub const KOTLIN_STRING: Options = Options::builder()
    .inf_string(options::KOTLIN_STRING_INF)
    .infinity_string(options::KOTLIN_STRING_INFINITY)
    .build_strict();

/// Number format for a `Julia` literal floating-point number.
#[rustfmt::skip]
pub const JULIA_LITERAL: Options = Options::builder()
    .inf_string(options::JULIA_LITERAL_INF)
    .infinity_string(options::JULIA_LITERAL_INFINITY)
    .build_strict();

/// Number format for a `C#` literal floating-point number.
#[rustfmt::skip]
pub const CSHARP_LITERAL: Options = Options::builder()
    .nan_string(options::CSHARP_LITERAL)
    .inf_string(options::CSHARP_LITERAL)
    .infinity_string(options::CSHARP_LITERAL)
    .build_strict();

/// Number format to parse a `C#` float from string.
#[rustfmt::skip]
pub const CSHARP_STRING: Options = Options::builder()
    .inf_string(options::CSHARP_STRING_INF)
    .infinity_string(options::CSHARP_STRING_INFINITY)
    .build_strict();

/// Number format for a `Kawa` literal floating-point number.
#[rustfmt::skip]
pub const KAWA_LITERAL: Options = Options::builder()
    .nan_string(options::KAWA)
    .inf_string(options::KAWA)
    .infinity_string(options::KAWA)
    .build_strict();

/// Number format to parse a `Kawa` float from string.
#[rustfmt::skip]
pub const KAWA_STRING: Options = Options::builder()
    .nan_string(options::KAWA)
    .inf_string(options::KAWA)
    .infinity_string(options::KAWA)
    .build_strict();

/// Number format for a `Gambit-C` literal floating-point number.
#[rustfmt::skip]
pub const GAMBITC_LITERAL: Options = Options::builder()
    .nan_string(options::GAMBITC)
    .inf_string(options::GAMBITC)
    .infinity_string(options::GAMBITC)
    .build_strict();

/// Number format to parse a `Gambit-C` float from string.
#[rustfmt::skip]
pub const GAMBITC_STRING: Options = Options::builder()
    .nan_string(options::GAMBITC)
    .inf_string(options::GAMBITC)
    .infinity_string(options::GAMBITC)
    .build_strict();

/// Number format for a `Guile` literal floating-point number.
#[rustfmt::skip]
pub const GUILE_LITERAL: Options = Options::builder()
    .nan_string(options::GUILE)
    .inf_string(options::GUILE)
    .infinity_string(options::GUILE)
    .build_strict();

/// Number format to parse a `Guile` float from string.
#[rustfmt::skip]
pub const GUILE_STRING: Options = Options::builder()
    .nan_string(options::GUILE)
    .inf_string(options::GUILE)
    .infinity_string(options::GUILE)
    .build_strict();

/// Number format for a `Clojure` literal floating-point number.
#[rustfmt::skip]
pub const CLOJURE_LITERAL: Options = Options::builder()
    .nan_string(options::CLOJURE_LITERAL)
    .inf_string(options::CLOJURE_LITERAL)
    .infinity_string(options::CLOJURE_LITERAL)
    .build_strict();

/// Number format to parse a `Clojure` float from string.
#[rustfmt::skip]
pub const CLOJURE_STRING: Options = Options::builder()
    .inf_string(options::CLOJURE_STRING_INF)
    .infinity_string(options::CLOJURE_STRING_INFINITY)
    .build_strict();

/// Number format for an `Erlang` literal floating-point number.
#[rustfmt::skip]
pub const ERLANG_LITERAL: Options = Options::builder()
    .nan_string(options::ERLANG_LITERAL_NAN)
    .build_strict();

/// Number format to parse an `Erlang` float from string.
#[rustfmt::skip]
pub const ERLANG_STRING: Options = Options::builder()
    .nan_string(options::ERLANG_STRING)
    .inf_string(options::ERLANG_STRING)
    .infinity_string(options::ERLANG_STRING)
    .build_strict();

/// Number format for an `Elm` literal floating-point number.
#[rustfmt::skip]
pub const ELM_LITERAL: Options = Options::builder()
    .nan_string(options::ELM_LITERAL)
    .inf_string(options::ELM_LITERAL)
    .infinity_string(options::ELM_LITERAL)
    .build_strict();

/// Number format to parse an `Elm` float from string.
#[rustfmt::skip]
pub const ELM_STRING: Options = Options::builder()
    .nan_string(options::ELM_STRING_NAN)
    .inf_string(options::ELM_STRING_INF)
    .infinity_string(options::ELM_STRING_INFINITY)
    .build_strict();

/// Number format for a `Scala` literal floating-point number.
#[rustfmt::skip]
pub const SCALA_LITERAL: Options = Options::builder()
    .nan_string(options::SCALA_LITERAL)
    .inf_string(options::SCALA_LITERAL)
    .infinity_string(options::SCALA_LITERAL)
    .build_strict();

/// Number format to parse a `Scala` float from string.
#[rustfmt::skip]
pub const SCALA_STRING: Options = Options::builder()
    .inf_string(options::SCALA_STRING_INF)
    .infinity_string(options::SCALA_STRING_INFINITY)
    .build_strict();

/// Number format for an `Elixir` literal floating-point number.
#[rustfmt::skip]
pub const ELIXIR_LITERAL: Options = Options::builder()
    .nan_string(options::ELIXIR)
    .inf_string(options::ELIXIR)
    .infinity_string(options::ELIXIR)
    .build_strict();

/// Number format to parse an `Elixir` float from string.
#[rustfmt::skip]
pub const ELIXIR_STRING: Options = Options::builder()
    .nan_string(options::ELIXIR)
    .inf_string(options::ELIXIR)
    .infinity_string(options::ELIXIR)
    .build_strict();

/// Number format for a `FORTRAN` literal floating-point number.
#[rustfmt::skip]
pub const FORTRAN_LITERAL: Options = Options::builder()
    .nan_string(options::FORTRAN_LITERAL)
    .inf_string(options::FORTRAN_LITERAL)
    .infinity_string(options::FORTRAN_LITERAL)
    .build_strict();

/// Number format for a `D` literal floating-point number.
#[rustfmt::skip]
pub const D_LITERAL: Options = Options::builder()
    .nan_string(options::D_LITERAL)
    .inf_string(options::D_LITERAL)
    .infinity_string(options::D_LITERAL)
    .build_strict();

/// Number format for a `Coffeescript` literal floating-point number.
#[rustfmt::skip]
pub const COFFEESCRIPT_LITERAL: Options = Options::builder()
    .inf_string(options::COFFEESCRIPT_INF)
    .infinity_string(options::COFFEESCRIPT_INFINITY)
    .build_strict();

/// Number format to parse a `Coffeescript` float from string.
#[rustfmt::skip]
pub const COFFEESCRIPT_STRING: Options = Options::builder()
    .inf_string(options::COFFEESCRIPT_INF)
    .infinity_string(options::COFFEESCRIPT_INFINITY)
    .build_strict();

/// Number format for a `COBOL` literal floating-point number.
#[rustfmt::skip]
pub const COBOL_LITERAL: Options = Options::builder()
    .nan_string(options::COBOL)
    .inf_string(options::COBOL)
    .infinity_string(options::COBOL)
    .build_strict();

/// Number format to parse a `COBOL` float from string.
#[rustfmt::skip]
pub const COBOL_STRING: Options = Options::builder()
    .nan_string(options::COBOL)
    .inf_string(options::COBOL)
    .infinity_string(options::COBOL)
    .build_strict();

/// Number format for an `F#` literal floating-point number.
#[rustfmt::skip]
pub const FSHARP_LITERAL: Options = Options::builder()
    .nan_string(options::FSHARP_LITERAL_NAN)
    .inf_string(options::FSHARP_LITERAL_INF)
    .infinity_string(options::FSHARP_LITERAL_INFINITY)
    .build_strict();

/// Number format for a Visual Basic literal floating-point number.
#[rustfmt::skip]
pub const VB_LITERAL: Options = Options::builder()
    .nan_string(options::VB_LITERAL)
    .inf_string(options::VB_LITERAL)
    .infinity_string(options::VB_LITERAL)
    .build_strict();

/// Number format to parse a `Visual Basic` float from string.
#[rustfmt::skip]
pub const VB_STRING: Options = Options::builder()
    .inf_string(options::VB_STRING_INF)
    .infinity_string(options::VB_STRING_INFINITY)
    .build_strict();

/// Number format for an `OCaml` literal floating-point number.
#[rustfmt::skip]
pub const OCAML_LITERAL: Options = Options::builder()
    .nan_string(options::OCAML_LITERAL_NAN)
    .inf_string(options::OCAML_LITERAL_INF)
    .infinity_string(options::OCAML_LITERAL_INFINITY)
    .build_strict();

/// Number format for an `Objective-C` literal floating-point number.
#[rustfmt::skip]
pub const OBJECTIVEC_LITERAL: Options = Options::builder()
    .nan_string(options::OBJECTIVEC)
    .inf_string(options::OBJECTIVEC)
    .infinity_string(options::OBJECTIVEC)
    .build_strict();

/// Number format to parse an `Objective-C` float from string.
#[rustfmt::skip]
pub const OBJECTIVEC_STRING: Options = Options::builder()
    .nan_string(options::OBJECTIVEC)
    .inf_string(options::OBJECTIVEC)
    .infinity_string(options::OBJECTIVEC)
    .build_strict();

/// Number format for an `ReasonML` literal floating-point number.
#[rustfmt::skip]
pub const REASONML_LITERAL: Options = Options::builder()
    .nan_string(options::REASONML_LITERAL_NAN)
    .inf_string(options::REASONML_LITERAL_INF)
    .infinity_string(options::REASONML_LITERAL_INFINITY)
    .build_strict();

/// Number format for a `MATLAB` literal floating-point number.
#[rustfmt::skip]
pub const MATLAB_LITERAL: Options = Options::builder()
    .inf_string(options::MATLAB_LITERAL_INF)
    .infinity_string(options::MATLAB_LITERAL_INFINITY)
    .build_strict();

/// Number format for a `Zig` literal floating-point number.
#[rustfmt::skip]
pub const ZIG_LITERAL: Options = Options::builder()
    .nan_string(options::ZIG_LITERAL)
    .inf_string(options::ZIG_LITERAL)
    .infinity_string(options::ZIG_LITERAL)
    .build_strict();

/// Number format for a `Sage` literal floating-point number.
#[rustfmt::skip]
pub const SAGE_LITERAL: Options = Options::builder()
    .inf_string(options::SAGE_LITERAL_INF)
    .infinity_string(options::SAGE_LITERAL_INFINITY)
    .build_strict();

/// Number format for a `JSON` literal floating-point number.
#[rustfmt::skip]
pub const JSON: Options = Options::builder()
    .nan_string(options::JSON)
    .inf_string(options::JSON)
    .infinity_string(options::JSON)
    .build_strict();

/// Number format for a `TOML` literal floating-point number.
#[rustfmt::skip]
pub const TOML: Options = Options::builder()
    .nan_string(options::TOML)
    .inf_string(options::TOML)
    .infinity_string(options::TOML)
    .build_strict();

/// Number format for a `YAML` literal floating-point number.
#[rustfmt::skip]
pub const YAML: Options = JSON;

/// Number format for an `XML` literal floating-point number.
#[rustfmt::skip]
pub const XML: Options = Options::builder()
    .inf_string(options::XML_INF)
    .infinity_string(options::XML_INFINITY)
    .build_strict();

/// Number format for a `SQLite` literal floating-point number.
#[rustfmt::skip]
pub const SQLITE: Options = Options::builder()
    .nan_string(options::SQLITE)
    .inf_string(options::SQLITE)
    .infinity_string(options::SQLITE)
    .build_strict();

/// Number format for a `PostgreSQL` literal floating-point number.
#[rustfmt::skip]
pub const POSTGRESQL: Options = Options::builder()
    .nan_string(options::POSTGRESQL)
    .inf_string(options::POSTGRESQL)
    .infinity_string(options::POSTGRESQL)
    .build_strict();

/// Number format for a `MySQL` literal floating-point number.
#[rustfmt::skip]
pub const MYSQL: Options = Options::builder()
    .nan_string(options::MYSQL)
    .inf_string(options::MYSQL)
    .infinity_string(options::MYSQL)
    .build_strict();

/// Number format for a `MongoDB` literal floating-point number.
#[rustfmt::skip]
pub const MONGODB: Options = Options::builder()
    .inf_string(options::MONGODB_INF)
    .infinity_string(options::MONGODB_INFINITY)
    .build_strict();
