//! Configuration options for parsing integers.
//!
//! # Pre-Defined Formats
//!
//! This contains pre-defined options optimized for either the parsing of large
//! or small numbers.
//! - [`SMALL_NUMBERS`][`SMALL_NUMBERS`]: Optimize the parsing of small
//!   integers, at a major performance cost to larger values.
//! - [`LARGE_NUMBERS`][`LARGE_NUMBERS`]: Optimize the parsing of large
//!   integers, at a slight performance cost to smaller values.
//!
//! # Examples
//!
//! ```rust
//! use lexical_parse_integer::{FromLexicalWithOptions, Options};
//! use lexical_parse_integer::format::STANDARD;
//!
//! const OPTIONS: Options = Options::builder()
//!     .no_multi_digit(true)
//!     .build_strict();
//!
//! let value = "1234";
//! let result = u64::from_lexical_with_options::<STANDARD>(value.as_bytes(), &OPTIONS);
//! assert_eq!(result, Ok(1234));
//! ```

use lexical_util::options::ParseOptions;
use lexical_util::result::Result;

/// Builder for [`Options`].
///
/// # Examples
///
/// ```rust
/// use lexical_parse_integer::{FromLexicalWithOptions, Options};
/// use lexical_parse_integer::format::STANDARD;
///
/// const OPTIONS: Options = Options::builder()
///     .no_multi_digit(true)
///     .build_strict();
///
/// let value = "1234";
/// let result = u64::from_lexical_with_options::<STANDARD>(value.as_bytes(), &OPTIONS);
/// assert_eq!(result, Ok(1234));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct OptionsBuilder {
    /// Disable multi-digit optimizations.
    ///
    /// Using multi-digit optimizations allows parsing many digits
    /// from longer input strings at once which can dramatically
    /// improve performance (>70%) for long strings, but the
    /// increased branching can decrease performance for simple
    /// strings by 5-20%. Choose based on your inputs.
    no_multi_digit: bool,
}

impl OptionsBuilder {
    /// Create new options builder with default options.
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            no_multi_digit: true,
        }
    }

    // GETTERS

    /// Get if we disable the use of multi-digit optimizations.
    ///
    /// Defaults to [`true`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_integer::Options;
    ///
    /// let builder = Options::builder();
    /// assert_eq!(builder.get_no_multi_digit(), true);
    /// ```
    #[inline(always)]
    pub const fn get_no_multi_digit(&self) -> bool {
        self.no_multi_digit
    }

    // SETTERS

    /// Set if we disable the use of multi-digit optimizations.
    ///
    /// Defaults to [`true`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_integer::Options;
    ///
    /// const OPTIONS: Options = Options::builder()
    ///     .no_multi_digit(false)
    ///     .build_strict();
    /// assert_eq!(OPTIONS.get_no_multi_digit(), false);
    /// ```
    #[inline(always)]
    pub const fn no_multi_digit(mut self, no_multi_digit: bool) -> Self {
        self.no_multi_digit = no_multi_digit;
        self
    }

    // BUILDERS

    /// Check if the builder state is valid (always [`true`]).
    #[inline(always)]
    pub const fn is_valid(&self) -> bool {
        true
    }

    /// Build the [`Options`] struct without validation.
    ///
    /// <div class="warning">
    ///
    /// This is completely safe, however, misusing this could cause panics at
    /// runtime. Always check if [`is_valid`] prior to using the built
    /// options.
    ///
    /// </div>
    ///
    /// [`is_valid`]: Self::is_valid
    #[inline(always)]
    pub const fn build_unchecked(&self) -> Options {
        Options {
            no_multi_digit: self.no_multi_digit,
        }
    }

    /// Build the [`Options`] struct.
    ///
    /// This can never panic.
    ///
    /// <!-- # Panics
    ///
    /// If the built options are not valid. This should always
    /// be used within a const context to avoid panics at runtime.
    /// -->
    #[inline(always)]
    pub const fn build_strict(&self) -> Options {
        match self.build() {
            Ok(value) => value,
            Err(error) => core::panic!("{}", error.description()),
        }
    }

    /// Build the [`Options`] struct. Always [`Ok`].
    #[inline(always)]
    pub const fn build(&self) -> Result<Options> {
        Ok(self.build_unchecked())
    }
}

impl Default for OptionsBuilder {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

/// Options to customize the parsing integers.
///
/// # Examples
///
/// ```rust
/// use lexical_parse_integer::{FromLexicalWithOptions, Options};
/// use lexical_parse_integer::format::STANDARD;
///
/// const OPTIONS: Options = Options::builder()
///     .no_multi_digit(true)
///     .build_strict();
///
/// let value = "1234";
/// let result = u64::from_lexical_with_options::<STANDARD>(value.as_bytes(), &OPTIONS);
/// assert_eq!(result, Ok(1234));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Options {
    /// Disable multi-digit optimizations.
    ///
    /// Using multi-digit optimizations allows parsing many digits
    /// from longer input strings at once which can dramatically
    /// improve performance (>70%) for long strings, but the
    /// increased branching can decrease performance for simple
    /// strings by 5-20%. Choose based on your inputs.
    no_multi_digit: bool,
}

impl Options {
    /// Create [`Options`] with default values.
    #[must_use]
    #[inline(always)]
    pub const fn new() -> Self {
        Self::builder().build_unchecked()
    }

    /// Create the default options for a given radix.
    #[inline(always)]
    #[cfg(feature = "power-of-two")]
    pub const fn from_radix(_: u8) -> Self {
        Self::new()
    }

    // GETTERS

    /// Check if the builder state is valid (always [`true`]).
    #[inline(always)]
    pub const fn is_valid(&self) -> bool {
        self.rebuild().is_valid()
    }

    /// Get if we disable the use of multi-digit optimizations.
    ///
    /// Defaults to [`true`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_parse_integer::options::Options;
    ///
    /// const OPTIONS: Options = Options::builder()
    ///     .no_multi_digit(true)
    ///     .build_strict();
    /// assert_eq!(OPTIONS.get_no_multi_digit(), true);
    /// ```
    #[inline(always)]
    pub const fn get_no_multi_digit(&self) -> bool {
        self.no_multi_digit
    }

    // SETTERS

    /// Set if we disable the use of multi-digit optimizations.
    #[deprecated = "Setters should have a `set_` prefix. Use `set_no_multi_digit` instead. Will be removed in 2.0."]
    #[inline(always)]
    pub fn no_multi_digit(&mut self, no_multi_digit: bool) {
        self.no_multi_digit = no_multi_digit;
    }

    /// Set if we disable the use of multi-digit optimizations.
    #[deprecated = "Options should be treated as immutable, use `OptionsBuilder` instead. Will be removed in 2.0."]
    #[inline(always)]
    pub fn set_no_multi_digit(&mut self, no_multi_digit: bool) {
        self.no_multi_digit = no_multi_digit;
    }

    // BUILDERS

    /// Get [`OptionsBuilder`] as a static function.
    #[inline(always)]
    pub const fn builder() -> OptionsBuilder {
        OptionsBuilder::new()
    }

    /// Create [`OptionsBuilder`] using existing values.
    #[inline(always)]
    pub const fn rebuild(&self) -> OptionsBuilder {
        OptionsBuilder {
            no_multi_digit: self.no_multi_digit,
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

// PRE-DEFINED CONSTANTS
// ---------------------

/// Standard number format.
#[rustfmt::skip]
pub const STANDARD: Options = Options::new();

/// Options optimized for small numbers.
#[rustfmt::skip]
pub const SMALL_NUMBERS: Options = Options::builder()
    .no_multi_digit(true)
    .build_strict();

/// Options optimized for large numbers and long strings.
#[rustfmt::skip]
pub const LARGE_NUMBERS: Options = Options::builder()
    .no_multi_digit(false)
    .build_strict();
