//! Configuration options for parsing and formatting numbers.
//!
//! This comprises 2 parts: a low-level API for generating packed structs
//! containing enumerating for number formats (both syntax and lexer).
//!
//! # Syntax Format
//!
//! The syntax format defines **which** numeric string are valid.
//! For example, if exponent notation is required or not
//! allowed.
//!
//! # Control Format
//!
//! The control format defines what characters are valid, that is, which
//! characters should be consider valid to continue tokenization.

#![cfg(feature = "format")]

use crate::error::Error;
use crate::format_builder::NumberFormatBuilder;
use crate::format_flags as flags;

/// Add multiple flags to `SyntaxFormat`.
macro_rules! from_flag {
    ($format:ident, $flag:ident) => {{
        $format & flags::$flag != 0
    }};
}

/// Helper to access features from the packed format struct.
///
/// This contains accessory methods to read the formatting settings
/// without using bitmasks directly on the underlying packed struct.
///
/// Some of the core functionality includes support for:
/// - Digit separators: ignored characters used to make numbers more readable,
///   such as `100,000`.
/// - Non-decimal radixes: writing or parsing numbers written in binary,
///   hexadecimal, or other bases.
/// - Special numbers: disabling support for special floating-point, such as
///   [`NaN`][f64::NAN].
/// - Number components: require signs, significant digits, and more.
///
/// This should always be constructed via [`NumberFormatBuilder`].
/// See [`NumberFormatBuilder`] for the fields for the packed struct.
///
/// # Examples
///
/// ```rust
/// # #[cfg(feature = "format")] {
/// use lexical_util::format::{RUST_LITERAL, NumberFormat};
///
/// let format = NumberFormat::<{ RUST_LITERAL }> {};
/// assert!(format.no_positive_mantissa_sign());
/// assert!(format.no_special());
/// assert!(format.internal_digit_separator());
/// assert!(format.trailing_digit_separator());
/// assert!(format.consecutive_digit_separator());
/// assert!(!format.no_exponent_notation());
/// # }
/// ```
pub struct NumberFormat<const FORMAT: u128>;

#[rustfmt::skip]
impl<const FORMAT: u128> NumberFormat<FORMAT> {
    // CONSTRUCTORS

    /// Create new instance (for methods and validation).
    ///
    /// This uses the same settings as in the `FORMAT` packed struct.
    #[inline(always)]
    pub const fn new() -> Self {
        Self {}
    }

    // VALIDATION

    /// Determine if the number format is valid.
    #[inline(always)]
    pub const fn is_valid(&self) -> bool {
        self.error().is_success()
    }

    /// Get the error type from the format.
    ///
    /// If [`Error::Success`] is returned, then no error occurred.
    #[inline(always)]
    pub const fn error(&self) -> Error {
        format_error_impl(FORMAT)
    }

    /// Determine if the radixes in the number format are valid.
    #[inline(always)]
    pub const fn is_valid_radix(&self) -> bool {
        self.error_radix().is_success()
    }

    /// Get the error type from the radix-only for the format.
    ///
    /// If [`Error::Success`] is returned, then no error occurred.
    #[inline(always)]
    pub const fn error_radix(&self) -> Error {
        radix_error_impl(FORMAT)
    }

    // NON-DIGIT SEPARATOR FLAGS & MASKS

    /// If digits are required before the decimal point.
    ///
    /// See [`required_integer_digits`][Self::required_integer_digits].
    pub const REQUIRED_INTEGER_DIGITS: bool = from_flag!(FORMAT, REQUIRED_INTEGER_DIGITS);

    /// Get if digits are required before the decimal point.
    ///
    /// Can only be modified with [`feature`][crate#features] `format`. Defaults
    /// to [`false`].
    ///
    /// # Examples
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1.1` | ✔️ |
    /// | `0.1` | ✔️ |
    /// | `1` | ✔️ |
    /// | `.1` | ❌ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    #[inline(always)]
    pub const fn required_integer_digits(&self) -> bool {
        Self::REQUIRED_INTEGER_DIGITS
    }

    /// If digits are required after the decimal point.
    ///
    /// See [`required_fraction_digits`][Self::required_fraction_digits].
    pub const REQUIRED_FRACTION_DIGITS: bool = from_flag!(FORMAT, REQUIRED_FRACTION_DIGITS);

    /// Get if digits are required after the decimal point.
    ///
    /// Can only be modified with [`feature`][crate#features] `format`. Defaults
    /// to [`false`].
    ///
    /// # Examples
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1.1` | ✔️ |
    /// | `1` | ✔️ |
    /// | `1.` | ❌ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    #[inline(always)]
    pub const fn required_fraction_digits(&self) -> bool {
        Self::REQUIRED_FRACTION_DIGITS
    }

    /// If digits are required after the exponent character.
    ///
    /// See [`required_exponent_digits`][Self::required_exponent_digits].
    pub const REQUIRED_EXPONENT_DIGITS: bool = from_flag!(FORMAT, REQUIRED_EXPONENT_DIGITS);

    /// Get if digits are required after the exponent character.
    ///
    /// Can only be modified with [`feature`][crate#features] `format`. Defaults
    /// to [`true`].
    ///
    /// # Examples
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1.1e+3` | ✔️ |
    /// | `1.1e3` | ✔️ |
    /// | `1.1e+` | ❌ |
    /// | `1.1e` | ❌ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    #[inline(always)]
    pub const fn required_exponent_digits(&self) -> bool {
        Self::REQUIRED_EXPONENT_DIGITS
    }

    /// If significant digits are required.
    ///
    /// See [`required_mantissa_digits`][Self::required_mantissa_digits].
    pub const REQUIRED_MANTISSA_DIGITS: bool = from_flag!(FORMAT, REQUIRED_MANTISSA_DIGITS);

    /// Get if at least 1 significant digit is required.
    ///
    /// If not required, then values like `.` (`0`) are valid, but empty strings
    /// are still invalid. Can only be modified with [`feature`][crate#features]
    /// `format`. Defaults to [`true`].
    ///
    /// # Examples
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1.1` | ✔️ |
    /// | `.` | ✔️ |
    /// | `e10` | ✔️ |
    /// | `.e10` | ✔️ |
    /// | `` | ❌ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    #[inline(always)]
    pub const fn required_mantissa_digits(&self) -> bool {
        Self::REQUIRED_MANTISSA_DIGITS
    }

    /// If at least 1 digit in the number is required.
    ///
    /// See [`required_digits`][Self::required_digits].
    pub const REQUIRED_DIGITS: bool = from_flag!(FORMAT, REQUIRED_DIGITS);

    /// Get if at least 1 digit in the number is required.
    ///
    /// This requires either [`mantissa`] or [`exponent`] digits.
    ///
    /// [`mantissa`]: Self::required_mantissa_digits
    /// [`exponent`]: Self::required_exponent_digits
    #[inline(always)]
    pub const fn required_digits(&self) -> bool {
        Self::REQUIRED_DIGITS
    }

    /// If a positive sign before the mantissa is not allowed.
    ///
    /// See [`no_positive_mantissa_sign`][Self::no_positive_mantissa_sign].
    pub const NO_POSITIVE_MANTISSA_SIGN: bool = from_flag!(FORMAT, NO_POSITIVE_MANTISSA_SIGN);

    /// Get if a positive sign before the mantissa is not allowed.
    ///
    /// Can only be modified with [`feature`][crate#features] `format`. Defaults
    /// to `false`.
    ///
    /// # Examples
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1.1` | ✔️ |
    /// | `-1.1` | ✔️ |
    /// | `+1.1` | ❌ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    /// - Parse Integer
    /// - Write Float
    #[inline(always)]
    pub const fn no_positive_mantissa_sign(&self) -> bool {
        Self::NO_POSITIVE_MANTISSA_SIGN
    }

    /// If a sign symbol before the mantissa is required.
    ///
    /// See [`required_mantissa_sign`][Self::required_mantissa_sign].
    pub const REQUIRED_MANTISSA_SIGN: bool = from_flag!(FORMAT, REQUIRED_MANTISSA_SIGN);

    /// Get if a sign symbol before the mantissa is required.
    ///
    /// Can only be modified with [`feature`][crate#features] `format`. Defaults
    /// to `false`.
    ///
    /// # Examples
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1.1` | ❌ |
    /// | `-1.1` | ✔️ |
    /// | `+1.1` | ✔️ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    /// - Parse Integer
    /// - Write Float
    #[inline(always)]
    pub const fn required_mantissa_sign(&self) -> bool {
        Self::REQUIRED_MANTISSA_SIGN
    }

    /// If exponent notation is not allowed.
    ///
    /// See [`no_exponent_notation`][Self::no_exponent_notation].
    pub const NO_EXPONENT_NOTATION: bool = from_flag!(FORMAT, NO_EXPONENT_NOTATION);

    /// Get if exponent notation is not allowed.
    ///
    /// Can only be modified with [`feature`][crate#features] `format`. Defaults
    /// to `false`.
    ///
    /// # Examples
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1` | ✔️ |
    /// | `1.1` | ✔️ |
    /// | `1.1e` | ❌ |
    /// | `1.1e5` | ❌ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    /// - Write Float
    #[inline(always)]
    pub const fn no_exponent_notation(&self) -> bool {
        Self::NO_EXPONENT_NOTATION
    }

    /// If a positive sign before the exponent is not allowed.
    ///
    /// See [`no_positive_exponent_sign`][Self::no_positive_exponent_sign].
    pub const NO_POSITIVE_EXPONENT_SIGN: bool = from_flag!(FORMAT, NO_POSITIVE_EXPONENT_SIGN);

    /// Get if a positive sign before the exponent is not allowed.
    ///
    /// Can only be modified with [`feature`][crate#features] `format`. Defaults
    /// to `false`.
    ///
    /// # Examples
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1.1e3` | ✔️ |
    /// | `1.1e-3` | ✔️ |
    /// | `1.1e+3` | ❌ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    /// - Write Float
    #[inline(always)]
    pub const fn no_positive_exponent_sign(&self) -> bool {
        Self::NO_POSITIVE_EXPONENT_SIGN
    }

    /// If a sign symbol before the exponent is required.
    ///
    /// See [`required_exponent_sign`][Self::required_exponent_sign].
    pub const REQUIRED_EXPONENT_SIGN: bool = from_flag!(FORMAT, REQUIRED_EXPONENT_SIGN);

    /// Get if a sign symbol before the exponent is required.
    ///
    /// Can only be modified with [`feature`][crate#features] `format`. Defaults
    /// to `false`.
    ///
    /// # Examples
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1.1e3` | ❌ |
    /// | `1.1e-3` | ✔️ |
    /// | `1.1e+3` | ✔️ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    /// - Write Float
    #[inline(always)]
    pub const fn required_exponent_sign(&self) -> bool {
        Self::REQUIRED_EXPONENT_SIGN
    }

    /// If an exponent without fraction is not allowed.
    ///
    /// See [`no_exponent_without_fraction`][Self::no_exponent_without_fraction].
    pub const NO_EXPONENT_WITHOUT_FRACTION: bool = from_flag!(FORMAT, NO_EXPONENT_WITHOUT_FRACTION);

    /// Get if an exponent without fraction is not allowed.
    ///
    /// Can only be modified with [`feature`][crate#features] `format`. Defaults
    /// to `false`.
    ///
    /// # Examples
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1e3` | ❌ |
    /// | `1.e3` | ❌ |
    /// | `1.1e` | ✔️ |
    /// | `.1e3` | ✔️ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    #[inline(always)]
    pub const fn no_exponent_without_fraction(&self) -> bool {
        Self::NO_EXPONENT_WITHOUT_FRACTION
    }

    /// If special (non-finite) values are not allowed.
    ///
    /// See [`no_special`][Self::no_special].
    pub const NO_SPECIAL: bool = from_flag!(FORMAT, NO_SPECIAL);

    /// Get if special (non-finite) values are not allowed.
    ///
    /// Can only be modified with [`feature`][crate#features] `format`. Defaults
    /// to `false`.
    ///
    /// # Examples
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `NaN` | ❌ |
    /// | `inf` | ❌ |
    /// | `-Infinity` | ❌ |
    /// | `1.1e` | ✔️ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    #[inline(always)]
    pub const fn no_special(&self) -> bool {
        Self::NO_SPECIAL
    }

    /// If special (non-finite) values are case-sensitive.
    ///
    /// See [`case_sensitive_special`][Self::case_sensitive_special].
    pub const CASE_SENSITIVE_SPECIAL: bool = from_flag!(FORMAT, CASE_SENSITIVE_SPECIAL);

    /// Get if special (non-finite) values are case-sensitive.
    ///
    /// If set to [`true`], then `NaN` and `nan` are treated as the same value
    /// ([Not a Number][f64::NAN]). Can only be modified with
    /// [`feature`][crate#features] `format`. Defaults to [`false`].
    ///
    /// # Used For
    ///
    /// - Parse Float
    #[inline(always)]
    pub const fn case_sensitive_special(&self) -> bool {
        Self::CASE_SENSITIVE_SPECIAL
    }

    /// If leading zeros before an integer are not allowed.
    ///
    /// See [`no_integer_leading_zeros`][Self::no_integer_leading_zeros].
    pub const NO_INTEGER_LEADING_ZEROS: bool = from_flag!(FORMAT, NO_INTEGER_LEADING_ZEROS);

    /// Get if leading zeros before an integer are not allowed.
    ///
    /// Can only be modified with [`feature`][crate#features] `format`. Defaults
    /// to [`false`].
    ///
    /// # Examples
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `01` | ❌ |
    /// | `0` | ✔️ |
    /// | `10` | ✔️ |
    ///
    /// # Used For
    ///
    /// - Parse Integer
    #[inline(always)]
    pub const fn no_integer_leading_zeros(&self) -> bool {
        Self::NO_INTEGER_LEADING_ZEROS
    }

    /// If leading zeros before a float are not allowed.
    ///
    /// See [`no_float_leading_zeros`][Self::no_float_leading_zeros].
    pub const NO_FLOAT_LEADING_ZEROS: bool = from_flag!(FORMAT, NO_FLOAT_LEADING_ZEROS);

    /// Get if leading zeros before a float are not allowed.
    ///
    /// This is before the significant digits of the float, that is, if there is
    /// 1 or more digits in the integral component and the leading digit is 0,
    /// Can only be modified with [`feature`][crate#features] `format`. Defaults
    /// to [`false`].
    ///
    /// # Examples
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `01` | ❌ |
    /// | `01.0` | ❌ |
    /// | `0` | ✔️ |
    /// | `10` | ✔️ |
    /// | `0.1` | ✔️ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    #[inline(always)]
    pub const fn no_float_leading_zeros(&self) -> bool {
        Self::NO_FLOAT_LEADING_ZEROS
    }

    /// If exponent notation is required.
    ///
    /// See [`required_exponent_notation`][Self::required_exponent_notation].
    pub const REQUIRED_EXPONENT_NOTATION: bool = from_flag!(FORMAT, REQUIRED_EXPONENT_NOTATION);

    /// Get if exponent notation is required.
    ///
    /// Can only be modified with [`feature`][crate#features] `format`. Defaults
    /// to [`false`].
    ///
    /// # Examples
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1` | ❌ |
    /// | `1.0` | ❌ |
    /// | `1e3` | ✔️ |
    /// | `1.1e3` | ✔️ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    /// - Write Float
    #[inline(always)]
    pub const fn required_exponent_notation(&self) -> bool {
        Self::REQUIRED_EXPONENT_NOTATION
    }

    /// If exponent characters are case-sensitive.
    ///
    /// See [`case_sensitive_exponent`][Self::case_sensitive_exponent].
    pub const CASE_SENSITIVE_EXPONENT: bool = from_flag!(FORMAT, CASE_SENSITIVE_EXPONENT);

    /// Get if exponent characters are case-sensitive.
    ///
    /// If set to [`true`], then the exponent character `e` would be considered
    /// the different from `E`. Can only be modified with
    /// [`feature`][crate#features] `format`. Defaults to [`false`].
    ///
    /// # Used For
    ///
    /// - Parse Float
    #[inline(always)]
    pub const fn case_sensitive_exponent(&self) -> bool {
        Self::CASE_SENSITIVE_EXPONENT
    }

    /// If base prefixes are case-sensitive.
    ///
    /// See [`case_sensitive_base_prefix`][Self::case_sensitive_base_prefix].
    pub const CASE_SENSITIVE_BASE_PREFIX: bool = from_flag!(FORMAT, CASE_SENSITIVE_BASE_PREFIX);

    /// Get if base prefixes are case-sensitive.
    ///
    /// If set to [`true`], then the base prefix `x` would be considered the
    /// different from `X`. Can only be modified with
    /// [`feature`][crate#features] `power-of-two` or `radix` along with
    /// `format`. Defaults to [`false`].
    ///
    /// # Used For
    ///
    /// - Parse Float
    /// - Parse Integer
    #[inline(always)]
    pub const fn case_sensitive_base_prefix(&self) -> bool {
        Self::CASE_SENSITIVE_BASE_PREFIX
    }

    /// If base suffixes are case-sensitive.
    ///
    /// See [`case_sensitive_base_suffix`][Self::case_sensitive_base_suffix].
    pub const CASE_SENSITIVE_BASE_SUFFIX: bool = from_flag!(FORMAT, CASE_SENSITIVE_BASE_SUFFIX);

    /// Get if base suffixes are case-sensitive.
    ///
    /// If set to [`true`], then the base suffix `x` would be considered the
    /// different from `X`. Can only be modified with
    /// [`feature`][crate#features] `power-of-two` or `radix` along with
    /// `format`. Defaults to [`false`].
    ///
    /// # Used For
    ///
    /// - Parse Float
    /// - Parse Integer
    #[inline(always)]
    pub const fn case_sensitive_base_suffix(&self) -> bool {
        Self::CASE_SENSITIVE_BASE_SUFFIX
    }

    // DIGIT SEPARATOR FLAGS & MASKS

    /// If digit separators are allowed between integer digits.
    ///
    /// This will not consider an input of only the digit separator
    /// to be a valid separator: the digit separator must be surrounded by
    /// digits.
    ///
    /// See [`integer_internal_digit_separator`][Self::integer_internal_digit_separator].
    pub const INTEGER_INTERNAL_DIGIT_SEPARATOR: bool = from_flag!(FORMAT, INTEGER_INTERNAL_DIGIT_SEPARATOR);

    /// Get if digit separators are allowed between integer digits.
    ///
    /// This will not consider an input of only the digit separator
    /// to be a valid separator: the digit separator must be surrounded by
    /// digits. Can only be modified with [`feature`][crate#features] `format`.
    /// Defaults to [`false`].
    ///
    /// # Examples
    ///
    /// Using a digit separator of `_`.
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1` | ✔️ |
    /// | `_` | ❌ |
    /// | `1_1` | ✔️ |
    /// | `1_` | ❌ |
    /// | `_1` | ❌ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    /// - Parse Integer
    #[inline(always)]
    pub const fn integer_internal_digit_separator(&self) -> bool {
        Self::INTEGER_INTERNAL_DIGIT_SEPARATOR
    }

    /// If digit separators are allowed between fraction digits.
    ///
    /// This will not consider an input of only the digit separator
    /// to be a valid separator: the digit separator must be surrounded by
    /// digits.
    ///
    /// See [`fraction_internal_digit_separator`][Self::fraction_internal_digit_separator].
    pub const FRACTION_INTERNAL_DIGIT_SEPARATOR: bool = from_flag!(FORMAT, FRACTION_INTERNAL_DIGIT_SEPARATOR);

    /// Get if digit separators are allowed between fraction digits.
    ///
    /// This will not consider an input of only the digit separator
    /// to be a valid separator: the digit separator must be surrounded by
    /// digits. Can only be modified with [`feature`][crate#features] `format`.
    /// Defaults to [`false`].
    ///
    /// # Examples
    ///
    /// Using a digit separator of `_`.
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1.1` | ✔️ |
    /// | `1._` | ❌ |
    /// | `1.1_1` | ✔️ |
    /// | `1.1_` | ❌ |
    /// | `1._1` | ❌ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    #[inline(always)]
    pub const fn fraction_internal_digit_separator(&self) -> bool {
        Self::FRACTION_INTERNAL_DIGIT_SEPARATOR
    }

    /// If digit separators are allowed between exponent digits.
    ///
    /// This will not consider an input of only the digit separator
    /// to be a valid separator: the digit separator must be surrounded by
    /// digits.
    ///
    /// See [`exponent_internal_digit_separator`][Self::exponent_internal_digit_separator].
    pub const EXPONENT_INTERNAL_DIGIT_SEPARATOR: bool = from_flag!(FORMAT, EXPONENT_INTERNAL_DIGIT_SEPARATOR);

    /// Get if digit separators are allowed between exponent digits.
    ///
    /// This will not consider an input of only the digit separator
    /// to be a valid separator: the digit separator must be surrounded by
    /// digits. Can only be modified with [`feature`][crate#features] `format`.
    /// Defaults to [`false`].
    ///
    /// # Examples
    ///
    /// Using a digit separator of `_`.
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1.1e1` | ✔️ |
    /// | `1.1e_` | ❌ |
    /// | `1.1e1_1` | ✔️ |
    /// | `1.1e1_` | ❌ |
    /// | `1.1e_1` | ❌ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    #[inline(always)]
    pub const fn exponent_internal_digit_separator(&self) -> bool {
        Self::EXPONENT_INTERNAL_DIGIT_SEPARATOR
    }

    /// If digit separators are allowed between digits.
    ///
    /// This will not consider an input of only the digit separator
    /// to be a valid separator: the digit separator must be surrounded by
    /// digits.
    ///
    /// See [`internal_digit_separator`][Self::internal_digit_separator].
    pub const INTERNAL_DIGIT_SEPARATOR: bool = from_flag!(FORMAT, INTERNAL_DIGIT_SEPARATOR);

    /// Get if digit separators are allowed between digits.
    ///
    /// This will not consider an input of only the digit separator
    /// to be a valid separator: the digit separator must be surrounded by
    /// digits. This is equivalent to any of [`integer_internal_digit_separator`],
    /// [`fraction_internal_digit_separator`], or
    /// [`exponent_internal_digit_separator`] being set.
    ///
    /// [`integer_internal_digit_separator`]: Self::integer_internal_digit_separator
    /// [`fraction_internal_digit_separator`]: Self::fraction_internal_digit_separator
    /// [`exponent_internal_digit_separator`]: Self::exponent_internal_digit_separator
    #[inline(always)]
    pub const fn internal_digit_separator(&self) -> bool {
        Self::INTERNAL_DIGIT_SEPARATOR
    }

    /// If a digit separator is allowed before any integer digits.
    ///
    /// This will consider an input of only the digit separator
    /// to be a identical to empty input.
    ///
    /// See [`integer_leading_digit_separator`][Self::integer_leading_digit_separator].
    pub const INTEGER_LEADING_DIGIT_SEPARATOR: bool = from_flag!(FORMAT, INTEGER_LEADING_DIGIT_SEPARATOR);

    /// Get if a digit separator is allowed before any integer digits.
    ///
    /// This will consider an input of only the digit separator
    /// to be a identical to empty input. Can only be modified with
    /// [`feature`][crate#features] `format`. Defaults to [`false`].
    ///
    /// # Examples
    ///
    /// Using a digit separator of `_`.
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1` | ✔️ |
    /// | `_` | ❌ |
    /// | `1_1` | ❌ |
    /// | `1_` | ❌ |
    /// | `_1` | ✔️ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    /// - Parse Integer
    #[inline(always)]
    pub const fn integer_leading_digit_separator(&self) -> bool {
        Self::INTEGER_LEADING_DIGIT_SEPARATOR
    }

    /// If a digit separator is allowed before any integer digits.
    ///
    /// This will consider an input of only the digit separator
    /// to be a identical to empty input.
    ///
    /// See [`fraction_leading_digit_separator`][Self::fraction_leading_digit_separator].
    pub const FRACTION_LEADING_DIGIT_SEPARATOR: bool = from_flag!(FORMAT, FRACTION_LEADING_DIGIT_SEPARATOR);

    /// Get if a digit separator is allowed before any fraction digits.
    ///
    /// This will consider an input of only the digit separator
    /// to be a identical to empty input. Can only be modified with
    /// [`feature`][crate#features] `format`. Defaults to [`false`].
    ///
    /// # Examples
    ///
    /// Using a digit separator of `_`.
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1.1` | ✔️ |
    /// | `1._` | ❌ |
    /// | `1.1_1` | ❌ |
    /// | `1.1_` | ❌ |
    /// | `1._1` | ✔️ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    #[inline(always)]
    pub const fn fraction_leading_digit_separator(&self) -> bool {
        Self::FRACTION_LEADING_DIGIT_SEPARATOR
    }

    /// If a digit separator is allowed before any exponent digits.
    ///
    /// This will consider an input of only the digit separator
    /// to be a identical to empty input.
    ///
    /// See [`exponent_leading_digit_separator`][Self::exponent_leading_digit_separator].
    pub const EXPONENT_LEADING_DIGIT_SEPARATOR: bool = from_flag!(FORMAT, EXPONENT_LEADING_DIGIT_SEPARATOR);

    /// Get if a digit separator is allowed before any exponent digits.
    ///
    /// This will consider an input of only the digit separator
    /// to be a identical to empty input. Can only be modified with
    /// [`feature`][crate#features] `format`. Defaults to [`false`].
    ///
    /// # Examples
    ///
    /// Using a digit separator of `_`.
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1.1e1` | ✔️ |
    /// | `1.1e_` | ❌ |
    /// | `1.1e1_1` | ❌ |
    /// | `1.1e1_` | ❌ |
    /// | `1.1e_1` | ✔️ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    #[inline(always)]
    pub const fn exponent_leading_digit_separator(&self) -> bool {
        Self::EXPONENT_LEADING_DIGIT_SEPARATOR
    }

    /// If a digit separator is allowed before any digits.
    ///
    /// This will consider an input of only the digit separator
    /// to be a identical to empty input.
    ///
    /// See [`leading_digit_separator`][Self::leading_digit_separator].
    pub const LEADING_DIGIT_SEPARATOR: bool = from_flag!(FORMAT, LEADING_DIGIT_SEPARATOR);

    /// Get if a digit separator is allowed before any digits.
    ///
    /// This will consider an input of only the digit separator
    /// to be a identical to empty input. This is equivalent to
    /// any of [`integer_leading_digit_separator`],
    /// [`fraction_leading_digit_separator`], or
    /// [`exponent_leading_digit_separator`] being set.
    ///
    /// [`integer_leading_digit_separator`]: Self::integer_leading_digit_separator
    /// [`fraction_leading_digit_separator`]: Self::fraction_leading_digit_separator
    /// [`exponent_leading_digit_separator`]: Self::exponent_leading_digit_separator
    #[inline(always)]
    pub const fn leading_digit_separator(&self) -> bool {
        Self::LEADING_DIGIT_SEPARATOR
    }

    /// If a digit separator is allowed after any integer digits.
    ///
    /// This will consider an input of only the digit separator
    /// to be a identical to empty input.
    ///
    /// See [`integer_trailing_digit_separator`][Self::integer_trailing_digit_separator].
    pub const INTEGER_TRAILING_DIGIT_SEPARATOR: bool = from_flag!(FORMAT, INTEGER_TRAILING_DIGIT_SEPARATOR);

    /// Get if a digit separator is allowed after any integer digits.
    ///
    /// This will consider an input of only the digit separator
    /// to be a identical to empty input. Can only be modified with
    /// [`feature`][crate#features] `format`. Defaults to [`false`].
    ///
    /// # Examples
    ///
    /// Using a digit separator of `_`.
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1` | ✔️ |
    /// | `_` | ❌ |
    /// | `1_1` | ❌ |
    /// | `1_` | ✔️ |
    /// | `_1` | ❌ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    /// - Parse Integer
    #[inline(always)]
    pub const fn integer_trailing_digit_separator(&self) -> bool {
        Self::INTEGER_TRAILING_DIGIT_SEPARATOR
    }

    /// If a digit separator is allowed after any fraction digits.
    ///
    /// This will consider an input of only the digit separator
    /// to be a identical to empty input.
    ///
    /// See [`fraction_trailing_digit_separator`][Self::fraction_trailing_digit_separator].
    pub const FRACTION_TRAILING_DIGIT_SEPARATOR: bool = from_flag!(FORMAT, FRACTION_TRAILING_DIGIT_SEPARATOR);

    /// Get if a digit separator is allowed after any fraction digits.
    ///
    /// This will consider an input of only the digit separator
    /// to be a identical to empty input. Can only be modified with
    /// [`feature`][crate#features] `format`. Defaults to [`false`]. # Examples
    ///
    /// Using a digit separator of `_`.
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1.1` | ✔️ |
    /// | `1._` | ❌ |
    /// | `1.1_1` | ❌ |
    /// | `1.1_` | ✔️ |
    /// | `1._1` | ❌ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    #[inline(always)]
    pub const fn fraction_trailing_digit_separator(&self) -> bool {
        Self::FRACTION_TRAILING_DIGIT_SEPARATOR
    }

    /// If a digit separator is allowed after any exponent digits.
    ///
    /// This will consider an input of only the digit separator
    /// to be a identical to empty input.
    ///
    /// See [`exponent_trailing_digit_separator`][Self::exponent_trailing_digit_separator].
    pub const EXPONENT_TRAILING_DIGIT_SEPARATOR: bool = from_flag!(FORMAT, EXPONENT_TRAILING_DIGIT_SEPARATOR);

    /// Get if a digit separator is allowed after any exponent digits.
    ///
    /// This will consider an input of only the digit separator
    /// to be a identical to empty input. Can only be modified with
    /// [`feature`][crate#features] `format`. Defaults to [`false`].
    ///
    /// # Examples
    ///
    /// Using a digit separator of `_`.
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1.1e1` | ✔️ |
    /// | `1.1e_` | ❌ |
    /// | `1.1e1_1` | ❌ |
    /// | `1.1e1_` | ✔️ |
    /// | `1.1e_1` | ❌ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    #[inline(always)]
    pub const fn exponent_trailing_digit_separator(&self) -> bool {
        Self::EXPONENT_TRAILING_DIGIT_SEPARATOR
    }

    /// If a digit separator is allowed after any digits.
    ///
    /// This will consider an input of only the digit separator
    /// to be a identical to empty input.
    ///
    /// See [`trailing_digit_separator`][Self::trailing_digit_separator].
    pub const TRAILING_DIGIT_SEPARATOR: bool = from_flag!(FORMAT, TRAILING_DIGIT_SEPARATOR);

    /// Get if a digit separator is allowed after any digits.
    ///
    /// This will consider an input of only the digit separator
    /// to be a identical to empty input. This is equivalent to
    /// any of [`integer_trailing_digit_separator`],
    /// [`fraction_trailing_digit_separator`], or
    /// [`exponent_trailing_digit_separator`] being set.
    ///
    /// [`integer_trailing_digit_separator`]: Self::integer_trailing_digit_separator
    /// [`fraction_trailing_digit_separator`]: Self::fraction_trailing_digit_separator
    /// [`exponent_trailing_digit_separator`]: Self::exponent_trailing_digit_separator
    #[inline(always)]
    pub const fn trailing_digit_separator(&self) -> bool {
        Self::TRAILING_DIGIT_SEPARATOR
    }

    /// If multiple consecutive integer digit separators are allowed.
    ///
    /// See [`integer_consecutive_digit_separator`][Self::integer_consecutive_digit_separator].
    pub const INTEGER_CONSECUTIVE_DIGIT_SEPARATOR: bool = from_flag!(FORMAT, INTEGER_CONSECUTIVE_DIGIT_SEPARATOR);

    /// Get if multiple consecutive integer digit separators are allowed.
    ///
    /// That is, using `_` as a digit separator `__` would be allowed where any
    /// digit separators (leading, trailing, internal) are allowed in the
    /// integer. Can only be modified with [`feature`][crate#features] `format`.
    /// Defaults to [`false`].
    ///
    /// # Used For
    ///
    /// - Parse Float
    /// - Parse Integer
    #[inline(always)]
    pub const fn integer_consecutive_digit_separator(&self) -> bool {
        Self::INTEGER_CONSECUTIVE_DIGIT_SEPARATOR
    }

    /// If multiple consecutive fraction digit separators are allowed.
    ///
    /// See [`fraction_consecutive_digit_separator`][Self::fraction_consecutive_digit_separator].
    pub const FRACTION_CONSECUTIVE_DIGIT_SEPARATOR: bool = from_flag!(FORMAT, FRACTION_CONSECUTIVE_DIGIT_SEPARATOR);

    /// Get if multiple consecutive fraction digit separators are allowed.
    ///
    /// That is, using `_` as a digit separator `__` would be allowed where any
    /// digit separators (leading, trailing, internal) are allowed in the
    /// fraction. Can only be modified with [`feature`][crate#features]
    /// `format`. Defaults to [`false`].
    ///
    /// # Used For
    ///
    /// - Parse Float
    #[inline(always)]
    pub const fn fraction_consecutive_digit_separator(&self) -> bool {
        Self::FRACTION_CONSECUTIVE_DIGIT_SEPARATOR
    }

    /// If multiple consecutive exponent digit separators are allowed.
    ///
    /// See [`exponent_consecutive_digit_separator`][Self::exponent_consecutive_digit_separator].
    pub const EXPONENT_CONSECUTIVE_DIGIT_SEPARATOR: bool = from_flag!(FORMAT, EXPONENT_CONSECUTIVE_DIGIT_SEPARATOR);

    /// Get if multiple consecutive exponent digit separators are allowed.
    ///
    /// That is, using `_` as a digit separator `__` would be allowed where any
    /// digit separators (leading, trailing, internal) are allowed in the
    /// exponent. Can only be modified with [`feature`][crate#features]
    /// `format`. Defaults to [`false`].
    ///
    /// # Used For
    ///
    /// - Parse Float
    #[inline(always)]
    pub const fn exponent_consecutive_digit_separator(&self) -> bool {
        Self::EXPONENT_CONSECUTIVE_DIGIT_SEPARATOR
    }

    /// If multiple consecutive digit separators are allowed.
    ///
    /// See [`consecutive_digit_separator`][Self::consecutive_digit_separator].
    pub const CONSECUTIVE_DIGIT_SEPARATOR: bool = from_flag!(FORMAT, CONSECUTIVE_DIGIT_SEPARATOR);

    /// Get if multiple consecutive digit separators are allowed.
    ///
    /// This is equivalent to any of [`integer_consecutive_digit_separator`],
    /// [`fraction_consecutive_digit_separator`], or
    /// [`exponent_consecutive_digit_separator`] being set.
    ///
    /// [`integer_consecutive_digit_separator`]: Self::integer_consecutive_digit_separator
    /// [`fraction_consecutive_digit_separator`]: Self::fraction_consecutive_digit_separator
    /// [`exponent_consecutive_digit_separator`]: Self::exponent_consecutive_digit_separator
    #[inline(always)]
    pub const fn consecutive_digit_separator(&self) -> bool {
        Self::CONSECUTIVE_DIGIT_SEPARATOR
    }

    /// If any digit separators are allowed in special (non-finite) values.
    ///
    /// See [`special_digit_separator`][Self::special_digit_separator].
    pub const SPECIAL_DIGIT_SEPARATOR: bool = from_flag!(FORMAT, SPECIAL_DIGIT_SEPARATOR);

    /// Get if any digit separators are allowed in special (non-finite) values.
    ///
    /// This enables leading, trailing, internal, and consecutive digit
    /// separators for any special floats: for example, `N__a_N_` is considered
    /// the same as `NaN`. Can only be modified with [`feature`][crate#features]
    /// `format`. Defaults to [`false`].
    ///
    /// # Used For
    ///
    /// - Parse Float
    #[inline(always)]
    pub const fn special_digit_separator(&self) -> bool {
        Self::SPECIAL_DIGIT_SEPARATOR
    }

    // CHARACTERS

    /// The digit separator character in the packed struct.
    ///
    /// See [`digit_separator`][Self::digit_separator].
    pub const DIGIT_SEPARATOR: u8 = flags::digit_separator(FORMAT);

    /// Get the digit separator for the number format.
    ///
    /// Digit separators are frequently used in number literals to group
    /// digits: `1,000,000` is a lot more readable than `1000000`, but
    /// the `,` characters should be ignored in the parsing of the number.
    ///
    /// Can only be modified with [`feature`][crate#features] `format`. Defaults
    /// to `0`, or no digit separators allowed.
    ///
    /// # Examples
    ///
    /// Using a digit separator of `_` (note that the validity
    /// oh where a digit separator can appear depends on the other digit
    /// separator flags).
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1` | ✔️ |
    /// | `1_4` | ✔️ |
    /// | `+_14` | ✔️ |
    /// | `+14e3_5` | ✔️ |
    /// | `1_d` | ❌ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    /// - Parse Integer
    #[inline(always)]
    pub const fn digit_separator(&self) -> u8 {
        Self::DIGIT_SEPARATOR
    }

    /// Get if the format has a digit separator.
    #[inline(always)]
    pub const fn has_digit_separator(&self) -> bool {
        self.digit_separator() != 0
    }

    /// The base prefix character in the packed struct.
    ///
    /// See [`base_prefix`][Self::base_prefix].
    pub const BASE_PREFIX: u8 = flags::base_prefix(FORMAT);

    /// Get the optional character for the base prefix.
    ///
    /// This character will come after a leading zero, so for example
    /// setting the base prefix to `x` means that a leading `0x` will
    /// be ignore, if present. Can only be modified with
    /// [`feature`][crate#features] `power-of-two` or `radix` along with
    /// `format`. Defaults to `0`, or no base prefix allowed.
    ///
    /// # Examples
    ///
    /// Using a base prefix of `x`.
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `0x1` | ✔️ |
    /// | `x1` | ❌ |
    /// | `1` | ✔️ |
    /// | `1x` | ❌ |
    /// | `1x1` | ❌ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    /// - Parse Integer
    #[inline(always)]
    pub const fn base_prefix(&self) -> u8 {
        Self::BASE_PREFIX
    }

    /// Get if the format has a base suffix.
    #[inline(always)]
    pub const fn has_base_prefix(&self) -> bool {
        self.base_prefix() != 0
    }

    /// The base suffix character in the packed struct.
    ///
    /// See [`base_suffix`][Self::base_suffix].
    pub const BASE_SUFFIX: u8 = flags::base_suffix(FORMAT);

    /// Get the optional character for the base suffix.
    ///
    /// This character will at the end of the buffer, so for example
    /// setting the base prefix to `x` means that a trailing `x` will
    /// be ignored, if present.  Can only be modified with
    /// [`feature`][crate#features] `power-of-two` or `radix` along with
    /// `format`. Defaults to `0`, or no base suffix allowed.
    ///
    /// # Examples
    ///
    /// Using a base suffix of `x`.
    ///
    /// | Input | Valid? |
    /// |:-:|:-:|
    /// | `1` | ✔️ |
    /// | `1x` | ✔️ |
    /// | `1d` | ❌ |
    ///
    /// # Used For
    ///
    /// - Parse Float
    /// - Parse Integer
    #[inline(always)]
    pub const fn base_suffix(&self) -> u8 {
        Self::BASE_SUFFIX
    }

    /// Get if the format has a base suffix.
    #[inline(always)]
    pub const fn has_base_suffix(&self) -> bool {
        self.base_suffix() != 0
    }

    // RADIX

    /// The radix for the significant digits in the packed struct.
    ///
    /// See [`mantissa_radix`][Self::mantissa_radix].
    pub const MANTISSA_RADIX: u32 = flags::mantissa_radix(FORMAT);

    /// Get the radix for mantissa digits.
    ///
    /// This is only used for the significant digits, that is, the integral and
    /// fractional components. Can only be modified with
    /// [`feature`][crate#features] `power-of-two` or `radix`. Defaults
    /// to `10`.
    ///
    /// | Radix | String | Number |
    /// |:-:|:-:|:-:|
    /// | 2 | "10011010010" | 1234 |
    /// | 3 | "1200201" | 1234 |
    /// | 8 | "2322" | 1234 |
    /// | 10 | "1234" | 1234 |
    /// | 16 | "4d2" | 1234 |
    /// | 31 | "18p" | 1234 |
    ///
    /// # Used For
    ///
    /// - Parse Float
    /// - Parse Integer
    /// - Write Float
    /// - Write Integer
    #[inline(always)]
    pub const fn mantissa_radix(&self) -> u32 {
        Self::MANTISSA_RADIX
    }

    /// The radix for the significant digits in the packed struct.
    ///
    /// Alias for [`MANTISSA_RADIX`][Self::MANTISSA_RADIX].
    pub const RADIX: u32 = Self::MANTISSA_RADIX;

    /// Get the radix for the significant digits.
    ///
    /// This is an alias for [`mantissa_radix`][Self::mantissa_radix].
    #[inline(always)]
    pub const fn radix(&self) -> u32 {
        Self::RADIX
    }

    /// Get the `radix^2` for the significant digits.
    #[inline(always)]
    pub const fn radix2(&self) -> u32 {
        self.radix().wrapping_mul(self.radix())
    }

    /// Get the `radix^4` for the significant digits.
    #[inline(always)]
    pub const fn radix4(&self) -> u32 {
        self.radix2().wrapping_mul(self.radix2())
    }

    /// Get the `radix^8` for the significant digits.
    #[inline(always)]
    pub const fn radix8(&self) -> u32 {
        // NOTE: radix >= 16 will overflow here but this has no security concerns
        self.radix4().wrapping_mul(self.radix4())
    }

    /// The base for the exponent.
    ///
    /// See [`exponent_base`][Self::exponent_base].
    pub const EXPONENT_BASE: u32 = flags::exponent_base(FORMAT);

    /// Get the radix for the exponent.
    ///
    /// For example, in `1.234e3`, it means `1.234 * 10^3`, and the exponent
    /// base here is 10. Some programming languages, like C, support hex floats
    /// with an exponent base of 2, for example `0x1.8p3`, or `1.5 * 2^3`.
    /// Defaults to `10`. Can only be modified with [`feature`][crate#features]
    /// `power-of-two` or `radix`. Defaults to `10`.
    ///
    /// # Used For
    ///
    /// - Parse Float
    /// - Parse Integer
    #[inline(always)]
    pub const fn exponent_base(&self) -> u32 {
        Self::EXPONENT_BASE
    }

    /// The radix for the exponent digits.
    ///
    /// See [`exponent_radix`][Self::exponent_radix].
    pub const EXPONENT_RADIX: u32 = flags::exponent_radix(FORMAT);

    /// Get the radix for exponent digits.
    ///
    /// This is only used for the exponent digits. We assume the radix for the
    /// significant digits ([`mantissa_radix`][Self::mantissa_radix]) is
    /// 10 as is the exponent base. Defaults to `10`. Can only be modified with
    /// [`feature`][crate#features] `power-of-two` or `radix`. Defaults to `10`.
    ///
    /// | Radix | String | Number |
    /// |:-:|:-:|:-:|
    /// | 2 | "1.234^1100" | 1.234e9 |
    /// | 3 | "1.234^110" | 1.234e9 |
    /// | 8 | "1.234^14" | 1.234e9 |
    /// | 10 | "1.234^12" | 1.234e9 |
    /// | 16 | "1.234^c" | 1.234e9 |
    /// | 31 | "1.234^c" | 1.234e9 |
    ///
    /// # Used For
    ///
    /// - Parse Float
    /// - Parse Integer
    #[inline(always)]
    pub const fn exponent_radix(&self) -> u32 {
        Self::EXPONENT_RADIX
    }

    // FLAGS

    /// Get the flags from the number format.
    ///
    /// This contains all the non-character and non-radix values
    /// in the packed struct.
    #[inline(always)]
    pub const fn flags(&self) -> u128 {
        FORMAT & flags::FLAG_MASK
    }

    /// Get the interface flags from the number format.
    ///
    /// This contains all the flags that dictate code flows, and
    /// therefore excludes logic like case-sensitive characters.
    #[inline(always)]
    pub const fn interface_flags(&self) -> u128 {
        FORMAT & flags::INTERFACE_FLAG_MASK
    }

    /// Get the digit separator flags from the number format.
    #[inline(always)]
    pub const fn digit_separator_flags(&self) -> u128 {
        FORMAT & flags::DIGIT_SEPARATOR_FLAG_MASK
    }

    /// Get the exponent flags from the number format.
    ///
    /// This contains all the flags pertaining to exponent
    /// formats, including digit separators.
    #[inline(always)]
    pub const fn exponent_flags(&self) -> u128 {
        FORMAT & flags::EXPONENT_FLAG_MASK
    }

    /// Get the integer digit separator flags from the number format.
    #[inline(always)]
    pub const fn integer_digit_separator_flags(&self) -> u128 {
        FORMAT & flags::INTEGER_DIGIT_SEPARATOR_FLAG_MASK
    }

    /// Get the fraction digit separator flags from the number format.
    #[inline(always)]
    pub const fn fraction_digit_separator_flags(&self) -> u128 {
        FORMAT & flags::FRACTION_DIGIT_SEPARATOR_FLAG_MASK
    }

    /// Get the exponent digit separator flags from the number format.
    #[inline(always)]
    pub const fn exponent_digit_separator_flags(&self) -> u128 {
        FORMAT & flags::EXPONENT_DIGIT_SEPARATOR_FLAG_MASK
    }

    // BUILDER

    /// Get [`NumberFormatBuilder`] as a static function.
    #[inline(always)]
    pub const fn builder() -> NumberFormatBuilder {
        NumberFormatBuilder::new()
    }

    /// Create [`NumberFormatBuilder`] using existing values.
    #[inline(always)]
    pub const fn rebuild() -> NumberFormatBuilder {
        NumberFormatBuilder::rebuild(FORMAT)
    }
}

impl<const FORMAT: u128> Default for NumberFormat<FORMAT> {
    fn default() -> Self {
        Self::new()
    }
}

/// Get if the radix is valid.
#[inline(always)]
pub(crate) const fn radix_error_impl(format: u128) -> Error {
    if !flags::is_valid_radix(flags::mantissa_radix(format)) {
        Error::InvalidMantissaRadix
    } else if !flags::is_valid_radix(flags::exponent_base(format)) {
        Error::InvalidExponentBase
    } else if !flags::is_valid_radix(flags::exponent_radix(format)) {
        Error::InvalidExponentRadix
    } else {
        Error::Success
    }
}

/// Get the error type from the format.
#[inline(always)]
#[allow(clippy::if_same_then_else)] // reason="all are different logic conditions"
pub(crate) const fn format_error_impl(format: u128) -> Error {
    if !flags::is_valid_radix(flags::mantissa_radix(format)) {
        Error::InvalidMantissaRadix
    } else if !flags::is_valid_radix(flags::exponent_base(format)) {
        Error::InvalidExponentBase
    } else if !flags::is_valid_radix(flags::exponent_radix(format)) {
        Error::InvalidExponentRadix
    } else if !flags::is_valid_digit_separator(format) {
        Error::InvalidDigitSeparator
    } else if !flags::is_valid_base_prefix(format) {
        Error::InvalidBasePrefix
    } else if !flags::is_valid_base_suffix(format) {
        Error::InvalidBaseSuffix
    } else if !flags::is_valid_punctuation(format) {
        Error::InvalidPunctuation
    } else if !flags::is_valid_exponent_flags(format) {
        Error::InvalidExponentFlags
    } else if from_flag!(format, NO_POSITIVE_MANTISSA_SIGN)
        && from_flag!(format, REQUIRED_MANTISSA_SIGN)
    {
        Error::InvalidMantissaSign
    } else if from_flag!(format, NO_POSITIVE_EXPONENT_SIGN)
        && from_flag!(format, REQUIRED_EXPONENT_SIGN)
    {
        Error::InvalidExponentSign
    } else if from_flag!(format, NO_SPECIAL) && from_flag!(format, CASE_SENSITIVE_SPECIAL) {
        Error::InvalidSpecial
    } else if from_flag!(format, NO_SPECIAL) && from_flag!(format, SPECIAL_DIGIT_SEPARATOR) {
        Error::InvalidSpecial
    } else if (format & flags::INTEGER_DIGIT_SEPARATOR_FLAG_MASK)
        == flags::INTEGER_CONSECUTIVE_DIGIT_SEPARATOR
    {
        Error::InvalidConsecutiveIntegerDigitSeparator
    } else if (format & flags::FRACTION_DIGIT_SEPARATOR_FLAG_MASK)
        == flags::FRACTION_CONSECUTIVE_DIGIT_SEPARATOR
    {
        Error::InvalidConsecutiveFractionDigitSeparator
    } else if (format & flags::EXPONENT_DIGIT_SEPARATOR_FLAG_MASK)
        == flags::EXPONENT_CONSECUTIVE_DIGIT_SEPARATOR
    {
        Error::InvalidConsecutiveExponentDigitSeparator
    } else {
        Error::Success
    }
}
