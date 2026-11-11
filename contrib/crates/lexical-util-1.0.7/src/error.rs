//! Error type for numeric parsing functions.
//!
//! The error type is C-compatible, simplifying use external language
//! bindings.

#![doc(hidden)]

use core::fmt;
#[cfg(feature = "std")]
use std::error;

/// Error code during parsing, indicating failure type.
#[non_exhaustive]
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Error {
    // PARSE ERRORS
    /// Integral overflow occurred during numeric parsing.
    Overflow(usize),
    /// Integral underflow occurred during numeric parsing.
    Underflow(usize),
    /// Invalid digit found before string termination.
    InvalidDigit(usize),
    /// Empty byte array found.
    Empty(usize),
    /// Empty mantissa found.
    EmptyMantissa(usize),
    /// Empty exponent found.
    EmptyExponent(usize),
    /// Empty integer found.
    EmptyInteger(usize),
    /// Empty fraction found.
    EmptyFraction(usize),
    /// Invalid positive mantissa sign was found.
    InvalidPositiveMantissaSign(usize),
    /// Mantissa sign was required(usize), but not found.
    MissingMantissaSign(usize),
    /// Exponent was present but not allowed.
    InvalidExponent(usize),
    /// Invalid positive exponent sign was found.
    InvalidPositiveExponentSign(usize),
    /// Exponent sign was required(usize), but not found.
    MissingExponentSign(usize),
    /// Exponent was present without fraction component.
    ExponentWithoutFraction(usize),
    /// Integer or integer component of float had invalid leading zeros.
    InvalidLeadingZeros(usize),
    /// No exponent with required exponent notation.
    MissingExponent(usize),
    /// Integral sign was required(usize), but not found.
    MissingSign(usize),
    /// Invalid positive sign for an integer was found.
    InvalidPositiveSign(usize),
    /// Invalid negative sign for an unsigned type was found.
    InvalidNegativeSign(usize),

    // NUMBER FORMAT ERRORS
    /// Invalid radix for the mantissa (significant) digits.
    InvalidMantissaRadix,
    /// Invalid base for the exponent.
    InvalidExponentBase,
    /// Invalid radix for the exponent digits.
    InvalidExponentRadix,
    /// Invalid digit separator character.
    InvalidDigitSeparator,
    /// Invalid decimal point character.
    InvalidDecimalPoint,
    /// Invalid symbol to represent exponent notation.
    InvalidExponentSymbol,
    /// Invalid character for a base prefix.
    InvalidBasePrefix,
    /// Invalid character for a base suffix.
    InvalidBaseSuffix,
    /// Invalid punctuation characters: multiple symbols overlap.
    InvalidPunctuation,
    /// Optional exponent flags were set while disabling exponent notation.
    InvalidExponentFlags,
    /// Set no positive mantissa sign while requiring mantissa signs.
    InvalidMantissaSign,
    /// Set no positive exponent sign while requiring exponent signs.
    InvalidExponentSign,
    /// Set optional special float flags while disable special floats.
    InvalidSpecial,
    /// Invalid consecutive integer digit separator.
    InvalidConsecutiveIntegerDigitSeparator,
    /// Invalid consecutive fraction digit separator.
    InvalidConsecutiveFractionDigitSeparator,
    /// Invalid consecutive exponent digit separator.
    InvalidConsecutiveExponentDigitSeparator,
    /// Invalid flags were set without the format feature.
    InvalidFlags,

    // OPTION ERRORS
    /// Invalid NaN string: must start with an `n` character.
    InvalidNanString,
    /// NaN string is too long.
    NanStringTooLong,
    /// Invalid short infinity string: must start with an `i` character.
    InvalidInfString,
    /// Short infinity string is too long.
    InfStringTooLong,
    /// Invalid long infinity string: must start with an `i` character.
    InvalidInfinityString,
    /// Long infinity string is too long.
    InfinityStringTooLong,
    /// Long infinity string is too short: it must be as long as short infinity.
    InfinityStringTooShort,
    /// Invalid float parsing algorithm.
    InvalidFloatParseAlgorithm,
    /// Invalid radix for the significant digits.
    InvalidRadix,
    /// Invalid precision flags for writing floats.
    InvalidFloatPrecision,
    /// Invalid negative exponent break: break is above 0.
    InvalidNegativeExponentBreak,
    /// Invalid positive exponent break: break is below 0.
    InvalidPositiveExponentBreak,

    // NOT AN ERROR
    /// An error did not actually occur, and the result was successful.
    Success,
}

macro_rules! is_error_type {
    ($name:ident, $type:ident$($t:tt)*) => (
        /// const fn check to see if an error is of a specific type.
        pub const fn $name(&self) -> bool {
            // Note: enum equality is not a const fn, so use a let expression.
            if let Self::$type$($t)* = self {
                true
            } else {
                false
            }
        }
    );
}

impl Error {
    /// Get a description of the error in a const, panic friendly way.
    #[inline]
    pub const fn description(&self) -> &'static str {
        match self {
            // PARSE ERRORS
            Self::Overflow(_) => "'numeric overflow occurred'",
            Self::Underflow(_) => "'numeric underflow occurred'",
            Self::InvalidDigit(_) => "'invalid digit found'",
            Self::Empty(_) => "'the string to parse was empty'",
            Self::EmptyMantissa(_) => "'no significant digits found'",
            Self::EmptyExponent(_) => "'exponent notation found without an exponent'",
            Self::EmptyInteger(_) => "'invalid float with no integer digits'",
            Self::EmptyFraction(_) => "'invalid float with no fraction digits'",
            Self::InvalidPositiveMantissaSign(_) => "'invalid `+` sign before significant digits'",
            Self::MissingMantissaSign(_) => "'missing required `+/-` sign for significant digits'",
            Self::InvalidExponent(_) => "'exponent found but not allowed'",
            Self::InvalidPositiveExponentSign(_) => "'invalid `+` sign in exponent'",
            Self::MissingExponentSign(_) => "'missing required `+/-` sign for exponent'",
            Self::ExponentWithoutFraction(_) =>  "'invalid float containing exponent without fraction'",
            Self::InvalidLeadingZeros(_) => "'invalid number with leading zeros before digits'",
            Self::MissingExponent(_) => "'missing required exponent'",
            Self::MissingSign(_) => "'missing required `+/-` sign for integer'",
            Self::InvalidPositiveSign(_) => "'invalid `+` sign for an integer was found'",
            Self::InvalidNegativeSign(_) => "'invalid `-` sign for an unsigned type was found'",

            // NUMBER FORMAT ERRORS
            Self::InvalidMantissaRadix => "'invalid radix for mantissa digits'",
            Self::InvalidExponentBase => "'invalid exponent base'",
            Self::InvalidExponentRadix => "'invalid radix for exponent digits'",
            Self::InvalidDigitSeparator => "'invalid digit separator: must be ASCII and not a digit or a `+/-` sign'",
            Self::InvalidDecimalPoint => "'invalid decimal point: must be ASCII and not a digit or a `+/-` sign'",
            Self::InvalidExponentSymbol => "'invalid exponent symbol: must be ASCII and not a digit or a `+/-` sign'",
            Self::InvalidBasePrefix => "'invalid base prefix character'",
            Self::InvalidBaseSuffix => "'invalid base suffix character'",
            Self::InvalidPunctuation => "'invalid punctuation: multiple characters overlap'",
            Self::InvalidExponentFlags => "'exponent flags set while disabling exponent notation'",
            Self::InvalidMantissaSign => "'disabled the `+` sign while requiring a sign for significant digits'",
            Self::InvalidExponentSign => "'disabled the `+` sign while requiring a sign for exponent digits'",
            Self::InvalidSpecial => "'special flags set while disabling special floats'",
            Self::InvalidConsecutiveIntegerDigitSeparator => "'enabled consecutive digit separators in the integer without setting a valid location'",
            Self::InvalidConsecutiveFractionDigitSeparator => "'enabled consecutive digit separators in the fraction without setting a valid location'",
            Self::InvalidConsecutiveExponentDigitSeparator => "'enabled consecutive digit separators in the exponent without setting a valid location'",
            Self::InvalidFlags => "'invalid flags enabled without the format feature'",

            // OPTION ERRORS
            Self::InvalidNanString => "'NaN string must started with `n`'",
            Self::NanStringTooLong => "'NaN string is too long'",
            Self::InvalidInfString => "'short infinity string must started with `i`'",
            Self::InfStringTooLong => "'short infinity string is too long'",
            Self::InvalidInfinityString => "'long infinity string must started with `i`'",
            Self::InfinityStringTooLong => "'long infinity string is too long'",
            Self::InfinityStringTooShort => "'long infinity string is too short'",
            Self::InvalidFloatParseAlgorithm => "'invalid combination of float parse algorithms'",
            Self::InvalidRadix => "'invalid radix for significant digits'",
            Self::InvalidFloatPrecision => "'invalid float precision: min digits is larger than max digits'",
            Self::InvalidNegativeExponentBreak => "'invalid negative exponent break: value is above 0'",
            Self::InvalidPositiveExponentBreak => "'invalid positive exponent break: value is below 0'",

            // NOT AN ERROR
            Self::Success => "'not actually an error'",
        }
    }

    /// Get the index for the parsing error.
    #[inline]
    pub fn index(&self) -> Option<&usize> {
        match self {
            // PARSE ERRORS
            Self::Overflow(index) => Some(index),
            Self::Underflow(index) => Some(index),
            Self::InvalidDigit(index) => Some(index),
            Self::Empty(index) => Some(index),
            Self::EmptyMantissa(index) => Some(index),
            Self::EmptyExponent(index) => Some(index),
            Self::EmptyInteger(index) => Some(index),
            Self::EmptyFraction(index) => Some(index),
            Self::InvalidPositiveMantissaSign(index) => Some(index),
            Self::MissingMantissaSign(index) => Some(index),
            Self::InvalidExponent(index) => Some(index),
            Self::InvalidPositiveExponentSign(index) => Some(index),
            Self::MissingExponentSign(index) => Some(index),
            Self::ExponentWithoutFraction(index) => Some(index),
            Self::InvalidLeadingZeros(index) => Some(index),
            Self::MissingExponent(index) => Some(index),
            Self::MissingSign(index) => Some(index),
            Self::InvalidPositiveSign(index) => Some(index),
            Self::InvalidNegativeSign(index) => Some(index),

            // NUMBER FORMAT ERRORS
            Self::InvalidMantissaRadix => None,
            Self::InvalidExponentBase => None,
            Self::InvalidExponentRadix => None,
            Self::InvalidDigitSeparator => None,
            Self::InvalidDecimalPoint => None,
            Self::InvalidExponentSymbol => None,
            Self::InvalidBasePrefix => None,
            Self::InvalidBaseSuffix => None,
            Self::InvalidPunctuation => None,
            Self::InvalidExponentFlags => None,
            Self::InvalidMantissaSign => None,
            Self::InvalidExponentSign => None,
            Self::InvalidSpecial => None,
            Self::InvalidConsecutiveIntegerDigitSeparator => None,
            Self::InvalidConsecutiveFractionDigitSeparator => None,
            Self::InvalidConsecutiveExponentDigitSeparator => None,
            Self::InvalidFlags => None,

            // OPTION ERRORS
            Self::InvalidNanString => None,
            Self::NanStringTooLong => None,
            Self::InvalidInfString => None,
            Self::InfStringTooLong => None,
            Self::InvalidInfinityString => None,
            Self::InfinityStringTooLong => None,
            Self::InfinityStringTooShort => None,
            Self::InvalidFloatParseAlgorithm => None,
            Self::InvalidRadix => None,
            Self::InvalidFloatPrecision => None,
            Self::InvalidNegativeExponentBreak => None,
            Self::InvalidPositiveExponentBreak => None,

            // NOT AN ERROR
            Self::Success => None,
        }
    }

    is_error_type!(is_overflow, Overflow(_));
    is_error_type!(is_underflow, Underflow(_));
    is_error_type!(is_invalid_digit, InvalidDigit(_));
    is_error_type!(is_empty, Empty(_));
    is_error_type!(is_empty_mantissa, EmptyMantissa(_));
    is_error_type!(is_empty_exponent, EmptyExponent(_));
    is_error_type!(is_empty_integer, EmptyInteger(_));
    is_error_type!(is_empty_fraction, EmptyFraction(_));
    is_error_type!(is_invalid_positive_mantissa_sign, InvalidPositiveMantissaSign(_));
    is_error_type!(is_missing_mantissa_sign, MissingMantissaSign(_));
    is_error_type!(is_invalid_exponent, InvalidExponent(_));
    is_error_type!(is_invalid_positive_exponent_sign, InvalidPositiveExponentSign(_));
    is_error_type!(is_missing_exponent_sign, MissingExponentSign(_));
    is_error_type!(is_exponent_without_fraction, ExponentWithoutFraction(_));
    is_error_type!(is_invalid_leading_zeros, InvalidLeadingZeros(_));
    is_error_type!(is_missing_exponent, MissingExponent(_));
    is_error_type!(is_missing_sign, MissingSign(_));
    is_error_type!(is_invalid_positive_sign, InvalidPositiveSign(_));
    is_error_type!(is_invalid_negative_sign, InvalidNegativeSign(_));
    is_error_type!(is_invalid_mantissa_radix, InvalidMantissaRadix);
    is_error_type!(is_invalid_exponent_base, InvalidExponentBase);
    is_error_type!(is_invalid_exponent_radix, InvalidExponentRadix);
    is_error_type!(is_invalid_digit_separator, InvalidDigitSeparator);
    is_error_type!(is_invalid_decimal_point, InvalidDecimalPoint);
    is_error_type!(is_invalid_exponent_symbol, InvalidExponentSymbol);
    is_error_type!(is_invalid_base_prefix, InvalidBasePrefix);
    is_error_type!(is_invalid_base_suffix, InvalidBaseSuffix);
    is_error_type!(is_invalid_punctuation, InvalidPunctuation);
    is_error_type!(is_invalid_exponent_flags, InvalidExponentFlags);
    is_error_type!(is_invalid_mantissa_sign, InvalidMantissaSign);
    is_error_type!(is_invalid_exponent_sign, InvalidExponentSign);
    is_error_type!(is_invalid_special, InvalidSpecial);
    is_error_type!(
        is_invalid_consecutive_integer_digit_separator,
        InvalidConsecutiveIntegerDigitSeparator
    );
    is_error_type!(
        is_invalid_consecutive_fraction_digit_separator,
        InvalidConsecutiveFractionDigitSeparator
    );
    is_error_type!(
        is_invalid_consecutive_exponent_digit_separator,
        InvalidConsecutiveExponentDigitSeparator
    );
    is_error_type!(is_invalid_flags, InvalidFlags);
    is_error_type!(is_invalid_nan_string, InvalidNanString);
    is_error_type!(is_nan_string_too_long, NanStringTooLong);
    is_error_type!(is_invalid_inf_string, InvalidInfString);
    is_error_type!(is_inf_string_too_long, InfStringTooLong);
    is_error_type!(is_invalid_infinity_string, InvalidInfinityString);
    is_error_type!(is_infinity_string_too_long, InfinityStringTooLong);
    is_error_type!(is_infinity_string_too_short, InfinityStringTooShort);
    is_error_type!(is_invalid_float_parse_algorithm, InvalidFloatParseAlgorithm);
    is_error_type!(is_invalid_radix, InvalidRadix);
    is_error_type!(is_invalid_float_precision, InvalidFloatPrecision);
    is_error_type!(is_invalid_negative_exponent_break, InvalidNegativeExponentBreak);
    is_error_type!(is_invalid_positive_exponent_break, InvalidPositiveExponentBreak);
    is_error_type!(is_success, Success);
}

/// Add an error message for parsing errors.
macro_rules! write_parse_error {
    ($formatter:ident, $message:expr, $index:ident) => {
        write!($formatter, "lexical parse error: {} at index {}", $message, $index)
    };
}

/// Add an error message for number format errors.
macro_rules! format_message {
    ($formatter:ident, $message:expr) => {
        write!($formatter, "lexical number format error: {}", $message)
    };
}

/// Add an error message for options errors.
macro_rules! options_message {
    ($formatter:ident, $message:expr) => {
        write!($formatter, "lexical options error: {}", $message)
    };
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let description = self.description();
        match self {
            // PARSE ERRORS
            Self::Overflow(index) => write_parse_error!(formatter, description, index),
            Self::Underflow(index) => write_parse_error!(formatter, description, index),
            Self::InvalidDigit(index) => write_parse_error!(formatter, description, index),
            Self::Empty(index) => write_parse_error!(formatter, description, index),
            Self::EmptyMantissa(index) => write_parse_error!(formatter, description, index),
            Self::EmptyExponent(index) => write_parse_error!(formatter, description, index),
            Self::EmptyInteger(index) => write_parse_error!(formatter, description, index),
            Self::EmptyFraction(index) => write_parse_error!(formatter, description, index),
            Self::InvalidPositiveMantissaSign(index) => {
                write_parse_error!(formatter, description, index)
            },
            Self::MissingMantissaSign(index) => write_parse_error!(formatter, description, index),
            Self::InvalidExponent(index) => write_parse_error!(formatter, description, index),
            Self::InvalidPositiveExponentSign(index) => {
                write_parse_error!(formatter, description, index)
            },
            Self::MissingExponentSign(index) => write_parse_error!(formatter, description, index),
            Self::ExponentWithoutFraction(index) => {
                write_parse_error!(formatter, description, index)
            },
            Self::InvalidLeadingZeros(index) => write_parse_error!(formatter, description, index),
            Self::MissingExponent(index) => write_parse_error!(formatter, description, index),
            Self::MissingSign(index) => write_parse_error!(formatter, description, index),
            Self::InvalidPositiveSign(index) => write_parse_error!(formatter, description, index),
            Self::InvalidNegativeSign(index) => write_parse_error!(formatter, description, index),

            // NUMBER FORMAT ERRORS
            Self::InvalidMantissaRadix => format_message!(formatter, description),
            Self::InvalidExponentBase => format_message!(formatter, description),
            Self::InvalidExponentRadix => format_message!(formatter, description),
            Self::InvalidDigitSeparator => format_message!(formatter, description),
            Self::InvalidDecimalPoint => format_message!(formatter, description),
            Self::InvalidExponentSymbol => format_message!(formatter, description),
            Self::InvalidBasePrefix => format_message!(formatter, description),
            Self::InvalidBaseSuffix => format_message!(formatter, description),
            Self::InvalidPunctuation => format_message!(formatter, description),
            Self::InvalidExponentFlags => format_message!(formatter, description),
            Self::InvalidMantissaSign => format_message!(formatter, description),
            Self::InvalidExponentSign => format_message!(formatter, description),
            Self::InvalidSpecial => format_message!(formatter, description),
            Self::InvalidConsecutiveIntegerDigitSeparator => {
                format_message!(formatter, description)
            },
            Self::InvalidConsecutiveFractionDigitSeparator => {
                format_message!(formatter, description)
            },
            Self::InvalidConsecutiveExponentDigitSeparator => {
                format_message!(formatter, description)
            },
            Self::InvalidFlags => format_message!(formatter, description),

            // OPTION ERRORS
            Self::InvalidNanString => options_message!(formatter, description),
            Self::NanStringTooLong => options_message!(formatter, description),
            Self::InvalidInfString => options_message!(formatter, description),
            Self::InfStringTooLong => options_message!(formatter, description),
            Self::InvalidInfinityString => options_message!(formatter, description),
            Self::InfinityStringTooLong => options_message!(formatter, description),
            Self::InfinityStringTooShort => options_message!(formatter, description),
            Self::InvalidFloatParseAlgorithm => options_message!(formatter, description),
            Self::InvalidRadix => options_message!(formatter, description),
            Self::InvalidFloatPrecision => options_message!(formatter, description),
            Self::InvalidNegativeExponentBreak => options_message!(formatter, description),
            Self::InvalidPositiveExponentBreak => options_message!(formatter, description),

            // NOT AN ERROR
            Self::Success => write!(formatter, "{description}"),
        }
    }
}

#[cfg(feature = "std")]
impl error::Error for Error {
}
