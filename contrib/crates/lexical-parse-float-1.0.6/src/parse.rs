//! Shared trait and methods for parsing floats.
//!
//! This is adapted from [fast-float-rust](https://github.com/aldanor/fast-float-rust),
//! a port of [fast_float](https://github.com/fastfloat/fast_float) to Rust.

// NOTE: We never want to disable multi-digit optimizations when parsing our floats,
// since the nanoseconds it saves on branching is irrelevant when considering decimal
// points and fractional digits and it majorly improves longer floats.

#![doc(hidden)]

#[cfg(not(feature = "compact"))]
use lexical_parse_integer::algorithm;
#[cfg(feature = "f16")]
use lexical_util::bf16::bf16;
use lexical_util::digit::{char_to_digit_const, char_to_valid_digit_const};
use lexical_util::error::Error;
#[cfg(feature = "f16")]
use lexical_util::f16::f16;
use lexical_util::format::NumberFormat;
use lexical_util::iterator::{AsBytes, Bytes, DigitsIter, Iter};
use lexical_util::result::Result;
use lexical_util::step::u64_step;

#[cfg(any(feature = "compact", feature = "radix"))]
use crate::bellerophon::bellerophon;
#[cfg(feature = "power-of-two")]
use crate::binary::{binary, slow_binary};
use crate::float::{extended_to_float, ExtendedFloat80, LemireFloat};
#[cfg(not(feature = "compact"))]
use crate::lemire::lemire;
use crate::number::Number;
use crate::options::Options;
use crate::shared;
use crate::slow::slow_radix;

// API
// ---

/// Check if the radix is a power-of-2.
#[cfg(feature = "power-of-two")]
macro_rules! is_power_two {
    ($radix:expr) => {
        matches!($radix, 2 | 4 | 8 | 16 | 32)
    };
}

/// Check if the radix is valid and error otherwise
#[cfg(feature = "power-of-two")]
macro_rules! check_radix {
    ($format:ident) => {{
        let format = NumberFormat::<{ $format }> {};
        if format.error() != Error::Success {
            return Err(Error::InvalidRadix);
        } else if format.radix() != format.exponent_base() {
            let valid_radix = matches!(
                (format.radix(), format.exponent_base()),
                (4, 2) | (8, 2) | (16, 2) | (32, 2) | (16, 4)
            );
            if !valid_radix {
                return Err(Error::InvalidRadix);
            }
        }
    }};
}

/// Check if the decimal radix is valid and error otherwise.
#[cfg(not(feature = "power-of-two"))]
macro_rules! check_radix {
    ($format:ident) => {{
        let format = NumberFormat::<{ $format }> {};
        if format.error() != Error::Success {
            return Err(Error::InvalidRadix);
        }
    }};
}

/// Parse integer trait, implemented in terms of the optimized back-end.
pub trait ParseFloat: LemireFloat {
    /// Forward complete parser parameters to the backend.
    #[cfg_attr(not(feature = "compact"), inline(always))]
    fn parse_complete<const FORMAT: u128>(bytes: &[u8], options: &Options) -> Result<Self> {
        check_radix!(FORMAT);
        parse_complete::<Self, FORMAT>(bytes, options)
    }

    /// Forward partial parser parameters to the backend.
    #[cfg_attr(not(feature = "compact"), inline(always))]
    fn parse_partial<const FORMAT: u128>(bytes: &[u8], options: &Options) -> Result<(Self, usize)> {
        check_radix!(FORMAT);
        parse_partial::<Self, FORMAT>(bytes, options)
    }

    /// Forward complete parser parameters to the backend, using only the fast
    /// path.
    #[cfg_attr(not(feature = "compact"), inline(always))]
    fn fast_path_complete<const FORMAT: u128>(bytes: &[u8], options: &Options) -> Result<Self> {
        check_radix!(FORMAT);
        fast_path_complete::<Self, FORMAT>(bytes, options)
    }

    /// Forward partial parser parameters to the backend, using only the fast
    /// path.
    #[cfg_attr(not(feature = "compact"), inline(always))]
    fn fast_path_partial<const FORMAT: u128>(
        bytes: &[u8],
        options: &Options,
    ) -> Result<(Self, usize)> {
        check_radix!(FORMAT);
        fast_path_partial::<Self, FORMAT>(bytes, options)
    }
}

macro_rules! parse_float_impl {
    ($($t:ty)*) => ($(
        impl ParseFloat for $t {}
    )*)
}

parse_float_impl! { f32 f64 }

#[cfg(feature = "f16")]
macro_rules! parse_float_as_f32 {
    ($($t:ty)*) => ($(
        impl ParseFloat for $t {
            #[cfg_attr(not(feature = "compact"), inline(always))]
            fn parse_complete<const FORMAT: u128>(bytes: &[u8], options: &Options)
                -> Result<Self>
            {
                Ok(Self::from_f32(parse_complete::<f32, FORMAT>(bytes, options)?))
            }

            #[cfg_attr(not(feature = "compact"), inline(always))]
            fn parse_partial<const FORMAT: u128>(bytes: &[u8], options: &Options)
                -> Result<(Self, usize)>
            {
                let (float, count) = parse_partial::<f32, FORMAT>(bytes, options)?;
                Ok((Self::from_f32(float), count))
            }

            #[cfg_attr(not(feature = "compact"), inline(always))]
            fn fast_path_complete<const FORMAT: u128>(bytes: &[u8], options: &Options)
                -> Result<Self>
            {
                Ok(Self::from_f32(fast_path_complete::<f32, FORMAT>(bytes, options)?))
            }

            #[cfg_attr(not(feature = "compact"), inline(always))]
            fn fast_path_partial<const FORMAT: u128>(bytes: &[u8], options: &Options)
                -> Result<(Self, usize)>
            {
                let (float, count) = fast_path_partial::<f32, FORMAT>(bytes, options)?;
                Ok((Self::from_f32(float), count))
            }
        }
    )*)
}

#[cfg(feature = "f16")]
parse_float_as_f32! { bf16 f16 }

// PARSE
// -----

// NOTE:
//  The partial and complete parsers are done separately because it provides
//  minor optimizations when parsing invalid input, and the logic is slightly
//  different internally. Most of the code is shared, so the duplicated
//  code is only like 30 lines.

/// Parse the sign from the leading digits.
#[cfg_attr(not(feature = "compact"), inline(always))]
pub fn parse_mantissa_sign<const FORMAT: u128>(byte: &mut Bytes<'_, FORMAT>) -> Result<bool> {
    let format = NumberFormat::<{ FORMAT }> {};
    parse_sign!(
        byte,
        true,
        format.no_positive_mantissa_sign(),
        format.required_mantissa_sign(),
        InvalidPositiveSign,
        MissingSign
    )
}

/// Parse the sign from the leading digits.
#[cfg_attr(not(feature = "compact"), inline(always))]
pub fn parse_exponent_sign<const FORMAT: u128>(byte: &mut Bytes<'_, FORMAT>) -> Result<bool> {
    let format = NumberFormat::<{ FORMAT }> {};
    parse_sign!(
        byte,
        true,
        format.no_positive_exponent_sign(),
        format.required_exponent_sign(),
        InvalidPositiveExponentSign,
        MissingExponentSign
    )
}

/// Utility to extract the result and handle any errors from parsing a `Number`.
///
/// - `format` - The numerical format as a packed integer
/// - `byte` - The `DigitsIter` iterator
/// - `is_negative` - If the final value is negative
/// - `parse_normal` - The function to parse non-special numbers with
/// - `parse_special` - The function to parse special numbers with
macro_rules! parse_number {
    (
        $format:ident,
        $byte:ident,
        $is_negative:ident,
        $options:ident,
        $parse_normal:ident,
        $parse_special:ident
    ) => {{
        match $parse_normal::<$format>($byte.clone(), $is_negative, $options) {
            Ok(n) => n,
            Err(e) => {
                if let Some(value) =
                    $parse_special::<_, $format>($byte.clone(), $is_negative, $options)
                {
                    return Ok(value);
                } else {
                    return Err(e);
                }
            },
        }
    }};
}

/// Convert extended float to native.
///
/// - `type` - The native floating point type.
/// - `fp` - The extended floating-point representation.
macro_rules! to_native {
    ($type:ident, $fp:ident, $is_negative:ident) => {{
        let mut float = extended_to_float::<$type>($fp);
        if $is_negative {
            float = -float;
        }
        float
    }};
}

/// Parse a float from bytes using a complete parser.
#[inline(always)]
#[allow(clippy::missing_inline_in_public_items)] // reason = "only public for testing"
pub fn parse_complete<F: LemireFloat, const FORMAT: u128>(
    bytes: &[u8],
    options: &Options,
) -> Result<F> {
    let mut byte = bytes.bytes::<{ FORMAT }>();
    let is_negative = parse_mantissa_sign(&mut byte)?;
    if byte.integer_iter().is_consumed() {
        if NumberFormat::<FORMAT>::REQUIRED_INTEGER_DIGITS
            || NumberFormat::<FORMAT>::REQUIRED_MANTISSA_DIGITS
        {
            return Err(Error::Empty(byte.cursor()));
        } else {
            return Ok(F::ZERO);
        }
    }

    // Parse our a small representation of our number.
    let num: Number<'_> =
        parse_number!(FORMAT, byte, is_negative, options, parse_complete_number, parse_special);
    // Try the fast-path algorithm.
    if let Some(value) = num.try_fast_path::<_, FORMAT>() {
        return Ok(value);
    }
    // Now try the moderate path algorithm.
    let mut fp = moderate_path::<F, FORMAT>(&num, options.lossy());

    // Unable to correctly round the float using the fast or moderate algorithms.
    // Fallback to a slower, but always correct algorithm. If we have
    // lossy, we can't be here.
    if fp.exp < 0 {
        debug_assert!(!options.lossy(), "lossy algorithms never use slow algorithms");
        // Undo the invalid extended float biasing.
        fp.exp -= shared::INVALID_FP;
        fp = slow_path::<F, FORMAT>(num, fp);
    }

    // Convert to native float and return result.
    Ok(to_native!(F, fp, is_negative))
}

/// Parse a float using only the fast path as a complete parser.
#[inline(always)]
#[allow(clippy::missing_inline_in_public_items)] // reason = "only public for testing"
pub fn fast_path_complete<F: LemireFloat, const FORMAT: u128>(
    bytes: &[u8],
    options: &Options,
) -> Result<F> {
    let mut byte = bytes.bytes::<{ FORMAT }>();
    let is_negative = parse_mantissa_sign(&mut byte)?;
    if byte.integer_iter().is_consumed() {
        if NumberFormat::<FORMAT>::REQUIRED_INTEGER_DIGITS
            || NumberFormat::<FORMAT>::REQUIRED_MANTISSA_DIGITS
        {
            return Err(Error::Empty(byte.cursor()));
        } else {
            return Ok(F::ZERO);
        }
    }

    // Parse our a small representation of our number.
    let num =
        parse_number!(FORMAT, byte, is_negative, options, parse_complete_number, parse_special);
    Ok(num.force_fast_path::<_, FORMAT>())
}

/// Parse a float from bytes using a partial parser.
#[inline(always)]
#[allow(clippy::missing_inline_in_public_items)] // reason = "only public for testing"
pub fn parse_partial<F: LemireFloat, const FORMAT: u128>(
    bytes: &[u8],
    options: &Options,
) -> Result<(F, usize)> {
    let mut byte = bytes.bytes::<{ FORMAT }>();
    let is_negative = parse_mantissa_sign(&mut byte)?;
    if byte.integer_iter().is_consumed() {
        if NumberFormat::<FORMAT>::REQUIRED_INTEGER_DIGITS
            || NumberFormat::<FORMAT>::REQUIRED_MANTISSA_DIGITS
        {
            return Err(Error::Empty(byte.cursor()));
        } else {
            return Ok((F::ZERO, byte.cursor()));
        }
    }

    // Parse our a small representation of our number.
    let (num, count) = parse_number!(
        FORMAT,
        byte,
        is_negative,
        options,
        parse_partial_number,
        parse_partial_special
    );
    // Try the fast-path algorithm.
    if let Some(value) = num.try_fast_path::<_, FORMAT>() {
        return Ok((value, count));
    }
    // Now try the moderate path algorithm.
    let mut fp = moderate_path::<F, FORMAT>(&num, options.lossy());

    // Unable to correctly round the float using the fast or moderate algorithms.
    // Fallback to a slower, but always correct algorithm. If we have
    // lossy, we can't be here.
    if fp.exp < 0 {
        debug_assert!(!options.lossy(), "lossy algorithms never use slow algorithms");
        // Undo the invalid extended float biasing.
        fp.exp -= shared::INVALID_FP;
        fp = slow_path::<F, FORMAT>(num, fp);
    }

    // Convert to native float and return result.
    Ok((to_native!(F, fp, is_negative), count))
}

/// Parse a float using only the fast path as a partial parser.
#[inline(always)]
#[allow(clippy::missing_inline_in_public_items)] // reason = "only public for testing"
pub fn fast_path_partial<F: LemireFloat, const FORMAT: u128>(
    bytes: &[u8],
    options: &Options,
) -> Result<(F, usize)> {
    let mut byte = bytes.bytes::<{ FORMAT }>();
    let is_negative = parse_mantissa_sign(&mut byte)?;
    if byte.integer_iter().is_consumed() {
        if NumberFormat::<FORMAT>::REQUIRED_INTEGER_DIGITS
            || NumberFormat::<FORMAT>::REQUIRED_MANTISSA_DIGITS
        {
            return Err(Error::Empty(byte.cursor()));
        } else {
            return Ok((F::ZERO, byte.cursor()));
        }
    }

    // Parse our a small representation of our number.
    let (num, count) = parse_number!(
        FORMAT,
        byte,
        is_negative,
        options,
        parse_partial_number,
        parse_partial_special
    );
    Ok((num.force_fast_path::<_, FORMAT>(), count))
}

// PATHS
// -----

/// Wrapper for different moderate-path algorithms.
/// A return exponent of `-1` indicates an invalid value.
#[must_use]
#[inline(always)]
pub fn moderate_path<F: LemireFloat, const FORMAT: u128>(
    num: &Number,
    lossy: bool,
) -> ExtendedFloat80 {
    #[cfg(feature = "compact")]
    {
        #[cfg(feature = "power-of-two")]
        {
            let format = NumberFormat::<{ FORMAT }> {};
            if is_power_two!(format.mantissa_radix()) {
                // Implement the power-of-two backends.
                binary::<F, FORMAT>(num, lossy)
            } else {
                bellerophon::<F, FORMAT>(num, lossy)
            }
        }

        #[cfg(not(feature = "power-of-two"))]
        {
            bellerophon::<F, FORMAT>(num, lossy)
        }
    }

    #[cfg(not(feature = "compact"))]
    {
        #[cfg(feature = "radix")]
        {
            let format = NumberFormat::<{ FORMAT }> {};
            let radix = format.mantissa_radix();
            if radix == 10 {
                lemire::<F>(num, lossy)
            } else if is_power_two!(radix) {
                // Implement the power-of-two backends.
                binary::<F, FORMAT>(num, lossy)
            } else {
                bellerophon::<F, FORMAT>(num, lossy)
            }
        }

        #[cfg(all(feature = "power-of-two", not(feature = "radix")))]
        {
            let format = NumberFormat::<{ FORMAT }> {};
            let radix = format.mantissa_radix();
            debug_assert!(matches!(radix, 2 | 4 | 8 | 10 | 16 | 32));
            if radix == 10 {
                lemire::<F>(num, lossy)
            } else {
                // Implement the power-of-two backends.
                binary::<F, FORMAT>(num, lossy)
            }
        }

        #[cfg(not(feature = "power-of-two"))]
        {
            lemire::<F>(num, lossy)
        }
    }
}

/// Invoke the slow path.
/// At this point, the float string has already been validated.
#[must_use]
#[inline(always)]
pub fn slow_path<F: LemireFloat, const FORMAT: u128>(
    num: Number,
    fp: ExtendedFloat80,
) -> ExtendedFloat80 {
    #[cfg(not(feature = "power-of-two"))]
    {
        slow_radix::<F, FORMAT>(num, fp)
    }

    #[cfg(feature = "power-of-two")]
    {
        let format = NumberFormat::<{ FORMAT }> {};
        if is_power_two!(format.mantissa_radix()) {
            slow_binary::<F, FORMAT>(num)
        } else {
            slow_radix::<F, FORMAT>(num, fp)
        }
    }
}

// NUMBER
// ------

/// Parse a partial, non-special floating point number.
///
/// This creates a representation of the float as the
/// significant digits and the decimal exponent.
#[cfg_attr(not(feature = "compact"), inline(always))]
#[allow(unused_mut)] // reason = "used when format is enabled"
#[allow(clippy::unwrap_used)] // reason = "developer error if we incorrectly assume an overflow"
#[allow(clippy::collapsible_if)] // reason = "more readable uncollapsed"
#[allow(clippy::cast_possible_wrap)] // reason = "no hardware supports buffers >= i64::MAX"
#[allow(clippy::too_many_lines)] // reason = "function is one logical entity"
pub fn parse_number<'a, const FORMAT: u128, const IS_PARTIAL: bool>(
    mut byte: Bytes<'a, FORMAT>,
    is_negative: bool,
    options: &Options,
) -> Result<(Number<'a>, usize)> {
    //  NOTE:
    //      There are no satisfactory optimizations to reduce the number
    //      of multiplications for very long input strings, but this will
    //      be a small fraction of the performance penalty anyway.
    //
    //      We've tried:
    //          - checking for explicit overflow, via `overflowing_mul`.
    //          - counting the max number of steps.
    //          - subslicing the string, and only processing the first `step`
    //            digits.
    //          - pre-computing the maximum power, and only adding until then.
    //
    //      All of these lead to substantial performance penalty.
    //      If we pre-parse the string, then only process it then, we
    //      get a performance penalty of ~2.5x (20ns to 50ns) for common
    //      floats, an unacceptable cost, while only improving performance
    //      for rare floats 5-25% (9.3µs to 7.5µs for denormal with 6400
    //      digits, and 7.8µs to 7.4µs for large floats with 6400 digits).
    //
    //      The performance cost is **almost** entirely in this function,
    //      but additional branching **does** not improve performance,
    //      and pre-tokenization is a recipe for failure. For halfway
    //      cases with smaller numbers of digits, the majority of the
    //      performance cost is in the big integer arithmetic (`pow` and
    //      `parse_mantissa`), which suggests few optimizations can or should
    //      be made.

    // Config options
    let format = NumberFormat::<{ FORMAT }> {};
    let decimal_point = options.decimal_point();
    let exponent_character = options.exponent();
    debug_assert!(format.is_valid(), "should have already checked for an invalid number format");
    debug_assert!(!byte.is_buffer_empty(), "should have previously checked for empty input");
    let bits_per_digit = shared::log2(format.mantissa_radix()) as i64;
    let bits_per_base = shared::log2(format.exponent_base()) as i64;

    // INTEGER

    // Check to see if we have a valid base prefix.
    #[allow(unused_variables)]
    let mut is_prefix = false;
    #[cfg(feature = "format")]
    {
        let base_prefix = format.base_prefix();
        let mut iter = byte.integer_iter();
        if base_prefix != 0 && iter.read_if_value_cased(b'0').is_some() {
            // Check to see if the next character is the base prefix.
            // We must have a format like `0x`, `0d`, `0o`.
            // NOTE: The check for empty integer digits happens below so
            // we don't need a redundant check here.
            is_prefix = true;
            if iter.read_if_value(base_prefix, format.case_sensitive_base_prefix()).is_some()
                && iter.is_buffer_empty()
                && format.required_integer_digits()
            {
                return Err(Error::EmptyInteger(iter.cursor()));
            }
        }
    }

    // Parse our integral digits.
    let mut mantissa = 0_u64;
    let start = byte.clone();
    #[cfg(not(feature = "compact"))]
    parse_8digits::<_, FORMAT>(byte.integer_iter(), &mut mantissa);
    parse_digits(byte.integer_iter(), format.mantissa_radix(), |digit| {
        mantissa = mantissa.wrapping_mul(format.radix() as u64).wrapping_add(digit as u64);
    });
    let mut n_digits = byte.current_count() - start.current_count();
    #[cfg(feature = "format")]
    if format.required_integer_digits() && n_digits == 0 {
        return Err(Error::EmptyInteger(byte.cursor()));
    }

    // Store the integer digits for slow-path algorithms.
    // NOTE: We can't use the number of digits to extract the slice for
    // non-contiguous iterators, but we also need to the number of digits
    // for our value calculation. We store both, and let the compiler know
    // to optimize it out when not needed.
    let b_digits = if cfg!(feature = "format") && !byte.integer_iter().is_contiguous() {
        byte.cursor() - start.cursor()
    } else {
        n_digits
    };
    debug_assert!(
        b_digits <= start.as_slice().len(),
        "number of digits parsed must <= buffer length"
    );
    // SAFETY: safe, since `n_digits <= start.as_slice().len()`.
    // This is since `byte.len() >= start.len()` but has to have
    // the same end bounds (that is, `start = byte.clone()`), so
    // `0 <= byte.current_count() <= start.current_count() <= start.lent()`
    // so, this will always return only the integer digits.
    //
    // NOTE: Removing this code leads to ~10% reduction in parsing
    // that triggers the Eisell-Lemire algorithm or the digit comp
    // algorithms, so don't remove the unsafe indexing.
    let integer_digits = unsafe { start.as_slice().get_unchecked(..b_digits) };

    // Check if integer leading zeros are disabled.
    #[cfg(feature = "format")]
    if !is_prefix && format.no_float_leading_zeros() {
        if integer_digits.len() > 1 && integer_digits.first() == Some(&b'0') {
            return Err(Error::InvalidLeadingZeros(start.cursor()));
        }
    }

    // FRACTION

    // Handle decimal point and digits afterwards.
    let mut n_after_dot = 0;
    let mut exponent = 0_i64;
    let mut implicit_exponent: i64;
    let int_end = n_digits as i64;
    let mut fraction_digits = None;
    let has_decimal = byte.first_is_cased(decimal_point);
    if has_decimal {
        // SAFETY: byte cannot be empty due to `first_is`
        unsafe { byte.step_unchecked() };
        let before = byte.clone();
        #[cfg(not(feature = "compact"))]
        parse_8digits::<_, FORMAT>(byte.fraction_iter(), &mut mantissa);
        parse_digits(byte.fraction_iter(), format.mantissa_radix(), |digit| {
            mantissa = mantissa.wrapping_mul(format.radix() as u64).wrapping_add(digit as u64);
        });
        n_after_dot = byte.current_count() - before.current_count();
        // NOTE: We can't use the number of digits to extract the slice for
        // non-contiguous iterators, but we also need to the number of digits
        // for our value calculation. We store both, and let the compiler know
        // to optimize it out when not needed.
        let b_after_dot = if cfg!(feature = "format") && !byte.fraction_iter().is_contiguous() {
            byte.cursor() - before.cursor()
        } else {
            n_after_dot
        };

        // Store the fraction digits for slow-path algorithms.
        debug_assert!(
            b_after_dot <= before.as_slice().len(),
            "digits after dot must be smaller than buffer"
        );
        // SAFETY: safe, since `idx_after_dot <= before.as_slice().len()`.
        fraction_digits = Some(unsafe { before.as_slice().get_unchecked(..b_after_dot) });

        // Calculate the implicit exponent: the number of digits after the dot.
        implicit_exponent = -(n_after_dot as i64);
        if format.mantissa_radix() == format.exponent_base() {
            exponent = implicit_exponent;
        } else {
            debug_assert!(bits_per_digit % bits_per_base == 0, "exponent must be a power of base");
            exponent = implicit_exponent * bits_per_digit / bits_per_base;
        };
        #[cfg(feature = "format")]
        if format.required_fraction_digits() && n_after_dot == 0 {
            return Err(Error::EmptyFraction(byte.cursor()));
        }
    }

    // NOTE: Check if we have our exponent **BEFORE** checking if the
    // mantissa is empty, so we can ensure
    let has_exponent = byte
        .first_is(exponent_character, format.case_sensitive_exponent() && cfg!(feature = "format"));

    // check to see if we have any invalid leading zeros
    n_digits += n_after_dot;
    if format.required_mantissa_digits()
        && (n_digits == 0 || (cfg!(feature = "format") && byte.current_count() == 0))
    {
        let any_digits = start.clone().integer_iter().peek().is_some();
        // NOTE: This is because numbers like `_12.34` have significant digits,
        // they just don't have a valid digit (#97).
        if has_decimal || has_exponent || !any_digits || IS_PARTIAL {
            return Err(Error::EmptyMantissa(byte.cursor()));
        } else {
            return Err(Error::InvalidDigit(start.cursor()));
        }
    }

    // EXPONENT

    // Handle scientific notation.
    let mut explicit_exponent = 0_i64;
    if has_exponent {
        // NOTE: See above for the safety invariant above `required_mantissa_digits`.
        // This is separated for correctness concerns, and therefore the two cannot
        // be on the same line.
        // SAFETY: byte cannot be empty due to `first_is` from `has_exponent`.`
        unsafe { byte.step_unchecked() };

        // Check float format syntax checks.
        #[cfg(feature = "format")]
        {
            // NOTE: We've overstepped for the safety invariant before.
            if format.no_exponent_notation() {
                return Err(Error::InvalidExponent(byte.cursor() - 1));
            }
            // Check if we have no fraction but we required exponent notation.
            if format.no_exponent_without_fraction() && fraction_digits.is_none() {
                return Err(Error::ExponentWithoutFraction(byte.cursor() - 1));
            }
        }

        let is_negative_exponent = parse_exponent_sign(&mut byte)?;
        let before = byte.current_count();
        parse_digits(byte.exponent_iter(), format.exponent_radix(), |digit| {
            if explicit_exponent < 0x10000000 {
                explicit_exponent *= format.exponent_radix() as i64;
                explicit_exponent += digit as i64;
            }
        });
        if format.required_exponent_digits() && byte.current_count() - before == 0 {
            return Err(Error::EmptyExponent(byte.cursor()));
        }
        // Handle our sign, and get the explicit part of the exponent.
        explicit_exponent = if is_negative_exponent {
            -explicit_exponent
        } else {
            explicit_exponent
        };
        exponent += explicit_exponent;
    } else if cfg!(feature = "format") && format.required_exponent_notation() {
        return Err(Error::MissingExponent(byte.cursor()));
    }

    // Check to see if we have a valid base suffix.
    // We've already trimmed any leading digit separators here, so we can be safe
    // that the first character **is not** a digit separator.
    #[allow(unused_variables)]
    let base_suffix = format.base_suffix();
    #[cfg(feature = "format")]
    if base_suffix != 0 {
        if byte.first_is(base_suffix, format.case_sensitive_base_suffix()) {
            // SAFETY: safe since `byte.len() >= 1`.
            unsafe { byte.step_unchecked() };
        }
    }

    // CHECK OVERFLOW

    // Get the number of parsed digits (total), and redo if we had overflow.
    let end = byte.cursor();
    let mut step = u64_step(format.mantissa_radix());
    let mut many_digits = false;
    #[cfg(feature = "format")]
    if !format.required_mantissa_digits() && n_digits == 0 {
        exponent = 0;
    }
    if n_digits <= step {
        return Ok((
            Number {
                exponent,
                mantissa,
                is_negative,
                many_digits: false,
                integer: integer_digits,
                fraction: fraction_digits,
            },
            end,
        ));
    }

    // Check for leading zeros, and to see if we had a false overflow.
    n_digits -= step;
    let mut zeros = start.clone();
    let mut zeros_integer = zeros.integer_iter();
    n_digits = n_digits.saturating_sub(zeros_integer.skip_zeros());
    if zeros.first_is_cased(decimal_point) {
        // SAFETY: safe since zeros cannot be empty due to `first_is`
        unsafe { zeros.step_unchecked() };
    }
    let mut zeros_fraction = zeros.fraction_iter();
    n_digits = n_digits.saturating_sub(zeros_fraction.skip_zeros());

    // OVERFLOW

    // Now, check if we explicitly overflowed.
    if n_digits > 0 {
        // Have more than 19 significant digits, so we overflowed.
        many_digits = true;
        mantissa = 0;
        let mut integer = integer_digits.bytes::<{ FORMAT }>();
        // Skip leading zeros, so we can use the step properly.
        let mut integer_iter = integer.integer_iter();
        integer_iter.skip_zeros();
        parse_u64_digits::<_, FORMAT>(integer_iter, &mut mantissa, &mut step);
        // NOTE: With the format feature enabled and non-contiguous iterators, we can
        // have null fraction digits even if step was not 0. We want to make the
        // none check as late in there as possible: any of them should
        // short-circuit and should be determined at compile time. So, the
        // conditions are either:
        // 1. Step == 0
        // 2. `cfg!(feature = "format") && !byte.is_contiguous() &&
        //    fraction_digits.is_none()`
        implicit_exponent = if step == 0
            || (cfg!(feature = "format") && !byte.is_contiguous() && fraction_digits.is_none())
        {
            // Filled our mantissa with just the integer.
            int_end - integer.current_count() as i64
        } else {
            // We know this can't be a None since we had more than 19
            // digits previously, so we overflowed a 64-bit integer,
            // but parsing only the integral digits produced less
            // than 19 digits. That means we must have a decimal
            // point, and at least 1 fractional digit.
            let mut fraction = fraction_digits.unwrap().bytes::<{ FORMAT }>();
            let mut fraction_iter = fraction.fraction_iter();
            // Skip leading zeros, so we can use the step properly.
            if mantissa == 0 {
                fraction_iter.skip_zeros();
            }
            parse_u64_digits::<_, FORMAT>(fraction_iter, &mut mantissa, &mut step);
            -(fraction.current_count() as i64)
        };
        if format.mantissa_radix() == format.exponent_base() {
            exponent = implicit_exponent;
        } else {
            debug_assert!(bits_per_digit % bits_per_base == 0, "exponent must be a power of base");
            exponent = implicit_exponent * bits_per_digit / bits_per_base;
        };
        // Add back the explicit exponent.
        exponent += explicit_exponent;
    }

    Ok((
        Number {
            exponent,
            mantissa,
            is_negative,
            many_digits,
            integer: integer_digits,
            fraction: fraction_digits,
        },
        end,
    ))
}

#[inline(always)]
pub fn parse_partial_number<'a, const FORMAT: u128>(
    byte: Bytes<'a, FORMAT>,
    is_negative: bool,
    options: &Options,
) -> Result<(Number<'a>, usize)> {
    parse_number::<FORMAT, true>(byte, is_negative, options)
}

/// Try to parse a non-special floating point number.
#[inline(always)]
pub fn parse_complete_number<'a, const FORMAT: u128>(
    byte: Bytes<'a, FORMAT>,
    is_negative: bool,
    options: &Options,
) -> Result<Number<'a>> {
    // Then have a const `IsPartial` as well
    let length = byte.buffer_length();
    let (float, count) = parse_number::<FORMAT, false>(byte, is_negative, options)?;
    if count == length {
        Ok(float)
    } else {
        Err(Error::InvalidDigit(count))
    }
}

// DIGITS
// ------

/// Iteratively parse and consume digits from bytes.
#[inline(always)]
pub fn parse_digits<'a, Iter, Cb>(mut iter: Iter, radix: u32, mut cb: Cb)
where
    Iter: DigitsIter<'a>,
    Cb: FnMut(u32),
{
    while let Some(&c) = iter.peek() {
        match char_to_digit_const(c, radix) {
            Some(v) => cb(v),
            None => break,
        }
        // SAFETY: iter cannot be empty due to `iter.peek()`.
        // NOTE: Because of the match statement, this would optimize poorly with
        // `read_if`.
        unsafe { iter.step_unchecked() };
        iter.increment_count();
    }
}

/// Iteratively parse and consume digits in intervals of 8.
///
/// # Preconditions
///
/// The iterator must be of the significant digits, not the exponent.
#[inline(always)]
#[cfg(not(feature = "compact"))]
pub fn parse_8digits<'a, Iter, const FORMAT: u128>(mut iter: Iter, mantissa: &mut u64)
where
    Iter: DigitsIter<'a>,
{
    let format = NumberFormat::<{ FORMAT }> {};
    let radix: u64 = format.radix() as u64;
    if can_try_parse_multidigit!(iter, radix) {
        debug_assert!(radix < 16, "radices over 16 will overflow with radix^8");
        let radix8 = format.radix8() as u64;
        // Can do up to 2 iterations without overflowing, however, for large
        // inputs, this is much faster than any other alternative.
        while let Some(v) = algorithm::try_parse_8digits::<u64, _, FORMAT>(&mut iter) {
            *mantissa = mantissa.wrapping_mul(radix8).wrapping_add(v);
        }
    }
}

/// Iteratively parse and consume digits without overflowing.
///
/// # Preconditions
///
/// There must be at least `step` digits left in iterator. The iterator almost
/// must be of the significant digits, not the exponent.
#[cfg_attr(not(feature = "compact"), inline(always))]
pub fn parse_u64_digits<'a, Iter, const FORMAT: u128>(
    mut iter: Iter,
    mantissa: &mut u64,
    step: &mut usize,
) where
    Iter: DigitsIter<'a>,
{
    let format = NumberFormat::<{ FORMAT }> {};
    let radix = format.radix() as u64;

    // Try to parse 8 digits at a time, if we can.
    #[cfg(not(feature = "compact"))]
    if can_try_parse_multidigit!(iter, radix) {
        debug_assert!(radix < 16, "radices over 16 will overflow with radix^8");
        let radix8 = format.radix8() as u64;
        while *step > 8 {
            if let Some(v) = algorithm::try_parse_8digits::<u64, _, FORMAT>(&mut iter) {
                *mantissa = mantissa.wrapping_mul(radix8).wrapping_add(v);
                *step -= 8;
            } else {
                break;
            }
        }
    }

    // Parse single digits at a time.
    while let Some(&c) = iter.peek() {
        if *step > 0 {
            let digit = char_to_valid_digit_const(c, radix as u32);
            *mantissa = *mantissa * radix + digit as u64;
            *step -= 1;
            // SAFETY: safe, since `iter` cannot be empty due to `iter.peek()`.
            unsafe { iter.step_unchecked() };
            iter.increment_count();
        } else {
            break;
        }
    }
}

// SPECIAL
// -------

/// Determine if the input data matches the special string.
/// If there's no match, returns 0. Otherwise, returns the byte's cursor.
#[must_use]
#[inline(always)]
pub fn is_special_eq<const FORMAT: u128>(mut byte: Bytes<FORMAT>, string: &'static [u8]) -> usize {
    let format = NumberFormat::<{ FORMAT }> {};
    if cfg!(feature = "format") && format.case_sensitive_special() {
        if shared::starts_with(byte.special_iter(), string.iter()) {
            // Trim the iterator afterwards.
            byte.special_iter().peek();
            return byte.cursor();
        }
    } else if shared::starts_with_uncased(byte.special_iter(), string.iter()) {
        // Trim the iterator afterwards.
        byte.special_iter().peek();
        return byte.cursor();
    }
    0
}

/// Parse a positive representation of a special, non-finite float.
#[must_use]
#[cfg_attr(not(feature = "compact"), inline(always))]
pub fn parse_positive_special<F, const FORMAT: u128>(
    byte: Bytes<FORMAT>,
    options: &Options,
) -> Option<(F, usize)>
where
    F: LemireFloat,
{
    let format = NumberFormat::<{ FORMAT }> {};
    if cfg!(feature = "format") && format.no_special() {
        return None;
    }

    let cursor = byte.cursor();
    let length = byte.buffer_length() - cursor;
    if let Some(nan_string) = options.nan_string() {
        if length >= nan_string.len() {
            let count = is_special_eq::<FORMAT>(byte.clone(), nan_string);
            if count != 0 {
                return Some((F::NAN, count));
            }
        }
    }
    if let Some(infinity_string) = options.infinity_string() {
        if length >= infinity_string.len() {
            let count = is_special_eq::<FORMAT>(byte.clone(), infinity_string);
            if count != 0 {
                return Some((F::INFINITY, count));
            }
        }
    }
    if let Some(inf_string) = options.inf_string() {
        if length >= inf_string.len() {
            let count = is_special_eq::<FORMAT>(byte.clone(), inf_string);
            if count != 0 {
                return Some((F::INFINITY, count));
            }
        }
    }

    None
}

/// Parse a partial representation of a special, non-finite float.
#[must_use]
#[inline(always)]
pub fn parse_partial_special<F, const FORMAT: u128>(
    byte: Bytes<FORMAT>,
    is_negative: bool,
    options: &Options,
) -> Option<(F, usize)>
where
    F: LemireFloat,
{
    let (mut float, count) = parse_positive_special::<F, FORMAT>(byte, options)?;
    if is_negative {
        float = -float;
    }
    Some((float, count))
}

/// Try to parse a special, non-finite float.
#[must_use]
#[inline(always)]
pub fn parse_special<F, const FORMAT: u128>(
    byte: Bytes<FORMAT>,
    is_negative: bool,
    options: &Options,
) -> Option<F>
where
    F: LemireFloat,
{
    let length = byte.buffer_length();
    if let Some((float, count)) = parse_partial_special::<F, FORMAT>(byte, is_negative, options) {
        if count == length {
            return Some(float);
        }
    }
    None
}
