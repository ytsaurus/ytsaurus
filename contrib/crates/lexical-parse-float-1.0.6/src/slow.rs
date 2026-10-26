//! Slow, fallback cases where we cannot unambiguously round a float.
//!
//! This occurs when we cannot determine the exact representation using
//! both the fast path (native) cases nor the Lemire/Bellerophon algorithms,
//! and therefore must fallback to a slow, arbitrary-precision representation.

#![doc(hidden)]

use core::cmp;

#[cfg(not(feature = "compact"))]
use lexical_parse_integer::algorithm;
use lexical_util::digit::char_to_valid_digit_const;
#[cfg(feature = "radix")]
use lexical_util::digit::digit_to_char_const;
use lexical_util::format::NumberFormat;
use lexical_util::iterator::{AsBytes, DigitsIter, Iter};
use lexical_util::num::{AsPrimitive, Integer};

#[cfg(feature = "radix")]
use crate::bigint::Bigfloat;
use crate::bigint::{Bigint, Limb};
use crate::float::{extended_to_float, ExtendedFloat80, RawFloat};
use crate::limits::{u32_power_limit, u64_power_limit};
use crate::number::Number;
use crate::shared;

// ALGORITHM
// ---------

/// Parse the significant digits and biased, binary exponent of a float.
///
/// This is a fallback algorithm that uses a big-integer representation
/// of the float, and therefore is considerably slower than faster
/// approximations. However, it will always determine how to round
/// the significant digits to the nearest machine float, allowing
/// use to handle near half-way cases.
///
/// Near half-way cases are halfway between two consecutive machine floats.
/// For example, the float `16777217.0` has a bitwise representation of
/// `100000000000000000000000 1`. Rounding to a single-precision float,
/// the trailing `1` is truncated. Using round-nearest, tie-even, any
/// value above `16777217.0` must be rounded up to `16777218.0`, while
/// any value before or equal to `16777217.0` must be rounded down
/// to `16777216.0`. These near-halfway conversions therefore may require
/// a large number of digits to unambiguously determine how to round.
#[must_use]
#[allow(clippy::unwrap_used)] // reason = "none is a developer error"
pub fn slow_radix<F: RawFloat, const FORMAT: u128>(
    num: Number,
    fp: ExtendedFloat80,
) -> ExtendedFloat80 {
    // Ensure our preconditions are valid:
    //  1. The significant digits are not shifted into place.
    debug_assert!(fp.mant & (1 << 63) != 0, "number must be normalized");

    let format = NumberFormat::<{ FORMAT }> {};

    // This assumes the sign bit has already been parsed, and we're
    // starting with the integer digits, and the float format has been
    // correctly validated.
    let sci_exp = scientific_exponent::<FORMAT>(&num);

    // We have 3 major algorithms we use for this:
    //  1. An algorithm with a finite number of digits and a positive exponent.
    //  2. An algorithm with a finite number of digits and a negative exponent.
    //  3. A fallback algorithm with a non-finite number of digits.

    // In order for a float in radix `b` with a finite number of digits
    // to have a finite representation in radix `y`, `b` should divide
    // an integer power of `y`. This means for binary, all even radixes
    // have finite representations, and all odd ones do not.
    #[cfg(feature = "radix")]
    {
        if let Some(max_digits) = F::max_digits(format.radix()) {
            // Can use our finite number of digit algorithm.
            digit_comp::<F, FORMAT>(num, fp, sci_exp, max_digits)
        } else {
            // Fallback to infinite digits.
            byte_comp::<F, FORMAT>(num, fp, sci_exp)
        }
    }

    #[cfg(not(feature = "radix"))]
    {
        // Can use our finite number of digit algorithm.
        let max_digits = F::max_digits(format.radix()).unwrap();
        digit_comp::<F, FORMAT>(num, fp, sci_exp, max_digits)
    }
}

/// Algorithm that generates the mantissa for a finite representation.
///
/// For a positive exponent relative to the significant digits, this
/// is just a multiplication by an exponent power. For a negative
/// exponent relative to the significant digits, we scale the real
/// digits to the theoretical digits for `b` and determine if we
/// need to round-up.
#[must_use]
#[inline(always)]
#[allow(clippy::cast_possible_wrap)] // reason = "the value range is [-324, 308]"
pub fn digit_comp<F: RawFloat, const FORMAT: u128>(
    num: Number,
    fp: ExtendedFloat80,
    sci_exp: i32,
    max_digits: usize,
) -> ExtendedFloat80 {
    let (bigmant, digits) = parse_mantissa::<FORMAT>(num, max_digits);
    // This can't underflow, since `digits` is at most `max_digits`.
    let exponent = sci_exp + 1 - digits as i32;
    if exponent >= 0 {
        positive_digit_comp::<F, FORMAT>(bigmant, exponent)
    } else {
        negative_digit_comp::<F, FORMAT>(bigmant, fp, exponent)
    }
}

/// Generate the significant digits with a positive exponent relative to
/// mantissa.
#[must_use]
#[inline(always)]
#[allow(clippy::unwrap_used)] // reason = "none is a developer error"
#[allow(clippy::cast_possible_wrap)] // reason = "can't wrap in practice: max is ~1000 limbs"
#[allow(clippy::missing_inline_in_public_items)] // reason = "only public for testing"
pub fn positive_digit_comp<F: RawFloat, const FORMAT: u128>(
    mut bigmant: Bigint,
    exponent: i32,
) -> ExtendedFloat80 {
    let format = NumberFormat::<{ FORMAT }> {};

    // Simple, we just need to multiply by the power of the radix.
    // Now, we can calculate the mantissa and the exponent from this.
    // The binary exponent is the binary exponent for the mantissa
    // shifted to the hidden bit.
    bigmant.pow(format.radix(), exponent as u32).unwrap();

    // Get the exact representation of the float from the big integer.
    // hi64 checks **all** the remaining bits after the mantissa,
    // so it will check if **any** truncated digits exist.
    let (mant, is_truncated) = bigmant.hi64();
    let exp = bigmant.bit_length() as i32 - 64 + F::EXPONENT_BIAS;
    let mut fp = ExtendedFloat80 {
        mant,
        exp,
    };

    // Shift the digits into position and determine if we need to round-up.
    shared::round::<F, _>(&mut fp, |f, s| {
        shared::round_nearest_tie_even(f, s, |is_odd, is_halfway, is_above| {
            is_above || (is_halfway && is_truncated) || (is_odd && is_halfway)
        });
    });
    fp
}

/// Generate the significant digits with a negative exponent relative to
/// mantissa.
///
/// This algorithm is quite simple: we have the significant digits `m1 * b^N1`,
/// where `m1` is the bigint mantissa, `b` is the radix, and `N1` is the radix
/// exponent. We then calculate the theoretical representation of `b+h`, which
/// is `m2 * 2^N2`, where `m2` is the bigint mantissa and `N2` is the binary
/// exponent. If we had infinite, efficient floating precision, this would be
/// equal to `m1 / b^-N1` and then compare it to `m2 * 2^N2`.
///
/// Since we cannot divide and keep precision, we must multiply the other:
/// if we want to do `m1 / b^-N1 >= m2 * 2^N2`, we can do
/// `m1 >= m2 * b^-N1 * 2^N2` Going to the decimal case, we can show and example
/// and simplify this further: `m1 >= m2 * 2^N2 * 10^-N1`. Since we can remove
/// a power-of-two, this is `m1 >= m2 * 2^(N2 - N1) * 5^-N1`. Therefore, if
/// `N2 - N1 > 0`, we need have `m1 >= m2 * 2^(N2 - N1) * 5^-N1`, otherwise,
/// we have `m1 * 2^(N1 - N2) >= m2 * 5^-N1`, where the resulting exponents
/// are all positive.
///
/// This allows us to compare both floats using integers efficiently
/// without any loss of precision.
#[must_use]
#[inline(always)]
#[allow(clippy::match_bool)] // reason = "simplifies documentation"
#[allow(clippy::unwrap_used)] // reason = "unwrap panics if a developer error"
#[allow(clippy::comparison_chain)] // reason = "logically different conditions for algorithm"
#[allow(clippy::missing_inline_in_public_items)] // reason = "only exposed for unittesting"
pub fn negative_digit_comp<F: RawFloat, const FORMAT: u128>(
    bigmant: Bigint,
    mut fp: ExtendedFloat80,
    exponent: i32,
) -> ExtendedFloat80 {
    // Ensure our preconditions are valid:
    //  1. The significant digits are not shifted into place.
    debug_assert!(fp.mant & (1 << 63) != 0, "the significant digits must be normalized");

    let format = NumberFormat::<FORMAT> {};
    let radix = format.radix();

    // Get the significant digits and radix exponent for the real digits.
    let mut real_digits = bigmant;
    let real_exp = exponent;
    debug_assert!(real_exp < 0, "algorithm only works with negative numbers");

    // Round down our extended-precision float and calculate `b`.
    let mut b = fp;
    shared::round::<F, _>(&mut b, shared::round_down);
    let b = extended_to_float::<F>(b);

    // Get the significant digits and the binary exponent for `b+h`.
    let theor = bh(b);
    let mut theor_digits = Bigint::from_u64(theor.mant);
    let theor_exp = theor.exp;

    // We need to scale the real digits and `b+h` digits to be the same
    // order. We currently have `real_exp`, in `radix`, that needs to be
    // shifted to `theor_digits` (since it is negative), and `theor_exp`
    // to either `theor_digits` or `real_digits` as a power of 2 (since it
    // may be positive or negative). Try to remove as many powers of 2
    // as possible. All values are relative to `theor_digits`, that is,
    // reflect the power you need to multiply `theor_digits` by.
    let (binary_exp, halfradix_exp, radix_exp) = match radix.is_even() {
        // Can remove a power-of-two.
        // Both are on opposite-sides of equation, can factor out a
        // power of two.
        //
        // Example: 10^-10, 2^-10   -> ( 0, 10, 0)
        // Example: 10^-10, 2^-15   -> (-5, 10, 0)
        // Example: 10^-10, 2^-5    -> ( 5, 10, 0)
        // Example: 10^-10, 2^5     -> (15, 10, 0)
        true => (theor_exp - real_exp, -real_exp, 0),
        // Cannot remove a power-of-two.
        false => (theor_exp, 0, -real_exp),
    };

    if halfradix_exp != 0 {
        theor_digits.pow(radix / 2, halfradix_exp as u32).unwrap();
    }
    if radix_exp != 0 {
        theor_digits.pow(radix, radix_exp as u32).unwrap();
    }
    if binary_exp > 0 {
        theor_digits.pow(2, binary_exp as u32).unwrap();
    } else if binary_exp < 0 {
        real_digits.pow(2, (-binary_exp) as u32).unwrap();
    }

    // Compare our theoretical and real digits and round nearest, tie even.
    let ord = real_digits.data.cmp(&theor_digits.data);
    shared::round::<F, _>(&mut fp, |f, s| {
        shared::round_nearest_tie_even(f, s, |is_odd, _, _| {
            // Can ignore `is_halfway` and `is_above`, since those were
            // calculates using less significant digits.
            match ord {
                cmp::Ordering::Greater => true,
                cmp::Ordering::Less => false,
                cmp::Ordering::Equal if is_odd => true,
                cmp::Ordering::Equal => false,
            }
        });
    });
    fp
}

/// Try to parse 8 digits at a time.
///
/// - `format` - The numerical format specification as a packed 128-bit integer
/// - `iter` - An iterator over all bytes in the buffer
/// - `value` - The currently parsed value.
/// - `count` - The total number of parsed digits
/// - `counter` - The number of parsed digits since creating the current u32
/// - `step` - The maximum number of digits for the radix that can fit in a u32.
/// - `max_digits` - The maximum number of digits that can affect floating-point
///   rounding.
#[cfg(not(feature = "compact"))]
macro_rules! try_parse_8digits {
    (
        $format:ident,
        $iter:ident,
        $value:ident,
        $count:ident,
        $counter:ident,
        $step:ident,
        $max_digits:ident
    ) => {{
        let format = NumberFormat::<$format> {};
        let radix = format.radix() as Limb;

        // Try 8-digit optimizations.
        if can_try_parse_multidigit!($iter, radix) {
            debug_assert!(radix < 16);
            let radix8 = format.radix8() as Limb;
            while $step - $counter >= 8 && $max_digits - $count >= 8 {
                if let Some(v) = algorithm::try_parse_8digits::<Limb, _, FORMAT>(&mut $iter) {
                    $value = $value.wrapping_mul(radix8).wrapping_add(v);
                    $counter += 8;
                    $count += 8;
                } else {
                    break;
                }
            }
        }
    }};
}

/// Add a digit to the temporary value.
///
/// - `c` - The character to convert to a digit.
/// - `value` - The currently parsed value.
/// - `count` - The total number of parsed digits
/// - `counter` - The number of parsed digits since creating the current u32
macro_rules! add_digit {
    ($c:ident, $radix:ident, $value:ident, $counter:ident, $count:ident) => {{
        let digit = char_to_valid_digit_const($c, $radix);
        $value *= $radix as Limb;
        $value += digit as Limb;

        // Increment our counters.
        $counter += 1;
        $count += 1;
    }};
}

/// Add a temporary value to our mantissa.
///
/// - `format` - The numerical format specification as a packed 128-bit integer
/// - `result` - The big integer,
/// - `power` - The power to scale the big integer by.
/// - `value` - The value to add to the big integer,
/// - `counter` - The number of parsed digits since creating the current u32
macro_rules! add_temporary {
    // Multiply by the small power and add the native value.
    (@mul $result:ident, $power:expr, $value:expr) => {
        $result.data.mul_small($power).unwrap();
        $result.data.add_small($value).unwrap();
    };

    // Add a temporary where we won't read the counter results internally.
    (@end $format:ident, $result:ident, $counter:ident, $value:ident) => {
        if $counter != 0 {
            let small_power = f64::int_pow_fast_path($counter, $format.radix());
            add_temporary!(@mul $result, small_power as Limb, $value);
        }
    };

    // Add the maximum native value.
    (@max $format:ident, $result:ident, $counter:ident, $value:ident, $max:ident) => {
        add_temporary!(@mul $result, $max, $value);
        $counter = 0;
        $value = 0;
    };
}

/// Round-up a truncated value.
///
/// - `format` - The numerical format specification as a packed 128-bit integer
/// - `result` - The big integer,
/// - `count` - The total number of parsed digits
macro_rules! round_up_truncated {
    ($format:ident, $result:ident, $count:ident) => {{
        // Need to round-up.
        // Can't just add 1, since this can accidentally round-up
        // values to a halfway point, which can cause invalid results.
        add_temporary!(@mul $result, $format.radix() as Limb, 1);
        $count += 1;
    }};
}

/// Check and round-up the fraction if any non-zero digits exist.
///
/// - `format` - The numerical format specification as a packed 128-bit integer
/// - `iter` - An iterator over all bytes in the buffer
/// - `result` - The big integer,
/// - `count` - The total number of parsed digits
macro_rules! round_up_nonzero {
    ($format:ident, $iter:expr, $result:ident, $count:ident) => {{
        // NOTE: All digits must already be valid.
        let mut iter = $iter;

        // First try reading 8-digits at a time.
        if iter.is_contiguous() {
            while let Some(value) = iter.peek_u64() {
                // SAFETY: safe since we have at least 8 bytes in the buffer.
                unsafe { iter.step_by_unchecked(8) };
                if value != 0x3030_3030_3030_3030 {
                    // Have non-zero digits, exit early.
                    round_up_truncated!($format, $result, $count);
                    return ($result, $count);
                }
            }
        }

        for &digit in iter {
            if digit != b'0' {
                round_up_truncated!($format, $result, $count);
                return ($result, $count);
            }
        }
    }};
}

/// Parse the full mantissa into a big integer.
///
/// Returns the parsed mantissa and the number of digits in the mantissa.
/// The max digits is the maximum number of digits plus one.
#[must_use]
#[allow(clippy::cognitive_complexity)] // reason = "complexity broken into macros"
#[allow(clippy::missing_inline_in_public_items)] // reason = "only public for testing"
pub fn parse_mantissa<const FORMAT: u128>(num: Number, max_digits: usize) -> (Bigint, usize) {
    let format = NumberFormat::<FORMAT> {};
    let radix = format.radix();

    // Iteratively process all the data in the mantissa.
    // We do this via small, intermediate values which once we reach
    // the maximum number of digits we can process without overflow,
    // we add the temporary to the big integer.
    let mut counter: usize = 0;
    let mut count: usize = 0;
    let mut value: Limb = 0;
    let mut result = Bigint::new();

    // Now use our pre-computed small powers iteratively.
    let step = if Limb::BITS == 32 {
        u32_power_limit(format.radix())
    } else {
        u64_power_limit(format.radix())
    } as usize;
    let max_native = (format.radix() as Limb).pow(step as u32);

    // Process the integer digits.
    let mut integer = num.integer.bytes::<FORMAT>();
    let mut integer_iter = integer.integer_iter();
    integer_iter.skip_zeros();
    'integer: loop {
        #[cfg(not(feature = "compact"))]
        try_parse_8digits!(FORMAT, integer_iter, value, count, counter, step, max_digits);

        // Parse a digit at a time, until we reach step.
        while counter < step && count < max_digits {
            if let Some(&c) = integer_iter.next() {
                add_digit!(c, radix, value, counter, count);
            } else {
                break 'integer;
            }
        }

        // Check if we've exhausted our max digits.
        if count == max_digits {
            // Need to check if we're truncated, and round-up accordingly.
            // SAFETY: safe since `counter <= step`.
            add_temporary!(@end format, result, counter, value);
            round_up_nonzero!(format, integer_iter, result, count);
            if let Some(fraction) = num.fraction {
                let mut fraction = fraction.bytes::<FORMAT>();
                round_up_nonzero!(format, fraction.fraction_iter(), result, count);
            }
            return (result, count);
        } else {
            // Add our temporary from the loop.
            // SAFETY: safe since `counter <= step`.
            add_temporary!(@max format, result, counter, value, max_native);
        }
    }

    // Process the fraction digits.
    if let Some(fraction) = num.fraction {
        let mut fraction = fraction.bytes::<FORMAT>();
        let mut fraction_iter = fraction.integer_iter();
        if count == 0 {
            // No digits added yet, can skip leading fraction zeros too.
            fraction_iter.skip_zeros();
        }
        'fraction: loop {
            #[cfg(not(feature = "compact"))]
            try_parse_8digits!(FORMAT, fraction_iter, value, count, counter, step, max_digits);

            // Parse a digit at a time, until we reach step.
            while counter < step && count < max_digits {
                if let Some(&c) = fraction_iter.next() {
                    add_digit!(c, radix, value, counter, count);
                } else {
                    break 'fraction;
                }
            }

            // Check if we've exhausted our max digits.
            if count == max_digits {
                // SAFETY: safe since `counter <= step`.
                add_temporary!(@end format, result, counter, value);
                round_up_nonzero!(format, fraction_iter, result, count);
                return (result, count);
            } else {
                // Add our temporary from the loop.
                // SAFETY: safe since `counter <= step`.
                add_temporary!(@max format, result, counter, value, max_native);
            }
        }
    }

    // We will always have a remainder, as long as we entered the loop
    // once, or counter % step is 0.
    // SAFETY: safe since `counter <= step`.
    add_temporary!(@end format, result, counter, value);

    (result, count)
}

/// Compare actual integer digits to the theoretical digits.
///
/// - `iter` - An iterator over all bytes in the buffer
/// - `num` - The actual digits of the real floating point number.
/// - `den` - The theoretical digits created by `b+h` to determine if `b` or
///   `b+1`
#[cfg(feature = "radix")]
macro_rules! integer_compare {
    ($iter:ident, $num:ident, $den:ident, $radix:ident) => {{
        // Compare the integer digits.
        while !$num.data.is_empty() {
            // All digits **must** be valid.
            let actual = match $iter.next() {
                Some(&v) => v,
                // Could have hit the decimal point.
                _ => break,
            };
            let rem = $num.data.quorem(&$den.data) as u32;
            let expected = digit_to_char_const(rem, $radix);
            $num.data.mul_small($radix as Limb).unwrap();
            if actual < expected {
                return cmp::Ordering::Less;
            } else if actual > expected {
                return cmp::Ordering::Greater;
            }
        }

        // Still have integer digits, check if any are non-zero.
        if $num.data.is_empty() {
            for &digit in $iter {
                if digit != b'0' {
                    return cmp::Ordering::Greater;
                }
            }
        }
    }};
}

/// Compare actual fraction digits to the theoretical digits.
///
/// - `iter` - An iterator over all bytes in the buffer
/// - `num` - The actual digits of the real floating point number.
/// - `den` - The theoretical digits created by `b+h` to determine if `b` or
///   `b+1`
#[cfg(feature = "radix")]
macro_rules! fraction_compare {
    ($iter:ident, $num:ident, $den:ident, $radix:ident) => {{
        // Compare the fraction digits.
        // We can only be here if we hit a decimal point.
        while !$num.data.is_empty() {
            // All digits **must** be valid.
            let actual = match $iter.next() {
                Some(&v) => v,
                // No more actual digits, or hit the exponent.
                _ => return cmp::Ordering::Less,
            };
            let rem = $num.data.quorem(&$den.data) as u32;
            let expected = digit_to_char_const(rem, $radix);
            $num.data.mul_small($radix as Limb).unwrap();
            if actual < expected {
                return cmp::Ordering::Less;
            } else if actual > expected {
                return cmp::Ordering::Greater;
            }
        }

        // Still have fraction digits, check if any are non-zero.
        for &digit in $iter {
            if digit != b'0' {
                return cmp::Ordering::Greater;
            }
        }
    }};
}

/// Compare theoretical digits to halfway point from theoretical digits.
///
/// Generates a float representing the halfway point, and generates
/// theoretical digits as bytes, and compares the generated digits to
/// the actual input.
///
/// Compares the known string to theoretical digits generated on the
/// fly for `b+h`, where a string representation of a float is between
/// `b` and `b+u`, where `b+u` is 1 unit in the least-precision. Therefore,
/// the string must be close to `b+h`.
///
/// Adapted from "Bigcomp: Deciding Truncated, Near Halfway Conversions",
/// available [here](https://www.exploringbinary.com/bigcomp-deciding-truncated-near-halfway-conversions/).
#[cfg(feature = "radix")]
#[allow(clippy::unwrap_used)] // reason = "none is a developer error due to shl overflow"
#[allow(clippy::comparison_chain)] // reason = "logically different conditions for algorithm"
pub fn byte_comp<F: RawFloat, const FORMAT: u128>(
    number: Number,
    mut fp: ExtendedFloat80,
    sci_exp: i32,
) -> ExtendedFloat80 {
    // Ensure our preconditions are valid:
    //  1. The significant digits are not shifted into place.
    debug_assert!(fp.mant & (1 << 63) != 0);

    let format = NumberFormat::<FORMAT> {};

    // Round down our extended-precision float and calculate `b`.
    let mut b = fp;
    shared::round::<F, _>(&mut b, shared::round_down);
    let b = extended_to_float::<F>(b);

    // Calculate `b+h` to create a ratio for our theoretical digits.
    let theor = Bigfloat::from_float(bh::<F>(b));

    // Now, create a scaling factor for the digit count.
    let mut factor = Bigfloat::from_u32(1);
    factor.pow(format.radix(), sci_exp.unsigned_abs()).unwrap();
    let mut num: Bigfloat;
    let mut den: Bigfloat;

    if sci_exp < 0 {
        // Need to have the basen factor be the numerator, and the `fp`
        // be the denominator. Since we assumed that `theor` was the numerator,
        // if it's the denominator, we need to multiply it into the numerator.
        num = factor;
        num.data *= &theor.data;
        den = Bigfloat::from_u32(1);
        den.exp = -theor.exp;
    } else {
        num = theor;
        den = factor;
    }

    // Scale the denominator so it has the number of bits
    // in the radix as the number of leading zeros.
    let wlz = integral_binary_factor(format.radix());
    let nlz = den.leading_zeros().wrapping_sub(wlz) & (32 - 1);
    if nlz != 0 {
        den.shl_bits(nlz as usize).unwrap();
        den.exp -= nlz as i32;
    }

    // Need to scale the numerator or denominator to the same value.
    // We don't want to shift the denominator, so...
    let diff = den.exp - num.exp;
    let shift = diff.unsigned_abs() as usize;
    if diff < 0 {
        // Need to shift the numerator left.
        num.shl(shift).unwrap();
        num.exp -= shift as i32;
    } else if diff > 0 {
        // Need to shift denominator left, go by a power of Limb::BITS.
        // After this, the numerator will be non-normalized, and the
        // denominator will be normalized. We need to add one to the
        // quotient,since we're calculating the ceiling of the divmod.
        let (q, r) = shift.ceil_divmod(Limb::BITS as usize);
        let r = -r;
        if r != 0 {
            num.shl_bits(r as usize).unwrap();
            num.exp -= r;
        }
        if q != 0 {
            den.shl_limbs(q).unwrap();
            den.exp -= Limb::BITS as i32 * q as i32;
        }
    }

    // Compare our theoretical and real digits and round nearest, tie even.
    let ord = compare_bytes::<FORMAT>(number, num, den);
    shared::round::<F, _>(&mut fp, |f, s| {
        shared::round_nearest_tie_even(f, s, |is_odd, _, _| {
            // Can ignore `is_halfway` and `is_above`, since those were
            // calculates using less significant digits.
            match ord {
                cmp::Ordering::Greater => true,
                cmp::Ordering::Less => false,
                cmp::Ordering::Equal if is_odd => true,
                cmp::Ordering::Equal => false,
            }
        });
    });
    fp
}

/// Compare digits between the generated values the ratio and the actual view.
///
/// - `number` - The representation of the float as a big number, with the
///   parsed digits.
/// - `num` - The actual digits of the real floating point number.
/// - `den` - The theoretical digits created by `b+h` to determine if `b` or
///   `b+1`
#[cfg(feature = "radix")]
#[allow(clippy::unwrap_used)] // reason = "none is a developer error due to a missing fraction"
pub fn compare_bytes<const FORMAT: u128>(
    number: Number,
    mut num: Bigfloat,
    den: Bigfloat,
) -> cmp::Ordering {
    let format = NumberFormat::<FORMAT> {};
    let radix = format.radix();

    // Now need to compare the theoretical digits. First, I need to trim
    // any leading zeros, and will also need to ignore trailing ones.
    let mut integer = number.integer.bytes::<{ FORMAT }>();
    let mut integer_iter = integer.integer_iter();
    integer_iter.skip_zeros();
    if integer_iter.is_buffer_empty() {
        // Cannot be empty, since we must have at least **some** significant digits.
        let mut fraction = number.fraction.unwrap().bytes::<{ FORMAT }>();
        let mut fraction_iter = fraction.fraction_iter();
        fraction_iter.skip_zeros();
        fraction_compare!(fraction_iter, num, den, radix);
    } else {
        integer_compare!(integer_iter, num, den, radix);
        if let Some(fraction) = number.fraction {
            let mut fraction = fraction.bytes::<{ FORMAT }>();
            let mut fraction_iter = fraction.fraction_iter();
            fraction_compare!(fraction_iter, num, den, radix);
        } else if !num.data.is_empty() {
            // We had more theoretical digits, but no more actual digits.
            return cmp::Ordering::Less;
        }
    }

    // Exhausted both, must be equal.
    cmp::Ordering::Equal
}

// SCALING
// -------

/// Calculate the scientific exponent from a `Number` value.
/// Any other attempts would require slowdowns for faster algorithms.
#[must_use]
#[inline(always)]
pub fn scientific_exponent<const FORMAT: u128>(num: &Number) -> i32 {
    // This has the significant digits and exponent relative to those
    // digits: therefore, we just need to scale to mantissa to `[1, radix)`.
    // This doesn't need to be very fast.
    let format = NumberFormat::<FORMAT> {};

    // Use power reduction to make this faster: we need at least
    // `F::MANTISSA_SIZE` bits, so we must have at least radix^4 digits.
    // IF we're using base 3, we can have at most 11 divisions, and
    // base 36, at most ~4. So, this is reasonably efficient.
    let radix = format.radix() as u64;
    let radix2 = radix * radix;
    let radix4 = radix2 * radix2;
    let mut mantissa = num.mantissa;
    let mut exponent = num.exponent;
    while mantissa >= radix4 {
        mantissa /= radix4;
        exponent += 4;
    }
    while mantissa >= radix2 {
        mantissa /= radix2;
        exponent += 2;
    }
    while mantissa >= radix {
        mantissa /= radix;
        exponent += 1;
    }
    exponent as i32
}

/// Calculate `b` from a a representation of `b` as a float.
#[must_use]
#[inline(always)]
pub fn b<F: RawFloat>(float: F) -> ExtendedFloat80 {
    ExtendedFloat80 {
        mant: float.mantissa().as_u64(),
        exp: float.exponent(),
    }
}

/// Calculate `b+h` from a a representation of `b` as a float.
#[must_use]
#[inline(always)]
pub fn bh<F: RawFloat>(float: F) -> ExtendedFloat80 {
    let fp = b(float);
    ExtendedFloat80 {
        mant: (fp.mant << 1) + 1,
        exp: fp.exp - 1,
    }
}

// NOTE: There will never be binary factors here.

/// Calculate the integral ceiling of the binary factor from a basen number.
#[must_use]
#[inline(always)]
#[cfg(feature = "radix")]
pub const fn integral_binary_factor(radix: u32) -> u32 {
    match radix {
        3 => 2,
        5 => 3,
        6 => 3,
        7 => 3,
        9 => 4,
        10 => 4,
        11 => 4,
        12 => 4,
        13 => 4,
        14 => 4,
        15 => 4,
        17 => 5,
        18 => 5,
        19 => 5,
        20 => 5,
        21 => 5,
        22 => 5,
        23 => 5,
        24 => 5,
        25 => 5,
        26 => 5,
        27 => 5,
        28 => 5,
        29 => 5,
        30 => 5,
        31 => 5,
        33 => 6,
        34 => 6,
        35 => 6,
        36 => 6,
        // Invalid radix
        _ => 0,
    }
}

/// Calculate the integral ceiling of the binary factor from a basen number.
#[must_use]
#[inline(always)]
#[cfg(not(feature = "radix"))]
pub const fn integral_binary_factor(radix: u32) -> u32 {
    match radix {
        10 => 4,
        // Invalid radix
        _ => 0,
    }
}
