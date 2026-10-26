//! Optimized float parser for radixes powers of 2.
//!
//! Note: this does not require the mantissa radix and the
//! exponent base to be the same.

#![cfg(feature = "power-of-two")]
#![doc(hidden)]

#[cfg(not(feature = "compact"))]
use lexical_parse_integer::algorithm;
use lexical_util::digit::char_to_valid_digit_const;
use lexical_util::format::NumberFormat;
use lexical_util::iterator::{AsBytes, DigitsIter};
use lexical_util::step::u64_step;

use crate::float::{ExtendedFloat80, RawFloat};
use crate::mask::lower_n_halfway;
use crate::number::Number;
use crate::shared;

// ALGORITHM
// ---------

/// Algorithm specialized for radixes of powers-of-two.
#[cfg_attr(not(feature = "compact"), inline(always))]
pub fn binary<F: RawFloat, const FORMAT: u128>(num: &Number, lossy: bool) -> ExtendedFloat80 {
    let format = NumberFormat::<{ FORMAT }> {};
    debug_assert!(
        matches!(format.radix(), 2 | 4 | 8 | 16 | 32),
        "algorithm requires a power-of-two"
    );

    let fp_zero = ExtendedFloat80 {
        mant: 0,
        exp: 0,
    };

    // Normalize our mantissa for simpler results.
    let ctlz = num.mantissa.leading_zeros();
    let mantissa = num.mantissa << ctlz;

    // Quick check if we're close to a halfway point.
    // Since we're using powers-of-two, we can clearly tell if we're at
    // a halfway point, unless it's even and we're exactly halfway so far.
    // This is true even for radixes like 8 and 32, where `log2(radix)`
    // is not a power-of-two. If it's odd and we're at halfway, we'll
    // always round-up **anyway**.
    //
    // We need to check the truncated bits are equal to `0b100000....`,
    // if it's above that, always round-up. If it's odd, we can always
    // disambiguate the float. If it's even, and exactly halfway, this
    // step fails.
    let power2 = shared::calculate_power2::<F, FORMAT>(num.exponent, ctlz);
    if -power2 + 1 >= 64 {
        // Have more than 63 bits below the minimum exponent, must be 0.
        // Since we can't have partial digit rounding, this is true always
        // if the power-of-two >= 64.
        return fp_zero;
    }

    // Get our shift to shift the digits to the hidden bit, or correct spot.
    // This differs for denormal floats, so do that carefully, but that's
    // relative to the current leading zeros of the float.
    let shift = shared::calculate_shift::<F>(power2);

    // Determine if we can see if we're at a halfway point.
    let last_bit = 1u64 << shift;
    let truncated = last_bit - 1;
    let halfway = lower_n_halfway(shift as u64);
    let is_even = mantissa & last_bit == 0;
    let is_halfway = mantissa & truncated == halfway;
    if !lossy && is_even && is_halfway && num.many_digits {
        // Exactly halfway and even, cannot safely determine our representation.
        // Bias the exponent so we know it's invalid.
        return ExtendedFloat80 {
            mant: mantissa,
            exp: power2 + shared::INVALID_FP,
        };
    }

    // Shift our digits into place, and round up if needed.
    let is_above = mantissa & truncated > halfway;
    let round_up = is_above || (!is_even && is_halfway);
    let mut fp = ExtendedFloat80 {
        mant: mantissa,
        exp: power2,
    };

    shared::round::<F, _>(&mut fp, |f, s| {
        shared::round_nearest_tie_even(f, s, |_, _, _| round_up);
    });
    fp
}

/// Iteratively parse and consume digits without overflowing.
///
/// We're guaranteed to have a large number of digits here
/// (in general, 20+ or much higher), due to how close we
/// are to a halfway representation, so an unchecked loop
/// optimization isn't worth it.
#[cfg_attr(not(feature = "compact"), inline(always))]
#[allow(unused_mut)]
pub fn parse_u64_digits<'a, Iter, const FORMAT: u128>(
    mut iter: Iter,
    mantissa: &mut u64,
    step: &mut usize,
    overflowed: &mut bool,
    zero: &mut bool,
) where
    Iter: DigitsIter<'a>,
{
    let format = NumberFormat::<{ FORMAT }> {};
    let radix = format.radix() as u64;

    // Try to parse 8 digits at a time, if we can.
    #[cfg(not(feature = "compact"))]
    if can_try_parse_multidigit!(iter, radix) {
        debug_assert!(radix < 16, "larger radices will wrap on radix^8");
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
    for &c in iter {
        let digit = char_to_valid_digit_const(c, radix as u32);
        if !*overflowed {
            let result = mantissa.checked_mul(radix).and_then(|x| x.checked_add(digit as u64));
            if let Some(mant) = result {
                *mantissa = mant;
            } else {
                *overflowed = true;
                *zero &= digit == 0;
            }
        } else {
            *zero &= digit == 0;
        }
        *step = step.saturating_sub(1);
    }
}

/// Fallback, slow algorithm optimized for powers-of-two.
///
/// This avoids the need for arbitrary-precision arithmetic, since the result
/// will always be a near-halfway representation where rounded-down it's even.
pub fn slow_binary<F: RawFloat, const FORMAT: u128>(num: Number) -> ExtendedFloat80 {
    let format = NumberFormat::<{ FORMAT }> {};
    let radix = format.radix();
    debug_assert!(matches!(radix, 2 | 4 | 8 | 16 | 32), "algorithm requires a power-of-two");

    // This assumes the sign bit has already been parsed, and we're
    // starting with the integer digits, and the float format has been
    // correctly validated.

    // This is quite simple: parse till we get overflow, check if all
    // the remaining digits are zero/non-zero, and determine if we round-up
    // or down as a result.

    let mut mantissa = 0_u64;
    let mut overflow = false;
    let mut zero = true;

    // Parse the integer digits.
    let mut step = u64_step(radix);
    let mut integer = num.integer.bytes::<FORMAT>();
    integer.integer_iter().skip_zeros();
    parse_u64_digits::<_, FORMAT>(
        integer.integer_iter(),
        &mut mantissa,
        &mut step,
        &mut overflow,
        &mut zero,
    );

    // Parse the fraction digits.
    if let Some(fraction) = num.fraction {
        let mut fraction = fraction.bytes::<FORMAT>();
        if mantissa == 0 {
            fraction.fraction_iter().skip_zeros();
        }
        parse_u64_digits::<_, FORMAT>(
            fraction.fraction_iter(),
            &mut mantissa,
            &mut step,
            &mut overflow,
            &mut zero,
        );
    }

    // Note: we're not guaranteed to have overflowed here, although it's
    // very, very likely. We can also skip the exponent, since we already
    // know it, and we already know the total parsed digits.

    // Normalize our mantissa for simpler results.
    let ctlz = mantissa.leading_zeros();
    mantissa <<= ctlz;
    let power2 = shared::calculate_power2::<F, FORMAT>(num.exponent, ctlz);

    let mut fp = ExtendedFloat80 {
        mant: mantissa,
        exp: power2,
    };

    shared::round::<F, _>(&mut fp, |f, s| {
        shared::round_nearest_tie_even(f, s, |_, _, _| !zero);
    });
    fp
}
