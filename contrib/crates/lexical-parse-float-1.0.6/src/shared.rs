//! Shared utilities and algorithms.

#![doc(hidden)]

#[cfg(feature = "power-of-two")]
use lexical_util::format::NumberFormat;
use lexical_util::num::AsPrimitive;

use crate::float::{ExtendedFloat80, RawFloat};
use crate::mask::{lower_n_halfway, lower_n_mask};

// 8 DIGIT
// -------

/// Check if we can try to parse 8 digits at one.
#[cfg(not(feature = "compact"))]
macro_rules! can_try_parse_multidigit {
    ($iter:expr, $radix:expr) => {
        $iter.is_contiguous() && (cfg!(not(feature = "power-of-two")) || $radix <= 10)
    };
}

// POWER2
// ------

/// Calculate the shift to move the significant digits into place.
#[inline(always)]
pub fn calculate_shift<F: RawFloat>(power2: i32) -> i32 {
    let mantissa_shift = 64 - F::MANTISSA_SIZE - 1;
    if -power2 >= mantissa_shift {
        -power2 + 1
    } else {
        mantissa_shift
    }
}

/// Calculate the biased, binary exponent from the mantissa shift and exponent.
#[inline(always)]
#[cfg(feature = "power-of-two")]
pub fn calculate_power2<F: RawFloat, const FORMAT: u128>(exponent: i64, ctlz: u32) -> i32 {
    let format = NumberFormat::<{ FORMAT }> {};
    exponent as i32 * log2(format.exponent_base()) + F::EXPONENT_BIAS - ctlz as i32
}

/// Bias for marking an invalid extended float.
pub const INVALID_FP: i32 = i16::MIN as i32;

// LOG2
// ----

/// Quick log2 that evaluates at compile time for the radix.
/// Note that this may produce inaccurate results for other radixes:
/// we don't care since it's only called for powers-of-two.
#[inline(always)]
pub const fn log2(radix: u32) -> i32 {
    match radix {
        2 => 1,
        4 => 2,
        8 => 3,
        16 => 4,
        32 => 5,
        // Fallthrough to 1 for non-power-of-two radixes.
        _ => 1,
    }
}

// STARTS WITH
// -----------

/// Check if left iter starts with right iter.
///
/// This optimizes decently well, to the following ASM for pure slices:
///
/// ```text
/// starts_with_slc:
///         xor     eax, eax
/// .LBB0_1:
///         cmp     rcx, rax
///         je      .LBB0_2
///         cmp     rsi, rax
///         je      .LBB0_5
///         movzx   r8d, byte ptr [rdi + rax]
///         lea     r9, [rax + 1]
///         cmp     r8b, byte ptr [rdx + rax]
///         mov     rax, r9
///         je      .LBB0_1
/// .LBB0_5:
///         xor     eax, eax
///         ret
/// .LBB0_2:
///         mov     al, 1
///         ret
/// ```
#[cfg_attr(not(feature = "compact"), inline(always))]
pub fn starts_with<'a, 'b, Iter1, Iter2>(mut x: Iter1, mut y: Iter2) -> bool
where
    Iter1: Iterator<Item = &'a u8>,
    Iter2: Iterator<Item = &'b u8>,
{
    loop {
        // Only call `next()` on x if y is not None, otherwise,
        // we may incorrectly consume an x character.
        let yi = y.next();
        if yi.is_none() {
            return true;
        } else if x.next() != yi {
            return false;
        }
    }
}

/// Check if left iter starts with right iter without case-sensitivity.
///
/// This optimizes decently well, to the following ASM for pure slices:
///
/// ```text
/// starts_with_uncased:
///         xor     eax, eax
/// .LBB1_1:
///         cmp     rcx, rax
///         je      .LBB1_2
///         cmp     rsi, rax
///         je      .LBB1_5
///         movzx   r8d, byte ptr [rdi + rax]
///         xor     r8b, byte ptr [rdx + rax]
///         add     rax, 1
///         test    r8b, -33
///         je      .LBB1_1
/// .LBB1_5:
///         xor     eax, eax
///         ret
/// .LBB1_2:
///         mov     al, 1
///         ret
/// ```
#[cfg_attr(not(feature = "compact"), inline(always))]
#[allow(clippy::unwrap_used)] // reason="yi cannot be none due to previous check"
pub fn starts_with_uncased<'a, 'b, Iter1, Iter2>(mut x: Iter1, mut y: Iter2) -> bool
where
    Iter1: Iterator<Item = &'a u8>,
    Iter2: Iterator<Item = &'b u8>,
{
    // We use a faster optimization here for ASCII letters, which NaN
    // and infinite strings **must** be. [A-Z] is 0x41-0x5A, while
    // [a-z] is 0x61-0x7A. Therefore, the xor must be 0 or 32 if they
    // are case-insensitive equal, but only if at least 1 of the inputs
    // is an ASCII letter.
    loop {
        let yi = y.next();
        if yi.is_none() {
            return true;
        }
        let yi = *yi.unwrap();
        let is_not_equal = x.next().map_or(true, |&xi| {
            let xor = xi ^ yi;
            xor != 0 && xor != 0x20
        });
        if is_not_equal {
            return false;
        }
    }
}

// ROUNDING
// --------

/// Round an extended-precision float to the nearest machine float.
///
/// Shifts the significant digits into place, adjusts the exponent,
/// so it can be easily converted to a native float.
#[cfg_attr(not(feature = "compact"), inline(always))]
pub fn round<F, Cb>(fp: &mut ExtendedFloat80, cb: Cb)
where
    F: RawFloat,
    Cb: Fn(&mut ExtendedFloat80, i32),
{
    let fp_inf = ExtendedFloat80 {
        mant: 0,
        exp: F::INFINITE_POWER,
    };

    // Calculate our shift in significant digits.
    let mantissa_shift = 64 - F::MANTISSA_SIZE - 1;

    // Check for a denormal float, if after the shift the exponent is negative.
    if -fp.exp >= mantissa_shift {
        // Have a denormal float that isn't a literal 0.
        // The extra 1 is to adjust for the denormal float, which is
        // `1 - F::EXPONENT_BIAS`. This works as before, because our
        // old logic rounded to `F::DENORMAL_EXPONENT` (now 1), and then
        // checked if `exp == F::DENORMAL_EXPONENT` and no hidden mask
        // bit was set. Here, we handle that here, rather than later.
        //
        // This might round-down to 0, but shift will be at **max** 65,
        // for halfway cases rounding towards 0.
        let shift = -fp.exp + 1;
        debug_assert!(shift <= 65);
        cb(fp, shift.min(64));
        // Check for round-up: if rounding-nearest carried us to the hidden bit.
        fp.exp = (fp.mant >= F::HIDDEN_BIT_MASK.as_u64()) as i32;
        return;
    }

    // The float is normal, round to the hidden bit.
    cb(fp, mantissa_shift);

    // Check if we carried, and if so, shift the bit to the hidden bit.
    let carry_mask = F::CARRY_MASK.as_u64();
    if fp.mant & carry_mask == carry_mask {
        fp.mant >>= 1;
        fp.exp += 1;
    }

    // Handle if we carried and check for overflow again.
    if fp.exp >= F::INFINITE_POWER {
        // Exponent is above largest normal value, must be infinite.
        *fp = fp_inf;
        return;
    }

    // Remove the hidden bit.
    fp.mant &= F::MANTISSA_MASK.as_u64();
}

/// Shift right N-bytes and round towards a direction.
///
/// Callback should take the following parameters:
///     1. `is_odd`
///     1. `is_halfway`
///     1. `is_above`
#[cfg_attr(not(feature = "compact"), inline(always))]
pub fn round_nearest_tie_even<Cb>(fp: &mut ExtendedFloat80, shift: i32, cb: Cb)
where
    // `is_odd`, `is_halfway`, `is_above`
    Cb: Fn(bool, bool, bool) -> bool,
{
    // Ensure we've already handled denormal values that underflow.
    debug_assert!(shift <= 64);

    // Extract the truncated bits using mask.
    // Calculate if the value of the truncated bits are either above
    // the mid-way point, or equal to it.
    //
    // For example, for 4 truncated bytes, the mask would be 0b1111
    // and the midway point would be 0b1000.
    let mask = lower_n_mask(shift as u64);
    let halfway = lower_n_halfway(shift as u64);
    let truncated_bits = fp.mant & mask;
    let is_above = truncated_bits > halfway;
    let is_halfway = truncated_bits == halfway;

    // Bit shift so the leading bit is in the hidden bit.
    // This optimizes pretty well:
    //  ```text
    //   mov     ecx, esi
    //   shr     rdi, cl
    //   xor     eax, eax
    //   cmp     esi, 64
    //   cmovne  rax, rdi
    //   ret
    //  ```
    fp.mant = match shift == 64 {
        true => 0,
        false => fp.mant >> shift,
    };
    fp.exp += shift;

    // Extract the last bit after shifting (and determine if it is odd).
    let is_odd = fp.mant & 1 == 1;

    // Calculate if we need to roundup.
    // We need to roundup if we are above halfway, or if we are odd
    // and at half-way (need to tie-to-even). Avoid the branch here.
    fp.mant += cb(is_odd, is_halfway, is_above) as u64;
}

/// Round our significant digits into place, truncating them.
#[cfg_attr(not(feature = "compact"), inline(always))]
pub fn round_down(fp: &mut ExtendedFloat80, shift: i32) {
    // Might have a shift greater than 64 if we have an error.
    fp.mant = match shift == 64 {
        true => 0,
        false => fp.mant >> shift,
    };
    fp.exp += shift;
}
