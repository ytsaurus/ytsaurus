//! Pre-computed tables for writing non-decimal strings.

#![cfg(feature = "power-of-two")]
#![cfg(not(feature = "compact"))]
#![doc(hidden)]

use lexical_util::num::Float;

#[cfg(not(feature = "radix"))]
use crate::table_decimal::*;

// HELPERS
// -------

/// Get lookup table for small int powers.
///
/// # Safety
///
/// Safe as long as the radix provided is valid, and exponent is smaller
/// than the table for the radix.
#[inline(always)]
#[cfg(not(feature = "radix"))]
pub const fn get_small_int_power(exponent: usize, radix: u32) -> u64 {
    // NOTE: don't check the radix since we also use it for half radix, or 5.
    match radix {
        2 => get_small_int_power2(exponent),
        4 => get_small_int_power4(exponent),
        5 => get_small_int_power5(exponent),
        8 => get_small_int_power8(exponent),
        10 => get_small_int_power10(exponent),
        16 => get_small_int_power16(exponent),
        32 => get_small_int_power32(exponent),
        _ => unreachable!(),
    }
}

/// Get lookup table for small f32 powers.
#[inline(always)]
#[cfg(not(feature = "radix"))]
pub fn get_small_f32_power(exponent: usize, radix: u32) -> f32 {
    match radix {
        2 => get_small_f32_power2(exponent),
        4 => get_small_f32_power4(exponent),
        8 => get_small_f32_power8(exponent),
        10 => get_small_f32_power10(exponent),
        16 => get_small_f32_power16(exponent),
        32 => get_small_f32_power32(exponent),
        _ => unreachable!(),
    }
}

/// Get lookup table for small f64 powers.
///
/// # Safety
///
/// Safe as long as the radix provided is valid, and exponent is smaller
/// than the table for the radix.
#[inline(always)]
#[cfg(not(feature = "radix"))]
pub fn get_small_f64_power(exponent: usize, radix: u32) -> f64 {
    match radix {
        2 => get_small_f64_power2(exponent),
        4 => get_small_f64_power4(exponent),
        8 => get_small_f64_power8(exponent),
        10 => get_small_f64_power10(exponent),
        16 => get_small_f64_power16(exponent),
        32 => get_small_f64_power32(exponent),
        _ => unreachable!(),
    }
}

//  NOTE:
//      These functions use the fact that **all** powers-of-two
//      can be exactly represented and cheaply using bit shifts for
//      integers, or by setting the exponent directly.

/// Get pre-computed int power of 2.
#[inline(always)]
pub const fn get_small_int_power2(exponent: usize) -> u64 {
    1 << exponent
}

/// Get pre-computed f32 power of 2.
#[inline(always)]
pub fn get_small_f32_power2(exponent: usize) -> f32 {
    // Can't handle values above the denormal size.
    debug_assert!(exponent as i32 <= f32::EXPONENT_BIAS - f32::MANTISSA_SIZE);
    let shift = (f32::EXPONENT_BIAS - f32::MANTISSA_SIZE) as u32;
    let bits = (exponent as u32 + shift) << f32::MANTISSA_SIZE;
    f32::from_bits(bits)
}

/// Get pre-computed f64 power of 2.
#[inline(always)]
pub fn get_small_f64_power2(exponent: usize) -> f64 {
    // Can't handle values above the denormal size.
    debug_assert!(exponent as i32 <= f64::EXPONENT_BIAS - f64::MANTISSA_SIZE);
    let shift = (f64::EXPONENT_BIAS - f64::MANTISSA_SIZE) as u64;
    let bits = (exponent as u64 + shift) << f64::MANTISSA_SIZE;
    f64::from_bits(bits)
}

/// Get pre-computed int power of 4.
#[inline(always)]
pub const fn get_small_int_power4(exponent: usize) -> u64 {
    get_small_int_power2(2 * exponent)
}

/// Get pre-computed f32 power of 4.
#[inline(always)]
pub fn get_small_f32_power4(exponent: usize) -> f32 {
    get_small_f32_power2(2 * exponent)
}

/// Get pre-computed f64 power of 4.
#[inline(always)]
pub fn get_small_f64_power4(exponent: usize) -> f64 {
    get_small_f64_power2(2 * exponent)
}

/// Get pre-computed int power of 8.
#[inline(always)]
pub const fn get_small_int_power8(exponent: usize) -> u64 {
    get_small_int_power2(3 * exponent)
}

/// Get pre-computed f32 power of 8.
#[inline(always)]
pub fn get_small_f32_power8(exponent: usize) -> f32 {
    get_small_f32_power2(3 * exponent)
}

/// Get pre-computed f64 power of 8.
#[inline(always)]
pub fn get_small_f64_power8(exponent: usize) -> f64 {
    get_small_f64_power2(3 * exponent)
}

/// Get pre-computed int power of 16.
#[inline(always)]
pub const fn get_small_int_power16(exponent: usize) -> u64 {
    get_small_int_power2(4 * exponent)
}

/// Get pre-computed f32 power of 16.
#[inline(always)]
pub fn get_small_f32_power16(exponent: usize) -> f32 {
    get_small_f32_power2(4 * exponent)
}

/// Get pre-computed f64 power of 16.
#[inline(always)]
pub fn get_small_f64_power16(exponent: usize) -> f64 {
    get_small_f64_power2(4 * exponent)
}

/// Get pre-computed int power of 32.
#[inline(always)]
pub const fn get_small_int_power32(exponent: usize) -> u64 {
    get_small_int_power2(5 * exponent)
}

/// Get pre-computed f32 power of 32.
#[inline(always)]
pub fn get_small_f32_power32(exponent: usize) -> f32 {
    get_small_f32_power2(5 * exponent)
}

/// Get pre-computed f64 power of 32.
#[inline(always)]
pub fn get_small_f64_power32(exponent: usize) -> f64 {
    get_small_f64_power2(5 * exponent)
}
