//! Pre-computed tables for writing non-decimal strings.

#![cfg(feature = "radix")]
#![cfg(not(feature = "compact"))]
#![doc(hidden)]
#![allow(clippy::excessive_precision)] // reason = "auto-generated values that need to be exact"

use lexical_util::assert::debug_assert_radix;

use crate::bigint::Limb;
use crate::limits::{f32_exponent_limit, f64_exponent_limit, f64_mantissa_limit, u64_power_limit};
use crate::table_binary::*;
use crate::table_decimal::*;

// HELPERS
// -------

/// Get lookup table for 2 digit radix conversions.
#[inline(always)]
pub const fn get_small_int_power(exponent: usize, radix: u32) -> u64 {
    match radix {
        2 => get_small_int_power2(exponent),
        3 => get_small_int_power3(exponent),
        4 => get_small_int_power4(exponent),
        5 => get_small_int_power5(exponent),
        6 => get_small_int_power6(exponent),
        7 => get_small_int_power7(exponent),
        8 => get_small_int_power8(exponent),
        9 => get_small_int_power9(exponent),
        10 => get_small_int_power10(exponent),
        11 => get_small_int_power11(exponent),
        12 => get_small_int_power12(exponent),
        13 => get_small_int_power13(exponent),
        14 => get_small_int_power14(exponent),
        15 => get_small_int_power15(exponent),
        16 => get_small_int_power16(exponent),
        17 => get_small_int_power17(exponent),
        18 => get_small_int_power18(exponent),
        19 => get_small_int_power19(exponent),
        20 => get_small_int_power20(exponent),
        21 => get_small_int_power21(exponent),
        22 => get_small_int_power22(exponent),
        23 => get_small_int_power23(exponent),
        24 => get_small_int_power24(exponent),
        25 => get_small_int_power25(exponent),
        26 => get_small_int_power26(exponent),
        27 => get_small_int_power27(exponent),
        28 => get_small_int_power28(exponent),
        29 => get_small_int_power29(exponent),
        30 => get_small_int_power30(exponent),
        31 => get_small_int_power31(exponent),
        32 => get_small_int_power32(exponent),
        33 => get_small_int_power33(exponent),
        34 => get_small_int_power34(exponent),
        35 => get_small_int_power35(exponent),
        36 => get_small_int_power36(exponent),
        _ => unreachable!(),
    }
}

/// Get lookup table for small f32 powers.
#[inline(always)]
pub fn get_small_f32_power(exponent: usize, radix: u32) -> f32 {
    debug_assert_radix(radix);
    match radix {
        2 => get_small_f32_power2(exponent),
        3 => get_small_f32_power3(exponent),
        4 => get_small_f32_power4(exponent),
        5 => get_small_f32_power5(exponent),
        6 => get_small_f32_power6(exponent),
        7 => get_small_f32_power7(exponent),
        8 => get_small_f32_power8(exponent),
        9 => get_small_f32_power9(exponent),
        10 => get_small_f32_power10(exponent),
        11 => get_small_f32_power11(exponent),
        12 => get_small_f32_power12(exponent),
        13 => get_small_f32_power13(exponent),
        14 => get_small_f32_power14(exponent),
        15 => get_small_f32_power15(exponent),
        16 => get_small_f32_power16(exponent),
        17 => get_small_f32_power17(exponent),
        18 => get_small_f32_power18(exponent),
        19 => get_small_f32_power19(exponent),
        20 => get_small_f32_power20(exponent),
        21 => get_small_f32_power21(exponent),
        22 => get_small_f32_power22(exponent),
        23 => get_small_f32_power23(exponent),
        24 => get_small_f32_power24(exponent),
        25 => get_small_f32_power25(exponent),
        26 => get_small_f32_power26(exponent),
        27 => get_small_f32_power27(exponent),
        28 => get_small_f32_power28(exponent),
        29 => get_small_f32_power29(exponent),
        30 => get_small_f32_power30(exponent),
        31 => get_small_f32_power31(exponent),
        32 => get_small_f32_power32(exponent),
        33 => get_small_f32_power33(exponent),
        34 => get_small_f32_power34(exponent),
        35 => get_small_f32_power35(exponent),
        36 => get_small_f32_power36(exponent),
        _ => unreachable!(),
    }
}

/// Get lookup table for small f64 powers.
#[inline(always)]
pub fn get_small_f64_power(exponent: usize, radix: u32) -> f64 {
    debug_assert_radix(radix);
    match radix {
        2 => get_small_f64_power2(exponent),
        3 => get_small_f64_power3(exponent),
        4 => get_small_f64_power4(exponent),
        5 => get_small_f64_power5(exponent),
        6 => get_small_f64_power6(exponent),
        7 => get_small_f64_power7(exponent),
        8 => get_small_f64_power8(exponent),
        9 => get_small_f64_power9(exponent),
        10 => get_small_f64_power10(exponent),
        11 => get_small_f64_power11(exponent),
        12 => get_small_f64_power12(exponent),
        13 => get_small_f64_power13(exponent),
        14 => get_small_f64_power14(exponent),
        15 => get_small_f64_power15(exponent),
        16 => get_small_f64_power16(exponent),
        17 => get_small_f64_power17(exponent),
        18 => get_small_f64_power18(exponent),
        19 => get_small_f64_power19(exponent),
        20 => get_small_f64_power20(exponent),
        21 => get_small_f64_power21(exponent),
        22 => get_small_f64_power22(exponent),
        23 => get_small_f64_power23(exponent),
        24 => get_small_f64_power24(exponent),
        25 => get_small_f64_power25(exponent),
        26 => get_small_f64_power26(exponent),
        27 => get_small_f64_power27(exponent),
        28 => get_small_f64_power28(exponent),
        29 => get_small_f64_power29(exponent),
        30 => get_small_f64_power30(exponent),
        31 => get_small_f64_power31(exponent),
        32 => get_small_f64_power32(exponent),
        33 => get_small_f64_power33(exponent),
        34 => get_small_f64_power34(exponent),
        35 => get_small_f64_power35(exponent),
        36 => get_small_f64_power36(exponent),
        _ => unreachable!(),
    }
}

/// Get pre-computed power for a large power of radix.
pub const fn get_large_int_power(radix: u32) -> (&'static [Limb], u32) {
    match radix {
        3 => (&LARGE_POW3, LARGE_POW3_STEP),
        5 => (&LARGE_POW5, LARGE_POW5_STEP),
        7 => (&LARGE_POW7, LARGE_POW7_STEP),
        9 => (&LARGE_POW9, LARGE_POW9_STEP),
        11 => (&LARGE_POW11, LARGE_POW11_STEP),
        13 => (&LARGE_POW13, LARGE_POW13_STEP),
        15 => (&LARGE_POW15, LARGE_POW15_STEP),
        17 => (&LARGE_POW17, LARGE_POW17_STEP),
        19 => (&LARGE_POW19, LARGE_POW19_STEP),
        21 => (&LARGE_POW21, LARGE_POW21_STEP),
        23 => (&LARGE_POW23, LARGE_POW23_STEP),
        25 => (&LARGE_POW25, LARGE_POW25_STEP),
        27 => (&LARGE_POW27, LARGE_POW27_STEP),
        29 => (&LARGE_POW29, LARGE_POW29_STEP),
        31 => (&LARGE_POW31, LARGE_POW31_STEP),
        33 => (&LARGE_POW33, LARGE_POW33_STEP),
        // Remaining radix: must be 35.
        _ => (&LARGE_POW35, LARGE_POW35_STEP),
    }
}

/// Get pre-computed int power of 3.
#[inline(always)]
pub const fn get_small_int_power3(exponent: usize) -> u64 {
    SMALL_INT_POW3[exponent]
}

/// Get pre-computed f32 power of 3.
#[inline(always)]
pub fn get_small_f32_power3(exponent: usize) -> f32 {
    SMALL_F32_POW3[exponent]
}

/// Get pre-computed f64 power of 3.
#[inline(always)]
pub fn get_small_f64_power3(exponent: usize) -> f64 {
    SMALL_F64_POW3[exponent]
}

/// Get pre-computed f32 power of 5.
#[inline(always)]
pub fn get_small_f32_power5(exponent: usize) -> f32 {
    SMALL_F32_POW5[exponent]
}

/// Get pre-computed f64 power of 5.
#[inline(always)]
pub fn get_small_f64_power5(exponent: usize) -> f64 {
    SMALL_F64_POW5[exponent]
}

/// Get pre-computed int power of 6.
#[inline(always)]
pub const fn get_small_int_power6(exponent: usize) -> u64 {
    SMALL_INT_POW6[exponent]
}

/// Get pre-computed f32 power of 6.
#[inline(always)]
pub fn get_small_f32_power6(exponent: usize) -> f32 {
    SMALL_F32_POW6[exponent]
}

/// Get pre-computed f64 power of 6.
#[inline(always)]
pub fn get_small_f64_power6(exponent: usize) -> f64 {
    SMALL_F64_POW6[exponent]
}

/// Get pre-computed int power of 7.
#[inline(always)]
pub const fn get_small_int_power7(exponent: usize) -> u64 {
    SMALL_INT_POW7[exponent]
}

/// Get pre-computed f32 power of 7.
#[inline(always)]
pub fn get_small_f32_power7(exponent: usize) -> f32 {
    SMALL_F32_POW7[exponent]
}

/// Get pre-computed f64 power of 7.
#[inline(always)]
pub fn get_small_f64_power7(exponent: usize) -> f64 {
    SMALL_F64_POW7[exponent]
}

/// Get pre-computed int power of 9.
#[inline(always)]
pub const fn get_small_int_power9(exponent: usize) -> u64 {
    SMALL_INT_POW9[exponent]
}

/// Get pre-computed f32 power of 9.
#[inline(always)]
pub fn get_small_f32_power9(exponent: usize) -> f32 {
    SMALL_F32_POW9[exponent]
}

/// Get pre-computed f64 power of 9.
#[inline(always)]
pub fn get_small_f64_power9(exponent: usize) -> f64 {
    SMALL_F64_POW9[exponent]
}

/// Get pre-computed int power of 11.
#[inline(always)]
pub const fn get_small_int_power11(exponent: usize) -> u64 {
    SMALL_INT_POW11[exponent]
}

/// Get pre-computed f32 power of 11.
#[inline(always)]
pub fn get_small_f32_power11(exponent: usize) -> f32 {
    SMALL_F32_POW11[exponent]
}

/// Get pre-computed f64 power of 11.
#[inline(always)]
pub fn get_small_f64_power11(exponent: usize) -> f64 {
    SMALL_F64_POW11[exponent]
}

/// Get pre-computed int power of 12.
#[inline(always)]
pub const fn get_small_int_power12(exponent: usize) -> u64 {
    SMALL_INT_POW12[exponent]
}

/// Get pre-computed f32 power of 12.
#[inline(always)]
pub fn get_small_f32_power12(exponent: usize) -> f32 {
    SMALL_F32_POW12[exponent]
}

/// Get pre-computed f64 power of 12.
#[inline(always)]
pub fn get_small_f64_power12(exponent: usize) -> f64 {
    SMALL_F64_POW12[exponent]
}

/// Get pre-computed int power of 13.
#[inline(always)]
pub const fn get_small_int_power13(exponent: usize) -> u64 {
    SMALL_INT_POW13[exponent]
}

/// Get pre-computed f32 power of 13.
#[inline(always)]
pub fn get_small_f32_power13(exponent: usize) -> f32 {
    SMALL_F32_POW13[exponent]
}

/// Get pre-computed f64 power of 13.
#[inline(always)]
pub fn get_small_f64_power13(exponent: usize) -> f64 {
    SMALL_F64_POW13[exponent]
}

/// Get pre-computed int power of 14.
#[inline(always)]
pub const fn get_small_int_power14(exponent: usize) -> u64 {
    SMALL_INT_POW14[exponent]
}

/// Get pre-computed f32 power of 14.
#[inline(always)]
pub fn get_small_f32_power14(exponent: usize) -> f32 {
    SMALL_F32_POW14[exponent]
}

/// Get pre-computed f64 power of 14.
#[inline(always)]
pub fn get_small_f64_power14(exponent: usize) -> f64 {
    SMALL_F64_POW14[exponent]
}

/// Get pre-computed int power of 15.
#[inline(always)]
pub const fn get_small_int_power15(exponent: usize) -> u64 {
    SMALL_INT_POW15[exponent]
}

/// Get pre-computed f32 power of 15.
#[inline(always)]
pub fn get_small_f32_power15(exponent: usize) -> f32 {
    SMALL_F32_POW15[exponent]
}

/// Get pre-computed f64 power of 15.
#[inline(always)]
pub fn get_small_f64_power15(exponent: usize) -> f64 {
    SMALL_F64_POW15[exponent]
}

/// Get pre-computed int power of 17.
#[inline(always)]
pub const fn get_small_int_power17(exponent: usize) -> u64 {
    SMALL_INT_POW17[exponent]
}

/// Get pre-computed f32 power of 17.
#[inline(always)]
pub fn get_small_f32_power17(exponent: usize) -> f32 {
    SMALL_F32_POW17[exponent]
}

/// Get pre-computed f64 power of 17.
#[inline(always)]
pub fn get_small_f64_power17(exponent: usize) -> f64 {
    SMALL_F64_POW17[exponent]
}

/// Get pre-computed int power of 18.
#[inline(always)]
pub const fn get_small_int_power18(exponent: usize) -> u64 {
    SMALL_INT_POW18[exponent]
}

/// Get pre-computed f32 power of 18.
#[inline(always)]
pub fn get_small_f32_power18(exponent: usize) -> f32 {
    SMALL_F32_POW18[exponent]
}

/// Get pre-computed f64 power of 18.
#[inline(always)]
pub fn get_small_f64_power18(exponent: usize) -> f64 {
    SMALL_F64_POW18[exponent]
}

/// Get pre-computed int power of 19.
#[inline(always)]
pub const fn get_small_int_power19(exponent: usize) -> u64 {
    SMALL_INT_POW19[exponent]
}

/// Get pre-computed f32 power of 19.
#[inline(always)]
pub fn get_small_f32_power19(exponent: usize) -> f32 {
    SMALL_F32_POW19[exponent]
}

/// Get pre-computed f64 power of 19.
#[inline(always)]
pub fn get_small_f64_power19(exponent: usize) -> f64 {
    SMALL_F64_POW19[exponent]
}

/// Get pre-computed int power of 20.
#[inline(always)]
pub const fn get_small_int_power20(exponent: usize) -> u64 {
    SMALL_INT_POW20[exponent]
}

/// Get pre-computed f32 power of 20.
#[inline(always)]
pub fn get_small_f32_power20(exponent: usize) -> f32 {
    SMALL_F32_POW20[exponent]
}

/// Get pre-computed f64 power of 20.
#[inline(always)]
pub fn get_small_f64_power20(exponent: usize) -> f64 {
    SMALL_F64_POW20[exponent]
}

/// Get pre-computed int power of 21.
#[inline(always)]
pub const fn get_small_int_power21(exponent: usize) -> u64 {
    SMALL_INT_POW21[exponent]
}

/// Get pre-computed f32 power of 21.
#[inline(always)]
pub fn get_small_f32_power21(exponent: usize) -> f32 {
    SMALL_F32_POW21[exponent]
}

/// Get pre-computed f64 power of 21.
#[inline(always)]
pub fn get_small_f64_power21(exponent: usize) -> f64 {
    SMALL_F64_POW21[exponent]
}

/// Get pre-computed int power of 22.
#[inline(always)]
pub const fn get_small_int_power22(exponent: usize) -> u64 {
    SMALL_INT_POW22[exponent]
}

/// Get pre-computed f32 power of 22.
#[inline(always)]
pub fn get_small_f32_power22(exponent: usize) -> f32 {
    SMALL_F32_POW22[exponent]
}

/// Get pre-computed f64 power of 22.
#[inline(always)]
pub fn get_small_f64_power22(exponent: usize) -> f64 {
    SMALL_F64_POW22[exponent]
}

/// Get pre-computed int power of 23.
#[inline(always)]
pub const fn get_small_int_power23(exponent: usize) -> u64 {
    SMALL_INT_POW23[exponent]
}

/// Get pre-computed f32 power of 23.
#[inline(always)]
pub fn get_small_f32_power23(exponent: usize) -> f32 {
    SMALL_F32_POW23[exponent]
}

/// Get pre-computed f64 power of 23.
#[inline(always)]
pub fn get_small_f64_power23(exponent: usize) -> f64 {
    SMALL_F64_POW23[exponent]
}

/// Get pre-computed int power of 24.
#[inline(always)]
pub const fn get_small_int_power24(exponent: usize) -> u64 {
    SMALL_INT_POW24[exponent]
}

/// Get pre-computed f32 power of 24.
#[inline(always)]
pub fn get_small_f32_power24(exponent: usize) -> f32 {
    SMALL_F32_POW24[exponent]
}

/// Get pre-computed f64 power of 24.
#[inline(always)]
pub fn get_small_f64_power24(exponent: usize) -> f64 {
    SMALL_F64_POW24[exponent]
}

/// Get pre-computed int power of 25.
#[inline(always)]
pub const fn get_small_int_power25(exponent: usize) -> u64 {
    SMALL_INT_POW25[exponent]
}

/// Get pre-computed f32 power of 25.
#[inline(always)]
pub fn get_small_f32_power25(exponent: usize) -> f32 {
    SMALL_F32_POW25[exponent]
}

/// Get pre-computed f64 power of 25.
#[inline(always)]
pub fn get_small_f64_power25(exponent: usize) -> f64 {
    SMALL_F64_POW25[exponent]
}

/// Get pre-computed int power of 26.
#[inline(always)]
pub const fn get_small_int_power26(exponent: usize) -> u64 {
    SMALL_INT_POW26[exponent]
}

/// Get pre-computed f32 power of 26.
#[inline(always)]
pub fn get_small_f32_power26(exponent: usize) -> f32 {
    SMALL_F32_POW26[exponent]
}

/// Get pre-computed f64 power of 26.
#[inline(always)]
pub fn get_small_f64_power26(exponent: usize) -> f64 {
    SMALL_F64_POW26[exponent]
}

/// Get pre-computed int power of 27.
#[inline(always)]
pub const fn get_small_int_power27(exponent: usize) -> u64 {
    SMALL_INT_POW27[exponent]
}

/// Get pre-computed f32 power of 27.
#[inline(always)]
pub fn get_small_f32_power27(exponent: usize) -> f32 {
    SMALL_F32_POW27[exponent]
}

/// Get pre-computed f64 power of 27.
#[inline(always)]
pub fn get_small_f64_power27(exponent: usize) -> f64 {
    SMALL_F64_POW27[exponent]
}

/// Get pre-computed int power of 28.
#[inline(always)]
pub const fn get_small_int_power28(exponent: usize) -> u64 {
    SMALL_INT_POW28[exponent]
}

/// Get pre-computed f32 power of 28.
#[inline(always)]
pub fn get_small_f32_power28(exponent: usize) -> f32 {
    SMALL_F32_POW28[exponent]
}

/// Get pre-computed f64 power of 28.
#[inline(always)]
pub fn get_small_f64_power28(exponent: usize) -> f64 {
    SMALL_F64_POW28[exponent]
}

/// Get pre-computed int power of 29.
#[inline(always)]
pub const fn get_small_int_power29(exponent: usize) -> u64 {
    SMALL_INT_POW29[exponent]
}

/// Get pre-computed f32 power of 29.
#[inline(always)]
pub fn get_small_f32_power29(exponent: usize) -> f32 {
    SMALL_F32_POW29[exponent]
}

/// Get pre-computed f64 power of 29.
#[inline(always)]
pub fn get_small_f64_power29(exponent: usize) -> f64 {
    SMALL_F64_POW29[exponent]
}

/// Get pre-computed int power of 30.
#[inline(always)]
pub const fn get_small_int_power30(exponent: usize) -> u64 {
    SMALL_INT_POW30[exponent]
}

/// Get pre-computed f32 power of 30.
#[inline(always)]
pub fn get_small_f32_power30(exponent: usize) -> f32 {
    SMALL_F32_POW30[exponent]
}

/// Get pre-computed f64 power of 30.
#[inline(always)]
pub fn get_small_f64_power30(exponent: usize) -> f64 {
    SMALL_F64_POW30[exponent]
}

/// Get pre-computed int power of 31.
#[inline(always)]
pub const fn get_small_int_power31(exponent: usize) -> u64 {
    SMALL_INT_POW31[exponent]
}

/// Get pre-computed f32 power of 31.
#[inline(always)]
pub fn get_small_f32_power31(exponent: usize) -> f32 {
    SMALL_F32_POW31[exponent]
}

/// Get pre-computed f64 power of 31.
#[inline(always)]
pub fn get_small_f64_power31(exponent: usize) -> f64 {
    SMALL_F64_POW31[exponent]
}

/// Get pre-computed int power of 33.
#[inline(always)]
pub const fn get_small_int_power33(exponent: usize) -> u64 {
    SMALL_INT_POW33[exponent]
}

/// Get pre-computed f32 power of 33.
#[inline(always)]
pub fn get_small_f32_power33(exponent: usize) -> f32 {
    SMALL_F32_POW33[exponent]
}

/// Get pre-computed f64 power of 33.
#[inline(always)]
pub fn get_small_f64_power33(exponent: usize) -> f64 {
    SMALL_F64_POW33[exponent]
}

/// Get pre-computed int power of 34.
#[inline(always)]
pub const fn get_small_int_power34(exponent: usize) -> u64 {
    SMALL_INT_POW34[exponent]
}

/// Get pre-computed f32 power of 34.
#[inline(always)]
pub fn get_small_f32_power34(exponent: usize) -> f32 {
    SMALL_F32_POW34[exponent]
}

/// Get pre-computed f64 power of 34.
#[inline(always)]
pub fn get_small_f64_power34(exponent: usize) -> f64 {
    SMALL_F64_POW34[exponent]
}

/// Get pre-computed int power of 35.
#[inline(always)]
pub const fn get_small_int_power35(exponent: usize) -> u64 {
    SMALL_INT_POW35[exponent]
}

/// Get pre-computed f32 power of 35.
#[inline(always)]
pub fn get_small_f32_power35(exponent: usize) -> f32 {
    SMALL_F32_POW35[exponent]
}

/// Get pre-computed f64 power of 35.
#[inline(always)]
pub fn get_small_f64_power35(exponent: usize) -> f64 {
    SMALL_F64_POW35[exponent]
}

/// Get pre-computed int power of 36.
#[inline(always)]
pub const fn get_small_int_power36(exponent: usize) -> u64 {
    SMALL_INT_POW36[exponent]
}

/// Get pre-computed f32 power of 36.
#[inline(always)]
pub fn get_small_f32_power36(exponent: usize) -> f32 {
    SMALL_F32_POW36[exponent]
}

/// Get pre-computed f64 power of 36.
#[inline(always)]
pub fn get_small_f64_power36(exponent: usize) -> f64 {
    SMALL_F64_POW36[exponent]
}

// TABLES
// ------

//  NOTE:
//      These tables were automatically generated using `etc/powers_table.py`.
//      Do not modify them unless you have a very good reason to.

/// Pre-computed, small powers-of-3.
pub const SMALL_INT_POW3: [u64; 41] = [
    1,
    3,
    9,
    27,
    81,
    243,
    729,
    2187,
    6561,
    19683,
    59049,
    177147,
    531441,
    1594323,
    4782969,
    14348907,
    43046721,
    129140163,
    387420489,
    1162261467,
    3486784401,
    10460353203,
    31381059609,
    94143178827,
    282429536481,
    847288609443,
    2541865828329,
    7625597484987,
    22876792454961,
    68630377364883,
    205891132094649,
    617673396283947,
    1853020188851841,
    5559060566555523,
    16677181699666569,
    50031545098999707,
    150094635296999121,
    450283905890997363,
    1350851717672992089,
    4052555153018976267,
    12157665459056928801,
];
const _: () = assert!(SMALL_INT_POW3.len() > f64_mantissa_limit(3) as usize);
const _: () = assert!(SMALL_INT_POW3.len() == u64_power_limit(3) as usize + 1);

/// Pre-computed, small powers-of-3.
pub const SMALL_F32_POW3: [f32; 16] = [
    1.0, 3.0, 9.0, 27.0, 81.0, 243.0, 729.0, 2187.0, 6561.0, 19683.0, 59049.0, 177147.0, 531441.0,
    1594323.0, 4782969.0, 14348907.0,
];
const _: () = assert!(SMALL_F32_POW3.len() > f32_exponent_limit(3).1 as usize);

/// Pre-computed, small powers-of-3.
pub const SMALL_F64_POW3: [f64; 34] = [
    1.0,
    3.0,
    9.0,
    27.0,
    81.0,
    243.0,
    729.0,
    2187.0,
    6561.0,
    19683.0,
    59049.0,
    177147.0,
    531441.0,
    1594323.0,
    4782969.0,
    14348907.0,
    43046721.0,
    129140163.0,
    387420489.0,
    1162261467.0,
    3486784401.0,
    10460353203.0,
    31381059609.0,
    94143178827.0,
    282429536481.0,
    847288609443.0,
    2541865828329.0,
    7625597484987.0,
    22876792454961.0,
    68630377364883.0,
    205891132094649.0,
    617673396283947.0,
    1853020188851841.0,
    5559060566555523.0,
];
const _: () = assert!(SMALL_F64_POW3.len() > f64_exponent_limit(3).1 as usize);

/// Pre-computed large power-of-3 for 32-bit limbs.
#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub const LARGE_POW3: [u32; 10] = [
    2868424865, 1543175966, 3836194338, 2213345014, 1148585654, 4252227966, 1995653935, 3256521594,
    1051739806, 534087228,
];

/// Pre-computed large power-of-3 for 64-bit limbs.
#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub const LARGE_POW3: [u64; 5] = [
    6627890308811632801,
    9506244453730856482,
    18263180050255185590,
    13986653746943443759,
    2293887178523035294,
];

/// Step for large power-of-3 for 32-bit limbs.
pub const LARGE_POW3_STEP: u32 = 200;

/// Pre-computed, small powers-of-5.
pub const SMALL_F32_POW5: [f32; 11] =
    [1.0, 5.0, 25.0, 125.0, 625.0, 3125.0, 15625.0, 78125.0, 390625.0, 1953125.0, 9765625.0];
const _: () = assert!(SMALL_F32_POW5.len() > f32_exponent_limit(5).1 as usize);

/// Pre-computed, small powers-of-5.
pub const SMALL_F64_POW5: [f64; 23] = [
    1.0,
    5.0,
    25.0,
    125.0,
    625.0,
    3125.0,
    15625.0,
    78125.0,
    390625.0,
    1953125.0,
    9765625.0,
    48828125.0,
    244140625.0,
    1220703125.0,
    6103515625.0,
    30517578125.0,
    152587890625.0,
    762939453125.0,
    3814697265625.0,
    19073486328125.0,
    95367431640625.0,
    476837158203125.0,
    2384185791015625.0,
];
const _: () = assert!(SMALL_F64_POW5.len() > f64_exponent_limit(5).1 as usize);

/// Pre-computed, small powers-of-6.
pub const SMALL_INT_POW6: [u64; 25] = [
    1,
    6,
    36,
    216,
    1296,
    7776,
    46656,
    279936,
    1679616,
    10077696,
    60466176,
    362797056,
    2176782336,
    13060694016,
    78364164096,
    470184984576,
    2821109907456,
    16926659444736,
    101559956668416,
    609359740010496,
    3656158440062976,
    21936950640377856,
    131621703842267136,
    789730223053602816,
    4738381338321616896,
];
const _: () = assert!(SMALL_INT_POW6.len() > f64_mantissa_limit(6) as usize);
const _: () = assert!(SMALL_INT_POW6.len() == u64_power_limit(6) as usize + 1);

/// Pre-computed, small powers-of-6.
pub const SMALL_F32_POW6: [f32; 16] = [
    1.0,
    6.0,
    36.0,
    216.0,
    1296.0,
    7776.0,
    46656.0,
    279936.0,
    1679616.0,
    10077696.0,
    60466176.0,
    362797056.0,
    2176782336.0,
    13060694016.0,
    78364164096.0,
    470184984576.0,
];
const _: () = assert!(SMALL_F32_POW6.len() > f32_exponent_limit(6).1 as usize);

/// Pre-computed, small powers-of-6.
pub const SMALL_F64_POW6: [f64; 34] = [
    1.0,
    6.0,
    36.0,
    216.0,
    1296.0,
    7776.0,
    46656.0,
    279936.0,
    1679616.0,
    10077696.0,
    60466176.0,
    362797056.0,
    2176782336.0,
    13060694016.0,
    78364164096.0,
    470184984576.0,
    2821109907456.0,
    16926659444736.0,
    101559956668416.0,
    609359740010496.0,
    3656158440062976.0,
    2.1936950640377856e+16,
    1.3162170384226714e+17,
    7.897302230536028e+17,
    4.738381338321617e+18,
    2.84302880299297e+19,
    1.705817281795782e+20,
    1.0234903690774692e+21,
    6.140942214464815e+21,
    3.6845653286788893e+22,
    2.2107391972073336e+23,
    1.3264435183244001e+24,
    7.958661109946401e+24,
    4.7751966659678405e+25,
];
const _: () = assert!(SMALL_F64_POW6.len() > f64_exponent_limit(6).1 as usize);

/// Pre-computed, small powers-of-7.
pub const SMALL_INT_POW7: [u64; 23] = [
    1,
    7,
    49,
    343,
    2401,
    16807,
    117649,
    823543,
    5764801,
    40353607,
    282475249,
    1977326743,
    13841287201,
    96889010407,
    678223072849,
    4747561509943,
    33232930569601,
    232630513987207,
    1628413597910449,
    11398895185373143,
    79792266297612001,
    558545864083284007,
    3909821048582988049,
];
const _: () = assert!(SMALL_INT_POW7.len() > f64_mantissa_limit(7) as usize);
const _: () = assert!(SMALL_INT_POW7.len() == u64_power_limit(7) as usize + 1);

/// Pre-computed, small powers-of-7.
pub const SMALL_F32_POW7: [f32; 9] =
    [1.0, 7.0, 49.0, 343.0, 2401.0, 16807.0, 117649.0, 823543.0, 5764801.0];
const _: () = assert!(SMALL_F32_POW7.len() > f32_exponent_limit(7).1 as usize);

/// Pre-computed, small powers-of-7.
pub const SMALL_F64_POW7: [f64; 19] = [
    1.0,
    7.0,
    49.0,
    343.0,
    2401.0,
    16807.0,
    117649.0,
    823543.0,
    5764801.0,
    40353607.0,
    282475249.0,
    1977326743.0,
    13841287201.0,
    96889010407.0,
    678223072849.0,
    4747561509943.0,
    33232930569601.0,
    232630513987207.0,
    1628413597910449.0,
];
const _: () = assert!(SMALL_F64_POW7.len() > f64_exponent_limit(7).1 as usize);

/// Pre-computed large power-of-7 for 32-bit limbs.
#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub const LARGE_POW7: [u32; 10] = [
    3938635601, 4013708425, 513691597, 1762742544, 3619207677, 480247883, 3793395133, 740892944,
    1592317061, 1837154,
];

/// Pre-computed large power-of-7 for 64-bit limbs.
#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub const LARGE_POW7: [u64; 5] = [
    17238746424993304401,
    7570921578261532621,
    2062648955077442045,
    3182110968110554557,
    7890517940032645,
];

/// Step for large power-of-7 for 32-bit limbs.
pub const LARGE_POW7_STEP: u32 = 110;

/// Pre-computed, small powers-of-9.
pub const SMALL_INT_POW9: [u64; 21] = [
    1,
    9,
    81,
    729,
    6561,
    59049,
    531441,
    4782969,
    43046721,
    387420489,
    3486784401,
    31381059609,
    282429536481,
    2541865828329,
    22876792454961,
    205891132094649,
    1853020188851841,
    16677181699666569,
    150094635296999121,
    1350851717672992089,
    12157665459056928801,
];
const _: () = assert!(SMALL_INT_POW9.len() > f64_mantissa_limit(9) as usize);
const _: () = assert!(SMALL_INT_POW9.len() == u64_power_limit(9) as usize + 1);

/// Pre-computed, small powers-of-9.
pub const SMALL_F32_POW9: [f32; 8] = [1.0, 9.0, 81.0, 729.0, 6561.0, 59049.0, 531441.0, 4782969.0];
const _: () = assert!(SMALL_F32_POW9.len() > f32_exponent_limit(9).1 as usize);

/// Pre-computed, small powers-of-9.
pub const SMALL_F64_POW9: [f64; 17] = [
    1.0,
    9.0,
    81.0,
    729.0,
    6561.0,
    59049.0,
    531441.0,
    4782969.0,
    43046721.0,
    387420489.0,
    3486784401.0,
    31381059609.0,
    282429536481.0,
    2541865828329.0,
    22876792454961.0,
    205891132094649.0,
    1853020188851841.0,
];
const _: () = assert!(SMALL_F64_POW9.len() > f64_exponent_limit(9).1 as usize);

/// Pre-computed large power-of-9 for 32-bit limbs.
#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub const LARGE_POW9: [u32; 10] = [
    2868424865, 1543175966, 3836194338, 2213345014, 1148585654, 4252227966, 1995653935, 3256521594,
    1051739806, 534087228,
];

/// Pre-computed large power-of-9 for 64-bit limbs.
#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub const LARGE_POW9: [u64; 5] = [
    6627890308811632801,
    9506244453730856482,
    18263180050255185590,
    13986653746943443759,
    2293887178523035294,
];

/// Step for large power-of-9 for 32-bit limbs.
pub const LARGE_POW9_STEP: u32 = 100;

/// Pre-computed, small powers-of-11.
pub const SMALL_INT_POW11: [u64; 19] = [
    1,
    11,
    121,
    1331,
    14641,
    161051,
    1771561,
    19487171,
    214358881,
    2357947691,
    25937424601,
    285311670611,
    3138428376721,
    34522712143931,
    379749833583241,
    4177248169415651,
    45949729863572161,
    505447028499293771,
    5559917313492231481,
];
const _: () = assert!(SMALL_INT_POW11.len() > f64_mantissa_limit(11) as usize);
const _: () = assert!(SMALL_INT_POW11.len() == u64_power_limit(11) as usize + 1);

/// Pre-computed, small powers-of-11.
pub const SMALL_F32_POW11: [f32; 7] = [1.0, 11.0, 121.0, 1331.0, 14641.0, 161051.0, 1771561.0];
const _: () = assert!(SMALL_F32_POW11.len() > f32_exponent_limit(11).1 as usize);

/// Pre-computed, small powers-of-11.
pub const SMALL_F64_POW11: [f64; 16] = [
    1.0,
    11.0,
    121.0,
    1331.0,
    14641.0,
    161051.0,
    1771561.0,
    19487171.0,
    214358881.0,
    2357947691.0,
    25937424601.0,
    285311670611.0,
    3138428376721.0,
    34522712143931.0,
    379749833583241.0,
    4177248169415651.0,
];
const _: () = assert!(SMALL_F64_POW11.len() > f64_exponent_limit(11).1 as usize);

/// Pre-computed large power-of-11 for 32-bit limbs.
#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub const LARGE_POW11: [u32; 10] = [
    2172432537, 2346616081, 1851665372, 2301834192, 1763429507, 4086589879, 4002403721, 2932076170,
    987565374, 10683238,
];

/// Pre-computed large power-of-11 for 64-bit limbs.
#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub const LARGE_POW11: [u64; 5] = [
    10078639326335119513,
    9886302577306250204,
    17551769884233026691,
    12593171263533340041,
    45884158812949822,
];

/// Step for large power-of-11 for 32-bit limbs.
pub const LARGE_POW11_STEP: u32 = 90;

/// Pre-computed, small powers-of-12.
pub const SMALL_INT_POW12: [u64; 18] = [
    1,
    12,
    144,
    1728,
    20736,
    248832,
    2985984,
    35831808,
    429981696,
    5159780352,
    61917364224,
    743008370688,
    8916100448256,
    106993205379072,
    1283918464548864,
    15407021574586368,
    184884258895036416,
    2218611106740436992,
];
const _: () = assert!(SMALL_INT_POW12.len() > f64_mantissa_limit(12) as usize);
const _: () = assert!(SMALL_INT_POW12.len() == u64_power_limit(12) as usize + 1);

/// Pre-computed, small powers-of-12.
pub const SMALL_F32_POW12: [f32; 16] = [
    1.0,
    12.0,
    144.0,
    1728.0,
    20736.0,
    248832.0,
    2985984.0,
    35831808.0,
    429981696.0,
    5159780352.0,
    61917364224.0,
    743008370688.0,
    8916100448256.0,
    106993205379072.0,
    1283918464548864.0,
    1.5407021574586368e+16,
];
const _: () = assert!(SMALL_F32_POW12.len() > f32_exponent_limit(12).1 as usize);

/// Pre-computed, small powers-of-12.
pub const SMALL_F64_POW12: [f64; 34] = [
    1.0,
    12.0,
    144.0,
    1728.0,
    20736.0,
    248832.0,
    2985984.0,
    35831808.0,
    429981696.0,
    5159780352.0,
    61917364224.0,
    743008370688.0,
    8916100448256.0,
    106993205379072.0,
    1283918464548864.0,
    1.5407021574586368e+16,
    1.848842588950364e+17,
    2.218611106740437e+18,
    2.6623333280885244e+19,
    3.194799993706229e+20,
    3.833759992447475e+21,
    4.60051199093697e+22,
    5.520614389124364e+23,
    6.624737266949237e+24,
    7.949684720339084e+25,
    9.539621664406901e+26,
    1.1447545997288282e+28,
    1.3737055196745938e+29,
    1.6484466236095125e+30,
    1.978135948331415e+31,
    2.373763137997698e+32,
    2.8485157655972377e+33,
    3.418218918716685e+34,
    4.101862702460022e+35,
];
const _: () = assert!(SMALL_F64_POW12.len() > f64_exponent_limit(12).1 as usize);

/// Pre-computed, small powers-of-13.
pub const SMALL_INT_POW13: [u64; 18] = [
    1,
    13,
    169,
    2197,
    28561,
    371293,
    4826809,
    62748517,
    815730721,
    10604499373,
    137858491849,
    1792160394037,
    23298085122481,
    302875106592253,
    3937376385699289,
    51185893014090757,
    665416609183179841,
    8650415919381337933,
];
const _: () = assert!(SMALL_INT_POW13.len() > f64_mantissa_limit(13) as usize);
const _: () = assert!(SMALL_INT_POW13.len() == u64_power_limit(13) as usize + 1);

/// Pre-computed, small powers-of-13.
pub const SMALL_F32_POW13: [f32; 7] = [1.0, 13.0, 169.0, 2197.0, 28561.0, 371293.0, 4826809.0];
const _: () = assert!(SMALL_F32_POW13.len() > f32_exponent_limit(13).1 as usize);

/// Pre-computed, small powers-of-13.
pub const SMALL_F64_POW13: [f64; 15] = [
    1.0,
    13.0,
    169.0,
    2197.0,
    28561.0,
    371293.0,
    4826809.0,
    62748517.0,
    815730721.0,
    10604499373.0,
    137858491849.0,
    1792160394037.0,
    23298085122481.0,
    302875106592253.0,
    3937376385699289.0,
];
const _: () = assert!(SMALL_F64_POW13.len() > f64_exponent_limit(13).1 as usize);

/// Pre-computed large power-of-13 for 32-bit limbs.
#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub const LARGE_POW13: [u32; 10] = [
    3146523293, 4222426932, 2977536293, 1295813598, 1909522258, 1606005718, 3366933208, 327990755,
    3779976816, 97397137,
];

/// Pre-computed large power-of-13 for 64-bit limbs.
#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub const LARGE_POW13: [u64; 5] = [
    18135185585836139165,
    5565477028099627301,
    6897742037908520786,
    1408709569482281688,
    418317521919008368,
];

/// Step for large power-of-13 for 32-bit limbs.
pub const LARGE_POW13_STEP: u32 = 85;

/// Pre-computed, small powers-of-14.
pub const SMALL_INT_POW14: [u64; 17] = [
    1,
    14,
    196,
    2744,
    38416,
    537824,
    7529536,
    105413504,
    1475789056,
    20661046784,
    289254654976,
    4049565169664,
    56693912375296,
    793714773254144,
    11112006825558016,
    155568095557812224,
    2177953337809371136,
];
const _: () = assert!(SMALL_INT_POW14.len() > f64_mantissa_limit(14) as usize);
const _: () = assert!(SMALL_INT_POW14.len() == u64_power_limit(14) as usize + 1);

/// Pre-computed, small powers-of-14.
pub const SMALL_F32_POW14: [f32; 9] =
    [1.0, 14.0, 196.0, 2744.0, 38416.0, 537824.0, 7529536.0, 105413504.0, 1475789056.0];
const _: () = assert!(SMALL_F32_POW14.len() > f32_exponent_limit(14).1 as usize);

/// Pre-computed, small powers-of-14.
pub const SMALL_F64_POW14: [f64; 19] = [
    1.0,
    14.0,
    196.0,
    2744.0,
    38416.0,
    537824.0,
    7529536.0,
    105413504.0,
    1475789056.0,
    20661046784.0,
    289254654976.0,
    4049565169664.0,
    56693912375296.0,
    793714773254144.0,
    1.1112006825558016e+16,
    1.5556809555781222e+17,
    2.1779533378093711e+18,
    3.0491346729331196e+19,
    4.2687885421063674e+20,
];
const _: () = assert!(SMALL_F64_POW14.len() > f64_exponent_limit(14).1 as usize);

/// Pre-computed, small powers-of-15.
pub const SMALL_INT_POW15: [u64; 17] = [
    1,
    15,
    225,
    3375,
    50625,
    759375,
    11390625,
    170859375,
    2562890625,
    38443359375,
    576650390625,
    8649755859375,
    129746337890625,
    1946195068359375,
    29192926025390625,
    437893890380859375,
    6568408355712890625,
];
const _: () = assert!(SMALL_INT_POW15.len() > f64_mantissa_limit(15) as usize);
const _: () = assert!(SMALL_INT_POW15.len() == u64_power_limit(15) as usize + 1);

/// Pre-computed, small powers-of-15.
pub const SMALL_F32_POW15: [f32; 7] = [1.0, 15.0, 225.0, 3375.0, 50625.0, 759375.0, 11390625.0];
const _: () = assert!(SMALL_F32_POW15.len() > f32_exponent_limit(15).1 as usize);

/// Pre-computed, small powers-of-15.
pub const SMALL_F64_POW15: [f64; 14] = [
    1.0,
    15.0,
    225.0,
    3375.0,
    50625.0,
    759375.0,
    11390625.0,
    170859375.0,
    2562890625.0,
    38443359375.0,
    576650390625.0,
    8649755859375.0,
    129746337890625.0,
    1946195068359375.0,
];
const _: () = assert!(SMALL_F64_POW15.len() > f64_exponent_limit(15).1 as usize);

/// Pre-computed large power-of-15 for 32-bit limbs.
#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub const LARGE_POW15: [u32; 10] = [
    3507049217, 2300028134, 3886839708, 4190270956, 1622122702, 1947334599, 204338878, 3105278257,
    2490561006, 24584533,
];

/// Pre-computed large power-of-15 for 64-bit limbs.
#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub const LARGE_POW15: [u64; 5] = [
    9878545618916954881,
    17997076721285494684,
    8363738418696397006,
    13337068558999221950,
    105589767712993774,
];

/// Step for large power-of-15 for 32-bit limbs.
pub const LARGE_POW15_STEP: u32 = 80;

/// Pre-computed, small powers-of-17.
pub const SMALL_INT_POW17: [u64; 16] = [
    1,
    17,
    289,
    4913,
    83521,
    1419857,
    24137569,
    410338673,
    6975757441,
    118587876497,
    2015993900449,
    34271896307633,
    582622237229761,
    9904578032905937,
    168377826559400929,
    2862423051509815793,
];
const _: () = assert!(SMALL_INT_POW17.len() > f64_mantissa_limit(17) as usize);
const _: () = assert!(SMALL_INT_POW17.len() == u64_power_limit(17) as usize + 1);

/// Pre-computed, small powers-of-17.
pub const SMALL_F32_POW17: [f32; 6] = [1.0, 17.0, 289.0, 4913.0, 83521.0, 1419857.0];
const _: () = assert!(SMALL_F32_POW17.len() > f32_exponent_limit(17).1 as usize);

/// Pre-computed, small powers-of-17.
pub const SMALL_F64_POW17: [f64; 13] = [
    1.0,
    17.0,
    289.0,
    4913.0,
    83521.0,
    1419857.0,
    24137569.0,
    410338673.0,
    6975757441.0,
    118587876497.0,
    2015993900449.0,
    34271896307633.0,
    582622237229761.0,
];
const _: () = assert!(SMALL_F64_POW17.len() > f64_exponent_limit(17).1 as usize);

/// Pre-computed large power-of-17 for 32-bit limbs.
#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub const LARGE_POW17: [u32; 10] = [
    2990615473, 2810986799, 4066186761, 2554374905, 4073187723, 2831536001, 529177471, 3891721527,
    4211495815, 386393,
];

/// Pre-computed large power-of-17 for 64-bit limbs.
#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub const LARGE_POW17: [u64; 5] = [
    12073096374183340977,
    10970956682764293641,
    12161354525814811019,
    16714816684133358463,
    1659549509899143,
];

/// Step for large power-of-17 for 32-bit limbs.
pub const LARGE_POW17_STEP: u32 = 75;

/// Pre-computed, small powers-of-18.
pub const SMALL_INT_POW18: [u64; 16] = [
    1,
    18,
    324,
    5832,
    104976,
    1889568,
    34012224,
    612220032,
    11019960576,
    198359290368,
    3570467226624,
    64268410079232,
    1156831381426176,
    20822964865671168,
    374813367582081024,
    6746640616477458432,
];
const _: () = assert!(SMALL_INT_POW18.len() > f64_mantissa_limit(18) as usize);
const _: () = assert!(SMALL_INT_POW18.len() == u64_power_limit(18) as usize + 1);

/// Pre-computed, small powers-of-18.
pub const SMALL_F32_POW18: [f32; 8] =
    [1.0, 18.0, 324.0, 5832.0, 104976.0, 1889568.0, 34012224.0, 612220032.0];
const _: () = assert!(SMALL_F32_POW18.len() > f32_exponent_limit(18).1 as usize);

/// Pre-computed, small powers-of-18.
pub const SMALL_F64_POW18: [f64; 17] = [
    1.0,
    18.0,
    324.0,
    5832.0,
    104976.0,
    1889568.0,
    34012224.0,
    612220032.0,
    11019960576.0,
    198359290368.0,
    3570467226624.0,
    64268410079232.0,
    1156831381426176.0,
    2.082296486567117e+16,
    3.74813367582081e+17,
    6.746640616477458e+18,
    1.2143953109659425e+20,
];
const _: () = assert!(SMALL_F64_POW18.len() > f64_exponent_limit(18).1 as usize);

/// Pre-computed, small powers-of-19.
pub const SMALL_INT_POW19: [u64; 16] = [
    1,
    19,
    361,
    6859,
    130321,
    2476099,
    47045881,
    893871739,
    16983563041,
    322687697779,
    6131066257801,
    116490258898219,
    2213314919066161,
    42052983462257059,
    799006685782884121,
    15181127029874798299,
];
const _: () = assert!(SMALL_INT_POW19.len() > f64_mantissa_limit(19) as usize);
const _: () = assert!(SMALL_INT_POW19.len() == u64_power_limit(19) as usize + 1);

/// Pre-computed, small powers-of-19.
pub const SMALL_F32_POW19: [f32; 6] = [1.0, 19.0, 361.0, 6859.0, 130321.0, 2476099.0];
const _: () = assert!(SMALL_F32_POW19.len() > f32_exponent_limit(19).1 as usize);

/// Pre-computed, small powers-of-19.
pub const SMALL_F64_POW19: [f64; 13] = [
    1.0,
    19.0,
    361.0,
    6859.0,
    130321.0,
    2476099.0,
    47045881.0,
    893871739.0,
    16983563041.0,
    322687697779.0,
    6131066257801.0,
    116490258898219.0,
    2213314919066161.0,
];
const _: () = assert!(SMALL_F64_POW19.len() > f64_exponent_limit(19).1 as usize);

/// Pre-computed large power-of-19 for 32-bit limbs.
#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub const LARGE_POW19: [u32; 10] = [
    844079147, 4109067463, 2265902219, 1405351247, 3107957240, 2205473157, 271286156, 2969717342,
    1924040718, 1621366965,
];

/// Pre-computed large power-of-19 for 64-bit limbs.
#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub const LARGE_POW19: [u64; 5] = [
    17648310371486769195,
    6035937647523720331,
    9472435084628830712,
    12754838862525333388,
    6963718091413817358,
];

/// Step for large power-of-19 for 32-bit limbs.
pub const LARGE_POW19_STEP: u32 = 75;

/// Pre-computed, small powers-of-20.
pub const SMALL_INT_POW20: [u64; 15] = [
    1,
    20,
    400,
    8000,
    160000,
    3200000,
    64000000,
    1280000000,
    25600000000,
    512000000000,
    10240000000000,
    204800000000000,
    4096000000000000,
    81920000000000000,
    1638400000000000000,
];
const _: () = assert!(SMALL_INT_POW20.len() > f64_mantissa_limit(20) as usize);
const _: () = assert!(SMALL_INT_POW20.len() == u64_power_limit(20) as usize + 1);

/// Pre-computed, small powers-of-20.
pub const SMALL_F32_POW20: [f32; 11] = [
    1.0,
    20.0,
    400.0,
    8000.0,
    160000.0,
    3200000.0,
    64000000.0,
    1280000000.0,
    25600000000.0,
    512000000000.0,
    10240000000000.0,
];
const _: () = assert!(SMALL_F32_POW20.len() > f32_exponent_limit(20).1 as usize);

/// Pre-computed, small powers-of-20.
pub const SMALL_F64_POW20: [f64; 23] = [
    1.0,
    20.0,
    400.0,
    8000.0,
    160000.0,
    3200000.0,
    64000000.0,
    1280000000.0,
    25600000000.0,
    512000000000.0,
    10240000000000.0,
    204800000000000.0,
    4096000000000000.0,
    8.192e+16,
    1.6384e+18,
    3.2768e+19,
    6.5536e+20,
    1.31072e+22,
    2.62144e+23,
    5.24288e+24,
    1.048576e+26,
    2.097152e+27,
    4.194304e+28,
];
const _: () = assert!(SMALL_F64_POW20.len() > f64_exponent_limit(20).1 as usize);

/// Pre-computed, small powers-of-21.
pub const SMALL_INT_POW21: [u64; 15] = [
    1,
    21,
    441,
    9261,
    194481,
    4084101,
    85766121,
    1801088541,
    37822859361,
    794280046581,
    16679880978201,
    350277500542221,
    7355827511386641,
    154472377739119461,
    3243919932521508681,
];
const _: () = assert!(SMALL_INT_POW21.len() > f64_mantissa_limit(21) as usize);
const _: () = assert!(SMALL_INT_POW21.len() == u64_power_limit(21) as usize + 1);

/// Pre-computed, small powers-of-21.
pub const SMALL_F32_POW21: [f32; 6] = [1.0, 21.0, 441.0, 9261.0, 194481.0, 4084101.0];
const _: () = assert!(SMALL_F32_POW21.len() > f32_exponent_limit(21).1 as usize);

/// Pre-computed, small powers-of-21.
pub const SMALL_F64_POW21: [f64; 13] = [
    1.0,
    21.0,
    441.0,
    9261.0,
    194481.0,
    4084101.0,
    85766121.0,
    1801088541.0,
    37822859361.0,
    794280046581.0,
    16679880978201.0,
    350277500542221.0,
    7355827511386641.0,
];
const _: () = assert!(SMALL_F64_POW21.len() > f64_exponent_limit(21).1 as usize);

/// Pre-computed large power-of-21 for 32-bit limbs.
#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub const LARGE_POW21: [u32; 10] = [
    138418921, 1265804130, 2218244279, 959999061, 1977606600, 816701562, 1115590038, 3476226057,
    1985711423, 722290,
];

/// Pre-computed large power-of-21 for 64-bit limbs.
#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub const LARGE_POW21: [u64; 5] = [
    5436587341630151401,
    4123164573403953335,
    3507706501359722952,
    14930277229433621910,
    3102213913939263,
];

/// Step for large power-of-21 for 32-bit limbs.
pub const LARGE_POW21_STEP: u32 = 70;

/// Pre-computed, small powers-of-22.
pub const SMALL_INT_POW22: [u64; 15] = [
    1,
    22,
    484,
    10648,
    234256,
    5153632,
    113379904,
    2494357888,
    54875873536,
    1207269217792,
    26559922791424,
    584318301411328,
    12855002631049216,
    282810057883082752,
    6221821273427820544,
];
const _: () = assert!(SMALL_INT_POW22.len() > f64_mantissa_limit(22) as usize);
const _: () = assert!(SMALL_INT_POW22.len() == u64_power_limit(22) as usize + 1);

/// Pre-computed, small powers-of-22.
pub const SMALL_F32_POW22: [f32; 7] = [1.0, 22.0, 484.0, 10648.0, 234256.0, 5153632.0, 113379904.0];
const _: () = assert!(SMALL_F32_POW22.len() > f32_exponent_limit(22).1 as usize);

/// Pre-computed, small powers-of-22.
pub const SMALL_F64_POW22: [f64; 16] = [
    1.0,
    22.0,
    484.0,
    10648.0,
    234256.0,
    5153632.0,
    113379904.0,
    2494357888.0,
    54875873536.0,
    1207269217792.0,
    26559922791424.0,
    584318301411328.0,
    1.2855002631049216e+16,
    2.8281005788308275e+17,
    6.221821273427821e+18,
    1.3688006801541205e+20,
];
const _: () = assert!(SMALL_F64_POW22.len() > f64_exponent_limit(22).1 as usize);

/// Pre-computed, small powers-of-23.
pub const SMALL_INT_POW23: [u64; 15] = [
    1,
    23,
    529,
    12167,
    279841,
    6436343,
    148035889,
    3404825447,
    78310985281,
    1801152661463,
    41426511213649,
    952809757913927,
    21914624432020321,
    504036361936467383,
    11592836324538749809,
];
const _: () = assert!(SMALL_INT_POW23.len() > f64_mantissa_limit(23) as usize);
const _: () = assert!(SMALL_INT_POW23.len() == u64_power_limit(23) as usize + 1);

/// Pre-computed, small powers-of-23.
pub const SMALL_F32_POW23: [f32; 6] = [1.0, 23.0, 529.0, 12167.0, 279841.0, 6436343.0];
const _: () = assert!(SMALL_F32_POW23.len() > f32_exponent_limit(23).1 as usize);

/// Pre-computed, small powers-of-23.
pub const SMALL_F64_POW23: [f64; 12] = [
    1.0,
    23.0,
    529.0,
    12167.0,
    279841.0,
    6436343.0,
    148035889.0,
    3404825447.0,
    78310985281.0,
    1801152661463.0,
    41426511213649.0,
    952809757913927.0,
];
const _: () = assert!(SMALL_F64_POW23.len() > f64_exponent_limit(23).1 as usize);

/// Pre-computed large power-of-23 for 32-bit limbs.
#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub const LARGE_POW23: [u32; 10] = [
    1403677489, 2801905613, 3028338484, 1469351396, 2741227823, 193620048, 1084942677, 2905110101,
    3742230796, 421026827,
];

/// Pre-computed large power-of-23 for 64-bit limbs.
#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub const LARGE_POW23: [u64; 5] = [
    12034092975717509937,
    6310816195180283700,
    831591776751178031,
    12477352876159199573,
    1808296456445880588,
];

/// Step for large power-of-23 for 32-bit limbs.
pub const LARGE_POW23_STEP: u32 = 70;

/// Pre-computed, small powers-of-24.
pub const SMALL_INT_POW24: [u64; 14] = [
    1,
    24,
    576,
    13824,
    331776,
    7962624,
    191102976,
    4586471424,
    110075314176,
    2641807540224,
    63403380965376,
    1521681143169024,
    36520347436056576,
    876488338465357824,
];
const _: () = assert!(SMALL_INT_POW24.len() > f64_mantissa_limit(24) as usize);
const _: () = assert!(SMALL_INT_POW24.len() == u64_power_limit(24) as usize + 1);

/// Pre-computed, small powers-of-24.
pub const SMALL_F32_POW24: [f32; 16] = [
    1.0,
    24.0,
    576.0,
    13824.0,
    331776.0,
    7962624.0,
    191102976.0,
    4586471424.0,
    110075314176.0,
    2641807540224.0,
    63403380965376.0,
    1521681143169024.0,
    3.652034743605658e+16,
    8.764883384653578e+17,
    2.1035720123168588e+19,
    5.048572829560461e+20,
];
const _: () = assert!(SMALL_F32_POW24.len() > f32_exponent_limit(24).1 as usize);

/// Pre-computed, small powers-of-24.
pub const SMALL_F64_POW24: [f64; 34] = [
    1.0,
    24.0,
    576.0,
    13824.0,
    331776.0,
    7962624.0,
    191102976.0,
    4586471424.0,
    110075314176.0,
    2641807540224.0,
    63403380965376.0,
    1521681143169024.0,
    3.652034743605658e+16,
    8.764883384653578e+17,
    2.1035720123168588e+19,
    5.048572829560461e+20,
    1.2116574790945107e+22,
    2.9079779498268256e+23,
    6.979147079584381e+24,
    1.6749952991002515e+26,
    4.0199887178406037e+27,
    9.647972922817449e+28,
    2.3155135014761877e+30,
    5.5572324035428505e+31,
    1.333735776850284e+33,
    3.200965864440682e+34,
    7.682318074657637e+35,
    1.8437563379178328e+37,
    4.425015211002799e+38,
    1.0620036506406717e+40,
    2.548808761537612e+41,
    6.117141027690269e+42,
    1.4681138466456645e+44,
    3.523473231949595e+45,
];
const _: () = assert!(SMALL_F64_POW24.len() > f64_exponent_limit(24).1 as usize);

/// Pre-computed, small powers-of-25.
pub const SMALL_INT_POW25: [u64; 14] = [
    1,
    25,
    625,
    15625,
    390625,
    9765625,
    244140625,
    6103515625,
    152587890625,
    3814697265625,
    95367431640625,
    2384185791015625,
    59604644775390625,
    1490116119384765625,
];
const _: () = assert!(SMALL_INT_POW25.len() > f64_mantissa_limit(25) as usize);
const _: () = assert!(SMALL_INT_POW25.len() == u64_power_limit(25) as usize + 1);

/// Pre-computed, small powers-of-25.
pub const SMALL_F32_POW25: [f32; 6] = [1.0, 25.0, 625.0, 15625.0, 390625.0, 9765625.0];
const _: () = assert!(SMALL_F32_POW25.len() > f32_exponent_limit(25).1 as usize);

/// Pre-computed, small powers-of-25.
pub const SMALL_F64_POW25: [f64; 12] = [
    1.0,
    25.0,
    625.0,
    15625.0,
    390625.0,
    9765625.0,
    244140625.0,
    6103515625.0,
    152587890625.0,
    3814697265625.0,
    95367431640625.0,
    2384185791015625.0,
];
const _: () = assert!(SMALL_F64_POW25.len() > f64_exponent_limit(25).1 as usize);

/// Pre-computed large power-of-25 for 32-bit limbs.
#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub const LARGE_POW25: [u32; 10] = [
    2358447641, 1624633829, 2031259829, 1986676888, 2941191183, 611941596, 1880507741, 990341507,
    3289036379, 14772,
];

/// Pre-computed large power-of-25 for 64-bit limbs.
#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub const LARGE_POW25: [u64; 5] = [
    6977749165888704025,
    8532712263710314677,
    2628269144823235599,
    4253484386316862813,
    63448545932891,
];

/// Step for large power-of-25 for 32-bit limbs.
pub const LARGE_POW25_STEP: u32 = 65;

/// Pre-computed, small powers-of-26.
pub const SMALL_INT_POW26: [u64; 14] = [
    1,
    26,
    676,
    17576,
    456976,
    11881376,
    308915776,
    8031810176,
    208827064576,
    5429503678976,
    141167095653376,
    3670344486987776,
    95428956661682176,
    2481152873203736576,
];
const _: () = assert!(SMALL_INT_POW26.len() > f64_mantissa_limit(26) as usize);
const _: () = assert!(SMALL_INT_POW26.len() == u64_power_limit(26) as usize + 1);

/// Pre-computed, small powers-of-26.
pub const SMALL_F32_POW26: [f32; 7] =
    [1.0, 26.0, 676.0, 17576.0, 456976.0, 11881376.0, 308915776.0];
const _: () = assert!(SMALL_F32_POW26.len() > f32_exponent_limit(26).1 as usize);

/// Pre-computed, small powers-of-26.
pub const SMALL_F64_POW26: [f64; 15] = [
    1.0,
    26.0,
    676.0,
    17576.0,
    456976.0,
    11881376.0,
    308915776.0,
    8031810176.0,
    208827064576.0,
    5429503678976.0,
    141167095653376.0,
    3670344486987776.0,
    9.542895666168218e+16,
    2.4811528732037366e+18,
    6.450997470329715e+19,
];
const _: () = assert!(SMALL_F64_POW26.len() > f64_exponent_limit(26).1 as usize);

/// Pre-computed, small powers-of-27.
pub const SMALL_INT_POW27: [u64; 14] = [
    1,
    27,
    729,
    19683,
    531441,
    14348907,
    387420489,
    10460353203,
    282429536481,
    7625597484987,
    205891132094649,
    5559060566555523,
    150094635296999121,
    4052555153018976267,
];
const _: () = assert!(SMALL_INT_POW27.len() > f64_mantissa_limit(27) as usize);
const _: () = assert!(SMALL_INT_POW27.len() == u64_power_limit(27) as usize + 1);

/// Pre-computed, small powers-of-27.
pub const SMALL_F32_POW27: [f32; 6] = [1.0, 27.0, 729.0, 19683.0, 531441.0, 14348907.0];
const _: () = assert!(SMALL_F32_POW27.len() > f32_exponent_limit(27).1 as usize);

/// Pre-computed, small powers-of-27.
pub const SMALL_F64_POW27: [f64; 12] = [
    1.0,
    27.0,
    729.0,
    19683.0,
    531441.0,
    14348907.0,
    387420489.0,
    10460353203.0,
    282429536481.0,
    7625597484987.0,
    205891132094649.0,
    5559060566555523.0,
];
const _: () = assert!(SMALL_F64_POW27.len() > f64_exponent_limit(27).1 as usize);

/// Pre-computed large power-of-27 for 32-bit limbs.
#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub const LARGE_POW27: [u32; 10] = [
    1249037595, 465894344, 2861423576, 2518924695, 4122946360, 4029669975, 3949684612, 3795800505,
    3556955416, 2197889,
];

/// Pre-computed large power-of-27 for 64-bit limbs.
#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub const LARGE_POW27: [u64; 5] = [
    2001000972120411419,
    10818699188973198296,
    17307300760421083960,
    16302839035064969092,
    9439864932193560,
];

/// Step for large power-of-27 for 32-bit limbs.
pub const LARGE_POW27_STEP: u32 = 65;

/// Pre-computed, small powers-of-28.
pub const SMALL_INT_POW28: [u64; 14] = [
    1,
    28,
    784,
    21952,
    614656,
    17210368,
    481890304,
    13492928512,
    377801998336,
    10578455953408,
    296196766695424,
    8293509467471872,
    232218265089212416,
    6502111422497947648,
];
const _: () = assert!(SMALL_INT_POW28.len() > f64_mantissa_limit(28) as usize);
const _: () = assert!(SMALL_INT_POW28.len() == u64_power_limit(28) as usize + 1);

/// Pre-computed, small powers-of-28.
pub const SMALL_F32_POW28: [f32; 9] =
    [1.0, 28.0, 784.0, 21952.0, 614656.0, 17210368.0, 481890304.0, 13492928512.0, 377801998336.0];
const _: () = assert!(SMALL_F32_POW28.len() > f32_exponent_limit(28).1 as usize);

/// Pre-computed, small powers-of-28.
pub const SMALL_F64_POW28: [f64; 19] = [
    1.0,
    28.0,
    784.0,
    21952.0,
    614656.0,
    17210368.0,
    481890304.0,
    13492928512.0,
    377801998336.0,
    10578455953408.0,
    296196766695424.0,
    8293509467471872.0,
    2.322182650892124e+17,
    6.502111422497948e+18,
    1.8205911982994253e+20,
    5.097655355238391e+21,
    1.4273434994667495e+23,
    3.9965617985068985e+24,
    1.1190373035819316e+26,
];
const _: () = assert!(SMALL_F64_POW28.len() > f64_exponent_limit(28).1 as usize);

/// Pre-computed, small powers-of-29.
pub const SMALL_INT_POW29: [u64; 14] = [
    1,
    29,
    841,
    24389,
    707281,
    20511149,
    594823321,
    17249876309,
    500246412961,
    14507145975869,
    420707233300201,
    12200509765705829,
    353814783205469041,
    10260628712958602189,
];
const _: () = assert!(SMALL_INT_POW29.len() > f64_mantissa_limit(29) as usize);
const _: () = assert!(SMALL_INT_POW29.len() == u64_power_limit(29) as usize + 1);

/// Pre-computed, small powers-of-29.
pub const SMALL_F32_POW29: [f32; 5] = [1.0, 29.0, 841.0, 24389.0, 707281.0];
const _: () = assert!(SMALL_F32_POW29.len() > f32_exponent_limit(29).1 as usize);

/// Pre-computed, small powers-of-29.
pub const SMALL_F64_POW29: [f64; 11] = [
    1.0,
    29.0,
    841.0,
    24389.0,
    707281.0,
    20511149.0,
    594823321.0,
    17249876309.0,
    500246412961.0,
    14507145975869.0,
    420707233300201.0,
];
const _: () = assert!(SMALL_F64_POW29.len() > f64_exponent_limit(29).1 as usize);

/// Pre-computed large power-of-29 for 32-bit limbs.
#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub const LARGE_POW29: [u32; 10] = [
    3437097245, 219578399, 3191687836, 3061529344, 4005823358, 3201416410, 694756510, 1988053185,
    463784885, 228681542,
];

/// Pre-computed large power-of-29 for 64-bit limbs.
#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub const LARGE_POW29: [u64; 5] = [
    943082046050136349,
    13149168411416021660,
    13749978785833550718,
    8538623412978394270,
    982179744552635317,
];

/// Step for large power-of-29 for 32-bit limbs.
pub const LARGE_POW29_STEP: u32 = 65;

/// Pre-computed, small powers-of-30.
pub const SMALL_INT_POW30: [u64; 14] = [
    1,
    30,
    900,
    27000,
    810000,
    24300000,
    729000000,
    21870000000,
    656100000000,
    19683000000000,
    590490000000000,
    17714700000000000,
    531441000000000000,
    15943230000000000000,
];
const _: () = assert!(SMALL_INT_POW30.len() > f64_mantissa_limit(30) as usize);
const _: () = assert!(SMALL_INT_POW30.len() == u64_power_limit(30) as usize + 1);

/// Pre-computed, small powers-of-30.
pub const SMALL_F32_POW30: [f32; 7] =
    [1.0, 30.0, 900.0, 27000.0, 810000.0, 24300000.0, 729000000.0];
const _: () = assert!(SMALL_F32_POW30.len() > f32_exponent_limit(30).1 as usize);

/// Pre-computed, small powers-of-30.
pub const SMALL_F64_POW30: [f64; 14] = [
    1.0,
    30.0,
    900.0,
    27000.0,
    810000.0,
    24300000.0,
    729000000.0,
    21870000000.0,
    656100000000.0,
    19683000000000.0,
    590490000000000.0,
    1.77147e+16,
    5.31441e+17,
    1.594323e+19,
];
const _: () = assert!(SMALL_F64_POW30.len() > f64_exponent_limit(30).1 as usize);

/// Pre-computed, small powers-of-31.
pub const SMALL_INT_POW31: [u64; 13] = [
    1,
    31,
    961,
    29791,
    923521,
    28629151,
    887503681,
    27512614111,
    852891037441,
    26439622160671,
    819628286980801,
    25408476896404831,
    787662783788549761,
];
const _: () = assert!(SMALL_INT_POW31.len() > f64_mantissa_limit(31) as usize);
const _: () = assert!(SMALL_INT_POW31.len() == u64_power_limit(31) as usize + 1);

/// Pre-computed, small powers-of-31.
pub const SMALL_F32_POW31: [f32; 5] = [1.0, 31.0, 961.0, 29791.0, 923521.0];
const _: () = assert!(SMALL_F32_POW31.len() > f32_exponent_limit(31).1 as usize);

/// Pre-computed, small powers-of-31.
pub const SMALL_F64_POW31: [f64; 11] = [
    1.0,
    31.0,
    961.0,
    29791.0,
    923521.0,
    28629151.0,
    887503681.0,
    27512614111.0,
    852891037441.0,
    26439622160671.0,
    819628286980801.0,
];
const _: () = assert!(SMALL_F64_POW31.len() > f64_exponent_limit(31).1 as usize);

/// Pre-computed large power-of-31 for 32-bit limbs.
#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub const LARGE_POW31: [u32; 10] = [
    3128270977, 627186439, 3737223222, 1519964902, 4275419645, 1305227997, 3310009113, 99290790,
    2685019127, 609,
];

/// Pre-computed large power-of-31 for 64-bit limbs.
#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub const LARGE_POW31: [u64; 5] = [
    2693745247127969921,
    6528199548895068214,
    5605911565214005757,
    426450699154012953,
    2618320102391,
];

/// Step for large power-of-31 for 32-bit limbs.
pub const LARGE_POW31_STEP: u32 = 60;

/// Pre-computed, small powers-of-33.
pub const SMALL_INT_POW33: [u64; 13] = [
    1,
    33,
    1089,
    35937,
    1185921,
    39135393,
    1291467969,
    42618442977,
    1406408618241,
    46411484401953,
    1531578985264449,
    50542106513726817,
    1667889514952984961,
];
const _: () = assert!(SMALL_INT_POW33.len() > f64_mantissa_limit(33) as usize);
const _: () = assert!(SMALL_INT_POW33.len() == u64_power_limit(33) as usize + 1);

/// Pre-computed, small powers-of-33.
pub const SMALL_F32_POW33: [f32; 5] = [1.0, 33.0, 1089.0, 35937.0, 1185921.0];
const _: () = assert!(SMALL_F32_POW33.len() > f32_exponent_limit(33).1 as usize);

/// Pre-computed, small powers-of-33.
pub const SMALL_F64_POW33: [f64; 11] = [
    1.0,
    33.0,
    1089.0,
    35937.0,
    1185921.0,
    39135393.0,
    1291467969.0,
    42618442977.0,
    1406408618241.0,
    46411484401953.0,
    1531578985264449.0,
];
const _: () = assert!(SMALL_F64_POW33.len() > f64_exponent_limit(33).1 as usize);

/// Pre-computed large power-of-33 for 32-bit limbs.
#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub const LARGE_POW33: [u32; 10] = [
    1612820353, 1081423072, 127566253, 3291061608, 3338225311, 2497994496, 2486573331, 4032720849,
    2585834285, 25953,
];

/// Pre-computed large power-of-33 for 64-bit limbs.
#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub const LARGE_POW33: [u64; 5] = [
    4644676728992673665,
    14135001975608738221,
    10728804669246228127,
    17320404162838927635,
    111469872067373,
];

/// Step for large power-of-33 for 32-bit limbs.
pub const LARGE_POW33_STEP: u32 = 60;

/// Pre-computed, small powers-of-34.
pub const SMALL_INT_POW34: [u64; 13] = [
    1,
    34,
    1156,
    39304,
    1336336,
    45435424,
    1544804416,
    52523350144,
    1785793904896,
    60716992766464,
    2064377754059776,
    70188843638032384,
    2386420683693101056,
];
const _: () = assert!(SMALL_INT_POW34.len() > f64_mantissa_limit(34) as usize);
const _: () = assert!(SMALL_INT_POW34.len() == u64_power_limit(34) as usize + 1);

/// Pre-computed, small powers-of-34.
pub const SMALL_F32_POW34: [f32; 6] = [1.0, 34.0, 1156.0, 39304.0, 1336336.0, 45435424.0];
const _: () = assert!(SMALL_F32_POW34.len() > f32_exponent_limit(34).1 as usize);

/// Pre-computed, small powers-of-34.
pub const SMALL_F64_POW34: [f64; 13] = [
    1.0,
    34.0,
    1156.0,
    39304.0,
    1336336.0,
    45435424.0,
    1544804416.0,
    52523350144.0,
    1785793904896.0,
    60716992766464.0,
    2064377754059776.0,
    7.018884363803238e+16,
    2.386420683693101e+18,
];
const _: () = assert!(SMALL_F64_POW34.len() > f64_exponent_limit(34).1 as usize);

/// Pre-computed, small powers-of-35.
pub const SMALL_INT_POW35: [u64; 13] = [
    1,
    35,
    1225,
    42875,
    1500625,
    52521875,
    1838265625,
    64339296875,
    2251875390625,
    78815638671875,
    2758547353515625,
    96549157373046875,
    3379220508056640625,
];
const _: () = assert!(SMALL_INT_POW35.len() > f64_mantissa_limit(35) as usize);
const _: () = assert!(SMALL_INT_POW35.len() == u64_power_limit(35) as usize + 1);

/// Pre-computed, small powers-of-35.
pub const SMALL_F32_POW35: [f32; 5] = [1.0, 35.0, 1225.0, 42875.0, 1500625.0];
const _: () = assert!(SMALL_F32_POW35.len() > f32_exponent_limit(35).1 as usize);

/// Pre-computed, small powers-of-35.
pub const SMALL_F64_POW35: [f64; 11] = [
    1.0,
    35.0,
    1225.0,
    42875.0,
    1500625.0,
    52521875.0,
    1838265625.0,
    64339296875.0,
    2251875390625.0,
    78815638671875.0,
    2758547353515625.0,
];
const _: () = assert!(SMALL_F64_POW35.len() > f64_exponent_limit(35).1 as usize);

/// Pre-computed large power-of-35 for 32-bit limbs.
#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub const LARGE_POW35: [u32; 10] = [
    2481068081, 3589182317, 2073348182, 2214889340, 548239849, 1614245998, 4081052795, 291764764,
    3369344364, 886020,
];

/// Pre-computed large power-of-35 for 64-bit limbs.
#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub const LARGE_POW35: [u64; 5] = [
    15415420673377572913,
    9512877281632372822,
    6933133769657121257,
    1253120123586210939,
    3805430292946284,
];

/// Step for large power-of-35 for 32-bit limbs.
pub const LARGE_POW35_STEP: u32 = 60;

/// Pre-computed, small powers-of-36.
pub const SMALL_INT_POW36: [u64; 13] = [
    1,
    36,
    1296,
    46656,
    1679616,
    60466176,
    2176782336,
    78364164096,
    2821109907456,
    101559956668416,
    3656158440062976,
    131621703842267136,
    4738381338321616896,
];
const _: () = assert!(SMALL_INT_POW36.len() > f64_mantissa_limit(36) as usize);
const _: () = assert!(SMALL_INT_POW36.len() == u64_power_limit(36) as usize + 1);

/// Pre-computed, small powers-of-36.
pub const SMALL_F32_POW36: [f32; 8] =
    [1.0, 36.0, 1296.0, 46656.0, 1679616.0, 60466176.0, 2176782336.0, 78364164096.0];
const _: () = assert!(SMALL_F32_POW36.len() > f32_exponent_limit(36).1 as usize);

/// Pre-computed, small powers-of-36.
pub const SMALL_F64_POW36: [f64; 17] = [
    1.0,
    36.0,
    1296.0,
    46656.0,
    1679616.0,
    60466176.0,
    2176782336.0,
    78364164096.0,
    2821109907456.0,
    101559956668416.0,
    3656158440062976.0,
    1.3162170384226714e+17,
    4.738381338321617e+18,
    1.705817281795782e+20,
    6.140942214464815e+21,
    2.2107391972073336e+23,
    7.958661109946401e+24,
];
const _: () = assert!(SMALL_F64_POW36.len() > f64_exponent_limit(36).1 as usize);
