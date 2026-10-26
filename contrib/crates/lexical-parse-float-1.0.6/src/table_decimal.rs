//! Pre-computed tables for writing decimal strings.

#![doc(hidden)]
#![cfg(not(feature = "compact"))]

#[cfg(not(feature = "radix"))]
use crate::bigint::Limb;
use crate::limits::{f32_exponent_limit, f64_exponent_limit, f64_mantissa_limit, u64_power_limit};

// HELPERS
// -------

/// Get lookup table for small int powers.
#[must_use]
#[inline(always)]
#[cfg(not(feature = "power-of-two"))]
pub const fn get_small_int_power(exponent: usize, radix: u32) -> u64 {
    // NOTE: don't check the radix since we also use it for half radix, or 5.
    match radix {
        5 => get_small_int_power5(exponent),
        10 => get_small_int_power10(exponent),
        _ => unreachable!(),
    }
}

/// Get lookup table for small f32 powers.
#[must_use]
#[inline(always)]
#[cfg(not(feature = "power-of-two"))]
pub const fn get_small_f32_power(exponent: usize, radix: u32) -> f32 {
    _ = radix;
    get_small_f32_power10(exponent)
}

/// Get lookup table for small f64 powers.
#[must_use]
#[inline(always)]
#[cfg(not(feature = "power-of-two"))]
pub const fn get_small_f64_power(exponent: usize, radix: u32) -> f64 {
    _ = radix;
    get_small_f64_power10(exponent)
}

/// Get pre-computed power for a large power of radix.
#[must_use]
#[inline(always)]
#[cfg(not(feature = "radix"))]
pub const fn get_large_int_power(_: u32) -> (&'static [Limb], u32) {
    (&LARGE_POW5, LARGE_POW5_STEP)
}

/// Get pre-computed int power of 5.
#[must_use]
#[inline(always)]
pub const fn get_small_int_power5(exponent: usize) -> u64 {
    SMALL_INT_POW5[exponent]
}

/// Get pre-computed int power of 10.
#[must_use]
#[inline(always)]
pub const fn get_small_int_power10(exponent: usize) -> u64 {
    SMALL_INT_POW10[exponent]
}

/// Get pre-computed f32 power of 10.
#[must_use]
#[inline(always)]
pub const fn get_small_f32_power10(exponent: usize) -> f32 {
    SMALL_F32_POW10[exponent]
}

/// Get pre-computed f64 power of 10.
#[must_use]
#[inline(always)]
pub const fn get_small_f64_power10(exponent: usize) -> f64 {
    SMALL_F64_POW10[exponent]
}

// TABLES
// ------

/// Pre-computed, small powers-of-5.
pub const SMALL_INT_POW5: [u64; 28] = [
    1,
    5,
    25,
    125,
    625,
    3125,
    15625,
    78125,
    390625,
    1953125,
    9765625,
    48828125,
    244140625,
    1220703125,
    6103515625,
    30517578125,
    152587890625,
    762939453125,
    3814697265625,
    19073486328125,
    95367431640625,
    476837158203125,
    2384185791015625,
    11920928955078125,
    59604644775390625,
    298023223876953125,
    1490116119384765625,
    7450580596923828125,
];
const _: () = assert!(SMALL_INT_POW5.len() > f64_mantissa_limit(5) as usize);
const _: () = assert!(SMALL_INT_POW5.len() == u64_power_limit(5) as usize + 1);

/// Pre-computed, small powers-of-10.
pub const SMALL_INT_POW10: [u64; 20] = [
    1,
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000,
    10000000000,
    100000000000,
    1000000000000,
    10000000000000,
    100000000000000,
    1000000000000000,
    10000000000000000,
    100000000000000000,
    1000000000000000000,
    10000000000000000000,
];
const _: () = assert!(SMALL_INT_POW10.len() > f64_mantissa_limit(10) as usize);
const _: () = assert!(SMALL_INT_POW10.len() == u64_power_limit(10) as usize + 1);

/// Pre-computed, small powers-of-10.
pub const SMALL_F32_POW10: [f32; 16] =
    [1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 0., 0., 0., 0., 0.];
const _: () = assert!(SMALL_F32_POW10.len() > f32_exponent_limit(10).1 as usize);

/// Pre-computed, small powers-of-10.
pub const SMALL_F64_POW10: [f64; 32] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18, 1e19, 1e20, 1e21, 1e22, 0., 0., 0., 0., 0., 0., 0., 0., 0.,
];
const _: () = assert!(SMALL_F64_POW10.len() > f64_exponent_limit(10).1 as usize);

/// Pre-computed large power-of-5 for 32-bit limbs.
#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub const LARGE_POW5: [u32; 10] = [
    4279965485, 329373468, 4020270615, 2137533757, 4287402176, 1057042919, 1071430142, 2440757623,
    381945767, 46164893,
];

/// Pre-computed large power-of-5 for 64-bit limbs.
#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub const LARGE_POW5: [u64; 5] = [
    1414648277510068013,
    9180637584431281687,
    4539964771860779200,
    10482974169319127550,
    198276706040285095,
];

/// Step for large power-of-5 for 32-bit limbs.
pub const LARGE_POW5_STEP: u32 = 135;
