//! The maximum digits that can be held in a u64 for a given radix without
//! overflowing.
//!
//! This is useful for 128-bit division and operations, since it can
//! reduces the number of inefficient, non-native operations.
//!
//! # Generation
//!
//! See [`etc/step.py`] for the script to generate the divisors and the
//! constants, and the division algorithm.
//!
//! [`etc/step.py`]: https://github.com/Alexhuszagh/rust-lexical/blob/main/lexical-util/etc/step.py

#![cfg(any(
    feature = "parse-floats",
    feature = "parse-integers",
    feature = "write-floats",
    feature = "write-integers",
))]

// NOTE:
//  Fallback radixes use 1 for the value to avoid infinite loops,
//  but allowing them in `const fn`.

/// Get the number of digits that can be always processed without overflowing.
///
/// Calculate the number of digits that can always be processed without
/// overflowing for a given type, that is, it can process every (positive) value
/// in the range `[0, radix^N)` where `N` is the number of digits.
///
/// For example, with [`u8`] with radix `10`, we have a maximum value of `255`,
/// so we have a min step of `2`: that is, we can always process values from
/// `[0, 10^2)` (or `[0, 100)`).
#[inline(always)]
#[allow(clippy::needless_return)] // reason="required depending on our radix configuration"
pub const fn min_step(radix: u32, bits: usize, is_signed: bool) -> usize {
    // NOTE: to avoid branching when w don't need it, we use the compile logic

    #[cfg(feature = "radix")]
    {
        return match radix {
            2 => min_step_2(bits, is_signed),
            3 => min_step_3(bits, is_signed),
            4 => min_step_4(bits, is_signed),
            5 => min_step_5(bits, is_signed),
            6 => min_step_6(bits, is_signed),
            7 => min_step_7(bits, is_signed),
            8 => min_step_8(bits, is_signed),
            9 => min_step_9(bits, is_signed),
            10 => min_step_10(bits, is_signed),
            11 => min_step_11(bits, is_signed),
            12 => min_step_12(bits, is_signed),
            13 => min_step_13(bits, is_signed),
            14 => min_step_14(bits, is_signed),
            15 => min_step_15(bits, is_signed),
            16 => min_step_16(bits, is_signed),
            17 => min_step_17(bits, is_signed),
            18 => min_step_18(bits, is_signed),
            19 => min_step_19(bits, is_signed),
            20 => min_step_20(bits, is_signed),
            21 => min_step_21(bits, is_signed),
            22 => min_step_22(bits, is_signed),
            23 => min_step_23(bits, is_signed),
            24 => min_step_24(bits, is_signed),
            25 => min_step_25(bits, is_signed),
            26 => min_step_26(bits, is_signed),
            27 => min_step_27(bits, is_signed),
            28 => min_step_28(bits, is_signed),
            29 => min_step_29(bits, is_signed),
            30 => min_step_30(bits, is_signed),
            31 => min_step_31(bits, is_signed),
            32 => min_step_32(bits, is_signed),
            33 => min_step_33(bits, is_signed),
            34 => min_step_34(bits, is_signed),
            35 => min_step_35(bits, is_signed),
            36 => min_step_36(bits, is_signed),
            _ => 1,
        };
    }

    #[cfg(all(feature = "power-of-two", not(feature = "radix")))]
    {
        return match radix {
            2 => min_step_2(bits, is_signed),
            4 => min_step_4(bits, is_signed),
            8 => min_step_8(bits, is_signed),
            10 => min_step_10(bits, is_signed),
            16 => min_step_16(bits, is_signed),
            32 => min_step_32(bits, is_signed),
            _ => 1,
        };
    }

    #[cfg(not(feature = "power-of-two"))]
    {
        _ = radix;
        return min_step_10(bits, is_signed);
    }
}

/// Get the maximum number of digits that can be processed without overflowing.
///
/// Calculate the number of digits that can be processed without overflowing for
/// a given type, that is, it can process at least `radix^N`. This does not
/// necessarily mean it can process every value in the range `[0, radix^(N+1))`.
///
/// For example, with [`u8`] with radix `10`, we have a maximum value of `255`,
/// so we have a max step of `3`: that is, it can process up to 3 digits (`[100,
/// 256)`), even if it **cannot** process every 3 digit value (`[256, 1000)`).
#[inline(always)]
#[allow(clippy::needless_return)] // reason="required depending on our radix configuration"
pub const fn max_step(radix: u32, bits: usize, is_signed: bool) -> usize {
    #[cfg(feature = "radix")]
    {
        return match radix {
            2 => max_step_2(bits, is_signed),
            3 => max_step_3(bits, is_signed),
            4 => max_step_4(bits, is_signed),
            5 => max_step_5(bits, is_signed),
            6 => max_step_6(bits, is_signed),
            7 => max_step_7(bits, is_signed),
            8 => max_step_8(bits, is_signed),
            9 => max_step_9(bits, is_signed),
            10 => max_step_10(bits, is_signed),
            11 => max_step_11(bits, is_signed),
            12 => max_step_12(bits, is_signed),
            13 => max_step_13(bits, is_signed),
            14 => max_step_14(bits, is_signed),
            15 => max_step_15(bits, is_signed),
            16 => max_step_16(bits, is_signed),
            17 => max_step_17(bits, is_signed),
            18 => max_step_18(bits, is_signed),
            19 => max_step_19(bits, is_signed),
            20 => max_step_20(bits, is_signed),
            21 => max_step_21(bits, is_signed),
            22 => max_step_22(bits, is_signed),
            23 => max_step_23(bits, is_signed),
            24 => max_step_24(bits, is_signed),
            25 => max_step_25(bits, is_signed),
            26 => max_step_26(bits, is_signed),
            27 => max_step_27(bits, is_signed),
            28 => max_step_28(bits, is_signed),
            29 => max_step_29(bits, is_signed),
            30 => max_step_30(bits, is_signed),
            31 => max_step_31(bits, is_signed),
            32 => max_step_32(bits, is_signed),
            33 => max_step_33(bits, is_signed),
            34 => max_step_34(bits, is_signed),
            35 => max_step_35(bits, is_signed),
            36 => max_step_36(bits, is_signed),
            _ => 1,
        };
    }

    #[cfg(all(feature = "power-of-two", not(feature = "radix")))]
    {
        return match radix {
            2 => max_step_2(bits, is_signed),
            4 => max_step_4(bits, is_signed),
            8 => max_step_8(bits, is_signed),
            10 => max_step_10(bits, is_signed),
            16 => max_step_16(bits, is_signed),
            32 => max_step_32(bits, is_signed),
            _ => 1,
        };
    }

    #[cfg(not(feature = "power-of-two"))]
    {
        _ = radix;
        return max_step_10(bits, is_signed);
    }
}

/// Calculate the number of digits that can be processed without overflowing a
/// [`u64`]. Helper function since this is used for 128-bit division.
///
/// This is an alias for [`min_step`] with `bits == 64` and `is_signed ==
/// false`.
pub const fn u64_step(radix: u32) -> usize {
    min_step(radix, 64, false)
}

// AUTO-GENERATED
// These functions were auto-generated by `etc/step.py`.
// Do not edit them unless there is a good reason to.
// Preferably, edit the source code to generate the constants.
//
// NOTE: For the fallthrough value for types (in case of adding short
// or wider type support in the future), use 1 so it doesn't infinitely
// recurse. Under normal circumstances, this will never be called.

#[inline(always)]
#[cfg_attr(not(feature = "power-of-two"), allow(dead_code))]
const fn max_step_2(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 7,
        8 if !is_signed => 8,
        16 if is_signed => 15,
        16 if !is_signed => 16,
        32 if is_signed => 31,
        32 if !is_signed => 32,
        64 if is_signed => 63,
        64 if !is_signed => 64,
        128 if is_signed => 127,
        128 if !is_signed => 128,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "power-of-two"), allow(dead_code))]
const fn min_step_2(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 7,
        8 if !is_signed => 8,
        16 if is_signed => 15,
        16 if !is_signed => 16,
        32 if is_signed => 31,
        32 if !is_signed => 32,
        64 if is_signed => 63,
        64 if !is_signed => 64,
        128 if is_signed => 127,
        128 if !is_signed => 128,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_3(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 5,
        8 if !is_signed => 6,
        16 if is_signed => 10,
        16 if !is_signed => 11,
        32 if is_signed => 20,
        32 if !is_signed => 21,
        64 if is_signed => 40,
        64 if !is_signed => 41,
        128 if is_signed => 81,
        128 if !is_signed => 81,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_3(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 4,
        8 if !is_signed => 5,
        16 if is_signed => 9,
        16 if !is_signed => 10,
        32 if is_signed => 19,
        32 if !is_signed => 20,
        64 if is_signed => 39,
        64 if !is_signed => 40,
        128 if is_signed => 80,
        128 if !is_signed => 80,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "power-of-two"), allow(dead_code))]
const fn max_step_4(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 4,
        8 if !is_signed => 4,
        16 if is_signed => 8,
        16 if !is_signed => 8,
        32 if is_signed => 16,
        32 if !is_signed => 16,
        64 if is_signed => 32,
        64 if !is_signed => 32,
        128 if is_signed => 64,
        128 if !is_signed => 64,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "power-of-two"), allow(dead_code))]
const fn min_step_4(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 3,
        8 if !is_signed => 4,
        16 if is_signed => 7,
        16 if !is_signed => 8,
        32 if is_signed => 15,
        32 if !is_signed => 16,
        64 if is_signed => 31,
        64 if !is_signed => 32,
        128 if is_signed => 63,
        128 if !is_signed => 64,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_5(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 4,
        8 if !is_signed => 4,
        16 if is_signed => 7,
        16 if !is_signed => 7,
        32 if is_signed => 14,
        32 if !is_signed => 14,
        64 if is_signed => 28,
        64 if !is_signed => 28,
        128 if is_signed => 55,
        128 if !is_signed => 56,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_5(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 3,
        8 if !is_signed => 3,
        16 if is_signed => 6,
        16 if !is_signed => 6,
        32 if is_signed => 13,
        32 if !is_signed => 13,
        64 if is_signed => 27,
        64 if !is_signed => 27,
        128 if is_signed => 54,
        128 if !is_signed => 55,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_6(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 3,
        8 if !is_signed => 4,
        16 if is_signed => 6,
        16 if !is_signed => 7,
        32 if is_signed => 12,
        32 if !is_signed => 13,
        64 if is_signed => 25,
        64 if !is_signed => 25,
        128 if is_signed => 50,
        128 if !is_signed => 50,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_6(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 3,
        16 if is_signed => 5,
        16 if !is_signed => 6,
        32 if is_signed => 11,
        32 if !is_signed => 12,
        64 if is_signed => 24,
        64 if !is_signed => 24,
        128 if is_signed => 49,
        128 if !is_signed => 49,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_7(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 3,
        8 if !is_signed => 3,
        16 if is_signed => 6,
        16 if !is_signed => 6,
        32 if is_signed => 12,
        32 if !is_signed => 12,
        64 if is_signed => 23,
        64 if !is_signed => 23,
        128 if is_signed => 46,
        128 if !is_signed => 46,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_7(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 5,
        16 if !is_signed => 5,
        32 if is_signed => 11,
        32 if !is_signed => 11,
        64 if is_signed => 22,
        64 if !is_signed => 22,
        128 if is_signed => 45,
        128 if !is_signed => 45,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "power-of-two"), allow(dead_code))]
const fn max_step_8(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 3,
        8 if !is_signed => 3,
        16 if is_signed => 5,
        16 if !is_signed => 6,
        32 if is_signed => 11,
        32 if !is_signed => 11,
        64 if is_signed => 21,
        64 if !is_signed => 22,
        128 if is_signed => 43,
        128 if !is_signed => 43,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "power-of-two"), allow(dead_code))]
const fn min_step_8(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 5,
        16 if !is_signed => 5,
        32 if is_signed => 10,
        32 if !is_signed => 10,
        64 if is_signed => 21,
        64 if !is_signed => 21,
        128 if is_signed => 42,
        128 if !is_signed => 42,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_9(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 3,
        8 if !is_signed => 3,
        16 if is_signed => 5,
        16 if !is_signed => 6,
        32 if is_signed => 10,
        32 if !is_signed => 11,
        64 if is_signed => 20,
        64 if !is_signed => 21,
        128 if is_signed => 41,
        128 if !is_signed => 41,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_9(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 5,
        32 if is_signed => 9,
        32 if !is_signed => 10,
        64 if is_signed => 19,
        64 if !is_signed => 20,
        128 if is_signed => 40,
        128 if !is_signed => 40,
        _ => 1,
    }
}

#[inline(always)]
const fn max_step_10(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 3,
        8 if !is_signed => 3,
        16 if is_signed => 5,
        16 if !is_signed => 5,
        32 if is_signed => 10,
        32 if !is_signed => 10,
        64 if is_signed => 19,
        64 if !is_signed => 20,
        128 if is_signed => 39,
        128 if !is_signed => 39,
        _ => 1,
    }
}

#[inline(always)]
const fn min_step_10(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 9,
        32 if !is_signed => 9,
        64 if is_signed => 18,
        64 if !is_signed => 19,
        128 if is_signed => 38,
        128 if !is_signed => 38,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_11(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 3,
        8 if !is_signed => 3,
        16 if is_signed => 5,
        16 if !is_signed => 5,
        32 if is_signed => 9,
        32 if !is_signed => 10,
        64 if is_signed => 19,
        64 if !is_signed => 19,
        128 if is_signed => 37,
        128 if !is_signed => 38,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_11(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 8,
        32 if !is_signed => 9,
        64 if is_signed => 18,
        64 if !is_signed => 18,
        128 if is_signed => 36,
        128 if !is_signed => 37,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_12(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 3,
        16 if is_signed => 5,
        16 if !is_signed => 5,
        32 if is_signed => 9,
        32 if !is_signed => 9,
        64 if is_signed => 18,
        64 if !is_signed => 18,
        128 if is_signed => 36,
        128 if !is_signed => 36,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_12(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 8,
        32 if !is_signed => 8,
        64 if is_signed => 17,
        64 if !is_signed => 17,
        128 if is_signed => 35,
        128 if !is_signed => 35,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_13(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 3,
        16 if is_signed => 5,
        16 if !is_signed => 5,
        32 if is_signed => 9,
        32 if !is_signed => 9,
        64 if is_signed => 18,
        64 if !is_signed => 18,
        128 if is_signed => 35,
        128 if !is_signed => 35,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_13(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 8,
        32 if !is_signed => 8,
        64 if is_signed => 17,
        64 if !is_signed => 17,
        128 if is_signed => 34,
        128 if !is_signed => 34,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_14(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 3,
        16 if is_signed => 4,
        16 if !is_signed => 5,
        32 if is_signed => 9,
        32 if !is_signed => 9,
        64 if is_signed => 17,
        64 if !is_signed => 17,
        128 if is_signed => 34,
        128 if !is_signed => 34,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_14(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 2,
        16 if is_signed => 3,
        16 if !is_signed => 4,
        32 if is_signed => 8,
        32 if !is_signed => 8,
        64 if is_signed => 16,
        64 if !is_signed => 16,
        128 if is_signed => 33,
        128 if !is_signed => 33,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_15(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 3,
        16 if is_signed => 4,
        16 if !is_signed => 5,
        32 if is_signed => 8,
        32 if !is_signed => 9,
        64 if is_signed => 17,
        64 if !is_signed => 17,
        128 if is_signed => 33,
        128 if !is_signed => 33,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_15(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 2,
        16 if is_signed => 3,
        16 if !is_signed => 4,
        32 if is_signed => 7,
        32 if !is_signed => 8,
        64 if is_signed => 16,
        64 if !is_signed => 16,
        128 if is_signed => 32,
        128 if !is_signed => 32,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "power-of-two"), allow(dead_code))]
const fn max_step_16(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 8,
        32 if !is_signed => 8,
        64 if is_signed => 16,
        64 if !is_signed => 16,
        128 if is_signed => 32,
        128 if !is_signed => 32,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "power-of-two"), allow(dead_code))]
const fn min_step_16(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 2,
        16 if is_signed => 3,
        16 if !is_signed => 4,
        32 if is_signed => 7,
        32 if !is_signed => 8,
        64 if is_signed => 15,
        64 if !is_signed => 16,
        128 if is_signed => 31,
        128 if !is_signed => 32,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_17(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 8,
        32 if !is_signed => 8,
        64 if is_signed => 16,
        64 if !is_signed => 16,
        128 if is_signed => 32,
        128 if !is_signed => 32,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_17(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 3,
        16 if !is_signed => 3,
        32 if is_signed => 7,
        32 if !is_signed => 7,
        64 if is_signed => 15,
        64 if !is_signed => 15,
        128 if is_signed => 31,
        128 if !is_signed => 31,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_18(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 8,
        32 if !is_signed => 8,
        64 if is_signed => 16,
        64 if !is_signed => 16,
        128 if is_signed => 31,
        128 if !is_signed => 31,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_18(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 3,
        16 if !is_signed => 3,
        32 if is_signed => 7,
        32 if !is_signed => 7,
        64 if is_signed => 15,
        64 if !is_signed => 15,
        128 if is_signed => 30,
        128 if !is_signed => 30,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_19(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 8,
        32 if !is_signed => 8,
        64 if is_signed => 15,
        64 if !is_signed => 16,
        128 if is_signed => 30,
        128 if !is_signed => 31,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_19(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 3,
        16 if !is_signed => 3,
        32 if is_signed => 7,
        32 if !is_signed => 7,
        64 if is_signed => 14,
        64 if !is_signed => 15,
        128 if is_signed => 29,
        128 if !is_signed => 30,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_20(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 8,
        32 if !is_signed => 8,
        64 if is_signed => 15,
        64 if !is_signed => 15,
        128 if is_signed => 30,
        128 if !is_signed => 30,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_20(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 3,
        16 if !is_signed => 3,
        32 if is_signed => 7,
        32 if !is_signed => 7,
        64 if is_signed => 14,
        64 if !is_signed => 14,
        128 if is_signed => 29,
        128 if !is_signed => 29,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_21(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 8,
        32 if !is_signed => 8,
        64 if is_signed => 15,
        64 if !is_signed => 15,
        128 if is_signed => 29,
        128 if !is_signed => 30,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_21(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 3,
        16 if !is_signed => 3,
        32 if is_signed => 7,
        32 if !is_signed => 7,
        64 if is_signed => 14,
        64 if !is_signed => 14,
        128 if is_signed => 28,
        128 if !is_signed => 29,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_22(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 7,
        32 if !is_signed => 8,
        64 if is_signed => 15,
        64 if !is_signed => 15,
        128 if is_signed => 29,
        128 if !is_signed => 29,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_22(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 3,
        16 if !is_signed => 3,
        32 if is_signed => 6,
        32 if !is_signed => 7,
        64 if is_signed => 14,
        64 if !is_signed => 14,
        128 if is_signed => 28,
        128 if !is_signed => 28,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_23(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 7,
        32 if !is_signed => 8,
        64 if is_signed => 14,
        64 if !is_signed => 15,
        128 if is_signed => 29,
        128 if !is_signed => 29,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_23(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 3,
        16 if !is_signed => 3,
        32 if is_signed => 6,
        32 if !is_signed => 7,
        64 if is_signed => 13,
        64 if !is_signed => 14,
        128 if is_signed => 28,
        128 if !is_signed => 28,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_24(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 7,
        32 if !is_signed => 7,
        64 if is_signed => 14,
        64 if !is_signed => 14,
        128 if is_signed => 28,
        128 if !is_signed => 28,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_24(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 3,
        16 if !is_signed => 3,
        32 if is_signed => 6,
        32 if !is_signed => 6,
        64 if is_signed => 13,
        64 if !is_signed => 13,
        128 if is_signed => 27,
        128 if !is_signed => 27,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_25(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 7,
        32 if !is_signed => 7,
        64 if is_signed => 14,
        64 if !is_signed => 14,
        128 if is_signed => 28,
        128 if !is_signed => 28,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_25(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 3,
        16 if !is_signed => 3,
        32 if is_signed => 6,
        32 if !is_signed => 6,
        64 if is_signed => 13,
        64 if !is_signed => 13,
        128 if is_signed => 27,
        128 if !is_signed => 27,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_26(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 7,
        32 if !is_signed => 7,
        64 if is_signed => 14,
        64 if !is_signed => 14,
        128 if is_signed => 28,
        128 if !is_signed => 28,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_26(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 3,
        16 if !is_signed => 3,
        32 if is_signed => 6,
        32 if !is_signed => 6,
        64 if is_signed => 13,
        64 if !is_signed => 13,
        128 if is_signed => 27,
        128 if !is_signed => 27,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_27(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 7,
        32 if !is_signed => 7,
        64 if is_signed => 14,
        64 if !is_signed => 14,
        128 if is_signed => 27,
        128 if !is_signed => 27,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_27(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 3,
        16 if !is_signed => 3,
        32 if is_signed => 6,
        32 if !is_signed => 6,
        64 if is_signed => 13,
        64 if !is_signed => 13,
        128 if is_signed => 26,
        128 if !is_signed => 26,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_28(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 7,
        32 if !is_signed => 7,
        64 if is_signed => 14,
        64 if !is_signed => 14,
        128 if is_signed => 27,
        128 if !is_signed => 27,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_28(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 3,
        16 if !is_signed => 3,
        32 if is_signed => 6,
        32 if !is_signed => 6,
        64 if is_signed => 13,
        64 if !is_signed => 13,
        128 if is_signed => 26,
        128 if !is_signed => 26,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_29(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 7,
        32 if !is_signed => 7,
        64 if is_signed => 13,
        64 if !is_signed => 14,
        128 if is_signed => 27,
        128 if !is_signed => 27,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_29(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 3,
        16 if !is_signed => 3,
        32 if is_signed => 6,
        32 if !is_signed => 6,
        64 if is_signed => 12,
        64 if !is_signed => 13,
        128 if is_signed => 26,
        128 if !is_signed => 26,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_30(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 7,
        32 if !is_signed => 7,
        64 if is_signed => 13,
        64 if !is_signed => 14,
        128 if is_signed => 26,
        128 if !is_signed => 27,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_30(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 3,
        16 if !is_signed => 3,
        32 if is_signed => 6,
        32 if !is_signed => 6,
        64 if is_signed => 12,
        64 if !is_signed => 13,
        128 if is_signed => 25,
        128 if !is_signed => 26,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_31(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 4,
        16 if !is_signed => 4,
        32 if is_signed => 7,
        32 if !is_signed => 7,
        64 if is_signed => 13,
        64 if !is_signed => 13,
        128 if is_signed => 26,
        128 if !is_signed => 26,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_31(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 3,
        16 if !is_signed => 3,
        32 if is_signed => 6,
        32 if !is_signed => 6,
        64 if is_signed => 12,
        64 if !is_signed => 12,
        128 if is_signed => 25,
        128 if !is_signed => 25,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "power-of-two"), allow(dead_code))]
const fn max_step_32(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 3,
        16 if !is_signed => 4,
        32 if is_signed => 7,
        32 if !is_signed => 7,
        64 if is_signed => 13,
        64 if !is_signed => 13,
        128 if is_signed => 26,
        128 if !is_signed => 26,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "power-of-two"), allow(dead_code))]
const fn min_step_32(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 3,
        16 if !is_signed => 3,
        32 if is_signed => 6,
        32 if !is_signed => 6,
        64 if is_signed => 12,
        64 if !is_signed => 12,
        128 if is_signed => 25,
        128 if !is_signed => 25,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_33(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 3,
        16 if !is_signed => 4,
        32 if is_signed => 7,
        32 if !is_signed => 7,
        64 if is_signed => 13,
        64 if !is_signed => 13,
        128 if is_signed => 26,
        128 if !is_signed => 26,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_33(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 2,
        16 if !is_signed => 3,
        32 if is_signed => 6,
        32 if !is_signed => 6,
        64 if is_signed => 12,
        64 if !is_signed => 12,
        128 if is_signed => 25,
        128 if !is_signed => 25,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_34(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 3,
        16 if !is_signed => 4,
        32 if is_signed => 7,
        32 if !is_signed => 7,
        64 if is_signed => 13,
        64 if !is_signed => 13,
        128 if is_signed => 25,
        128 if !is_signed => 26,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_34(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 2,
        16 if !is_signed => 3,
        32 if is_signed => 6,
        32 if !is_signed => 6,
        64 if is_signed => 12,
        64 if !is_signed => 12,
        128 if is_signed => 24,
        128 if !is_signed => 25,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_35(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 3,
        16 if !is_signed => 4,
        32 if is_signed => 7,
        32 if !is_signed => 7,
        64 if is_signed => 13,
        64 if !is_signed => 13,
        128 if is_signed => 25,
        128 if !is_signed => 25,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_35(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 2,
        16 if !is_signed => 3,
        32 if is_signed => 6,
        32 if !is_signed => 6,
        64 if is_signed => 12,
        64 if !is_signed => 12,
        128 if is_signed => 24,
        128 if !is_signed => 24,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn max_step_36(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 2,
        8 if !is_signed => 2,
        16 if is_signed => 3,
        16 if !is_signed => 4,
        32 if is_signed => 6,
        32 if !is_signed => 7,
        64 if is_signed => 13,
        64 if !is_signed => 13,
        128 if is_signed => 25,
        128 if !is_signed => 25,
        _ => 1,
    }
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
const fn min_step_36(bits: usize, is_signed: bool) -> usize {
    match bits {
        8 if is_signed => 1,
        8 if !is_signed => 1,
        16 if is_signed => 2,
        16 if !is_signed => 3,
        32 if is_signed => 5,
        32 if !is_signed => 6,
        64 if is_signed => 12,
        64 if !is_signed => 12,
        128 if is_signed => 24,
        128 if !is_signed => 24,
        _ => 1,
    }
}
