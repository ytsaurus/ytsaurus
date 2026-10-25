//! Optimized division algorithms for u128.
//!
//! # Fast Algorithms
//!
//! The more optimized algorithms for calculating the divisor constants are
//! based off of the paper "Division by Invariant Integers Using
//! Multiplication", by T. Granlund and P. Montgomery, in "Proc. of the
//! SIGPLAN94 Conference on Programming Language Design and Implementation",
//! available online [here](https://gmplib.org/~tege/divcnst-pldi94.pdf).
//!
//! This approach is derived from the Rust algorithm for formatting 128-bit
//! values, and therefore is similarly dual-licensed under MIT/Apache-2.0.
//!
//! # Fallback Algorithms
//!
//! The slower algorithms in this module are derived off of `dtolnay/itoa`
//! and Rust's compiler-builtins crate. This copies a specific
//! path of LLVM's `__udivmodti4` intrinsic, which does division/
//! modulus for u128 in a single step. Rust implements both division
//! and modulus in terms of this intrinsic, but calls the intrinsic
//! twice for subsequent division and modulus operations on the same
//! dividend/divisor, leading to significant performance overhead.
//!
//! This module calculates the optimal divisors for each radix,
//! and exports a general-purpose division algorithm for u128 where
//! the divisor can fit in a u64. The moderate algorithm is derived from
//! dtolnay/itoa, which can be found
//! [here](https://github.com/dtolnay/itoa/blob/master/src/udiv128.rs), which
//! in turn is derived from Rust's compiler-builtins crate, which can be found
//! [here](https://github.com/rust-lang-nursery/compiler-builtins/blob/master/src/int/udiv.rs).
//!
//! Licensing for these routines is therefore subject to an MIT/Illinois
//! dual license (a BSD-like license), while the rest of the module is
//! subject to an MIT/Apache-2.0 dual-license.
//!
//! # Generation
//!
//! See [`etc/div128.py`] for the script to generate the divisors and the
//! constants, and the division algorithm.
//!
//! [`etc/div128.py`]: https://github.com/Alexhuszagh/rust-lexical/blob/main/lexical-util/etc/div128.py

#![cfg(not(feature = "compact"))]
#![cfg(any(feature = "write-floats", feature = "write-integers"))]

use crate::assert::debug_assert_radix;
use crate::mul::mulhi;

/// Calculate a div/remainder algorithm optimized for power-of-two radixes.
///
/// This is trivial: the number of digits we process is `64 / log2(radix)`.
/// Therefore, the `shr` is `log2(radix) * digits`, and the mask is just the
/// lower `shr` bits of the digits.
#[inline(always)]
#[allow(clippy::many_single_char_names)] // reason="mathematical names"
pub const fn pow2_u128_divrem(n: u128, mask: u64, shr: u32) -> (u128, u64) {
    let quot = n >> shr;
    let rem = mask & n as u64;
    (quot, rem)
}

/// Fast division/remainder algorithm for u128, without a fast native
/// approximation.
#[inline(always)]
#[allow(clippy::many_single_char_names)] // reason="mathematical names"
pub fn fast_u128_divrem(
    n: u128,
    d: u64,
    fast: u128,
    fast_shr: u32,
    factor: u128,
    factor_shr: u32,
) -> (u128, u64) {
    let quot = if n < fast {
        ((n >> fast_shr) as u64 / (d >> fast_shr)) as u128
    } else {
        mulhi::<u128, u64>(n, factor) >> factor_shr
    };
    let rem = (n - quot * d as u128) as u64;
    (quot, rem)
}

/// Fast division/remainder algorithm for u128, without a fast native
/// approximation.
#[inline(always)]
#[allow(clippy::many_single_char_names)] // reason="mathematical names"
pub fn moderate_u128_divrem(n: u128, d: u64, factor: u128, factor_shr: u32) -> (u128, u64) {
    let quot = mulhi::<u128, u64>(n, factor) >> factor_shr;
    let rem = (n - quot * d as u128) as u64;
    (quot, rem)
}

/// Optimized fallback division/remainder algorithm for u128.
///
/// This is because the code generation for u128 divrem is very inefficient
/// in Rust, calling both `__udivmodti4` twice internally, rather than a single
/// time.
///
/// This is still a fair bit slower than the optimized algorithms described
/// in the above paper, but this is a suitable fallback when we cannot use
/// the faster algorithm.
#[cfg_attr(not(feature = "compact"), inline(always))]
#[allow(clippy::many_single_char_names)] // reason="mathematical names"
pub fn slow_u128_divrem(n: u128, d: u64, d_ctlz: u32) -> (u128, u64) {
    // Ensure we have the correct number of leading zeros passed.
    debug_assert_eq!(d_ctlz, d.leading_zeros());

    // Optimize if we can divide using u64 first.
    let high = (n >> 64) as u64;
    if high == 0 {
        let low = n as u64;
        return ((low / d) as u128, low % d);
    }

    // sr = 1 + u64::BITS + d.leading_zeros() - high.leading_zeros();
    let sr = 65 + d_ctlz - high.leading_zeros();

    // 1 <= sr <= u64::BITS - 1
    let mut q: u128 = n << (128 - sr);
    let mut r: u128 = n >> sr;
    let mut carry: u64 = 0;

    // Don't use a range because they may generate references to memcpy in
    // unoptimized code Loop invariants:  r < d; carry is 0 or 1
    let mut i = 0;
    while i < sr {
        i += 1;

        // r:q = ((r:q) << 1) | carry
        r = (r << 1) | (q >> 127);
        q = (q << 1) | carry as u128;

        // carry = 0
        // if r >= d {
        //     r -= d;
        //     carry = 1;
        // }
        let s = (d as u128).wrapping_sub(r).wrapping_sub(1) as i128 >> 127;
        carry = (s & 1) as u64;
        r -= (d as u128) & s as u128;
    }

    ((q << 1) | carry as u128, r as u64)
}

/// Calculate the div/remainder of a value based on the radix.
///
/// This uses the largest divisor possible for the given size,
/// and uses various fast-path approximations for different types.
///
/// 1. Powers-of-two can be cleanly split into 2 64-bit products.
/// 2. Division that can be simulated as if by multiplication by a constant.
/// 3. Cases of 2. with a power-of-two divisor.
/// 4. Fallback cases.
///
/// This returns the quotient and the remainder.
/// For the number of digits processed, see
/// [`min_step`](crate::step::min_step).
#[inline(always)]
#[allow(clippy::needless_return)] // reason="required based on radix configuration"
pub fn u128_divrem(n: u128, radix: u32) -> (u128, u64) {
    debug_assert_radix(radix);

    // NOTE: to avoid branching when w don't need it, we use the compile logic

    #[cfg(feature = "radix")]
    {
        return match radix {
            2 => u128_divrem_2(n),
            3 => u128_divrem_3(n),
            4 => u128_divrem_4(n),
            5 => u128_divrem_5(n),
            6 => u128_divrem_6(n),
            7 => u128_divrem_7(n),
            8 => u128_divrem_8(n),
            9 => u128_divrem_9(n),
            10 => u128_divrem_10(n),
            11 => u128_divrem_11(n),
            12 => u128_divrem_12(n),
            13 => u128_divrem_13(n),
            14 => u128_divrem_14(n),
            15 => u128_divrem_15(n),
            16 => u128_divrem_16(n),
            17 => u128_divrem_17(n),
            18 => u128_divrem_18(n),
            19 => u128_divrem_19(n),
            20 => u128_divrem_20(n),
            21 => u128_divrem_21(n),
            22 => u128_divrem_22(n),
            23 => u128_divrem_23(n),
            24 => u128_divrem_24(n),
            25 => u128_divrem_25(n),
            26 => u128_divrem_26(n),
            27 => u128_divrem_27(n),
            28 => u128_divrem_28(n),
            29 => u128_divrem_29(n),
            30 => u128_divrem_30(n),
            31 => u128_divrem_31(n),
            32 => u128_divrem_32(n),
            33 => u128_divrem_33(n),
            34 => u128_divrem_34(n),
            35 => u128_divrem_35(n),
            36 => u128_divrem_36(n),
            _ => unreachable!(),
        };
    }

    #[cfg(all(feature = "power-of-two", not(feature = "radix")))]
    {
        return match radix {
            2 => u128_divrem_2(n),
            4 => u128_divrem_4(n),
            8 => u128_divrem_8(n),
            10 => u128_divrem_10(n),
            16 => u128_divrem_16(n),
            32 => u128_divrem_32(n),
            _ => unreachable!(),
        };
    }

    #[cfg(not(feature = "power-of-two"))]
    {
        return u128_divrem_10(n);
    }
}

// AUTO-GENERATED
// These functions were auto-generated by `etc/div128.py`.
// Do not edit them unless there is a good reason to.
// Preferably, edit the source code to generate the constants.
//
// The seemingly magical values are all derived there, and are explained
// in the function signatures of the functions they call.

#[inline(always)]
#[cfg_attr(not(feature = "power-of-two"), allow(dead_code))]
const fn u128_divrem_2(n: u128) -> (u128, u64) {
    pow2_u128_divrem(n, 18446744073709551615, 64)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_3(n: u128) -> (u128, u64) {
    slow_u128_divrem(n, 12157665459056928801, 0)
}

#[inline(always)]
#[cfg_attr(not(feature = "power-of-two"), allow(dead_code))]
const fn u128_divrem_4(n: u128) -> (u128, u64) {
    pow2_u128_divrem(n, 18446744073709551615, 64)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_5(n: u128) -> (u128, u64) {
    moderate_u128_divrem(n, 7450580596923828125, 105312291668557186697918027683670432319, 61)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_6(n: u128) -> (u128, u64) {
    fast_u128_divrem(
        n,
        4738381338321616896,
        309485009821345068724781056,
        24,
        165591931273573223021296166324748699891,
        61,
    )
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_7(n: u128) -> (u128, u64) {
    moderate_u128_divrem(n, 3909821048582988049, 200683792729517998822275406364627986707, 61)
}

#[inline(always)]
#[cfg_attr(not(feature = "power-of-two"), allow(dead_code))]
const fn u128_divrem_8(n: u128) -> (u128, u64) {
    pow2_u128_divrem(n, 9223372036854775807, 63)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_9(n: u128) -> (u128, u64) {
    slow_u128_divrem(n, 12157665459056928801, 0)
}

#[inline(always)]
fn u128_divrem_10(n: u128) -> (u128, u64) {
    fast_u128_divrem(
        n,
        10000000000000000000,
        9671406556917033397649408,
        19,
        156927543384667019095894735580191660403,
        62,
    )
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_11(n: u128) -> (u128, u64) {
    slow_u128_divrem(n, 5559917313492231481, 1)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_12(n: u128) -> (u128, u64) {
    slow_u128_divrem(n, 2218611106740436992, 3)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_13(n: u128) -> (u128, u64) {
    moderate_u128_divrem(n, 8650415919381337933, 181410402513790565292660635782582404765, 62)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_14(n: u128) -> (u128, u64) {
    fast_u128_divrem(
        n,
        2177953337809371136,
        1208925819614629174706176,
        16,
        1407280417134467544760816054546363235,
        53,
    )
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_15(n: u128) -> (u128, u64) {
    moderate_u128_divrem(n, 6568408355712890625, 1866504587258795246613513364166764993, 55)
}

#[inline(always)]
#[cfg_attr(not(feature = "power-of-two"), allow(dead_code))]
const fn u128_divrem_16(n: u128) -> (u128, u64) {
    pow2_u128_divrem(n, 18446744073709551615, 64)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_17(n: u128) -> (u128, u64) {
    moderate_u128_divrem(n, 2862423051509815793, 68529153692836345537218837732158950089, 59)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_18(n: u128) -> (u128, u64) {
    fast_u128_divrem(
        n,
        6746640616477458432,
        604462909807314587353088,
        15,
        232601011830094623283686247347795155951,
        62,
    )
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_19(n: u128) -> (u128, u64) {
    moderate_u128_divrem(n, 15181127029874798299, 25842538415601616733690423925257626679, 60)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_20(n: u128) -> (u128, u64) {
    fast_u128_divrem(
        n,
        1638400000000000000,
        4951760157141521099596496896,
        28,
        239452428260295134118491722992235809941,
        60,
    )
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_21(n: u128) -> (u128, u64) {
    moderate_u128_divrem(n, 3243919932521508681, 120939747781233590383781714337497669585, 60)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_22(n: u128) -> (u128, u64) {
    slow_u128_divrem(n, 6221821273427820544, 1)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_23(n: u128) -> (u128, u64) {
    moderate_u128_divrem(n, 11592836324538749809, 270731922700393644432243678371210997949, 63)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_24(n: u128) -> (u128, u64) {
    fast_u128_divrem(
        n,
        876488338465357824,
        10141204801825835211973625643008,
        39,
        55950381945266105153185943557606235389,
        57,
    )
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_25(n: u128) -> (u128, u64) {
    moderate_u128_divrem(n, 1490116119384765625, 131640364585696483372397534604588040399, 59)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_26(n: u128) -> (u128, u64) {
    fast_u128_divrem(
        n,
        2481152873203736576,
        151115727451828646838272,
        13,
        316239166637962178669658228673482425689,
        61,
    )
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_27(n: u128) -> (u128, u64) {
    slow_u128_divrem(n, 4052555153018976267, 2)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_28(n: u128) -> (u128, u64) {
    fast_u128_divrem(
        n,
        6502111422497947648,
        1237940039285380274899124224,
        26,
        241348591538561183926479953354701294803,
        62,
    )
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_29(n: u128) -> (u128, u64) {
    moderate_u128_divrem(n, 10260628712958602189, 152941450056053853841698190746050519297, 62)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_30(n: u128) -> (u128, u64) {
    slow_u128_divrem(n, 15943230000000000000, 0)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_31(n: u128) -> (u128, u64) {
    moderate_u128_divrem(n, 787662783788549761, 124519929891402176328714857711808162537, 58)
}

#[inline(always)]
#[cfg_attr(not(feature = "power-of-two"), allow(dead_code))]
const fn u128_divrem_32(n: u128) -> (u128, u64) {
    pow2_u128_divrem(n, 1152921504606846975, 60)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_33(n: u128) -> (u128, u64) {
    slow_u128_divrem(n, 1667889514952984961, 3)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_34(n: u128) -> (u128, u64) {
    fast_u128_divrem(
        n,
        2386420683693101056,
        75557863725914323419136,
        12,
        328792707121977505492535302517672775183,
        61,
    )
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_35(n: u128) -> (u128, u64) {
    moderate_u128_divrem(n, 3379220508056640625, 116097442450503652080238494022501325491, 60)
}

#[inline(always)]
#[cfg_attr(not(feature = "radix"), allow(dead_code))]
fn u128_divrem_36(n: u128) -> (u128, u64) {
    fast_u128_divrem(
        n,
        4738381338321616896,
        309485009821345068724781056,
        24,
        165591931273573223021296166324748699891,
        61,
    )
}
