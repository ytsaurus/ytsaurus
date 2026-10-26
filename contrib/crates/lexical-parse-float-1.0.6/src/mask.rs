//! Utilities to generate bitmasks.

#![doc(hidden)]

/// Generate a bitwise mask for the lower `n` bits.
///
/// # Examples
///
/// ```rust
/// # use lexical_parse_float::mask::lower_n_mask;
/// assert_eq!(lower_n_mask(2), 0b11);
/// ```
#[must_use]
#[inline(always)]
#[allow(clippy::match_bool)] // reason="easier to visualize logic"
pub const fn lower_n_mask(n: u64) -> u64 {
    debug_assert!(n <= 64, "lower_n_mask() overflow in shl.");

    match n == 64 {
        true => u64::MAX,
        false => (1 << n) - 1,
    }
}

/// Calculate the halfway point for the lower `n` bits.
///
/// # Examples
///
/// ```rust
/// # use lexical_parse_float::mask::lower_n_halfway;
/// assert_eq!(lower_n_halfway(2), 0b10);
/// ```
#[must_use]
#[inline(always)]
#[allow(clippy::match_bool)] // reason="easier to visualize logic"
pub const fn lower_n_halfway(n: u64) -> u64 {
    debug_assert!(n <= 64, "lower_n_halfway() overflow in shl.");

    match n == 0 {
        true => 0,
        false => nth_bit(n - 1),
    }
}

/// Calculate a scalar factor of 2 above the halfway point.
///
/// # Examples
///
/// ```rust
/// # use lexical_parse_float::mask::nth_bit;
/// assert_eq!(nth_bit(2), 0b100);
/// ```
#[must_use]
#[inline(always)]
pub const fn nth_bit(n: u64) -> u64 {
    debug_assert!(n < 64, "nth_bit() overflow in shl.");
    1 << n
}
