//! Utilities to process digits.
//!
//! This both contains routines to convert to and from digits,
//! as well as iterate over digits while skipping digit separators.

// CONST FNS
// ---------

// These are optimized functions for when the radix is known at compile-time,
// which is **most** of our cases. There are cases where for code generation,
// using a runtime algorithm is preferable.

/// Unchecked, highly optimized algorithm to convert a char to a digit.
/// This only works if the input character is known to be a valid digit.
#[inline(always)]
pub const fn char_to_valid_digit_const(c: u8, radix: u32) -> u32 {
    if radix <= 10 {
        // Optimize for small radixes.
        (c.wrapping_sub(b'0')) as u32
    } else {
        // Fallback, still decently fast.
        let digit = match c {
            b'0'..=b'9' => c - b'0',
            b'A'..=b'Z' => c - b'A' + 10,
            b'a'..=b'z' => c - b'a' + 10,
            _ => 0xFF,
        };
        digit as u32
    }
}

/// Convert a character to a digit with a radix known at compile time.
///
/// This optimizes for cases where radix is <= 10, and uses a decent,
/// match-based fallback algorithm.
#[inline(always)]
pub const fn char_to_digit_const(c: u8, radix: u32) -> Option<u32> {
    let digit = char_to_valid_digit_const(c, radix);
    if digit < radix {
        Some(digit)
    } else {
        None
    }
}

/// Determine if a character is a digit with a radix known at compile time.
#[inline(always)]
pub const fn char_is_digit_const(c: u8, radix: u32) -> bool {
    char_to_digit_const(c, radix).is_some()
}

/// Convert a digit to a character with a radix known at compile time.
///
/// This optimizes for cases where radix is <= 10, and uses a decent,
/// match-based fallback algorithm.
#[inline(always)]
#[cfg(any(feature = "parse-floats", feature = "write-floats", feature = "write-integers"))]
pub const fn digit_to_char_const(digit: u32, radix: u32) -> u8 {
    if radix <= 10 || digit < 10 {
        // Can short-circuit if we know the radix is small at compile time.
        digit as u8 + b'0'
    } else {
        digit as u8 + b'A' - 10
    }
}

// NON-CONST
// ---------

// These are less optimized functions for when the radix is not known at
// compile-time, which is a few (but important) cases. These generally have
// improved compiler optimization passes when generics are used more sparingly.

/// Convert a character to a digit.
#[inline(always)]
#[cfg(any(feature = "parse-floats", feature = "parse-integers"))]
pub const fn char_to_digit(c: u8, radix: u32) -> Option<u32> {
    // Fallback, still decently fast.
    let digit = match c {
        b'0'..=b'9' => c - b'0',
        b'A'..=b'Z' => c - b'A' + 10,
        b'a'..=b'z' => c - b'a' + 10,
        _ => 0xFF,
    } as u32;
    if digit < radix {
        Some(digit)
    } else {
        None
    }
}

/// Determine if a character is a digit.
#[inline(always)]
#[cfg(any(feature = "parse-floats", feature = "parse-integers"))]
pub const fn char_is_digit(c: u8, radix: u32) -> bool {
    char_to_digit(c, radix).is_some()
}

/// Convert a digit to a character. This uses a pre-computed table to avoid
/// branching.
///
/// # Panics
///
/// Panics if `digit >= 36`.
#[inline(always)]
#[cfg(any(feature = "write-floats", feature = "write-integers"))]
pub fn digit_to_char(digit: u32) -> u8 {
    const TABLE: [u8; 36] = [
        b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'A', b'B', b'C', b'D', b'E',
        b'F', b'G', b'H', b'I', b'J', b'K', b'L', b'M', b'N', b'O', b'P', b'Q', b'R', b'S', b'T',
        b'U', b'V', b'W', b'X', b'Y', b'Z',
    ];
    debug_assert!(digit < 36, "digit_to_char() invalid character.");
    TABLE[digit as usize]
}
