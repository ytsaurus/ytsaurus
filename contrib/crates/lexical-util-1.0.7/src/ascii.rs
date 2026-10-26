//! Utilities for working with ASCII characters.

/// Determine if a character is a valid ASCII character for float grammar.
#[inline(always)]
pub const fn is_valid_ascii(c: u8) -> bool {
    // Below 0x20 is mostly control characters, with no representation.
    // 0x7F is a control character, DEL, so don't include it.
    // We also want the few visual characters below 0x20:
    //  0x09 - Horizontal Tab
    //  0x0A - Newline
    //  0x0B - Vertical Tab (Deprecated)
    //  0x0C - Form Feed (Deprecated)
    //  0x0D - Carriage Return
    (c >= 0x09 && c <= 0x0d) || (c >= 0x20 && c < 0x7F)
}

/// Determine if a slice is all valid ASCII characters for float grammar.
#[inline(always)]
pub const fn is_valid_ascii_slice(slc: &[u8]) -> bool {
    let mut index = 0;
    while index < slc.len() {
        if !is_valid_ascii(slc[index]) {
            return false;
        }
        index += 1;
    }
    true
}

/// Determine if a character is a valid ASCII letter.
#[inline(always)]
pub const fn is_valid_letter(c: u8) -> bool {
    (c >= 0x41 && c <= 0x5a) || (c >= 0x61 && c <= 0x7a)
}

/// Determine if a slice is all valid ASCII letters.
#[inline(always)]
pub const fn is_valid_letter_slice(slc: &[u8]) -> bool {
    let mut index = 0;
    while index < slc.len() {
        if !is_valid_letter(slc[index]) {
            return false;
        }
        index += 1;
    }
    true
}
