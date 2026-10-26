//! Simple, shared algorithms for slices and iterators.

use crate::num::Integer;

/// Copy bytes from source to destination.
///
/// This is only used in our compact and radix integer formatted, so
/// performance isn't the highest consideration here.
#[inline(always)]
#[cfg(any(feature = "write-floats", feature = "write-integers"))]
pub fn copy_to_dst<T: Copy, Bytes: AsRef<[T]>>(dst: &mut [T], src: Bytes) -> usize {
    let src = src.as_ref();
    dst[..src.len()].copy_from_slice(src);

    src.len()
}

/// Count the number of trailing characters equal to a given value.
#[inline(always)]
#[cfg(any(feature = "write-floats", feature = "write-integers"))]
pub fn rtrim_char_count(slc: &[u8], c: u8) -> usize {
    slc.iter().rev().take_while(|&&si| si == c).count()
}

/// Count the number of leading characters equal to a given value.
#[inline(always)]
#[cfg(any(feature = "write-floats", feature = "write-integers"))]
pub fn ltrim_char_count(slc: &[u8], c: u8) -> usize {
    slc.iter().take_while(|&&si| si == c).count()
}

/// Check to see if parsing the float cannot possible overflow.
///
/// This allows major optimizations for those types, since we can skip checked
/// arithmetic. Adapted from the rust [corelib][`core`].
///
/// [`core`]: <https://doc.rust-lang.org/1.81.0/src/core/num/mod.rs.html#1389>
#[inline(always)]
pub fn cannot_overflow<T: Integer>(length: usize, radix: u32) -> bool {
    length <= T::overflow_digits(radix)
}
