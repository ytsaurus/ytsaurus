//! An iterator over a slice.
//!
//! This iterator has both the length of the original slice, as
//! well as the current position of the iterator in the buffer.

#![cfg(all(any(feature = "parse-floats", feature = "parse-integers"), not(feature = "format")))]

use core::{mem, ptr};

use crate::digit::char_is_digit_const;
use crate::format::NumberFormat;
use crate::iterator::{DigitsIter, Iter};

// AS DIGITS
// ---------

/// Trait to simplify creation of a `Bytes` object.
pub trait AsBytes<'a> {
    /// Create `Bytes` from object.
    fn bytes<const __: u128>(&'a self) -> Bytes<'a, __>;
}

impl<'a> AsBytes<'a> for [u8] {
    #[inline(always)]
    fn bytes<const __: u128>(&'a self) -> Bytes<'a, __> {
        Bytes::new(self)
    }
}

// DIGITS
// ------

/// Slice iterator that stores the original length of the slice.
#[derive(Clone)]
pub struct Bytes<'a, const __: u128> {
    /// The raw slice for the iterator.
    slc: &'a [u8],
    /// Current index of the iterator in the slice.
    index: usize,
}

impl<'a, const __: u128> Bytes<'a, __> {
    /// Create new byte object.
    #[inline(always)]
    pub const fn new(slc: &'a [u8]) -> Self {
        Self {
            slc,
            index: 0,
        }
    }

    /// Initialize the slice from raw parts.
    ///
    /// # Safety
    ///
    /// This is safe if and only if the index is <= `slc.len()`.
    /// For this reason, since it's easy to get wrong, we only
    /// expose it to `DigitsIterator` and nothing else.
    #[inline(always)]
    #[allow(clippy::assertions_on_constants)] // reason="ensuring safety invariants are valid"
    const unsafe fn from_parts(slc: &'a [u8], index: usize) -> Self {
        debug_assert!(index <= slc.len());
        debug_assert!(Self::IS_CONTIGUOUS);
        Self {
            slc,
            index,
        }
    }

    /// Get iterator over integer digits.
    #[inline(always)]
    pub fn integer_iter<'b>(&'b mut self) -> DigitsIterator<'a, 'b, __> {
        DigitsIterator {
            byte: self,
        }
    }

    /// Get iterator over fraction digits.
    #[inline(always)]
    pub fn fraction_iter<'b>(&'b mut self) -> DigitsIterator<'a, 'b, __> {
        DigitsIterator {
            byte: self,
        }
    }

    /// Get iterator over exponent digits.
    #[inline(always)]
    pub fn exponent_iter<'b>(&'b mut self) -> DigitsIterator<'a, 'b, __> {
        DigitsIterator {
            byte: self,
        }
    }

    /// Get iterator over special floating point values.
    #[inline(always)]
    pub fn special_iter<'b>(&'b mut self) -> DigitsIterator<'a, 'b, __> {
        DigitsIterator {
            byte: self,
        }
    }
}

unsafe impl<'a, const __: u128> Iter<'a> for Bytes<'a, __> {
    const IS_CONTIGUOUS: bool = true;

    #[inline(always)]
    fn get_buffer(&self) -> &'a [u8] {
        self.slc
    }

    /// Get the current index of the iterator in the slice.
    #[inline(always)]
    fn cursor(&self) -> usize {
        self.index
    }

    /// Set the current index of the iterator in the slice.
    ///
    /// # Safety
    ///
    /// Safe if `index <= self.buffer_length()`.
    #[inline(always)]
    unsafe fn set_cursor(&mut self, index: usize) {
        debug_assert!(index <= self.buffer_length());
        self.index = index;
    }

    /// Get the current number of digits returned by the iterator.
    ///
    /// For contiguous iterators, this can include the sign character, decimal
    /// point, and the exponent sign (that is, it is always the cursor). For
    /// non-contiguous iterators, this must always be the only the number of
    /// digits returned.
    #[inline(always)]
    fn current_count(&self) -> usize {
        self.index
    }

    #[inline(always)]
    #[allow(clippy::assertions_on_constants)] // reason="ensuring safety invariants are valid"
    unsafe fn step_by_unchecked(&mut self, count: usize) {
        assert!(Self::IS_CONTIGUOUS);
        debug_assert!(self.as_slice().len() >= count);
        self.index += count;
    }

    #[inline(always)]
    #[allow(clippy::assertions_on_constants)] // reason="ensuring safety invariants are valid"
    unsafe fn peek_many_unchecked<V>(&self) -> V {
        debug_assert!(Self::IS_CONTIGUOUS);
        debug_assert!(self.as_slice().len() >= mem::size_of::<V>());

        // SAFETY: safe as long as the slice has at least count elements.
        unsafe { ptr::read_unaligned::<V>(self.as_ptr() as *const _) }
    }
}

// DIGITS ITERATOR
// ---------------

/// Slice iterator that stores the original length of the slice.
pub struct DigitsIterator<'a: 'b, 'b, const __: u128> {
    /// The internal byte object for the no-skip iterator.
    byte: &'b mut Bytes<'a, __>,
}

impl<'a: 'b, 'b, const __: u128> DigitsIterator<'a, 'b, __> {
    /// Create a new digits iterator from the bytes underlying item.
    #[inline(always)]
    pub fn new(byte: &'b mut Bytes<'a, __>) -> Self {
        Self {
            byte,
        }
    }

    /// Take the first N digits from the iterator.
    ///
    /// This only takes the digits if we have a contiguous iterator.
    /// It takes the digits, validating the bounds, and then advanced
    /// the iterators state.
    #[cfg_attr(not(feature = "compact"), inline(always))]
    #[allow(clippy::assertions_on_constants)] // reason="ensuring safety invariants are valid"
    pub fn take_n(&mut self, n: usize) -> Option<Bytes<'a, __>> {
        debug_assert!(Self::IS_CONTIGUOUS);
        let end = self.byte.slc.len().min(n + self.cursor());
        // NOTE: The compiler should be able to optimize this out.
        let slc: &[u8] = &self.byte.slc[..end];

        // SAFETY: Safe since we just ensured the underlying slice has that count
        // elements, so both the underlying slice for this and this **MUST**
        // have at least count elements. We do static checking on the bounds for this.
        unsafe {
            let byte: Bytes<'_, __> = Bytes::from_parts(slc, self.cursor());
            unsafe { self.set_cursor(end) };
            Some(byte)
        }
    }
}

unsafe impl<'a: 'b, 'b, const __: u128> Iter<'a> for DigitsIterator<'a, 'b, __> {
    const IS_CONTIGUOUS: bool = Bytes::<'a, __>::IS_CONTIGUOUS;

    #[inline(always)]
    fn get_buffer(&self) -> &'a [u8] {
        self.byte.get_buffer()
    }

    #[inline(always)]
    fn cursor(&self) -> usize {
        self.byte.cursor()
    }

    #[inline(always)]
    unsafe fn set_cursor(&mut self, index: usize) {
        debug_assert!(index <= self.buffer_length());
        // SAFETY: safe if `index <= self.buffer_length()`.
        unsafe { self.byte.set_cursor(index) };
    }

    #[inline(always)]
    fn current_count(&self) -> usize {
        self.byte.current_count()
    }

    #[inline(always)]
    unsafe fn step_by_unchecked(&mut self, count: usize) {
        debug_assert!(self.as_slice().len() >= count);
        // SAFETY: safe as long as `slc.len() >= count`.
        unsafe { self.byte.step_by_unchecked(count) }
    }

    #[inline(always)]
    unsafe fn peek_many_unchecked<V>(&self) -> V {
        debug_assert!(self.as_slice().len() >= mem::size_of::<V>());
        // SAFETY: safe as long as the slice has at least count elements.
        unsafe { self.byte.peek_many_unchecked() }
    }
}

impl<'a: 'b, 'b, const FORMAT: u128> DigitsIter<'a> for DigitsIterator<'a, 'b, FORMAT> {
    #[inline(always)]
    fn is_consumed(&mut self) -> bool {
        self.is_buffer_empty()
    }

    // Always a no-op
    #[inline(always)]
    fn increment_count(&mut self) {
    }

    #[inline(always)]
    fn peek(&mut self) -> Option<<Self as Iterator>::Item> {
        self.byte.slc.get(self.byte.index)
    }

    /// Determine if the character is a digit.
    #[inline(always)]
    fn is_digit(&self, value: u8) -> bool {
        let format = NumberFormat::<{ FORMAT }> {};
        char_is_digit_const(value, format.mantissa_radix())
    }
}

impl<'a: 'b, 'b, const __: u128> Iterator for DigitsIterator<'a, 'b, __> {
    type Item = &'a u8;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let value = self.byte.slc.get(self.byte.index)?;
        self.byte.index += 1;
        Some(value)
    }
}

impl<'a: 'b, 'b, const __: u128> ExactSizeIterator for DigitsIterator<'a, 'b, __> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.buffer_length() - self.cursor()
    }
}
