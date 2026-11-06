//! Specialized iterator traits.
//!
//! The traits are for iterables containing bytes, and provide optimizations
//! which then can be used for contiguous or non-contiguous iterables,
//! including containers or iterators of any kind.

#![cfg(any(feature = "parse-floats", feature = "parse-integers"))]

use core::mem;

// Re-export our digit iterators.
#[cfg(not(feature = "format"))]
pub use crate::noskip::{AsBytes, Bytes};
#[cfg(feature = "format")]
pub use crate::skip::{AsBytes, Bytes};

/// A trait for working with iterables of bytes.
///
/// These iterators can either be contiguous or not contiguous and provide
/// methods for reading data and accessing underlying data. The readers
/// can either be contiguous or non-contiguous, although performance and
/// some API methods may not be available for both.
///
/// # Safety
///
/// Safe if [`set_cursor`] is set to an index <= [`buffer_length`], so no
/// out-of-bounds reads can occur. Also, [`get_buffer`] must return a slice of
/// initialized bytes. The caller must also ensure that any calls that increment
/// the cursor, such as [`step_by_unchecked`], [`step_unchecked`], and
/// [`peek_many_unchecked`] never exceed [`buffer_length`] as well.
///
/// [`set_cursor`]: `Iter::set_cursor`
/// [`buffer_length`]: `Iter::buffer_length`
/// [`get_buffer`]: `Iter::get_buffer`
/// [`step_by_unchecked`]: `Iter::step_by_unchecked`
/// [`step_unchecked`]: `Iter::step_unchecked`
/// [`peek_many_unchecked`]: `Iter::peek_many_unchecked`
pub unsafe trait Iter<'a> {
    /// Determine if the buffer is contiguous in memory.
    const IS_CONTIGUOUS: bool;

    // CURSORS
    // -------

    /// Get a ptr to the current start of the buffer.
    #[inline(always)]
    fn as_ptr(&self) -> *const u8 {
        self.as_slice().as_ptr()
    }

    /// Get a slice to the current start of the buffer.
    #[inline(always)]
    fn as_slice(&self) -> &'a [u8] {
        debug_assert!(self.cursor() <= self.buffer_length());
        // SAFETY: safe since index must be in range.
        unsafe { self.get_buffer().get_unchecked(self.cursor()..) }
    }

    /// Get a slice to the full underlying contiguous buffer,
    fn get_buffer(&self) -> &'a [u8];

    /// Get the total number of elements in the underlying buffer.
    #[inline(always)]
    fn buffer_length(&self) -> usize {
        self.get_buffer().len()
    }

    /// Get if no bytes are available in the buffer.
    ///
    /// This operators on the underlying buffer: that is,
    /// it returns if [`as_slice`] would return an empty slice.
    ///
    /// [`as_slice`]: Iter::as_slice
    #[inline(always)]
    fn is_buffer_empty(&self) -> bool {
        self.cursor() >= self.get_buffer().len()
    }

    /// Get the current index of the iterator in the slice.
    fn cursor(&self) -> usize;

    /// Set the current index of the iterator in the slice.
    ///
    /// This is **NOT** the current position of the iterator,
    /// since iterators may skip digits: this is the cursor
    /// in the underlying buffer. For example, if `slc[2]` is
    /// skipped, `set_cursor(3)` would be the 3rd element in
    /// the iterator, not the 4th.
    ///
    /// # Safety
    ///
    /// Safe if `index <= self.buffer_length()`. Although this
    /// won't affect safety, the caller also should be careful it
    /// does not set the cursor within skipped characters
    /// since this could affect correctness: an iterator that
    /// only accepts non-consecutive digit separators would
    /// pass if the cursor was set between the two.
    unsafe fn set_cursor(&mut self, index: usize);

    /// Get the current number of digits returned by the iterator.
    ///
    /// For contiguous iterators, this can include the sign character, decimal
    /// point, and the exponent sign (that is, it is always the cursor). For
    /// non-contiguous iterators, this must always be the only the number of
    /// digits returned.
    ///
    /// This is never used for indexing but will be used for API detection.
    fn current_count(&self) -> usize;

    // PROPERTIES

    /// Determine if the buffer is contiguous.
    #[inline(always)]
    fn is_contiguous(&self) -> bool {
        Self::IS_CONTIGUOUS
    }

    /// Get the next value available without consuming it.
    ///
    /// This does **NOT** skip digits, and directly fetches the item
    /// from the underlying buffer.
    #[inline(always)]
    fn first(&self) -> Option<&'a u8> {
        self.get_buffer().get(self.cursor())
    }

    /// Check if the next element is a given value.
    #[inline(always)]
    fn first_is_cased(&self, value: u8) -> bool {
        Some(&value) == self.first()
    }

    /// Check if the next element is a given value without case sensitivity.
    #[inline(always)]
    fn first_is_uncased(&self, value: u8) -> bool {
        if let Some(&c) = self.first() {
            c.eq_ignore_ascii_case(&value)
        } else {
            false
        }
    }

    /// Check if the next item in buffer is a given value with optional case
    /// sensitivity.
    #[inline(always)]
    fn first_is(&self, value: u8, is_cased: bool) -> bool {
        if is_cased {
            self.first_is_cased(value)
        } else {
            self.first_is_uncased(value)
        }
    }

    // STEP BY
    // -------

    /// Advance the internal slice by `N` elements.
    ///
    /// This does not advance the iterator by `N` elements for
    /// non-contiguous iterators: this just advances the internal,
    /// underlying buffer. This is useful for multi-digit optimizations
    /// for contiguous iterators.
    ///
    /// This does not increment the count of items: returns: this only
    /// increments the index, not the total digits returned. You must use
    /// this carefully: if stepping over a digit, you must then call
    /// [`increment_count`] afterwards or else the internal count will
    /// be incorrect.
    ///
    /// [`increment_count`]: DigitsIter::increment_count
    ///
    /// # Panics
    ///
    /// This will panic if the buffer advances for non-contiguous
    /// iterators if the current byte is a digit separator, or if the
    /// count is more than 1.
    ///
    /// # Safety
    ///
    /// As long as the iterator is at least `N` elements, this
    /// is safe.
    unsafe fn step_by_unchecked(&mut self, count: usize);

    /// Advance the internal slice by 1 element.
    ///
    ///
    /// This does not increment the count of items: returns: this only
    /// increments the index, not the total digits returned. You must
    /// use this carefully: if stepping over a digit, you must then call
    /// [`increment_count`] afterwards or else the internal count will
    /// be incorrect.
    ///
    /// [`increment_count`]: DigitsIter::increment_count
    ///
    /// # Panics
    ///
    /// This will panic if the buffer advances for non-contiguous
    /// iterators if the current byte is a digit separator.
    ///
    /// # Safety
    ///
    /// Safe as long as the iterator is not empty.
    #[inline(always)]
    unsafe fn step_unchecked(&mut self) {
        debug_assert!(!self.as_slice().is_empty());
        // SAFETY: safe if `self.index < self.buffer_length()`.
        unsafe { self.step_by_unchecked(1) };
    }

    // READ
    // ----

    /// Read a value of a difference type from the iterator.
    ///
    /// This does **not** advance the internal state of the iterator.
    /// This can only be implemented for contiguous iterators: non-
    /// contiguous iterators **MUST** panic.
    ///
    /// # Panics
    ///
    /// If the iterator is a non-contiguous iterator.
    ///
    /// # Safety
    ///
    /// Safe as long as the number of the buffer is contains as least as
    /// many bytes as the size of V. This must be unimplemented for
    /// non-contiguous iterators.
    #[inline(always)]
    unsafe fn peek_many_unchecked<V>(&self) -> V {
        unimplemented!();
    }

    /// Try to read a the next four bytes as a u32.
    ///
    /// This does not advance the internal state of the iterator.
    #[inline(always)]
    fn peek_u32(&self) -> Option<u32> {
        if Self::IS_CONTIGUOUS && self.as_slice().len() >= mem::size_of::<u32>() {
            // SAFETY: safe since we've guaranteed the buffer is greater than
            // the number of elements read. u32 is valid for all bit patterns
            unsafe { Some(self.peek_many_unchecked()) }
        } else {
            None
        }
    }

    /// Try to read the next eight bytes as a u64.
    ///
    /// This does not advance the internal state of the iterator.
    #[inline(always)]
    fn peek_u64(&self) -> Option<u64> {
        if Self::IS_CONTIGUOUS && self.as_slice().len() >= mem::size_of::<u64>() {
            // SAFETY: safe since we've guaranteed the buffer is greater than
            // the number of elements read. u64 is valid for all bit patterns
            unsafe { Some(self.peek_many_unchecked()) }
        } else {
            None
        }
    }
}

/// Iterator over a contiguous block of bytes.
///
/// This allows us to convert to-and-from-slices, raw pointers, and
/// peek/query the data from either end cheaply.
///
/// A default implementation is provided for slice iterators.
/// This trait **should never** return `null` from `as_ptr`, or be
/// implemented for non-contiguous data.
pub trait DigitsIter<'a>: Iterator<Item = &'a u8> + Iter<'a> {
    /// Get if the iterator cannot return any more elements.
    ///
    /// This may advance the internal iterator state, but not
    /// modify the next returned value.
    ///
    /// If this is an iterator, this is based on the number of items
    /// left to be returned. We do not necessarly know the length of
    /// the buffer. If this is a non-contiguous iterator, this **MUST**
    /// advance the state until it knows a value can be returned.
    ///
    /// Any incorrect implementations of this affect all safety invariants
    /// for the rest of the trait. For contiguous iterators, this can be
    /// as simple as checking if `self.cursor >= self.slc.len()`, but for
    /// non-contiguous iterators you **MUST** advance to the next element
    /// to be returned, then check to see if a value exists. The safest
    /// implementation is always to check if `self.peek().is_none()` and
    /// ensure [`peek`] is always safe.
    ///
    /// If you would like to see if the cursor is at the end of the buffer,
    /// see [`is_buffer_empty`] instead.
    ///
    /// [`is_buffer_empty`]: Iter::is_buffer_empty
    /// [`peek`]: DigitsIter::peek
    #[inline(always)]
    #[allow(clippy::wrong_self_convention)] // reason="required for peeking next item"
    fn is_consumed(&mut self) -> bool {
        self.peek().is_none()
    }

    /// Increment the number of digits that have been returned by the iterator.
    ///
    /// For contiguous iterators, this is a no-op. For non-contiguous iterators,
    /// this increments the count by 1.
    fn increment_count(&mut self);

    /// Peek the next value of the iterator, without consuming it.
    ///
    /// Note that this can modify the internal state, by skipping digits
    /// for iterators that find the first non-zero value, etc. We optimize
    /// this for the case where we have contiguous iterators, since
    /// non-contiguous iterators already have a major performance penalty.
    fn peek(&mut self) -> Option<Self::Item>;

    /// Peek the next value of the iterator, and step only if it exists.
    #[inline(always)]
    fn try_read(&mut self) -> Option<Self::Item> {
        if let Some(value) = self.peek() {
            // SAFETY: the slice cannot be empty because we peeked a value.
            unsafe { self.step_unchecked() };
            Some(value)
        } else {
            None
        }
    }

    /// Check if the next element is a given value.
    #[inline(always)]
    fn peek_is_cased(&mut self, value: u8) -> bool {
        Some(&value) == self.peek()
    }

    /// Check if the next element is a given value without case sensitivity.
    #[inline(always)]
    fn peek_is_uncased(&mut self, value: u8) -> bool {
        if let Some(&c) = self.peek() {
            c.eq_ignore_ascii_case(&value)
        } else {
            false
        }
    }

    /// Check if the next element is a given value with optional case
    /// sensitivity.
    #[inline(always)]
    fn peek_is(&mut self, value: u8, is_cased: bool) -> bool {
        if is_cased {
            self.peek_is_cased(value)
        } else {
            self.peek_is_uncased(value)
        }
    }

    /// Peek the next value and consume it if the read value matches the
    /// expected one.
    #[inline(always)]
    fn read_if<Pred: FnOnce(u8) -> bool>(&mut self, pred: Pred) -> Option<u8> {
        // NOTE: This was implemented to remove usage of unsafe throughout to code
        // base, however, performance was really not up to scratch. I'm not sure
        // the cause of this.
        if let Some(&peeked) = self.peek() {
            if pred(peeked) {
                // SAFETY: the slice cannot be empty because we peeked a value.
                unsafe { self.step_unchecked() };
                Some(peeked)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Read a value if the value matches the provided one.
    #[inline(always)]
    fn read_if_value_cased(&mut self, value: u8) -> Option<u8> {
        if self.peek() == Some(&value) {
            // SAFETY: the slice cannot be empty because we peeked a value.
            unsafe { self.step_unchecked() };
            Some(value)
        } else {
            None
        }
    }

    /// Read a value if the value matches the provided one without case
    /// sensitivity.
    #[inline(always)]
    fn read_if_value_uncased(&mut self, value: u8) -> Option<u8> {
        self.read_if(|x| x.eq_ignore_ascii_case(&value))
    }

    /// Read a value if the value matches the provided one.
    #[inline(always)]
    fn read_if_value(&mut self, value: u8, is_cased: bool) -> Option<u8> {
        if is_cased {
            self.read_if_value_cased(value)
        } else {
            self.read_if_value_uncased(value)
        }
    }

    /// Skip zeros from the start of the iterator
    #[inline(always)]
    fn skip_zeros(&mut self) -> usize {
        let start = self.current_count();
        while self.read_if_value_cased(b'0').is_some() {
            self.increment_count();
        }
        self.current_count() - start
    }

    /// Determine if the character is a digit.
    fn is_digit(&self, value: u8) -> bool;
}
