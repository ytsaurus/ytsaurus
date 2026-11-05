//! A simple big-integer type for slow path algorithms.
//!
//! This includes minimal stack vector for use in big-integer arithmetic.

#![doc(hidden)]

use core::{cmp, mem, ops, ptr, slice};

#[cfg(feature = "radix")]
use crate::float::ExtendedFloat80;
use crate::float::RawFloat;
use crate::limits::{u32_power_limit, u64_power_limit};
#[cfg(not(feature = "compact"))]
use crate::table::get_large_int_power;

/// Index an array without bounds checking.
///
/// # Safety
///
/// Safe if `index < array.len()`.
macro_rules! index_unchecked {
    ($x:ident[$i:expr]) => {
        // SAFETY: safe if `index < array.len()`.
        *$x.get_unchecked($i)
    };
}

// BIGINT
// ------

/// Number of bits in a Bigint.
///
/// This needs to be at least the number of bits required to store
/// a Bigint, which is `log2(radix**digits)`.
/// ≅ 5600 for base-36, rounded-up.
#[cfg(feature = "radix")]
const BIGINT_BITS: usize = 6000;

/// ≅ 3600 for base-10, rounded-up.
#[cfg(not(feature = "radix"))]
const BIGINT_BITS: usize = 4000;

/// The number of limbs for the bigint.
const BIGINT_LIMBS: usize = BIGINT_BITS / Limb::BITS as usize;

/// Storage for a big integer type.
///
/// This is used for algorithms when we have a finite number of digits.
/// Specifically, it stores all the significant digits scaled to the
/// proper exponent, as an integral type, and then directly compares
/// these digits.
///
/// This requires us to store the number of significant bits, plus the
/// number of exponent bits (required) since we scale everything
/// to the same exponent.
#[derive(Clone, PartialEq, Eq)]
pub struct Bigint {
    /// Significant digits for the float, stored in a big integer in LE order.
    ///
    /// This is pretty much the same number of digits for any radix, since the
    ///  significant digits balances out the zeros from the exponent:
    ///     1. Decimal is 1091 digits, 767 mantissa digits + 324 exponent zeros.
    ///     2. Base 6 is 1097 digits, or 680 mantissa digits + 417 exponent
    ///        zeros.
    ///     3. Base 36 is 1086 digits, or 877 mantissa digits + 209 exponent
    ///        zeros.
    ///
    /// However, the number of bytes required is larger for large radixes:
    /// for decimal, we need `log2(10**1091) ≅ 3600`, while for base 36
    /// we need `log2(36**1086) ≅ 5600`. Since we use uninitialized data,
    /// we avoid a major performance hit from the large buffer size.
    pub data: StackVec<BIGINT_LIMBS>,
}

impl Bigint {
    /// Construct a bigfloat representing 0.
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            data: StackVec::new(),
        }
    }

    /// Construct a bigfloat from an integer.
    #[inline(always)]
    pub fn from_u32(value: u32) -> Self {
        Self {
            data: StackVec::from_u32(value),
        }
    }

    /// Construct a bigfloat from an integer.
    #[inline(always)]
    pub fn from_u64(value: u64) -> Self {
        Self {
            data: StackVec::from_u64(value),
        }
    }

    #[inline(always)]
    pub fn hi64(&self) -> (u64, bool) {
        self.data.hi64()
    }

    /// Multiply and assign as if by exponentiation by a power.
    #[inline(always)]
    pub fn pow(&mut self, base: u32, exp: u32) -> Option<()> {
        let (odd, shift) = split_radix(base);
        if odd != 0 {
            pow::<BIGINT_LIMBS>(&mut self.data, odd, exp)?;
        }
        if shift != 0 {
            shl(&mut self.data, (exp * shift) as usize)?;
        }
        Some(())
    }

    /// Calculate the bit-length of the big-integer.
    #[inline(always)]
    pub fn bit_length(&self) -> u32 {
        bit_length(&self.data)
    }
}

impl ops::MulAssign<&Bigint> for Bigint {
    fn mul_assign(&mut self, rhs: &Bigint) {
        self.data *= &rhs.data;
    }
}

impl Default for Bigint {
    fn default() -> Self {
        Self::new()
    }
}

/// Number of bits in a Bigfloat.
///
/// This needs to be at least the number of bits required to store
/// a Bigint, which is `F::EXPONENT_BIAS + F::BITS`.
/// Bias ≅ 1075, with 64 extra for the digits.
#[cfg(feature = "radix")]
const BIGFLOAT_BITS: usize = 1200;

/// The number of limbs for the Bigfloat.
#[cfg(feature = "radix")]
const BIGFLOAT_LIMBS: usize = BIGFLOAT_BITS / Limb::BITS as usize;

/// Storage for a big floating-point type.
///
/// This is used for the algorithm with a non-finite digit count, which creates
/// a representation of `b+h` and the float scaled into the range `[1, radix)`.
#[cfg(feature = "radix")]
#[derive(Clone, PartialEq, Eq)]
pub struct Bigfloat {
    /// Significant digits for the float, stored in a big integer in LE order.
    ///
    /// This only needs ~1075 bits for the exponent, and ~64 more for the
    /// significant digits, since it's based on a theoretical representation
    /// of the halfway point. This means we can have a significantly smaller
    /// representation. The largest 64-bit exponent in magnitude is 2^1074,
    /// which will produce the same number of bits in any radix.
    pub data: StackVec<BIGFLOAT_LIMBS>,
    /// Binary exponent for the float type.
    pub exp: i32,
}

#[cfg(feature = "radix")]
impl Bigfloat {
    /// Construct a bigfloat representing 0.
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            data: StackVec::new(),
            exp: 0,
        }
    }

    /// Construct a bigfloat from an extended-precision float.
    #[inline(always)]
    pub fn from_float(fp: ExtendedFloat80) -> Self {
        Self {
            data: StackVec::from_u64(fp.mant),
            exp: fp.exp,
        }
    }

    /// Construct a bigfloat from an integer.
    #[inline(always)]
    pub fn from_u32(value: u32) -> Self {
        Self {
            data: StackVec::from_u32(value),
            exp: 0,
        }
    }

    /// Construct a bigfloat from an integer.
    #[inline(always)]
    pub fn from_u64(value: u64) -> Self {
        Self {
            data: StackVec::from_u64(value),
            exp: 0,
        }
    }

    /// Multiply and assign as if by exponentiation by a power.
    #[inline(always)]
    pub fn pow(&mut self, base: u32, exp: u32) -> Option<()> {
        let (odd, shift) = split_radix(base);
        if odd != 0 {
            pow::<BIGFLOAT_LIMBS>(&mut self.data, odd, exp)?;
        }
        if shift != 0 {
            self.exp += (exp * shift) as i32;
        }
        Some(())
    }

    /// Shift-left the entire buffer n bits, where bits is less than the limb
    /// size.
    #[inline(always)]
    pub fn shl_bits(&mut self, n: usize) -> Option<()> {
        shl_bits(&mut self.data, n)
    }

    /// Shift-left the entire buffer n limbs.
    #[inline(always)]
    pub fn shl_limbs(&mut self, n: usize) -> Option<()> {
        shl_limbs(&mut self.data, n)
    }

    /// Shift-left the entire buffer n bits.
    #[inline(always)]
    pub fn shl(&mut self, n: usize) -> Option<()> {
        shl(&mut self.data, n)
    }

    /// Get number of leading zero bits in the storage.
    /// Assumes the value is normalized.
    #[inline(always)]
    pub fn leading_zeros(&self) -> u32 {
        leading_zeros(&self.data)
    }
}

#[cfg(feature = "radix")]
impl ops::MulAssign<&Bigfloat> for Bigfloat {
    #[inline(always)]
    #[allow(clippy::suspicious_op_assign_impl)] // reason="intended increment"
    #[allow(clippy::unwrap_used)] // reason="exceeding the bounds is a developer error"
    fn mul_assign(&mut self, rhs: &Bigfloat) {
        large_mul(&mut self.data, &rhs.data).unwrap();
        self.exp += rhs.exp;
    }
}

#[cfg(feature = "radix")]
impl Default for Bigfloat {
    fn default() -> Self {
        Self::new()
    }
}

// VEC
// ---

/// Simple stack vector implementation.
#[derive(Clone)]
pub struct StackVec<const SIZE: usize> {
    /// The raw buffer for the elements.
    data: [mem::MaybeUninit<Limb>; SIZE],
    /// The number of elements in the array (we never need more than
    /// `u16::MAX`).
    length: u16,
}

/// Extract the hi bits from the buffer.
///
/// NOTE: Modifying this to remove unsafety which we statically
/// check directly in every caller leads to ~20% degradation in
/// performance.
/// - `rview`   - A reversed view over a slice.
/// - `fn`      - The callback to extract the high bits.
macro_rules! hi {
    (@1 $self:ident, $rview:ident, $t:ident, $fn:ident) => {{
        $fn(unsafe { index_unchecked!($rview[0]) as $t })
    }};

    // # Safety
    //
    // Safe as long as the `stackvec.len() >= 2`.
    (@2 $self:ident, $rview:ident, $t:ident, $fn:ident) => {{
        let r0 = unsafe { index_unchecked!($rview[0]) as $t };
        let r1 = unsafe { index_unchecked!($rview[1]) as $t };
        $fn(r0, r1)
    }};

    // # Safety
    //
    // Safe as long as the `stackvec.len() >= 2`.
    (@nonzero2 $self:ident, $rview:ident, $t:ident, $fn:ident) => {{
        let (v, n) = hi!(@2 $self, $rview, $t, $fn);
        (v, n || unsafe { nonzero($self, 2 ) })
    }};

    // # Safety
    //
    // Safe as long as the `stackvec.len() >= 3`.
    (@3 $self:ident, $rview:ident, $t:ident, $fn:ident) => {{
        let r0 = unsafe { index_unchecked!($rview[0]) as $t };
        let r1 = unsafe { index_unchecked!($rview[1]) as $t };
        let r2 = unsafe { index_unchecked!($rview[2]) as $t };
        $fn(r0, r1, r2)
    }};

    // # Safety
    //
    // Safe as long as the `stackvec.len() >= 3`.
    (@nonzero3 $self:ident, $rview:ident, $t:ident, $fn:ident) => {{
        let (v, n) = hi!(@3 $self, $rview, $t, $fn);
        (v, n || unsafe { nonzero($self, 3 ) })
    }};
}

impl<const SIZE: usize> StackVec<SIZE> {
    /// Construct an empty vector.
    #[must_use]
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            length: 0,
            data: [mem::MaybeUninit::uninit(); SIZE],
        }
    }

    /// Get a mutable ptr to the current start of the big integer.
    #[must_use]
    #[inline(always)]
    pub fn as_mut_ptr(&mut self) -> *mut Limb {
        self.data.as_mut_ptr().cast::<Limb>()
    }

    /// Get a ptr to the current start of the big integer.
    #[must_use]
    #[inline(always)]
    pub fn as_ptr(&self) -> *const Limb {
        self.data.as_ptr().cast::<Limb>()
    }

    /// Construct a vector from an existing slice.
    #[must_use]
    #[inline(always)]
    pub fn try_from(x: &[Limb]) -> Option<Self> {
        let mut vec = Self::new();
        vec.try_extend(x)?;
        Some(vec)
    }

    /// Sets the length of a vector.
    ///
    /// This will explicitly set the size of the vector, without actually
    /// modifying its buffers, so it is up to the caller to ensure that the
    /// vector is actually the specified size.
    ///
    /// # Safety
    ///
    /// Safe as long as `len` is less than `SIZE`.
    #[inline(always)]
    pub unsafe fn set_len(&mut self, len: usize) {
        debug_assert!(len <= u16::MAX as usize, "indexing must fit in 16 bits");
        debug_assert!(len <= SIZE, "cannot exceed our array bounds");
        self.length = len as u16;
    }

    /// Get the number of elements stored in the vector.
    #[must_use]
    #[inline(always)]
    pub const fn len(&self) -> usize {
        self.length as usize
    }

    /// If the vector is empty.
    #[must_use]
    #[inline(always)]
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The number of items the vector can hold.
    #[must_use]
    #[inline(always)]
    pub const fn capacity(&self) -> usize {
        SIZE
    }

    /// Append an item to the vector, without bounds checking.
    ///
    /// # Safety
    ///
    /// Safe if `self.len() < self.capacity()`.
    #[inline(always)]
    unsafe fn push_unchecked(&mut self, value: Limb) {
        debug_assert!(self.len() < self.capacity(), "cannot exceed our array bounds");
        // SAFETY: safe, capacity is less than the current size.
        unsafe {
            let len = self.len();
            let ptr = self.as_mut_ptr().add(len);
            ptr.write(value);
            self.length += 1;
        }
    }

    /// Append an item to the vector.
    #[inline(always)]
    pub fn try_push(&mut self, value: Limb) -> Option<()> {
        if self.len() < self.capacity() {
            // SAFETY: safe, capacity is less than the current size.
            unsafe { self.push_unchecked(value) };
            Some(())
        } else {
            None
        }
    }

    /// Remove an item from the end of a vector, without bounds checking.
    ///
    /// # Safety
    ///
    /// Safe if `self.len() > 0`.
    #[inline(always)]
    unsafe fn pop_unchecked(&mut self) -> Limb {
        debug_assert!(!self.is_empty(), "cannot pop a value if none exists");
        self.length -= 1;
        // SAFETY: safe if `self.length > 0`.
        // We have a trivial drop and copy, so this is safe.
        unsafe { ptr::read(self.as_mut_ptr().add(self.len())) }
    }

    /// Remove an item from the end of the vector and return it, or None if
    /// empty.
    #[inline(always)]
    pub fn pop(&mut self) -> Option<Limb> {
        if self.is_empty() {
            None
        } else {
            // SAFETY: safe, since `self.len() > 0`.
            unsafe { Some(self.pop_unchecked()) }
        }
    }

    /// Add items from a slice to the vector, without bounds checking.
    ///
    /// # Safety
    ///
    /// Safe if `self.len() + slc.len() <= self.capacity()`.
    #[inline(always)]
    unsafe fn extend_unchecked(&mut self, slc: &[Limb]) {
        let index = self.len();
        let new_len = index + slc.len();
        debug_assert!(self.len() + slc.len() <= self.capacity(), "cannot exceed our array bounds");
        let src = slc.as_ptr();
        // SAFETY: safe if `self.len() + slc.len() <= self.capacity()`.
        unsafe {
            let dst = self.as_mut_ptr().add(index);
            ptr::copy_nonoverlapping(src, dst, slc.len());
            self.set_len(new_len);
        }
    }

    /// Copy elements from a slice and append them to the vector.
    #[inline(always)]
    pub fn try_extend(&mut self, slc: &[Limb]) -> Option<()> {
        if self.len() + slc.len() <= self.capacity() {
            // SAFETY: safe, since `self.len() + slc.len() <= self.capacity()`.
            unsafe { self.extend_unchecked(slc) };
            Some(())
        } else {
            None
        }
    }

    /// Truncate vector to new length, dropping any items after `len`.
    ///
    /// # Safety
    ///
    /// Safe as long as `len <= self.capacity()`.
    unsafe fn truncate_unchecked(&mut self, len: usize) {
        debug_assert!(len <= self.capacity(), "cannot exceed our array bounds");
        self.length = len as u16;
    }

    /// Resize the buffer, without bounds checking.
    ///
    /// # Safety
    ///
    /// Safe as long as `len <= self.capacity()`.
    #[inline(always)]
    pub unsafe fn resize_unchecked(&mut self, len: usize, value: Limb) {
        debug_assert!(len <= self.capacity(), "cannot exceed our array bounds");
        let old_len = self.len();
        if len > old_len {
            // We have a trivial drop, so there's no worry here.
            // Just, don't set the length until all values have been written,
            // so we don't accidentally read uninitialized memory.

            let count = len - old_len;
            for index in 0..count {
                // SAFETY: safe if `len < self.capacity()`.
                unsafe {
                    let dst = self.as_mut_ptr().add(old_len + index);
                    ptr::write(dst, value);
                }
            }
            self.length = len as u16;
        } else {
            // SAFETY: safe since `len < self.len()`.
            unsafe { self.truncate_unchecked(len) };
        }
    }

    /// Try to resize the buffer.
    ///
    /// If the new length is smaller than the current length, truncate
    /// the input. If it's larger, then append elements to the buffer.
    #[inline(always)]
    pub fn try_resize(&mut self, len: usize, value: Limb) -> Option<()> {
        if len > self.capacity() {
            None
        } else {
            // SAFETY: safe, since `len <= self.capacity()`.
            unsafe { self.resize_unchecked(len, value) };
            Some(())
        }
    }

    // HI

    /// Get the high 16 bits from the vector.
    #[inline(always)]
    pub fn hi16(&self) -> (u16, bool) {
        let rview = self.rview();
        // SAFETY: the buffer must be at least length bytes long which we check on the
        // match.
        unsafe {
            match rview.len() {
                0 => (0, false),
                1 if Limb::BITS == 32 => hi!(@1 self, rview, u32, u32_to_hi16_1),
                1 => hi!(@1 self, rview, u64, u64_to_hi16_1),
                _ if Limb::BITS == 32 => hi!(@nonzero2 self, rview, u32, u32_to_hi16_2),
                _ => hi!(@nonzero2 self, rview, u64, u64_to_hi16_2),
            }
        }
    }

    /// Get the high 32 bits from the vector.
    #[inline(always)]
    pub fn hi32(&self) -> (u32, bool) {
        let rview = self.rview();
        // SAFETY: the buffer must be at least length bytes long which we check on the
        // match.
        unsafe {
            match rview.len() {
                0 => (0, false),
                1 if Limb::BITS == 32 => hi!(@1 self, rview, u32, u32_to_hi32_1),
                1 => hi!(@1 self, rview, u64, u64_to_hi32_1),
                _ if Limb::BITS == 32 => hi!(@nonzero2 self, rview, u32, u32_to_hi32_2),
                _ => hi!(@nonzero2 self, rview, u64, u64_to_hi32_2),
            }
        }
    }

    /// Get the high 64 bits from the vector.
    #[inline(always)]
    pub fn hi64(&self) -> (u64, bool) {
        let rview = self.rview();
        // SAFETY: the buffer must be at least length bytes long which we check on the
        // match.
        unsafe {
            match rview.len() {
                0 => (0, false),
                1 if Limb::BITS == 32 => hi!(@1 self, rview, u32, u32_to_hi64_1),
                1 => hi!(@1 self, rview, u64, u64_to_hi64_1),
                2 if Limb::BITS == 32 => hi!(@2 self, rview, u32, u32_to_hi64_2),
                2 => hi!(@2 self, rview, u64, u64_to_hi64_2),
                _ if Limb::BITS == 32 => hi!(@nonzero3 self, rview, u32, u32_to_hi64_3),
                _ => hi!(@nonzero2 self, rview, u64, u64_to_hi64_2),
            }
        }
    }

    // FROM

    /// Create `StackVec` from u16 value.
    #[must_use]
    #[inline(always)]
    pub fn from_u16(x: u16) -> Self {
        let mut vec = Self::new();
        assert!(1 <= vec.capacity(), "cannot exceed our array bounds");
        _ = vec.try_push(x as Limb);
        vec.normalize();
        vec
    }

    /// Create `StackVec` from u32 value.
    #[must_use]
    #[inline(always)]
    pub fn from_u32(x: u32) -> Self {
        let mut vec = Self::new();
        debug_assert!(1 <= vec.capacity(), "cannot exceed our array bounds");
        assert!(1 <= SIZE, "cannot exceed our array bounds");
        _ = vec.try_push(x as Limb);
        vec.normalize();
        vec
    }

    /// Create `StackVec` from u64 value.
    #[must_use]
    #[inline(always)]
    pub fn from_u64(x: u64) -> Self {
        let mut vec = Self::new();
        debug_assert!(2 <= vec.capacity(), "cannot exceed our array bounds");
        assert!(2 <= SIZE, "cannot exceed our array bounds");
        if Limb::BITS == 32 {
            _ = vec.try_push(x as Limb);
            _ = vec.try_push((x >> 32) as Limb);
        } else {
            _ = vec.try_push(x as Limb);
        }
        vec.normalize();
        vec
    }

    // INDEX

    /// Create a reverse view of the vector for indexing.
    #[must_use]
    #[inline(always)]
    pub fn rview(&self) -> ReverseView<'_, Limb> {
        ReverseView {
            inner: self,
        }
    }

    // MATH

    /// Normalize the integer, so any leading zero values are removed.
    #[inline(always)]
    pub fn normalize(&mut self) {
        // We don't care if this wraps: the index is bounds-checked.
        while let Some(&value) = self.get(self.len().wrapping_sub(1)) {
            if value == 0 {
                self.length -= 1;
            } else {
                break;
            }
        }
    }

    /// Get if the big integer is normalized.
    #[must_use]
    #[inline(always)]
    pub fn is_normalized(&self) -> bool {
        // We don't care if this wraps: the index is bounds-checked.
        self.get(self.len().wrapping_sub(1)) != Some(&0)
    }

    /// Calculate the fast quotient for a single limb-bit quotient.
    ///
    /// This requires a non-normalized divisor, where there at least
    /// `integral_binary_factor` 0 bits set, to ensure at maximum a single
    /// digit will be produced for a single base.
    ///
    /// Warning: This is not a general-purpose division algorithm,
    /// it is highly specialized for peeling off singular digits.
    #[inline(always)]
    #[cfg(feature = "radix")]
    pub fn quorem(&mut self, y: &Self) -> Limb {
        large_quorem(self, y)
    }

    /// `AddAssign` small integer.
    #[inline(always)]
    pub fn add_small(&mut self, y: Limb) -> Option<()> {
        small_add(self, y)
    }

    /// `MulAssign` small integer.
    #[inline(always)]
    pub fn mul_small(&mut self, y: Limb) -> Option<()> {
        small_mul(self, y)
    }
}

impl<const SIZE: usize> PartialEq for StackVec<SIZE> {
    #[inline(always)]
    #[allow(clippy::op_ref)] // reason="need to convert to slice for equality"
    fn eq(&self, other: &Self) -> bool {
        use core::ops::Deref;
        self.len() == other.len() && self.deref() == other.deref()
    }
}

impl<const SIZE: usize> Eq for StackVec<SIZE> {
}

impl<const SIZE: usize> cmp::PartialOrd for StackVec<SIZE> {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<const SIZE: usize> cmp::Ord for StackVec<SIZE> {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        compare(self, other)
    }
}

impl<const SIZE: usize> ops::Deref for StackVec<SIZE> {
    type Target = [Limb];
    #[inline(always)]
    fn deref(&self) -> &[Limb] {
        debug_assert!(self.len() <= self.capacity(), "cannot exceed our array bounds");
        // SAFETY: safe since `self.data[..self.len()]` must be initialized
        // and `self.len() <= self.capacity()`.
        unsafe {
            let ptr = self.data.as_ptr() as *const Limb;
            slice::from_raw_parts(ptr, self.len())
        }
    }
}

impl<const SIZE: usize> ops::DerefMut for StackVec<SIZE> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut [Limb] {
        debug_assert!(self.len() <= self.capacity(), "cannot exceed our array bounds");
        // SAFETY: safe since `self.data[..self.len()]` must be initialized
        // and `self.len() <= self.capacity()`.
        unsafe {
            let ptr = self.data.as_mut_ptr() as *mut Limb;
            slice::from_raw_parts_mut(ptr, self.len())
        }
    }
}

impl<const SIZE: usize> ops::MulAssign<&[Limb]> for StackVec<SIZE> {
    #[inline(always)]
    #[allow(clippy::unwrap_used)] // reason="exceeding the bounds is a developer error"
    fn mul_assign(&mut self, rhs: &[Limb]) {
        large_mul(self, rhs).unwrap();
    }
}

impl<const SIZE: usize> Default for StackVec<SIZE> {
    fn default() -> Self {
        Self::new()
    }
}

// REVERSE VIEW

/// Reverse, immutable view of a sequence.
pub struct ReverseView<'a, T: 'a> {
    inner: &'a [T],
}

impl<'a, T: 'a> ReverseView<'a, T> {
    /// Get a reference to a value, without bounds checking.
    ///
    /// # Safety
    ///
    /// Safe if forward indexing would be safe for the type,
    /// or `index < self.inner.len()`.
    #[inline(always)]
    pub unsafe fn get_unchecked(&self, index: usize) -> &T {
        debug_assert!(index < self.inner.len(), "cannot exceed our array bounds");
        let len = self.inner.len();
        // SAFETY: Safe as long as the index < length, so len - index - 1 >= 0 and <=
        // len.
        unsafe { self.inner.get_unchecked(len - index - 1) }
    }

    /// Get a reference to a value.
    #[inline(always)]
    pub fn get(&self, index: usize) -> Option<&T> {
        let len = self.inner.len();
        // We don't care if this wraps: the index is bounds-checked.
        self.inner.get(len.wrapping_sub(index + 1))
    }

    /// Get the length of the inner buffer.
    #[inline(always)]
    pub const fn len(&self) -> usize {
        self.inner.len()
    }

    /// If the vector is empty.
    #[inline(always)]
    pub const fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<T> ops::Index<usize> for ReverseView<'_, T> {
    type Output = T;

    #[inline(always)]
    fn index(&self, index: usize) -> &T {
        let len = self.inner.len();
        &(*self.inner)[len - index - 1]
    }
}

// HI
// --

/// Check if any of the remaining bits are non-zero.
///
/// # Safety
///
/// Safe as long as `rindex <= x.len()`. This is only called
/// where the type size is directly from the caller, and removing
/// it leads to a ~20% degradation in performance.
#[must_use]
#[inline(always)]
pub unsafe fn nonzero(x: &[Limb], rindex: usize) -> bool {
    debug_assert!(rindex <= x.len(), "cannot exceed our array bounds");
    let len = x.len();
    // SAFETY: safe if `rindex < x.len()`, since then `x.len() - rindex < x.len()`.
    let slc = unsafe { &index_unchecked!(x[..len - rindex]) };
    slc.iter().rev().any(|&x| x != 0)
}

// These return the high X bits and if the bits were truncated.

/// Shift 32-bit integer to high 16-bits.
#[must_use]
#[inline(always)]
pub const fn u32_to_hi16_1(r0: u32) -> (u16, bool) {
    let r0 = u32_to_hi32_1(r0).0;
    ((r0 >> 16) as u16, r0 as u16 != 0)
}

/// Shift 2 32-bit integers to high 16-bits.
#[must_use]
#[inline(always)]
pub const fn u32_to_hi16_2(r0: u32, r1: u32) -> (u16, bool) {
    let (r0, n) = u32_to_hi32_2(r0, r1);
    ((r0 >> 16) as u16, n || r0 as u16 != 0)
}

/// Shift 32-bit integer to high 32-bits.
#[must_use]
#[inline(always)]
pub const fn u32_to_hi32_1(r0: u32) -> (u32, bool) {
    let ls = r0.leading_zeros();
    (r0 << ls, false)
}

/// Shift 2 32-bit integers to high 32-bits.
#[must_use]
#[inline(always)]
pub const fn u32_to_hi32_2(r0: u32, r1: u32) -> (u32, bool) {
    let ls = r0.leading_zeros();
    let rs = 32 - ls;
    let v = match ls {
        0 => r0,
        _ => (r0 << ls) | (r1 >> rs),
    };
    let n = r1 << ls != 0;
    (v, n)
}

/// Shift 32-bit integer to high 64-bits.
#[must_use]
#[inline(always)]
pub const fn u32_to_hi64_1(r0: u32) -> (u64, bool) {
    u64_to_hi64_1(r0 as u64)
}

/// Shift 2 32-bit integers to high 64-bits.
#[must_use]
#[inline(always)]
pub const fn u32_to_hi64_2(r0: u32, r1: u32) -> (u64, bool) {
    let r0 = (r0 as u64) << 32;
    let r1 = r1 as u64;
    u64_to_hi64_1(r0 | r1)
}

/// Shift 3 32-bit integers to high 64-bits.
#[must_use]
#[inline(always)]
pub const fn u32_to_hi64_3(r0: u32, r1: u32, r2: u32) -> (u64, bool) {
    let r0 = r0 as u64;
    let r1 = (r1 as u64) << 32;
    let r2 = r2 as u64;
    u64_to_hi64_2(r0, r1 | r2)
}

/// Shift 64-bit integer to high 16-bits.
#[must_use]
#[inline(always)]
pub const fn u64_to_hi16_1(r0: u64) -> (u16, bool) {
    let r0 = u64_to_hi64_1(r0).0;
    ((r0 >> 48) as u16, r0 as u16 != 0)
}

/// Shift 2 64-bit integers to high 16-bits.
#[must_use]
#[inline(always)]
pub const fn u64_to_hi16_2(r0: u64, r1: u64) -> (u16, bool) {
    let (r0, n) = u64_to_hi64_2(r0, r1);
    ((r0 >> 48) as u16, n || r0 as u16 != 0)
}

/// Shift 64-bit integer to high 32-bits.
#[must_use]
#[inline(always)]
pub const fn u64_to_hi32_1(r0: u64) -> (u32, bool) {
    let r0 = u64_to_hi64_1(r0).0;
    ((r0 >> 32) as u32, r0 as u32 != 0)
}

/// Shift 2 64-bit integers to high 32-bits.
#[must_use]
#[inline(always)]
pub const fn u64_to_hi32_2(r0: u64, r1: u64) -> (u32, bool) {
    let (r0, n) = u64_to_hi64_2(r0, r1);
    ((r0 >> 32) as u32, n || r0 as u32 != 0)
}

/// Shift 64-bit integer to high 64-bits.
#[must_use]
#[inline(always)]
pub const fn u64_to_hi64_1(r0: u64) -> (u64, bool) {
    let ls = r0.leading_zeros();
    (r0 << ls, false)
}

/// Shift 2 64-bit integers to high 64-bits.
#[must_use]
#[inline(always)]
pub const fn u64_to_hi64_2(r0: u64, r1: u64) -> (u64, bool) {
    let ls = r0.leading_zeros();
    let rs = 64 - ls;
    let v = match ls {
        0 => r0,
        _ => (r0 << ls) | (r1 >> rs),
    };
    let n = r1 << ls != 0;
    (v, n)
}

// POWERS
// ------

/// MulAssign by a power.
///
/// Theoretically...
///
/// Use an exponentiation by squaring method, since it reduces the time
/// complexity of the multiplication to ~`O(log(n))` for the squaring,
/// and `O(n*m)` for the result. Since `m` is typically a lower-order
/// factor, this significantly reduces the number of multiplications
/// we need to do. Iteratively multiplying by small powers follows
/// the nth triangular number series, which scales as `O(p^2)`, but
/// where `p` is `n+m`. In short, it scales very poorly.
///
/// Practically....
///
/// Exponentiation by Squaring:
///     running 2 tests
///     test bigcomp_f32_lexical ... bench:       1,018 ns/iter (+/- 78)
///     test bigcomp_f64_lexical ... bench:       3,639 ns/iter (+/- 1,007)
///
/// Exponentiation by Iterative Small Powers:
///     running 2 tests
///     test bigcomp_f32_lexical ... bench:         518 ns/iter (+/- 31)
///     test bigcomp_f64_lexical ... bench:         583 ns/iter (+/- 47)
///
/// Exponentiation by Iterative Large Powers (of 2):
///     running 2 tests
///     test bigcomp_f32_lexical ... bench:         671 ns/iter (+/- 31)
///     test bigcomp_f64_lexical ... bench:       1,394 ns/iter (+/- 47)
///
/// The following benchmarks were run on `1 * 5^300`, using native `pow`,
/// a version with only small powers, and one with pre-computed powers
/// of `5^(3 * max_exp)`, rather than `5^(5 * max_exp)`.
///
/// However, using large powers is crucial for good performance for higher
/// powers.
///     pow/default             time:   [426.20 ns 427.96 ns 429.89 ns]
///     pow/small               time:   [2.9270 us 2.9411 us 2.9565 us]
///     pow/large:3             time:   [838.51 ns 842.21 ns 846.27 ns]
///
/// Even using worst-case scenarios, exponentiation by squaring is
/// significantly slower for our workloads. Just multiply by small powers,
/// in simple cases, and use pre-calculated large powers in other cases.
///
/// Furthermore, using sufficiently big large powers is also crucial for
/// performance. This is a trade-off of binary size and performance, and
/// using a single value at ~`5^(5 * max_exp)` seems optimal.
#[allow(clippy::doc_markdown)] // reason="not attempted to be referencing items"
#[allow(clippy::missing_inline_in_public_items)] // reason="only public for testing"
pub fn pow<const SIZE: usize>(x: &mut StackVec<SIZE>, base: u32, mut exp: u32) -> Option<()> {
    // Minimize the number of iterations for large exponents: just
    // do a few steps with a large powers.
    #[cfg(not(feature = "compact"))]
    {
        let (large, step) = get_large_int_power(base);
        while exp >= step {
            large_mul(x, large)?;
            exp -= step;
        }
    }

    // Now use our pre-computed small powers iteratively.
    let small_step = if Limb::BITS == 32 {
        u32_power_limit(base)
    } else {
        u64_power_limit(base)
    };
    let max_native = (base as Limb).pow(small_step);
    while exp >= small_step {
        small_mul(x, max_native)?;
        exp -= small_step;
    }
    if exp != 0 {
        let small_power = f64::int_pow_fast_path(exp as usize, base);
        small_mul(x, small_power as Limb)?;
    }
    Some(())
}

// SCALAR
// ------

/// Add two small integers and return the resulting value and if overflow
/// happens.
#[must_use]
#[inline(always)]
pub const fn scalar_add(x: Limb, y: Limb) -> (Limb, bool) {
    x.overflowing_add(y)
}

/// Multiply two small integers (with carry) (and return the overflow
/// contribution).
///
/// Returns the (low, high) components.
#[must_use]
#[inline(always)]
pub const fn scalar_mul(x: Limb, y: Limb, carry: Limb) -> (Limb, Limb) {
    // Cannot overflow, as long as wide is 2x as wide. This is because
    // the following is always true:
    // `Wide::MAX - (Narrow::MAX * Narrow::MAX) >= Narrow::MAX`
    let z: Wide = (x as Wide) * (y as Wide) + (carry as Wide);
    (z as Limb, (z >> Limb::BITS) as Limb)
}

// SMALL
// -----

/// Add small integer to bigint starting from offset.
#[inline(always)]
pub fn small_add_from<const SIZE: usize>(
    x: &mut StackVec<SIZE>,
    y: Limb,
    start: usize,
) -> Option<()> {
    let mut index = start;
    let mut carry = y;
    while carry != 0 && index < x.len() {
        // NOTE: Don't need unsafety because the compiler will optimize it out.
        let result = scalar_add(x[index], carry);
        x[index] = result.0;
        carry = result.1 as Limb;
        index += 1;
    }
    // If we carried past all the elements, add to the end of the buffer.
    if carry != 0 {
        x.try_push(carry)?;
    }
    Some(())
}

/// Add small integer to bigint.
#[inline(always)]
pub fn small_add<const SIZE: usize>(x: &mut StackVec<SIZE>, y: Limb) -> Option<()> {
    small_add_from(x, y, 0)
}

/// Multiply bigint by small integer.
#[inline(always)]
pub fn small_mul<const SIZE: usize>(x: &mut StackVec<SIZE>, y: Limb) -> Option<()> {
    let mut carry = 0;
    for xi in x.iter_mut() {
        let result = scalar_mul(*xi, y, carry);
        *xi = result.0;
        carry = result.1;
    }
    // If we carried past all the elements, add to the end of the buffer.
    if carry != 0 {
        x.try_push(carry)?;
    }
    Some(())
}

// LARGE
// -----

/// Add bigint to bigint starting from offset.
#[allow(clippy::missing_inline_in_public_items)] // reason="only public for testing"
pub fn large_add_from<const SIZE: usize>(
    x: &mut StackVec<SIZE>,
    y: &[Limb],
    start: usize,
) -> Option<()> {
    // The effective `x` buffer is from `xstart..x.len()`, so we need to treat
    // that as the current range. If the effective `y` buffer is longer, need
    // to resize to that, + the start index.
    if y.len() > x.len().saturating_sub(start) {
        // Ensure we panic if we can't extend the buffer.
        // This avoids any unsafe behavior afterwards.
        x.try_resize(y.len() + start, 0)?;
    }

    // Iteratively add elements from `y` to `x`.
    let mut carry = false;
    for index in 0..y.len() {
        let xi = &mut x[start + index];
        let yi = y[index];

        // Only one op of the two ops can overflow, since we added at max
        // `Limb::max_value() + Limb::max_value()`. Add the previous carry,
        // and store the current carry for the next.
        let result = scalar_add(*xi, yi);
        *xi = result.0;
        let mut tmp = result.1;
        if carry {
            let result = scalar_add(*xi, 1);
            *xi = result.0;
            tmp |= result.1;
        }
        carry = tmp;
    }

    // Handle overflow.
    if carry {
        small_add_from(x, 1, y.len() + start)?;
    }
    Some(())
}

/// Add bigint to bigint.
#[inline(always)]
pub fn large_add<const SIZE: usize>(x: &mut StackVec<SIZE>, y: &[Limb]) -> Option<()> {
    large_add_from(x, y, 0)
}

/// Grade-school multiplication algorithm.
///
/// Slow, naive algorithm, using limb-bit bases and just shifting left for
/// each iteration. This could be optimized with numerous other algorithms,
/// but it's extremely simple, and works in O(n*m) time, which is fine
/// by me. Each iteration, of which there are `m` iterations, requires
/// `n` multiplications, and `n` additions, or grade-school multiplication.
///
/// Don't use Karatsuba multiplication, since out implementation seems to
/// be slower asymptotically, which is likely just due to the small sizes
/// we deal with here. For example, running on the following data:
///
/// ```text
/// const SMALL_X: &[u32] = &[
///     766857581, 3588187092, 1583923090, 2204542082, 1564708913, 2695310100, 3676050286,
///     1022770393, 468044626, 446028186
/// ];
/// const SMALL_Y: &[u32] = &[
///     3945492125, 3250752032, 1282554898, 1708742809, 1131807209, 3171663979, 1353276095,
///     1678845844, 2373924447, 3640713171
/// ];
/// const LARGE_X: &[u32] = &[
///     3647536243, 2836434412, 2154401029, 1297917894, 137240595, 790694805, 2260404854,
///     3872698172, 690585094, 99641546, 3510774932, 1672049983, 2313458559, 2017623719,
///     638180197, 1140936565, 1787190494, 1797420655, 14113450, 2350476485, 3052941684,
///     1993594787, 2901001571, 4156930025, 1248016552, 848099908, 2660577483, 4030871206,
///     692169593, 2835966319, 1781364505, 4266390061, 1813581655, 4210899844, 2137005290,
///     2346701569, 3715571980, 3386325356, 1251725092, 2267270902, 474686922, 2712200426,
///     197581715, 3087636290, 1379224439, 1258285015, 3230794403, 2759309199, 1494932094,
///     326310242
/// ];
/// const LARGE_Y: &[u32] = &[
///     1574249566, 868970575, 76716509, 3198027972, 1541766986, 1095120699, 3891610505,
///     2322545818, 1677345138, 865101357, 2650232883, 2831881215, 3985005565, 2294283760,
///     3468161605, 393539559, 3665153349, 1494067812, 106699483, 2596454134, 797235106,
///     705031740, 1209732933, 2732145769, 4122429072, 141002534, 790195010, 4014829800,
///     1303930792, 3649568494, 308065964, 1233648836, 2807326116, 79326486, 1262500691,
///     621809229, 2258109428, 3819258501, 171115668, 1139491184, 2979680603, 1333372297,
///     1657496603, 2790845317, 4090236532, 4220374789, 601876604, 1828177209, 2372228171,
///     2247372529
/// ];
/// ```
///
/// We get the following results:
///
/// ```text
/// mul/small:long          time:   [220.23 ns 221.47 ns 222.81 ns]
/// Found 4 outliers among 100 measurements (4.00%)
///   2 (2.00%) high mild
///   2 (2.00%) high severe
/// mul/small:karatsuba     time:   [233.88 ns 234.63 ns 235.44 ns]
/// Found 11 outliers among 100 measurements (11.00%)
///   8 (8.00%) high mild
///   3 (3.00%) high severe
/// mul/large:long          time:   [1.9365 us 1.9455 us 1.9558 us]
/// Found 12 outliers among 100 measurements (12.00%)
///   7 (7.00%) high mild
///   5 (5.00%) high severe
/// mul/large:karatsuba     time:   [4.4250 us 4.4515 us 4.4812 us]
/// ```
///
/// In short, Karatsuba multiplication is never worthwhile for out use-case.
#[must_use]
#[allow(clippy::needless_range_loop)] // reason="required for performance, see benches"
#[allow(clippy::missing_inline_in_public_items)] // reason="only public for testing"
pub fn long_mul<const SIZE: usize>(x: &[Limb], y: &[Limb]) -> Option<StackVec<SIZE>> {
    // Using the immutable value, multiply by all the scalars in y, using
    // the algorithm defined above. Use a single buffer to avoid
    // frequent reallocations. Handle the first case to avoid a redundant
    // addition, since we know y.len() >= 1.
    let mut z = StackVec::<SIZE>::try_from(x)?;
    if let Some(&y0) = y.first() {
        small_mul(&mut z, y0)?;

        // NOTE: Don't use enumerate/skip since it's slow.
        for index in 1..y.len() {
            let yi = y[index];
            if yi != 0 {
                let mut zi = StackVec::<SIZE>::try_from(x)?;
                small_mul(&mut zi, yi)?;
                large_add_from(&mut z, &zi, index)?;
            }
        }
    }

    z.normalize();
    Some(z)
}

/// Multiply bigint by bigint using grade-school multiplication algorithm.
#[inline(always)]
pub fn large_mul<const SIZE: usize>(x: &mut StackVec<SIZE>, y: &[Limb]) -> Option<()> {
    // Karatsuba multiplication never makes sense, so just use grade school
    // multiplication.
    if y.len() == 1 {
        // SAFETY: safe since `y.len() == 1`.
        // NOTE: The compiler does not seem to optimize this out correctly.
        small_mul(x, unsafe { index_unchecked!(y[0]) })?;
    } else {
        *x = long_mul(y, x)?;
    }
    Some(())
}

/// Emit a single digit for the quotient and store the remainder in-place.
///
/// An extremely efficient division algorithm for small quotients, requiring
/// you to know the full range of the quotient prior to use. For example,
/// with a quotient that can range from [0, 10), you must have 4 leading
/// zeros in the divisor, so we can use a single-limb division to get
/// an accurate estimate of the quotient. Since we always underestimate
/// the quotient, we can add 1 and then emit the digit.
///
/// Requires a non-normalized denominator, with at least [1-6] leading
/// zeros, depending on the base (for example, 1 for base2, 6 for base36).
///
/// Adapted from David M. Gay's dtoa, and therefore under an MIT license:
///     www.netlib.org/fp/dtoa.c
#[cfg(feature = "radix")]
#[allow(clippy::many_single_char_names)] // reason = "mathematical names of variables"
pub fn large_quorem<const SIZE: usize>(x: &mut StackVec<SIZE>, y: &[Limb]) -> Limb {
    // If we have an empty divisor, error out early.
    assert!(!y.is_empty(), "large_quorem:: division by zero error.");
    assert!(x.len() <= y.len(), "large_quorem:: oversized numerator.");
    let mask = Limb::MAX as Wide;

    // Numerator is smaller the denominator, quotient always 0.
    if x.len() < y.len() {
        return 0;
    }

    // Calculate our initial estimate for q.
    let xm_1 = x[x.len() - 1];
    let yn_1 = y[y.len() - 1];
    let mut q = xm_1 / (yn_1 + 1);

    // Need to calculate the remainder if we don't have a 0 quotient.
    if q != 0 {
        let mut borrow: Wide = 0;
        let mut carry: Wide = 0;
        for j in 0..x.len() {
            let yj = y[j] as Wide;
            let p = yj * q as Wide + carry;
            carry = p >> Limb::BITS;
            let xj = x[j] as Wide;
            let t = xj.wrapping_sub(p & mask).wrapping_sub(borrow);
            borrow = (t >> Limb::BITS) & 1;
            x[j] = t as Limb;
        }
        x.normalize();
    }

    // Check if we under-estimated x.
    if compare(x, y) != cmp::Ordering::Less {
        q += 1;
        let mut borrow: Wide = 0;
        let mut carry: Wide = 0;
        for j in 0..x.len() {
            let yj = y[j] as Wide;
            let p = yj + carry;
            carry = p >> Limb::BITS;
            let xj = x[j] as Wide;
            let t = xj.wrapping_sub(p & mask).wrapping_sub(borrow);
            borrow = (t >> Limb::BITS) & 1;
            x[j] = t as Limb;
        }
        x.normalize();
    }

    q
}

// COMPARE
// -------

/// Compare `x` to `y`, in little-endian order.
#[must_use]
#[inline(always)]
pub fn compare(x: &[Limb], y: &[Limb]) -> cmp::Ordering {
    match x.len().cmp(&y.len()) {
        cmp::Ordering::Equal => {
            let iter = x.iter().rev().zip(y.iter().rev());
            for (&xi, yi) in iter {
                match xi.cmp(yi) {
                    cmp::Ordering::Equal => (),
                    ord => return ord,
                }
            }
            // Equal case.
            cmp::Ordering::Equal
        },
        ord => ord,
    }
}

// SHIFT
// -----

/// Shift-left `n` bits inside a buffer.
#[inline(always)]
pub fn shl_bits<const SIZE: usize>(x: &mut StackVec<SIZE>, n: usize) -> Option<()> {
    debug_assert!(n != 0, "cannot shift left by 0 bits");

    // Internally, for each item, we shift left by n, and add the previous
    // right shifted limb-bits.
    // For example, we transform (for u8) shifted left 2, to:
    //      b10100100 b01000010
    //      b10 b10010001 b00001000
    debug_assert!(n < Limb::BITS as usize, "cannot shift left more bits than in our limb");
    let rshift = Limb::BITS as usize - n;
    let lshift = n;
    let mut prev: Limb = 0;
    for xi in x.iter_mut() {
        let tmp = *xi;
        *xi <<= lshift;
        *xi |= prev >> rshift;
        prev = tmp;
    }

    // Always push the carry, even if it creates a non-normal result.
    let carry = prev >> rshift;
    if carry != 0 {
        x.try_push(carry)?;
    }

    Some(())
}

/// Shift-left `n` limbs inside a buffer.
#[inline(always)]
pub fn shl_limbs<const SIZE: usize>(x: &mut StackVec<SIZE>, n: usize) -> Option<()> {
    debug_assert!(n != 0, "cannot shift left by 0 bits");
    if n + x.len() > x.capacity() {
        None
    } else if !x.is_empty() {
        let len = n + x.len();
        let x_len = x.len();
        let ptr = x.as_mut_ptr();
        let src = ptr;
        // SAFETY: since x is not empty, and `x.len() + n <= x.capacity()`.
        unsafe {
            // Move the elements.
            let dst = ptr.add(n);
            ptr::copy(src, dst, x_len);
            // Write our 0s.
            ptr::write_bytes(ptr, 0, n);
            x.set_len(len);
        }
        Some(())
    } else {
        Some(())
    }
}

/// Shift-left buffer by n bits.
#[must_use]
#[inline(always)]
pub fn shl<const SIZE: usize>(x: &mut StackVec<SIZE>, n: usize) -> Option<()> {
    let rem = n % Limb::BITS as usize;
    let div = n / Limb::BITS as usize;
    if rem != 0 {
        shl_bits(x, rem)?;
    }
    if div != 0 {
        shl_limbs(x, div)?;
    }
    Some(())
}

/// Get number of leading zero bits in the storage.
#[must_use]
#[inline(always)]
pub fn leading_zeros(x: &[Limb]) -> u32 {
    let length = x.len();
    // `wrapping_sub` is fine, since it'll just return None.
    if let Some(&value) = x.get(length.wrapping_sub(1)) {
        value.leading_zeros()
    } else {
        0
    }
}

/// Calculate the bit-length of the big-integer.
#[must_use]
#[inline(always)]
pub fn bit_length(x: &[Limb]) -> u32 {
    let nlz = leading_zeros(x);
    Limb::BITS * x.len() as u32 - nlz
}

// RADIX
// -----

/// Get the base, odd radix, and the power-of-two for the type.
#[must_use]
#[inline(always)]
#[cfg(feature = "radix")]
pub const fn split_radix(radix: u32) -> (u32, u32) {
    match radix {
        2 => (0, 1),
        3 => (3, 0),
        4 => (0, 2),
        5 => (5, 0),
        6 => (3, 1),
        7 => (7, 0),
        8 => (0, 3),
        9 => (9, 0),
        10 => (5, 1),
        11 => (11, 0),
        12 => (6, 1),
        13 => (13, 0),
        14 => (7, 1),
        15 => (15, 0),
        16 => (0, 4),
        17 => (17, 0),
        18 => (9, 1),
        19 => (19, 0),
        20 => (5, 2),
        21 => (21, 0),
        22 => (11, 1),
        23 => (23, 0),
        24 => (3, 3),
        25 => (25, 0),
        26 => (13, 1),
        27 => (27, 0),
        28 => (7, 2),
        29 => (29, 0),
        30 => (15, 1),
        31 => (31, 0),
        32 => (0, 5),
        33 => (33, 0),
        34 => (17, 1),
        35 => (35, 0),
        36 => (9, 2),
        // Any other radix should be unreachable.
        _ => (0, 0),
    }
}

/// Get the base, odd radix, and the power-of-two for the type.
#[must_use]
#[inline(always)]
#[cfg(all(feature = "power-of-two", not(feature = "radix")))]
pub const fn split_radix(radix: u32) -> (u32, u32) {
    match radix {
        // Is also needed for decimal floats, due to `negative_digit_comp`.
        2 => (0, 1),
        4 => (0, 2),
        // Is also needed for decimal floats, due to `negative_digit_comp`.
        5 => (5, 0),
        8 => (0, 3),
        10 => (5, 1),
        16 => (0, 4),
        32 => (0, 5),
        // Any other radix should be unreachable.
        _ => (0, 0),
    }
}

/// Get the base, odd radix, and the power-of-two for the type.
#[must_use]
#[inline(always)]
#[cfg(not(feature = "power-of-two"))]
pub const fn split_radix(radix: u32) -> (u32, u32) {
    match radix {
        // Is also needed for decimal floats, due to `negative_digit_comp`.
        2 => (0, 1),
        // Is also needed for decimal floats, due to `negative_digit_comp`.
        5 => (5, 0),
        10 => (5, 1),
        // Any other radix should be unreachable.
        _ => (0, 0),
    }
}

// LIMB
// ----

//  Type for a single limb of the big integer.
//
//  A limb is analogous to a digit in base10, except, it stores 32-bit
//  or 64-bit numbers instead. We want types where 64-bit multiplication
//  is well-supported by the architecture, rather than emulated in 3
//  instructions. The quickest way to check this support is using a
//  cross-compiler for numerous architectures, along with the following
//  source file and command:
//
//  Compile with `gcc main.c -c -S -O3 -masm=intel`
//
//  And the source code is:
//  ```text
//  #include <stdint.h>
//
//  struct i128 {
//      uint64_t hi;
//      uint64_t lo;
//  };
//
//  // Type your code here, or load an example.
//  struct i128 square(uint64_t x, uint64_t y) {
//      __int128 prod = (__int128)x * (__int128)y;
//      struct i128 z;
//      z.hi = (uint64_t)(prod >> 64);
//      z.lo = (uint64_t)prod;
//      return z;
//  }
//  ```
//
//  If the result contains `call __multi3`, then the multiplication
//  is emulated by the compiler. Otherwise, it's natively supported.
//
//  This should be all-known 64-bit platforms supported by Rust.
//      https://forge.rust-lang.org/platform-support.html
//
//  # Supported
//
//  Platforms where native 128-bit multiplication is explicitly supported:
//      - x86_64 (Supported via `MUL`).
//      - mips64 (Supported via `DMULTU`, which `HI` and `LO` can be read-from).
//      - s390x (Supported via `MLGR`).
//
//  # Efficient
//
//  Platforms where native 64-bit multiplication is supported and
//  you can extract hi-lo for 64-bit multiplications.
//      - aarch64 (Requires `UMULH` and `MUL` to capture high and low bits).
//      - powerpc64 (Requires `MULHDU` and `MULLD` to capture high and low
//        bits).
//      - riscv64 (Requires `MUL` and `MULH` to capture high and low bits).
//
//  # Unsupported
//
//  Platforms where native 128-bit multiplication is not supported,
//  requiring software emulation.
//      sparc64 (`UMUL` only supports double-word arguments).
//      sparcv9 (Same as sparc64).
//
//  These tests are run via `xcross`, my own library for C cross-compiling,
//  which supports numerous targets (far in excess of Rust's tier 1 support,
//  or rust-embedded/cross's list). xcross may be found here:
//      https://github.com/Alexhuszagh/xcross
//
//  To compile for the given target, run:
//      `xcross gcc main.c -c -S -O3 --target $target`
//
//  All 32-bit architectures inherently do not have support. That means
//  we can essentially look for 64-bit architectures that are not SPARC.

#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub type Limb = u64;
#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub type Wide = u128;
#[cfg(all(target_pointer_width = "64", not(target_arch = "sparc")))]
pub type SignedWide = i128;

#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub type Limb = u32;
#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub type Wide = u64;
#[cfg(not(all(target_pointer_width = "64", not(target_arch = "sparc"))))]
pub type SignedWide = i64;
