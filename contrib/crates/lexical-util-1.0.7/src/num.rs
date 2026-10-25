//! Utilities for Rust numbers.
//!
//! These traits define useful properties, methods, associated
//! types, and trait bounds, and conversions for working with
//! numbers in generic code.

use core::{fmt, mem, ops};

#[cfg(feature = "f16")]
use crate::bf16::bf16;
#[cfg(feature = "f16")]
use crate::f16::f16;
#[cfg(all(not(feature = "std"), any(feature = "parse-floats", feature = "write-floats")))]
use crate::libm;

// AS PRIMITIVE
// ------------

/// Type that can be converted to [`primitive`] values with `as`.
///
/// [`primitive`]: https://doc.rust-lang.org/rust-by-example/primitives.html
pub trait AsPrimitive: Copy + PartialEq + PartialOrd + Send + Sync + Sized {
    /// Convert the value to a [`u8`], as if by `value as u8`.
    fn as_u8(self) -> u8;

    /// Convert the value to a [`u16`], as if by `value as u16`.
    fn as_u16(self) -> u16;

    /// Convert the value to a [`u32`], as if by `value as u32`.
    fn as_u32(self) -> u32;

    /// Convert the value to a [`u64`], as if by `value as u64`.
    fn as_u64(self) -> u64;

    /// Convert the value to a [`u128`], as if by `value as u128`.
    fn as_u128(self) -> u128;

    /// Convert the value to a [`usize`], as if by `value as usize`.
    fn as_usize(self) -> usize;

    /// Convert the value to an [`i8`], as if by `value as i8`.
    fn as_i8(self) -> i8;

    /// Convert the value to an [`i16`], as if by `value as i16`.
    fn as_i16(self) -> i16;

    /// Convert the value to an [`i32`], as if by `value as i32`.
    fn as_i32(self) -> i32;

    /// Convert the value to an [`i64`], as if by `value as i64`.
    fn as_i64(self) -> i64;

    /// Convert the value to an [`i128`], as if by `value as i128`.
    fn as_i128(self) -> i128;

    /// Convert the value to an [`isize`], as if by `value as isize`.
    fn as_isize(self) -> isize;

    /// Convert the value to an [`f32`], as if by `value as f32`.
    fn as_f32(self) -> f32;

    /// Convert the value to an [`f64`], as if by `value as f64`.
    fn as_f64(self) -> f64;

    /// Convert the value from a [`u32`], as if by `value as _`.
    fn from_u32(value: u32) -> Self;

    /// Convert the value from a [`u64`], as if by `value as _`.
    fn from_u64(value: u64) -> Self;

    /// Convert the value to an [`struct@f16`], identical to `value as f16`
    /// if [`struct@f16`] was a primitive type.
    #[cfg(feature = "f16")]
    fn as_f16(self) -> f16;

    /// Convert the value to an [`struct@bf16`], identical to `value as bf16`
    /// if [`struct@bf16`] was a primitive type.
    #[cfg(feature = "f16")]
    fn as_bf16(self) -> bf16;
}

macro_rules! as_primitive {
    ($($t:ty)*) => ($(
        impl AsPrimitive for $t {
            #[inline(always)]
            fn as_u8(self) -> u8 {
                self as u8
            }

            #[inline(always)]
            fn as_u16(self) -> u16 {
                self as u16
            }

            #[inline(always)]
            fn as_u32(self) -> u32 {
                self as u32
            }

            #[inline(always)]
            fn as_u64(self) -> u64 {
                self as u64
            }

            #[inline(always)]
            fn as_u128(self) -> u128 {
                self as u128
            }

            #[inline(always)]
            fn as_usize(self) -> usize {
                self as usize
            }

            #[inline(always)]
            fn as_i8(self) -> i8 {
                self as i8
            }

            #[inline(always)]
            fn as_i16(self) -> i16 {
                self as i16
            }

            #[inline(always)]
            fn as_i32(self) -> i32 {
                self as i32
            }

            #[inline(always)]
            fn as_i64(self) -> i64 {
                self as i64
            }

            #[inline(always)]
            fn as_i128(self) -> i128 {
                self as i128
            }

            #[inline(always)]
            fn as_isize(self) -> isize {
                self as isize
            }

            #[inline(always)]
            fn as_f32(self) -> f32 {
                self as f32
            }

            #[inline(always)]
            fn as_f64(self) -> f64 {
                self as f64
            }

            #[inline(always)]
            fn from_u32(value: u32) -> Self {
                value as Self
            }

            #[inline(always)]
            fn from_u64(value: u64) -> Self {
                value as Self
            }

            #[cfg(feature = "f16")]
            #[inline(always)]
            fn as_f16(self) -> f16 {
                f16::from_f32(self as f32)
            }

            #[cfg(feature = "f16")]
            #[inline(always)]
            fn as_bf16(self) -> bf16 {
                bf16::from_f32(self as f32)
            }
        }
    )*)
}

as_primitive! { u8 u16 u32 u64 u128 usize i8 i16 i32 i64 i128 isize f32 f64 }

#[cfg(feature = "f16")]
macro_rules! half_as_primitive {
    ($($t:ty)*) => ($(
        impl AsPrimitive for $t {
            #[inline(always)]
            fn as_u8(self) -> u8 {
                self.as_f32() as u8
            }

            #[inline(always)]
            fn as_u16(self) -> u16 {
                self.as_f32() as u16
            }

            #[inline(always)]
            fn as_u32(self) -> u32 {
                self.as_f32() as u32
            }

            #[inline(always)]
            fn as_u64(self) -> u64 {
                self.as_f32() as u64
            }

            #[inline(always)]
            fn as_u128(self) -> u128 {
                self.as_f32() as u128
            }

            #[inline(always)]
            fn as_usize(self) -> usize {
                self.as_f32() as usize
            }

            #[inline(always)]
            fn as_i8(self) -> i8 {
                self.as_f32() as i8
            }

            #[inline(always)]
            fn as_i16(self) -> i16 {
                self.as_f32() as i16
            }

            #[inline(always)]
            fn as_i32(self) -> i32 {
                self.as_f32() as i32
            }

            #[inline(always)]
            fn as_i64(self) -> i64 {
                self.as_f32() as i64
            }

            #[inline(always)]
            fn as_i128(self) -> i128 {
                self.as_f32() as i128
            }

            #[inline(always)]
            fn as_isize(self) -> isize {
                self.as_f32() as isize
            }

            #[inline(always)]
            fn as_f32(self) -> f32 {
                self.as_f32() as f32
            }

            #[inline(always)]
            fn as_f64(self) -> f64 {
                self.as_f32() as f64
            }

            #[inline(always)]
            #[allow(clippy::as_underscore)] // reason="intentionally used in a generic sense"
            fn from_u32(value: u32) -> Self {
                Self::from_f32(value as _)
            }

            #[inline(always)]
            fn from_u64(value: u64) -> Self {
                _ = value;
                unimplemented!()
            }

            #[inline(always)]
            fn as_f16(self) -> f16 {
                f16::from_f32(self.as_f32())
            }

            #[inline(always)]
            fn as_bf16(self) -> bf16 {
                bf16::from_f32(self.as_f32())
            }
        }
    )*)
}

#[cfg(feature = "f16")]
half_as_primitive! { f16 bf16 }

// AS CAST
// -------

/// An interface for casting between machine scalars, as if `as` was used.
///
/// All values that the type can be cast to must be [`primitive`] values.
///
/// [`primitive`]: https://doc.rust-lang.org/rust-by-example/primitives.html
pub trait AsCast: AsPrimitive {
    /// Creates a number from another value that can be converted into
    /// a primitive via the [`AsPrimitive`] trait.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_util::num::AsCast;
    ///
    /// assert_eq!(u8::as_cast(256u16), 256u16 as u8); // 0
    /// ```
    fn as_cast<N: AsPrimitive>(n: N) -> Self;
}

/// Allows the high-level conversion of generic types as if `as` was used.
///
/// # Examples
///
/// ```rust
/// use lexical_util::num::as_cast;
///
/// assert_eq!(as_cast::<u8, u16>(256u16), 256u16 as u8); // 0
/// ```
#[inline(always)]
pub fn as_cast<U: AsCast, T: AsCast>(t: T) -> U {
    U::as_cast(t)
}

macro_rules! as_cast {
    ($($t:ty, $meth:ident ; )*) => ($(
        impl AsCast for $t {
            #[inline(always)]
            #[allow(clippy::as_underscore)] // reason="intentional due to generic API"
            fn as_cast<N: AsPrimitive>(n: N) -> $t {
                n.$meth() as _
            }
        }
    )*);
}

as_cast!(
    u8, as_u8 ;
    u16, as_u16 ;
    u32, as_u32 ;
    u64, as_u64 ;
    u128, as_u128 ;
    usize, as_usize ;
    i8, as_i8 ;
    i16, as_i16 ;
    i32, as_i32 ;
    i64, as_i64 ;
    i128, as_i128 ;
    isize, as_isize ;
    f32, as_f32 ;
    f64, as_f64 ;
);

#[cfg(feature = "f16")]
as_cast!(
    f16, as_f16 ;
    bf16, as_bf16 ;
);

// PRIMITIVE
// ---------

/// The base trait for all [`primitive`] types.
///
/// [`primitive`]: https://doc.rust-lang.org/rust-by-example/primitives.html
pub trait Primitive: 'static + fmt::Debug + fmt::Display + AsCast {}

macro_rules! primitive {
    ($($t:ty)*) => ($(
        impl Primitive for $t {}
    )*)
}

primitive! { u8 u16 u32 u64 u128 usize i8 i16 i32 i64 i128 isize f32 f64 }

#[cfg(feature = "f16")]
primitive! { f16 bf16 }

// NUMBER
// ------

/// The base trait for all numbers (integers and floating-point numbers).
pub trait Number:
    Default +
    Primitive +
    // Operations
    ops::Add<Output=Self> +
    ops::AddAssign +
    ops::Div<Output=Self> +
    ops::DivAssign +
    ops::Mul<Output=Self> +
    ops::MulAssign +
    ops::Rem<Output=Self> +
    ops::RemAssign +
    ops::Sub<Output=Self> +
    ops::SubAssign
{
    /// If the number can hold negative values.
    const IS_SIGNED: bool;
}

macro_rules! number_impl {
    ($($t:tt $is_signed:literal ; )*) => ($(
        impl Number for $t {
            const IS_SIGNED: bool = $is_signed;
        }
    )*)
}

number_impl! {
    u8 false ;
    u16 false ;
    u32 false ;
    u64 false ;
    u128 false ;
    usize false ;
    i8 true ;
    i16 true ;
    i32 true ;
    i64 true ;
    i128 true ;
    isize true ;
    f32 true ;
    f64 true ;
    // f128 true
}

#[cfg(feature = "f16")]
number_impl! {
    f16 true ;
    bf16 true ;
}

// INTEGER
// -------

/// The base trait for all signed and unsigned [`integers`].
///
/// [`integers`]: https://en.wikipedia.org/wiki/Integer_(computer_science)
pub trait Integer:
    // Basic
    Number + Eq + Ord +
    // Operations
    ops::BitAnd<Output=Self> +
    ops::BitAndAssign +
    ops::BitOr<Output=Self> +
    ops::BitOrAssign +
    ops::BitXor<Output=Self> +
    ops::BitXorAssign +
    ops::Not<Output=Self> +
    ops::Shl<Self, Output=Self> +
    ops::Shl<i32, Output=Self> +
    ops::ShlAssign<i32> +
    ops::Shr<i32, Output=Self> +
    ops::ShrAssign<i32> +
{
    // CONSTANTS
    /// A value equal to `0`.
    const ZERO: Self;

    /// A value equal to `1`.
    const ONE: Self;

    /// A value equal to `2`.
    const TWO: Self;

    /// The largest value that can be represented by this integer type.
    ///
    /// See [`u32::MAX`].
    const MAX: Self;

    /// The smallest value that can be represented by this integer type.
    ///
    /// See [`u32::MIN`].
    const MIN: Self;

    /// The size of this integer type in bits.
    ///
    /// See [`u32::BITS`].
    const BITS: usize;

    // FUNCTIONS (INHERITED)
    /// Returns the number of leading zeros in the binary representation
    /// of `self`.
    ///
    /// See [`u32::leading_zeros`].
    fn leading_zeros(self) -> u32;

    /// Returns the number of trailing zeros in the binary representation
    /// of `self`.
    ///
    /// See [`u32::trailing_zeros`].
    fn trailing_zeros(self) -> u32;

    /// Raises self to the power of `exp`, using exponentiation by squaring.
    ///
    /// See [`u32::pow`].
    fn pow(self, exp: u32) -> Self;

    /// Checked exponentiation. Computes `self.pow(exp)`, returning
    /// `None` if overflow occurred.
    ///
    /// See [`u32::checked_pow`].
    fn checked_pow(self, exp: u32) -> Option<Self>;

    /// Raises self to the power of `exp`, using exponentiation by squaring.
    ///
    /// Returns a tuple of the exponentiation along with a bool indicating
    /// whether an overflow happened.
    ///
    /// See [`u32::overflowing_pow`].
    fn overflowing_pow(self, exp: u32) -> (Self, bool);

    /// Checked integer addition. Computes `self + i`, returning `None` if
    /// overflow occurred.
    ///
    /// See [`u32::checked_add`].
    fn checked_add(self, i: Self) -> Option<Self>;

    /// Checked integer subtraction. Computes `self - i`, returning `None`
    /// if overflow occurred.
    ///
    /// See [`u32::checked_sub`].
    fn checked_sub(self, i: Self) -> Option<Self>;

    /// Checked integer multiplication. Computes `self * rhs`, returning `None`
    /// if overflow occurred.
    ///
    /// See [`u32::checked_mul`].
    fn checked_mul(self, i: Self) -> Option<Self>;

    /// Calculates `self + i`.
    ///
    /// Returns a tuple of the addition along with a boolean indicating whether
    /// an arithmetic overflow would occur. If an overflow would have occurred
    /// then the wrapped value is returned. See [`u32::overflowing_add`].
    fn overflowing_add(self, i: Self) -> (Self, bool);

    /// Calculates `self - i`.
    ///
    /// Returns a tuple of the addition along with a boolean indicating whether
    /// an arithmetic overflow would occur. If an overflow would have occurred
    /// then the wrapped value is returned. See [`u32::overflowing_sub`].
    fn overflowing_sub(self, i: Self) -> (Self, bool);

    /// Calculates `self * i`.
    ///
    /// Returns a tuple of the addition along with a boolean indicating whether
    /// an arithmetic overflow would occur. If an overflow would have occurred
    /// then the wrapped value is returned. See [`u32::overflowing_mul`].
    fn overflowing_mul(self, i: Self) -> (Self, bool);

    /// Wrapping (modular) addition. Computes `self + i`, wrapping around at
    /// the boundary of the type.
    ///
    /// See [`u32::wrapping_add`].
    fn wrapping_add(self, i: Self) -> Self;

    /// Wrapping (modular) subtraction. Computes `self - i`, wrapping around at
    /// the boundary of the type.
    ///
    /// See [`u32::wrapping_sub`].
    fn wrapping_sub(self, i: Self) -> Self;

    /// Wrapping (modular) multiplication. Computes `self * i`, wrapping around at
    /// the boundary of the type.
    ///
    /// See [`u32::wrapping_mul`].
    fn wrapping_mul(self, i: Self) -> Self;

    /// Wrapping (modular) negation. Computes `-self`, wrapping around at
    /// the boundary of the type.
    ///
    /// See [`u32::wrapping_neg`].
    fn wrapping_neg(self) -> Self;

    /// Saturating integer addition. Computes `self + i`, saturating at the
    /// numeric bounds instead of overflowing.
    ///
    /// See [`u32::saturating_add`].
    fn saturating_add(self, i: Self) -> Self;

    /// Saturating integer subtraction. Computes `self - i`, saturating at the
    /// numeric bounds instead of overflowing.
    ///
    /// See [`u32::saturating_sub`].
    fn saturating_sub(self, i: Self) -> Self;

    /// Saturating integer multiplication. Computes `self * i`, saturating at
    /// the numeric bounds instead of overflowing.
    ///
    /// See [`u32::saturating_mul`].
    fn saturating_mul(self, i: Self) -> Self;

    /// Get the fast ceiling of the quotient from integer division.
    ///
    /// The remainder may wrap to the numerical boundaries for the type.
    /// See [`u32::div_ceil`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_util::num::Integer;
    ///
    /// assert_eq!(250u16.ceil_divmod(10), (25, 0));
    /// assert_eq!(256u16.ceil_divmod(10), (26, -4));
    /// assert_eq!(i32::MAX.ceil_divmod(-2), (-0x3FFFFFFE, 3));
    ///
    /// // notice how `-1` wraps since `i32` cannot hold `i128::MAX`.
    /// assert_eq!((i128::MAX - 1).ceil_divmod(i128::MAX), (1, -1));
    /// ```
    #[inline(always)]
    fn ceil_divmod(self, y: Self) -> (Self, i32) {
        let q = self / y;
        let r = self % y;
        match r == Self::ZERO {
            true  => (q, i32::as_cast(r)),
            false => (q + Self::ONE, i32::as_cast(r) - i32::as_cast(y))
        }
    }

    /// Get the fast ceiling of the quotient from integer division.
    ///
    /// This is identical to [`u32::div_ceil`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_util::num::Integer;
    ///
    /// assert_eq!(250u16.ceil_div(10), 25);
    /// assert_eq!(256u16.ceil_div(10), 26);
    /// assert_eq!(i32::MAX.ceil_div(-2), -0x3FFFFFFE);
    /// assert_eq!((i128::MAX - 1).ceil_div(i128::MAX), 1);
    /// ```
    #[inline(always)]
    fn ceil_div(self, y: Self) -> Self {
        self.ceil_divmod(y).0
    }

    /// Get the fast ceiling modulus from integer division.
    ///
    /// The remainder is not guaranteed to be valid since it can
    /// overflow if the remainder is not 0. See [`Self::ceil_divmod`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_util::num::Integer;
    ///
    /// assert_eq!(250u16.ceil_mod(10), 0);
    /// assert_eq!(256u16.ceil_mod(10), -4);
    /// assert_eq!(i32::MAX.ceil_mod(-2), 3);
    ///
    /// // notice how `-1` wraps since `i32` cannot hold `i128::MAX`.
    /// assert_eq!((i128::MAX - 1).ceil_mod(i128::MAX), -1);
    /// ```
    #[inline(always)]
    fn ceil_mod(self, y: Self) -> i32 {
        self.ceil_divmod(y).1
    }

    // PROPERTIES

    /// Get the number of bits in a value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_util::num::Integer;
    ///
    /// assert_eq!(1u64.bit_length(), 1);
    /// assert_eq!(2u64.bit_length(), 2);
    /// assert_eq!(3u64.bit_length(), 2);
    /// assert_eq!(16u64.bit_length(), 5);
    /// ```
    #[inline(always)]
    fn bit_length(self) -> u32 {
        Self::BITS as u32 - self.leading_zeros()
    }

    /// Returns true if the least-significant bit is odd.
    #[inline(always)]
    fn is_odd(self) -> bool {
        self & Self::ONE == Self::ONE
    }

    /// Returns true if the least-significant bit is even.
    #[inline(always)]
    fn is_even(self) -> bool {
        !self.is_odd()
    }

    /// Get the maximum number of digits before the slice will overflow.
    ///
    /// This is effectively the `floor(log(2^BITS-1, radix))`, but we can
    /// try to go a bit lower without worrying too much.
    #[inline(always)]
    fn overflow_digits(radix: u32) -> usize {
        // this is heavily optimized for base10 and it's a way under estimate
        // that said, it's fast and works.
        if radix <= 16 {
            mem::size_of::<Self>() * 2 - Self::IS_SIGNED as usize
        } else {
            // way under approximation but always works and is fast
            mem::size_of::<Self>()
        }
    }
}

macro_rules! integer_impl {
($($t:tt)*) => ($(
    impl Integer for $t {
        const ZERO: $t = 0;
        const ONE: $t = 1;
        const TWO: $t = 2;
        const MAX: $t = $t::MAX;
        const MIN: $t = $t::MIN;
        const BITS: usize = $t::BITS as usize;

        #[inline(always)]
        fn leading_zeros(self) -> u32 {
            $t::leading_zeros(self)
        }

        #[inline(always)]
        fn trailing_zeros(self) -> u32 {
            $t::trailing_zeros(self)
        }

        #[inline(always)]
        fn checked_add(self, i: Self) -> Option<Self> {
            $t::checked_add(self, i)
        }

        #[inline(always)]
        fn checked_sub(self, i: Self) -> Option<Self> {
            $t::checked_sub(self, i)
        }

        #[inline(always)]
        fn checked_mul(self, i: Self) -> Option<Self> {
            $t::checked_mul(self, i)
        }

        #[inline(always)]
        fn overflowing_add(self, i: Self) -> (Self, bool) {
            $t::overflowing_add(self, i)
        }

        #[inline(always)]
        fn overflowing_sub(self, i: Self) -> (Self, bool) {
            $t::overflowing_sub(self, i)
        }

        #[inline(always)]
        fn overflowing_mul(self, i: Self) -> (Self, bool) {
            $t::overflowing_mul(self, i)
        }

        #[inline(always)]
        fn wrapping_add(self, i: Self) -> Self {
            $t::wrapping_add(self, i)
        }

        #[inline(always)]
        fn wrapping_sub(self, i: Self) -> Self {
            $t::wrapping_sub(self, i)
        }

        #[inline(always)]
        fn wrapping_mul(self, i: Self) -> Self {
            $t::wrapping_mul(self, i)
        }

        #[inline(always)]
        fn wrapping_neg(self) -> Self {
            $t::wrapping_neg(self)
        }

        #[inline(always)]
        fn pow(self, exp: u32) -> Self {
            Self::pow(self, exp)
        }

        #[inline(always)]
        fn checked_pow(self, exp: u32) -> Option<Self> {
            Self::checked_pow(self, exp)
        }

        #[inline(always)]
        fn overflowing_pow(self, exp: u32) -> (Self, bool) {
            Self::overflowing_pow(self, exp)
        }

        #[inline(always)]
        fn saturating_add(self, i: Self) -> Self {
            $t::saturating_add(self, i)
        }

        #[inline(always)]
        fn saturating_sub(self, i: Self) -> Self {
            $t::saturating_sub(self, i)
        }

        #[inline(always)]
        fn saturating_mul(self, i: Self) -> Self {
            $t::saturating_mul(self, i)
        }
    }
)*)
}

integer_impl! { u8 u16 u32 u64 u128 i8 i16 i32 i64 i128 usize isize }

// SIGNED INTEGER
// --------------

/// The trait for types that support [`signed`] integral operations, that is,
/// they can hold negative numbers.
///
/// [`signed`]: https://en.wikipedia.org/wiki/Integer_(computer_science)#Value_and_representation
pub trait SignedInteger: Integer + ops::Neg<Output = Self> {}

macro_rules! signed_integer_impl {
($($t:tt)*) => ($(
    impl SignedInteger for $t {}
)*)
}

signed_integer_impl! { i8 i16 i32 i64 i128 isize }

// UNSIGNED INTEGER
// ----------------

/// The trait for types that support [`unsigned`] integral operations, that is,
/// they can only hold positive numbers.
///
/// [`unsigned`]: https://en.wikipedia.org/wiki/Integer_(computer_science)#Value_and_representation
pub trait UnsignedInteger: Integer {}

macro_rules! unsigned_integer_impl {
($($t:ty)*) => ($(
    impl UnsignedInteger for $t {}
)*)
}

unsigned_integer_impl! { u8 u16 u32 u64 u128 usize }

// FLOAT
// -----

/// The trait for floating-point [`numbers`][`floats`].
///
/// Floating-point numbers are numbers that may contain a fraction
/// and are stored internally as the significant digits and an
/// exponent of base 2.
///
/// [`floats`]: https://en.wikipedia.org/wiki/Floating-point_arithmetic
#[cfg(any(feature = "parse-floats", feature = "write-floats"))]
pub trait Float: Number + ops::Neg<Output = Self> {
    /// Unsigned type of the same size.
    type Unsigned: UnsignedInteger;

    // CONSTANTS

    /// A value equal to `0`.
    const ZERO: Self;

    /// A value equal to `1`.
    const ONE: Self;

    /// A value equal to `2`.
    const TWO: Self;

    /// Largest finite value.
    ///
    /// See [`f64::MAX`].
    const MAX: Self;

    /// Smallest finite value.
    ///
    /// See [`f64::MIN`].
    const MIN: Self;

    /// Infinity (`∞`).
    ///
    /// See [`f64::INFINITY`].
    const INFINITY: Self;

    /// Negative infinity (`−∞`).
    ///
    /// See [`f64::NEG_INFINITY`].
    const NEG_INFINITY: Self;

    /// Not a Number (NaN).
    ///
    /// See [`f64::NAN`].
    const NAN: Self;

    /// The size of this float type in bits.
    ///
    /// Analogous to [`u32::BITS`].
    const BITS: usize;

    /// Bitmask to extract the sign from the float.
    const SIGN_MASK: Self::Unsigned;

    /// Bitmask to extract the biased exponent, including the hidden bit.
    const EXPONENT_MASK: Self::Unsigned;

    /// Bitmask to extract the hidden bit in the exponent, which is an
    /// implicit 1 in the significant digits.
    const HIDDEN_BIT_MASK: Self::Unsigned;

    /// Bitmask to extract the mantissa (significant digits), excluding
    /// the hidden bit.
    const MANTISSA_MASK: Self::Unsigned;

    /// Mask to determine if a full-carry occurred (1 in bit above hidden bit).
    const CARRY_MASK: Self::Unsigned;

    // PROPERTIES

    // The following constants can be calculated as follows:
    //  - `INFINITY_BITS`: EXPONENT_MASK
    //  - `NEGATIVE_INFINITY_BITS`: INFINITY_BITS | SIGN_MASK
    //  - `EXPONENT_BIAS`: `2^(EXPONENT_SIZE-1) - 1 + MANTISSA_SIZE`
    //  - `DENORMAL_EXPONENT`: `1 - EXPONENT_BIAS`
    //  - `MAX_EXPONENT`: `2^EXPONENT_SIZE - 1 - EXPONENT_BIAS`

    /// Positive infinity as bits.
    const INFINITY_BITS: Self::Unsigned;

    /// Positive infinity as bits.
    const NEGATIVE_INFINITY_BITS: Self::Unsigned;

    /// The number of bits in the exponent.
    const EXPONENT_SIZE: i32;

    /// Size of the significand (mantissa) without the hidden bit.
    const MANTISSA_SIZE: i32;

    /// Bias of the exponent. See [`exponent bias`].
    ///
    /// [`exponent bias`]: https://en.wikipedia.org/wiki/Exponent_bias
    const EXPONENT_BIAS: i32;

    /// Exponent portion of a [`denormal`] float.
    ///
    /// [`denormal`]: https://en.wikipedia.org/wiki/Subnormal_number
    const DENORMAL_EXPONENT: i32;

    /// Maximum (unbiased) exponent value in the float.
    const MAX_EXPONENT: i32;

    // FUNCTIONS (INHERITED)

    // Re-export the to and from bits methods.

    /// Raw transmutation to the unsigned integral type.
    ///
    /// See [`f64::to_bits`].
    fn to_bits(self) -> Self::Unsigned;

    /// Raw transmutation from the unsigned integral type.
    ///
    /// See [`f64::from_bits`].
    fn from_bits(u: Self::Unsigned) -> Self;

    /// Returns the natural logarithm of the number.
    ///
    /// See [`f64::ln`].
    fn ln(self) -> Self;

    /// Returns the largest integer less than or equal to `self`.
    ///
    /// See [`f64::floor`].
    fn floor(self) -> Self;

    /// Returns true if `self` has a positive sign, including `+0.0`,
    /// NaNs with positive sign bit and positive infinity.
    ///
    /// See [`f64::is_sign_positive`].
    fn is_sign_positive(self) -> bool;

    /// Returns true if `self` has a negative sign, including `-0.0`,
    /// NaNs with negative sign bit and negative infinity.
    ///
    /// See [`f64::is_sign_negative`].
    fn is_sign_negative(self) -> bool;

    /// Returns true if the float is [`denormal`].
    ///
    /// Denormal (subnormal) numbers fall below the range of numbers
    /// that can be stored as `mantissa * 2^exp`, and therefore
    /// always have the minimum exponent.
    ///
    /// [`denormal`]: https://en.wikipedia.org/wiki/Subnormal_number
    #[inline(always)]
    fn is_denormal(self) -> bool {
        self.to_bits() & Self::EXPONENT_MASK == Self::Unsigned::ZERO
    }

    /// Returns true if the float is NaN, positive infinity, or negative
    /// infinity.
    #[inline(always)]
    fn is_special(self) -> bool {
        self.to_bits() & Self::EXPONENT_MASK == Self::EXPONENT_MASK
    }

    /// Returns true if the float is NaN.
    #[inline(always)]
    fn is_nan(self) -> bool {
        self.is_special() && (self.to_bits() & Self::MANTISSA_MASK) != Self::Unsigned::ZERO
    }

    /// Returns true if the float is positive or negative infinity.
    #[inline(always)]
    fn is_inf(self) -> bool {
        self.is_special() && (self.to_bits() & Self::MANTISSA_MASK) == Self::Unsigned::ZERO
    }

    /// Returns true if the float's least-significant mantissa bit is odd.
    #[inline(always)]
    fn is_odd(self) -> bool {
        self.to_bits().is_odd()
    }

    /// Returns true if the float's least-significant mantissa bit is even.
    #[inline(always)]
    fn is_even(self) -> bool {
        !self.is_odd()
    }

    /// Returns true if the float needs a negative sign when serializing it.
    ///
    /// This is true if it's `-0.0` or it's below 0 and not NaN. But inf values
    /// need the sign.
    #[inline(always)]
    fn needs_negative_sign(self) -> bool {
        self.is_sign_negative() && !self.is_nan()
    }

    /// Get the unbiased exponent component from the float.
    #[inline(always)]
    fn exponent(self) -> i32 {
        if self.is_denormal() {
            return Self::DENORMAL_EXPONENT;
        }

        let bits = self.to_bits();
        let biased_e = i32::as_cast((bits & Self::EXPONENT_MASK) >> Self::MANTISSA_SIZE).as_i32();
        biased_e - Self::EXPONENT_BIAS
    }

    /// Get the mantissa (significand) component from float.
    #[inline(always)]
    fn mantissa(self) -> Self::Unsigned {
        let bits = self.to_bits();
        let s = bits & Self::MANTISSA_MASK;
        if !self.is_denormal() {
            s + Self::HIDDEN_BIT_MASK
        } else {
            s
        }
    }

    /// Get next greater float.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_util::num::Float;
    ///
    /// assert_eq!(1f32.next(), 1.0000001);
    /// assert_eq!((-0.0f32).next(), 0.0); // +0.0
    /// assert_eq!(0f32.next(), 1e-45);
    /// ```
    #[inline(always)]
    fn next(self) -> Self {
        let bits = self.to_bits();
        if self.is_sign_negative() && self == Self::ZERO {
            // -0.0
            Self::ZERO
        } else if bits == Self::INFINITY_BITS {
            Self::from_bits(Self::INFINITY_BITS)
        } else if self.is_sign_negative() {
            Self::from_bits(bits.saturating_sub(Self::Unsigned::ONE))
        } else {
            Self::from_bits(bits.saturating_add(Self::Unsigned::ONE))
        }
    }

    /// Get next greater float for a positive float.
    ///
    /// Value must be >= 0.0 and < INFINITY.
    #[inline(always)]
    fn next_positive(self) -> Self {
        debug_assert!(self.is_sign_positive() && !self.is_inf());
        Self::from_bits(self.to_bits() + Self::Unsigned::ONE)
    }

    /// Get previous greater float, such that `self.prev().next() == self`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use lexical_util::num::Float;
    ///
    /// assert_eq!(1f32.prev(), 0.99999994);
    /// assert_eq!(0.0f32.prev(), 0.0); // -0.0
    /// assert_eq!((-0.0f32).prev(), -1e-45);
    /// ```
    #[inline(always)]
    fn prev(self) -> Self {
        let bits = self.to_bits();
        if self.is_sign_positive() && self == Self::ZERO {
            // +0.0
            -Self::ZERO
        } else if bits == Self::NEGATIVE_INFINITY_BITS {
            Self::from_bits(Self::NEGATIVE_INFINITY_BITS)
        } else if self.is_sign_negative() {
            Self::from_bits(bits.saturating_add(Self::Unsigned::ONE))
        } else {
            Self::from_bits(bits.saturating_sub(Self::Unsigned::ONE))
        }
    }

    /// Get previous greater float for a positive float.
    /// Value must be > 0.0.
    #[inline(always)]
    fn prev_positive(self) -> Self {
        debug_assert!(self.is_sign_positive() && self != Self::ZERO);
        Self::from_bits(self.to_bits() - Self::Unsigned::ONE)
    }

    /// Round a positive number to even.
    #[inline(always)]
    fn round_positive_even(self) -> Self {
        if self.mantissa().is_odd() {
            self.next_positive()
        } else {
            self
        }
    }

    /// Get the max of two finite numbers.
    ///
    /// This assumes that both floats form a [`total ord`],
    /// that is, `x < y` is always `y >= x`. Non-finite floats,
    /// such as NaN, break this criteria, but finite floats enable
    /// simpler (and faster) comparison criteria while remaining
    /// accurate.
    ///
    /// [`total ord`]: https://doc.rust-lang.org/std/cmp/trait.Ord.html
    #[inline(always)]
    fn max_finite(self, f: Self) -> Self {
        debug_assert!(!self.is_special() && !f.is_special(), "max_finite self={} f={}", self, f);
        if self < f {
            f
        } else {
            self
        }
    }

    /// Get the min of two finite numbers.
    ///
    /// This assumes that both floats form a [`total ord`],
    /// that is, `x < y` is always `y >= x`. Non-finite floats,
    /// such as NaN, break this criteria, but finite floats enable
    /// simpler (and faster) comparison criteria while remaining
    /// accurate.
    ///
    /// [`total ord`]: https://doc.rust-lang.org/std/cmp/trait.Ord.html
    #[inline(always)]
    fn min_finite(self, f: Self) -> Self {
        debug_assert!(!self.is_special() && !f.is_special(), "min_finite self={} f={}", self, f);
        if self < f {
            self
        } else {
            f
        }
    }
}

/// Define the float literals.
#[cfg(any(feature = "parse-floats", feature = "write-floats"))]
macro_rules! float_literals {
    ($float:ty) => {
        const ZERO: $float = 0.0;
        const ONE: $float = 1.0;
        const TWO: $float = 2.0;
        const MAX: $float = <$float>::MAX;
        const MIN: $float = <$float>::MIN;
        const INFINITY: $float = <$float>::INFINITY;
        const NEG_INFINITY: $float = <$float>::NEG_INFINITY;
        const NAN: $float = <$float>::NAN;
        const BITS: usize = mem::size_of::<$float>() * 8;
    };
}

/// Define the float masks.
#[cfg(any(feature = "parse-floats", feature = "write-floats"))]
macro_rules! float_masks {
    (
        float =>
        $float:ty,sign_mask =>
        $sign:literal,exponent_mask =>
        $exponent:literal,hidden_bit_mask =>
        $hidden:literal,mantissa_mask =>
        $mantissa:literal,
    ) => {
        const SIGN_MASK: <$float>::Unsigned = $sign;
        const EXPONENT_MASK: <$float>::Unsigned = $exponent;
        const HIDDEN_BIT_MASK: <$float>::Unsigned = $hidden;
        const MANTISSA_MASK: <$float>::Unsigned = $mantissa;
        // The carry mask is always 1 bit above the hidden bit.
        const CARRY_MASK: <$float>::Unsigned = $hidden << 1;
        // Infinity is always every exponent bit set.
        const INFINITY_BITS: <$float>::Unsigned = $exponent;
        // Negative infinity is just infinity + sign.
        const NEGATIVE_INFINITY_BITS: <$float>::Unsigned = $exponent | $sign;
    };
}

//  Due to missing specifics or types for the following float types,
//  `Float` is not yet fully implemented for:
//      - f128

#[cfg(feature = "f16")]
macro_rules! float_one {
    ($f:ident) => {
        (($f::EXPONENT_BIAS - $f::MANTISSA_SIZE) as u16) << $f::MANTISSA_SIZE
    };
}

#[cfg(feature = "f16")]
macro_rules! float_two {
    ($f:ident) => {
        (($f::EXPONENT_BIAS - $f::MANTISSA_SIZE + 1) as u16) << $f::MANTISSA_SIZE
    };
}

#[cfg(feature = "f16")]
macro_rules! float_max {
    ($f:ident) => {
        ($f::EXPONENT_MASK ^ $f::HIDDEN_BIT_MASK) | $f::MANTISSA_MASK
    };
}

#[cfg(feature = "f16")]
macro_rules! float_min {
    ($f:ident) => {
        $f::MAX.to_bits() | $f::SIGN_MASK
    };
}

#[cfg(feature = "f16")]
macro_rules! float_nan {
    ($f:ident) => {
        $f::EXPONENT_MASK | ($f::HIDDEN_BIT_MASK >> 1)
    };
}

#[cfg(feature = "f16")]
impl Float for f16 {
    type Unsigned = u16;

    const ZERO: Self = Self::from_bits(0);
    const ONE: Self = Self::from_bits(float_one!(Self));
    const TWO: Self = Self::from_bits(float_two!(Self));
    const MAX: Self = Self::from_bits(float_max!(Self));
    const MIN: Self = Self::from_bits(float_min!(Self));
    const INFINITY: Self = Self::from_bits(Self::INFINITY_BITS);
    const NEG_INFINITY: Self = Self::from_bits(Self::NEGATIVE_INFINITY_BITS);
    const NAN: Self = Self::from_bits(float_nan!(Self));
    const BITS: usize = mem::size_of::<Self>() * 8;

    float_masks!(
        float => Self,
        sign_mask => 0x8000,
        exponent_mask => 0x7C00,
        hidden_bit_mask => 0x0400,
        mantissa_mask => 0x03FF,
    );
    const EXPONENT_SIZE: i32 = 5;
    const MANTISSA_SIZE: i32 = 10;
    const EXPONENT_BIAS: i32 = 15 + Self::MANTISSA_SIZE;
    const DENORMAL_EXPONENT: i32 = 1 - Self::EXPONENT_BIAS;
    const MAX_EXPONENT: i32 = 0x1F - Self::EXPONENT_BIAS;

    #[inline(always)]
    fn to_bits(self) -> u16 {
        f16::to_bits(self)
    }

    #[inline(always)]
    fn from_bits(u: u16) -> f16 {
        f16::from_bits(u)
    }

    #[inline(always)]
    fn ln(self) -> f16 {
        f16::from_f32(self.as_f32().ln())
    }

    #[inline(always)]
    fn floor(self) -> f16 {
        f16::from_f32(self.as_f32().floor())
    }

    #[inline(always)]
    fn is_sign_positive(self) -> bool {
        self.to_bits() & Self::SIGN_MASK == 0
    }

    #[inline(always)]
    fn is_sign_negative(self) -> bool {
        !self.is_sign_positive()
    }
}

#[cfg(feature = "f16")]
impl Float for bf16 {
    type Unsigned = u16;

    const ZERO: Self = Self::from_bits(0);
    const ONE: Self = Self::from_bits(float_one!(Self));
    const TWO: Self = Self::from_bits(float_two!(Self));
    const MAX: Self = Self::from_bits(float_max!(Self));
    const MIN: Self = Self::from_bits(float_min!(Self));
    const INFINITY: Self = Self::from_bits(Self::INFINITY_BITS);
    const NEG_INFINITY: Self = Self::from_bits(Self::NEGATIVE_INFINITY_BITS);
    const NAN: Self = Self::from_bits(float_nan!(Self));
    const BITS: usize = mem::size_of::<Self>() * 8;

    float_masks!(
        float => Self,
        sign_mask => 0x8000,
        exponent_mask => 0x7F80,
        hidden_bit_mask => 0x0080,
        mantissa_mask => 0x007F,
    );
    const EXPONENT_SIZE: i32 = 8;
    const MANTISSA_SIZE: i32 = 7;
    const EXPONENT_BIAS: i32 = 127 + Self::MANTISSA_SIZE;
    const DENORMAL_EXPONENT: i32 = 1 - Self::EXPONENT_BIAS;
    const MAX_EXPONENT: i32 = 0xFF - Self::EXPONENT_BIAS;

    #[inline(always)]
    fn to_bits(self) -> u16 {
        bf16::to_bits(self)
    }

    #[inline(always)]
    fn from_bits(u: u16) -> bf16 {
        bf16::from_bits(u)
    }

    #[inline(always)]
    fn ln(self) -> bf16 {
        bf16::from_f32(self.as_f32().ln())
    }

    #[inline(always)]
    fn floor(self) -> bf16 {
        bf16::from_f32(self.as_f32().floor())
    }

    #[inline(always)]
    fn is_sign_positive(self) -> bool {
        self.to_bits() & Self::SIGN_MASK == 0
    }

    #[inline(always)]
    fn is_sign_negative(self) -> bool {
        !self.is_sign_positive()
    }
}

#[cfg(any(feature = "parse-floats", feature = "write-floats"))]
impl Float for f32 {
    type Unsigned = u32;
    float_literals!(f32);
    float_masks!(
        float => Self,
        sign_mask => 0x80000000,
        exponent_mask => 0x7F800000,
        hidden_bit_mask => 0x00800000,
        mantissa_mask => 0x007FFFFF,
    );
    const EXPONENT_SIZE: i32 = 8;
    const MANTISSA_SIZE: i32 = 23;
    const EXPONENT_BIAS: i32 = 127 + Self::MANTISSA_SIZE;
    const DENORMAL_EXPONENT: i32 = 1 - Self::EXPONENT_BIAS;
    const MAX_EXPONENT: i32 = 0xFF - Self::EXPONENT_BIAS;

    #[inline(always)]
    fn to_bits(self) -> u32 {
        f32::to_bits(self)
    }

    #[inline(always)]
    fn from_bits(u: u32) -> f32 {
        f32::from_bits(u)
    }

    #[inline(always)]
    fn ln(self) -> f32 {
        #[cfg(feature = "std")]
        return f32::ln(self);

        #[cfg(not(feature = "std"))]
        return libm::logf(self);
    }

    #[inline(always)]
    fn floor(self) -> f32 {
        #[cfg(feature = "std")]
        return f32::floor(self);

        #[cfg(not(feature = "std"))]
        return libm::floorf(self);
    }

    #[inline(always)]
    fn is_sign_positive(self) -> bool {
        f32::is_sign_positive(self)
    }

    #[inline(always)]
    fn is_sign_negative(self) -> bool {
        f32::is_sign_negative(self)
    }
}

#[cfg(any(feature = "parse-floats", feature = "write-floats"))]
impl Float for f64 {
    type Unsigned = u64;
    float_literals!(f64);
    float_masks!(
        float => Self,
        sign_mask => 0x8000000000000000,
        exponent_mask => 0x7FF0000000000000,
        hidden_bit_mask => 0x0010000000000000,
        mantissa_mask => 0x000FFFFFFFFFFFFF,
    );
    const EXPONENT_SIZE: i32 = 11;
    const MANTISSA_SIZE: i32 = 52;
    const EXPONENT_BIAS: i32 = 1023 + Self::MANTISSA_SIZE;
    const DENORMAL_EXPONENT: i32 = 1 - Self::EXPONENT_BIAS;
    const MAX_EXPONENT: i32 = 0x7FF - Self::EXPONENT_BIAS;

    #[inline(always)]
    fn to_bits(self) -> u64 {
        f64::to_bits(self)
    }

    #[inline(always)]
    fn from_bits(u: u64) -> f64 {
        f64::from_bits(u)
    }

    #[inline(always)]
    fn ln(self) -> f64 {
        #[cfg(feature = "std")]
        return f64::ln(self);

        #[cfg(not(feature = "std"))]
        return libm::logd(self);
    }

    #[inline(always)]
    fn floor(self) -> f64 {
        #[cfg(feature = "std")]
        return f64::floor(self);

        #[cfg(not(feature = "std"))]
        return libm::floord(self);
    }

    #[inline(always)]
    fn is_sign_positive(self) -> bool {
        f64::is_sign_positive(self)
    }

    #[inline(always)]
    fn is_sign_negative(self) -> bool {
        f64::is_sign_negative(self)
    }
}

// #[cfg(feature = "f128")]
// impl Float for f128 {
//     type Unsigned = u128;
//     float_literals!(f128);
//     float_masks!(
//         float => Self,
//         sign_mask => 0x80000000000000000000000000000000,
//         exponent_mask => 0x7FFF0000000000000000000000000000,
//         hidden_bit_mask => 0x00010000000000000000000000000000,
//         mantissa_mask => 0x0000FFFFFFFFFFFFFFFFFFFFFFFFFFFF,
//     );
//     const EXPONENT_SIZE: i32 = 15;
//     const MANTISSA_SIZE: i32 = 112;
//     const EXPONENT_BIAS: i32 = 16383 + Self::MANTISSA_SIZE;
//     const DENORMAL_EXPONENT: i32 = 1 - Self::EXPONENT_BIAS;
//     const MAX_EXPONENT: i32 = 0x7FFF - Self::EXPONENT_BIAS;
// }
