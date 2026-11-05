//! Pre-defined constants for numeric types.

#![doc(hidden)]
#![cfg(any(feature = "write-floats", feature = "write-integers"))]

#[cfg(feature = "f16")]
use crate::bf16::bf16;
#[cfg(feature = "f16")]
use crate::f16::f16;

/// The size, in bytes, of formatted values.
pub trait FormattedSize {
    /// Maximum number of bytes required to serialize a number to string.
    /// If [`power-of-two`] or [`radix`] is not enabled, this is the same as
    /// [`FORMATTED_SIZE_DECIMAL`][`Self::FORMATTED_SIZE_DECIMAL`].
    ///
    /// <div class="warning">
    ///
    /// Note that this value may be insufficient if digit precision control,
    /// exponent break points, or disabling exponent notation is used. If
    /// you are changing the number significant digits written, the exponent
    /// break points, or disabling scientific notation, you will need a larger
    /// buffer than the one provided. An upper limit on the buffer size can
    /// then be determined using [`WriteOptions::buffer_size`].
    ///
    /// Using an insufficiently large buffer will lead to the code panicking.
    ///
    /// </div>
    ///
    /// [`WriteOptions::buffer_size`]: crate::options::WriteOptions::buffer_size
    /// [`lexical_write_float`]: https://github.com/Alexhuszagh/rust-lexical/tree/main/lexical-write-float
    /// [`power-of-two`]: crate#features
    /// [`radix`]: crate#features
    const FORMATTED_SIZE: usize;

    /// Maximum number of bytes required to serialize a number to a decimal
    /// string.
    ///
    /// <div class="warning">
    ///
    /// Note that this value may be insufficient if digit precision control,
    /// exponent break points, or disabling exponent notation is used. If
    /// you are changing the number significant digits written, the exponent
    /// break points, or disabling scientific notation, you will need a larger
    /// buffer than the one provided. An upper limit on the buffer size can
    /// then be determined using [`WriteOptions::buffer_size`].
    ///
    /// Using an insufficiently large buffer will lead to the code panicking.
    ///
    /// </div>
    ///
    /// [`WriteOptions::buffer_size`]: crate::options::WriteOptions::buffer_size
    /// [`lexical_write_float`]: https://github.com/Alexhuszagh/rust-lexical/tree/main/lexical-write-float
    const FORMATTED_SIZE_DECIMAL: usize;
}

macro_rules! formatted_size_impl {
    ($($t:tt $decimal:literal $radix:literal ; )*) => ($(
        impl FormattedSize for $t {
            #[cfg(feature = "power-of-two")]
            const FORMATTED_SIZE: usize = $radix;
            #[cfg(not(feature = "power-of-two"))]
            const FORMATTED_SIZE: usize = $decimal;
            const FORMATTED_SIZE_DECIMAL: usize = $decimal;
        }
    )*);
}

formatted_size_impl! {
    i8 4 16 ;
    i16 6 32 ;
    i32 11 64 ;
    i64 20 128 ;
    i128 40 256 ;
    u8 3 16 ;
    u16 5 32 ;
    u32 10 64 ;
    u64 20 128 ;
    u128 39 256 ;
    // The f64 buffer is actually a size of 60, but use 64 since it's a power of 2.
    // Use 256 for non-decimal values, actually, since we seem to have memory
    // issues with f64. Clearly not sufficient memory allocated for non-decimal
    // values.
    //bf16 64 256 ;
    //f16 64 256 ;
    f32 64 256 ;
    f64 64 256 ;
    //f128 128 512 ;
    //f256 256 1024 ;
}

#[cfg(feature = "f16")]
formatted_size_impl! {
    f16 64 256 ;
    bf16 64 256 ;
}

#[cfg(target_pointer_width = "16")]
formatted_size_impl! { isize 6 32 ; }
#[cfg(target_pointer_width = "16")]
formatted_size_impl! { usize 5 32 ; }

#[cfg(target_pointer_width = "32")]
formatted_size_impl! { isize 11 64 ; }
#[cfg(target_pointer_width = "32")]
formatted_size_impl! { usize 10 64 ; }

#[cfg(target_pointer_width = "64")]
formatted_size_impl! { isize 20 128 ; }
#[cfg(target_pointer_width = "64")]
formatted_size_impl! { usize 20 128 ; }

/// Maximum number of bytes required to serialize any number with default
/// options to string.
///
/// Note that this value may be insufficient if digit precision control,
/// exponent break points, or disabling exponent notation is used.
/// Please read the documentation in [`lexical_write_float`] for more
/// information.
///
/// [`lexical_write_float`]: https://github.com/Alexhuszagh/rust-lexical/tree/main/lexical-write-float
pub const BUFFER_SIZE: usize = f64::FORMATTED_SIZE;
