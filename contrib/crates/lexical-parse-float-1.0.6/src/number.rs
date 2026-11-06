//! Representation of a float as the significant digits and exponent.
//!
//! This is adapted from [fast-float-rust](https://github.com/aldanor/fast-float-rust),
//! a port of [fast_float](https://github.com/fastfloat/fast_float) to Rust.

#![doc(hidden)]
#![allow(clippy::exhaustive_structs)] // reason = "only public for testing"

use lexical_util::format::NumberFormat;

use crate::float::RawFloat;
use crate::fpu::set_precision;

/// Representation of a number as the significant digits and exponent.
///
/// This is only used if the exponent base and the significant digit
/// radix are the same, since we need to be able to move powers in and
/// out of the exponent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Number<'a> {
    /// The exponent of the float, scaled to the mantissa.
    pub exponent: i64,
    /// The significant digits of the float.
    pub mantissa: u64,
    /// If the float is negative.
    pub is_negative: bool,
    /// If the significant digits were truncated.
    pub many_digits: bool,
    /// The significant integer digits.
    pub integer: &'a [u8],
    /// The significant fraction digits.
    pub fraction: Option<&'a [u8]>,
}

impl Number<'_> {
    /// Detect if the float can be accurately reconstructed from native floats.
    #[must_use]
    #[inline(always)]
    pub fn is_fast_path<F: RawFloat, const FORMAT: u128>(&self) -> bool {
        let format = NumberFormat::<FORMAT> {};
        debug_assert!(
            format.mantissa_radix() == format.exponent_base(),
            "fast path requires same radix"
        );
        F::min_exponent_fast_path(format.radix()) <= self.exponent
            && self.exponent <= F::max_exponent_disguised_fast_path(format.radix())
            && self.mantissa <= F::MAX_MANTISSA_FAST_PATH
            && !self.many_digits
    }

    /// The fast path algorithm using machine-sized integers and floats.
    ///
    /// This is extracted into a separate function so that it can be attempted
    /// before constructing a Decimal. This only works if both the mantissa
    /// and the exponent can be exactly represented as a machine float,
    /// since IEE-754 guarantees no rounding will occur.
    ///
    /// There is an exception: disguised fast-path cases, where we can shift
    /// powers-of-10 from the exponent to the significant digits.
    // `set_precision` doesn't return a unit value on x87 FPUs.
    #[must_use]
    #[allow(clippy::missing_inline_in_public_items)] // reason = "only public for testing"
    #[allow(clippy::let_unit_value)] // reason = "intentional ASM drop for X87 FPUs"
    pub fn try_fast_path<F: RawFloat, const FORMAT: u128>(&self) -> Option<F> {
        let format = NumberFormat::<FORMAT> {};
        debug_assert!(
            format.mantissa_radix() == format.exponent_base(),
            "fast path requires same radix"
        );
        // The fast path crucially depends on arithmetic being rounded to the correct
        // number of bits without any intermediate rounding. On x86 (without SSE
        // or SSE2) this requires the precision of the x87 FPU stack to be
        // changed so that it directly rounds to 64/32 bit. The `set_precision`
        // function takes care of setting the precision on architectures which
        // require setting it by changing the global state (like the control word of the
        // x87 FPU).
        let _cw = set_precision::<F>();

        if self.is_fast_path::<F, FORMAT>() {
            let radix = format.radix();
            let max_exponent = F::max_exponent_fast_path(radix);
            let mut value = if self.exponent <= max_exponent {
                // normal fast path
                let value = F::as_cast(self.mantissa);
                if self.exponent < 0 {
                    value / F::pow_fast_path((-self.exponent) as usize, radix)
                } else {
                    value * F::pow_fast_path(self.exponent as usize, radix)
                }
            } else {
                // disguised fast path
                let shift = self.exponent - max_exponent;
                let int_power = F::int_pow_fast_path(shift as usize, radix);
                let mantissa = self.mantissa.checked_mul(int_power)?;
                if mantissa > F::MAX_MANTISSA_FAST_PATH {
                    return None;
                }
                F::as_cast(mantissa) * F::pow_fast_path(max_exponent as usize, radix)
            };
            if self.is_negative {
                value = -value;
            }
            Some(value)
        } else {
            None
        }
    }

    /// Force a fast-path algorithm, even when it may not be accurate.
    // `set_precision` doesn't return a unit value on x87 FPUs.
    #[must_use]
    #[allow(clippy::missing_inline_in_public_items)] // reason = "only public for testing"
    #[allow(clippy::let_unit_value)] // reason = "intentional ASM drop for X87 FPUs"
    pub fn force_fast_path<F: RawFloat, const FORMAT: u128>(&self) -> F {
        let format = NumberFormat::<FORMAT> {};
        debug_assert!(
            format.mantissa_radix() == format.exponent_base(),
            "fast path requires same radix"
        );

        let _cw = set_precision::<F>();

        let radix = format.radix();
        let mut value = F::as_cast(self.mantissa);
        let max_exponent = F::max_exponent_fast_path(radix);
        let mut exponent = self.exponent.abs();
        if self.exponent < 0 {
            while exponent > max_exponent {
                value /= F::pow_fast_path(max_exponent as usize, radix);
                exponent -= max_exponent;
            }
            value /= F::pow_fast_path(exponent as usize, radix);
        } else {
            while exponent > max_exponent {
                value *= F::pow_fast_path(max_exponent as usize, radix);
                exponent -= max_exponent;
            }
            value *= F::pow_fast_path(exponent as usize, radix);
        }
        if self.is_negative {
            value = -value;
        }
        value
    }
}
