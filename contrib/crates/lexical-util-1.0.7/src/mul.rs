//! Fast multiplication routines.

use crate::num::{as_cast, UnsignedInteger};

/// Multiply two unsigned, integral values, and return the high and low product.
///
/// The high product is the upper half of the product and the low product is the
/// lower half. The `full` type is the full type size, while the `half` type is
/// the type with exactly half the bits.
#[inline(always)]
pub fn mul<Full, Half>(x: Full, y: Full) -> (Full, Full)
where
    Full: UnsignedInteger,
    Half: UnsignedInteger,
{
    // Extract high-and-low masks.
    let x1 = x >> Half::BITS as i32;
    let x0 = x & as_cast(Half::MAX);
    let y1 = y >> Half::BITS as i32;
    let y0 = y & as_cast(Half::MAX);

    let w0 = x0 * y0;
    let tmp = (x1 * y0) + (w0 >> Half::BITS as i32);
    let w1 = tmp & as_cast(Half::MAX);
    let w2 = tmp >> Half::BITS as i32;
    let w1 = w1 + x0 * y1;
    let hi = (x1 * y1) + w2 + (w1 >> Half::BITS as i32);
    let lo = x.wrapping_mul(y);

    (hi, lo)
}

/// Multiply two unsigned, integral values, and return the high product.
///
/// The high product is the upper half of the product. The `full` type is the
/// full type size, while the `half` type is the type with exactly half the
/// bits.
#[inline(always)]
pub fn mulhi<Full, Half>(x: Full, y: Full) -> Full
where
    Full: UnsignedInteger,
    Half: UnsignedInteger,
{
    // Extract high-and-low masks.
    let x1 = x >> Half::BITS as i32;
    let x0 = x & as_cast(Half::MAX);
    let y1 = y >> Half::BITS as i32;
    let y0 = y & as_cast(Half::MAX);

    let w0 = x0 * y0;
    let m = (x0 * y1) + (w0 >> Half::BITS as i32);
    let w1 = m & as_cast(Half::MAX);
    let w2 = m >> Half::BITS as i32;

    let w3 = (x1 * y0 + w1) >> Half::BITS as i32;

    x1 * y1 + w2 + w3
}
