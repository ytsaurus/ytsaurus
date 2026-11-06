//! Radix-generic, optimized, string-to-integer conversion routines.
//!
//! These routines are highly optimized: they use various optimizations
//! to read multiple digits at-a-time with less multiplication instructions,
//! as well as other optimizations to avoid unnecessary compile-time branching.
//!
//! See [Algorithm](/docs/Algorithm.md) for a more detailed description of
//! the algorithm choice here. See [Benchmarks.md](/docs/Benchmarks) for
//! recent benchmark data.
//!
//! These allow implementations of partial and complete parsers
//! using a single code-path via macros.
//!
//! This looks relatively, complex, but it's quite simple. Almost all
//! of these branches are resolved at compile-time, so the resulting
//! code is quite small while handling all of the internal complexity.
//!
//! 1. Helpers to process ok and error results for both complete and partial
//!    parsers. They have different APIs, and mixing the APIs leads to
//!    substantial performance hits.
//! 2. Overflow checking on invalid digits for partial parsers, while just
//!    returning invalid digits for complete parsers.
//! 3. A format-aware sign parser.
//! 4. Digit parsing algorithms which explicitly wrap on overflow, for no
//!    additional overhead. This has major performance wins for **most**
//!    real-world integers, so most valid input will be substantially faster.
//! 5. An algorithm to detect if overflow occurred. This is comprehensive, and
//!    short-circuits for common cases.
//! 6. A parsing algorithm for unsigned integers, always producing positive
//!    values. This avoids any unnecessary branching.
//! 7. Multi-digit optimizations for larger sizes.

#![doc(hidden)]

use lexical_util::digit::char_to_digit_const;
use lexical_util::error::Error;
use lexical_util::format::NumberFormat;
use lexical_util::iterator::{AsBytes, Bytes, DigitsIter, Iter};
use lexical_util::num::{as_cast, Integer};
use lexical_util::result::Result;

use crate::Options;

// HELPERS

/// Check if we should do multi-digit optimizations
const fn can_try_parse_multidigits<'a, Iter: DigitsIter<'a>, const FORMAT: u128>(_: &Iter) -> bool {
    let format = NumberFormat::<FORMAT> {};
    Iter::IS_CONTIGUOUS && (cfg!(not(feature = "power-of-two")) || format.mantissa_radix() <= 10)
}

// Get if digits are required for the format.
#[cfg_attr(not(feature = "format"), allow(unused_macros))]
macro_rules! required_digits {
    () => {
        NumberFormat::<FORMAT>::REQUIRED_INTEGER_DIGITS
            || NumberFormat::<FORMAT>::REQUIRED_MANTISSA_DIGITS
    };
}

/// Return an value for a complete parser.
macro_rules! into_ok_complete {
    ($value:expr, $index:expr, $count:expr) => {{
        #[cfg(not(feature = "format"))]
        return Ok(as_cast($value));

        #[cfg(feature = "format")]
        if required_digits!() && $count == 0 {
            into_error!(Empty, $index);
        } else {
            return Ok(as_cast($value));
        }
    }};
}

/// Return an value and index for a partial parser.
macro_rules! into_ok_partial {
    ($value:expr, $index:expr, $count:expr) => {{
        #[cfg(not(feature = "format"))]
        return Ok((as_cast($value), $index));

        #[cfg(feature = "format")]
        if required_digits!() && $count == 0 {
            into_error!(Empty, $index);
        } else {
            return Ok((as_cast($value), $index));
        }
    }};
}

/// Return an error for a complete parser upon an invalid digit.
macro_rules! invalid_digit_complete {
    ($value:expr, $index:expr, $count:expr) => {
        // Don't do any overflow checking here: we don't need it.
        into_error!(InvalidDigit, $index - 1)
    };
}

/// Return a value for a partial parser upon an invalid digit.
/// This checks for numeric overflow, and returns the appropriate error.
macro_rules! invalid_digit_partial {
    ($value:expr, $index:expr, $count:expr) => {
        // NOTE: The value is already positive/negative
        into_ok_partial!($value, $index - 1, $count)
    };
}

/// Return an error, returning the index and the error.
macro_rules! into_error {
    ($code:ident, $index:expr) => {{
        return Err(Error::$code($index));
    }};
}

/// Handle an invalid digit if the format feature is enabled.
///
/// This is because we can have special, non-digit characters near
/// the start or internally. If `$is_end` is set to false, there **MUST**
/// be elements in the underlying slice after the current iterator.
#[cfg(feature = "format")]
macro_rules! fmt_invalid_digit {
    (
        $value:ident, $iter:ident, $c:expr, $start_index:ident, $invalid_digit:ident, $is_end:expr
    ) => {{
        // NOTE: If we have non-contiguous iterators, we could have a skip character
        // here at the boundary. This does not affect safety but it does affect
        // correctness.
        debug_assert!($iter.is_contiguous() || $is_end);

        let base_suffix = NumberFormat::<FORMAT>::BASE_SUFFIX;
        let uncased_base_suffix = NumberFormat::<FORMAT>::CASE_SENSITIVE_BASE_SUFFIX;
        // Need to check for a base suffix, if so, return a valid value.
        // We can't have a base suffix at the first value (need at least
        // 1 digit).
        if base_suffix != 0 && $iter.cursor() - $start_index > 1 {
            let is_suffix = if uncased_base_suffix {
                $c == base_suffix
            } else {
                $c.eq_ignore_ascii_case(&base_suffix)
            };
            // NOTE: If we're using the `take_n` optimization where it can't
            // be the end, then the iterator cannot be done. So, in that case,
            // we need to end. `take_n` also can never be used for non-
            // contiguous iterators.
            if is_suffix && $is_end && $iter.is_buffer_empty() {
                // Break out of the loop, we've finished parsing.
                break;
            } else if !$iter.is_buffer_empty() {
                // Haven't finished parsing, so we're going to call
                // `invalid_digit!`. Need to ensure we include the
                // base suffix in that.

                // SAFETY: safe since the iterator is not empty, as checked
                // in `$iter.is_buffer_empty()`. Adding in the check hopefully
                // will be elided since it's a known constant.
                unsafe { $iter.step_unchecked() };
            }
        }
        // Might have handled our base-prefix here.
        $invalid_digit!($value, $iter.cursor(), $iter.current_count())
    }};
}

/// Just return an invalid digit
#[cfg(not(feature = "format"))]
macro_rules! fmt_invalid_digit {
    (
        $value:ident, $iter:ident, $c:expr, $start_index:ident, $invalid_digit:ident, $is_end:expr
    ) => {{
        $invalid_digit!($value, $iter.cursor(), $iter.current_count());
    }};
}

/// Parse the sign from the leading digits.
///
/// This routine does the following:
///
/// 1. Parses the sign digit.
/// 2. Handles if positive signs before integers are not allowed.
/// 3. Handles negative signs if the type is unsigned.
/// 4. Handles if the sign is required, but missing.
/// 5. Handles if the iterator is empty, before or after parsing the sign.
/// 6. Handles if the iterator has invalid, leading zeros.
///
/// Returns if the value is negative, or any values detected when
/// validating the input.
#[doc(hidden)]
#[macro_export]
macro_rules! parse_sign {
    (
        $byte:ident,
        $is_signed:expr,
        $no_positive:expr,
        $required:expr,
        $invalid_positive:ident,
        $missing:ident
    ) => {
        // NOTE: `read_if` optimizes poorly since we then match after
        match $byte.integer_iter().first() {
            Some(&b'+') if !$no_positive => {
                // SAFETY: We have at least 1 item left since we peaked a value
                unsafe { $byte.step_unchecked() };
                Ok(false)
            },
            Some(&b'+') if $no_positive => Err(Error::$invalid_positive($byte.cursor())),
            Some(&b'-') if $is_signed => {
                // SAFETY: We have at least 1 item left since we peaked a value
                unsafe { $byte.step_unchecked() };
                Ok(true)
            },
            Some(_) if $required => Err(Error::$missing($byte.cursor())),
            _ if $required => Err(Error::$missing($byte.cursor())),
            _ => Ok(false),
        }
    };
}

/// Parse the sign from the leading digits.
#[cfg_attr(not(feature = "compact"), inline(always))]
pub fn parse_sign<T: Integer, const FORMAT: u128>(byte: &mut Bytes<'_, FORMAT>) -> Result<bool> {
    let format = NumberFormat::<FORMAT> {};
    parse_sign!(
        byte,
        T::IS_SIGNED,
        format.no_positive_mantissa_sign(),
        format.required_mantissa_sign(),
        InvalidPositiveSign,
        MissingSign
    )
}

// FOUR DIGITS

/// Determine if 4 bytes, read raw from bytes, are 4 digits for the radix.
#[cfg_attr(not(feature = "compact"), inline(always))]
pub fn is_4digits<const FORMAT: u128>(v: u32) -> bool {
    let radix = NumberFormat::<{ FORMAT }>::MANTISSA_RADIX;
    debug_assert!(radix <= 10);

    // We want to have a wrapping add and sub such that only values from the
    // range `[0x30, 0x39]` (or narrower for custom radixes) will not
    // overflow into the high bit. This means that the value needs to overflow
    // into into `0x80` if the digit is 1 above, or `0x46` for the value `0x39`.
    // Likewise, we only valid for `[0x30, 0x38]` for radix 8, so we need
    // `0x47`.
    let add = 0x46 + 10 - radix;
    let add = add + (add << 8) + (add << 16) + (add << 24);
    // This aims to underflow if anything is below the min digit: if we have any
    // values under `0x30`, then this underflows and wraps into the high bit.
    let sub = 0x3030_3030;
    let a = v.wrapping_add(add);
    let b = v.wrapping_sub(sub);

    (a | b) & 0x8080_8080 == 0
}

/// Parse 4 bytes read from bytes into 4 digits.
#[cfg_attr(not(feature = "compact"), inline(always))]
pub fn parse_4digits<const FORMAT: u128>(mut v: u32) -> u32 {
    let radix = NumberFormat::<{ FORMAT }>::MANTISSA_RADIX;
    debug_assert!(radix <= 10);

    // Normalize our digits to the range `[0, 9]`.
    v -= 0x3030_3030;
    // Scale digits in `0 <= Nn <= 99`.
    v = (v * radix) + (v >> 8);
    // Scale digits in `0 <= Nnnn <= 9999`.
    v = ((v & 0x0000007f) * radix * radix) + ((v >> 16) & 0x0000007f);

    v
}

/// Use a fast-path optimization, where we attempt to parse 4 digits at a time.
/// This reduces the number of multiplications necessary to 2, instead of 4.
///
/// This approach is described in full here:
/// <https://johnnylee-sde.github.io/Fast-numeric-string-to-int/>
#[cfg_attr(not(feature = "compact"), inline(always))]
pub fn try_parse_4digits<'a, T, Iter, const FORMAT: u128>(iter: &mut Iter) -> Option<T>
where
    T: Integer,
    Iter: DigitsIter<'a>,
{
    // Can't do fast optimizations with radixes larger than 10, since
    // we no longer have a contiguous ASCII block. Likewise, cannot
    // use non-contiguous iterators.
    debug_assert!(NumberFormat::<{ FORMAT }>::MANTISSA_RADIX <= 10);
    debug_assert!(Iter::IS_CONTIGUOUS);

    // Read our digits, validate the input, and check from there.
    let bytes = u32::from_le(iter.peek_u32()?);
    if is_4digits::<FORMAT>(bytes) {
        // SAFETY: safe since we have at least 4 bytes in the buffer.
        unsafe { iter.step_by_unchecked(4) };
        Some(T::as_cast(parse_4digits::<FORMAT>(bytes)))
    } else {
        None
    }
}

// EIGHT DIGITS

/// Determine if 8 bytes, read raw from bytes, are 8 digits for the radix.
/// See `is_4digits` for the algorithm description.
#[cfg_attr(not(feature = "compact"), inline(always))]
pub fn is_8digits<const FORMAT: u128>(v: u64) -> bool {
    let radix = NumberFormat::<{ FORMAT }>::MANTISSA_RADIX;
    debug_assert!(radix <= 10);

    let add = 0x46 + 10 - radix;
    let add = add + (add << 8) + (add << 16) + (add << 24);
    let add = (add as u64) | ((add as u64) << 32);
    // This aims to underflow if anything is below the min digit: if we have any
    // values under `0x30`, then this underflows and wraps into the high bit.
    let sub = 0x3030_3030_3030_3030;
    let a = v.wrapping_add(add);
    let b = v.wrapping_sub(sub);

    (a | b) & 0x8080_8080_8080_8080 == 0
}

/// Parse 8 bytes read from bytes into 8 digits.
/// Credit for this goes to @aqrit, which further optimizes the
/// optimization described by Johnny Lee above.
#[cfg_attr(not(feature = "compact"), inline(always))]
pub fn parse_8digits<const FORMAT: u128>(mut v: u64) -> u64 {
    let radix = NumberFormat::<{ FORMAT }>::MANTISSA_RADIX as u64;
    debug_assert!(radix <= 10);

    // Create our masks. Assume the optimizer will do this at compile time.
    // It seems like an optimizing compiler **will** do this, so we
    // should be safe.
    let radix2 = radix * radix;
    let radix4 = radix2 * radix2;
    let radix6 = radix2 * radix4;
    let mask = 0x0000_00FF_0000_00FFu64;
    let mul1 = radix2 + (radix6 << 32);
    let mul2 = 1 + (radix4 << 32);

    // Normalize our digits to the base.
    v -= 0x3030_3030_3030_3030;
    // Scale digits in `0 <= Nn <= 99`.
    v = (v * radix) + (v >> 8);
    let v1 = (v & mask).wrapping_mul(mul1);
    let v2 = ((v >> 16) & mask).wrapping_mul(mul2);

    ((v1.wrapping_add(v2) >> 32) as u32) as u64
}

/// Use a fast-path optimization, where we attempt to parse 8 digits at a time.
/// This reduces the number of multiplications necessary to 3, instead of 8.
#[cfg_attr(not(feature = "compact"), inline(always))]
pub fn try_parse_8digits<'a, T, Iter, const FORMAT: u128>(iter: &mut Iter) -> Option<T>
where
    T: Integer,
    Iter: DigitsIter<'a>,
{
    // Can't do fast optimizations with radixes larger than 10, since
    // we no longer have a contiguous ASCII block. Likewise, cannot
    // use non-contiguous iterators.
    debug_assert!(NumberFormat::<{ FORMAT }>::MANTISSA_RADIX <= 10);
    debug_assert!(Iter::IS_CONTIGUOUS);

    // Read our digits, validate the input, and check from there.
    let bytes = u64::from_le(iter.peek_u64()?);
    if is_8digits::<FORMAT>(bytes) {
        // SAFETY: safe since we have at least 8 bytes in the buffer.
        unsafe { iter.step_by_unchecked(8) };
        Some(T::as_cast(parse_8digits::<FORMAT>(bytes)))
    } else {
        None
    }
}

// ONE DIGIT

/// Run a loop where the integer cannot possibly overflow.
///
/// If the length of the str is short compared to the range of the type
/// we are parsing into, then we can be certain that an overflow will not occur.
/// This bound is when `radix.pow(digits.len()) - 1 <= T::MAX` but the condition
/// above is a faster (conservative) approximation of this.
///
/// Consider radix 16 as it has the highest information density per digit and
/// will thus overflow the earliest: `u8::MAX` is `ff` - any str of length 2 is
/// guaranteed to not overflow. `i8::MAX` is `7f` - only a str of length 1 is
/// guaranteed to not overflow.
///
/// This is based off of [core/num](core).
///
/// * `value` - The current parsed value.
/// * `iter` - An iterator over all bytes in the input.
/// * `add_op` - The unchecked add/sub op.
/// * `start_index` - The offset where parsing started.
/// * `invalid_digit` - Behavior when an invalid digit is found.
/// * `is_end` - If iter corresponds to the full input.
///
/// core: <https://doc.rust-lang.org/1.81.0/src/core/num/mod.rs.html#1480>
macro_rules! parse_1digit_unchecked {
    (
        $value:ident,
        $iter:ident,
        $add_op:ident,
        $start_index:ident,
        $invalid_digit:ident,
        $is_end:expr
    ) => {{
        // This is a slower parsing algorithm, going 1 digit at a time, but doing it in
        // an unchecked loop.
        let radix = NumberFormat::<FORMAT>::MANTISSA_RADIX;
        while let Some(&c) = $iter.next() {
            let digit = match char_to_digit_const(c, radix) {
                Some(v) => v,
                None => fmt_invalid_digit!($value, $iter, c, $start_index, $invalid_digit, $is_end),
            };
            // multiply first since compilers are good at optimizing things out and will do
            // a fused mul/add We must do this after getting the digit for
            // partial parsers
            $value = $value.wrapping_mul(as_cast(radix)).$add_op(as_cast(digit));
        }
    }};
}

/// Run a loop where the integer could overflow.
///
/// This is a standard, unoptimized algorithm. This is based off of
/// [core/num](core)
///
/// * `value` - The current parsed value.
/// * `iter` - An iterator over all bytes in the input.
/// * `add_op` - The checked add/sub op.
/// * `start_index` - The offset where parsing started.
/// * `invalid_digit` - Behavior when an invalid digit is found.
/// * `overflow` - If the error is overflow or underflow.
///
/// core: <https://doc.rust-lang.org/1.81.0/src/core/num/mod.rs.html#1505>
macro_rules! parse_1digit_checked {
    (
        $value:ident,
        $iter:ident,
        $add_op:ident,
        $start_index:ident,
        $invalid_digit:ident,
        $overflow:ident
    ) => {{
        // This is a slower parsing algorithm, going 1 digit at a time, but doing it in
        // an unchecked loop.
        let radix = NumberFormat::<FORMAT>::MANTISSA_RADIX;
        while let Some(&c) = $iter.next() {
            let digit = match char_to_digit_const(c, radix) {
                Some(v) => v,
                None => fmt_invalid_digit!($value, $iter, c, $start_index, $invalid_digit, true),
            };
            // multiply first since compilers are good at optimizing things out and will do
            // a fused mul/add
            $value =
                match $value.checked_mul(as_cast(radix)).and_then(|x| x.$add_op(as_cast(digit))) {
                    Some(value) => value,
                    None => into_error!($overflow, $iter.cursor() - 1),
                }
        }
    }};
}

// OVERALL DIGITS
// --------------

/// Run an unchecked loop where digits are processed incrementally.
///
/// If the type size is small or there's not many digits, skip multi-digit
/// optimizations. Otherwise, if the type size is large and we're not manually
/// skipping manual optimizations, then we do this here.
///
/// * `value` - The current parsed value.
/// * `iter` - An iterator over all bytes in the input.
/// * `add_op` - The unchecked add/sub op.
/// * `start_index` - The offset where parsing started.
/// * `invalid_digit` - Behavior when an invalid digit is found.
/// * `no_multi_digit` - If to disable multi-digit optimizations.
/// * `is_end` - If iter corresponds to the full input.
macro_rules! parse_digits_unchecked {
    (
        $value:ident,
        $iter:ident,
        $add_op:ident,
        $start_index:ident,
        $invalid_digit:ident,
        $no_multi_digit:expr,
        $is_end:expr
    ) => {{
        let can_multi = can_try_parse_multidigits::<_, FORMAT>(&$iter);
        let use_multi = can_multi && !$no_multi_digit;

        // these cannot overflow. also, we use at most 3 for a 128-bit float and 1 for a
        // 64-bit float NOTE: Miri will complain about this if we use radices >=
        // 16 but since they won't go into `try_parse_8digits!` or
        // `try_parse_4digits` it will be optimized out and the overflow won't
        // matter.
        let format = NumberFormat::<FORMAT> {};
        if use_multi && T::BITS >= 64 && $iter.buffer_length() >= 8 {
            // Try our fast, 8-digit at a time optimizations.
            let radix8 = T::from_u32(format.radix8());
            while let Some(value) = try_parse_8digits::<T, _, FORMAT>(&mut $iter) {
                $value = $value.wrapping_mul(radix8).$add_op(value);
            }
        } else if use_multi && T::BITS == 32 && $iter.buffer_length() >= 4 {
            // Try our fast, 8-digit at a time optimizations.
            let radix4 = T::from_u32(format.radix4());
            while let Some(value) = try_parse_4digits::<T, _, FORMAT>(&mut $iter) {
                $value = $value.wrapping_mul(radix4).$add_op(value);
            }
        }
        parse_1digit_unchecked!($value, $iter, $add_op, $start_index, $invalid_digit, $is_end)
    }};
}

/// Run  checked loop where digits are processed with overflow checking.
///
/// If the type size is small or there's not many digits, skip multi-digit
/// optimizations. Otherwise, if the type size is large and we're not manually
/// skipping manual optimizations, then we do this here.
///
/// * `value` - The current parsed value.
/// * `iter` - An iterator over all bytes in the input.
/// * `add_op` - The checked add/sub op.
/// * `add_op_uc` - The unchecked add/sub op for small digit optimizations.
/// * `start_index` - The offset where parsing started.
/// * `invalid_digit` - Behavior when an invalid digit is found.
/// * `overflow` - If the error is overflow or underflow.
/// * `no_multi_digit` - If to disable multi-digit optimizations.
/// * `overflow_digits` - The number of digits before we need to consider
///   checked ops.
macro_rules! parse_digits_checked {
    (
        $value:ident,
        $iter:ident,
        $add_op:ident,
        $add_op_uc:ident,
        $start_index:ident,
        $invalid_digit:ident,
        $overflow:ident,
        $no_multi_digit:expr,
        $overflow_digits:expr
    ) => {{
        // Can use the unchecked for the `max_digits` here. If we
        // have a non-contiguous iterator, we could have a case like
        // 123__456, with no consecutive digit separators allowed. If
        // it's broken between the `_` characters, the integer will be
        // seen as valid when it isn't.
        if cfg!(not(feature = "format")) || $iter.is_contiguous() {
            if let Some(mut small) = $iter.take_n($overflow_digits) {
                let mut small_iter = small.integer_iter();
                parse_digits_unchecked!(
                    $value,
                    small_iter,
                    $add_op_uc,
                    $start_index,
                    $invalid_digit,
                    $no_multi_digit,
                    false
                );
            }
        }

        // NOTE: all our multi-digit optimizations have been done here: skip this
        parse_1digit_checked!($value, $iter, $add_op, $start_index, $invalid_digit, $overflow)
    }};
}

// ALGORITHM

/// Generic algorithm for both partial and complete parsers.
///
/// * `invalid_digit` - Behavior on finding an invalid digit.
/// * `into_ok` - Behavior when returning a valid value.
/// * `invalid_digit` - Behavior when an invalid digit is found.
/// * `no_multi_digit` - If to disable multi-digit optimizations.
/// * `is_partial` - If the parser is a partial parser.
#[rustfmt::skip]
macro_rules! algorithm {
($bytes:ident, $into_ok:ident, $invalid_digit:ident, $no_multi_digit:expr) => {{
    // WARNING:
    // --------
    // None of this code can be changed for optimization reasons.
    // Do not change it without benchmarking every change.
    //  1. You cannot use the `NoSkipIterator` in the loop,
    //      you must either return a subslice (indexing)
    //      or increment outside of the loop.
    //      Failing to do so leads to numerous more, unnecessary
    //      conditional move instructions, killing performance.
    //  2. Return a 0 or 1 shift, and indexing unchecked outside
    //      of the loop is slightly faster.
    //  3. Partial and complete parsers cannot be efficiently done
    //      together.
    //
    // If you try to refactor without carefully monitoring benchmarks or
    // assembly generation, please log the number of wasted hours: so
    //  16 hours so far.

    // With `step_by_unchecked`, this is sufficiently optimized.
    // Removes conditional paths, to, which simplifies maintenance.
    // The skip version of the iterator automatically coalesces to
    // the no-skip iterator.
    let mut byte = $bytes.bytes::<FORMAT>();
    let radix = NumberFormat::<FORMAT>::MANTISSA_RADIX;

    let is_negative = parse_sign::<T, FORMAT>(&mut byte)?;
    let mut iter = byte.integer_iter();
    if iter.is_buffer_empty() {
        // Our default format **ALWAYS** requires significant digits, however,
        // we can have cases where we don
        #[cfg(not(feature = "format"))]
        into_error!(Empty, iter.cursor());

        #[cfg(feature = "format")]
        if required_digits!() {
            into_error!(Empty, iter.cursor());
        } else {
            $into_ok!(T::ZERO, iter.cursor(), 0)
        }
    }

    // Feature-gate a lot of format-only code here to simplify analysis with our branching
    // We only want to skip the zeros if have either require a base prefix or we don't
    // allow integer leading zeros, since the skip is expensive
    #[allow(unused_variables, unused_mut)]
    let mut start_index = iter.cursor();
    #[cfg_attr(not(feature = "format"), allow(unused_variables))]
    let format = NumberFormat::<FORMAT> {};
    #[cfg(feature = "format")]
    if format.has_base_prefix() || format.no_integer_leading_zeros() {
        // Skip any leading zeros. We want to do our check if it can't possibly overflow after.
        // For skipping digit-based formats, this approximation is a way over estimate.
        // NOTE: Skipping zeros is **EXPENSIVE* so we skip that without our format feature
        let zeros = iter.skip_zeros();
        start_index += zeros;

        // Now, check to see if we have a valid base prefix.
        let mut is_prefix = false;
        let base_prefix = format.base_prefix();
        if base_prefix != 0 && zeros == 1 {
            // Check to see if the next character is the base prefix.
            // We must have a format like `0x`, `0d`, `0o`. Note:
            if iter.read_if_value(base_prefix, format.case_sensitive_base_prefix()).is_some() {
                is_prefix = true;
                if iter.is_buffer_empty() {
                    into_error!(Empty, iter.cursor());
                } else {
                    start_index += 1;
                }
            }
        }

        // If we have a format that doesn't accept leading zeros,
        // check if the next value is invalid. It's invalid if the
        // first is 0, and the next is not a valid digit.
        if !is_prefix && format.no_integer_leading_zeros() && zeros != 0 {
            // Cannot have a base prefix and no leading zeros.
            let index = iter.cursor() - zeros;
            if zeros > 1 {
                into_error!(InvalidLeadingZeros, index);
            }
            // NOTE: Zeros has to be 0 here, so our index == 1 or 2 (depending on sign)
            match iter.peek().map(|&c| char_to_digit_const(c, format.radix())) {
                // Valid digit, we have an invalid value.
                Some(Some(_)) => into_error!(InvalidLeadingZeros, index),
                // Have a non-digit character that follows.
                Some(None) => $invalid_digit!(<T>::ZERO, iter.cursor() + 1, iter.current_count()),
                // No digits following, has to be ok
                None => $into_ok!(<T>::ZERO, index, iter.current_count()),
            };
        }
    }

    // shorter strings cannot possibly overflow so a great optimization
    let overflow_digits = T::overflow_digits(radix);
    let cannot_overflow = iter.as_slice().len() <= overflow_digits;

    //  NOTE:
    //      Don't add optimizations for 128-bit integers.
    //      128-bit multiplication is rather efficient, it's only division
    //      that's very slow. Any shortcut optimizations increasing branching,
    //      and even if parsing a 64-bit integer is marginally faster, it
    //      culminates in **way** slower performance overall for simple
    //      integers, and no improvement for large integers.
    let mut value = T::ZERO;
    if cannot_overflow && is_negative {
        parse_digits_unchecked!(value, iter, wrapping_sub, start_index, $invalid_digit, $no_multi_digit, true);
    } if cannot_overflow {
        parse_digits_unchecked!(value, iter, wrapping_add, start_index, $invalid_digit, $no_multi_digit, true);
    } else if is_negative {
        parse_digits_checked!(value, iter, checked_sub, wrapping_sub, start_index, $invalid_digit, Underflow, $no_multi_digit, overflow_digits);
    } else {
        parse_digits_checked!(value, iter, checked_add, wrapping_add, start_index, $invalid_digit, Overflow, $no_multi_digit, overflow_digits);
    }

    $into_ok!(value, iter.buffer_length(), iter.current_count())
}};
}

/// Algorithm for the complete parser.
#[cfg_attr(not(feature = "compact"), inline(always))]
pub fn algorithm_complete<T, const FORMAT: u128>(bytes: &[u8], options: &Options) -> Result<T>
where
    T: Integer,
{
    algorithm!(bytes, into_ok_complete, invalid_digit_complete, options.get_no_multi_digit())
}

/// Algorithm for the partial parser.
#[cfg_attr(not(feature = "compact"), inline(always))]
pub fn algorithm_partial<T, const FORMAT: u128>(
    bytes: &[u8],
    options: &Options,
) -> Result<(T, usize)>
where
    T: Integer,
{
    algorithm!(bytes, into_ok_partial, invalid_digit_partial, options.get_no_multi_digit())
}
