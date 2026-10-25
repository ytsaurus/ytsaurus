//! An iterator that skips values equal to a provided value.
//!
//! Iterators over a contiguous slice, returning all values
//! except for those matching the provided skip value.
//!
//! # Complexity
//!
//! Although superficially quite simple, the level of complexity
//! introduced by digit separators can be quite complex, due
//! the number of permutations during parsing.
//!
//! We can consume any combinations of of \[0,3\] items from the following set:
//!     - \[l\]eading digit separators, where digit separators occur before
//!       digits.
//!     - \[i\]nternal digit separators, where digit separators occur between
//!       digits.
//!     - \[t\]railing digit separators, where digit separators occur after
//!       digits.
//!
//! In addition to those combinations, we can also have:
//!     - \[c\]onsecutive digit separators, which allows two digit separators to
//!       be adjacent.
//!
//! # Shorthand
//!
//! We will use the term consumer to denote a function that consumes digits,
//! splitting an input buffer at an index, where the leading section contains
//! valid input digits, and the trailing section contains invalid characters.
//! Due to the number of combinations for consumers, we use the following
//! shorthand to denote consumers:
//!     - `no`, does not use a digit separator.
//!     - `l`, consumes leading digit separators.
//!     - `i`, consumes internal digit separators.
//!     - `t`, consumes trailing digit separators.
//!     - `c`, consumes consecutive digit separators.
//!
//! The `next`/`iter` algorithms are therefore named `next_x`, where `x`
//! represents the shorthand name of the consumer, in sorted order.
//!  For example, `next_ilt` means that consumer can skip internal,
//! leading, and trailing digit separators, but not consecutive ones.

#![cfg(all(feature = "format", any(feature = "parse-floats", feature = "parse-integers")))]

use core::{mem, ptr};

use crate::digit::char_is_digit_const;
use crate::format::NumberFormat;
use crate::format_flags as flags;
use crate::iterator::{DigitsIter, Iter};

// IS_ILTC
// -------

// NOTE:  The compiler optimizes all these methods pretty well: it's as
// efficient or almost as efficient as optimized assembly without unsafe
// code, especially since we have to do bounds checking
// before and the compiler can determine all cases correctly.

/// Helpers to get the next or previous elements for checks.
///
/// This has the non-consecutive iterator variants as well
/// as the consecutive ones. The consecutive ones will iteratively
/// process all digits.
macro_rules! indexing {
    (@next $self:ident, $index:expr) => {
        $index.wrapping_add(1)
    };

    (@nextc $self:ident, $index:expr) => {{
        let mut index = $index;
        let slc = $self.byte.slc;
        while slc.get(index.wrapping_add(1)).map_or(false, |&x| $self.is_digit_separator(x)) {
            index = index.wrapping_add(1);
        }
        index.wrapping_add(1)
    }};

    (@prev $self:ident, $index:expr) => {
        $index.wrapping_sub(1)
    };

    (@prevc $self:ident, $index:expr) => {{
        let mut index = $index;
        let slc = $self.byte.slc;
        while slc.get(index.wrapping_sub(1)).map_or(false, |&x| $self.is_digit_separator(x)) {
            index = index.wrapping_sub(1);
        }
        index.wrapping_sub(1)
    }};
}

/// Determine if a single digit separator is internal.
///
/// # Examples
///
/// - `1__1_23`- invalid
/// - `1_1__23`- invalid
/// - `1_1_23`- valid
/// - `11_x23`- invalid
/// - `_1123`- invalid
/// - `+_1123`- invalid
/// - `_+1123`- invalid
/// - `1123_`- invalid
/// - `1123_.`- invalid
/// - `112_3.`- valid
///
/// # Preconditions
///
/// Assumes `slc[index]` is a digit separator.
macro_rules! is_i {
    (@first $self:ident, $index:expr) => {{
        // NOTE: The conditions here then are that:
        // - `index - 1` is a digit
        // - `index + 1` is a digit

        let prev = indexing!(@prev $self, $index);
        let next = indexing!(@next $self, $index);
        let slc = $self.byte.slc;
        slc.get(prev).map_or(false, |&x| $self.is_digit(x)) &&
            slc.get(next).map_or(false, |&x| $self.is_digit(x))
    }};

    (@first $self:ident) => {
        is_i!(@first $self, $self.byte.index)
    };

    (@internal $self:ident, $index:expr) => {{
        // NOTE: We must have validated `prev`, so this just checks `next`.
        // NOTE: The conditions here then are that:
        // - `index + 1` is a digit

        let next = indexing!(@next $self, $index);
        let slc = $self.byte.slc;
        slc.get(next).map_or(false, |&x| $self.is_digit(x))
    }};

    (@internal $self:ident) => {
        is_i!(@internal $self, $self.byte.index)
    };
}

/// Determine if consecutive digit separators are internal.
///
/// # Examples
///
/// - `1__1_23`- valid
/// - `1_1__23`- valid
/// - `1_1_23`- valid
/// - `11_x23`- invalid
/// - `_1123`- invalid
/// - `+_1123`- invalid
/// - `_+1123`- invalid
/// - `1123_`- invalid
/// - `1123_.`- invalid
/// - `112_3.`- valid
///
/// # Preconditions
///
/// Assumes `slc[index]` is a digit separator.
macro_rules! is_ic {
    (@first $self:ident, $index:expr) => {{
        // NOTE: The conditions here then are that:
        // - `index - 1` is a digit after consuming digit separators
        // - `index + 1` is a digit after consuming digit separators

        let prev = indexing!(@prevc $self, $index);
        let next = indexing!(@nextc $self, $index);
        let slc = $self.byte.slc;
        slc.get(prev).map_or(false, |&x| $self.is_digit(x)) &&
            slc.get(next).map_or(false, |&x| $self.is_digit(x))
    }};

    (@first $self:ident) => {
        is_ic!(@first $self, $self.byte.index)
    };

    (@internal $self:ident, $index:expr) => {{
        // NOTE: We must have validated `prev`, so this just checks `next`.
        // NOTE: The conditions here then are that:
        // - `index + 1` is a digit after consuming digit separators

        let next = indexing!(@nextc $self, $index);
        let slc = $self.byte.slc;
        slc.get(next).map_or(false, |&x| $self.is_digit(x))
    }};

    (@internal $self:ident) => {
        is_ic!(@internal $self, $self.byte.index)
    };
}

/// Determine if a single digit separator is leading.
///
/// # Examples
///
/// - `__123`- invalid
/// - `+__123`- invalid
/// - `._123`- valid
/// - `_+123`- valid
/// - `_123`- valid
/// - `+_123`- valid
///
/// Having a subsequent sign character is fine since it might
/// be part of a partial parser.
///
/// # Preconditions
///
/// Assumes `slc[index]` is a digit separator.
macro_rules! is_l {
    (@first $self:ident, $index:expr) => {{
        // NOTE: The conditions here then are that:
        // - `index - 1` is not a digit
        // - `index - 1` is not a digit separator
        // - `index + 1` is not a digit separator

        let prev = indexing!(@prev $self, $index);
        let next = indexing!(@next $self, $index);
        let slc = $self.byte.slc;
        slc.get(prev).map_or(true, |&x| !$self.is_digit(x) && !$self.is_digit_separator(x)) &&
            slc.get(next).map_or(true, |&x| !$self.is_digit_separator(x))
    }};

    (@first $self:ident) => {
        is_l!(@first $self, $self.byte.index)
    };

    (@internal $self:ident, $index:expr) => {{
        // NOTE: Previous must have been a digit so this cannot be valid.
        false
    }};

    (@internal $self:ident) => {
        is_l!(@internal $self, $self.byte.index)
    };
}

/// Determine if one or more digit separators are leading.
///
/// # Examples
///
/// - `__123`- valid
/// - `+__123`- valid
/// - `+__+123`- valid
/// - `+__.123`- valid
/// - `._123`- valid
/// - `_+123`- invalid
/// - `_123`- valid
/// - `+_123`- valid
///
/// Having a subsequent sign character is fine since it might
/// be part of a partial parser.
///
/// # Preconditions
///
/// Assumes `slc[index]` is a digit separator.
macro_rules! is_lc {
    (@first $self:ident, $index:expr) => {{
        // NOTE: The conditions here then are that:
        // - `index - 1` is not a digit after removing digit separators

        let prev = indexing!(@prevc $self, $index);
        let slc = $self.byte.slc;
        slc.get(prev).map_or(true, |&x| !$self.is_digit(x))
    }};

    (@first $self:ident) => {
        is_lc!(@first $self, $self.byte.index)
    };

    (@internal $self:ident, $index:expr) => {{
        // NOTE: Previous must have been a digit so this cannot be valid.
        false
    }};

    (@internal $self:ident) => {
        is_lc!(@internal $self, $self.byte.index)
    };
}

/// Determine if a single digit separator is trailing.
///
/// # Examples
///
/// - `123_`- valid
/// - `123__`- invalid
/// - `123_.`- valid
/// - `123__.`- invalid
/// - `123_1`- invalid
/// - `123__1`- invalid
/// - _: valid
/// - _+: valid
/// - 1_+: valid
///
/// Having a subsequent sign character is fine since it might
/// be part of a partial parser.
///
/// # Preconditions
///
/// Assumes `slc[index]` is a digit separator.
macro_rules! is_t {
    (@first $self:ident, $index:expr) => {{
        // NOTE: The conditions here then are that:
        // - `index + 1` is not a digit
        // - `index + 1` is not a digit separator
        // - `index - 1` is not a digit separator

        let prev = indexing!(@prev $self, $index);
        let next = indexing!(@next $self, $index);
        let slc = $self.byte.slc;
        slc.get(next).map_or(true, |&x| !$self.is_digit(x) && !$self.is_digit_separator(x)) &&
            slc.get(prev).map_or(true, |&x| !$self.is_digit_separator(x))
    }};

    (@first $self:ident) => {
        is_t!(@first $self, $self.byte.index)
    };

    (@internal $self:ident, $index:expr) => {{
        // NOTE: We must have validated `prev`, so this just checks `next`.
        // NOTE: The conditions here then are that:
        // - `index + 1` is not a digit
        // - `index + 1` is not a digit separator
        let next = indexing!(@next $self, $index);
        let slc = $self.byte.slc;
        slc.get(next).map_or(true, |&x| !$self.is_digit(x) && !$self.is_digit_separator(x))
    }};

    (@internal $self:ident) => {
        is_t!(@internal $self, $self.byte.index)
    };
}

/// Determine if one or more digit separators are trailing.
///
/// # Examples
///
/// - `123_`- valid
/// - `123__`- valid
/// - `123_.`- valid
/// - `123__.`- valid
/// - `123_1`- invalid
/// - `123__1`- invalid
///
/// # Preconditions
///
/// Assumes `slc[index]` is a digit separator.
macro_rules! is_tc {
    (@first $self:ident, $index:expr) => {{
        // NOTE: The conditions here then are that:
        // - `index + 1` is not a digit

        let next = indexing!(@nextc $self, $index);
        let slc = $self.byte.slc;
        slc.get(next).map_or(true, |&x| !$self.is_digit(x))
    }};

    (@first $self:ident) => {
        is_tc!(@first $self, $self.byte.index)
    };

    (@internal $self:ident, $index:expr) => {
        // NOTE: This is already optimized for the first case.
        is_tc!(@first $self, $index)
    };

    (@internal $self:ident) => {
        is_tc!(@internal $self, $self.byte.index)
    };
}

/// Determine if the digit separator is leading or internal.
///
/// # Examples
///
/// - `__123`- invalid
/// - `+__123`- invalid
/// - `._123`- valid
/// - `_+123`- valid
/// - `_123`- valid
/// - `+_123`- valid
/// - `+1_23`- valid
/// - `+1__23`- invalid
/// - `+123_`- invalid
/// - `+123__`- invalid
/// - _: valid
/// - _+: valid
/// - 1_+: invalid
///
/// # Preconditions
///
/// Assumes `slc[index]` is a digit separator.
macro_rules! is_il {
    (@first $self:ident, $index:expr) => {{
        // NOTE: The conditions here then are that:
        // - `index + 1` is a digit
        // - `index + 1` is not a digit separator
        // - `index - 1` is not a digit separator
        //
        // # Logic
        //
        // If the previous character is a digit, then the
        // next character must be a digit. If the previous
        // character is not a digit, then the subsequent character can
        // be anything besides a digit separator

        let prev = indexing!(@prev $self, $index);
        let next = indexing!(@next $self, $index);
        let slc = $self.byte.slc;

        if slc.get(prev).map_or(false, |&x| $self.is_digit(x)) {
            slc.get(next).map_or(false, |&x| $self.is_digit(x))
        } else {
            slc.get(prev).map_or(true, |&x| !$self.is_digit_separator(x))
        }
    }};

    (@first $self:ident) => {
        is_il!(@first $self, $self.byte.index)
    };

    (@internal $self:ident, $index:expr) => {{
        // NOTE: We must have validated `prev`, so this just checks `next`.
        // NOTE: The conditions here then are that:
        // - `index + 1` is a digit

        let next = indexing!(@next $self, $index);
        let slc = $self.byte.slc;
        slc.get(next).map_or(false, |&x| $self.is_digit(x))
    }};

    (@internal $self:ident) => {
        is_il!(@internal $self, $self.byte.index)
    };
}

/// Determine if consecutive digit separators are leading or internal.
///
/// # Examples
///
/// - `__123`- valid
/// - `+__123`- valid
/// - `._123`- valid
/// - `_+123`- valid
/// - `_123`- valid
/// - `+_123`- valid
/// - `+1_23`- valid
/// - `+1__23`- valid
/// - `+123_`- invalid
/// - `+123__`- invalid
/// - _: valid
/// - _+: valid
/// - 1_+: invalid
///
/// # Preconditions
///
/// Assumes `slc[index]` is a digit separator.
macro_rules! is_ilc {
    (@first $self:ident, $index:expr) => {{
        // NOTE: The conditions here then are that:
        // - `index + 1` is a digit after consuming digit separators
        //
        // # Logic
        //
        // We also need to consider the case where it's empty,
        // that is, the previous one wasn't a digit if we don't
        // have a digit.

        let prev = indexing!(@prevc $self, $index);
        let next = indexing!(@nextc $self, $index);
        let slc = $self.byte.slc;
        slc.get(next).map_or(false, |&x| $self.is_digit(x)) ||
            slc.get(prev).map_or(true, |&x| !$self.is_digit(x))
    }};

    (@first $self:ident) => {
        is_ilc!(@first $self, $self.byte.index)
    };

    (@internal $self:ident, $index:expr) => {{
        // NOTE: The conditions here then are that:
        // - `index + 1` is a digit after consuming digit separators

        let next = indexing!(@nextc $self, $index);
        let slc = $self.byte.slc;
        slc.get(next).map_or(true, |&x| $self.is_digit(x))
    }};

    (@internal $self:ident) => {
        is_ilc!(@internal $self, $self.byte.index)
    };
}

/// Determine if the digit separator is internal or trailing.
///
/// # Examples
///
/// - `__123`- valid
/// - `+__123`- valid
/// - `._123`- valid
/// - `_+123`- valid
/// - `_123`- valid
/// - `+_123`- valid
/// - `+1_23`- valid
/// - `+1__23`- valid
/// - `+123_`- invalid
/// - `+123__`- invalid
/// - _: valid
/// - _+: valid
/// - 1_+: invalid
///
/// # Preconditions
///
/// Assumes `slc[index]` is a digit separator.
macro_rules! is_it {
    (@first$self:ident, $index:expr) => {{
        // NOTE: The conditions here then are that:
        // - `index - 1` is a digit
        // - `index - 1` is not a digit separator
        // - `index + 1` is not a digit separator
        //
        // # Logic
        //
        // If the previous character is not a digit, there cannot
        // be a digit for a following character. If the previous
        // character is a digit, then the following one must be
        // a digit as well.

        let prev = indexing!(@prev $self, $index);
        let next = indexing!(@next $self, $index);
        let slc = $self.byte.slc;
        if slc.get(prev).map_or(false, |&x| $self.is_digit(x)) {
            // Have a digit, any character besides a digit separator is valid
            slc.get(next).map_or(true, |&x| !$self.is_digit_separator(x))
        } else {
            // Not a digit, so we cannot have a digit or a digit separator
            slc.get(next).map_or(true, |&x| !$self.is_digit(x) && !$self.is_digit_separator(x))
        }
    }};

    (@first$self:ident) => {
        is_it!(@first $self, $self.byte.index)
    };

    (@internal $self:ident, $index:expr) => {{
        // NOTE: We must have validated `prev`, so this just checks `next`.
        // NOTE: The conditions here then are that:
        // - `index + 1` is not a digit separator
        // Since we've previously had a digit, this is guaranteed to
        // be internal or trailing.

        let next = indexing!(@next $self, $index);
        let slc = $self.byte.slc;
        slc.get(next).map_or(true, |&x| !$self.is_digit_separator(x))
    }};

    (@internal $self:ident) => {
        is_it!(@internal $self, $self.byte.index)
    };
}

/// Determine if consecutive digit separators are internal or trailing.
///
/// # Examples
///
/// - `__123`- invalid
/// - `+__123`- invalid
/// - `._123`- invalid
/// - `_+123`- invalid
/// - `_123`- invalid
/// - `+_123`- invalid
/// - `+1_23`- valid
/// - `+1__23`- valid
/// - `+123_`- valid
/// - `+123__`- valid
/// - _: valid
/// - _+: valid
/// - 1_+: valid
///
/// # Preconditions
///
/// Assumes `slc[index]` is a digit separator.
macro_rules! is_itc {
    (@first $self:ident, $index:expr) => {{
        // NOTE: The conditions here then are that:
        // - `index - 1` is not a digit after consuming digit separators
        //
        // # Logic
        //
        // We also need to consider the case where it's empty,
        // that is, the previous one wasn't a digit if we don't
        // have a digit.

        let prev = indexing!(@prevc $self, $index);
        let next = indexing!(@nextc $self, $index);
        let slc = $self.byte.slc;
        slc.get(prev).map_or(false, |&x| !$self.is_digit(x)) ||
            slc.get(next).map_or(true, |&x| !$self.is_digit(x))
    }};

    (@first $self:ident) => {
        is_itc!(@first $self, $self.byte.index)
    };

    (@internal $self:ident, $index:expr) => {
        // NOTE: Previous must have been a digit so this must be valid.
        true
    };

    (@internal $self:ident) => {
        is_itc!(@internal $self, $self.byte.index)
    };
}

/// Determine if the digit separator is leading or trailing.
///
/// # Examples
///
/// - `__123`- invalid
/// - `+__123`- invalid
/// - `._123`- valid
/// - `_+123`- valid
/// - `_123`- valid
/// - `+_123`- valid
/// - `+1_23`- invalid
/// - `+1__23`- invalid
/// - `+123_`- valid
/// - `+123__`- invalid
/// - _: valid
/// - _+: valid
/// - 1_+: valid
///
/// # Preconditions
///
/// Assumes `slc[index]` is a digit separator.
macro_rules! is_lt {
    (@first $self:ident, $index:expr) => {{
        // NOTE: The conditions here then are that:
        // - not (`index - 1` is a digit and `index + 1` is a digit)
        // - `index - 1` is not a digit separator
        // - `index + 1` is not a digit separator

        let prev = indexing!(@prev $self, $index);
        let next = indexing!(@next $self, $index);
        let slc = $self.byte.slc;
        let prev_value = slc.get(prev);
        let next_value = slc.get(next);

        let is_prev_sep = prev_value.map_or(false, |&x| $self.is_digit_separator(x));
        let is_prev_dig = prev_value.map_or(false, |&x| $self.is_digit(x));
        let is_next_sep = next_value.map_or(false, |&x| $self.is_digit_separator(x));
        let is_next_dig = next_value.map_or(false, |&x| $self.is_digit(x));

        !is_prev_sep && !is_next_sep && !(is_prev_dig && is_next_dig)
    }};

    (@first $self:ident) => {
        is_lt!(@first $self, $self.byte.index)
    };

    (@internal $self:ident, $index:expr) => {{
        // NOTE: We must have validated `prev`, so this just checks `next`.
        // NOTE: The conditions here then are that:
        // - `index + 1` is not a digit
        // - `index + 1` is not a digit separator

        let next = indexing!(@next $self, $index);
        let slc = $self.byte.slc;
        slc.get(next).map_or(true, |&x| !$self.is_digit(x) && !$self.is_digit_separator(x))
    }};

    (@internal $self:ident) => {
        is_lt!(@internal $self, $self.byte.index)
    };
}

/// Determine if consecutive digit separators are leading or trailing.
///
/// # Examples
///
/// - `__123`- valid
/// - `+__123`- valid
/// - `._123`- valid
/// - `_+123`- valid
/// - `_123`- valid
/// - `+_123`- valid
/// - `+1_23`- invalid
/// - `+1__23`- invalid
/// - `+123_`- valid
/// - `+123__`- valid
/// - _: valid
/// - _+: valid
/// - 1_+: valid
///
/// # Preconditions
///
/// Assumes `slc[index]` is a digit separator.
macro_rules! is_ltc {
    (@first $self:ident, $index:expr) => {{
        // NOTE: The conditions here then are that (after consuming separators):
        // - not (`index - 1` is a digit and `index + 1` is a digit)

        let prev = indexing!(@prevc $self, $index);
        let next = indexing!(@nextc $self, $index);
        let slc = $self.byte.slc;
        !(slc.get(prev).map_or(false, |&x| $self.is_digit(x)) && slc.get(next).map_or(false, |&x| $self.is_digit(x)))
    }};

    (@first $self:ident) => {
        is_ltc!(@first $self, $self.byte.index)
    };

    (@internal $self:ident, $index:expr) => {{
        // NOTE: We must have validated `prev`, so this just checks `next`.
        // NOTE: The conditions here then are that:
        // - `index + 1` is not a digit

        let next = indexing!(@nextc $self, $index);
        let slc = $self.byte.slc;
        slc.get(next).map_or(true, |&x| !$self.is_digit(x))
    }};

    (@internal $self:ident) => {
        is_ltc!(@internal $self, $self.byte.index)
    };
}

/// Determine if a single digit separator is internal, leading, or trailing.
///
/// # Examples
///
/// - `__123`- invalid
/// - `+__123`- invalid
/// - `._123`- valid
/// - `_+123`- valid
/// - `_123`- valid
/// - `+_123`- valid
/// - `+1_23`- valid
/// - `+1__23`- invalid
/// - `+123_`- valid
/// - `+123__`- invalid
/// - _: valid
/// - _+: valid
/// - 1_+: valid
///
/// # Preconditions
///
/// Assumes `slc[index]` is a digit separator.
macro_rules! is_ilt {
    (@first $self:ident, $index:expr) => {{
        // NOTE: The conditions here then are that:
        // - `index + 1` is not a digit separator
        // - `index - 1` is not a digit separator

        let prev = indexing!(@prev $self, $index);
        let next = indexing!(@next $self, $index);
        let slc = $self.byte.slc;
        !slc.get(next).map_or(false, |&x| $self.is_digit_separator(x)) &&
            !slc.get(prev).map_or(false, |&x| $self.is_digit_separator(x))
    }};

    (@first $self:ident) => {
        is_ilt!(@first $self, $self.byte.index)
    };

    (@internal $self:ident, $index:expr) => {{
        // NOTE: We must have validated `prev`, so this just checks `next`.
        // NOTE: The conditions here then are that:
        // - `index + 1` is not a digit separator

        let next = indexing!(@next $self, $index);
        let slc = $self.byte.slc;
        slc.get(next).map_or(true, |&x| !$self.is_digit_separator(x))
    }};

    (@internal $self:ident) => {
        is_ilt!(@internal $self, $self.byte.index)
    };
}

/// Determine if consecutive digit separators are internal, leading, or
/// trailing.
///
/// This is always true.
///
/// # Examples
///
/// - `__123`- valid
/// - `+__123`- valid
/// - `._123`- valid
/// - `_+123`- valid
/// - `_123`- valid
/// - `+_123`- valid
/// - `+1_23`- valid
/// - `+1__23`- valid
/// - `+123_`- valid
/// - `+123__`- valid
/// - _: valid
/// - _+: valid
/// - 1_+: valid
///
/// # Preconditions
///
/// Assumes `slc[index]` is a digit separator.
macro_rules! is_iltc {
    (@first $self:ident, $index:expr) => {
        true
    };

    (@first $self:ident) => {
        is_iltc!(@first $self, $self.byte.index)
    };

    (@internal $self:ident, $index:expr) => {
        true
    };

    (@internal $self:ident) => {
        is_iltc!(@internal $self, $self.byte.index)
    };
}

// PEEK
// ----

/// Consumes 1 or more digit separators.
/// Peeks the next token that's not a digit separator.
macro_rules! peek_1 {
    ($self:ident, $is_skip:ident) => {{
        // This will consume a single, non-consecutive digit separators.
        let index = $self.cursor();
        let buffer = $self.get_buffer();
        let value = buffer.get(index)?;
        let is_digit_separator = $self.is_digit_separator(*value);
        // NOTE: We can do some pretty major optimizations for internal values,
        // since we can check the location and don't need to check previous values.
        if is_digit_separator {
            // NOTE: This cannot iteratively search for the next value,
            // or else the consecutive digit separator has no effect (#96).
            let is_skip = if $self.current_count() == 0 {
                $is_skip!(@first $self)
            } else {
                $is_skip!(@internal $self)
            };
            if is_skip {
                // SAFETY: Safe since `index < buffer.len()`, so `index + 1 <= buffer.len()``
                unsafe { $self.set_cursor(index + 1) };
                buffer.get(index + 1)
            } else {
                Some(value)
            }
        } else {
            // Have 1 of 2 conditions:
            //  1. A non-digit separator character.
            //  2. A digit separator that is not valid in the context.
            Some(value)
        }
    }};
}

/// Consumes 1 or more digit separators.
/// Peeks the next token that's not a digit separator.
macro_rules! peek_n {
    ($self:ident, $is_skip:ident) => {{
        // This will consume consecutive digit separators.
        let mut index = $self.cursor();
        let buffer = $self.get_buffer();
        let value = buffer.get(index)?;
        let is_digit_separator = $self.is_digit_separator(*value);
        // NOTE: We can do some pretty major optimizations for internal values,
        // since we can check the location and don't need to check previous values.
        if is_digit_separator {
            let is_skip = if $self.current_count() == 0 {
                $is_skip!(@first $self)
            } else {
                $is_skip!(@internal $self)
            };
            if is_skip {
                // Have a skippable digit separator: keep incrementing until we find
                // a non-digit separator character. Don't need any complex checks
                // here, since we've already done them above.
                index += 1;
                while index < buffer.len()
                    && buffer.get(index).map_or(false, |&x| $self.is_digit_separator(x))
                {
                    index += 1;
                }
                // SAFETY: Safe since `index <= buffer.len()`.
                unsafe { $self.set_cursor(index) };
                buffer.get(index)
            } else {
                Some(value)
            }
        } else {
            // Have 1 of 2 conditions:
            //  1. A non-digit separator character.
            //  2. A digit separator that is not valid in the context.
            Some(value)
        }
    }};
}

/// Consumes no digit separators and peeks the next value.
macro_rules! peek_noskip {
    ($self:ident) => {
        $self.byte.slc.get($self.byte.index)
    };
}

/// Consumes at most 1 leading digit separator and peeks the next value.
macro_rules! peek_l {
    ($self:ident) => {
        peek_1!($self, is_l)
    };
}

/// Consumes at most 1 internal digit separator and peeks the next value.
macro_rules! peek_i {
    ($self:ident) => {
        peek_1!($self, is_i)
    };
}

/// Consumes at most 1 trailing digit separator and peeks the next value.
macro_rules! peek_t {
    ($self:ident) => {
        peek_1!($self, is_t)
    };
}

/// Consumes at most 1 internal/leading digit separator and peeks the next
/// value.
macro_rules! peek_il {
    ($self:ident) => {
        peek_1!($self, is_il)
    };
}

/// Consumes at most 1 internal/trailing digit separator and peeks the next
/// value.
macro_rules! peek_it {
    ($self:ident) => {
        peek_1!($self, is_it)
    };
}

/// Consumes at most 1 leading/trailing digit separator and peeks the next
/// value.
macro_rules! peek_lt {
    ($self:ident) => {
        peek_1!($self, is_lt)
    };
}

/// Consumes at most 1 digit separator and peeks the next value.
macro_rules! peek_ilt {
    ($self:ident) => {
        peek_1!($self, is_ilt)
    };
}

/// Consumes 1 or more leading digit separators and peeks the next value.
macro_rules! peek_lc {
    ($self:ident) => {
        peek_n!($self, is_lc)
    };
}

/// Consumes 1 or more internal digit separators and peeks the next value.
macro_rules! peek_ic {
    ($self:ident) => {
        peek_n!($self, is_ic)
    };
}

/// Consumes 1 or more trailing digit separators and peeks the next value.
macro_rules! peek_tc {
    ($self:ident) => {
        peek_n!($self, is_tc)
    };
}

/// Consumes 1 or more internal/leading digit separators and peeks the next
/// value.
macro_rules! peek_ilc {
    ($self:ident) => {
        peek_n!($self, is_ilc)
    };
}

/// Consumes 1 or more internal/trailing digit separators and peeks the next
/// value.
macro_rules! peek_itc {
    ($self:ident) => {
        peek_n!($self, is_itc)
    };
}

/// Consumes 1 or more leading/trailing digit separators and peeks the next
/// value.
macro_rules! peek_ltc {
    ($self:ident) => {
        peek_n!($self, is_ltc)
    };
}

/// Consumes 1 or more digit separators and peeks the next value.
macro_rules! peek_iltc {
    ($self:ident) => {
        peek_n!($self, is_iltc)
    };
}

// AS DIGITS
// ---------

/// Trait to simplify creation of a `Bytes` object.
pub trait AsBytes<'a> {
    /// Create `Bytes` from object.
    fn bytes<const FORMAT: u128>(&'a self) -> Bytes<'a, FORMAT>;
}

impl<'a> AsBytes<'a> for [u8] {
    #[inline(always)]
    fn bytes<const FORMAT: u128>(&'a self) -> Bytes<'a, FORMAT> {
        Bytes::new(self)
    }
}

// DIGITS
// ------

/// Slice iterator that skips characters matching a given value.
///
/// This wraps an iterator over a contiguous block of memory,
/// and only returns values that are not equal to skip.
///
/// The format allows us to dictate the actual behavior of
/// the iterator: in what contexts does it skip digit separators.
///
/// `FORMAT` is required to tell us what the digit separator is, and where
/// the digit separators are allowed, as well tell us the radix.
/// The radix is required to allow us to differentiate digit from
/// non-digit characters (see [`DigitSeparators`](/docs/DigitSeparators.md)
/// for a detailed explanation on why).
#[derive(Clone)]
pub struct Bytes<'a, const FORMAT: u128> {
    /// The raw slice for the iterator.
    slc: &'a [u8],
    /// Current index of the iterator in the slice.
    index: usize,
    /// The current count of integer digits returned by the iterator.
    /// This is only used if the iterator is not contiguous.
    integer_count: usize,
    /// The current count of fraction digits returned by the iterator.
    /// This is only used if the iterator is not contiguous.
    fraction_count: usize,
    /// The current count of exponent digits returned by the iterator.
    /// This is only used if the iterator is not contiguous.
    exponent_count: usize,
}

impl<'a, const FORMAT: u128> Bytes<'a, FORMAT> {
    /// Create new byte object.
    #[inline(always)]
    pub const fn new(slc: &'a [u8]) -> Self {
        Self {
            slc,
            index: 0,
            integer_count: 0,
            fraction_count: 0,
            exponent_count: 0,
        }
    }

    /// Initialize the slice from raw parts.
    ///
    /// # Safety
    /// This is safe if and only if the index is <= `slc.len()`.
    /// For this reason, since it's easy to get wrong, we only
    /// expose it to our `DigitsIterator`s and nothing else.
    ///
    /// This is only ever used for contiguous iterators. However,
    /// it's not guaranteed to only valid for our contiguous
    /// iterators.
    #[inline(always)]
    const unsafe fn from_parts(slc: &'a [u8], index: usize) -> Self {
        debug_assert!(index <= slc.len());
        Self {
            slc,
            index,
            integer_count: 0,
            fraction_count: 0,
            exponent_count: 0,
        }
    }

    /// Get iterator over integer digits.
    #[inline(always)]
    pub fn integer_iter<'b>(&'b mut self) -> IntegerDigitsIterator<'a, 'b, FORMAT> {
        IntegerDigitsIterator {
            byte: self,
        }
    }

    /// Get iterator over fraction digits.
    #[inline(always)]
    pub fn fraction_iter<'b>(&'b mut self) -> FractionDigitsIterator<'a, 'b, FORMAT> {
        FractionDigitsIterator {
            byte: self,
        }
    }

    /// Get iterator over exponent digits.
    #[inline(always)]
    pub fn exponent_iter<'b>(&'b mut self) -> ExponentDigitsIterator<'a, 'b, FORMAT> {
        ExponentDigitsIterator {
            byte: self,
        }
    }

    /// Get iterator over special floating point values.
    #[inline(always)]
    pub fn special_iter<'b>(&'b mut self) -> SpecialDigitsIterator<'a, 'b, FORMAT> {
        SpecialDigitsIterator {
            byte: self,
        }
    }

    /// Internal implementation that handles if it's contiguous.
    ///
    /// # Safety
    ///
    /// Safe if the buffer has at least `N` elements.
    #[inline(always)]
    unsafe fn step_by_unchecked_impl(&mut self, count: usize, is_contiguous: bool) {
        // NOTE: THIS IS NOT a duplicate calling `step_by_unchecked` from a digits
        // iterator on the byte, since they can have different contiguousness.
        if is_contiguous {
            // Contiguous, can skip most of these checks.
            debug_assert!(self.as_slice().len() >= count);
        } else {
            // Since this isn't contiguous, it only works
            // if the value is in the range `[0, 1]`.
            // We also need to make sure the **current** value
            // isn't a digit separator.
            let format = NumberFormat::<{ FORMAT }> {};
            debug_assert!(self.as_slice().len() >= count);
            debug_assert!(count == 0 || count == 1);
            debug_assert!(
                count == 0 || self.slc.get(self.index) != Some(&format.digit_separator())
            );
        }
        self.index += count;
    }

    /// Internal implementation that handles if it's contiguous.
    ///
    /// If it's contiguous or not does not affect the safety guarantees,
    /// however, it can affect correctness.
    ///
    /// # Safety
    ///
    /// Safe if the buffer has at least `size_of::<V>` elements.
    #[inline(always)]
    unsafe fn peek_many_unchecked_impl<V>(&self, is_contiguous: bool) -> V {
        // NOTE: THIS IS NOT a duplicate calling `peek_many_unchecked` from a digits
        // iterator on the byte, since they can have different contiguousness.
        debug_assert!(is_contiguous);
        debug_assert!(self.as_slice().len() >= mem::size_of::<V>());

        let slc = self.as_slice();
        // SAFETY: safe as long as the slice has at least count elements.
        unsafe { ptr::read_unaligned::<V>(slc.as_ptr() as *const _) }
    }
}

unsafe impl<'a, const FORMAT: u128> Iter<'a> for Bytes<'a, FORMAT> {
    /// If each yielded value is adjacent in memory.
    const IS_CONTIGUOUS: bool = NumberFormat::<{ FORMAT }>::DIGIT_SEPARATOR == 0;

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
        // If the buffer is contiguous, then we don't need to track the
        // number of values: the current index is enough.
        if Self::IS_CONTIGUOUS {
            self.index
        } else {
            self.integer_count + self.fraction_count + self.exponent_count
        }
    }

    #[inline(always)]
    unsafe fn step_by_unchecked(&mut self, count: usize) {
        // SAFETY: Safe if the buffer has at least `N` elements.
        unsafe { self.step_by_unchecked_impl(count, Self::IS_CONTIGUOUS) }
    }

    #[inline(always)]
    unsafe fn peek_many_unchecked<V>(&self) -> V {
        // SAFETY: Safe if the buffer has at least `size_of::<V>` elements.
        unsafe { self.peek_many_unchecked_impl(Self::IS_CONTIGUOUS) }
    }
}

// ITERATOR HELPERS
// ----------------

/// Create skip iterator definition.
macro_rules! skip_iterator {
    ($iterator:ident, $doc:literal) => {
        #[doc = $doc]
        pub struct $iterator<'a: 'b, 'b, const FORMAT: u128> {
            /// The internal byte object for the skip iterator.
            byte: &'b mut Bytes<'a, FORMAT>,
        }
    };
}

macro_rules! is_sign {
    () => {
        pub const fn is_sign(&self, value: u8) -> bool {
            matches!(value, b'+' | b'-')
        }
    };
}

macro_rules! is_digit_separator {
    ($format:ident) => {
        /// Determine if the character is a digit separator.
        pub const fn is_digit_separator(&self, value: u8) -> bool {
            let format = NumberFormat::<{ $format }> {};
            let digit_separator = format.digit_separator();
            if digit_separator == 0 {
                // Check at compile time if we have an invalid digit separator.
                // b'\x00', or the NUL character, is this invalid value.
                false
            } else {
                value == digit_separator
            }
        }
    };
}

/// Create impl block for skip iterator.
macro_rules! skip_iterator_impl {
    ($iterator:ident, $radix_cb:ident) => {
        impl<'a: 'b, 'b, const FORMAT: u128> $iterator<'a, 'b, FORMAT> {
            is_sign!();
            is_digit_separator!(FORMAT);

            /// Create a new digits iterator from the bytes underlying item.
            #[inline(always)]
            pub fn new(byte: &'b mut Bytes<'a, FORMAT>) -> Self {
                Self {
                    byte,
                }
            }

            /// Take the first N digits from the iterator.
            ///
            /// This only takes the digits if we have a contiguous iterator.
            /// It takes the digits, validating the bounds, and then advanced
            /// the iterators state. It does not support non-contiguous iterators
            /// since we would lose information on the count.
            #[cfg_attr(not(feature = "compact"), inline(always))]
            #[allow(clippy::assertions_on_constants)] // reason="ensuring safety invariants are valid"
            pub fn take_n(&mut self, n: usize) -> Option<Bytes<'a, FORMAT>> {
                if Self::IS_CONTIGUOUS {
                    let end = self.byte.slc.len().min(n + self.cursor());
                    // NOTE: The compiler should be able to optimize this out.
                    let slc: &[u8] = &self.byte.slc[..end];

                    // SAFETY: Safe since we just ensured the underlying slice has that count
                    // elements, so both the underlying slice for this and this **MUST**
                    // have at least count elements. We do static checking on the bounds for this.
                    unsafe {
                        let byte: Bytes<'_, FORMAT> = Bytes::from_parts(slc, self.cursor());
                        unsafe { self.set_cursor(end) };
                        Some(byte)
                    }
                } else {
                    None
                }
            }
        }
    };
}

/// Create impl Iterator block for skip iterator.
macro_rules! skip_iterator_iterator_impl {
    ($iterator:ident) => {
        impl<'a: 'b, 'b, const FORMAT: u128> Iterator for $iterator<'a, 'b, FORMAT> {
            type Item = &'a u8;

            #[inline(always)]
            fn next(&mut self) -> Option<Self::Item> {
                // Peek will handle everything properly internally.
                let value = self.peek()?;
                // Increment the index so we know not to re-fetch it.
                self.byte.index += 1;
                // NOTE: Only increment the count if it's not contiguous, otherwise,
                // this is an unnecessary performance penalty. We also need
                // to check if it's a digit, which adds on additional cost but
                // there's not much else we can do. Hopefully the previous inlining
                // checks will minimize the performance hit.
                if !Self::IS_CONTIGUOUS && self.is_digit(*value) {
                    self.increment_count();
                }
                Some(value)
            }
        }
    };
}

/// Create base methods for the Iter block of a skip iterator.
macro_rules! skip_iterator_iter_base {
    ($format:ident, $mask:ident, $count:ident) => {
        // It's contiguous if we don't skip over any values.
        // IE, the digit separator flags for the iterator over
        // the digits doesn't skip any values.
        const IS_CONTIGUOUS: bool = $format & flags::$mask == 0;

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

        /// Get the current number of digits returned by the iterator.
        ///
        /// For contiguous iterators, this can include the sign character, decimal
        /// point, and the exponent sign (that is, it is always the cursor). For
        /// non-contiguous iterators, this must always be the only the number of
        /// digits returned.
        #[inline(always)]
        fn current_count(&self) -> usize {
            if Self::IS_CONTIGUOUS {
                self.byte.current_count()
            } else {
                self.byte.$count
            }
        }

        #[inline(always)]
        unsafe fn step_by_unchecked(&mut self, count: usize) {
            // SAFETY: Safe if the buffer has at least `N` elements.
            unsafe { self.byte.step_by_unchecked_impl(count, Self::IS_CONTIGUOUS) }
        }

        #[inline(always)]
        unsafe fn peek_many_unchecked<V>(&self) -> V {
            // SAFETY: Safe if the buffer has at least `size_of::<V>` elements.
            unsafe { self.byte.peek_many_unchecked_impl(Self::IS_CONTIGUOUS) }
        }
    };
}

/// Create base methods for the `DigitsIter` block of a skip iterator.
macro_rules! skip_iterator_digits_iter_base {
    () => {
        #[inline(always)]
        fn is_consumed(&mut self) -> bool {
            self.peek().is_none()
        }
    };
}

/// Create impl `ByteIter` block for skip iterator.
macro_rules! skip_iterator_bytesiter_impl {
    ($iterator:ident, $mask:ident, $count:ident, $i:ident, $l:ident, $t:ident, $c:ident) => {
        unsafe impl<'a: 'b, 'b, const FORMAT: u128> Iter<'a> for $iterator<'a, 'b, FORMAT> {
            skip_iterator_iter_base!(FORMAT, $mask, $count);
        }

        impl<'a: 'b, 'b, const FORMAT: u128> DigitsIter<'a> for $iterator<'a, 'b, FORMAT> {
            skip_iterator_digits_iter_base!();

            /// Increment the number of digits that have been returned by the iterator.
            ///
            /// For contiguous iterators, this is a no-op. For non-contiguous iterators,
            /// this increments the count by 1.
            #[inline(always)]
            fn increment_count(&mut self) {
                self.byte.$count += 1;
            }

            /// Peek the next value of the iterator, without consuming it.
            ///
            /// Note that this can modify the internal state, by skipping digits
            /// for iterators that find the first non-zero value, etc. We optimize
            /// this for the case where we have contiguous iterators, since
            /// non-contiguous iterators already have a major performance penalty.
            ///
            /// Checking if the character is a digit in the `next()` implementation
            /// after skipping characters can:
            /// 1. Likely be optimized out due to the use of macros and inlining.
            /// 2. Is a small amount of overhead compared to the branching on
            ///    characters,
            #[inline(always)]
            fn peek(&mut self) -> Option<<Self as Iterator>::Item> {
                let format = NumberFormat::<{ FORMAT }> {};
                const I: u128 = flags::$i;
                const L: u128 = flags::$l;
                const T: u128 = flags::$t;
                const C: u128 = flags::$c;
                const IL: u128 = I | L;
                const IT: u128 = I | T;
                const LT: u128 = L | T;
                const ILT: u128 = I | L | T;
                const IC: u128 = I | C;
                const LC: u128 = L | C;
                const TC: u128 = T | C;
                const ILC: u128 = IL | C;
                const ITC: u128 = IT | C;
                const LTC: u128 = LT | C;
                const ILTC: u128 = ILT | C;

                match format.digit_separator_flags() & flags::$mask {
                    0 => peek_noskip!(self),
                    I => peek_i!(self),
                    L => peek_l!(self),
                    T => peek_t!(self),
                    IL => peek_il!(self),
                    IT => peek_it!(self),
                    LT => peek_lt!(self),
                    ILT => peek_ilt!(self),
                    IC => peek_ic!(self),
                    LC => peek_lc!(self),
                    TC => peek_tc!(self),
                    ILC => peek_ilc!(self),
                    ITC => peek_itc!(self),
                    LTC => peek_ltc!(self),
                    ILTC => peek_iltc!(self),
                    _ => unreachable!(),
                }
            }

            /// Determine if the character is a digit.
            #[inline(always)]
            fn is_digit(&self, value: u8) -> bool {
                let format = NumberFormat::<{ FORMAT }> {};
                char_is_digit_const(value, format.mantissa_radix())
            }
        }
    };
}

// INTEGER DIGITS ITERATOR
// -----------------------

skip_iterator!(IntegerDigitsIterator, "Iterator that skips over digit separators in the integer.");
skip_iterator_impl!(IntegerDigitsIterator, mantissa_radix);
skip_iterator_iterator_impl!(IntegerDigitsIterator);
skip_iterator_bytesiter_impl!(
    IntegerDigitsIterator,
    INTEGER_DIGIT_SEPARATOR_FLAG_MASK,
    integer_count,
    INTEGER_INTERNAL_DIGIT_SEPARATOR,
    INTEGER_LEADING_DIGIT_SEPARATOR,
    INTEGER_TRAILING_DIGIT_SEPARATOR,
    INTEGER_CONSECUTIVE_DIGIT_SEPARATOR
);

// FRACTION DIGITS ITERATOR
// ------------------------

skip_iterator!(
    FractionDigitsIterator,
    "Iterator that skips over digit separators in the fraction."
);
skip_iterator_impl!(FractionDigitsIterator, mantissa_radix);
skip_iterator_iterator_impl!(FractionDigitsIterator);
skip_iterator_bytesiter_impl!(
    FractionDigitsIterator,
    FRACTION_DIGIT_SEPARATOR_FLAG_MASK,
    fraction_count,
    FRACTION_INTERNAL_DIGIT_SEPARATOR,
    FRACTION_LEADING_DIGIT_SEPARATOR,
    FRACTION_TRAILING_DIGIT_SEPARATOR,
    FRACTION_CONSECUTIVE_DIGIT_SEPARATOR
);

// EXPONENT DIGITS ITERATOR
// ------------------------

skip_iterator!(
    ExponentDigitsIterator,
    "Iterator that skips over digit separators in the exponent."
);
skip_iterator_impl!(ExponentDigitsIterator, exponent_radix);
skip_iterator_iterator_impl!(ExponentDigitsIterator);
skip_iterator_bytesiter_impl!(
    ExponentDigitsIterator,
    EXPONENT_DIGIT_SEPARATOR_FLAG_MASK,
    exponent_count,
    EXPONENT_INTERNAL_DIGIT_SEPARATOR,
    EXPONENT_LEADING_DIGIT_SEPARATOR,
    EXPONENT_TRAILING_DIGIT_SEPARATOR,
    EXPONENT_CONSECUTIVE_DIGIT_SEPARATOR
);

// SPECIAL DIGITS ITERATOR
// -----------------------

skip_iterator!(
    SpecialDigitsIterator,
    "Iterator that skips over digit separators in special floats."
);
skip_iterator_iterator_impl!(SpecialDigitsIterator);

impl<'a: 'b, 'b, const FORMAT: u128> SpecialDigitsIterator<'a, 'b, FORMAT> {
    is_sign!();
    is_digit_separator!(FORMAT);
}

unsafe impl<'a: 'b, 'b, const FORMAT: u128> Iter<'a> for SpecialDigitsIterator<'a, 'b, FORMAT> {
    skip_iterator_iter_base!(FORMAT, SPECIAL_DIGIT_SEPARATOR, integer_count);
}

impl<'a: 'b, 'b, const FORMAT: u128> DigitsIter<'a> for SpecialDigitsIterator<'a, 'b, FORMAT> {
    skip_iterator_digits_iter_base!();

    // Always a no-op.
    #[inline(always)]
    fn increment_count(&mut self) {
    }

    /// Peek the next value of the iterator, without consuming it.
    #[inline(always)]
    fn peek(&mut self) -> Option<<Self as Iterator>::Item> {
        let format = NumberFormat::<{ FORMAT }> {};
        if format.special_digit_separator() {
            peek_iltc!(self)
        } else {
            peek_noskip!(self)
        }
    }

    /// Determine if the character is a digit.
    #[inline(always)]
    fn is_digit(&self, value: u8) -> bool {
        let format = NumberFormat::<{ FORMAT }> {};
        char_is_digit_const(value, format.mantissa_radix())
    }
}
