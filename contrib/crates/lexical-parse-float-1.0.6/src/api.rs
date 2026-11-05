////! Implements the algorithm in terms of the lexical API.

#![doc(hidden)]

#[cfg(feature = "f16")]
use lexical_util::bf16::bf16;
use lexical_util::error::Error;
#[cfg(feature = "f16")]
use lexical_util::f16::f16;
use lexical_util::format::{is_valid_options_punctuation, NumberFormat, STANDARD};
use lexical_util::{from_lexical, from_lexical_with_options};

use crate::options::Options;
use crate::parse::ParseFloat;

// API

const DEFAULT_OPTIONS: Options = Options::new();

/// Implement `FromLexical` for numeric type.
///
/// Need to inline these, otherwise code generation is sub-optimal.
/// For some reason, it can't determine some of the const evaluations
/// can actually be evaluated at compile-time, which causes major branching
/// issues.
macro_rules! float_from_lexical {
    ($($t:ident)*) => ($(
        impl FromLexical for $t {
            #[cfg_attr(not(feature = "compact"), inline)]
            fn from_lexical(bytes: &[u8]) -> lexical_util::result::Result<Self>
            {
                Self::parse_complete::<STANDARD>(bytes, &DEFAULT_OPTIONS)
            }

            #[cfg_attr(not(feature = "compact"), inline)]
            fn from_lexical_partial(
                bytes: &[u8],
            ) -> lexical_util::result::Result<(Self, usize)>
            {
                Self::parse_partial::<STANDARD>(bytes, &DEFAULT_OPTIONS)
            }
        }

        impl FromLexicalWithOptions for $t {
            type Options = Options;

            #[cfg_attr(not(feature = "compact"), inline)]
            fn from_lexical_with_options<const FORMAT: u128>(
                bytes: &[u8],
                options: &Self::Options,
            ) -> lexical_util::result::Result<Self>
            {
                let format = NumberFormat::<{ FORMAT }> {};
                if !format.is_valid() {
                    return Err(format.error());
                } else if !is_valid_options_punctuation(FORMAT, options.exponent(), options.decimal_point()) {
                    return Err(Error::InvalidPunctuation);
                }
                Self::parse_complete::<FORMAT>(bytes, options)
            }

            #[cfg_attr(not(feature = "compact"), inline)]
            fn from_lexical_partial_with_options<const FORMAT: u128>(
                bytes: &[u8],
                options: &Self::Options,
            ) -> lexical_util::result::Result<(Self, usize)>
            {
                Self::parse_partial::<FORMAT>(bytes, options)
            }
        }
    )*)
}

from_lexical!("lexical_parse_float", 1.234, f64, 5);
from_lexical_with_options!("lexical_parse_float", 1.234, f64, 5, Options);
float_from_lexical! { f32 f64 }

#[cfg(feature = "f16")]
float_from_lexical! { bf16 f16 }
