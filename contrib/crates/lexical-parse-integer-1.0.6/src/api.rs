////! Implements the algorithm in terms of the lexical API.

#![doc(hidden)]

use lexical_util::format::{NumberFormat, STANDARD};
use lexical_util::{from_lexical, from_lexical_with_options};

use crate::options::{Options, STANDARD as DEFAULT_OPTIONS};
use crate::parse::ParseInteger;

/// Implement `FromLexical` for numeric type.
///
/// Need to inline these, otherwise code generation is sub-optimal.
/// For some reason, it can't determine some of the const evaluations
/// can actually be evaluated at compile-time, which causes major branching
/// issues.
macro_rules! integer_from_lexical {
    ($($t:ident $unsigned:ident ; )*) => ($(
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
                }
                Self::parse_complete::<FORMAT>(bytes, options)
            }

            #[cfg_attr(not(feature = "compact"), inline)]
            fn from_lexical_partial_with_options<const FORMAT: u128>(
                bytes: &[u8],
                options: &Self::Options,
            ) -> lexical_util::result::Result<(Self, usize)>
            {
                let format = NumberFormat::<{ FORMAT }> {};
                if !format.is_valid() {
                    return Err(format.error());
                }
                Self::parse_partial::<FORMAT>(bytes, options)
            }
        }
    )*)
}

from_lexical!("lexical_parse_integer", 1234, u64, 4);
from_lexical_with_options!("lexical_parse_integer", 1234, u64, 4, Options);
integer_from_lexical! {
    u8 u8 ;
    u16 u16 ;
    u32 u32 ;
    u64 u64 ;
    u128 u128 ;
    usize usize ;
    i8 u8 ;
    i16 u16 ;
    i32 u32 ;
    i64 u64 ;
    i128 u128 ;
    isize usize ;
}
