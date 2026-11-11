#[cfg(feature = "num-bigint")]
use num_bigint::BigInt;
#[cfg(feature = "num-bigint")]
use num_traits::cast::ToPrimitive;
#[cfg(feature = "python")]
use pyo3::{IntoPyObject, IntoPyObjectRef};

use std::ops::Range;

use lexical_parse_float::{format as lexical_format, FromLexicalWithOptions, Options as ParseFloatOptions};

use crate::errors::{json_err, json_error, JsonError, JsonResult};

pub trait AbstractNumberDecoder {
    type Output;

    fn decode(data: &[u8], index: usize, first: u8, allow_inf_nan: bool) -> JsonResult<(Self::Output, usize)>;
}

/// A number that can be either an [i64] or a [BigInt](num_bigint::BigInt)
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "python", derive(IntoPyObject, IntoPyObjectRef))]
pub enum NumberInt {
    Int(i64),
    #[cfg(feature = "num-bigint")]
    BigInt(BigInt),
}

impl From<NumberInt> for f64 {
    fn from(num: NumberInt) -> Self {
        match num {
            NumberInt::Int(int) => int as f64,
            #[cfg(feature = "num-bigint")]
            NumberInt::BigInt(big_int) => big_int.to_f64().unwrap_or(f64::NAN),
        }
    }
}

impl TryFrom<&[u8]> for NumberInt {
    type Error = JsonError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let first = *value.first().ok_or_else(|| json_error!(InvalidNumber, 0))?;
        let (int_parse, index) = IntParse::parse(value, 0, first)?;
        match int_parse {
            IntParse::Int(int) => {
                if index == value.len() {
                    Ok(int)
                } else {
                    json_err!(InvalidNumber, index)
                }
            }
            _ => json_err!(InvalidNumber, index),
        }
    }
}

impl AbstractNumberDecoder for NumberInt {
    type Output = NumberInt;

    fn decode(data: &[u8], index: usize, first: u8, _allow_inf_nan: bool) -> JsonResult<(Self::Output, usize)> {
        let (int_parse, index) = IntParse::parse(data, index, first)?;
        match int_parse {
            IntParse::Int(int) => Ok((int, index)),
            _ => json_err!(FloatExpectingInt, index),
        }
    }
}

pub struct NumberFloat;

impl AbstractNumberDecoder for NumberFloat {
    type Output = f64;

    fn decode(data: &[u8], mut index: usize, first: u8, allow_inf_nan: bool) -> JsonResult<(Self::Output, usize)> {
        let start = index;

        let positive = match first {
            b'N' => return consume_nan(data, index, allow_inf_nan),
            b'-' => false,
            _ => true,
        };
        if !positive {
            // we started with a minus sign, so the first digit is at index + 1
            index += 1;
        }
        let first2 = if positive { Some(&first) } else { data.get(index) };

        if let Some(digit) = first2 {
            if INT_CHAR_MAP[*digit as usize] {
                const JSON: u128 = lexical_format::JSON;
                let options = ParseFloatOptions::new();
                match f64::from_lexical_partial_with_options::<JSON>(&data[start..], &options) {
                    Ok((float, index)) => Ok((float, index + start)),
                    Err(_) => {
                        // it's impossible to work out the right error from LexicalError here, so we parse again
                        // with NumberRange and use that error
                        match NumberRange::decode(data, start, first, allow_inf_nan) {
                            Err(e) => Err(e),
                            // NumberRange should always raise an error if `parse_partial_with_options`
                            // except for Infinity and -Infinity, which are handled above
                            Ok(_) => unreachable!("NumberRange should always return an error"),
                        }
                    }
                }
            } else if digit == &b'I' {
                consume_inf_f64(data, index, positive, allow_inf_nan)
            } else {
                json_err!(InvalidNumber, index)
            }
        } else {
            json_err!(EofWhileParsingValue, index)
        }
    }
}

/// A number that can be either a [NumberInt] or an [f64]
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "python", derive(IntoPyObject, IntoPyObjectRef))]
pub enum NumberAny {
    Int(NumberInt),
    Float(f64),
}

impl From<NumberAny> for f64 {
    fn from(num: NumberAny) -> Self {
        match num {
            NumberAny::Int(int) => int.into(),
            NumberAny::Float(f) => f,
        }
    }
}

impl AbstractNumberDecoder for NumberAny {
    type Output = NumberAny;

    fn decode(data: &[u8], index: usize, first: u8, allow_inf_nan: bool) -> JsonResult<(Self::Output, usize)> {
        let start = index;
        let (int_parse, index) = IntParse::parse(data, index, first)?;
        match int_parse {
            IntParse::Int(int) => Ok((Self::Int(int), index)),
            IntParse::Float => {
                NumberFloat::decode(data, start, first, allow_inf_nan).map(|(f, index)| (Self::Float(f), index))
            }
            IntParse::FloatInf(positive) => {
                consume_inf_f64(data, index, positive, allow_inf_nan).map(|(f, index)| (Self::Float(f), index))
            }
            IntParse::FloatNaN => consume_nan(data, index, allow_inf_nan).map(|(f, index)| (Self::Float(f), index)),
        }
    }
}

fn consume_inf(data: &[u8], index: usize, positive: bool, allow_inf_nan: bool) -> JsonResult<usize> {
    if allow_inf_nan {
        crate::parse::consume_infinity(data, index)
    } else if positive {
        json_err!(ExpectedSomeValue, index)
    } else {
        json_err!(InvalidNumber, index)
    }
}

fn consume_inf_f64(data: &[u8], index: usize, positive: bool, allow_inf_nan: bool) -> JsonResult<(f64, usize)> {
    let end = consume_inf(data, index, positive, allow_inf_nan)?;
    if positive {
        Ok((f64::INFINITY, end))
    } else {
        Ok((f64::NEG_INFINITY, end))
    }
}

fn consume_nan(data: &[u8], index: usize, allow_inf_nan: bool) -> JsonResult<(f64, usize)> {
    if allow_inf_nan {
        let end = crate::parse::consume_nan(data, index)?;
        Ok((f64::NAN, end))
    } else {
        json_err!(ExpectedSomeValue, index)
    }
}

#[derive(Debug)]
pub(crate) enum IntParse {
    Int(NumberInt),
    Float,
    FloatInf(bool),
    FloatNaN,
}

impl IntParse {
    pub(crate) fn parse(data: &[u8], mut index: usize, first: u8) -> JsonResult<(Self, usize)> {
        let start = index;
        let positive = match first {
            b'N' => return Ok((Self::FloatNaN, index)),
            b'-' => false,
            _ => true,
        };
        if !positive {
            // we started with a minus sign, so the first digit is at index + 1
            index += 1;
        }
        let first2 = if positive { Some(&first) } else { data.get(index) };
        let first_value = match first2 {
            Some(b'0') => {
                index += 1;
                return match data.get(index) {
                    Some(b'.') => Ok((Self::Float, index)),
                    Some(b'e' | b'E') => Ok((Self::Float, index)),
                    Some(digit) if digit.is_ascii_digit() => json_err!(InvalidNumber, index),
                    _ => Ok((Self::Int(NumberInt::Int(0)), index)),
                };
            }
            Some(b'I') => return Ok((Self::FloatInf(positive), index)),
            Some(digit) if (b'1'..=b'9').contains(digit) => (digit & 0x0f) as u64,
            Some(_) => return json_err!(InvalidNumber, index),
            None => return json_err!(EofWhileParsingValue, index),
        };

        index += 1;
        let (chunk, new_index) = IntChunk::parse_small(data, index, first_value);

        let ongoing: u64 = match chunk {
            IntChunk::Ongoing(value) => value,
            IntChunk::Done(value) => {
                let mut value_i64 = value as i64;
                if !positive {
                    value_i64 = -value_i64;
                }
                return Ok((Self::Int(NumberInt::Int(value_i64)), new_index));
            }
            IntChunk::Float => return Ok((Self::Float, new_index)),
        };

        // number is too big for i64, we need to use a BigInt,
        // or error out if num-bigint is not enabled

        #[cfg(not(feature = "num-bigint"))]
        {
            // silence unused variable warning
            let _ = (ongoing, start);
            return json_err!(NumberOutOfRange, index);
        }

        #[cfg(feature = "num-bigint")]
        {
            #[cfg(target_arch = "aarch64")]
            // in aarch64 we use a 128 bit registers - 16 bytes
            const ONGOING_CHUNK_MULTIPLIER: u64 = 10u64.pow(16);
            #[cfg(not(target_arch = "aarch64"))]
            // decode_int_chunk_fallback - we parse 18 bytes when the number is ongoing
            const ONGOING_CHUNK_MULTIPLIER: u64 = 10u64.pow(18);

            const POW_10: [u64; 18] = [
                10u64.pow(0),
                10u64.pow(1),
                10u64.pow(2),
                10u64.pow(3),
                10u64.pow(4),
                10u64.pow(5),
                10u64.pow(6),
                10u64.pow(7),
                10u64.pow(8),
                10u64.pow(9),
                10u64.pow(10),
                10u64.pow(11),
                10u64.pow(12),
                10u64.pow(13),
                10u64.pow(14),
                10u64.pow(15),
                10u64.pow(16),
                10u64.pow(17),
            ];

            let mut big_value: BigInt = ongoing.into();
            index = new_index;

            loop {
                let (chunk, new_index) = IntChunk::parse_big(data, index);
                if (new_index - start) > 4300 {
                    return json_err!(NumberOutOfRange, start + 4301);
                }
                match chunk {
                    IntChunk::Ongoing(value) => {
                        big_value *= ONGOING_CHUNK_MULTIPLIER;
                        big_value += value;
                        index = new_index;
                    }
                    IntChunk::Done(value) => {
                        big_value *= POW_10[new_index - index];
                        big_value += value;
                        if !positive {
                            big_value = -big_value;
                        }
                        return Ok((Self::Int(NumberInt::BigInt(big_value)), new_index));
                    }
                    IntChunk::Float => return Ok((Self::Float, new_index)),
                }
            }
        }
    }
}

pub(crate) enum IntChunk {
    Ongoing(u64),
    Done(u64),
    Float,
}

impl IntChunk {
    #[inline(always)]
    fn parse_small(data: &[u8], index: usize, value: u64) -> (Self, usize) {
        decode_int_chunk_fallback(data, index, value)
    }

    #[inline(always)]
    fn parse_big(data: &[u8], index: usize) -> (Self, usize) {
        // TODO x86_64: use simd

        #[cfg(target_arch = "aarch64")]
        {
            crate::simd_aarch64::decode_int_chunk(data, index)
        }
        #[cfg(not(target_arch = "aarch64"))]
        {
            decode_int_chunk_fallback(data, index, 0)
        }
    }
}

/// Turns out this is faster than fancy bit manipulation, see
/// https://github.com/Alexhuszagh/rust-lexical/blob/main/lexical-parse-integer/docs/Algorithm.md
/// for some context
#[inline(always)]
pub(crate) fn decode_int_chunk_fallback(data: &[u8], mut index: usize, mut value: u64) -> (IntChunk, usize) {
    // i64::MAX = 9223372036854775807 (19 chars) - so 18 chars is always valid as an i64
    for _ in 0..18 {
        if let Some(digit) = data.get(index) {
            if INT_CHAR_MAP[*digit as usize] {
                // we use wrapping add to avoid branching - we know the value cannot wrap
                value = value.wrapping_mul(10).wrapping_add((digit & 0x0f) as u64);
                index += 1;
                continue;
            } else if matches!(digit, b'.' | b'e' | b'E') {
                return (IntChunk::Float, index);
            }
        }
        return (IntChunk::Done(value), index);
    }
    (IntChunk::Ongoing(value), index)
}

pub(crate) static INT_CHAR_MAP: [bool; 256] = {
    const NU: bool = true;
    const __: bool = false;
    [
        //   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 0
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 1
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 2
        NU, NU, NU, NU, NU, NU, NU, NU, NU, NU, __, __, __, __, __, __, // 3
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 4
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 5
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 6
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 7
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 8
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 9
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // A
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // B
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // C
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // D
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // E
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // F
    ]
};

pub struct NumberRange {
    pub range: Range<usize>,
    // in some cfg configurations, this field is never read.
    #[allow(dead_code)]
    pub is_int: bool,
}

impl NumberRange {
    fn int(data: Range<usize>) -> Self {
        Self {
            range: data,
            is_int: true,
        }
    }

    fn float(data: Range<usize>) -> Self {
        Self {
            range: data,
            is_int: false,
        }
    }
}

impl AbstractNumberDecoder for NumberRange {
    type Output = Self;

    fn decode(data: &[u8], mut index: usize, first: u8, allow_inf_nan: bool) -> JsonResult<(Self::Output, usize)> {
        let start = index;

        let positive = match first {
            b'N' => {
                let (_, end) = consume_nan(data, index, allow_inf_nan)?;
                return Ok((Self::float(start..end), end));
            }
            b'-' => false,
            _ => true,
        };
        if !positive {
            // we started with a minus sign, so the first digit is at index + 1
            index += 1;
        }

        match data.get(index) {
            Some(b'0') => {
                // numbers start with zero must be floats, next char must be a dot
                index += 1;
                return match data.get(index) {
                    Some(b'.') => {
                        index += 1;
                        let end = consume_decimal(data, index)?;
                        Ok((Self::float(start..end), end))
                    }
                    Some(b'e' | b'E') => {
                        index += 1;
                        let end = consume_exponential(data, index)?;
                        Ok((Self::float(start..end), end))
                    }
                    Some(digit) if digit.is_ascii_digit() => json_err!(InvalidNumber, index),
                    _ => return Ok((Self::int(start..index), index)),
                };
            }
            Some(b'I') => {
                let end = consume_inf(data, index, positive, allow_inf_nan)?;
                return Ok((Self::float(start..end), end));
            }
            Some(digit) if (b'1'..=b'9').contains(digit) => (),
            Some(_) => return json_err!(InvalidNumber, index),
            None => return json_err!(EofWhileParsingValue, index),
        }

        index += 1;
        for _ in 0..18 {
            if let Some(digit) = data.get(index) {
                if INT_CHAR_MAP[*digit as usize] {
                    index += 1;
                    continue;
                } else if matches!(digit, b'.') {
                    index += 1;
                    let end = consume_decimal(data, index)?;
                    return Ok((Self::float(start..end), end));
                } else if matches!(digit, b'e' | b'E') {
                    index += 1;
                    let end = consume_exponential(data, index)?;
                    return Ok((Self::float(start..end), end));
                }
            }
            return Ok((Self::int(start..index), index));
        }
        loop {
            let (chunk, new_index) = IntChunk::parse_big(data, index);
            if (new_index - start) > 4300 {
                return json_err!(NumberOutOfRange, start + 4301);
            }
            #[allow(clippy::single_match_else)]
            match chunk {
                IntChunk::Ongoing(_) => {
                    index = new_index;
                }
                IntChunk::Done(_) => return Ok((Self::int(start..new_index), new_index)),
                IntChunk::Float => {
                    return match data.get(new_index) {
                        Some(b'.') => {
                            index = new_index + 1;
                            let end = consume_decimal(data, index)?;
                            Ok((Self::float(start..end), end))
                        }
                        _ => {
                            index = new_index + 1;
                            let end = consume_exponential(data, index)?;
                            Ok((Self::float(start..end), end))
                        }
                    }
                }
            }
        }
    }
}

fn consume_exponential(data: &[u8], mut index: usize) -> JsonResult<usize> {
    match data.get(index) {
        Some(b'-' | b'+') => {
            index += 1;
        }
        Some(v) if v.is_ascii_digit() => (),
        Some(_) => return json_err!(InvalidNumber, index),
        None => return json_err!(EofWhileParsingValue, index),
    }

    match data.get(index) {
        Some(v) if v.is_ascii_digit() => (),
        Some(_) => return json_err!(InvalidNumber, index),
        None => return json_err!(EofWhileParsingValue, index),
    }
    index += 1;

    while let Some(next) = data.get(index) {
        match next {
            b'0'..=b'9' => (),
            _ => break,
        }
        index += 1;
    }

    Ok(index)
}

fn consume_decimal(data: &[u8], mut index: usize) -> JsonResult<usize> {
    match data.get(index) {
        Some(v) if v.is_ascii_digit() => (),
        Some(_) => return json_err!(InvalidNumber, index),
        None => return json_err!(EofWhileParsingValue, index),
    }
    index += 1;

    while let Some(next) = data.get(index) {
        match next {
            b'0'..=b'9' => (),
            b'e' | b'E' => {
                index += 1;
                return consume_exponential(data, index);
            }
            _ => break,
        }
        index += 1;
    }

    Ok(index)
}
