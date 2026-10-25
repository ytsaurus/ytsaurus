use lexical_parse_float::{format as lexical_format, FromLexicalWithOptions, Options as ParseFloatOptions};

/// Parse a string as an int.
///
/// This is around 2x faster than using `str::parse::<i64>()`
pub fn int_parse_str(s: &str) -> Option<i64> {
    int_parse_bytes(s.as_bytes())
}

/// Parse bytes as an int.
pub fn int_parse_bytes(s: &[u8]) -> Option<i64> {
    int_parse_bytes_internal(s).ok()
}

#[derive(Debug)]
pub enum IntFloat {
    Int(i64),
    Float(f64),
    Err,
}

impl IntFloat {
    pub fn is_err(&self) -> bool {
        matches!(self, IntFloat::Err)
    }
}

/// Parse a string as a float.
///
/// This is around 2x faster than using `str::parse::<f64>()`
pub fn float_parse_str(s: &str) -> IntFloat {
    float_parse_bytes(s.as_bytes())
}

/// Parse bytes as an float.
pub fn float_parse_bytes(s: &[u8]) -> IntFloat {
    // optimistically try to parse as an integer
    match int_parse_bytes_internal(s) {
        Ok(int) => IntFloat::Int(int),
        // integer parsing failed on encountering a '.', try as a float
        Err(Some(b'.')) => {
            static OPTIONS: ParseFloatOptions = ParseFloatOptions::new();
            match f64::from_lexical_with_options::<{ lexical_format::STANDARD }>(s, &OPTIONS) {
                Ok(v) => IntFloat::Float(v),
                Err(_) => IntFloat::Err,
            }
        }
        // any other integer parse error is also a float error
        Err(_) => IntFloat::Err,
    }
}

const ERR: u8 = 0xff;

/// Optimized routine to either parse an integer or return the character which triggered the error.
fn int_parse_bytes_internal(s: &[u8]) -> Result<i64, Option<u8>> {
    let (neg, first_digit, digits) = match s {
        [b'-', first, digits @ ..] => (true, *first, digits),
        [b'+', first, digits @ ..] | [first, digits @ ..] => (false, *first, digits),
        [] => return Err(None),
    };

    let mut int_part = decoded_i64_value(first_digit)?;

    for &digit in digits {
        int_part = int_part.wrapping_mul(10);
        int_part = int_part.wrapping_add(decoded_i64_value(digit)?);

        // only check once for overflow per loop iteration to minimize branching
        if int_part < 0 {
            return Err(Some(digit));
        }
    }

    Ok(if neg { -int_part } else { int_part })
}

/// Helper to parse a single ascii digit as an i64.
fn decoded_i64_value(digit: u8) -> Result<i64, u8> {
    #[rustfmt::skip]
    static DECODE_MAP: [u8; 256] = {
        const __: u8 = ERR;
        [
            //   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F
            __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 0
            __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 1
            __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 2
             0,  1,  2,  3,  4,  5,  6,  7,  8,  9, __, __, __, __, __, __, // 3
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

    let value = DECODE_MAP[digit as usize];
    debug_assert!(value <= 9 || value == ERR);

    if value == ERR {
        return Err(digit);
    }

    Ok(value as i64)
}

/// Count the number of decimal places in a byte slice.
/// Caution: does not verify the integrity of the input,
/// so it may return incorrect results for invalid inputs.
pub(crate) fn decimal_digits(bytes: &[u8]) -> usize {
    match bytes.splitn(2, |&b| b == b'.').nth(1) {
        Some(b"") | None => 0,
        Some(fraction) => fraction.len(),
    }
}
