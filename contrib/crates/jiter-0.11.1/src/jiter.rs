use crate::errors::{json_error, JiterError, JsonType, LinePosition, DEFAULT_RECURSION_LIMIT};
use crate::number_decoder::{NumberAny, NumberFloat, NumberInt, NumberRange};
use crate::parse::{Parser, Peek};
use crate::string_decoder::{StringDecoder, StringDecoderRange, Tape};
use crate::value::{take_value_borrowed, take_value_owned, take_value_skip, JsonValue};
use crate::{JsonError, JsonErrorType, PartialMode};

pub type JiterResult<T> = Result<T, JiterError>;

/// A JSON iterator.
#[derive(Debug)]
pub struct Jiter<'j> {
    data: &'j [u8],
    parser: Parser<'j>,
    tape: Tape,
    allow_inf_nan: bool,
    allow_partial_strings: bool,
}

impl Clone for Jiter<'_> {
    /// Clone a `Jiter`. Like the default implementation, but a new empty `tape` is used.
    fn clone(&self) -> Self {
        Self {
            data: self.data,
            parser: self.parser.clone(),
            tape: Tape::default(),
            allow_inf_nan: self.allow_inf_nan,
            allow_partial_strings: self.allow_partial_strings,
        }
    }
}

impl<'j> Jiter<'j> {
    /// Constructs a new `Jiter`.
    ///
    /// # Arguments
    /// - `data`: The JSON data to be parsed.
    /// - `allow_inf_nan`: Whether to allow `NaN`, `Infinity` and `-Infinity` as numbers.
    pub fn new(data: &'j [u8]) -> Self {
        Self {
            data,
            parser: Parser::new(data),
            tape: Tape::default(),
            allow_inf_nan: false,
            allow_partial_strings: false,
        }
    }

    pub fn with_allow_inf_nan(mut self) -> Self {
        self.allow_inf_nan = true;
        self
    }

    pub fn with_allow_partial_strings(mut self) -> Self {
        self.allow_partial_strings = true;
        self
    }

    /// Get the current [LinePosition] of the parser.
    pub fn current_position(&self) -> LinePosition {
        self.parser.current_position()
    }

    /// Get the current index of the parser.
    pub fn current_index(&self) -> usize {
        self.parser.index
    }

    /// Get a slice of the underlying JSON data from `start` to `current_index`.
    pub fn slice_to_current(&self, start: usize) -> &'j [u8] {
        &self.data[start..self.current_index()]
    }

    /// Convert an error index to a [LinePosition].
    ///
    /// # Arguments
    /// - `index`: The index of the error to find the position of.
    pub fn error_position(&self, index: usize) -> LinePosition {
        LinePosition::find(self.data, index)
    }

    /// Peek at the next JSON value without consuming it.
    pub fn peek(&mut self) -> JiterResult<Peek> {
        self.parser.peek().map_err(Into::into)
    }

    /// Assuming the next value is `null`, consume it. Error if it is not `null`, or is invalid JSON.
    pub fn next_null(&mut self) -> JiterResult<()> {
        let peek = self.peek()?;
        match peek {
            Peek::Null => self.known_null(),
            _ => Err(self.wrong_type(JsonType::Null, peek)),
        }
    }

    /// Knowing the next value is `null`, consume it.
    pub fn known_null(&mut self) -> JiterResult<()> {
        self.parser.consume_null()?;
        Ok(())
    }

    /// Assuming the next value is `true` or `false`, consume it. Error if it is not a boolean, or is invalid JSON.
    ///
    /// # Returns
    /// The boolean value.
    pub fn next_bool(&mut self) -> JiterResult<bool> {
        let peek = self.peek()?;
        self.known_bool(peek)
    }

    /// Knowing the next value is `true` or `false`, parse it.
    pub fn known_bool(&mut self, peek: Peek) -> JiterResult<bool> {
        match peek {
            Peek::True => {
                self.parser.consume_true()?;
                Ok(true)
            }
            Peek::False => {
                self.parser.consume_false()?;
                Ok(false)
            }
            _ => Err(self.wrong_type(JsonType::Bool, peek)),
        }
    }

    /// Assuming the next value is a number, consume it. Error if it is not a number, or is invalid JSON.
    ///
    /// # Returns
    /// A [NumberAny] representing the number.
    pub fn next_number(&mut self) -> JiterResult<NumberAny> {
        let peek = self.peek()?;
        self.known_number(peek)
    }

    /// Knowing the next value is a number, parse it.
    pub fn known_number(&mut self, peek: Peek) -> JiterResult<NumberAny> {
        self.parser
            .consume_number::<NumberAny>(peek.into_inner(), self.allow_inf_nan)
            .map_err(|e| self.maybe_number_error(e, JsonType::Int, peek))
    }

    /// Assuming the next value is an integer, consume it. Error if it is not an integer, or is invalid JSON.
    pub fn next_int(&mut self) -> JiterResult<NumberInt> {
        let peek = self.peek()?;
        self.known_int(peek)
    }

    /// Knowing the next value is an integer, parse it.
    pub fn known_int(&mut self, peek: Peek) -> JiterResult<NumberInt> {
        self.parser
            .consume_number::<NumberInt>(peek.into_inner(), self.allow_inf_nan)
            .map_err(|e| {
                if e.error_type == JsonErrorType::FloatExpectingInt {
                    JiterError::wrong_type(JsonType::Int, JsonType::Float, self.parser.index)
                } else {
                    self.maybe_number_error(e, JsonType::Int, peek)
                }
            })
    }

    /// Assuming the next value is a float, consume it. Error if it is not a float, or is invalid JSON.
    pub fn next_float(&mut self) -> JiterResult<f64> {
        let peek = self.peek()?;
        self.known_float(peek)
    }

    /// Knowing the next value is a float, parse it.
    pub fn known_float(&mut self, peek: Peek) -> JiterResult<f64> {
        self.parser
            .consume_number::<NumberFloat>(peek.into_inner(), self.allow_inf_nan)
            .map_err(|e| self.maybe_number_error(e, JsonType::Float, peek))
    }

    /// Assuming the next value is a number, consume it and return bytes from the original JSON data.
    pub fn next_number_bytes(&mut self) -> JiterResult<&[u8]> {
        let peek = self.peek()?;
        self.known_number_bytes(peek)
    }

    /// Knowing the next value is a number, parse it and return bytes from the original JSON data.
    fn known_number_bytes(&mut self, peek: Peek) -> JiterResult<&[u8]> {
        match self
            .parser
            .consume_number::<NumberRange>(peek.into_inner(), self.allow_inf_nan)
        {
            Ok(numbe_range) => Ok(&self.data[numbe_range.range]),
            Err(e) => Err(self.maybe_number_error(e, JsonType::Float, peek)),
        }
    }

    /// Assuming the next value is a string, consume it. Error if it is not a string, or is invalid JSON.
    pub fn next_str(&mut self) -> JiterResult<&str> {
        let peek = self.peek()?;
        match peek {
            Peek::String => self.known_str(),
            _ => Err(self.wrong_type(JsonType::String, peek)),
        }
    }

    /// Knowing the next value is a string, parse it.
    pub fn known_str(&mut self) -> JiterResult<&str> {
        match self
            .parser
            .consume_string::<StringDecoder>(&mut self.tape, self.allow_partial_strings)
        {
            Ok(output) => Ok(output.as_str()),
            Err(e) => Err(e.into()),
        }
    }

    /// Assuming the next value is a string, consume it and return bytes from the original JSON data.
    pub fn next_bytes(&mut self) -> JiterResult<&[u8]> {
        let peek = self.peek()?;
        match peek {
            Peek::String => self.known_bytes(),
            _ => Err(self.wrong_type(JsonType::String, peek)),
        }
    }

    /// Knowing the next value is a string, parse it and return bytes from the original JSON data.
    pub fn known_bytes(&mut self) -> JiterResult<&[u8]> {
        let range = self
            .parser
            .consume_string::<StringDecoderRange>(&mut self.tape, self.allow_partial_strings)?;
        Ok(&self.data[range])
    }

    /// Parse the next JSON value and return it as a [JsonValue]. Error if it is invalid JSON.
    pub fn next_value(&mut self) -> JiterResult<JsonValue<'j>> {
        let peek = self.peek()?;
        self.known_value(peek)
    }

    /// Parse the next JSON value and return it as a [JsonValue]. Error if it is invalid JSON.
    ///
    /// # Arguments
    /// - `peek`: The [Peek] of the next JSON value.
    pub fn known_value(&mut self, peek: Peek) -> JiterResult<JsonValue<'j>> {
        take_value_borrowed(
            peek,
            &mut self.parser,
            &mut self.tape,
            DEFAULT_RECURSION_LIMIT,
            self.allow_inf_nan,
            PartialMode::Off,
        )
        .map_err(Into::into)
    }

    /// Parse the next JSON value, but don't return it.
    /// This should be faster than returning the value, useful when you don't care about this value.
    /// Error if it is invalid JSON.
    ///
    /// *WARNING:* For performance reasons, this method does not check that strings would be valid UTF-8.
    pub fn next_skip(&mut self) -> JiterResult<()> {
        let peek = self.peek()?;
        self.known_skip(peek)
    }

    /// Parse the next JSON value, but don't return it. Error if it is invalid JSON.
    ///
    /// # Arguments
    /// - `peek`: The [Peek] of the next JSON value.
    pub fn known_skip(&mut self, peek: Peek) -> JiterResult<()> {
        take_value_skip(
            peek,
            &mut self.parser,
            &mut self.tape,
            DEFAULT_RECURSION_LIMIT,
            self.allow_inf_nan,
        )
        .map_err(Into::into)
    }

    /// Parse the next JSON value and return it as a [JsonValue] with static lifetime. Error if it is invalid JSON.
    pub fn next_value_owned(&mut self) -> JiterResult<JsonValue<'static>> {
        let peek = self.peek()?;
        self.known_value_owned(peek)
    }

    /// Parse the next JSON value and return it as a [JsonValue] with static lifetime. Error if it is invalid JSON.
    ///
    /// # Arguments
    /// - `peek`: The [Peek] of the next JSON value.
    pub fn known_value_owned(&mut self, peek: Peek) -> JiterResult<JsonValue<'static>> {
        take_value_owned(
            peek,
            &mut self.parser,
            &mut self.tape,
            DEFAULT_RECURSION_LIMIT,
            self.allow_inf_nan,
            PartialMode::Off,
        )
        .map_err(Into::into)
    }

    /// Assuming the next value is an array, peek at the first value.
    /// Error if it is not an array, or is invalid JSON.
    ///
    /// # Returns
    /// The `Some(peek)` of the first value in the array is not empty, `None` if it is empty.
    pub fn next_array(&mut self) -> JiterResult<Option<Peek>> {
        let peek = self.peek()?;
        match peek {
            Peek::Array => self.known_array(),
            _ => Err(self.wrong_type(JsonType::Array, peek)),
        }
    }

    /// Assuming the next value is an array, peat at the first value.
    pub fn known_array(&mut self) -> JiterResult<Option<Peek>> {
        self.parser.array_first().map_err(Into::into)
    }

    /// Peek at the next value in an array.
    pub fn array_step(&mut self) -> JiterResult<Option<Peek>> {
        self.parser.array_step().map_err(Into::into)
    }

    /// Assuming the next value is an object, consume the first key.
    /// Error if it is not an object, or is invalid JSON.
    ///
    /// # Returns
    /// The `Some(key)` of the first key in the object is not empty, `None` if it is empty.
    pub fn next_object(&mut self) -> JiterResult<Option<&str>> {
        let peek = self.peek()?;
        match peek {
            Peek::Object => self.known_object(),
            _ => Err(self.wrong_type(JsonType::Object, peek)),
        }
    }

    /// Assuming the next value is an object, conssume the first key and return bytes from the original JSON data.
    pub fn known_object(&mut self) -> JiterResult<Option<&str>> {
        let op_str = self.parser.object_first::<StringDecoder>(&mut self.tape)?;
        Ok(op_str.map(|s| s.as_str()))
    }

    /// Assuming the next value is an object, peek at the first key.
    pub fn next_object_bytes(&mut self) -> JiterResult<Option<&[u8]>> {
        let peek = self.peek()?;
        match peek {
            Peek::Object => {
                let op_range = self.parser.object_first::<StringDecoderRange>(&mut self.tape)?;
                Ok(op_range.map(|r| &self.data[r]))
            }
            _ => Err(self.wrong_type(JsonType::Object, peek)),
        }
    }

    /// Get the next key in an object, or `None` if there are no more keys.
    pub fn next_key(&mut self) -> JiterResult<Option<&str>> {
        let strs = self.parser.object_step::<StringDecoder>(&mut self.tape)?;
        Ok(strs.map(|s| s.as_str()))
    }

    /// Get the next key in an object as bytes, or `None` if there are no more keys.
    pub fn next_key_bytes(&mut self) -> JiterResult<Option<&[u8]>> {
        let op_range = self.parser.object_step::<StringDecoderRange>(&mut self.tape)?;
        Ok(op_range.map(|r| &self.data[r]))
    }

    /// Finish parsing the JSON data. Error if there is more data to be parsed.
    pub fn finish(&mut self) -> JiterResult<()> {
        self.parser.finish().map_err(Into::into)
    }

    fn wrong_type(&self, expected: JsonType, peek: Peek) -> JiterError {
        match peek {
            Peek::True | Peek::False => JiterError::wrong_type(expected, JsonType::Bool, self.parser.index),
            Peek::Null => JiterError::wrong_type(expected, JsonType::Null, self.parser.index),
            Peek::String => JiterError::wrong_type(expected, JsonType::String, self.parser.index),
            Peek::Array => JiterError::wrong_type(expected, JsonType::Array, self.parser.index),
            Peek::Object => JiterError::wrong_type(expected, JsonType::Object, self.parser.index),
            _ if peek.is_num() => self.wrong_num(peek.into_inner(), expected),
            _ => json_error!(ExpectedSomeValue, self.parser.index).into(),
        }
    }

    fn wrong_num(&self, first: u8, expected: JsonType) -> JiterError {
        let mut parser2 = self.parser.clone();
        let actual = match parser2.consume_number::<NumberAny>(first, self.allow_inf_nan) {
            Ok(NumberAny::Int { .. }) => JsonType::Int,
            Ok(NumberAny::Float { .. }) => JsonType::Float,
            Err(e) => return e.into(),
        };
        JiterError::wrong_type(expected, actual, self.parser.index)
    }

    fn maybe_number_error(&self, e: JsonError, expected: JsonType, peek: Peek) -> JiterError {
        if peek.is_num() {
            e.into()
        } else {
            self.wrong_type(expected, peek)
        }
    }
}
