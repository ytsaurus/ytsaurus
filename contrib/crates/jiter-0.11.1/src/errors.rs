/// Enum representing all possible errors in JSON syntax.
///
/// Almost all of `JsonErrorType` is copied from [serde_json](https://github.com/serde-rs) so errors match
/// those expected from `serde_json`.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum JsonErrorType {
    /// float value was found where an int was expected
    FloatExpectingInt,

    /// duplicate keys in an object
    DuplicateKey(String),

    /// happens when getting the `Decimal` type or constructing a decimal fails
    InternalError(String),

    /// NOTE: all errors from here on are copied from serde_json
    /// [src/error.rs](https://github.com/serde-rs/json/blob/v1.0.107/src/error.rs#L236)
    /// with `Io` and `Message` removed
    ///
    /// EOF while parsing a list.
    EofWhileParsingList,

    /// EOF while parsing an object.
    EofWhileParsingObject,

    /// EOF while parsing a string.
    EofWhileParsingString,

    /// EOF while parsing a JSON value.
    EofWhileParsingValue,

    /// Expected this character to be a `':'`.
    ExpectedColon,

    /// Expected this character to be either a `','` or a `']'`.
    ExpectedListCommaOrEnd,

    /// Expected this character to be either a `','` or a `'}'`.
    ExpectedObjectCommaOrEnd,

    /// Expected to parse either a `true`, `false`, or a `null`.
    ExpectedSomeIdent,

    /// Expected this character to start a JSON value.
    ExpectedSomeValue,

    /// Invalid hex escape code.
    InvalidEscape,

    /// Invalid number.
    InvalidNumber,

    /// Number is bigger than the maximum value of its type.
    NumberOutOfRange,

    /// Invalid unicode code point.
    InvalidUnicodeCodePoint,

    /// Control character found while parsing a string.
    ControlCharacterWhileParsingString,

    /// Object key is not a string.
    KeyMustBeAString,

    /// Lone leading surrogate in hex escape.
    LoneLeadingSurrogateInHexEscape,

    /// JSON has a comma after the last value in an array or map.
    TrailingComma,

    /// JSON has non-whitespace trailing characters after the value.
    TrailingCharacters,

    /// Unexpected end of hex escape.
    UnexpectedEndOfHexEscape,

    /// Encountered nesting of JSON maps and arrays more than 128 layers deep.
    RecursionLimitExceeded,
}

impl std::fmt::Display for JsonErrorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Messages for enum members copied from serde_json are unchanged
        match self {
            Self::FloatExpectingInt => f.write_str("float value was found where an int was expected"),
            Self::DuplicateKey(s) => write!(f, "Detected duplicate key {s:?}"),
            Self::InternalError(s) => write!(f, "Internal error: {s:?}"),
            Self::EofWhileParsingList => f.write_str("EOF while parsing a list"),
            Self::EofWhileParsingObject => f.write_str("EOF while parsing an object"),
            Self::EofWhileParsingString => f.write_str("EOF while parsing a string"),
            Self::EofWhileParsingValue => f.write_str("EOF while parsing a value"),
            Self::ExpectedColon => f.write_str("expected `:`"),
            Self::ExpectedListCommaOrEnd => f.write_str("expected `,` or `]`"),
            Self::ExpectedObjectCommaOrEnd => f.write_str("expected `,` or `}`"),
            Self::ExpectedSomeIdent => f.write_str("expected ident"),
            Self::ExpectedSomeValue => f.write_str("expected value"),
            Self::InvalidEscape => f.write_str("invalid escape"),
            Self::InvalidNumber => f.write_str("invalid number"),
            Self::NumberOutOfRange => f.write_str("number out of range"),
            Self::InvalidUnicodeCodePoint => f.write_str("invalid unicode code point"),
            Self::ControlCharacterWhileParsingString => {
                f.write_str("control character (\\u0000-\\u001F) found while parsing a string")
            }
            Self::KeyMustBeAString => f.write_str("key must be a string"),
            Self::LoneLeadingSurrogateInHexEscape => f.write_str("lone leading surrogate in hex escape"),
            Self::TrailingComma => f.write_str("trailing comma"),
            Self::TrailingCharacters => f.write_str("trailing characters"),
            Self::UnexpectedEndOfHexEscape => f.write_str("unexpected end of hex escape"),
            Self::RecursionLimitExceeded => f.write_str("recursion limit exceeded"),
        }
    }
}

pub type JsonResult<T> = Result<T, JsonError>;

/// Represents an error from parsing JSON
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct JsonError {
    /// The type of error.
    pub error_type: JsonErrorType,
    /// The index in the data where the error occurred.
    pub index: usize,
}

impl JsonError {
    pub(crate) fn new(error_type: JsonErrorType, index: usize) -> Self {
        Self { error_type, index }
    }

    pub fn get_position(&self, json_data: &[u8]) -> LinePosition {
        LinePosition::find(json_data, self.index)
    }

    pub fn description(&self, json_data: &[u8]) -> String {
        let position = self.get_position(json_data);
        format!("{} at {}", self.error_type, position)
    }

    pub(crate) fn allowed_if_partial(&self) -> bool {
        matches!(
            self.error_type,
            JsonErrorType::EofWhileParsingList
                | JsonErrorType::EofWhileParsingObject
                | JsonErrorType::EofWhileParsingString
                | JsonErrorType::EofWhileParsingValue
                | JsonErrorType::ExpectedListCommaOrEnd
                | JsonErrorType::ExpectedObjectCommaOrEnd
        )
    }
}

impl std::fmt::Display for JsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} at index {}", self.error_type, self.index)
    }
}

impl std::error::Error for JsonError {}

macro_rules! json_error {
    ($error_type:ident, $index:expr) => {
        crate::errors::JsonError::new(crate::errors::JsonErrorType::$error_type, $index)
    };
}

pub(crate) use json_error;

macro_rules! json_err {
    ($error_type:ident, $index:expr) => {
        Err(crate::errors::json_error!($error_type, $index))
    };
}

use crate::Jiter;
pub(crate) use json_err;

pub(crate) const DEFAULT_RECURSION_LIMIT: u8 = 200;

/// Enum representing all JSON types.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum JsonType {
    Null,
    Bool,
    Int,
    Float,
    String,
    Array,
    Object,
}

impl std::fmt::Display for JsonType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => f.write_str("null"),
            Self::Bool => f.write_str("bool"),
            Self::Int => f.write_str("int"),
            Self::Float => f.write_str("float"),
            Self::String => f.write_str("string"),
            Self::Array => f.write_str("array"),
            Self::Object => f.write_str("object"),
        }
    }
}

/// Enum representing either a [JsonErrorType] or a WrongType error.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum JiterErrorType {
    JsonError(JsonErrorType),
    WrongType { expected: JsonType, actual: JsonType },
}

impl std::fmt::Display for JiterErrorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::JsonError(error_type) => write!(f, "{error_type}"),
            Self::WrongType { expected, actual } => {
                write!(f, "expected {expected} but found {actual}")
            }
        }
    }
}

/// An error from the Jiter iterator.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct JiterError {
    pub error_type: JiterErrorType,
    pub index: usize,
}

impl std::fmt::Display for JiterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} at index {}", self.error_type, self.index)
    }
}

impl std::error::Error for JiterError {}

impl JiterError {
    pub(crate) fn new(error_type: JiterErrorType, index: usize) -> Self {
        Self { error_type, index }
    }

    pub fn get_position(&self, jiter: &Jiter) -> LinePosition {
        jiter.error_position(self.index)
    }

    pub fn description(&self, jiter: &Jiter) -> String {
        let position = self.get_position(jiter);
        format!("{} at {}", self.error_type, position)
    }

    pub(crate) fn wrong_type(expected: JsonType, actual: JsonType, index: usize) -> Self {
        Self::new(JiterErrorType::WrongType { expected, actual }, index)
    }
}

impl From<JsonError> for JiterError {
    fn from(error: JsonError) -> Self {
        Self {
            error_type: JiterErrorType::JsonError(error.error_type),
            index: error.index,
        }
    }
}

/// Represents a line and column in a file or input string, used for both errors and value positions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinePosition {
    /// Line number, starting at 1.
    pub line: usize,
    /// Column number, starting at 1.
    pub column: usize,
}

impl std::fmt::Display for LinePosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "line {} column {}", self.line, self.column)
    }
}

impl LinePosition {
    pub fn new(line: usize, column: usize) -> Self {
        Self { line, column }
    }

    /// Find the line and column of a byte index in a string.
    pub fn find(json_data: &[u8], find: usize) -> Self {
        let mut line = 1;
        let mut last_line_start = 0;
        let mut index = 0;
        while let Some(next) = json_data.get(index) {
            if *next == b'\n' {
                line += 1;
                last_line_start = index + 1;
            }
            if index == find {
                return Self {
                    line,
                    column: index + 1 - last_line_start,
                };
            }
            index += 1;
        }
        Self {
            line,
            column: index.saturating_sub(last_line_start),
        }
    }

    pub fn short(&self) -> String {
        format!("{}:{}", self.line, self.column)
    }
}
