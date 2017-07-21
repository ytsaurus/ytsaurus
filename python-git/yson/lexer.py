from . import yson_types
from .yson_token import *

from .common import raise_yson_error, YsonError

from yt.packages.six.moves import xrange
from yt.packages.six import int2byte, iterbytes

import struct

_SEEMS_INT64 = int2byte(0)
_SEEMS_UINT64 = int2byte(1)
_SEEMS_DOUBLE = int2byte(2)

# Binary literals markers
STRING_MARKER = int2byte(1)
INT64_MARKER = int2byte(2)
DOUBLE_MARKER = int2byte(3)
FALSE_MARKER = int2byte(4)
TRUE_MARKER = int2byte(5)
UINT64_MARKER = int2byte(6)

def _get_numeric_type(string):
    for code in iterbytes(string):
        ch = int2byte(code)
        if ch == b'E' or ch == b'e' or ch == b'.':
            return _SEEMS_DOUBLE
        elif ch == b'u':
            return _SEEMS_UINT64
    return _SEEMS_INT64

def _zig_zag_decode(value):
    return (value >> 1) ^ -(value & 1)

class YsonLexer(object):
    def __init__(self, stream, encoding):
        self._line_index = 1
        self._position = 1
        self._offset = 0
        self._stream = stream
        self._lookahead = None
        self._encoding = encoding

    def _get_start_state(self, ch):
        tokens = {
            b"#": TOKEN_HASH,
            b"(": TOKEN_LEFT_PARENTHESIS,
            b")": TOKEN_RIGHT_PARENTHESIS,
            b",": TOKEN_COMMA,
            b":": TOKEN_COLON,
            b";": TOKEN_SEMICOLON,
            b"<": TOKEN_LEFT_ANGLE,
            b"=": TOKEN_EQUALS,
            b">": TOKEN_RIGHT_ANGLE,
            b"[": TOKEN_LEFT_BRACKET,
            b"]": TOKEN_RIGHT_BRACKET,
            b"{": TOKEN_LEFT_BRACE,
            b"}": TOKEN_RIGHT_BRACE,
        }
        return tokens.get(ch)

    def get_next_token(self):
        self._skip_whitespaces()
        ch = self._peek_char()
        if not ch:
            return YsonToken()

        if ch == STRING_MARKER:
            return YsonToken(value=self._parse_string(), type=TOKEN_STRING)

        elif ch == INT64_MARKER:
            return YsonToken(value=self._parse_binary_int64(), type=TOKEN_INT64)

        elif ch == UINT64_MARKER:
            return YsonToken(value=self._parse_binary_uint64(), type=TOKEN_UINT64)

        elif ch == DOUBLE_MARKER:
            return YsonToken(value=self._parse_binary_double(), type=TOKEN_DOUBLE)

        elif ch == FALSE_MARKER:
            self._expect_char(ch)
            return YsonToken(value=False, type=TOKEN_BOOLEAN)

        elif ch == TRUE_MARKER:
            self._expect_char(ch)
            return YsonToken(value=True, type=TOKEN_BOOLEAN)

        elif ch == b"%":
            return YsonToken(value=self._parse_boolean(), type=TOKEN_BOOLEAN)

        elif ch == b"#":
            return YsonToken(value=self._parse_entity(), type=TOKEN_HASH)

        elif ch == b"+" or ch == b"-" or ch.isdigit():
            value = self._parse_numeric()
            token_type = None
            if isinstance(value, yson_types._YsonIntegerBase):
                token_type = TOKEN_INT64
            elif isinstance(value, yson_types.YsonUint64):
                token_type = TOKEN_UINT64
            elif isinstance(value, float):
                token_type = TOKEN_DOUBLE
            return YsonToken(value=value, type=token_type)

        elif ch == b"_" or ch == b'"' or ch.isalpha():
            return YsonToken(value=self._parse_string(), type=TOKEN_STRING)

        state = self._get_start_state(ch)
        self._read_char()
        return YsonToken(value=ch, type=state)

    def get_position_info(self):
        return self._line_index, self._position, self._offset

    def _read_char(self, binary_input=False):
        if self._lookahead is None:
            result = self._stream.read(1)
        else:
            result = self._lookahead
        self._lookahead = None

        self._offset += 1
        if not binary_input and result == b"\n":
            self._line_index += 1
            self._position = 1
        else:
            self._position += 1

        return result

    def _peek_char(self):
        if self._lookahead is not None:
            return self._lookahead
        self._lookahead = self._stream.read(1)
        return self._lookahead

    def _read_binary_chars(self, char_count):
        result = []
        for i in xrange(char_count):
            ch = self._read_char(True)
            if not ch:
                raise_yson_error(
                    "Premature end-of-stream while reading byte %d out of %d" % (i + 1, char_count),
                    self.get_position_info())
            result.append(ch)
        return b"".join(result)

    def _expect_char(self, expected_ch):
        read_ch = self._read_char()
        if not read_ch:
            raise_yson_error(
                "Premature end-of-stream expecting '%s' in Yson" % expected_ch,
                self.get_position_info())
        if read_ch != expected_ch:
            raise_yson_error(
                "Found '%s' while expecting '%s' in Yson" % (read_ch, expected_ch),
                self.get_position_info())

    def _skip_whitespaces(self):
        while self._peek_char().isspace():
            self._read_char()

    def _read_string(self):
        ch = self._peek_char()
        if not ch:
            raise_yson_error(
                "Premature end-of-stream while expecting string literal in Yson",
                self.get_position_info())
        if ch == STRING_MARKER:
            return self._read_binary_string()
        if ch == b'"':
            return self._read_quoted_string()
        if not ch.isalpha() and not ch == b"_" and not ch == b"%":
            raise_yson_error(
                "Expecting string literal but found %s in Yson" % ch,
                self.get_position_info())
        return self._read_unquoted_string()

    def _read_binary_string(self):
        self._expect_char(STRING_MARKER)
        length = _zig_zag_decode(self._read_varint())
        return self._decode_string(self._read_binary_chars(length))

    def _read_varint(self):
        count = 0
        result = 0
        read_next = True
        while read_next:
            ch = self._read_char()
            if not ch:
                raise_yson_error(
                    "Premature end-of-stream while reading varinteger in Yson",
                    self.get_position_info())
            byte = ord(ch)
            result |= (byte & 0x7F) << (7 * count)
            if result > 2 ** 64 - 1:
                raise_yson_error(
                    "Varinteger is too large for Int64 in Yson",
                    self.get_position_info())
            count += 1
            read_next = byte & 0x80 != 0

        return yson_types._YsonIntegerBase(result)

    def _read_quoted_string(self):
        self._expect_char(b'"')
        result = []
        pending_next_char = False
        while True:
            ch = self._read_char()
            if not ch:
                raise_yson_error(
                    "Premature end-of-stream while reading string literal in Yson",
                    self.get_position_info())
            if ch == b'"' and not pending_next_char:
                break
            result.append(ch)
            if pending_next_char:
                pending_next_char = False
            elif ch == b"\\":
                pending_next_char = True
        return self._decode_string(self._unescape(b"".join(result)))

    def _unescape(self, string):
        return string.decode("unicode_escape").encode("latin1")

    def _decode_string(self, string):
        if self._encoding is not None:
            return string.decode(self._encoding)
        else:
            return string

    def _read_unquoted_string(self):
        result = []
        while True:
            ch = self._peek_char()
            if ch and (ch.isalpha() or ch.isdigit() or ch in b"_%-"):
                self._read_char()
                result.append(ch)
            else:
                break
        return self._decode_string(b"".join(result))

    def _read_numeric(self):
        result = []
        while True:
            ch = self._peek_char()
            if not ch or not (ch.isdigit() or ch in b"+-.eEu"):
                break
            self._read_char()
            result.append(ch)
        if not result:
            raise_yson_error(
                "Premature end-of-stream while parsing numeric literal in Yson",
                self.get_position_info())
        return b"".join(result)

    def _parse_entity(self):
        self._expect_char(b"#")
        return None

    def _parse_boolean(self):
        self._expect_char(b"%")
        ch = self._peek_char()
        if ch not in [b"f", b"t"]:
            raise YsonError("Found '%s' while expecting 'f' or 't'" % ch)
        if ch == b"f":
            str = self._read_binary_chars(5)
            if str != b"false":
                raise YsonError("Incorrect boolean value '%s', expected 'false'" % str)
            return False
        if ch == b"t":
            str = self._read_binary_chars(4)
            if str != b"true":
                raise YsonError("Incorrect boolean value '%s', expected 'true'" % str)
            return True
        return None

    def _parse_string(self):
        result = self._read_string()
        return result

    def _parse_binary_int64(self):
        self._expect_char(INT64_MARKER)
        result = _zig_zag_decode(self._read_varint())
        return result

    def _parse_binary_uint64(self):
        self._expect_char(UINT64_MARKER)
        result = yson_types.YsonUint64(self._read_varint())
        return result

    def _parse_binary_double(self):
        self._expect_char(DOUBLE_MARKER)
        bytes_ = self._read_binary_chars(struct.calcsize(b"d"))
        result = struct.unpack(b"d", bytes_)[0]
        return result

    def _parse_numeric(self):
        string = self._read_numeric()
        numeric_type = _get_numeric_type(string)
        if numeric_type == _SEEMS_INT64:
            try:
                result = yson_types._YsonIntegerBase(string)
                if result > 2 ** 63 - 1 or result < -(2 ** 63):
                    raise ValueError()
            except ValueError:
                raise_yson_error(
                    "Failed to parse Int64 literal %s in Yson" % string,
                    self.get_position_info())
        elif numeric_type == _SEEMS_UINT64:
            try:
                if string.endswith(b"u"):
                    string = string[:-1]
                else:
                    raise ValueError()
                result = yson_types.YsonUint64(int(string))
                if result > 2 ** 64 - 1:
                    raise ValueError()
            except ValueError:
                raise_yson_error(
                    "Failed to parse Uint64 literal %s in Yson" % string,
                    self.get_position_info())
        else:
            try:
                result = float(string)
            except ValueError:
                raise_yson_error(
                    "Failed to parse Double literal %s in Yson" % string,
                    self.get_position_info())
        return result
