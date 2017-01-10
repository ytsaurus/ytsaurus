from . import convert, yson_types
from .common import YsonError

from yt.packages.six import int2byte, PY3, indexbytes, iterbytes, BytesIO, text_type
from yt.packages.six.moves import xrange

import struct

_ENCODING_SENTINEL = object()

def _is_text_reader(stream):
    return type(stream.read(0)) is text_type

class YsonParseError(ValueError):
    def __init__(self, message, position_info):
        line_index, position, offset = position_info
        ValueError.__init__(self, _format_message(message, line_index, position, offset))
        self.message = message
        self.line_index = line_index
        self.position = position
        self.offset = offset

def _format_message(message, line_index, position, offset):
    return "%s (Line: %d, Position: %d, Offset: %d)" % (message, line_index, position, offset)

_SEEMS_INT64 = int2byte(0)
_SEEMS_UINT64 = int2byte(1)
_SEEMS_DOUBLE = int2byte(2)

def _get_numeric_type(string):
    for code in iterbytes(string):
        ch = int2byte(code)
        if ch == b'E' or ch == b'e' or ch == b'.':
            return _SEEMS_DOUBLE
        elif ch == b'u':
            return _SEEMS_UINT64
    return _SEEMS_INT64

# Binary literals markers
_STRING_MARKER = int2byte(1)
_INT64_MARKER = int2byte(2)
_DOUBLE_MARKER = int2byte(3)
_FALSE_MARKER = int2byte(4)
_TRUE_MARKER = int2byte(5)
_UINT64_MARKER = int2byte(6)

def _zig_zag_decode(value):
    return (value >> 1) ^ -(value & 1)

class YsonParserBase(object):
    def __init__(self, stream, encoding, always_create_attributes):
        self._line_index = 1
        self._position = 1
        self._offset = 0
        self._stream = stream
        self._lookahead = None
        self._encoding = encoding
        self._always_create_attributes = always_create_attributes

    def _get_position_info(self):
        return self._line_index, self._position, self._offset

    def _read_char(self, binary_input=False):
        if self._lookahead is None:
            result = self._stream.read(1)
        else:
            result = self._lookahead
        self._lookahead = None

        self._offset += 1
        if not binary_input and result == b'\n':
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
                raise YsonParseError(
                    "Premature end-of-stream while reading byte %d out of %d" % (i + 1, char_count),
                    self._get_position_info())
            result.append(ch)
        return b"".join(result)

    def _expect_char(self, expected_ch):
        read_ch = self._read_char()
        if not read_ch:
            raise YsonParseError(
                "Premature end-of-stream expecting '%s' in Yson" % expected_ch,
                self._get_position_info())
        if read_ch != expected_ch:
            raise YsonParseError(
                "Found '%s' while expecting '%s' in Yson" % (read_ch, expected_ch),
                self._get_position_info())

    def _skip_whitespaces(self):
        while self._peek_char().isspace():
            self._read_char()

    def _read_string(self):
        ch = self._peek_char()
        if not ch:
            raise YsonParseError(
                "Premature end-of-stream while expecting string literal in Yson",
                self._get_position_info())
        if ch == _STRING_MARKER:
            return self._read_binary_string()
        if ch == b'"':
            return self._read_quoted_string()
        if not ch.isalpha() and not ch == b'_' and not ch == b'%':
            raise YsonParseError(
                "Expecting string literal but found %s in Yson" % ch,
                self._get_position_info())
        return self._read_unquoted_string()

    def _read_binary_string(self):
        self._expect_char(_STRING_MARKER)
        length = _zig_zag_decode(self._read_varint())
        return self._decode_string(self._read_binary_chars(length))

    def _read_varint(self):
        count = 0
        result = 0
        read_next = True
        while read_next:
            ch = self._read_char()
            if not ch:
                raise YsonParseError(
                    "Premature end-of-stream while reading varinteger in Yson",
                    self._get_position_info())
            byte = ord(ch)
            result |= (byte & 0x7F) << (7 * count)
            if result > 2 ** 64 - 1:
                raise YsonParseError(
                    "Varinteger is too large for Int64 in Yson",
                    self._get_position_info())
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
                raise YsonParseError(
                    "Premature end-of-stream while reading string literal in Yson",
                    self._get_position_info())
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
            raise YsonParseError(
                "Premature end-of-stream while parsing numeric literal in Yson",
                self._get_position_info())
        return b"".join(result)

    def _parse_any(self):
        attributes = None
        if self._has_attributes():
            attributes = self._parse_attributes()

        self._skip_whitespaces()
        ch = self._peek_char()
        if not ch:
            raise YsonParseError(
                "Premature end-of-stream in Yson",
                self._get_position_info())
        elif ch == b'[':
            result = self._parse_list()

        elif ch == b'{':
            result = self._parse_map()

        elif ch == b'#':
            result = self._parse_entity()

        elif ch == _STRING_MARKER:
            result = self._parse_string()

        elif ch == _INT64_MARKER:
            result = self._parse_binary_int64()

        elif ch == _UINT64_MARKER:
            result = self._parse_binary_uint64()

        elif ch == _DOUBLE_MARKER:
            result = self._parse_binary_double()

        elif ch == _FALSE_MARKER:
            self._expect_char(ch)
            result = False

        elif ch == _TRUE_MARKER:
            self._expect_char(ch)
            result = True

        elif ch == b'%':
            result = self._parse_boolean()

        elif ch == b'+' or ch == b'-' or ch.isdigit():
            result = self._parse_numeric()

        elif ch == b'_' or ch == b'"' or ch.isalpha():
            result = self._parse_string()

        else:
            raise YsonParseError(
                "Unexpected character %s in Yson" % ch,
                self._get_position_info())

        return convert.to_yson_type(result, attributes, self._always_create_attributes)

    def _parse_list(self):
        self._expect_char(b'[')
        result = []
        while True:
            self._skip_whitespaces()
            if self._peek_char() == b']':
                break
            value = self._parse_any()
            result.append(value)
            self._skip_whitespaces()
            if self._peek_char() == b']':
                break
            self._expect_char(b';')
        self._expect_char(b']')
        return result

    def _parse_map(self):
        self._expect_char(b'{')
        result = {}
        while True:
            self._skip_whitespaces()
            if self._peek_char() == b'}':
                break
            key = self._read_string()
            self._skip_whitespaces()
            self._expect_char(b'=')
            value = self._parse_any()
            if key in result:
                raise YsonParseError(
                    "Repeated map key '%s' in Yson" % key,
                    self._get_position_info())
            result[key] = value
            self._skip_whitespaces()
            if self._peek_char() == b'}':
                break
            self._expect_char(b';')
        self._expect_char(b'}')
        return result

    def _parse_entity(self):
        self._expect_char(b'#')
        return None

    def _parse_boolean(self):
        self._expect_char(b'%')
        ch = self._peek_char()
        if ch not in [b'f', b't']:
            raise YsonParseError("Found '%s' while expecting 'f' or 't'" % ch)
        if ch == b'f':
            str = self._read_binary_chars(5)
            if str != b"false":
                raise YsonParseError("Incorrect boolean value '%s', expected 'false'" % str)
            return False
        if ch == b't':
            str = self._read_binary_chars(4)
            if str != b"true":
                raise YsonParseError("Incorrect boolean value '%s', expected 'true'" % str)
            return True
        return None

    def _parse_string(self):
        result = self._read_string()
        return result

    def _parse_binary_int64(self):
        self._expect_char(_INT64_MARKER)
        result = _zig_zag_decode(self._read_varint())
        return result

    def _parse_binary_uint64(self):
        self._expect_char(_UINT64_MARKER)
        result = yson_types.YsonUint64(self._read_varint())
        return result

    def _parse_binary_double(self):
        self._expect_char(_DOUBLE_MARKER)
        bytes_ = self._read_binary_chars(struct.calcsize(b'd'))
        result = struct.unpack(b'd', bytes_)[0]
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
                raise YsonParseError(
                    "Failed to parse Int64 literal %s in Yson" % string,
                    self._get_position_info())
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
                raise YsonParseError(
                    "Failed to parse Uint64 literal %s in Yson" % string,
                    self._get_position_info())
        else:
            try:
                result = float(string)
            except ValueError:
                raise YsonParseError(
                    "Failed to parse Double literal %s in Yson" % string,
                    self._get_position_info())
        return result

    def _has_attributes(self):
        self._skip_whitespaces()
        return self._peek_char() == b'<'

    def _parse_attributes(self):
        self._expect_char(b'<')
        result = {}
        while True:
            self._skip_whitespaces()
            if self._peek_char() == b'>':
                break
            key = self._read_string()
            if not key:
                raise YsonParseError(
                    "Empty attribute name in Yson",
                    self._get_position_info())
            self._skip_whitespaces()
            self._expect_char(b'=')
            value = self._parse_any()
            if key in result:
                raise YsonParseError(
                    "Repeated attribute '%s' in Yson" % key,
                    self._get_position_info())
            result[key] = value
            self._skip_whitespaces()
            if self._peek_char() == b'>':
                break
            self._expect_char(b';')
        self._expect_char(b'>')
        return result

class YsonParser(YsonParserBase):
    def __init__(self, stream, encoding, always_create_attributes):
        # COMPAT: Before porting YSON to Python 3 it supported parsing from
        # unicode strings.
        if _is_text_reader(stream) and PY3:
            raise RuntimeError("Only binary streams are supported by YSON parser")
        super(YsonParser, self).__init__(stream, encoding, always_create_attributes)

    def parse(self):
        result = self._parse_any()
        self._skip_whitespaces()
        if self._peek_char():
            raise YsonParseError(
                "Unexpected symbol %s while expecting end-of-stream in Yson" % self._peek_char(),
                self._get_position_info())
        return result

class StreamWrap(object):
    def __init__(self, stream, header, footer):
        self.stream = stream
        self.header = header
        self.footer = footer

        self.pos = 0
        self.state = 0

    def read(self, n):
        if n == 0:
            return self.stream.read(0)

        assert n == 1

        if self.state == 0:
            if self.pos == len(self.header):
                self.state += 1
            else:
                res = int2byte(indexbytes(self.header, self.pos))
                self.pos += 1
                return res

        if self.state == 1:
            sym = self.stream.read(1)
            if sym:
                return sym
            else:
                self.state += 1
                self.pos = 0

        if self.state == 2:
            if self.pos == len(self.footer):
                self.state += 1
            else:
                res = int2byte(indexbytes(self.footer, self.pos))
                self.pos += 1
                return res

        if self.state == 3:
            return b""


def load(stream, yson_type=None, encoding=_ENCODING_SENTINEL, always_create_attributes=True):
    """Deserializes object from YSON formatted stream `stream`.

    :param str yson_type: type of YSON, one of ["node", "list_fragment", "map_fragment"].
    """
    if not PY3 and encoding is not _ENCODING_SENTINEL and encoding is not None:
        raise YsonError("Encoding parameter is not supported for Python 2")

    if encoding is _ENCODING_SENTINEL:
        if PY3:
            encoding = "utf-8"
        else:
            encoding = None

    if yson_type == "list_fragment":
        stream = StreamWrap(stream, b"[", b"]")
    if yson_type == "map_fragment":
        stream = StreamWrap(stream, b"{", b"}")

    parser = YsonParser(stream, encoding, always_create_attributes)
    return parser.parse()

def loads(string, yson_type=None, encoding=_ENCODING_SENTINEL, always_create_attributes=True):
    """Deserializes object from YSON formatted string `string`. See :func:`load <.load>`."""
    return load(BytesIO(string), yson_type, encoding=encoding,
                always_create_attributes=always_create_attributes)
