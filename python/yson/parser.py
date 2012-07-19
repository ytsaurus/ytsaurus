import yson_types

import struct
from StringIO import StringIO

__all__ = ["parse", "parse_string"]

class YSONParseError(ValueError):
    def __init__(self, message, (line_index, position, offset)):
        ValueError.__init__(self, _format_message(message, line_index, position, offset))
        self.message = message
        self.line_index = line_index
        self.position = position
        self.offset = offset

def _format_message(message, line_index, position, offset):
    return "%s (Line: %d, Poisition: %d, Offset: %d)" % (message, line_index, position, offset)

def _seems_integer(string):
    for ch in string:
        if ch == 'E' or ch == 'e' or ch == '.':
            return False
    return True

# Binary literals markers
_INT64_MARKER = chr(1)
_DOUBLE_MARKER = chr(2)
_STRING_MARKER = chr(3)

class YSONParserBase(object):
    def __init__(self, stream):
        self._line_index = 1
        self._position = 1
        self._offset = 0
        self._stream = stream
        self._lookahead = None

    def _get_position_info(self):
        return (self._line_index, self._position, self._offset)

    def _read_char(self, binary_input = False):
        if self._lookahead is None:
            self._peek_char()

        result = self._lookahead
        self._lookahead = None

        self._offset += 1
        if not binary_input and result == '\n':
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
        result = ''
        for i in xrange(char_count):
            ch = self._read_char(True)
            if not ch:
                raise YSONParseError(
                    "Premature end-of-stream while reading byte %d out of %d" % (i + 1, char_count),
                    self._get_position_info())
            result += ch
        return result

    def _expect_char(self, expected_ch):
        read_ch = self._read_char()
        if not read_ch:
            raise YSONParseError(
                "Premature end-of-stream expecting '%s' in YSON" % expected_ch,
                self._get_position_info())
        if read_ch != expected_ch:
            raise YSONParseError(
                "Found '%s' while expecting '%s' in YSON" % (read_ch, expected_ch),
                self._get_position_info())

    def _skip_whitespaces(self):
        while self._peek_char().isspace():
            self._read_char()

    def _read_string(self):
        ch = self._peek_char()
        if not ch:
            raise YSONParseError(
                "Premature end-of-stream while expecting string literal in YSON",
                self._get_position_info())
        if ch == _STRING_MARKER:
            return self._read_binary_string()
        if ch == '"':
            return self._read_quoted_string()
        if not ch.isalpha() and not ch == '_' and not ch == '%':
            raise YSONParseError(
                "Expecting string literal but found %s in YSON" % ch,
                self._get_position_info())
        return self._read_unquoted_string()

    def _read_binary_string(self):
        self._expect_char(_STRING_MARKER)
        length = self._read_varint()
        return self._read_binary_chars(length)

    def _read_varint(self):
        count = 0
        result = 0
        read_next = True
        while read_next:
            ch = self._read_char()
            if not ch:
                raise YSONParseError(
                    "Premature end-of-stream while reading varinteger in YSON",
                    self._get_position_info())
            byte = ord(ch)
            result |= (byte & 0x7F) << (7 * count)
            if result > 2 ** 64 - 1:
                raise YSONParseError(
                    "Varinteger is too large for Int64 in YSON",
                    self._get_position_info())
            count += 1
            read_next = byte & 0x80 != 0

        result = (result >> 1) ^ -(result & 1)
        return result

    def _read_quoted_string(self):
        self._expect_char('"')
        result = ""
        pending_next_char = False
        while True:
            ch = self._read_char()
            if not ch:
                raise YSONParseError(
                    "Premature end-of-stream while reading string literal in YSON",
                    self._get_position_info())
            if ch == '"' and not pending_next_char:
                break
            result += ch
            if pending_next_char:
                pending_next_char = False
            elif ch == '\\':
                pending_next_char = True
        return result.decode('string_escape')

    def _read_unquoted_string(self):
        result = ""
        while True:
            ch = self._peek_char()
            if ch and (ch.isalpha() or ch.isdigit() or ch in '_%-'):
                self._read_char()
                result += ch
            else:
                break
        return result

    def _read_numeric(self):
        result = ""
        while True:
            ch = self._peek_char()
            if not ch or not ch.isdigit() and ch not in "+-.eE":
                break
            self._read_char()
            result += ch
        if not result:
            raise YSONParseError(
                "Premature end-of-stream while parsing numeric literal in YSON",
                self._get_position_info())
        return result

    def _parse_any(self):
        attributes = None
        if self._has_attributes():
            attributes = self._parse_attributes()

        self._skip_whitespaces()
        ch = self._peek_char()
        if not ch:
            raise YSONParseError(
                "Premature end-of-stream in YSON",
                self._get_position_info())
        elif ch == '[':
            result = self._parse_list()

        elif ch == '{':
            result = self._parse_map()

        elif ch == '#':
            result = self._parse_entity()

        elif ch == _STRING_MARKER:
            result = self._parse_string()

        elif ch == _INT64_MARKER:
            result = self._parse_binary_int64()

        elif ch == _DOUBLE_MARKER:
            result = self._parse_binary_double()

        elif ch == '+' or ch == '-' or ch.isdigit():
            result = self._parse_numeric()

        elif ch == '_' or ch == '"' or ch == '%' or ch.isalpha():
            result = self._parse_string()

        else:
            raise YSONParseError(
                "Unexpected character %s in YSON" % ch,
                self._get_position_info())

        return yson_types.convert_to_YSON_type(result, attributes)

    def _parse_list(self):
        self._expect_char('[')
        result = []
        while True:
            self._skip_whitespaces()
            if self._peek_char() == ']':
                break
            value = self._parse_any()
            result.append(value)
            self._skip_whitespaces()
            if self._peek_char() == ']':
                break
            self._expect_char(';')
        self._expect_char(']')
        return result

    def _parse_map(self):
        self._expect_char('{')
        result = {}
        while True:
            self._skip_whitespaces()
            if self._peek_char() == '}':
                break
            key = self._read_string()
            if not key:
                raise YSONParseError(
                    "Empty map item name in YSON",
                    self._get_position_info())
            self._skip_whitespaces()
            self._expect_char('=')
            value = self._parse_any()
            if key in result:
                raise YSONParseError(
                    "Repeated map key '%s' in YSON" % key,
                    self._get_position_info())
            result[key] = value
            self._skip_whitespaces()
            if self._peek_char() == '}':
                break
            self._expect_char(';')
        self._expect_char('}')
        return result

    def _parse_entity(self):
        self._expect_char('#')
        return None

    def _parse_string(self):
        result = self._read_string()
        return result

    def _parse_binary_int64(self):
        self._expect_char(_INT64_MARKER)
        result = self._read_varint()
        return result

    def _parse_binary_double(self):
        self._expect_char(_DOUBLE_MARKER)
        bytes = self._read_binary_chars(struct.calcsize('d'))
        result = struct.unpack('d', bytes)[0]
        return result

    def _parse_numeric(self):
        string = self._read_numeric()
        if _seems_integer(string):
            try:
                result = int(string)
                if result > 2 ** 63 - 1 or result < -(2 ** 63):
                    raise ValueError()
            except ValueError:
                raise YSONParseError(
                    "Failed to parse Int64 literal %s in YSON" % string,
                    self._get_position_info())
        else:
            try:
                result = float(string)
            except ValueError:
                raise YSONParseError(
                    "Failed to parse Double literal %s in YSON" % string,
                    self._get_position_info())
        return result

    def _has_attributes(self):
        self._skip_whitespaces()
        return self._peek_char() == '<'

    def _parse_attributes(self):
        self._expect_char('<')
        result = {}
        while True:
            self._skip_whitespaces()
            if self._peek_char() == '>':
                break
            key = self._read_string()
            if not key:
                raise YSONParseError(
                    "Empty attribute name in YSON",
                    self._get_position_info())
            self._skip_whitespaces()
            self._expect_char('=')
            value = self._parse_any()
            if key in result:
                raise YSONParseError(
                    "Repeated attribute '%s' in YSON" % key,
                    self._get_position_info())
            result[key] = value
            self._skip_whitespaces()
            if self._peek_char() == '>':
                break
            self._expect_char(';')
        self._expect_char('>')
        return result

class YSONParser(YSONParserBase):
    def __init__(self, stream):
        super(YSONParser, self).__init__(stream)

    def parse(self):
        result = self._parse_any()
        self._skip_whitespaces()
        if self._peek_char():
            raise YSONParseError(
                "Unexpected symbol %s while expecting end-of-stream in YSON" % self._peek_char(),
                self._get_position_info())
        return result

class YSONFragmentedParser(YSONParserBase):
    def __init__(self, stream):
        super(YSONFragmentedParser, self).__init__(stream)

    def has_next(self):
        self._skip_whitespaces()
        return len(self._peek_char()) > 0

    def parse_next(self):
        return self._parse_any()

def parse(stream):
    parser = YSONParser(stream)
    return parser.parse()

def parse_string(string):
    return parse(StringIO(string))

#TODO(panin): refactor
def parse_list_fragment(string):
    return parse_string('[' + string + ']')
