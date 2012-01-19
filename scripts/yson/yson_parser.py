import struct
from StringIO import StringIO

class YSONParseError(ValueError):
    def __init__(self, message, (line_index, position, offset)):
        ValueError.__init__(self, _format_message(message, line_index, position, offset))
        self.message = message
        self.line_index = line_index
        self.position = position
        self.offset = offset

def _format_message(message, line_index, position, offset):
    return "%s (Line: %d, Poisition: %d, Offset: %d)" % (message, line_index, position, offset)

def _is_letter(ch):
    return ord('A') <= ord(ch) <= ord('Z') or \
           ord('a') <= ord(ch) <= ord('z')

def _is_digit(ch):
    return ord('0') <= ord(ch) <= ord('9')

def _is_whitespace(ch):
    return ch == ' ' or ch == '\t' or ch == '\n' or ch == '\r'

def _seems_integer(string):
    for ch in string:
        if ch == 'E' or ch == 'e' or ch == '.':
            return False
    return True

# Binary literals markers
_INT64_MARKER = chr(1)
_DOUBLE_MARKER = chr(2)
_STRING_MARKER = chr(3)

class YSONParser(object):
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
        while _is_whitespace(self._peek_char()):
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
        if not _is_letter(ch) and not ch == '_':
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

        #result = ctypes.c_longlong(result)
        #one = ctypes.c_int(1)
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
            if not ch or not _is_letter(ch) and ch != '_' and not (_is_digit(ch) and result):
                break
            self._read_char()
            result += ch
        return result

    def _read_numeric(self):
        result = ""
        while True:
            ch = self._peek_char()
            if not ch or not _is_digit(ch) and ch not in "+-.eE":
                break
            self._read_char()
            result += ch
        if not result:
            raise YSONParseError(
                "Premature end-of-stream while parsing numeric literal in YSON",
                self._get_position_info())
        return result

    def _parse_any(self):
        self._skip_whitespaces()
        ch = self._peek_char()
        if not ch:
            raise YSONParseError(
                "Premature end-of-stream in YSON",
                self._get_position_info())
        elif ch == '[':
            return self._parse_list()

        elif ch == '{':
            return self._parse_map()

        elif ch == '<':
            return self._parse_entity()

        elif ch == _STRING_MARKER:
            return self._parse_string()

        elif ch == _INT64_MARKER:
            return self._parse_binary_int64()

        elif ch == _DOUBLE_MARKER:
            return self._parse_binary_double()

        elif ch == '+' or ch == '-' or _is_digit(ch):
            return self._parse_numeric()

        elif ch == '_' or ch == '"' or _is_letter(ch):
            return self._parse_string()

        else:
            raise YSONParseError(
                "Unexpected character %s in YSON" % ch,
                self._get_position_info())

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
        if self._has_attributes():
            attributes = self._parse_attributes()
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
        if self._has_attributes():
            attributes = self._parse_attributes()
        return result

    def _parse_entity(self):
        attributes = self._parse_attributes()
        return None

    def _parse_string(self):
        result = self._read_string()
        if self._has_attributes():
            attributes = self._parse_attributes()
        return result

    def _parse_binary_int64(self):
        self._expect_char(_INT64_MARKER)
        result = self._read_varint()
        if self._has_attributes():
            attributes = self._parse_attributes()
        return result

    def _parse_binary_double(self):
        self._expect_char(_DOUBLE_MARKER)
        bytes = self._read_binary_chars(struct.calcsize('d'))
        result = struct.unpack('d', bytes)[0]
        if self._has_attributes():
            attributes = self._parse_attributes()
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
        if self._has_attributes():
            attributes = self._parse_attributes()
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

    def parse(self):
        return self._parse_any()

if __name__ == "__main__":
    import unittest

    class TestYSONParser(unittest.TestCase):
        def assert_parse(self, string, expected):
            source = StringIO(string)
            parse = YSONParser(source).parse()
            self.assertEqual(parse, expected)

        def test_quoted_string(self):
            self.assert_parse('"abc\\"\\n"', 'abc"\n')

        def test_unquoted_string(self):
            self.assert_parse('abc10', 'abc10')

        def test_binary_string(self):
            self.assert_parse('\x03\x06abc', 'abc')

        def test_int(self):
            self.assert_parse('64', 64)

        def test_binary_int(self):
            self.assert_parse('\x01\x81\x40', -(2 ** 12) - 1)

        def test_double(self):
            self.assert_parse('1.5', 1.5)

        def test_exp_double(self):
            self.assert_parse('1.73e23', 1.73e23)

        def test_binary_double(self):
            self.assert_parse('\x02\x00\x00\x00\x00\x00\x00\xF8\x3F', 1.5)

        def test_empty_list(self):
            self.assert_parse('[ ]', [])

        def test_one_element_list(self):
            self.assert_parse('[a]', ['a'])

        def test_list(self):
            self.assert_parse('[1; 2]', [1, 2])

        def test_empty_map(self):
            self.assert_parse('{ }', {})

        def test_one_element_map(self):
            self.assert_parse('{a=1}', {'a': 1})

        def test_map(self):
            self.assert_parse('{a = b; c = d}', {'a': 'b', 'c': 'd'})

        def test_entity(self):
            self.assert_parse(' <a = b; c = d>', None)

        def test_nested(self):
            self.assert_parse(
                '''
                {
                    path = "/home/sandello";
                    mode = 755;
                    read = [
                            "*.sh";
                            "*.py"
                           ]
                }
                ''',
                {'path' : '/home/sandello', 'mode' : 755, 'read' : ['*.sh', '*.py']})

    unittest.main()