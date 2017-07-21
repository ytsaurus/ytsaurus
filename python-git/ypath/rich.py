from .common import YPathError
from .tokenizer import YPathTokenizer

from yt.yson.yson_token import *
from yt.yson.tokenizer import YsonTokenizer
from yt.yson.parser import YsonParser
from yt.yson.common import StreamWrap
from yt.yson import YsonEntity
from yt.common import flatten, update

from yt.packages.six import BytesIO, PY3

from copy import deepcopy

class ReadLimit(object):
    def __init__(self, limit=None):
        if limit is not None:
            self.limit = limit
        else:
            self.limit = {}

    def add_key_part(self, key_list):
        if "key" not in self.limit:
            self.limit["key"] = []

        for key in flatten(key_list):
            self.limit["key"].append(key)

    def as_dict(self):
        return self.limit

    def set_row_index(self, row_index):
        self.limit["row_index"] = row_index

class RichYPath(object):
    def __init__(self, path=None, attributes=None):
        self._path = path
        if attributes:
            self._attributes = attributes
        else:
            self._attributes = {}

    def parse_attributes(self, path, attributes):
        if not path:
            return ""
        if PY3:
            encoding = "utf-8"
        else:
            encoding = None

        stream = BytesIO(path)
        parser = YsonParser(stream, encoding, True)

        if not parser._has_attributes():
            return path.decode() if PY3 else path

        parsed_attributes = parser._parse_attributes()
        update(attributes, parsed_attributes)

        str_without_attributes = stream.read()
        path_start = 0
        if PY3:
            str_without_attributes = str_without_attributes.decode()
        while path_start != len(str_without_attributes) and str(str_without_attributes[path_start]).isspace():
            path_start += 1
        return str_without_attributes[path_start:]

    def parse_channel(self, tokenizer, attributes):
        if tokenizer.get_current_type() != TOKEN_LEFT_BRACE:
            return

        columns = []
        tokenizer.parse_next()

        while tokenizer.get_current_type() != TOKEN_RIGHT_BRACE:
            tokenizer.get_current_token().expect_type(TOKEN_STRING)

            value = tokenizer.get_current_token().get_value()
            columns.append(value)
            tokenizer.parse_next()

            tokenizer.get_current_token().expect_type((TOKEN_COMMA, TOKEN_RIGHT_BRACE))
            if tokenizer.get_current_type() == TOKEN_COMMA:
                tokenizer.parse_next()

        tokenizer.parse_next()
        attributes["columns"] = columns

    def parse_key_part(self, tokenizer, row_builder):
        tokenizer.get_current_token().expect_type((TOKEN_STRING, TOKEN_INT64, TOKEN_UINT64,
                                                   TOKEN_DOUBLE, TOKEN_BOOLEAN, TOKEN_HASH))
        row_builder.append(tokenizer.get_current_token().get_value())
        tokenizer.parse_next()

    def parse_row_limit(self, tokenizer, separators, limit):
        if tokenizer.get_current_type() in separators:
            return

        row_builder = []

        if tokenizer.get_current_type() == TOKEN_HASH:
            tokenizer.parse_next()
            tokenizer.get_current_token().expect_type(TOKEN_INT64)
            limit.set_row_index(tokenizer.get_current_token().get_value())
            tokenizer.parse_next()
        elif tokenizer.get_current_type() == TOKEN_LEFT_PARENTHESIS:
            tokenizer.parse_next()

            while tokenizer.get_current_type() != TOKEN_RIGHT_PARENTHESIS:
                self.parse_key_part(tokenizer, row_builder)
                tokenizer.get_current_token().expect_type((TOKEN_COMMA, TOKEN_RIGHT_PARENTHESIS))
                if tokenizer.get_current_type() == TOKEN_COMMA:
                    tokenizer.parse_next()

            tokenizer.parse_next()
            limit.add_key_part(row_builder)
        else:
            self.parse_key_part(tokenizer, row_builder)
            limit.add_key_part(row_builder)

        tokenizer.get_current_token().expect_type(separators)

    def parse_row_ranges(self, tokenizer, attributes):
        if tokenizer.get_current_type() != TOKEN_LEFT_BRACKET:
            return
        ranges = []
        while tokenizer.get_current_token().get_type() != TOKEN_RIGHT_BRACKET:
            tokenizer.parse_next()
            lower_limit = ReadLimit()
            upper_limit = ReadLimit()

            self.parse_row_limit(tokenizer, (TOKEN_COLON, TOKEN_COMMA, TOKEN_RIGHT_BRACKET), lower_limit)

            if tokenizer.get_current_type() == TOKEN_COLON:
                tokenizer.parse_next()
                self.parse_row_limit(tokenizer, (TOKEN_COMMA, TOKEN_RIGHT_BRACKET), upper_limit)
            else:
                if "row_index" in lower_limit.as_dict():
                    upper_limit = ReadLimit(deepcopy(lower_limit.as_dict()))
                    upper_limit.set_row_index(lower_limit.as_dict()["row_index"] + 1)
                else:
                    upper_limit = ReadLimit(deepcopy(lower_limit.as_dict()))
                    key = YsonEntity()
                    key.attributes = {"type": "max"}
                    upper_limit.add_key_part(key)

            row_range = {}
            if lower_limit.as_dict():
                row_range["lower_limit"] = lower_limit.as_dict()

            if upper_limit.as_dict():
                row_range["upper_limit"] = upper_limit.as_dict()

            ranges.append(row_range)

        tokenizer.parse_next()
        attributes["ranges"] = ranges

    def parse(self, path):
        attributes = {}
        if PY3:
            path = bytes(path, "utf-8")

        str_without_attributes = self.parse_attributes(path, attributes)

        ypath_tokenizer = YPathTokenizer(str_without_attributes)

        while ypath_tokenizer.get_type() != TOKEN_END_OF_STREAM \
                and ypath_tokenizer.get_type() != TOKEN_RANGE:
            ypath_tokenizer.advance()

        path = ypath_tokenizer.get_prefix()
        range_str = ypath_tokenizer.get_token()

        if not path:
            raise YPathError("Path should be non-empty")

        if PY3:
            range_str = range_str.encode()

        if ypath_tokenizer.get_type() == TOKEN_RANGE:
            encoding = "utf-8" if PY3 else None
            yson_tokenizer = YsonTokenizer(StreamWrap(BytesIO(range_str), "", ""), encoding)
            yson_tokenizer.parse_next()
            self.parse_channel(yson_tokenizer, attributes)
            self.parse_row_ranges(yson_tokenizer, attributes)
            yson_tokenizer.get_current_token().expect_type(TOKEN_END_OF_STREAM)

        return path, attributes
