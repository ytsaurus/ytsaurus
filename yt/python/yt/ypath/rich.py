from .common import YPathError
from .tokenizer import YPathTokenizer

from yt.yson.yson_token import (
    TOKEN_END_OF_STREAM,
    TOKEN_RANGE,
    TOKEN_LEFT_BRACE,
    TOKEN_RIGHT_BRACE,
    TOKEN_HASH,
    TOKEN_LEFT_BRACKET,
    TOKEN_RIGHT_BRACKET,
    TOKEN_LEFT_PARENTHESIS,
    TOKEN_RIGHT_PARENTHESIS,
    TOKEN_COLON,
    TOKEN_COMMA,
    TOKEN_STRING,
    TOKEN_INT64,
    TOKEN_UINT64,
    TOKEN_DOUBLE,
    TOKEN_BOOLEAN)
from yt.yson.tokenizer import YsonTokenizer
from yt.yson.parser import YsonParser
from yt.yson.common import StreamWrap
from yt.common import flatten, update_inplace

try:
    from yt.packages.six import BytesIO, PY3, text_type
except ImportError:
    from six import BytesIO, PY3, text_type


class ReadLimit(object):
    def __init__(self, limit=None):
        if limit is not None:
            self.limit = limit
        else:
            self.limit = {}

    def set_key(self, key_list):
        assert "key" not in self.limit
        self.limit["key"] = []

        for value in flatten(key_list):
            self.limit["key"].append(value)

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
            return path

        parsed_attributes = parser._parse_attributes()
        update_inplace(attributes, parsed_attributes)

        str_without_attributes = stream.read()
        path_start = 0
        while path_start != len(str_without_attributes) and (str_without_attributes[path_start:path_start + 1] == b' '):
            path_start += 1
        return str_without_attributes[path_start:]

    def parse_columns(self, tokenizer, attributes):
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
            limit.set_key(row_builder)
        else:
            self.parse_key_part(tokenizer, row_builder)
            limit.set_key(row_builder)

        tokenizer.get_current_token().expect_type(separators)

    def parse_row_ranges(self, tokenizer, attributes):
        if tokenizer.get_current_type() != TOKEN_LEFT_BRACKET:
            return
        ranges = []
        while tokenizer.get_current_token().get_type() != TOKEN_RIGHT_BRACKET:
            tokenizer.parse_next()
            lower_limit = ReadLimit()
            upper_limit = ReadLimit()
            exact = ReadLimit()

            self.parse_row_limit(tokenizer, (TOKEN_COLON, TOKEN_COMMA, TOKEN_RIGHT_BRACKET), lower_limit)

            if tokenizer.get_current_type() == TOKEN_COLON:
                tokenizer.parse_next()
                self.parse_row_limit(tokenizer, (TOKEN_COMMA, TOKEN_RIGHT_BRACKET), upper_limit)
            else:
                # This is the case of exact limit.
                exact = lower_limit
                lower_limit = ReadLimit()

            row_range = {}
            if lower_limit.as_dict():
                row_range["lower_limit"] = lower_limit.as_dict()

            if upper_limit.as_dict():
                row_range["upper_limit"] = upper_limit.as_dict()

            if exact.as_dict():
                row_range["exact"] = exact.as_dict()

            ranges.append(row_range)

        tokenizer.parse_next()
        attributes["ranges"] = ranges

    def is_root_designator(self, symbol):
        return symbol in b"/#"

    def starts_with_root_designator(self, path):
        non_space_index = len(path) - len(path.lstrip(b' '))
        if non_space_index != len(path) and not self.is_root_designator(path[non_space_index]):
            return False
        return True

    def is_valid_cluster_symbol(self, symbol):
        if PY3:
            return symbol in b"_-" or chr(symbol).isalnum()
        return symbol in b"_-" or symbol.isalnum()

    def parse_cluster(self, path, attributes):
        if len(path) == 0:
            return path
        if self.starts_with_root_designator(path):
            return path
        if b'://' not in path and b':#' not in path:
            return path
        if b':' not in path:
            raise YPathError(
                "Path {0} does not start with a valid root-designator, cluster://path short-form assumed; "
                "no \':\' separator symbol found to parse cluster".format(path))

        cluster_separator_index = path.index(b':')
        cluster_name = path[0:cluster_separator_index]

        if len(cluster_name) == 0:
            raise YPathError("Path {0} does not start with a valid root-designator, cluster://path short-form assumed; "
                             "cluster name cannot be empty".format(path))

        is_valid_symbol = list(map(self.is_valid_cluster_symbol, cluster_name))

        if not all(is_valid_symbol):
            raise YPathError("Path {0} does not start with a valid root-designator, cluster://path short-form assumed; "
                             "cluster name contains illegal symbol {1}".format(path, is_valid_symbol.index(False)))

        remaining_string = path[cluster_separator_index + 1:]

        if not self.starts_with_root_designator(remaining_string):
            raise YPathError("Path {0} does not start with a valid root-designator, cluster://path short-form assumed; "
                             "path {1} after cluster-separator does not start with a valid root-designator".format(path, remaining_string))

        if PY3:
            cluster_name = cluster_name.decode()
        attributes["cluster"] = cluster_name
        return remaining_string

    def parse(self, path):
        attributes = {}
        if PY3:
            encoding = "utf-8"
        else:
            encoding = None
        if isinstance(path, text_type) and PY3:
            path = bytes(path, "utf-8")

        str_without_attributes = self.parse_attributes(path, attributes)
        str_without_attributes = self.parse_cluster(str_without_attributes, attributes)

        ypath_tokenizer = YPathTokenizer(str_without_attributes)

        while ypath_tokenizer.get_type() != TOKEN_END_OF_STREAM \
                and ypath_tokenizer.get_type() != TOKEN_RANGE:
            ypath_tokenizer.advance()

        path = ypath_tokenizer.get_prefix()
        range_str = ypath_tokenizer.get_token()

        if not path:
            raise YPathError("Path should be non-empty")

        if PY3:
            path = path.decode()

        if ypath_tokenizer.get_type() == TOKEN_RANGE:
            yson_tokenizer = YsonTokenizer(StreamWrap(BytesIO(range_str), "", ""), encoding)
            yson_tokenizer.parse_next()
            self.parse_columns(yson_tokenizer, attributes)
            self.parse_row_ranges(yson_tokenizer, attributes)
            yson_tokenizer.get_current_token().expect_type(TOKEN_END_OF_STREAM)

        return path, attributes
