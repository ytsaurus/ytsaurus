from .common import YPathError

from yt.yson.yson_token import (
    TOKEN_RANGE,
    TOKEN_LITERAL,
    TOKEN_START_OF_STREAM,
    TOKEN_END_OF_STREAM,
    TOKEN_LEFT_BRACE,
    TOKEN_LEFT_BRACKET,
    char_to_token_type)


class YPathTokenizer(object):
    def __init__(self, path):
        self._path = path
        self._type = TOKEN_START_OF_STREAM
        self._token = b""

        self._prefix_len = 0
        self._offset = 0

    def advance(self):
        self._offset += len(self._token)

        current = self._offset
        if current == len(self._path):
            self._type = TOKEN_END_OF_STREAM
            return self._type

        self._type = TOKEN_LITERAL
        while current != len(self._path):
            token = char_to_token_type(self._path[current])
            if token == TOKEN_LEFT_BRACE or token == TOKEN_LEFT_BRACKET or self._path[current] in b"/@&*":
                break

            elif self._path[current] in b"\\":
                current = self.advance_escaped(current)
            else:
                current += 1

        if current == self._offset:
            token = char_to_token_type(self._path[current])
            if token == TOKEN_LEFT_BRACE or token == TOKEN_LEFT_BRACKET:
                self._type = TOKEN_RANGE
                current = len(self._path)
            elif self._path[current] in b"/@&*":
                self._token = self._path[current:current + 1]
                self._type = char_to_token_type(self._path[current])
                return self._type

        self._token = self._path[self._offset: current]
        return self._type

    def throw_malformed_escape_sequence(self, context):
        raise YPathError("Malformed escape sequence {0} in YPath".format(context))

    def advance_escaped(self, current):
        assert self._path[current] in b"\\"
        current += 1
        if current == len(self._path):
            raise YPathError("Unexpected end-of-string in YPath while parsing escape sequence")

        if self._path[current] in b"\\/@&*[{":
            current += 1
        elif self._path[current] in b"x":
            if current + 2 >= len(self._path):
                self.throw_malformed_escape_sequence(self._path[current - 1:])
            context = self._path[current - 1: current + 2]
            try:
                int(self._path[current + 1: current + 3], base=16)
                current += 3
            except ValueError:
                self.throw_malformed_escape_sequence(context)
        else:
            self.throw_malformed_escape_sequence(self._path[current - 1:current + 1])
        return current

    def skip(self, type):
        if self._type == type:
            self.advance()
            return True
        return False

    def get_type(self):
        return self._type

    def get_token(self):
        return self._token

    def get_prefix(self):
        return self._path[:self._offset]
