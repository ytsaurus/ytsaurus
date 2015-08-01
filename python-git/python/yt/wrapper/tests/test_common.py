from yt.wrapper.common import update, unlist, parse_bool, dict_depth, bool_to_string, \
                              is_prefix, prefix, first_not_none, chunk_iter_lines
from yt.wrapper.response_stream import ResponseStream, EmptyResponseStream
from yt.wrapper.keyboard_interrupts_catcher import KeyboardInterruptsCatcher
from yt.wrapper.verified_dict import VerifiedDict
import yt.wrapper as yt

import random
import string
import sys
import thread

import pytest

def test_update():
    assert update({"a": 10}, {"b": 20}) == {"a": 10, "b": 20}
    assert update({"a": 10}, {"a": 20, "b": {"c": 10}}) == {"a": 20, "b": {"c": 10}}
    assert update({"a": 10, "b": "some"}, {"a": 20, "b": {"c": 10}}) == {"a": 20, "b": {"c": 10}}

def test_unlist():
    assert unlist(["a"]) == "a"
    assert unlist(4) == 4
    assert unlist("abc") == "abc"

def test_parse_bool():
    assert parse_bool("true")
    assert parse_bool("True")
    assert not parse_bool("false")
    assert not parse_bool("False")
    with pytest.raises(yt.YtError):
        parse_bool("42")

def test_dict_depth():
    assert dict_depth({"x": "y"}) == 1
    assert dict_depth({"x": {"y": 1}}) == 2
    assert dict_depth({"x": {"y": 1}, "z": {"t": {"v": 3}}}) == 3
    assert dict_depth(0) == 0

def test_bool_to_string():
    assert bool_to_string(True) == "true"
    assert bool_to_string(False) == "false"
    assert bool_to_string("true") == "true"
    assert bool_to_string("false") == "false"
    assert bool_to_string(1) == "true"
    assert bool_to_string(0) == "false"
    with pytest.raises(yt.YtError):
        bool_to_string("word")
    with pytest.raises(yt.YtError):
        bool_to_string(42)

def test_is_prefix():
    assert is_prefix("ab", "abc")
    assert not is_prefix("ab", "dbac")
    assert is_prefix("", "ab")
    assert is_prefix([1, 2], [1, 2, 3])
    assert not is_prefix([3, 2, 1], [1, 2, 3])
    assert is_prefix([], [1, 2, 3])
    assert not is_prefix(list(xrange(100)), [1])

def test_prefix():
    assert list(prefix([1, 2, 3], 1)) == [1]
    assert list(prefix([1, 2, 3], 10)) == [1, 2, 3]
    assert list(prefix("abc", 2)) == ["a", "b"]
    assert list(prefix([], 1)) == []
    assert list(prefix([1, 2], 0)) == []

def test_first_not_none():
    assert first_not_none([None, None, None, 1]) == 1
    assert first_not_none(["a", None]) == "a"
    with pytest.raises(StopIteration):
        first_not_none([])

def test_chunk_iter_lines():
    # Is it right behaviour?
    lines = ["ab", "abc", "def", "ghijklmn", "op"]
    assert list(chunk_iter_lines(lines, 100)) == [lines]
    assert list(chunk_iter_lines(lines, 3)) == \
           [["ab", "abc"], ["def"], ["ghijklmn"], ["op"]]
    assert list(chunk_iter_lines(["abcdef"], 2)) == [["abcdef"], []]

def test_wrapped_streams():
    import yt.wrapper._py_runner_helpers as runner_helpers
    with pytest.raises(yt.YtError):
        with runner_helpers.WrappedStreams():
            print "test"
    with pytest.raises(yt.YtError):
        with runner_helpers.WrappedStreams():
            sys.stdout.write("test")
    with pytest.raises(yt.YtError):
        with runner_helpers.WrappedStreams():
            input()
    with pytest.raises(yt.YtError):
        with runner_helpers.WrappedStreams():
            sys.stdin.read()
    with runner_helpers.WrappedStreams(wrap_stdout=False):
        print "",
        sys.stdout.write("")

def test_keyboard_interrupts_catcher():
    result = []

    def action():
        result.append(True)
        if len(result) in [1, 3, 5, 6, 7]:
            raise KeyboardInterrupt()

    with KeyboardInterruptsCatcher(action, limit=5):
        raise KeyboardInterrupt()
    with KeyboardInterruptsCatcher(action, limit=5):
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        with KeyboardInterruptsCatcher(action, limit=2):
            raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        with KeyboardInterruptsCatcher(action, enable=False):
            raise KeyboardInterrupt()

def test_verified_dict():
    vdict = VerifiedDict(["a"], {"b": 1, "c": True, "a": {"k": "v"}, "d": {"x": "y"}})
    assert len(vdict) == 4
    assert vdict["b"] == 1
    assert vdict["c"]
    vdict["b"] = 2
    assert vdict["b"] == 2
    del vdict["b"]
    assert "b" not in vdict
    with pytest.raises(RuntimeError):
        vdict["other_key"] = "value"
    with pytest.raises(RuntimeError):
        vdict["b"] = 2
    with pytest.raises(RuntimeError):
        vdict["d"]["other"] = "value"
    vdict["a"]["other"] = "value"

class TestResponseStream(object):
    def test_chunk_iterator(self):
        random_line = lambda: ''.join(random.choice(string.ascii_lowercase) for _ in xrange(100))
        s = '\n'.join(random_line() for _ in xrange(3))

        class StringIterator(object):
            def __init__(self, string, chunk_size=10):
                self.string = string
                self.pos = 0
                self.chunk_size = chunk_size

            def __iter__(self):
                return self

            def next(self):
                str_part = self.string[self.pos:self.pos + self.chunk_size]
                if not str_part:
                    raise StopIteration()
                self.pos += self.chunk_size
                return str_part

        close_list = []
        def close():
            close_list.append(True)

        string_iterator = StringIterator(s, 10)
        stream = ResponseStream(lambda: s, string_iterator, close, lambda x: None, lambda: None)

        assert stream.read(20) == s[:20]
        assert stream.read(2) == s[20:22]

        iterator = stream.chunk_iter()
        assert iterator.next() == s[22:30]
        assert iterator.next() == s[30:40]

        assert stream.readline() == s[40:101]
        assert stream.readline() == s[101:202]

        assert stream.read(8) == s[202:210]

        chunks = []
        for chunk in stream.chunk_iter():
            chunks.append(chunk)

        assert len(chunks) == 10
        assert "".join(chunks) == s[210:]
        assert stream.read() == ""

        stream.close()
        assert len(close_list) > 0

    def test_empty_response_stream(self):
        stream = EmptyResponseStream()
        assert stream.read() == ""
        assert len([x for x in stream.chunk_iter()]) == 0
        assert stream.readline() == ""

        values = []
        for value in stream:
            values.append(value)
        assert len(values) == 0

        with pytest.raises(StopIteration):
            stream.next()
