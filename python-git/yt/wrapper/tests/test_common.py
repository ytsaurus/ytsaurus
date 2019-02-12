# -*- coding: utf-8 -*-

from yt.wrapper.errors import YtHttpResponseError
from yt.wrapper.common import (update, unlist, parse_bool, dict_depth, bool_to_string,
                               is_prefix, prefix, first_not_none, group_blobs_by_size,
                               datetime_to_string, date_string_to_timestamp, chunk_iter_list,
                               escape_c)

from yt.packages.six.moves import xrange, cPickle as pickle

import yt.wrapper as yt

from datetime import datetime
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

def test_group_blobs_by_size():
    # Is it right behaviour?
    lines = ["ab", "abc", "def", "ghijklmn", "op"]
    assert list(group_blobs_by_size(lines, 100)) == [lines]
    assert list(group_blobs_by_size(lines, 3)) == \
           [["ab", "abc"], ["def"], ["ghijklmn"], ["op"]]
    assert list(group_blobs_by_size(["abcdef"], 2)) == [["abcdef"]]

def test_time_functions():
    now = datetime.now()
    now_utc = datetime.utcnow()
    str1 = datetime_to_string(now_utc)
    str2 = datetime_to_string(now, is_local=True)
    tm1 = date_string_to_timestamp(str1)
    tm2 = date_string_to_timestamp(str2)
    assert abs(tm1 - tm2) < 10

def test_error_pickling():
    error = yt.YtError("error", code=100, attributes={"attr": 10})
    pickled_error = pickle.dumps(error)
    assert pickle.loads(pickled_error).message == error.message

    error = YtHttpResponseError({"code": 10, "message": "error"}, url="http://aaa.bbb", headers={}, params={})
    pickled_error = pickle.dumps(error)
    assert pickle.loads(pickled_error).message == error.message

def test_error_str():
    error = yt.YtError(u"моя ошибка", code=100, attributes={"аттрибут": 10, "другой атрибут": "со странным значением"})
    assert "10" in str(error)

def test_chunk_iter_list():
    assert list(chunk_iter_list([1, 2, 3], chunk_size=1)) == [[1], [2], [3]]
    assert list(chunk_iter_list([1, 2, 3], chunk_size=2)) == [[1, 2], [3]]
    assert list(chunk_iter_list([1, 2, 3], chunk_size=5)) == [[1, 2, 3]]
    assert list(chunk_iter_list([], chunk_size=1)) == []

def test_escape_c():
    assert escape_c("http://ya.ru/") == "http://ya.ru/"
    assert escape_c("http://ya.ru/\x17\n") == "http://ya.ru/\\x17\\n"
    assert escape_c("http://ya.ru/\0") == "http://ya.ru/\\0"
    assert escape_c("http://ya.ru/\0\0" + "0") == "http://ya.ru/\\0\\0000"
    assert escape_c("http://ya.ru/\0\x00" + "1") == "http://ya.ru/\\0\\0001"
    assert escape_c("\2\4\6" + "78") == "\\2\\4\\00678"
    assert escape_c("\2\4\6" + "89") == "\\2\\4\\689"
    assert escape_c("\"Hello\", Alice said.") == "\\\"Hello\\\", Alice said."
    assert escape_c("Slash\\dash!") == "Slash\\\\dash!"
    assert escape_c("There\nare\r\nnewlines.") == "There\\nare\\r\\nnewlines."
    assert escape_c("There\tare\ttabs.") == "There\\tare\\ttabs."
    assert escape_c("There are questions ???") == "There are questions \\x3F\\x3F?"
    assert escape_c("There are questions ??") == "There are questions \\x3F?"
