# -*- coding: utf-8 -*-

from yt.testlib import authors, yatest_common  # noqa

from yt.wrapper.errors import create_http_response_error, YtRpcUnavailable
from yt.wrapper.common import (update, unlist, parse_bool, dict_depth,
                               is_prefix, prefix, first_not_none, merge_blobs_by_size,
                               datetime_to_string, date_string_to_timestamp, chunk_iter_list,
                               escape_c)

try:
    from yt.packages.six.moves import xrange, cPickle as pickle
except ImportError:
    from six.moves import xrange, cPickle as pickle

import yt.wrapper as yt

from flaky import flaky

from datetime import datetime, timedelta
import pytest

try:
    from yt.packages.six import PY3
except ImportError:
    from six import PY3


def is_debug():
    try:
        from yson_lib import is_debug_build
    except ImportError:
        from yt_yson_bindings.yson_lib import is_debug_build

    return is_debug_build()


@authors("ignat")
def test_update():
    assert update({"a": 10}, {"b": 20}) == {"a": 10, "b": 20}
    assert update({"a": 10}, {"a": 20, "b": {"c": 10}}) == {"a": 20, "b": {"c": 10}}
    assert update({"a": 10, "b": "some"}, {"a": 20, "b": {"c": 10}}) == {"a": 20, "b": {"c": 10}}


@authors("asaitgalin")
def test_unlist():
    assert unlist(["a"]) == "a"
    assert unlist(4) == 4
    assert unlist("abc") == "abc"


@authors("asaitgalin")
def test_parse_bool():
    assert parse_bool("true")
    assert parse_bool("True")
    assert not parse_bool("false")
    assert not parse_bool("False")
    with pytest.raises(yt.YtError):
        parse_bool("42")


@authors("asaitgalin")
def test_dict_depth():
    assert dict_depth({"x": "y"}) == 1
    assert dict_depth({"x": {"y": 1}}) == 2
    assert dict_depth({"x": {"y": 1}, "z": {"t": {"v": 3}}}) == 3
    assert dict_depth(0) == 0


@authors("asaitgalin")
def test_is_prefix():
    assert is_prefix("ab", "abc")
    assert not is_prefix("ab", "dbac")
    assert is_prefix("", "ab")
    assert is_prefix([1, 2], [1, 2, 3])
    assert not is_prefix([3, 2, 1], [1, 2, 3])
    assert is_prefix([], [1, 2, 3])
    assert not is_prefix(list(xrange(100)), [1])


@authors("asaitgalin")
def test_prefix():
    assert list(prefix([1, 2, 3], 1)) == [1]
    assert list(prefix([1, 2, 3], 10)) == [1, 2, 3]
    assert list(prefix("abc", 2)) == ["a", "b"]
    assert list(prefix([], 1)) == []
    assert list(prefix([1, 2], 0)) == []


@authors("asaitgalin")
def test_first_not_none():
    assert first_not_none([None, None, None, 1]) == 1
    assert first_not_none(["a", None]) == "a"
    with pytest.raises(StopIteration):
        first_not_none([])


@authors("levysotsky")
def test_merge_blobs_by_size():
    lines = [b"ab", b"abc", b"def", b"ghijklmn", b"op"]
    assert list(merge_blobs_by_size(lines, 100)) == [b"".join(lines)]
    assert list(merge_blobs_by_size(lines, 3)) == \
        [b"ababc", b"def", b"ghijklmn", b"op"]
    assert list(merge_blobs_by_size([b"abcdef"], 2)) == [b"abcdef"]


@authors("levysotsky")
@flaky(max_runs=3)
@pytest.mark.skipif('yatest_common.context.sanitize == "address"')
def test_merge_blobs_by_size_performance():
    if is_debug():
        pytest.skip()
    start = datetime.now()
    size = 1000000
    s = (b"x" for _ in xrange(size))
    assert list(merge_blobs_by_size(s, size)) == [b"x" * size]
    assert datetime.now() - start < timedelta(seconds=1)


@authors("ignat")
def test_time_functions():
    now = datetime.now()
    now_utc = datetime.utcnow()
    str1 = datetime_to_string(now_utc)
    str2 = datetime_to_string(now, is_local=True)
    tm1 = date_string_to_timestamp(str1)
    tm2 = date_string_to_timestamp(str2)
    assert abs(tm1 - tm2) < 10


@authors("ignat")
def test_error_pickling():
    error = yt.YtError("error", code=100, attributes={"attr": 10})
    pickled_error = pickle.dumps(error)
    assert pickle.loads(pickled_error).message == error.message

    error = create_http_response_error(
        {"code": 10, "message": "error"},
        url="http://aaa.bbb", request_headers={}, response_headers={}, params={})
    pickled_error = pickle.dumps(error)
    loaded_error = pickle.loads(pickled_error)
    assert loaded_error.url == error.url
    assert loaded_error.message == error.message

    error = create_http_response_error(
        {"code": 105, "message": "RPC unavailable"},
        url="http://aaa.bbb",  request_headers={}, response_headers={}, params={})
    assert isinstance(error, YtRpcUnavailable)

    pickled_error = pickle.dumps(error)
    loaded_error = pickle.loads(pickled_error)
    assert isinstance(loaded_error, YtRpcUnavailable)
    assert loaded_error.url == error.url
    assert loaded_error.message == error.message


@authors("ignat")
def test_error_str():
    error = yt.YtError(u"моя ошибка", code=100, attributes={"аттрибут": 10, "другой атрибут": "со странным значением"})
    assert "10" in str(error)

    # Check for YsonEntity in params.
    error = create_http_response_error(
        {"code": 10, "message": "error"},
        url="http://a.b", request_headers={}, response_headers={}, params={"x": yt.yson.YsonEntity()})
    assert "null" in str(error)


@authors("ostyakov")
def test_chunk_iter_list():
    assert list(chunk_iter_list([1, 2, 3], chunk_size=1)) == [[1], [2], [3]]
    assert list(chunk_iter_list([1, 2, 3], chunk_size=2)) == [[1, 2], [3]]
    assert list(chunk_iter_list([1, 2, 3], chunk_size=5)) == [[1, 2, 3]]
    assert list(chunk_iter_list([], chunk_size=1)) == []


@authors("ignat")
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
    assert escape_c(bytearray("\0\1What about some bytes?", "utf-8")) == "\\x00\\x01\\x57\\x68\\x61\\x74\\x20\\x61\\x62\\x6F\\x75\\x74\\x20\\x73\\x6F\\x6D\\x65\\x20\\x62\\x79\\x74\\x65\\x73\\x3F"
    if PY3:
        assert escape_c(b"\0\1What about some bytes?") == "\\x00\\x01\\x57\\x68\\x61\\x74\\x20\\x61\\x62\\x6F\\x75\\x74\\x20\\x73\\x6F\\x6D\\x65\\x20\\x62\\x79\\x74\\x65\\x73\\x3F"
    else:
        assert escape_c(b"\0\1What about some bytes?") == "\\0\\1What about some bytes?"
