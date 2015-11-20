import misc

import pytest

import subprocess
import datetime

pytestmark = pytest.mark.skipif("sys.version_info < (2,7)", reason="requires python2.7")


def test_compression_external():
    p = subprocess.Popen(["gzip", "-d"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdoutdata, stderrdata = p.communicate(misc.gzip_compress("Hello"))
    assert stdoutdata == "Hello"
    assert not stderrdata


def test_compression_internal():
    assert misc.gzip_decompress(misc.gzip_compress("Hello")) == "Hello"


def test_event_log_timestamp_parse():
    d = datetime.datetime.strptime("2014-10-02T15:31:24.887386Z".split(".")[0], "%Y-%m-%dT%H:%M:%S")
    assert d.hour == 15
    assert d.minute == 31
    assert d.second == 24
    assert d.date().year == 2014
    assert d.date().month == 10
    assert d.date().day == 2


def test_normalize_timestamp():
    assert misc.normalize_timestamp("2014-10-02T15:31:24.887386Z")[0] == "2014-10-02 15:31:24"
    assert misc.normalize_timestamp("2014-10-10T14:07:22.295882Z")[0] == "2014-10-10 14:07:22"

def test_normalize_revert():
    assert misc.revert_timestamp(*misc.normalize_timestamp("2014-10-02T15:31:24.887386Z")) == "2014-10-02T15:31:24.887386Z"


def compare_types(left, right):
    if isinstance(left, basestring) and isinstance(right, basestring):
        pass
    else:
        assert type(left) == type(right)


def compare(left, right):
    compare_types(left, right)
    if isinstance(left, dict):
        assert len(left.keys()) == len(right.keys())
        for k in left.keys():
            compare(left[k], right[k])
    else:
        assert left == right


def test_convert_to_from():
    dataset = [
        dict(key1="value1", key2="value2"),
        dict(ks="string", ki=3),
        dict(key=dict(s1="v1", s2="v2"), other_key="data")
#        dict(key="{}")
    ]
    for data in dataset:
        data_after = misc.convert_from_logbroker_format(misc.convert_to_logbroker_format(data))
        compare(data, data_after)

