# coding=utf-8
from __future__ import print_function

from .conftest import authors
from .helpers import set_config_option, yatest_common

from yt.common import update, YtResponseError
from yt.wrapper.driver import make_request, get_api_version
from yt.yson import loads, YsonString, YsonUnicode, YsonError, to_yson_type
from yt.wrapper import YtError, YsonFormat, YPath
from yt.ypath import YPathError, parse_ypath

try:
    from yt.packages.six.moves import xrange
except ImportError:
    from six.moves import xrange

from flaky import flaky

import copy
import pytest
import time
import sys

TEST_PATHS = ["//tmp/table[#1:#2]",
              "/",
              "//",
              "//[#10]",
              "//[#+10]",
              "//[:]",
              "//home/test/@[a:b]",
              "//home/test[12u]",
              "//home/test[12]",
              "//[:,a]",
              "//[abc]",
              "//[(a,b,c)]",
              "//[   ( a,   b,    c)   ]",
              "//[abc:]",
              "//[:abc]",
              "//{a, b}",
              "//{   a,      b    }",
              "//[abc]    ",
              "// [abc]    ",
              "//home/123/@",
              "//home/table[ab:ab]",
              "//home/table[#1:#1]",
              "//home/table[:(#, 10)]",
              "//home/table[(a, #):(#, b)]",
              "//some/table{}",
              "//some/table\\[brackets] and spaces and русский",
              "//some/table{a}",
              "//some/table{a,b}",
              "//some/table[(a,b,1):(a,c)]",
              "//some/table[(a,b,1):]",
              "//some/table[(a,b,1)]",
              "//some/table{ab,ac}[a:(b,1,0.0)]",
              "//some/table{ab,ac}[:5.0,10.0:]",
              "//some/list/before:2",
              "//some/list/after:5",
              "//home/vasya/*",
              "//home/vasya/*[a,#100,#1:#2]",
              "//some/list/before:2[100u:200u]",
              "<append=true;custom=123>//home/vasya/t",
              "<append=true;filename=name.txt>//home/petya/ttt",
              "/dir_\\\\_x",
              "//test/[\x01\x06abc, a]",
              "//table/[\x03\x00\x00\x00\x00\x00\x00\x03\x3F:test]",
              " <a=b> //home",
              "         <append=true;custom=123>     //home/vasya/t",
              "      //home",
              '//test/&[abc:"abc\x00"]',
              '//some/table{ab,ac}["abc,de"]',
              '//some/table{ab,ac}["abc,[]askjjh,,,,asdj  ade"]',
              '//some/table{"abc,[]askjjh,,,,asdj  ade"}',
              '<append=true;custom=123;test="test>">//some/table'
              '{"abc,[]askjjh,,,,asdj  ade"}']

FAILED_TEST_PATHS = ["<ds>><test",
                     "{}{}",
                     "[]{}",
                     "[?]",
                     "{::}",
                     "{a:1}",
                     "[+]",
                     r"//home/{/\;}",
                     "//home/{!@#.,/$%^&*()|_+}",
                     r"//home/[|/~`\;]",
                     "//home/[!@#$%/.,^&*()_+|]",
                     "//home/[\xd1]",
                     "[//home]",
                     "//home/test[]s",
                     "//home/test[]щ"]


def make_parse_ypath_request(path, client=None):
    attributes = {}
    if isinstance(path, (YsonString, YsonUnicode)):
        attributes = copy.deepcopy(path.attributes)

    result = loads(make_request(
        "parse_ypath",
        {"path": path, "output_format": YsonFormat(require_yson_bindings=False).to_yson_type()},
        client=client)
    )
    if get_api_version(client) == "v4":
        result = result["path"]

    result.attributes = update(attributes, result.attributes)

    return result


def _load_and_parse_tests(path, fields_count):
    from library.python import resource

    testdata = resource.find(path)
    lines = str(testdata).split("\n")
    line_idx = -1

    tests = []

    while True:
        line_idx += 1
        if line_idx >= len(lines):
            break
        line = lines[line_idx]
        if line.startswith("##"):
            continue
        if line == "":
            continue
        if line.startswith("==="):
            fields = []
            for field_id in range(fields_count):
                field_lines = []
                line_idx += 1
                line = lines[line_idx]
                separator = "---" if (field_id + 1 < fields_count) else "==="
                while not line.startswith(separator):
                    field_lines.append(line)
                    line_idx += 1
                    line = lines[line_idx]
                fields.append('\n'.join(field_lines))

            tests.append(fields)

    return tests


def _load_good_paths():
    path = "/good-rich-ypath.txt"
    return _load_and_parse_tests(path, 2)


def _load_bad_paths():
    path = "/bad-rich-ypath.txt"
    return _load_and_parse_tests(path, 1)


@pytest.mark.usefixtures("yt_env")
class TestParseYpath(object):
    @authors("ostyakov")
    def test_parse_ypath(self):
        for path in TEST_PATHS:
            obj2 = make_parse_ypath_request(path)
            obj1 = parse_ypath(path)
            assert obj1 == obj2

        for path in FAILED_TEST_PATHS:
            with pytest.raises(YtResponseError):
                make_parse_ypath_request(path)
            with pytest.raises((YtError, TypeError, YsonError, YPathError)):
                parse_ypath(path)

    @authors("ostyakov")
    @flaky(max_runs=5)
    def test_speed(self):
        if yatest_common.context.sanitize == "address":
            pytest.skip("speed tests disabled under asan")

        start_time = time.time()
        for _ in xrange(50):
            for path in TEST_PATHS:
                parse_ypath(path)
        print("Python YPath parser: {0}".format(time.time() - start_time), file=sys.stderr)

        start_time = time.time()
        for _ in xrange(50):
            for path in TEST_PATHS:
                make_parse_ypath_request(path)
        print("C++ YPath parser (local): {0}".format(time.time() - start_time), file=sys.stderr)

    @authors("ignat")
    def test_ypath_object(self):
        path = YPath("//my/path")
        assert str(path) == "//my/path"

        path = YPath("<my_attr=10>//my/path")
        assert path.attributes["my_attr"] == 10
        assert str(path) == "//my/path"

        path = YPath(to_yson_type("//my/path", attributes={"my_attr": 10}))
        assert path.attributes["my_attr"] == 10
        assert str(path) == "//my/path"

        with set_config_option("prefix", YPath("//home/levysotsky/")):
            path = YPath("me")
            assert str(path) == "//home/levysotsky/me"

    @authors("nadya73")
    def test_parse_yson_ypath(self):
        for ypath, expected_yson in _load_good_paths():
            assert YPath(ypath).to_yson_string(sort_keys=True) == expected_yson

        for ypath, in _load_bad_paths():
            with pytest.raises((YtError, TypeError, YsonError, YPathError)):
                YPath(ypath)
