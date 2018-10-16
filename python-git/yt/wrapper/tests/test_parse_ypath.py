# coding=utf-8
from __future__ import print_function

from yt.common import update
from yt.wrapper.driver import make_request
from yt.yson import loads, YsonString, YsonUnicode, YsonError
from yt.wrapper import YtHttpResponseError, YtError, YtResponseError, YsonFormat
from yt.ypath import YPathError, parse_ypath

from yt.packages.six.moves import xrange

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
              "test",
              "<>><test",
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
              '{"abc,[]askjjh,,,,asdj  ade"}',
              '1.1.1']

FAILED_TEST_PATHS = ["<ds>><test",
                     "{}{}",
                     "[]{}",
                     "[?]",
                     "{::}",
                     "{a:1}",
                     "[+]",
                     "//home/{/\;}",
                     "//home/{!@#.,/$%^&*()|_+}",
                     "//home/[|/~`\;]",
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
        client=client,
        decode_content=False))

    result.attributes = update(attributes, result.attributes)

    return result

@pytest.mark.usefixtures("yt_env")
class TestParseYpath(object):
    def test_parse_ypath(self):
        for path in TEST_PATHS:
            obj2 = make_parse_ypath_request(path)
            obj1 = parse_ypath(path)
            assert obj1 == obj2

        for path in FAILED_TEST_PATHS:
            with pytest.raises((YtHttpResponseError, YtResponseError)):
                make_parse_ypath_request(path)
            with pytest.raises((YtError, TypeError, YsonError, YPathError)):
                parse_ypath(path)

    @flaky(max_runs=5)
    def test_speed(self):
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
