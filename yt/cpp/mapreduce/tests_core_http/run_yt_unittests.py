"""
This script is used to setup local YT and lauch c++ unittest that will use local YT.

In order to use it you need to create directory that contains at least following files:
  - ya.make
  - yt_unittest_conf.py that provides test configuration


EXAMPLE
-------

$ cat ya.make:
    PY2TEST()
    PY_SRCS(
        yt_unittest_conf.py # path to test configuration
    )
    TEST_SRCS(
        yt/cpp/mapreduce/tests_core_http/run_yt_unittests.py # path to current script
    )
    PEERDIR(
        mapreduce/yt/python           # path to yt_stuff that provides local yt
    )
    DEPENDS(
        yt/packages/latest              # path to yt binaries (required by yt_stuff)
        yt/cpp/mapreduce/tests_core_http/native_new # path to unittest directory
    )
    END()
$ cat yt_unittest_conf.py
    BINARY_PATH = "yt/cpp/mapreduce/tests_core_http/native_new/test-native-interface" # path to unittest binary
"""

import subprocess
import sys
import pytest

import yatest.common
from mapreduce.yt.python.yt_stuff import yt_stuff

import yt_unittest_conf

BINARY_PATH = yatest.common.binary_path(yt_unittest_conf.BINARY_PATH)
if BINARY_PATH is None:
    raise RuntimeError, ("Cannot find source '{0}'\n"
                         "Check that binary path is spelled correctly and that it is included in DEPENDS section of ya.make\n"
                        ).format(yt_unittest_conf.BINARY_PATH)

TESTS_LIST = sorted(subprocess.check_output([BINARY_PATH, "--list-verbose"]).split())

assert len(TESTS_LIST) > 0

@pytest.mark.parametrize("test_name", TESTS_LIST)
def test(yt_stuff, test_name):
    # We save stderr of our test to file, so it's easy to find it on sandbox.
    stderr_file_name = yatest.common.output_path(test_name + '.stderr')

    try:
        with open(stderr_file_name, 'w') as stderr_file:
            yatest.common.execute(
                [BINARY_PATH,  test_name],
                env={"YT_PROXY": yt_stuff.get_server()}, stderr=stderr_file)
    except:
        with open(stderr_file_name) as inf:
            sys.stderr.write(inf.read())
        raise
