import subprocess
import sys
import pytest

import yatest.common
from mapreduce.yt.python.yt_stuff import yt_stuff, YtConfig

BINARY_PATH = yatest.common.binary_path("mapreduce/yt/tests/native_new/test-native-interface")
TESTS_LIST = sorted(subprocess.check_output([BINARY_PATH, "--list-verbose"]).split())

# check that format of --list-verbose is not changed
assert 'BatchRequestSuite::TestGet' in TESTS_LIST

@pytest.fixture
def yt_config(request):
    return YtConfig(enable_debug_log=True, yt_version="19_1")

@pytest.mark.parametrize("test_name", TESTS_LIST)
def test(yt_config, yt_stuff, test_name):
    yatest.common.execute(
        [BINARY_PATH,  test_name],
        env={"YT_PROXY": yt_stuff.get_server()}, stderr=sys.stderr)
