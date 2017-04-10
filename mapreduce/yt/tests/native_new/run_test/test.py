import subprocess
import pytest

import yatest.common
from mapreduce.yt.python.yt_stuff import yt_stuff

BINARY_PATH = yatest.common.binary_path("mapreduce/yt/tests/native_new/test-native-interface")
TESTS_LIST = sorted(subprocess.check_output([BINARY_PATH, "--list-verbose"]).split())

@pytest.mark.parametrize("test_name", TESTS_LIST)
def test(yt_stuff, test_name):
    yatest.common.execute(
        [BINARY_PATH,  test_name],
        env={"YT_PROXY": yt_stuff.get_server()})
