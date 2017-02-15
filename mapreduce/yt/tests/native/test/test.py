import subprocess
import pytest

import yatest.common
from mapreduce.yt.python.yt_stuff import yt_stuff

BINARY_PATH = yatest.common.binary_path("mapreduce/yt/tests/native/native")
TESTS_LIST = sorted(subprocess.check_output([BINARY_PATH, "--list"]).split())

@pytest.mark.parametrize("test_name", TESTS_LIST)

def test(yt_stuff, test_name):
    res = yatest.common.execute(
        [BINARY_PATH, "--run", yt_stuff.get_server(), test_name],
        env={"YT_PREFIX": "//"},
    )

    if res.std_out:
        return res.std_out

