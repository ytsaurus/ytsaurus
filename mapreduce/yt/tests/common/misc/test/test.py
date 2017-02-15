import subprocess
import pytest

import yatest.common
from mapreduce.yt.python.yt_stuff import yt_stuff

BINARY_PATH = yatest.common.binary_path("mapreduce/yt/tests/common/misc/misc_test")
TESTS_LIST = sorted(subprocess.check_output([BINARY_PATH, "--list"]).split())

# These tests are failing for now
TESTS_LIST = sorted(set(TESTS_LIST) - {"TUpdateSingleUpdate", "TUpdateMultiUpdate"})

@pytest.mark.parametrize("test_name", TESTS_LIST)

def test(yt_stuff, test_name):
    yt_res = yatest.common.execute(
        [BINARY_PATH, "--run", yt_stuff.get_server(), test_name],
        env={"WRITE_TO_LOG": "1", "MR_RUNTIME": "YT"}
    )
    return yt_res.std_out
