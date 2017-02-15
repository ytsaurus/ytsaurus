import subprocess
import pytest

import yatest.common
from mapreduce.yt.python.yt_stuff import yt_stuff

BINARY_PATH = yatest.common.binary_path("mapreduce/yt/tests/native/native")
TESTS_LIST = sorted(subprocess.check_output([BINARY_PATH, "--list"]).split())

# Note: these tests fail for now since local yt in arcadia
# doesn't have required functionality.
TESTS_LIST.remove("TCypressCopy_PreserveExpirationTime")
TESTS_LIST.remove("TCypressMove_PreserveExpirationTime")

@pytest.mark.parametrize("test_name", TESTS_LIST)

def test(yt_stuff, test_name):
    res = yatest.common.execute(
        [BINARY_PATH, "--run", yt_stuff.get_server(), test_name],
        env={"YT_PREFIX": "//"},
    )

    if res.std_out:
        return res.std_out

