import tempfile
import subprocess
import pytest

import yatest.common
from mapreduce.yt.python.yt_stuff import yt_stuff
from mr_runner import mapreduce

BINARY_PATH = yatest.common.binary_path("mapreduce/yt/tests/library/library")
TESTS_LIST = sorted(subprocess.check_output([BINARY_PATH, "--list"]).split())

@pytest.fixture(scope="module")
def tmpdir_module(request):
    return tempfile.mkdtemp(prefix="common_", dir=yatest.common.runtime.work_path())

@pytest.mark.parametrize("test_name", TESTS_LIST)

def test(tmpdir_module, mapreduce, yt_stuff, test_name):
    mr_res = yatest.common.execute(
        [BINARY_PATH, "--run", "localhost:%d" % mapreduce.ports["SERVER_PORT"], test_name],
        wait=False
    )

    yt_res = yatest.common.execute(
        [BINARY_PATH, "--run", yt_stuff.get_server(), test_name],
        wait=False,
        env={"WRITE_TO_LOG": "1", "MR_RUNTIME": "YT"}
    )

    mr_res.wait(check_exit_code=False)
    yt_res.wait(check_exit_code=False)

    assert mr_res.std_out == yt_res.std_out

    if mr_res.exit_code != 0 and yt_res.exit_code != 0:
        return "BOTH ERROR"
    elif mr_res.exit_code == 0 and yt_res.exit_code != 0:
        return "YT ERROR"
    elif mr_res.exit_code != 0 and yt_res.exit_code == 0:
        return "YAMR ERROR"

    if yt_res.std_out:
        return yt_res.std_out
