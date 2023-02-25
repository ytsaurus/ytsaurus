import subprocess
import sys
import pytest

import yatest.common

BINARY_PATH = yatest.common.binary_path("yt/cpp/mapreduce/tests_core_http/rpc_proxy/test-rpc-proxy")  # noqa
TESTS_LIST = sorted(subprocess.check_output([BINARY_PATH, "--list-verbose"]).split())  # noqa


@pytest.mark.parametrize("test_name", TESTS_LIST)
def test(yt_stuff, test_name):
    stderr_file_name = yatest.common.output_path(test_name + '.stderr')

    try:
        with open(stderr_file_name, 'w') as stderr_file:
            yatest.common.execute(
                [BINARY_PATH,  test_name],
                env={"YT_PROXY": yt_stuff.get_server()}, stderr=stderr_file)
    except Exception:
        with open(stderr_file_name) as inf:
            sys.stderr.write(inf.read())
        raise
