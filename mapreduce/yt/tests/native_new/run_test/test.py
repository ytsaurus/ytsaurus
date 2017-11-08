import subprocess
import sys
import pytest

import yatest.common
from mapreduce.yt.python.yt_stuff import yt_stuff

BINARY_PATH = yatest.common.binary_path("mapreduce/yt/tests/native_new/test-native-interface")
TESTS_LIST = sorted(subprocess.check_output([BINARY_PATH, "--list-verbose"]).split())

# check that format of --list-verbose is not changed
assert 'BatchRequestSuite::TestGet' in TESTS_LIST

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
