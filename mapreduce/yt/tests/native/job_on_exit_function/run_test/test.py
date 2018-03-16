import subprocess
import sys

import pytest

import yatest.common
from mapreduce.yt.python.yt_stuff import yt_stuff

BINARY_PATH = yatest.common.binary_path("mapreduce/yt/tests/native/job_on_exit_function/job_on_exit_function")

def test(yt_stuff):
    # We save stderr of our test to file, so it's easy to find it on sandbox.
    test_name = "test"
    stderr_file_name = yatest.common.output_path(test_name + '.stderr')

    try:
        with open(stderr_file_name, 'w') as stderr_file:
            yatest.common.execute(
                [BINARY_PATH],
                env={"YT_PROXY": yt_stuff.get_server()},
                stderr=stderr_file)
    except:
        with open(stderr_file_name) as inf:
            sys.stderr.write(inf.read())
        raise
