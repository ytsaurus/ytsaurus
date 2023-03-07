from .helpers import TEST_DIR, get_test_file_path, get_tests_sandbox, yatest_common

from yt.common import makedirp, which
import yt.wrapper as yt

from yt.packages.six.moves import xrange

import pytest

from flaky import flaky

import os
import random
import string
import subprocess
import time


@pytest.mark.skipif("yatest_common is not None")
@pytest.mark.usefixtures("yt_env_with_porto")
class TestDownloadCoreDump(object):
    # More details in YT-12581.
    @flaky(max_runs=3)
    def test_download_core_dump(self):
        assert which("g++")

        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        core_dump_table = TEST_DIR + "/core_dump_table"

        for path in [table, other_table, core_dump_table]:
            yt.create("table", path, force=True)

        yt.write_table(table, [{"a": i ** 2, "b": 2 ** i} for i in xrange(7)], format="yson")

        test_core_dumps_dir = os.path.join(get_tests_sandbox(), "test_core_dumps")
        makedirp(test_core_dumps_dir)
        core_output_dir = os.path.join(test_core_dumps_dir, "core" + "".join(random.sample(string.ascii_lowercase, 10)))

        makedirp(core_output_dir)

        cpp_file = get_test_file_path("bin_core_crash.cpp")
        cpp_bin_core_crash = os.path.join(test_core_dumps_dir, "cpp_bin_core_crash")
        subprocess.check_call(["g++", cpp_file, "-g", "-static-libgcc", "-o", cpp_bin_core_crash])

        op = yt.run_map("./cpp_bin_core_crash", table, other_table, local_files=cpp_bin_core_crash, format="yson",
                        sync=False, spec={"core_table_path": core_dump_table})

        while not op.get_state().is_finished():
            time.sleep(1.)

        assert op.get_state().name == "failed"

        yt.download_core_dump(output_directory=core_output_dir, operation_id=op.id)

        files = os.listdir(core_output_dir)
        assert len(files) == 1
        core_output_file = os.path.join(core_output_dir, files[0])

        gdb_binary = "gdb" if yatest_common is None else yatest_common.gdb_path()
        gdb_output = os.path.join(test_core_dumps_dir, "gdb_output.log")
        gdb_command = [gdb_binary,
                       "-ex", "set logging file " + gdb_output,
                       "-ex", "set logging overwrite on",
                       "-ex", "set logging on",
                       "-ex", "print t",
                       "-ex", "quit",
                       cpp_bin_core_crash,
                       core_output_file]
        subprocess.check_call(gdb_command)

        assert open(gdb_output).read() == "$1 = 1\n"

        os.remove(core_output_file)

    def test_error_core_dump(self):
        assert which("g++")

        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        core_dump_table = TEST_DIR + "/core_dump_table"

        test_core_dumps_dir = os.path.join(get_tests_sandbox(), "test_core_dumps")
        makedirp(test_core_dumps_dir)
        core_output_dir = os.path.join(test_core_dumps_dir, "core")
        makedirp(core_output_dir)

        for path in [table, other_table, core_dump_table]:
            yt.create("table", path, force=True)

        yt.write_table(table, input_stream=[{"a": i ** 2, "b": 2 ** i} for i in xrange(7)], format="yson")

        op = yt.run_map("cat", table, other_table, format="yson", spec={"core_table_path": core_dump_table})

        with pytest.raises(yt.YtError):
            yt.download_core_dump(output_directory=core_output_dir, operation_id=op.id)
