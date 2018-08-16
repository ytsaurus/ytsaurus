from .helpers import ENABLE_JOB_CONTROL, TEST_DIR, remove_asan_warning

from yt.wrapper.job_shell import JobShell
from yt.wrapper.driver import get_command_list

import yt.wrapper as yt

from flaky import flaky

import os
import pytest
import re

@pytest.mark.usefixtures("yt_env")
class TestJobCommands(object):
    def _poll_until_prompt(self, shell):
        output = b""
        while len(output) < 4 or output[-4:] != b":~$ ":
            rsp = shell.make_request("poll")
            if not rsp or b"output" not in rsp:
                raise yt.YtError("Poll failed: " + output)
            output += rsp[b"output"]
        return output

    def _poll_until_shell_exited(self, shell):
        output = b""
        try:
            while True:
                rsp = shell.make_request("poll")
                if not rsp or b"output" not in rsp:
                    raise yt.YtError("Poll failed: " + output)
                output += rsp[b"output"]
        except yt.YtResponseError as e:
            if e.is_shell_exited():
                return output
            raise

    # Remove after YT-8596
    @flaky(max_runs=5)
    def test_job_shell(self, job_events):
        if yt.config["backend"] == "native":
            pytest.skip()

        commands = get_command_list()
        if "poll_job_shell" not in commands:
            pytest.skip()

        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, [{"x": 1}, {"x": 2}])

        mapper = job_events.with_breakpoint("cat ; BREAKPOINT")

        op = yt.run_map(mapper, table, other_table, format=yt.DsvFormat(), sync=False)

        job_id = job_events.wait_breakpoint()[0]

        shell = JobShell(job_id, interactive=False, timeout=0)
        shell.make_request("spawn", term="screen-256color", height=50, width=132)
        self._poll_until_prompt(shell)

        command = b"echo $TERM; tput lines; tput cols; id -u; id -g\r"
        shell.make_request("update", keys=command, input_offset=0)
        output = self._poll_until_prompt(shell)

        expected = command + b"\nscreen-256color\r\n50\r\n132\r\n"
        assert output.startswith(expected) == True

        ids = re.match(b"(\\d+)\r\n(\\d+)\r\n", output[len(expected):])
        if ENABLE_JOB_CONTROL:
            assert int(ids.group(1)) != os.getuid()

        shell.make_request("terminate")
        with pytest.raises(yt.YtError):
            output = self._poll_until_prompt(shell)

        job_events.release_breakpoint()
        op.wait()

    # Remove after YT-8596
    @flaky(max_runs=5)
    def test_job_shell_command(self, yt_env, job_events):
        if yt.config["backend"] == "native":
            pytest.skip()

        commands = get_command_list()
        if "poll_job_shell" not in commands:
            pytest.skip()

        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, [{"x": 1}, {"x": 2}])

        mapper = job_events.with_breakpoint("cat ; BREAKPOINT")
        op = yt.run_map(mapper, table, other_table, format=yt.DsvFormat(), sync=False)

        job_id = job_events.wait_breakpoint()[0]

        shell = JobShell(job_id, interactive=False, timeout=0)
        shell.make_request("spawn", command="echo $TERM; tput lines; tput cols; env | grep -c YT_OPERATION_ID")
        output = self._poll_until_shell_exited(shell)

        expected = b"xterm\r\n24\r\n80\r\n1\r\n"
        assert output == expected

        shell.make_request("terminate")
        with pytest.raises(yt.YtError):
            output = self._poll_until_prompt(shell)

        job_events.release_breakpoint()
        op.wait()

    def test_get_job_stderr(self, job_events):
        input_table = TEST_DIR + "/input_table"
        output_table = TEST_DIR + "/output_table"
        yt.write_table(input_table, [{"x": 1}])

        mapper = job_events.with_breakpoint("echo STDERR OUTPUT >&2 ; BREAKPOINT ; cat")

        op = yt.run_map(mapper, input_table, output_table, format=yt.DsvFormat(), sync=False)
        job_id = job_events.wait_breakpoint()[0]

        assert b"STDERR OUTPUT\n" == remove_asan_warning(yt.get_job_stderr(op.id, job_id).read())

        job_events.release_breakpoint()
        op.wait()

    def test_abort_job(self, job_events):
        input_table = TEST_DIR + "/input_table"
        output_table = TEST_DIR + "/output_table"
        yt.write_table(input_table, [{"x": 1}])

        mapper = job_events.with_breakpoint("cat ; BREAKPOINT")

        op = yt.run_map(mapper, input_table, output_table, format=yt.DsvFormat(), sync=False)

        job_id = job_events.wait_breakpoint()[0]

        yt.abort_job(job_id, 0)

        job_events.release_breakpoint()
        op.wait()

        attrs = yt.get_operation_attributes(op.id)
        assert attrs["progress"]["jobs"]["aborted"]["total"] == 1
