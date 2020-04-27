from .helpers import ENABLE_JOB_CONTROL, TEST_DIR, create_job_events

from yt.wrapper.job_shell import JobShell
import yt.yson
import yt.ypath

import yt.wrapper as yt

from flaky import flaky

import os
import pytest
import re

@pytest.mark.usefixtures("yt_env_with_porto")
@pytest.mark.usefixtures("yt_env_with_rpc")
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

    def test_job_shell(self, job_events):
        if yt.config["backend"] in ("native", "rpc"):
            pytest.skip()

        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, [{"x": 1}, {"x": 2}])

        mapper = job_events.with_breakpoint("cat ; BREAKPOINT")

        op = yt.run_map(mapper, table, other_table, format=yt.YsonFormat(), sync=False)

        job_id = job_events.wait_breakpoint()[0]

        shell = JobShell(job_id, interactive=False, timeout=0)
        shell.make_request("spawn", term="screen-256color", height=50, width=132)

        output_before_prompt = self._poll_until_prompt(shell)
        assert b"YT_OPERATION_ID" in output_before_prompt

        command = b"echo $TERM; tput lines; tput cols; id -u; id -g\r"
        shell.make_request("update", keys=command, input_offset=0)
        output = self._poll_until_prompt(shell)

        expected = command + b"\nscreen-256color\r\n50\r\n132\r\n"
        assert output.startswith(expected) == True

        ids = re.match(b"(\\d+)\r\n(\\d+)\r\n", output[len(expected):])

        shell.make_request("terminate")
        with pytest.raises(yt.YtError):
            output = self._poll_until_prompt(shell)

        job_events.release_breakpoint()
        op.wait()

    def test_job_shell_command(self, yt_env_with_rpc, job_events):
        if yt.config["backend"] in ("native", "rpc"):
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

    def test_secure_vault_variables_in_job_shell(self):
        if yt.config["backend"] in ("native", "rpc"):
            pytest.skip()

        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, [{"x": 1}, {"x": 2}])

        job_events = create_job_events()
        mapper = job_events.with_breakpoint("cat ; BREAKPOINT")

        op = yt.run_map(mapper, table, other_table,
                        format=yt.YsonFormat(), spec={"secure_vault": {"MY_VAR": "10"}},
                        sync=False)

        job_id = job_events.wait_breakpoint()[0]

        shell = JobShell(job_id, interactive=False, timeout=0)
        shell.make_request("spawn", term="screen-256color", height=50, width=132)

        output_before_prompt = self._poll_until_prompt(shell)
        assert b"YT_OPERATION_ID" in output_before_prompt
        assert b"YT_SECURE_VAULT_MY_VAR" in output_before_prompt

        shell.make_request("terminate")
        with pytest.raises(yt.YtError):
            self._poll_until_prompt(shell)

        job_events.release_breakpoint()
        op.wait()


        job_events = create_job_events()
        mapper = job_events.with_breakpoint("cat ; BREAKPOINT")
        op = yt.run_map(mapper, table, other_table,
                        format=yt.YsonFormat(),
                        spec={
                            "secure_vault": {"MY_VAR": "10"},
                            "enable_secure_vault_variables_in_job_shell": False,
                        },
                        sync=False)

        job_id = job_events.wait_breakpoint()[0]

        shell = JobShell(job_id, interactive=False, timeout=0)
        shell.make_request("spawn", term="screen-256color", height=50, width=132)

        output_before_prompt = self._poll_until_prompt(shell)
        assert b"YT_OPERATION_ID" in output_before_prompt
        assert b"YT_SECURE_VAULT_MY_VAR" not in output_before_prompt

        shell.make_request("terminate")
        with pytest.raises(yt.YtError):
            self._poll_until_prompt(shell)

        job_events.release_breakpoint()
        op.wait()

    def test_get_job_stderr(self, job_events):
        input_table = TEST_DIR + "/input_table"
        output_table = TEST_DIR + "/output_table"
        yt.write_table(input_table, [{"x": 1}])

        mapper = job_events.with_breakpoint("echo STDERR OUTPUT >&2 ; BREAKPOINT ; cat")

        op = yt.run_map(mapper, input_table, output_table, format=yt.DsvFormat(), sync=False)
        job_id = job_events.wait_breakpoint()[0]

        assert b"STDERR OUTPUT\n" == yt.get_job_stderr(op.id, job_id).read()

        job_events.release_breakpoint()
        op.wait()

    def test_get_job_input(self, job_events):
        input_table = TEST_DIR + "/input_table"
        output_table = TEST_DIR + "/output_table"
        yt.write_table(input_table, [{"x": 1}])

        mapper = job_events.with_breakpoint("cat; BREAKPOINT")

        op = yt.run_map(mapper, input_table, output_table, format="yson", sync=False)
        job_id = job_events.wait_breakpoint()[0]

        actual_input = yt.yson.loads(yt.get_job_input(job_id).read(), yson_type="list_fragment")
        assert list(actual_input) == [{"x": 1}]

        job_events.release_breakpoint()
        op.wait()

    def test_get_job_input_paths(self, job_events):
        input_table = TEST_DIR + "/input_table"
        output_table = TEST_DIR + "/output_table"
        yt.write_table(input_table, [{"x": 1}])

        mapper = job_events.with_breakpoint("cat; BREAKPOINT")

        op = yt.run_map(mapper, input_table, output_table, format="yson", sync=False)
        job_id = job_events.wait_breakpoint()[0]

        expected_path = yt.ypath.TablePath(input_table, start_index=0, end_index=1)
        assert yt.get_job_input_paths(job_id) == [expected_path]

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
