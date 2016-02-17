import yt.wrapper as yt
from yt.wrapper.job_shell import JobShell
from yt.wrapper.http import get_api_commands

from helpers import ENABLE_JOB_CONTROL, TEST_DIR

import os
import pytest
import re
import time

@pytest.mark.usefixtures("yt_env")
class TestJobCommands(object):
    def _ensure_jobs_running(self, op):
        jobs_path = "//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op.id)
        progress_path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/jobs".format(op.id)

        # Wait till all jobs are scheduled.
        running_count = 0
        pending_count = 0
        while running_count == 0 or pending_count > 0:
            time.sleep(0.2)
            try:
                progress = yt.get(progress_path)
                running_count = progress["running"]
                pending_count = progress["pending"]
            except yt.YtResponseError as err:
                # running_jobs directory is not created yet.
                if err.is_resolve_error():
                    continue
                raise
        return yt.list(jobs_path)

    def _poll_until_prompt(self, shell):
        output = ""
        while len(output) < 4 or output[-4:] != ":~$ ":
            rsp = shell.make_request("poll")
            if not rsp or "output" not in rsp: #rsp.error:
                raise yt.YtError("Poll failed.")
            output += rsp["output"]
        return output

    def test_job_shell(self, yt_env):
        if yt.config["api_version"] == "v2" or yt.config["backend"] == "native":
            pytest.skip()
        commands = get_api_commands()
        if "poll_job_shell" not in commands:
            pytest.skip()

        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, [{"x": 1}, {"x": 2}])

        op = yt.run_map("sleep 10; cat", table, other_table, format=yt.DsvFormat(), sync=False)

        jobs = self._ensure_jobs_running(op)
        job_id = jobs[0]

        shell = JobShell(job_id, interactive=False)
        shell.make_request("spawn", term="screen-256color", height=50, width=132)
        self._poll_until_prompt(shell)

        command = "echo $TERM; tput lines; tput cols; id -u; id -g\r"
        shell.make_request("update", keys=command, input_offset=0)
        output = self._poll_until_prompt(shell)

        expected = "{0}\nscreen-256color\r\n50\r\n132\r\n".format(command)
        assert output.startswith(expected) == True

        ids = re.match("(\\d+)\r\n(\\d+)\r\n", output[len(expected):])
        assert ids and int(ids.group(1)) == int(ids.group(2))
        if ENABLE_JOB_CONTROL:
            assert int(ids.group(1)) != os.getuid()
        else:
            assert int(ids.group(1)) == os.getuid()

        rsp = shell.make_request("terminate")
        with pytest.raises(yt.YtError):
            output = self._poll_until_prompt(shell)

        op.abort()
        assert op.get_state() == "aborted"
