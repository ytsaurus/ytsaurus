import yt.wrapper as yt
from yt.wrapper.job_shell import JobShell
from yt.wrapper.http import get_api_commands

from helpers import ENABLE_JOB_CONTROL, TEST_DIR, TESTS_SANDBOX

import os
import stat
import pytest
import re
import tempfile
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
            time.sleep(self._poll_frequency)
            try:
                progress = yt.get(progress_path)
                running_count = progress["running"]
                pending_count = progress["pending"]
            except yt.YtResponseError as err:
                # running_jobs directory is not created yet.
                if err.is_resolve_error():
                    continue
                raise

        self.jobs = yt.list(jobs_path)

        # Wait till all jobs are actually running.
        while not all([os.path.exists(os.path.join(self._tmpdir, "started_" + job)) for job in self.jobs]):
            time.sleep(self._poll_frequency)

    def _resume_jobs(self):
        if self.jobs is None:
            raise RuntimeError('"_ensure_jobs_running" must be called before resuming jobs')

        for job in self.jobs:
            os.unlink(os.path.join(self._tmpdir, "started_" + job))

        try:
            os.rmdir(self._tmpdir)
        except OSError:
            sys.excepthook(*sys.exc_info())

    def _create_tmpdir(self, prefix):
        basedir = os.path.join(TESTS_SANDBOX, "tmp")
        try:
            if not os.path.exists(basedir):
                os.mkdir(basedir)
            tmpdir = tempfile.mkdtemp(
                prefix="{0}_{1}_".format(prefix, os.getpid()),
                dir=basedir)
            os.chmod(tmpdir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        except OSError:
            sys.excepthook(*sys.exc_info())
        return tmpdir

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

        self._tmpdir = self._create_tmpdir("job_shell")
        self._poll_frequency = 0.2
        mapper = (
            "(touch {0}/started_$YT_JOB_ID 2>/dev/null\n"
            "cat\n"
            "while [ -f {0}/started_$YT_JOB_ID ]; do sleep 0.1; done\n)"
            .format(self._tmpdir))

        op = yt.run_map(mapper, table, other_table, format=yt.DsvFormat(), sync=False)

        self._ensure_jobs_running(op)
        job_id = self.jobs[0]

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

        self._resume_jobs()
        op.wait()
