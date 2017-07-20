from .helpers import ENABLE_JOB_CONTROL, TEST_DIR, TESTS_SANDBOX

from yt.wrapper.job_shell import JobShell
from yt.wrapper.driver import get_command_list

import yt.wrapper as yt

import os
import sys
import stat
import pytest
import re
import tempfile
import time

@pytest.mark.usefixtures("yt_env")
class TestJobCommands(object):
    def _ensure_jobs_running(self, op, poll_frequency=0.2):
        jobs_path = "//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op.id)
        progress_path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/jobs".format(op.id)

        # Wait till all jobs are scheduled.
        running_count = 0
        pending_count = 0
        while running_count == 0 or pending_count > 0:
            time.sleep(poll_frequency)
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
            time.sleep(poll_frequency)

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

    def test_job_shell(self, yt_env):
        if yt.config["backend"] == "native":
            pytest.skip()

        commands = get_command_list()
        if "poll_job_shell" not in commands:
            pytest.skip()

        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, [{"x": 1}, {"x": 2}])

        self._tmpdir = self._create_tmpdir("job_shell")
        mapper = (
            "(touch {0}/started_$YT_JOB_ID 2>/dev/null\n"
            "cat\n"
            "while [ -f {0}/started_$YT_JOB_ID ]; do sleep 0.1; done\n)"
            .format(self._tmpdir))

        op = yt.run_map(mapper, table, other_table, format=yt.DsvFormat(), sync=False)

        self._ensure_jobs_running(op)
        job_id = self.jobs[0]

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

        self._resume_jobs()
        op.wait()

    def test_job_shell_command(self, yt_env):
        if yt.config["backend"] == "native" or yt_env.version <= "19.0":
            pytest.skip()

        commands = get_command_list()
        if "poll_job_shell" not in commands:
            pytest.skip()

        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, [{"x": 1}, {"x": 2}])

        self._tmpdir = self._create_tmpdir("job_shell")
        mapper = (
            "(touch {0}/started_$YT_JOB_ID 2>/dev/null\n"
            "cat\n"
            "while [ -f {0}/started_$YT_JOB_ID ]; do sleep 0.1; done\n)"
            .format(self._tmpdir))

        op = yt.run_map(mapper, table, other_table, format=yt.DsvFormat(), sync=False)

        self._ensure_jobs_running(op)
        job_id = self.jobs[0]

        shell = JobShell(job_id, interactive=False, timeout=0)
        shell.make_request("spawn", command="echo $TERM; tput lines; tput cols; env | grep -c YT_OPERATION_ID")
        output = self._poll_until_shell_exited(shell)

        expected = b"xterm\r\n24\r\n80\r\n1\r\n"
        assert output == expected

        shell.make_request("terminate")
        with pytest.raises(yt.YtError):
            output = self._poll_until_prompt(shell)

        self._resume_jobs()
        op.wait()

    def test_get_job_stderr(self, yt_env):
        input_table = TEST_DIR + "/input_table"
        output_table = TEST_DIR + "/output_table"
        yt.write_table(input_table, [{"x": 1}])

        self._tmpdir = self._create_tmpdir("get_job_stderr")
        mapper = (
            "(\n"
            "echo STDERR OUTPUT >&2\n"
            "touch {0}/started_$YT_JOB_ID 2>/dev/null\n"
            "cat\n"
            "while [ -f {0}/started_$YT_JOB_ID ]; do sleep 0.1; done\n)"
            .format(self._tmpdir))

        op = yt.run_map(mapper, input_table, output_table, format=yt.DsvFormat(), sync=False)

        self._ensure_jobs_running(op)
        job_id = self.jobs[0]

        assert b"STDERR OUTPUT\n" == yt.get_job_stderr(op.id, job_id).read()

        self._resume_jobs()
        op.wait()

    def test_abort_job(self):
        input_table = TEST_DIR + "/input_table"
        output_table = TEST_DIR + "/output_table"
        yt.write_table(input_table, [{"x": 1}])

        self._tmpdir = self._create_tmpdir("abort_job")
        mapper = (
            "(touch {0}/started_$YT_JOB_ID 2>/dev/null\n"
            "cat\n"
            "while [ -f {0}/started_$YT_JOB_ID ]; do sleep 0.1; done\n)"
                .format(self._tmpdir))

        op = yt.run_map(mapper, input_table, output_table, format=yt.DsvFormat(), sync=False)

        self._ensure_jobs_running(op)
        job_id = self.jobs[0]

        yt.abort_job(job_id, 0)

        self._resume_jobs()
        op.wait()

        attrs = yt.get_operation_attributes(op.id)
        assert attrs["brief_progress"]["jobs"]["aborted"]["total"] == 1
