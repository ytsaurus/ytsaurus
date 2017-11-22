from .helpers import get_tests_sandbox, TEST_DIR, get_tests_location, wait_record_in_job_archive, get_python, yatest_common

from yt.common import makedirp, to_native_str
import yt.yson as yson
import yt.subprocess_wrapper as subprocess

from yt.packages.six.moves import map as imap

import yt.wrapper as yt

import os
import stat
import sys
import tempfile
import shutil
import pytest
import time

ORCHID_JOB_PATH_PATTERN = "//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs/{1}"
NODE_ORCHID_JOB_PATH_PATTERN = "//sys/nodes/{0}/orchid/job_controller/active_jobs/scheduler/{1}"


class TestJobRunner(object):
    @classmethod
    def setup_class(cls):
        makedirp(get_tests_sandbox())

    def _start_job_runner(self, config):
        with tempfile.NamedTemporaryFile(dir=get_tests_sandbox(), prefix="job_runner", delete=False) as fout:
            yson.dump(config, fout, yson_format="pretty")
        return subprocess.Popen(
            [get_python(), "-m", "yt.wrapper.job_runner", "--config-path", fout.name],
            stderr=subprocess.PIPE)

    @pytest.mark.parametrize("use_yamr_descriptors", [True, False])
    def test_job_runner(self, use_yamr_descriptors):
        tmp_dir = tempfile.mkdtemp(dir=get_tests_sandbox())
        command_path = os.path.join(tmp_dir, "command")
        input_path = os.path.join(tmp_dir, "input")
        output_path = os.path.join(tmp_dir, "output")
        sandbox_path = os.path.join(tmp_dir, "sandbox")

        makedirp(sandbox_path)
        makedirp(output_path)

        script="""\
import os
import sys

assert os.environ["YT_JOB_ID"] == "a-b-c-d"
assert os.environ["YT_OPERATION_ID"] == "e-f-g-h"
assert os.environ["YT_JOB_INDEX"] == "0"

for line in sys.stdin:
    fd = int(line)
    os.fdopen(fd, "w").write("message for " + str(fd))
"""
        with open(os.path.join(sandbox_path, "script.py"), "w") as fout:
            fout.write(script)
        with open(command_path, "w") as fout:
            fout.write(get_python() + " script.py")
        with open(input_path, "w") as fout:
            if use_yamr_descriptors:
                descriptors = [1, 3, 4, 5, 6]
            else:
                descriptors = [1, 4, 7, 10]
            fout.write("\n".join(imap(str, descriptors)))

        runner = self._start_job_runner({
            "command_path": command_path,
            "sandbox_path": sandbox_path,
            "input_path": input_path,
            "output_path": output_path,
            # In YAMR mode descriptors 1 and 3 correspond to the same table
            "output_table_count": len(descriptors) - int(use_yamr_descriptors),
            "use_yamr_descriptors": use_yamr_descriptors,
            "job_id": "a-b-c-d",
            "operation_id": "e-f-g-h"
        })
        runner.wait()

        assert runner.returncode == 0, runner.stderr.read()
        for fd in descriptors:
            with open(os.path.join(output_path, str(fd))) as fin:
                assert fin.read().strip() == "message for " + str(fd)
        if not use_yamr_descriptors:
            assert os.path.exists(os.path.join(output_path, "5"))  # Statistics descriptor

class TestJobTool(object):
    if yatest_common is None:
        JOB_TOOL_BINARY = os.path.join(os.path.dirname(get_tests_location()), "bin", "yt-job-tool")
    else:
        JOB_TOOL_BINARY = yatest_common.binary_path("yt/python/yt/wrapper/bin/yt-job-tool_make/yt-job-tool")

    TEXT_YSON = "<format=pretty>yson"

    def get_failing_command(self):
        return "cat > {tmpdir}/$YT_JOB_ID.input && echo ERROR_INTENDED_BY_TEST >&2 && exit 1".format(tmpdir=self._tmpdir)

    def get_ok_command(self):
        return "cat > {tmpdir}/$YT_JOB_ID.input && echo OK_COMMAND >&2".format(tmpdir=self._tmpdir)

    def setup(self):
        self._tmpdir = tempfile.mkdtemp(dir=get_tests_sandbox())
        os.chmod(self._tmpdir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO) # allow user job to write to this directory

    def _prepare_job_environment(self, yt_env_job_archive, operation_id, job_id, full=False):
        if yatest_common is None:
            args = [sys.executable]
        else:
            args = []
        args += [
            self.JOB_TOOL_BINARY,
            "prepare-job-environment",
            operation_id,
            job_id,
            "--job-path",
            os.path.join(yt_env_job_archive.env.path, "test_job_tool", "job_" + job_id),
            "--proxy",
            yt_env_job_archive.config["proxy"]["url"]
        ]
        if full:
            args += ["--full"]
            wait_record_in_job_archive(operation_id, job_id)
        return subprocess.check_output(args).strip()

    def _check(self, operation_id, yt_env_job_archive, check_running=False, full=False, expect_ok_return_code=False):
        if not check_running:
            job_id = yt.list("//sys/operations/{0}/jobs".format(operation_id))[0]
        else:
            total_job_wait_timeout = 10
            start_time = time.time()

            running_jobs_pattern = "//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs"
            running_jobs_path = running_jobs_pattern.format(operation_id)

            while True:  # Waiting for job to start
                jobs = yt.list(running_jobs_path)
                if jobs:
                    break

                if time.time() - start_time > total_job_wait_timeout:
                    assert False, "Timeout occured while waiting any job of operation {0} to run".format(operation_id)

            job_id = jobs[0]
            node_address = yt.get(ORCHID_JOB_PATH_PATTERN.format(operation_id, job_id))["address"]
            while True:
                try:
                    job_info = yt.get(NODE_ORCHID_JOB_PATH_PATTERN.format(node_address, job_id))
                except yt.YtResponseError as err:
                    if not err.is_resolve_error():
                        raise
                    continue

                if job_info.get("job_phase") == "running":
                    break

                if time.time() - start_time > total_job_wait_timeout:
                    assert False, "Timeout occured while waiting for job {0} to run".format(job_id)

        job_path = self._prepare_job_environment(yt_env_job_archive, operation_id, job_id, full)

        assert open(os.path.join(job_path, "sandbox", "_test_file")).read().strip() == "stringdata"
        with open(os.path.join(self._tmpdir, job_id + ".input")) as canonical_input:
            assert canonical_input.read() == open(os.path.join(job_path, "input")).read()

        run_config = os.path.join(job_path, "run_config")
        assert os.path.exists(run_config)
        with open(run_config, "rb") as fin:
            config = yson.load(fin)
        assert config["operation_id"] == operation_id
        assert config["job_id"] == job_id

        if not check_running:
            proc = subprocess.Popen([self.JOB_TOOL_BINARY, "run-job", job_path], stderr=subprocess.PIPE)
            proc.wait()

            if expect_ok_return_code:
                assert proc.returncode == 0
            else:
                assert proc.returncode != 0
                assert "ERROR_INTENDED_BY_TEST" in to_native_str(proc.stderr.read())
                assert "ERROR_INTENDED_BY_TEST" in open(os.path.join(job_path, "output", "2")).read()

        shutil.rmtree(job_path)

    def test_job_tool(self, yt_env_job_archive):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"key": "1", "value": "2"}])
        yt.run_sort(table, sort_by=["key"])

        file_ = TEST_DIR + "/_test_file"
        yt.write_file(file_, b"stringdata")

        op = yt.run_map(self.get_failing_command(), table, TEST_DIR + "/output", format=self.TEXT_YSON,
                        yt_files=[file_], sync=False)
        op.wait(check_result=False)
        self._check(op.id, yt_env_job_archive)

        op = yt.run_reduce(self.get_failing_command(), table, TEST_DIR + "/output", format=self.TEXT_YSON,
                           yt_files=[file_], sync=False, reduce_by=["key"])
        op.wait(check_result=False)
        self._check(op.id, yt_env_job_archive)

        op = yt.run_map_reduce(self.get_failing_command(), "cat", table, TEST_DIR + "/output", format=self.TEXT_YSON,
                               map_yt_files=[file_], reduce_by=["key"], sync=False)
        op.wait(check_result=False)
        self._check(op.id, yt_env_job_archive)

        op = yt.run_map_reduce("cat", self.get_failing_command(), table, TEST_DIR + "/output", format=self.TEXT_YSON,
                               reduce_yt_files=[file_], reduce_by=["key"], sync=False)
        op.wait(check_result=False)
        self._check(op.id, yt_env_job_archive)

        # Should fallback on using input context
        op = yt.run_map(self.get_ok_command() + "; sleep 1000", table, TEST_DIR + "/output", format=self.TEXT_YSON,
                        yt_files=[file_], sync=False)
        self._check(op.id, yt_env_job_archive, check_running=True)
        op.abort()

    def test_job_tool_full(self, yt_env_job_archive):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"key": "1", "value": "2"}])

        file_ = TEST_DIR + "/_test_file"
        yt.write_file(file_, b"stringdata")

        op = yt.run_map(self.get_ok_command(), table, TEST_DIR + "/output", format=self.TEXT_YSON, yt_files=[file_])
        self._check(op.id, yt_env_job_archive, full=True, expect_ok_return_code=True)

    def test_job_tool_full_join_reduce(self, yt_env_job_archive):
        primary_table = TEST_DIR + "/primary"
        yt.write_table(yt.TablePath(primary_table, sorted_by=["key", "subkey"]), [{"key": "1", "subkey": "2", "value": "2"}])
        foreign_table = TEST_DIR + "/foreign"
        yt.write_table(yt.TablePath(foreign_table, sorted_by=["key"]), [{"key": "1"}])

        file_ = TEST_DIR + "/_test_file"
        yt.write_file(file_, b"stringdata")

        op = yt.run_join_reduce(
            self.get_ok_command(),
            source_table=[yt.TablePath(foreign_table, attributes={"foreign": True}),
                          yt.TablePath(primary_table)],
            destination_table=TEST_DIR + "/output",
            join_by=["key"],
            format="yson",
            yt_files=[file_])
        self._check(op.id, yt_env_job_archive, full=True, expect_ok_return_code=True)

    def test_run_sh(self, yt_env_job_archive):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"key": "1", "value": "2"}])

        file_ = TEST_DIR + "/_test_file"
        yt.write_file(file_, b"stringdata")

        op = yt.run_map(self.get_ok_command(), table, TEST_DIR + "/output", format=self.TEXT_YSON, yt_files=[file_])
        job_id = yt.list("//sys/operations/{0}/jobs".format(op.id))[0]
        path = self._prepare_job_environment(yt_env_job_archive, op.id, job_id, full=True)
        p = subprocess.Popen([os.path.join(path, "run.sh")], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, p_stderr = p.communicate()
        assert p_stderr == u"OK_COMMAND\n".encode("ascii")

    def test_environment(self, yt_env_job_archive):
        command = self.get_ok_command() + """ ; if [ "$YT_JOB_TOOL_TEST_VARIABLE" != "present" ] ; then echo "BAD VARIABLE" >&2 ; exit 1 ; fi """

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"key": "1", "value": "2"}])

        file_ = TEST_DIR + "/_test_file"
        yt.write_file(file_, b"stringdata")

        op = yt.run_map(command, table, TEST_DIR + "/output", format=self.TEXT_YSON, yt_files=[file_],
                        spec={"mapper": {"environment": {"YT_JOB_TOOL_TEST_VARIABLE": "present"}}})
        job_id = yt.list("//sys/operations/{0}/jobs".format(op.id))[0]
        path = self._prepare_job_environment(yt_env_job_archive, op.id, job_id, full=True)
        p = subprocess.Popen([os.path.join(path, "run.sh")], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, p_stderr = p.communicate()
        assert p_stderr == u"OK_COMMAND\n".encode("ascii")
