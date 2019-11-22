from .helpers import (get_tests_sandbox, TEST_DIR, get_tests_location, wait_record_in_job_archive,
                      yatest_common, get_operation_path)

from yt.common import to_native_str
import yt.subprocess_wrapper as subprocess
from yt.wrapper.job_tool import GetContextMode
from yt.wrapper.ypath import YPath
import yt.environment.arcadia_interop as arcadia_interop

import yt.wrapper as yt

import os
import stat
import sys
import tempfile
import shutil
import time
import json

NODE_ORCHID_JOB_PATH_PATTERN = "//sys/cluster_nodes/{0}/orchid/job_controller/active_jobs/scheduler/{1}"

class TestJobTool(object):
    if yatest_common is None:
        JOB_TOOL_BINARY = os.path.join(os.path.dirname(get_tests_location()), "bin", "yt-job-tool")
    else:
        _, python_root, _ = arcadia_interop.get_root_paths()
        JOB_TOOL_BINARY = yatest_common.binary_path(python_root + "/yt/wrapper/bin/yt-job-tool_make/yt-job-tool")

    TEXT_YSON = "<format=pretty>yson"

    def get_failing_command(self):
        return "cat > {tmpdir}/$YT_JOB_ID.input && echo ERROR_INTENDED_BY_TEST >&2 && exit 1".format(tmpdir=self._tmpdir)

    def get_ok_command(self):
        return "cat > {tmpdir}/$YT_JOB_ID.input && echo OK_COMMAND >&2".format(tmpdir=self._tmpdir)

    def setup(self):
        self._tmpdir = tempfile.mkdtemp(dir=get_tests_sandbox())
        os.chmod(self._tmpdir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO) # allow user job to write to this directory

    def _prepare_job_environment(self, yt_env_job_archive, operation_id, job_id,
                                 get_context_mode=GetContextMode.context):
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
            yt_env_job_archive.config["proxy"]["url"],
        ]
        if get_context_mode == GetContextMode.full_input:
            args += ["--full-input"]
            wait_record_in_job_archive(operation_id, job_id)
        else:
            args += ["--context"]
        return subprocess.check_output(args).strip()

    def _check(self, operation_id, yt_env_job_archive, check_running=False,
               get_context_mode=GetContextMode.context, expect_ok_return_code=False):
        if not check_running:
            job_id = yt.list(get_operation_path(operation_id) + "/jobs")[0]
        else:
            total_job_wait_timeout = 10
            start_time = time.time()

            running_jobs_path = get_operation_path(operation_id) + "/controller_orchid/running_jobs"

            while True:  # Waiting for job to start
                jobs = yt.list(running_jobs_path)
                if jobs:
                    break

                if time.time() - start_time > total_job_wait_timeout:
                    assert False, "Timeout occured while waiting any job of operation {0} to run".format(operation_id)

            job_id = jobs[0]
            node_address = yt.get(get_operation_path(operation_id) + "/controller_orchid/running_jobs/" + job_id + "/address")
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

        job_path = self._prepare_job_environment(yt_env_job_archive, operation_id, job_id, get_context_mode)

        assert open(os.path.join(job_path, "sandbox", "_test_file")).read().strip() == "stringdata"
        with open(os.path.join(self._tmpdir, job_id + ".input")) as canonical_input:
            assert canonical_input.read() == open(os.path.join(job_path, "input")).read()

        run_config = os.path.join(job_path, "run_config")
        assert os.path.exists(run_config)
        with open(run_config, "r") as fin:
            config = json.load(fin)
        assert config["operation_id"] == operation_id
        assert config["job_id"] == job_id

        if not check_running:
            proc = subprocess.Popen([self.JOB_TOOL_BINARY, "run-job", job_path,
                                     "--env", '{PATH="/bin:/usr/bin:' + os.environ["PATH"] +'"}'],
                                    stderr=subprocess.PIPE)
            proc.wait()

            if expect_ok_return_code:
                assert proc.returncode == 0, proc.stderr.read()
            else:
                assert proc.returncode != 0
                assert "ERROR_INTENDED_BY_TEST" in to_native_str(proc.stderr.read())
                assert "ERROR_INTENDED_BY_TEST" in open(os.path.join(job_path, "output", "2")).read()

        shutil.rmtree(job_path)

    def test_job_tool(self, yt_env_job_archive, job_events):
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
        op = yt.run_map(self.get_ok_command() + ";" + job_events.breakpoint_cmd(), table, TEST_DIR + "/output", format=self.TEXT_YSON,
                        yt_files=[file_], sync=False)
        job_events.wait_breakpoint()
        self._check(op.id, yt_env_job_archive, check_running=True)
        op.abort()

    def test_job_tool_full(self, yt_env_job_archive):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"key": "1", "value": "2"}])

        file_ = TEST_DIR + "/_test_file"
        yt.write_file(file_, b"stringdata")

        op = yt.run_map(self.get_ok_command(), table, TEST_DIR + "/output", format=self.TEXT_YSON, yt_files=[file_])
        self._check(op.id, yt_env_job_archive, get_context_mode=GetContextMode.full_input,
                    expect_ok_return_code=True)

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
        self._check(op.id, yt_env_job_archive, get_context_mode=GetContextMode.full_input,
                    expect_ok_return_code=True)

    def test_run_sh(self, yt_env_job_archive):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"key": "1", "value": "2"}])

        file_ = TEST_DIR + "/_test_file"
        yt.write_file(file_, b"stringdata")

        op = yt.run_map(self.get_ok_command(), table, TEST_DIR + "/output", format=self.TEXT_YSON, yt_files=[file_])
        job_id = yt.list(get_operation_path(op.id) + "/jobs")[0]
        path = self._prepare_job_environment(yt_env_job_archive, op.id, job_id,
                                             get_context_mode=GetContextMode.full_input)
        p = subprocess.Popen([os.path.join(path, "run.sh")], env={"PATH": "/bin:/usr/bin:" + os.environ["PATH"]}, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
        job_id = yt.list(get_operation_path(op.id) + "/jobs")[0]
        path = self._prepare_job_environment(yt_env_job_archive, op.id, job_id,
                                             get_context_mode=GetContextMode.full_input)
        p = subprocess.Popen([os.path.join(path, "run.sh")], env={"PATH": "/bin:/usr/bin:" + os.environ["PATH"]}, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, p_stderr = p.communicate()
        assert p_stderr == u"OK_COMMAND\n".encode("ascii")

    def test_file_name_precedence(self, yt_env_job_archive):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"key": "1", "value": "2"}])

        file_content = b"FILE_CONTENT"

        def check_file_present(file_cypress_path, file_name_in_job):
            command = "cat < {} >&2; cat ".format(file_name_in_job)
            op = yt.run_map(command, table, TEST_DIR + "/output", format=self.TEXT_YSON, yt_files=[file_cypress_path])
            job_id = yt.list(get_operation_path(op.id) + "/jobs")[0]
            path = self._prepare_job_environment(yt_env_job_archive, op.id, job_id,
                                                 get_context_mode=GetContextMode.full_input)
            env = os.environ.copy()
            env["PATH"] = "/bin:/usr/bin:" + env.get("PATH")
            p = subprocess.Popen([os.path.join(path, "run.sh")], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            _, p_stderr = p.communicate()
            assert p_stderr == file_content

        file_ = TEST_DIR + "/file_name_in_key"
        yt.write_file(file_, file_content)

        check_file_present(file_, "file_name_in_key")

        yt.set(file_ + "/@file_name", "file_name_in_node_attribute")

        check_file_present(file_, "file_name_in_node_attribute")

        file_path_with_attribute = YPath(file_, attributes={"file_name": "file_name_in_path_attribute"})

        check_file_present(file_path_with_attribute, "file_name_in_path_attribute")

        link_to_file = TEST_DIR + "/link_to_file"
        yt.link(file_, link_to_file)

        check_file_present(link_to_file, "link_to_file")

        yt.set(link_to_file + "&/@file_name", "file_name_in_link_attribute")

        check_file_present(link_to_file, "file_name_in_link_attribute")

    def test_bash_env(self, yt_env_job_archive):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"key": "1", "value": "2"}])

        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=get_tests_sandbox(),
            prefix="bash_env_",
            suffix=".sh",
            delete=False,
        ) as bash_env:
            bash_env.write("echo \"FROM_BASH_ENV\" >&2")

        spec = {"mapper": {"environment": {"BASH_ENV": bash_env.name}}}
        op = yt.run_map(self.get_ok_command(), table, TEST_DIR + "/output", format=self.TEXT_YSON, spec=spec)
        job_id = yt.list(get_operation_path(op.id) + "/jobs")[0]
        path = self._prepare_job_environment(yt_env_job_archive, op.id, job_id,
                                             get_context_mode=GetContextMode.full_input)
        p = subprocess.Popen([os.path.join(path, "run.sh")], env={"PATH": "/bin:/usr/bin:" + os.environ["PATH"]}, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, p_stderr = p.communicate()
        assert p_stderr == u"FROM_BASH_ENV\nOK_COMMAND\n".encode("ascii")

    def test_table_download(self, yt_env_job_archive):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"key": "1", "value": "2"}])

        TABLE_AS_FILE_DATA = {"key": "42", "value": "forty two"}
        table_as_file = TEST_DIR + "/table_as_file"
        yt.write_table(table_as_file, [TABLE_AS_FILE_DATA])

        command = self.get_ok_command() + """ ; if [ ! -f table_as_file.json ] ; then echo "CANNOT FIND table_as_file.json" >&2 ; exit 1 ; fi """
        op = yt.run_map(command, table, TEST_DIR + "/output",
                        format=self.TEXT_YSON,
                        yt_files=["<file_name=\"table_as_file.json\";format=json>" + table_as_file])
        job_id = yt.list(get_operation_path(op.id) + "/jobs")[0]
        path = self._prepare_job_environment(yt_env_job_archive, op.id, job_id,
                                             get_context_mode=GetContextMode.full_input)
        p = subprocess.Popen([os.path.join(path, "run.sh")], env={"PATH": "/bin:/usr/bin:" + os.environ["PATH"]}, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, p_stderr = p.communicate()
        assert p_stderr == u"OK_COMMAND\n".encode("ascii")

        with open(os.path.join(path, "sandbox", "table_as_file.json")) as inf:
            table_as_file_saved = json.load(inf)
            assert table_as_file_saved == TABLE_AS_FILE_DATA
