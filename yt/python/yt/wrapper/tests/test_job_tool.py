from .conftest import authors
from .helpers import (get_tests_sandbox, TEST_DIR, get_operation_path)

from yt.common import to_native_str, YT_NULL_TRANSACTION_ID, YtError
import yt.subprocess_wrapper as subprocess
from yt.wrapper.errors import YtOperationFailedError
from yt.wrapper.ypath import YPath

import yt.wrapper as yt

import yt.wrapper.job_tool as yt_job_tool

import os
import stat
import tempfile
import shutil
import time
import json

NODE_ORCHID_JOB_PATH_PATTERN = "//sys/cluster_nodes/{0}/orchid/exec_node/job_controller/active_jobs/{1}"


class TestJobTool(object):
    TEXT_YSON = "<format=pretty>yson"

    def get_failing_command(self):
        return "cat > {tmpdir}/$YT_JOB_ID.input && echo ERROR_INTENDED_BY_TEST >&2 && exit 1".format(tmpdir=self._tmpdir)

    def get_ok_command(self):
        return "cat > {tmpdir}/$YT_JOB_ID.input && echo OK_COMMAND >&2".format(tmpdir=self._tmpdir)

    def setup(self):
        self._tmpdir = tempfile.mkdtemp(dir=get_tests_sandbox())
        # allow user job to write to this directory
        os.chmod(self._tmpdir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    def _prepare_job_environment(self, yt_env_job_archive, operation_id, job_id,
                                 get_context_mode=yt_job_tool.INPUT_CONTEXT_MODE):
        job_path = os.path.join(yt_env_job_archive.env.path, "test_job_tool", "job_" + job_id)
        try:
            return yt_job_tool.prepare_job_environment(
                operation_id,
                job_id,
                job_path,
                get_context_mode=get_context_mode)
        except YtError:
            shutil.rmtree(job_path, ignore_errors=True)
            raise

    def _check(self, operation_id, yt_env_job_archive, check_running=False,
               get_context_mode=yt_job_tool.INPUT_CONTEXT_MODE, expect_ok_return_code=False):
        if not check_running:
            job_infos = yt.list_jobs(operation_id, with_stderr=True)["jobs"]
            assert job_infos
            job_id = job_infos[0]["id"]
        else:
            total_job_wait_timeout = 10
            start_time = time.time()

            running_jobs_path = get_operation_path(operation_id) + "/controller_orchid/running_jobs"

            while True:  # Waiting for job to start
                jobs = yt.list(running_jobs_path)
                if jobs:
                    break

                if time.time() - start_time > total_job_wait_timeout:
                    assert False, "Timeout occurred while waiting any job of operation {0} to run".format(operation_id)

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
                    assert False, "Timeout occurred while waiting for job {0} to run".format(job_id)

        job_path = self._prepare_job_environment(yt_env_job_archive, operation_id, job_id, get_context_mode)

        assert open(os.path.join(job_path, "sandbox", "_test_file")).read().strip() == "stringdata"
        with open(os.path.join(self._tmpdir, job_id + ".input")) as canonical_input:
            assert canonical_input.read() == open(os.path.join(job_path, "input")).read()

        if not check_running:
            run_script = os.path.join(job_path, "run.sh")
            proc = subprocess.Popen(
                [run_script],
                env={"PATH": "/bin:/usr/bin:" + os.environ["PATH"]},
                stderr=subprocess.PIPE)
            proc.wait()

            if expect_ok_return_code:
                assert proc.returncode == 0, proc.stderr.read()
            else:
                assert proc.returncode != 0
                assert "ERROR_INTENDED_BY_TEST" in to_native_str(proc.stderr.read())
                with open(os.path.join(job_path, "output", "2")) as fin:
                    assert "ERROR_INTENDED_BY_TEST" in fin.read()

        shutil.rmtree(job_path)

    @authors("ignat")
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

    @authors("ignat")
    def test_job_tool_full(self, yt_env_job_archive):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"key": "1", "value": "2"}])

        file_ = TEST_DIR + "/_test_file"
        yt.write_file(file_, b"stringdata")

        op = yt.run_map(self.get_ok_command(), table, TEST_DIR + "/output", format=self.TEXT_YSON, yt_files=[file_])
        self._check(op.id, yt_env_job_archive, get_context_mode=yt_job_tool.FULL_INPUT_MODE,
                    expect_ok_return_code=True)

    @authors("ignat")
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
        self._check(op.id, yt_env_job_archive, get_context_mode=yt_job_tool.FULL_INPUT_MODE,
                    expect_ok_return_code=True)

    @authors("ignat")
    def test_run_sh(self, yt_env_job_archive):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"key": "1", "value": "2"}])

        file_ = TEST_DIR + "/_test_file"
        yt.write_file(file_, b"stringdata")

        op = yt.run_map(self.get_ok_command(), table, TEST_DIR + "/output", format=self.TEXT_YSON, yt_files=[file_])
        job_id = yt.list_jobs(op.id)["jobs"][0]["id"]
        path = self._prepare_job_environment(yt_env_job_archive, op.id, job_id, get_context_mode=yt_job_tool.FULL_INPUT_MODE)
        p = subprocess.Popen([os.path.join(path, "run.sh")], env={"PATH": "/bin:/usr/bin:" + os.environ["PATH"]}, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, p_stderr = p.communicate()
        assert p_stderr == u"OK_COMMAND\n".encode("ascii")

    @authors("ignat")
    def test_environment(self, yt_env_job_archive):
        command = self.get_ok_command() + """ ; if [ "$YT_JOB_TOOL_TEST_VARIABLE" != "present" ] ; then echo "BAD VARIABLE" >&2 ; exit 1 ; fi """

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"key": "1", "value": "2"}])

        file_ = TEST_DIR + "/_test_file"
        yt.write_file(file_, b"stringdata")

        op = yt.run_map(command, table, TEST_DIR + "/output", format=self.TEXT_YSON, yt_files=[file_],
                        spec={"mapper": {"environment": {"YT_JOB_TOOL_TEST_VARIABLE": "present"}}})
        job_id = yt.list_jobs(op.id)["jobs"][0]["id"]
        path = self._prepare_job_environment(yt_env_job_archive, op.id, job_id, get_context_mode=yt_job_tool.FULL_INPUT_MODE)
        p = subprocess.Popen([os.path.join(path, "run.sh")], env={"PATH": "/bin:/usr/bin:" + os.environ["PATH"]}, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, p_stderr = p.communicate()
        assert p_stderr == u"OK_COMMAND\n".encode("ascii")

    @authors("max42")
    def test_file_name_precedence(self, yt_env_job_archive):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"key": "1", "value": "2"}])

        file_content = b"FILE_CONTENT"

        def check_file_present(file_cypress_path, file_name_in_job):
            command = "cat < {} >&2; cat ".format(file_name_in_job)
            op = yt.run_map(command, table, TEST_DIR + "/output", format=self.TEXT_YSON, yt_files=[file_cypress_path])
            job_id = yt.list_jobs(op.id)["jobs"][0]["id"]
            path = self._prepare_job_environment(yt_env_job_archive, op.id, job_id, get_context_mode=yt_job_tool.FULL_INPUT_MODE)
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

    @authors("ignat")
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
        job_id = yt.list_jobs(op.id)["jobs"][0]["id"]
        path = self._prepare_job_environment(yt_env_job_archive, op.id, job_id, get_context_mode=yt_job_tool.FULL_INPUT_MODE)
        p = subprocess.Popen([os.path.join(path, "run.sh")], env={"PATH": "/bin:/usr/bin:" + os.environ["PATH"]}, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, p_stderr = p.communicate()
        assert p_stderr == u"FROM_BASH_ENV\nOK_COMMAND\n".encode("ascii")

    @authors("ignat")
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
        job_id = yt.list_jobs(op.id)["jobs"][0]["id"]
        path = self._prepare_job_environment(yt_env_job_archive, op.id, job_id, get_context_mode=yt_job_tool.FULL_INPUT_MODE)
        p = subprocess.Popen([os.path.join(path, "run.sh")], env={"PATH": "/bin:/usr/bin:" + os.environ["PATH"]}, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, p_stderr = p.communicate()
        assert p_stderr == u"OK_COMMAND\n".encode("ascii")

        with open(os.path.join(path, "sandbox", "table_as_file.json")) as inf:
            table_as_file_saved = json.load(inf)
            assert table_as_file_saved == TABLE_AS_FILE_DATA

    @authors("ignat")
    def test_user_transaction(self, yt_env_job_archive):
        with yt.Transaction():
            file_ = TEST_DIR + "/_test_file"
            yt.write_file(file_, b"stringdata")

            table = TEST_DIR + "/table"
            yt.write_table(table, [{"key": "1", "value": "2"}])

            try:
                op = yt.run_map("exit 1", table, TEST_DIR + "/output", yt_files=[file_], format=self.TEXT_YSON,
                                sync=False)
                op.wait()
            except YtOperationFailedError:
                assert True

            with yt.Transaction(transaction_id=YT_NULL_TRANSACTION_ID):
                job_id = yt.list_jobs(op.id)["jobs"][0]["id"]
                path = self._prepare_job_environment(yt_env_job_archive, op.id, job_id,
                                                     get_context_mode=yt_job_tool.FULL_INPUT_MODE)
                p = subprocess.Popen([os.path.join(path, "run.sh")],
                                     env={"PATH": "/bin:/usr/bin:" + os.environ["PATH"]},
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                _, p_stderr = p.communicate()

                job_path = os.path.join(yt_env_job_archive.env.path, "test_job_tool", "job_" + job_id)
                sandbox_path = os.path.join(job_path, "sandbox")
                assert os.listdir(sandbox_path)[0] == "_test_file"

    @authors("ermolovd")
    def test_vanilla_operation_no_outputs(self, yt_env_job_archive):
        job_file_data = b"testresource"

        job_file = TEST_DIR + "/job_file"
        output_table = TEST_DIR + "/output_table"
        yt.write_file(job_file, job_file_data)
        yt.create("table", output_table)

        task_spec = yt.TaskSpecBuilder() \
            .job_count(1) \
            .command("cat job_file >&2") \
            .add_file_path(job_file)

        vanilla_spec = yt.VanillaSpecBuilder() \
            .task("foo", task_spec)

        op = yt.run_operation(vanilla_spec)

        job_id = yt.list_jobs(op.id)["jobs"][0]["id"]
        job_path = self._prepare_job_environment(yt_env_job_archive, op.id, job_id, get_context_mode=yt_job_tool.FULL_INPUT_MODE)

        p = subprocess.Popen([os.path.join(job_path, "run.sh")], env={"PATH": "/bin:/usr/bin:" + os.environ["PATH"]}, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, p_stderr = p.communicate()
        assert p_stderr == job_file_data

    @authors("ermolovd")
    def test_vanilla_operation_with_outputs(self, yt_env_job_archive):
        job_file_data_0 = b"""{"foo": 42}"""
        job_file_data_1 = b"""{"foo": 54}"""

        job_file_0 = TEST_DIR + "/job_file_0"
        job_file_1 = TEST_DIR + "/job_file_1"
        output_table_0 = TEST_DIR + "/output_table_0"
        output_table_1 = TEST_DIR + "/output_table_1"
        yt.write_file(job_file_0, job_file_data_0)
        yt.write_file(job_file_1, job_file_data_1)
        yt.create("table", output_table_0)
        yt.create("table", output_table_1)

        task_spec = yt.TaskSpecBuilder() \
            .job_count(1) \
            .command("cat job_file_0 && echo OK_FROM_JOB >&2 && cat job_file_1 >&4") \
            .add_file_path(job_file_0) \
            .add_file_path(job_file_1) \
            .format("json") \
            .spec({"output_table_paths": [output_table_0, output_table_1]})

        vanilla_spec = yt.VanillaSpecBuilder() \
            .task("foo", task_spec)

        op = yt.run_operation(vanilla_spec)

        job_id = yt.list_jobs(op.id)["jobs"][0]["id"]
        job_path = self._prepare_job_environment(yt_env_job_archive, op.id, job_id, get_context_mode=yt_job_tool.FULL_INPUT_MODE)

        with open(os.path.join(job_path, "sandbox", "job_file_0"), "rb") as inf:
            assert inf.read() == job_file_data_0

        with open(os.path.join(job_path, "sandbox", "job_file_1"), "rb") as inf:
            assert inf.read() == job_file_data_1

        p = subprocess.Popen([os.path.join(job_path, "run.sh")], env={"PATH": "/bin:/usr/bin:" + os.environ["PATH"]}, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, p_stderr = p.communicate()
        assert p_stderr == u"OK_FROM_JOB\n".encode("ascii")

        with open(os.path.join(job_path, "output", "1"), "rb") as fin:
            assert job_file_data_0 == fin.read()

        with open(os.path.join(job_path, "output", "4"), "rb") as fin:
            assert job_file_data_1 == fin.read()
