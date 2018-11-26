import pytest
import time

from yt_env_setup import YTEnvSetup, require_ytserver_root_privileges, unix_only, wait
from yt_commands import *

from copy import deepcopy
from random import shuffle

from test_get_operation import _get_operation_from_cypress

import yt.environment.init_operation_archive as init_operation_archive

class TestRpcProxy(YTEnvSetup):
    DRIVER_BACKEND = "rpc"

    def test_non_sticky_transactions_dont_stick(self):
        tx = start_transaction(timeout=1000)
        wait(lambda: not exists("//sys/transactions/" + tx))

# TODO (kiselyovp) tests for file caching (when read_file is implemented)

class TestRpcProxyBase(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_PROXY = True

    _schema_dicts = [{"name": "index", "type": "int64"},
                     {"name": "str", "type": "string"}]
    _schema = make_schema(_schema_dicts, strict=True)

    _schema_dicts_sorted = [{"name": "index", "type": "int64", "sort_order": "ascending"},
                            {"name": "str", "type": "string"}]
    _schema_sorted = make_schema(_schema_dicts_sorted, strict=True, unique_keys=True)
    _sample_index = 241
    _sample_text = "sample text"
    _sample_line = {"index": _sample_index, "str": _sample_text}

    def _create_simple_table(self, path, data = [], dynamic=True, sorted=True):
        schema = self._schema_sorted if sorted else self._schema
        create("table", path,
               attributes={
                   "dynamic": dynamic,
                   "schema": schema})
        if not data:
            return
        if dynamic:
            sync_create_cells(1)
            sync_mount_table(path)
            insert_rows(path, data)
            sync_unmount_table(path)
        else:
            insert_rows(path, data)

    def _start_simple_operation(self, cmd, **kwargs):
        self._create_simple_table("//tmp/t_in",
                                  data=[self._sample_line],
                                  sorted=True,
                                  dynamic=True)
        self._create_simple_table("//tmp/t_out", dynamic=False, sorted=True)

        return map(in_="//tmp/t_in",
                   out="//tmp/t_out",
                   dont_track=True,
                   mapper_command=cmd,
                   **kwargs)

    def _start_simple_operation_on_fs(self, event_name="barrier", **kwargs):
        return self._start_simple_operation(events_on_fs().wait_event_cmd(event_name), **kwargs)

    def _start_simple_operation_with_breakpoint(self, cmd_with_breakpoint = "BREAKPOINT", **kwargs):
        return self._start_simple_operation(events_on_fs().with_breakpoint(cmd_with_breakpoint), **kwargs)

    def _prepare_output_table(self):
        alter_table("//tmp/t_out", dynamic=True, schema=self._schema_sorted)
        sync_mount_table("//tmp/t_out")

    def _remove_simple_tables(self):
        remove("//tmp/t_in", recursive=True)
        remove("//tmp/t_out", recursive=True)

class TestOperationsRpcProxy(TestRpcProxyBase):
    def test_map_reduce_simple(self):
        self._create_simple_table("//tmp/t_in",
                                  data=[self._sample_line])

        self._create_simple_table("//tmp/t_out", dynamic=False)
        map_reduce(in_="//tmp/t_in",
                   out="//tmp/t_out",
                   sort_by="index",
                   reducer_command="cat")

        self._prepare_output_table()

        assert len(select_rows("* from [//tmp/t_out]")) == 1
        assert len(lookup_rows("//tmp/t_out", [{"index": self._sample_index - 2}])) == 0
        assert len(lookup_rows("//tmp/t_out", [{"index": self._sample_index}])) == 1

    def test_sort(self):
        size = 10 ** 3
        original_table = [{"index": num, "str": "number " + str(num)} for num in range(size)]
        new_table = deepcopy(original_table)
        shuffle(new_table)

        self._create_simple_table("//tmp/t_in1",
                                  data=new_table[:size / 2],
                                  sorted=False)
        self._create_simple_table("//tmp/t_in2",
                                  data=new_table[size / 2:],
                                  sorted=False)
        self._create_simple_table("//tmp/t_out",
                                  dynamic=False,
                                  sorted=True)
        sort(in_=["//tmp/t_in1", "//tmp/t_in2"],
             out="//tmp/t_out",
             sort_by="index")

        self._prepare_output_table()

        assert select_rows("* from [//tmp/t_out] LIMIT " + str(2 * size)) == original_table

    def test_abort_operation(self):
        op = self._start_simple_operation_with_breakpoint()
        wait(lambda: op.get_state() == "running")

        op.abort()

        wait(lambda: op.get_state() == "aborted")

    def test_complete_operation(self):
        op = self._start_simple_operation_with_breakpoint()
        wait(lambda: op.get_state() == "running")

        op.complete()

        op.track()
        assert op.get_state() == "completed"

    def _check_get_operation(self, op):
        def filter_attrs(attrs):
            PROPER_ATTRS = [
                "id",
                "authenticated_user",
                "brief_progress",
                "brief_spec",
                "runtime_parameters",
                "finish_time",
                "type",
                # COMPAT(levysotsky): Old name for "type"
                "operation_type",
                "result",
                "start_time",
                "state",
                "suspended",
                "spec",
                "unrecognized_spec",
                "full_spec",
                "slot_index_per_pool_tree",
            ]
            return {key: attrs[key] for key in PROPER_ATTRS if key in attrs}

        res_get_operation = get_operation(op.id, include_scheduler=True)
        res_cypress = _get_operation_from_cypress(op.id)

        assert filter_attrs(res_get_operation) == filter_attrs(res_cypress)

    def test_suspend_resume_operation(self):
        op = self._start_simple_operation_with_breakpoint()
        wait(lambda: op.get_state() == "running")

        op.suspend(abort_running_jobs=True)
        wait(lambda: get(op.get_path() + "/@suspended"))
        assert op.get_state() == "running"
        events_on_fs().release_breakpoint()
        time.sleep(2)
        assert get(op.get_path() + "/@suspended")
        assert op.get_state() == "running"

        op.resume()
        op.track()
        assert op.get_state() == "completed"
        # XXX(kiselyovp) a test with abort_running_jobs=False?

    def test_get_operation(self):
        op = self._start_simple_operation_with_breakpoint()
        events_on_fs().wait_breakpoint()

        wait(lambda: exists(op.get_path()))
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs/running") == 1)

        self._check_get_operation(op)

        events_on_fs().release_breakpoint()
        op.track()

        self._check_get_operation(op)

    def test_update_op_params_check_perms(self):
        op = self._start_simple_operation_with_breakpoint()
        wait(lambda: op.get_state() == "running")

        create_user("u")

        update_op_parameters(op.id, parameters={"owners": ["u"]})
        wait(lambda: check_permission("u", "write", op.get_path())["action"] == "allow")

        events_on_fs().release_breakpoint()
        op.track()

@require_ytserver_root_privileges
class TestSchedulerRpcProxy(TestRpcProxyBase):
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "slot_manager": {
                "job_environment": {
                    "type": "cgroups",
                    "supported_cgroups": [
                        "cpuacct",
                        "blkio",
                        "cpu"],
                },
            }
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
            "enable_job_stderr_reporter": True,
        }
    }

    def setup(self):
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client())

    def teardown(self):
        remove("//sys/operations_archive")

    def _get_job_ids(self, op):
        return op.get_running_jobs().keys()

    def _get_first_job_id(self, op):
        return self._get_job_ids(op)[0]

    def test_get_job(self):
        op = self._start_simple_operation_with_breakpoint()
        events_on_fs().wait_breakpoint()

        wait(lambda: exists(op.get_path()))
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs/running") == 1)

        get_result = op.get_running_jobs()
        job_id = get_result.keys()[0]
        get_result = get_result[job_id]
        get_result["type"] = get_result["job_type"]

        get_job_result = retry(lambda: get_job(op.id, job_id))
        assert get_job_result["job_id"] == job_id

        JOB_ATTRIBUTES = ["type",
                          "state",
                          "address"]
        for attr in JOB_ATTRIBUTES:
            assert get_job_result[attr] == get_result[attr]
        assert get_job_result["type"] == "map"

        events_on_fs().release_breakpoint()
        op.track()

    def test_abandon_job(self):
        def check_result_length(cmd_with_breakpoint, func, length):
            op = self._start_simple_operation_with_breakpoint(cmd_with_breakpoint)
            events_on_fs().wait_breakpoint()

            wait(lambda: exists(op.get_path()))
            wait(lambda: get(op.get_path() + "/@brief_progress/jobs/running") == 1)

            job_id = self._get_first_job_id(op)
            func(job_id)

            wait(lambda: get(op.get_path() + "/@brief_progress/jobs/running") == 0)

            events_on_fs().release_breakpoint()
            op.track()

            self._prepare_output_table()

            assert len(select_rows("* from [//tmp/t_out]")) == length

            self._remove_simple_tables()
            reset_events_on_fs()

        check_result_length("BREAKPOINT ; cat", abandon_job, 0)
        check_result_length("cat; BREAKPOINT", abandon_job, 0)

    # XXX(kiselyovp) All tests below are basically copypasta.
    # XXX(kiselyovp) Delete them when {read|write}_{table|file|journal} methods are supported in RPC proxy.
    def test_abort_job(self):
        op = self._start_simple_operation_with_breakpoint("BREAKPOINT ; cat")
        events_on_fs().wait_breakpoint()

        wait(lambda: exists(op.get_path()))
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs/running") == 1)

        job_id = self._get_first_job_id(op)
        abort_job(job_id)

        events_on_fs().release_breakpoint()
        op.track()

        self._prepare_output_table()
        assert len(select_rows("* from [//tmp/t_out]")) == 1

        assert get(op.get_path() + "/@progress/jobs/aborted/total") == 1
        assert get(op.get_path() + "/@progress/jobs/failed") == 0

    def test_strace_job(self):
        op = self._start_simple_operation("{notify_running} ; sleep 5000".
                                          format(notify_running=events_on_fs().notify_event_cmd("job_is_running")))

        events_on_fs().wait_event("job_is_running")
        time.sleep(1.0) # give job proxy some time to send a heartbeat
        result = strace_job(self._get_first_job_id(op))

        assert len(result) > 0
        for pid, trace in result["traces"].iteritems():
            assert trace["trace"].startswith("Process {0} attached".format(pid))
            assert "process_command_line" in trace
            assert "process_name" in trace

    def _poll_until_prompt(self, job_id, shell_id):
        output = ""
        while len(output) < 4 or output[-4:] != ":~$ ":
            r = poll_job_shell(job_id, operation="poll", shell_id=shell_id)
            output += r["output"]
        return output

    def test_poll_job_shell(self):
        op = self._start_simple_operation(with_breakpoint("BREAKPOINT ; cat"))
        job_id = wait_breakpoint()[0]

        r = poll_job_shell(job_id, operation="spawn", term="screen-256color", height=50, width=132)
        shell_id = r["shell_id"]
        self._poll_until_prompt(job_id, shell_id)

        command = "echo $TERM; tput lines; tput cols; env | grep -c YT_OPERATION_ID\r"
        poll_job_shell(
            job_id,
            operation="update",
            shell_id=shell_id,
            keys=command.encode("hex"),
            input_offset=0)
        output = self._poll_until_prompt(job_id, shell_id)

        expected = "{0}\nscreen-256color\r\n50\r\n132\r\n1".format(command)
        assert output.startswith(expected)

        poll_job_shell(job_id, operation="terminate", shell_id=shell_id)
        with pytest.raises(YtError):
            self._poll_until_prompt(job_id, shell_id)

        abandon_job(job_id)

        op.track()
        self._prepare_output_table()

        assert len(select_rows("* from [//tmp/t_out]")) == 0

    def test_dump_job_context(self):
        op = self._start_simple_operation(with_breakpoint("cat ; BREAKPOINT"))

        jobs = wait_breakpoint()
        # Wait till job starts reading input
        wait(lambda: get(op.get_path() + "/controller_orchid/running_jobs/" + jobs[0] + "/progress") >= 0.5)

        dump_job_context(jobs[0], "//tmp/input_context")

        events_on_fs().release_breakpoint()
        op.track()

        # XXX(kiselyovp) read_file is not implemented in RPC proxy yet
        '''context = read_file("//tmp/input_context")
        assert get("//tmp/input_context/@description/type") == "input_context"
        assert JsonFormat().loads_row(context)["foo"] == "bar"'''

        assert exists("//tmp/input_context")
        assert get("//tmp/input_context/@uncompressed_data_size") > 0

    @unix_only
    def test_signal_job_with_no_job_restart(self):
        op = self._start_simple_operation_with_breakpoint(
            # XXX(kiselyovp) magic constants galore
            """(trap "echo '{"'"index": 242, "str": "SIGUSR1"}'"'" USR1 ; trap "echo '{"'"index": 243, "str": "SIGUSR2"}'"'" USR2 ; cat ; BREAKPOINT)""",
            spec={
                "mapper": {
                    "input_format": "json",
                    "output_format": "json"
                },
                "max_failed_job_count": 1
            })

        jobs = events_on_fs().wait_breakpoint()

        time.sleep(1.0) # give job proxy some time to send a heartbeat
        signal_job(jobs[0], "SIGUSR1")
        signal_job(jobs[0], "SIGUSR2")

        events_on_fs().release_breakpoint()
        op.track()

        assert get(op.get_path() + "/@progress/jobs/aborted/total") == 0
        assert get(op.get_path() + "/@progress/jobs/failed") == 0

        op.track()
        self._prepare_output_table()

        assert select_rows("* from [//tmp/t_out] LIMIT 5") ==\
               [{"index": self._sample_index, "str": self._sample_text},
                {"index": self._sample_index + 1, "str": "SIGUSR1"},
                {"index": self._sample_index + 2, "str": "SIGUSR2"}]

    @unix_only
    def test_signal_job_with_job_restart(self):
        op = self._start_simple_operation_with_breakpoint(
            # XXX(kiselyovp) magic constants galore
            """(trap "echo '{"'"index": 242, "str": "SIGUSR1"}'"'"" ; echo stderr >&2 ; exit 1" USR1; cat; BREAKPOINT)""",
            spec={
                "mapper": {
                    "input_format": "json",
                    "output_format": "json"
                },
                "max_failed_job_count": 1
            })

        jobs = wait_breakpoint()

        time.sleep(1.0) # give job proxy some time to send a heartbeat
        signal_job(jobs[0], "SIGUSR1")
        release_breakpoint()

        op.track()

        assert get(op.get_path() + "/@progress/jobs/aborted/total") == 1
        assert get(op.get_path() + "/@progress/jobs/aborted/scheduled/user_request") == 1
        assert get(op.get_path() + "/@progress/jobs/aborted/scheduled/other") == 0
        assert get(op.get_path() + "/@progress/jobs/failed") == 0

        self._prepare_output_table()
        assert select_rows("* from [//tmp/t_out]") == [self._sample_line]
        # XXX(kiselyovp) read_file is not implemented in RPC proxy yet
        # check_all_stderrs(op, "stderr\n", 1, substring=True)
