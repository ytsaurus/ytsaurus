import pytest
import time

from yt_env_setup import YTEnvSetup, require_ytserver_root_privileges, unix_only, wait
from yt_commands import *

from copy import deepcopy
from random import shuffle

from test_get_job_input import wait_for_data_in_job_archive

import yt.environment.init_operation_archive as init_operation_archive

# for launching other tests with RPC backend
def create_input_table(path, data, dynamic_schema, driver_backend, authenticated_user=None):
    if driver_backend == "rpc":
        create("table", path,
            authenticated_user=authenticated_user,
            attributes={
                "schema": dynamic_schema,
                "dynamic": True
            })
        sync_mount_table(path)
        insert_rows(path, data, authenticated_user=authenticated_user)
        sync_unmount_table(path)
    else:
        create("table", path, authenticated_user=authenticated_user)
        write_table(path, data, authenticated_user=authenticated_user)

##################################################################

class TestRpcProxy(YTEnvSetup):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    def test_non_sticky_transactions_dont_stick(self):
        tx = start_transaction(timeout=1000)
        wait(lambda: not exists("//sys/transactions/" + tx))

##################################################################

# TODO(kiselyovp) tests for file caching (when read_file is implemented)

class TestRpcProxyBase(YTEnvSetup):
    NUM_MASTERS = 1
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

    def _create_simple_table(self, path, data = [], dynamic=True, sorted=True, **kwargs):
        schema = self._schema_sorted if sorted else self._schema
        create("table", path,
               attributes={
                   "dynamic": dynamic,
                   "schema": schema},
               **kwargs)

        if dynamic:
            sync_create_cells(1)
        if not data:
            return
        if dynamic:
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

##################################################################

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

    def test_update_op_params_check_perms(self):
        op = self._start_simple_operation_with_breakpoint()
        wait(lambda: op.get_state() == "running")

        create_user("u")

        update_op_parameters(op.id, parameters={"acl": [make_ace("allow", "u", ["read", "manage"])]})
        # No exception.
        op.complete(authenticated_user="u")

        events_on_fs().release_breakpoint()
        op.track()

##################################################################

@require_ytserver_root_privileges
class TestSchedulerRpcProxy(TestRpcProxyBase):
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "statistics_reporter": {
                "enabled": True,
                "reporting_period": 10,
                "min_repeat_delay": 10,
                "max_repeat_delay": 10,
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

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            # Force snapshot never happen
            "snapshot_period": 10**9,
        }
    }

    def setup(self):
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client(), override_tablet_cell_bundle="default")

    def teardown(self):
        remove("//sys/operations_archive")

    def _get_job_ids(self, op):
        return op.get_running_jobs().keys()

    def _get_first_job_id(self, op):
        return self._get_job_ids(op)[0]

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

    def test_map_input_paths(self):
        tmpdir = create_tmpdir("inputs")
        self._create_simple_table(
            "//tmp/in1",
            [{"index": i+j, "str": "foo"} for i in xrange(0, 5, 2) for j in xrange(2)],
            sorted=False)

        self._create_simple_table(
            "//tmp/in2",
            [{"index": (i+j) / 4, "str": "bar"} for i in xrange(3, 24, 2) for j in xrange(2)],
            sorted=True)

        create("table", "//tmp/out")
        in2 = '//tmp/in2[1:4,5:6]'
        op = map(
            dont_track=True,
            in_=["//tmp/in1", in2],
            out="//tmp/out",
            command="cat > {0}/$YT_JOB_ID && exit 1".format(tmpdir),
            spec={
                "mapper": {
                    "format": "dsv"
                },
                "job_count": 1,
                "max_failed_job_count": 1
            })
        with pytest.raises(YtError):
            op.track()

        job_ids = os.listdir(tmpdir)
        assert job_ids
        wait_for_data_in_job_archive(op.id, job_ids)

        assert len(job_ids) == 1
        expected = yson.loads("""[
            <ranges=[{lower_limit={row_index=0};upper_limit={row_index=6}}]>"//tmp/in1";
            <ranges=[
                {lower_limit={key=[1]};upper_limit={key=[4]}};
                {lower_limit={key=[5]};upper_limit={key=[6]}}
            ]>"//tmp/in2"]""")
        actual = yson.loads(get_job_input_paths(job_ids[0]))
        assert expected == actual

##################################################################

class TestPessimisticQuotaCheckRpcProxy(TestRpcProxyBase):
    NUM_MASTERS = 1
    NUM_NODES = 16
    NUM_SCHEDULERS = 0

    REPLICATOR_REACTION_TIME = 3.5

    def _replicator_sleep(self):
        time.sleep(self.REPLICATOR_REACTION_TIME)

    def _set_account_chunk_count_limit(self, account, value):
        set("//sys/accounts/{0}/@resource_limits/chunk_count".format(account), value)

    def _set_account_tablet_count_limit(self, account, value):
        set("//sys/accounts/{0}/@resource_limits/tablet_count".format(account), value)

    def _is_account_disk_space_limit_violated(self, account):
        return get("//sys/accounts/{0}/@violated_resource_limits/disk_space".format(account))

    def _is_account_chunk_count_limit_violated(self, account):
        return get("//sys/accounts/{0}/@violated_resource_limits/chunk_count".format(account))

    def test_chunk_count_limits(self):
        create_account("max")
        self._set_account_tablet_count_limit("max", 100500)

        self._create_simple_table("//tmp/t", [self._sample_line])
        create("map_node", "//tmp/a")
        set("//tmp/a/@account", "max")

        self._set_account_chunk_count_limit("max", 0)
        self._replicator_sleep()
        assert not self._is_account_chunk_count_limit_violated("max")

        with pytest.raises(YtError): copy("//tmp/t", "//tmp/a/t")
        assert not exists("//tmp/a/t")
        copy("//tmp/t", "//tmp/a/t", pessimistic_quota_check=False)
        assert exists("//tmp/a/t")

    def test_disk_space_limits(self):
        create_account("max")
        self._set_account_tablet_count_limit("max", 100500)

        self._create_simple_table("//tmp/t", [self._sample_line])
        create("map_node", "//tmp/a")
        set("//tmp/a/@account", "max")

        set_account_disk_space_limit("max", 0)
        self._replicator_sleep()
        assert not self._is_account_disk_space_limit_violated("max")

        with pytest.raises(YtError): copy("//tmp/t", "//tmp/a/t")
        assert not exists("//tmp/a/t")
        copy("//tmp/t", "//tmp/a/t", pessimistic_quota_check=False)
        assert exists("//tmp/a/t")

##################################################################

class TestPessimisticQuotaCheckMulticellRpcProxy(TestPessimisticQuotaCheckRpcProxy):
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_SCHEDULERS = 1

##################################################################

class TestAclsRpcProxy(TestRpcProxyBase):
    def test_check_permission_by_acl(self):
        create_user("u1")
        create_user("u2")
        assert check_permission_by_acl("u1", "remove", [{"subjects": ["u1"], "permissions": ["remove"], "action": "allow"}])["action"] == "allow"
        assert check_permission_by_acl("u1", "remove", [{"subjects": ["u2"], "permissions": ["remove"], "action": "allow"}])["action"] == "deny"
        assert check_permission_by_acl(None, "remove", [{"subjects": ["u2"], "permissions": ["remove"], "action": "allow"}])["action"] == "allow"

##################################################################

class TestModifyRowsRpcProxy(TestRpcProxyBase):
    BATCH_CAPACITY = 10
    DELTA_DRIVER_CONFIG = {"modify_rows_batch_capacity": BATCH_CAPACITY}

    def _test_modify_rows_batching(self, request_count, key_count, tx_type="tablet"):
        self._create_simple_table("//tmp/table")
        sync_mount_table("//tmp/table")

        tx = start_transaction(type=tx_type, sticky=True)

        for i in range(request_count):
            insert_rows(
                "//tmp/table",
                [{"index": i % key_count, "str": str(i / key_count)}],
                tx=tx)

        commit_transaction(tx, sticky=True)

        expected_result = [
            {"index": i,
            "str": str((request_count - i - 1) // key_count)}\
                for i in range(key_count)]
        assert select_rows("* from [//tmp/table]") == expected_result

        sync_unmount_table("//tmp/table")
        remove("//tmp/table")

    def test_modify_rows_batching(self):
        self._test_modify_rows_batching(60, 7, "tablet")
        self._test_modify_rows_batching(65, 7, "master")

