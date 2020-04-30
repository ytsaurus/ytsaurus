from yt_env_setup import wait, YTEnvSetup
from yt_commands import *

import yt.environment.init_operation_archive as init_operation_archive
from yt.wrapper.common import uuid_hash_pair
from yt.common import parts_to_uuid

import pytest
from flaky import flaky

import __builtin__

import datetime
import json
import pytest
import shutil
import sys
import time

FORMAT_LIST = [
    "yson",
    "json",
    "dsv",
    "yamr",
]

OPERATION_JOB_ARCHIVE_TABLE = "//sys/operations_archive/jobs"
OPERATION_JOB_SPEC_ARCHIVE_TABLE = "//sys/operations_archive/job_specs"

def get_job_rows_for_operation(operation_id):
    hash_pair = uuid_hash_pair(operation_id)
    return select_rows("* from [{0}] where operation_id_lo = {1}u and operation_id_hi = {2}u".format(
        OPERATION_JOB_ARCHIVE_TABLE,  hash_pair.lo, hash_pair.hi))

def generate_job_key(job_id):
    pair = uuid_hash_pair(job_id)
    return {
        "job_id_hi": pair.hi,
        "job_id_lo": pair.lo}

def get_job_spec_rows_for_jobs(job_ids):
    return lookup_rows(OPERATION_JOB_SPEC_ARCHIVE_TABLE, [generate_job_key(job_id) for job_id in job_ids])

def wait_for_data_in_job_archive(op_id, job_ids):
    print_debug("Waiting for jobs to appear in archive: ", job_ids)
    wait(lambda: len(get_job_rows_for_operation(op_id)) == len(job_ids))
    wait(lambda: len(get_job_spec_rows_for_jobs(job_ids)) == len(job_ids))

class TestGetJobInput(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_reporter": {
                "enabled": True,
                "reporting_period": 10,
                "min_repeat_delay": 10,
                "max_repeat_delay": 10,
            },
            "job_controller": {
                # Force total confirmation happen on every heartbeat
                "total_confirmation_period": 0
            },
        },
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            # Force snapshot never happen
            "snapshot_period": 10**9,
        }
    }

    def setup(self):
        self._tmpdir = create_tmpdir("inputs")
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client(), override_tablet_cell_bundle="default")

    def teardown(self):
        shutil.rmtree(self._tmpdir)
        remove("//sys/operations_archive")

    def check_job_ids(self, job_id_iter):
        for job_id in job_id_iter:
            input_file = os.path.join(self._tmpdir, job_id)
            with open(input_file) as inf:
                actual_input = inf.read()
            assert actual_input
            assert get_job_input(job_id) == actual_input

    @authors("ermolovd")
    @pytest.mark.parametrize("format", FORMAT_LIST)
    def test_map_in_progress(self, format):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("//tmp/t_input", [{"foo": i} for i in xrange(100)])

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="cat > {tmpdir}/$YT_JOB_ID ; {notify} ; {wait}".format(
                tmpdir=self._tmpdir,
                notify=events_on_fs().notify_event_cmd("job_is_running"),
                wait=events_on_fs().wait_event_cmd("job_can_finish")),
            format=format,
            track=False,
            spec={"job_count": 1})

        events_on_fs().wait_event("job_is_running")

        job_ids = os.listdir(self._tmpdir)
        assert len(job_ids) == 1

        # It only works because the operation consists of one job,
        # so there are no released jobs.
        self.check_job_ids(job_ids)

        events_on_fs().notify_event("job_can_finish")
        op.track()

    @authors("ermolovd")
    def test_map_complete(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("//tmp/t_input", [{"foo": i} for i in xrange(100)])

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="cat > {0}/$YT_JOB_ID".format(self._tmpdir),
            format="yson")

        job_ids = os.listdir(self._tmpdir)
        assert job_ids
        wait_for_data_in_job_archive(op.id, job_ids)

        self.check_job_ids(job_ids)

    @authors("ermolovd")
    @pytest.mark.xfail(run=False, reason="Job input is unavailable while the operation is not finished")
    def test_map_reduce(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("//tmp/t_input", [{"foo": i} for i in xrange(100)])

        cmd = "echo 1 >&2 ; cat | tee {0}/$YT_JOB_ID".format(self._tmpdir)
        reducer_cmd = " ; ".join([
            cmd,
            events_on_fs().notify_event_cmd("reducer_almost_complete"),
            events_on_fs().wait_event_cmd("continue_reducer")])
        op = map_reduce(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            mapper_command=cmd,
            reduce_combiner_command=cmd,
            reducer_command=reducer_cmd,
            sort_by="foo",
            spec={
                "mapper": {"format": "yson"},
                "reduce_combiner": {"format": "yson"},
                "reducer": {"format": "yson"},
                "data_size_per_sort_job": 10,
                "force_reduce_combiners": True,
            },
            track=False)

        events_on_fs().wait_event("reducer_almost_complete", timeout=datetime.timedelta(300))

        job_ids = os.listdir(self._tmpdir)
        assert len(job_ids) >= 3
        wait_for_data_in_job_archive(op.id, job_ids)

        self.check_job_ids(job_ids)

        events_on_fs().notify_event("continue_reducer")
        op.track()

    @authors("ermolovd")
    @pytest.mark.parametrize("format", FORMAT_LIST)
    def test_reduce_with_join(self, format):
        create("table", "//tmp/t_primary_input_1")
        create("table", "//tmp/t_primary_input_2")
        create("table", "//tmp/t_foreign_input_1")
        create("table", "//tmp/t_foreign_input_2")
        create("table", "//tmp/t_output")

        write_table("//tmp/t_primary_input_1", [
            {"host": "bb.ru", "path": "/1", "data": "foo"},
            {"host": "go.ru", "path": "/1", "data": "foo"},
            {"host": "go.ru", "path": "/2", "data": "foo"},
            {"host": "go.ru", "path": "/3", "data": "foo"},
            {"host": "ya.ru", "path": "/1", "data": "foo"},
            {"host": "ya.ru", "path": "/2", "data": "foo"},
            {"host": "zz.ru", "path": "/1", "data": "foo"},
        ], sorted_by=["host", "path"])

        write_table("//tmp/t_primary_input_2", [
            {"host": "cc.ru", "path": "/1", "data": "bar"},
            {"host": "go.ru", "path": "/1", "data": "bar"},
            {"host": "go.ru", "path": "/2", "data": "bar"},
            {"host": "go.ru", "path": "/3", "data": "bar"},
            {"host": "ya.ru", "path": "/1", "data": "bar"},
            {"host": "ya.ru", "path": "/2", "data": "bar"},
            {"host": "zz.ru", "path": "/1", "data": "bar"},
        ], sorted_by=["host", "path"])

        write_table("//tmp/t_foreign_input_1", [
            {"host": "aa.ru", "data": "baz"},
            {"host": "ya.ru", "data": "baz"},
            {"host": "ya.ru", "data": "baz"},
            {"host": "zz.ru", "data": "baz"},
        ], sorted_by=["host"])

        write_table("//tmp/t_foreign_input_2", [
            {"host": "aa.ru", "data": "baz"},
            {"host": "bb.ru", "data": "baz"},
            {"host": "go.ru", "data": "baz"},
            {"host": "ya.ru", "data": "baz"},
        ], sorted_by=["host"])

        op = reduce(
            in_=[
                "//tmp/t_primary_input_1",
                "//tmp/t_primary_input_2",
                "<foreign=true>//tmp/t_foreign_input_1",
                "<foreign=true>//tmp/t_foreign_input_2",
            ],
            reduce_by=["host", "path"],
            join_by=["host"],
            out="//tmp/t_output",
            command="cat > {0}/$YT_JOB_ID".format(self._tmpdir),
            format=format)

        job_ids = os.listdir(self._tmpdir)
        assert job_ids
        wait_for_data_in_job_archive(op.id, job_ids)

        self.check_job_ids(job_ids)

        assert len(job_ids) == 1
        paths = yson.loads(get_job_input_paths(job_ids[0]))
        assert len(paths) == 4

        foreign_path_count = 0
        for path in paths:
            assert len(path.attributes["ranges"]) == 1
            if "foreign" in path.attributes:
                foreign_path_count += 1
                assert "key" in path.attributes["ranges"][0]["lower_limit"]

        assert foreign_path_count == 2

        # Job input paths in JSON format.
        paths = json.loads(get_job_input_paths(job_ids[0], output_format="json"))
        assert len(paths) == 4

    @authors("psushin")
    def test_map_input_paths(self):
        create("table", "//tmp/in1")
        for i in xrange(0, 5, 2):
            write_table(
                "<append=true>//tmp/in1",
                [{"key": "%05d" % (i+j), "value": "foo"} for j in xrange(2)],
                sorted_by=["key"])

        create("table", "//tmp/in2")
        for i in xrange(3, 24, 2):
            write_table(
                "<append=true>//tmp/in2",
                [{"key": "%05d" % ((i+j) / 4), "value": "bar"} for j in xrange(2)],
                sorted_by=["key", "value"])

        create("table", "//tmp/out")
        in2 = '//tmp/in2["00001":"00004","00005":"00006"]'
        op = map(
            track=False,
            in_=["//tmp/in1", in2],
            out="//tmp/out",
            command="cat > {0}/$YT_JOB_ID && exit 1".format(self._tmpdir),
            spec={
                "mapper": {
                    "format": "dsv"
                },
                "job_count": 1,
                "max_failed_job_count": 1
            })
        with pytest.raises(YtError):
            op.track()

        job_ids = os.listdir(self._tmpdir)
        assert job_ids
        wait_for_data_in_job_archive(op.id, job_ids)

        assert len(job_ids) == 1
        expected = yson.loads("""[
            <ranges=[{lower_limit={row_index=0};upper_limit={row_index=6}}]>"//tmp/in1";
            <ranges=[
                {lower_limit={row_index=0;key=["00001"]};upper_limit={row_index=14;key=["00004"]}};
                {lower_limit={row_index=16;key=["00005"]};upper_limit={row_index=22;key=["00006"]}}
            ]>"//tmp/in2"]""")
        actual = yson.loads(get_job_input_paths(job_ids[0]))
        assert expected == actual

    @authors("ermolovd")
    def test_nonuser_job_type(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", [{"foo": i} for i in xrange(100)])
        op = sort(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            sort_by=["foo"])

        wait(lambda: get_job_rows_for_operation(op.id))

        rows = get_job_rows_for_operation(op.id)
        assert len(rows) == 1

        op_id = parts_to_uuid(
            id_lo=rows[0]["operation_id_lo"],
            id_hi=rows[0]["operation_id_hi"])

        assert op_id == op.id
        job_id = parts_to_uuid(
            id_lo=rows[0]["job_id_lo"],
            id_hi=rows[0]["job_id_hi"])

        with pytest.raises(YtError):
            get_job_input(job_id)

    @authors("ermolovd")
    def test_table_is_rewritten(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")

        write_table("//tmp/t_input", [{"foo": i} for i in xrange(100)])

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="cat > {0}/$YT_JOB_ID".format(self._tmpdir),
            spec = {
                "stderr_table_path": "//tmp/t_stderr",
            })

        job_ids = os.listdir(self._tmpdir)
        assert job_ids
        wait_for_data_in_job_archive(op.id, job_ids)

        old_chunk_list_id = get("//tmp/t_input/@chunk_list_id")
        write_table("//tmp/t_input", [{"bar": i} for i in xrange(100)])

        wait(lambda: not exists("#" + old_chunk_list_id))
        gc_collect()

        job_ids = os.listdir(self._tmpdir)
        assert job_ids
        for job_id in job_ids:
            input_file = os.path.join(self._tmpdir, job_id)
            with open(input_file) as inf:
                actual_input = inf.read()
            assert actual_input
            with pytest.raises(YtError):
                get_job_input(job_id)


    @authors("ermolovd")
    def test_wrong_spec_version(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("//tmp/t_input", [{"foo": i} for i in xrange(100)])

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="cat > {0}/$YT_JOB_ID".format(self._tmpdir))

        job_ids = os.listdir(self._tmpdir)
        assert job_ids
        wait_for_data_in_job_archive(op.id, job_ids)

        rows = get_job_spec_rows_for_jobs(job_ids)
        updated = []
        for r in rows:
            new_r = {}
            for key in ["job_id_hi", "job_id_lo"]:
                new_r[key] = r[key]
            new_r["spec"] = "junk"
            updated.append(new_r)
        insert_rows(OPERATION_JOB_SPEC_ARCHIVE_TABLE, updated, update=True, atomicity="none")

        job_ids = os.listdir(self._tmpdir)
        assert job_ids
        for job_id in job_ids:
            input_file = os.path.join(self._tmpdir, job_id)
            with open(input_file) as inf:
                actual_input = inf.read()
            assert actual_input
            with pytest.raises(YtError):
                get_job_input(job_id)

    @authors("ermolovd")
    def test_map_with_query(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")

        write_table("//tmp/t_input", [{"a": i, "b": i*3} for i in xrange(10)])

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="tee {0}/$YT_JOB_ID".format(self._tmpdir),
            spec={
                "input_query": "(a + b) as c where a > 0",
                "input_schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "b", "type": "int64"},
                ]})

        job_ids = os.listdir(self._tmpdir)
        assert job_ids
        wait_for_data_in_job_archive(op.id, job_ids)

        assert read_table("//tmp/t_output") == [{"c": i * 4} for i in xrange(1, 10)]

        job_ids = os.listdir(self._tmpdir)
        assert job_ids

        self.check_job_ids(job_ids)

    @authors("ermolovd")
    def test_bad_format_spec(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("//tmp/t_input", [{"a": i, "b": i*3} for i in xrange(10)])

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="tee {0}/$YT_JOB_ID".format(self._tmpdir),
            # NB. yamr format will throw error because it expects key/subkey/value fields
            spec={"mapper": {"format": "yamr"}, "max_failed_job_count": 1},
            track=False,
        )
        with raises_yt_error("Failed jobs limit exceeded"):
            op.track()

        job_info_list = list_jobs(op.id, state="failed")["jobs"]
        assert len(job_info_list) > 0
        job_id_list = []
        for job in job_info_list:
            assert job["state"] == "failed"
            job_id_list.append(job["id"])

        wait_for_data_in_job_archive(op.id, job_id_list)

        for job_id in job_id_list:
            with raises_yt_error("Failed to get job input"):
                get_job_input(job_id)


    @authors("iskhakovt")
    @pytest.mark.parametrize("successfull_jobs", [False, True])
    def test_archive_job_spec(self, successfull_jobs):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", [{"foo": i} for i in xrange(25)])

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="cat > {0}/$YT_JOB_ID; exit {1}".format(self._tmpdir, 0 if successfull_jobs else 1),
            spec={"data_size_per_job": 1, "max_failed_job_count": 25},
            track=False
        )

        if successfull_jobs:
            op.track()
        else:
            with pytest.raises(YtError):
                op.track()

        job_ids = os.listdir(self._tmpdir)

        if successfull_jobs:
            wait(lambda: len(get_job_spec_rows_for_jobs(job_ids)) == 10)
        else:
            # TODO(babenko): maybe stricter condition?
            wait(lambda: len(get_job_spec_rows_for_jobs(job_ids)) > 0)

class TestGetJobStderr(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_reporter": {
                "enabled": True,
                "reporting_period": 10,
                "min_repeat_delay": 10,
                "max_repeat_delay": 10,
            }
        },
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "operations_cleaner": {
                "enable": False,
                "analysis_period": 100,
                # Cleanup all operations
                "hard_retained_operation_count": 0,
                "clean_delay": 0,
            },
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
            "enable_job_stderr_reporter": True,
        }
    }

    def setup(self):
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client(), override_tablet_cell_bundle="default")

    def teardown(self):
        remove("//sys/operations_archive")

    def do_test_get_job_stderr(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        op = map(
            track=False,
            label="get_job_stderr",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("echo STDERR-OUTPUT >&2 ; BREAKPOINT ; cat"),
            spec={
                "mapper": {
                    "input_format": "json",
                    "output_format": "json"
                }
            })

        job_id = wait_breakpoint()[0]

        wait(lambda: retry(lambda: get_job_stderr(op.id, job_id)) == "STDERR-OUTPUT\n")
        release_breakpoint()
        op.track()
        res = get_job_stderr(op.id, job_id)
        assert res == "STDERR-OUTPUT\n"

        clean_operations()

        wait(lambda: get_job_stderr(op.id, job_id) == "STDERR-OUTPUT\n")

    @authors("ignat")
    def test_get_job_stderr(self):
        self.do_test_get_job_stderr()

    @authors("ignat")
    def test_get_job_stderr_without_cypress(self):
        instances = ls("//sys/controller_agents/instances")
        old_value = get("//sys/controller_agents/instances/{}/orchid/controller_agent/config/enable_cypress_job_nodes".format(instances[0]))
        set("//sys/controller_agents/config/enable_cypress_job_nodes", False, recursive=True)
        self.do_test_get_job_stderr()

    @authors("ignat")
    def test_get_job_stderr_acl(self):
        create_user("u")
        create_user("other")

        set("//sys/operations/@inherit_acl", False)

        try:
            create("table", "//tmp/t1", authenticated_user="u")
            create("table", "//tmp/t2", authenticated_user="u")
            write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}, {"foo": "lev"}], authenticated_user="u")

            op = map(
                track=False,
                label="get_job_stderr",
                in_="//tmp/t1",
                out="//tmp/t2",
                command=with_breakpoint("echo STDERR-OUTPUT >&2 ; BREAKPOINT ; cat"),
                spec={
                    "mapper": {
                        "input_format": "json",
                        "output_format": "json"
                    },
                    "data_weight_per_job": 1,
                },
                authenticated_user="u")

            job_ids = wait_breakpoint(job_count=3)
            job_id = job_ids[0]

            # We should use 'wait' since job can be still in prepare phase in the opinion of the node.
            wait(lambda: retry(lambda: get_job_stderr(op.id, job_id, authenticated_user="u")) == "STDERR-OUTPUT\n")
            with pytest.raises(YtError):
                get_job_stderr(op.id, job_id, authenticated_user="other")

            update_op_parameters(op.id, parameters={"acl": [make_ace("allow", "other", ["read", "manage"])]})

            release_breakpoint()
            op.track()

            def get_other_job_ids():
                return __builtin__.set(ls(op.get_path() + "/jobs")) - __builtin__.set(job_ids)

            wait(lambda: len(get_other_job_ids()) == 1)
            other_job_id = list(get_other_job_ids())[0]

            res = get_job_stderr(op.id, job_id, authenticated_user="u")
            assert res == "STDERR-OUTPUT\n"

            # NB: Operation has fixed ACL and we use it while job presented in cypress.
            # But archive contains old ACL.
            get_job_stderr(op.id, job_id, authenticated_user="other")

            clean_operations()

            wait(lambda: get_job_stderr(op.id, job_id, authenticated_user="u") == "STDERR-OUTPUT\n")
            with pytest.raises(YtError):
                get_job_stderr(op.id, job_id, authenticated_user="other")

            get_job_stderr(op.id, other_job_id, authenticated_user="other")
        finally:
            set("//sys/operations/@inherit_acl", True)

##################################################################

class TestGetJobInputRpcProxy(TestGetJobInput):
    DRIVER_BACKEND = "rpc"
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True

class TestGetJobStderrRpcProxy(TestGetJobStderr):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

