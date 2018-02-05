from yt_env_setup import wait, YTEnvSetup
from yt_commands import *
import yt.environment.init_operation_archive as init_operation_archive
from yt.wrapper.common import uuid_hash_pair

import __builtin__
import datetime
import itertools
import pytest
import shutil

FORMAT_LIST = [
    "yson",
    "json",
    "dsv",
    "yamr",
]

OPERATION_JOB_ARCHIVE_TABLE = "//sys/operations_archive/jobs"
OPERATION_JOB_SPEC_ARCHIVE_TABLE = "//sys/operations_archive/job_specs"

def get_stderr_dict_from_table(table_path):
    result = {}
    stderr_rows = read_table("//tmp/t_stderr")
    for job_id, part_iter in itertools.groupby(stderr_rows, key=lambda x: x["job_id"]):
        job_stderr = ""
        for row in part_iter:
            job_stderr += row["data"]
        result[job_id] = job_stderr
    return result

def get_job_rows_for_operation(operation_id):
    hash_pair = uuid_hash_pair(operation_id)
    return select_rows("* from [{0}] where operation_id_lo = {1}u and operation_id_hi = {2}u".format(
        OPERATION_JOB_ARCHIVE_TABLE,  hash_pair.lo, hash_pair.hi))

def get_job_spec_rows_for_job(job_id_list):
    def generate_keys(job_id):
        pair = uuid_hash_pair(job_id)
        return {
            "job_id_hi": pair.hi,
            "job_id_lo": pair.lo}

    return lookup_rows(OPERATION_JOB_SPEC_ARCHIVE_TABLE, [generate_keys(job_id) for job_id in job_id_list])

def check_all_jobs_in_operation_archive(job_id_list, table):
    rows = select_rows("job_id_hi,job_id_lo from [{0}]".format(table))
    job_ids_in_archive = __builtin__.set(get_guid_from_parts(r["job_id_lo"], r["job_id_hi"]) for r in rows)
    return all(job_id in job_ids_in_archive for job_id in job_id_list)

def wait_data_in_operation_table_archive(job_id_list):
    wait(lambda: check_all_jobs_in_operation_archive(job_id_list, OPERATION_JOB_ARCHIVE_TABLE))
    wait(lambda: check_all_jobs_in_operation_archive(job_id_list, OPERATION_JOB_SPEC_ARCHIVE_TABLE))

class TestGetJobInput(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "statistics_reporter": {
                "enabled": True,
                "reporting_period": 10,
                "min_repeat_delay": 10,
                "max_repeat_delay": 10,
            }
        },
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
        },
    }

    def setup(self):
        self._tmpdir = create_tmpdir("inputs")
        self.sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client())

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
            dont_track=True,
            spec={"job_count": 1})

        events_on_fs().wait_event("job_is_running")

        job_id_list = os.listdir(self._tmpdir)
        assert len(job_id_list) == 1

        self.check_job_ids(job_id_list)

        events_on_fs().notify_event("job_can_finish")
        op.track()

    def test_map_complete(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("//tmp/t_input", [{"foo": i} for i in xrange(100)])

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="cat > {0}/$YT_JOB_ID".format(self._tmpdir),
            format="yson")

        job_id_list = os.listdir(self._tmpdir)
        assert job_id_list
        wait_data_in_operation_table_archive(job_id_list)

        self.check_job_ids(job_id_list)

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
            dont_track=True)

        events_on_fs().wait_event("reducer_almost_complete", timeout=datetime.timedelta(300))

        job_id_list = os.listdir(self._tmpdir)
        assert len(job_id_list) >= 3
        wait_data_in_operation_table_archive(job_id_list)

        self.check_job_ids(job_id_list)

        events_on_fs().notify_event("continue_reducer")
        op.track()

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

        job_id_list = os.listdir(self._tmpdir)
        assert job_id_list
        wait_data_in_operation_table_archive(job_id_list)

        job_id_list = os.listdir(self._tmpdir)
        assert job_id_list

        self.check_job_ids(job_id_list)

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

        op_id = get_guid_from_parts(
            lo=rows[0]["operation_id_lo"],
            hi=rows[0]["operation_id_hi"])

        assert op_id == op.id
        job_id = get_guid_from_parts(
            lo=rows[0]["job_id_lo"],
            hi=rows[0]["job_id_hi"])

        with pytest.raises(YtError):
            get_job_input(job_id)

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

        job_id_list = os.listdir(self._tmpdir)
        assert job_id_list
        wait_data_in_operation_table_archive(job_id_list)

        write_table("//tmp/t_input", [{"bar": i} for i in xrange(100)])

        gc_collect()

        job_id_list = os.listdir(self._tmpdir)
        assert job_id_list
        for job_id in job_id_list:
            input_file = os.path.join(self._tmpdir, job_id)
            with open(input_file) as inf:
                actual_input = inf.read()
            assert actual_input
            with pytest.raises(YtError):
                get_job_input(job_id)


    def test_wrong_spec_version(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("//tmp/t_input", [{"foo": i} for i in xrange(100)])

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="cat > {0}/$YT_JOB_ID".format(self._tmpdir))

        job_id_list = os.listdir(self._tmpdir)
        assert job_id_list
        wait_data_in_operation_table_archive(job_id_list)

        rows = get_job_spec_rows_for_job(job_id_list)
        updated = []
        for r in rows:
            new_r = {}
            for key in ["job_id_hi", "job_id_lo"]:
                new_r[key] = r[key]
            new_r["spec"] = "junk"
            updated.append(new_r)
        insert_rows(OPERATION_JOB_SPEC_ARCHIVE_TABLE, updated, update=True, atomicity="none")

        job_id_list = os.listdir(self._tmpdir)
        assert job_id_list
        for job_id in job_id_list:
            input_file = os.path.join(self._tmpdir, job_id)
            with open(input_file) as inf:
                actual_input = inf.read()
            assert actual_input
            with pytest.raises(YtError):
                get_job_input(job_id)

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

        job_id_list = os.listdir(self._tmpdir)
        assert job_id_list
        wait_data_in_operation_table_archive(job_id_list)

        assert read_table("//tmp/t_output") == [{"c": i * 4} for i in xrange(1, 10)]

        job_id_list = os.listdir(self._tmpdir)
        assert job_id_list

        self.check_job_ids(job_id_list)
