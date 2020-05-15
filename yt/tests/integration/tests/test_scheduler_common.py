from yt_env_setup import YTEnvSetup, unix_only, patch_porto_env_only, wait,\
    Restarter, SCHEDULERS_SERVICE, CONTROLLER_AGENTS_SERVICE,\
    get_porto_delta_node_config, porto_avaliable
from yt_commands import *
from yt_helpers import *

from yt.yson import *
from yt.wrapper import JsonFormat
from yt.common import date_string_to_timestamp, update

import pytest

import gzip
import time
from datetime import datetime

import __builtin__

import json

##################################################################

SCHEDULER_COMMON_NODE_CONFIG_PATCH = {
    "exec_agent": {
        "job_controller": {
            "resource_limits": {
                "user_slots": 5,
                "cpu": 5,
                "memory": 5 * 1024 ** 3,
            }
        }
    }
}

@pytest.mark.skip_if('not porto_avaliable()')
class TestSchedulerCommon(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True
    REQUIRE_YTSERVER_ROOT_PRIVILEGES = True

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_jobs_update_period": 10,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
            "operations_update_period": 10,
            "map_operation_options": {
                "job_splitter": {
                    "min_job_time": 5000,
                    "min_total_data_size": 1024,
                    "update_period": 100,
                    "candidate_percentile": 0.8,
                    "max_jobs_per_split": 3,
                },
            },
        }
    }

    DELTA_NODE_CONFIG = update(
        get_porto_delta_node_config(),
        SCHEDULER_COMMON_NODE_CONFIG_PATCH
    )
    USE_PORTO_FOR_SERVERS = True

    @authors("ignat")
    def test_failed_jobs_twice(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"} for _ in xrange(200)])

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command='trap "" HUP; bash -c "sleep 60" &; sleep $[( $RANDOM % 5 )]s; exit 42;',
            spec={"max_failed_job_count": 1, "job_count": 200})

        with pytest.raises(YtError):
            op.track()

        for job_desc in ls(op.get_path() + "/jobs", attributes=["error"]):
            print_debug(job_desc.attributes)
            print_debug(job_desc.attributes["error"]["inner_errors"][0]["message"])
            assert "Process exited with code " in job_desc.attributes["error"]["inner_errors"][0]["message"]

    @authors("ignat")
    def test_job_progress(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"} for _ in xrange(10)])

        op = map(
            track=False,
            label="job_progress",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"test_flag": to_yson_type("value", attributes={"attr": 0})})

        jobs = wait_breakpoint()
        progress = get(op.get_path() + "/controller_orchid/running_jobs/" + jobs[0] + "/progress")
        assert progress >= 0

        test_flag = get("//sys/scheduler/orchid/scheduler/operations/{0}/spec/test_flag".format(op.id))
        assert str(test_flag) == "value"
        assert test_flag.attributes == {"attr": 0}

        release_breakpoint()
        op.track()

    @authors("ermolovd")
    def test_job_stderr_size(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"} for _ in xrange(10)])

        op = map(
            track=False,
            label="job_progress",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("echo FOOBAR >&2 ; BREAKPOINT; cat"))

        jobs = wait_breakpoint()
        def get_stderr_size():
            return get(op.get_path() + "/controller_orchid/running_jobs/" + jobs[0] + "/stderr_size")
        wait(lambda: get_stderr_size() == len("FOOBAR\n"))

        release_breakpoint()
        op.track()

    @authors("ignat")
    def test_estimated_statistics(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"key": i} for i in xrange(5)])

        sort(in_="//tmp/t1", out="//tmp/t1", sort_by="key")
        op = map(command="cat", in_="//tmp/t1[:1]", out="//tmp/t2")

        statistics = get(op.get_path() + "/@progress/estimated_input_statistics")
        for key in ["uncompressed_data_size", "compressed_data_size", "row_count", "data_weight"]:
            assert statistics[key] > 0
        assert statistics["unavailable_chunk_count"] == 0
        assert statistics["chunk_count"] == 1

    @authors("ignat")
    def test_invalid_output_record(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"key": "foo", "value": "ninja"})

        command = """awk '($1=="foo"){print "bar"}'"""

        with pytest.raises(YtError):
            map(in_="//tmp/t1",
                out="//tmp/t2",
                command=command,
                spec={"mapper": {"format": "yamr"}})

    @authors("ignat")
    @unix_only
    def test_fail_context(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command='python -c "import os; os.read(0, 1);"',
            spec={"mapper": {"input_format": "dsv", "check_input_fully_consumed": True}})

        # If all jobs failed then operation is also failed
        with pytest.raises(YtError):
            op.track()

        jobs_path = op.get_path() + "/jobs"
        for job_id in ls(jobs_path):
            assert len(read_file(jobs_path + "/" + job_id + "/fail_context")) > 0
            assert read_file(jobs_path + "/" + job_id + "/fail_context") == get_job_fail_context(op.id, job_id)

    @authors("ignat")
    def test_dump_job_context(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            track=False,
            label="dump_job_context",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={
                "mapper": {
                    "input_format": "json",
                    "output_format": "json"
                }
            })

        jobs = wait_breakpoint()
        # Wait till job starts reading input
        wait(lambda: get(op.get_path() + "/controller_orchid/running_jobs/" + jobs[0] + "/progress") >= 0.5)

        dump_job_context(jobs[0], "//tmp/input_context")

        release_breakpoint()
        op.track()

        context = read_file("//tmp/input_context")
        assert get("//tmp/input_context/@description/type") == "input_context"
        assert JsonFormat().loads_row(context)["foo"] == "bar"

    @authors("ignat")
    def test_dump_job_context_permissions(self):
        create_user("abc")
        create("map_node", "//tmp/dir", attributes={"acl": [{"action": "deny", "subjects": ["abc"], "permissions": ["write"]}]})

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            track=False,
            label="dump_job_context",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={
                "mapper": {
                    "input_format": "json",
                    "output_format": "json"
                }
            },
            authenticated_user="abc")

        jobs = wait_breakpoint()
        # Wait till job starts reading input
        wait(lambda: get(op.get_path() + "/controller_orchid/running_jobs/" + jobs[0] + "/progress") >= 0.5)

        with pytest.raises(YtError):
            dump_job_context(jobs[0], "//tmp/dir/input_context", authenticated_user="abc")

        assert not exists("//tmp/dir/input_context")

        release_breakpoint()
        op.track()

    @authors("ignat")
    def test_large_spec(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", [{"a": "b"}])

        with pytest.raises(YtError):
            map(in_="//tmp/t1", out="//tmp/t2", command="cat", spec={"attribute": "really_large" * (2 * 10 ** 6)}, verbose=False)

    @authors("ignat")
    def test_job_with_exit_immediately_flag(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            track=False,
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command='set -e; /non_existed_command; echo stderr >&2;',
            spec={
                "max_failed_job_count": 1
            })

        with pytest.raises(YtError):
            op.track()

        jobs_path = op.get_path() + "/jobs"
        assert get(jobs_path + "/@count") == 1
        for job_id in ls(jobs_path):
            assert read_file(jobs_path + "/" + job_id + "/stderr") == \
                "/bin/bash: /non_existed_command: No such file or directory\n"

    @authors("ignat")
    def test_pipe_statistics(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output")

        statistics = get(op.get_path() + "/@progress/job_statistics")
        assert get_statistics(statistics, "user_job.pipes.input.bytes.$.completed.map.sum") == 15
        assert get_statistics(statistics, "user_job.pipes.output.0.bytes.$.completed.map.sum") == 15

    @authors("ignat")
    def test_writer_config(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out",
            attributes={
                "chunk_writer": {"block_size": 1024},
                "compression_codec": "none"
            })

        write_table("//tmp/t_in", [{"value": "A"*1024} for _ in xrange(10)])

        map(
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"job_count": 1})

        chunk_id = get_singular_chunk_id("//tmp/t_out")
        assert get("#" + chunk_id + "/@compressed_data_size") > 1024 * 10
        assert get("#" + chunk_id + "/@max_block_size") < 1024 * 2

    @authors("ignat")
    def test_invalid_schema_in_path(self):
        create("table", "//tmp/input")
        create("table", "//tmp/output")

        with pytest.raises(YtError):
            map(in_="//tmp/input",
                out="<schema=[{name=key; type=int64}; {name=key;type=string}]>//tmp/output",
                command="cat")

    @authors("ignat")
    def test_ypath_attributes_on_output_tables(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"a": "b" * 10000})

        for optimize_for in ["lookup", "scan"]:
            create("table", "//tmp/tout1_" + optimize_for)
            map(in_="//tmp/t1", out="<optimize_for={0}>//tmp/tout1_{0}".format(optimize_for), command="cat")
            assert get("//tmp/tout1_{}/@optimize_for".format(optimize_for)) == optimize_for

        for compression_codec in ["none", "lz4"]:
            create("table", "//tmp/tout2_" + compression_codec)
            map(in_="//tmp/t1", out="<compression_codec={0}>//tmp/tout2_{0}".format(compression_codec), command="cat")

            stats = get("//tmp/tout2_{}/@compression_statistics".format(compression_codec))
            assert compression_codec in stats, str(stats)
            assert stats[compression_codec]["chunk_count"] > 0

    @authors("ignat")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_unique_keys_validation(self, optimize_for):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2", attributes={
            "optimize_for": optimize_for,
            "schema": make_schema([
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"}],
                unique_keys=True)
            })

        for i in xrange(2):
            write_table("<append=true>//tmp/t1", {"key": "foo", "value": "ninja"})

        command = 'cat >/dev/null; echo "{key=1; value=one}"'

        with pytest.raises(YtError):
            map(
                in_="//tmp/t1",
                out="//tmp/t2",
                command=command,
                spec={"job_count": 2})

        command = 'cat >/dev/null; echo "{key=1; value=one}; {key=1; value=two}"'

        with pytest.raises(YtError):
            map(
                in_="//tmp/t1",
                out="//tmp/t2",
                command=command,
                spec={"job_count": 1})

    @authors("dakovalkov")
    def test_append_to_sorted_table_simple(self):
        create("table", "//tmp/sorted_table", attributes={
            "schema": make_schema([
                {"name": "key", "type": "int64", "sort_order": "ascending"}],
                unique_keys=False)
            })
        write_table("//tmp/sorted_table", [{"key": 1}, {"key": 5}, {"key": 10}])
        op = map(
            in_="//tmp/sorted_table",
            out="<append=%true>//tmp/sorted_table",
            command="echo '{key=30};{key=39}'",
            spec={"job_count": 1})

        assert read_table("//tmp/sorted_table") == [{"key": 1}, {"key": 5}, {"key": 10}, {"key": 30}, {"key": 39}]

    @authors("dakovalkov")
    def test_append_to_sorted_table_failed(self):
        create("table", "//tmp/sorted_table", attributes={
            "schema": make_schema([
                {"name": "key", "type": "int64", "sort_order": "ascending"}],
                unique_keys=False)
            });
        write_table("//tmp/sorted_table", [{"key": 1}, {"key": 5}, {"key": 10}])

        with pytest.raises(YtError):
            op = map(
                in_="//tmp/sorted_table",
                out="<append=%true>//tmp/sorted_table",
                command="echo '{key=7};{key=39}'",
                spec={"job_count": 1})

    @authors("dakovalkov")
    def test_append_to_sorted_table_unique_keys(self):
        create("table", "//tmp/sorted_table", attributes={
            "schema": make_schema([
                {"name": "key", "type": "int64", "sort_order": "ascending"}],
                unique_keys=False)
            });
        write_table("//tmp/sorted_table", [{"key": 1}, {"key": 5}, {"key": 10}])
        op = map(
            in_="//tmp/sorted_table",
            out="<append=%true>//tmp/sorted_table",
            command="echo '{key=10};{key=39}'",
            spec={"job_count": 1})

        assert read_table("//tmp/sorted_table") == [{"key": 1}, {"key": 5}, {"key": 10}, {"key": 10}, {"key": 39}]

    @authors("dakovalkov")
    def test_append_to_sorted_table_unique_keys_failed(self):
        create("table", "//tmp/sorted_table", attributes={
            "schema": make_schema([
                {"name": "key", "type": "int64", "sort_order": "ascending"}],
                unique_keys=True)
            });
        write_table("//tmp/sorted_table", [{"key": 1}, {"key": 5}, {"key": 10}])

        with pytest.raises(YtError):
            op = map(
                in_="//tmp/sorted_table",
                out="<append=%true>//tmp/sorted_table",
                command="echo '{key=10};{key=39}'",
                spec={"job_count": 1})

    @authors("dakovalkov")
    def test_append_to_sorted_table_empty_table(self):
        create("table", "//tmp/sorted_table", attributes={
            "schema": make_schema([
                {"name": "key", "type": "int64", "sort_order": "ascending"}],
                unique_keys=False)
            })
        create("table", "//tmp/t1")
        write_table("//tmp/t1", [{}])
        op = map(
            in_="//tmp/t1",
            out="<append=%true>//tmp/sorted_table",
            command="echo '{key=30};{key=39}'",
            spec={"job_count": 1})

        assert read_table("//tmp/sorted_table") == [{"key": 30}, {"key": 39}]

    @authors("dakovalkov")
    def test_append_to_sorted_table_empty_row_empty_table(self):
        create("table", "//tmp/sorted_table", attributes={
            "schema": make_schema([
                {"name": "key", "type": "int64", "sort_order": "ascending"}],
                unique_keys=False)
            })
        create("table", "//tmp/t1")
        write_table("//tmp/t1", [{}])
        op = map(
            in_="//tmp/t1",
            out="<append=%true>//tmp/sorted_table",
            command="echo '{ }'",
            spec={"job_count": 1})

        assert read_table("//tmp/sorted_table") == [{"key": YsonEntity()}]

    @authors("dakovalkov")
    def test_append_to_sorted_table_exclusive_lock(self):
        create("table", "//tmp/sorted_table", attributes={
            "schema": make_schema([
                {"name": "key", "type": "int64", "sort_order": "ascending"}],
                unique_keys=False)
            })
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/sorted_table", [{"key": 5}])
        write_table("//tmp/t1", [{"key": 6}, {"key":10}])
        write_table("//tmp/t2", [{"key": 8}, {"key": 12}])

        op = map(
            track=False,
            in_="//tmp/t1",
            out="<append=%true>//tmp/sorted_table",
            command="sleep 10; cat")

        time.sleep(5)

        with pytest.raises(YtError):
            map(in_="//tmp/t2",
                out="<append=%true>//tmp/sorted_table",
                command="cat")

    @authors("ignat")
    def test_many_parallel_operations(self):
        create("table", "//tmp/input")

        testing_options = {"scheduling_delay": 100}

        job_count = 20
        original_data = [{"index": i} for i in xrange(job_count)]
        write_table("//tmp/input", original_data)

        operation_count = 5
        ops = []
        for index in range(operation_count):
            output = "//tmp/output" + str(index)
            create("table", output)
            ops.append(
                map(in_="//tmp/input",
                    out=[output],
                    command="sleep 0.1; cat",
                    spec={"data_size_per_job": 1, "testing": testing_options},
                    track=False))

        failed_ops = []
        for index in range(operation_count):
            output = "//tmp/failed_output" + str(index)
            create("table", output)
            failed_ops.append(
                map(in_="//tmp/input",
                    out=[output],
                    command="sleep 0.1; exit 1",
                    spec={"data_size_per_job": 1, "max_failed_job_count": 1, "testing": testing_options},
                    track=False))

        for index, op in enumerate(failed_ops):
            "//tmp/failed_output" + str(index)
            with pytest.raises(YtError):
                op.track()

        for index, op in enumerate(ops):
            output = "//tmp/output" + str(index)
            op.track()
            assert sorted(read_table(output)) == original_data

        time.sleep(5)
        statistics = get("//sys/scheduler/orchid/monitoring/ref_counted/statistics")
        operation_objects = ["NYT::NScheduler::TOperation", "NYT::NScheduler::NVectorScheduler::TOperationElement",
                             "NYT::NScheduler::NClassicScheduler::TOperationElement"]
        records = [record for record in statistics if record["name"] in operation_objects]
        assert len(records) == 2
        assert records[0]["objects_alive"] == 0
        assert records[1]["objects_alive"] == 0

    @authors("ignat")
    def test_concurrent_fail(self):
        create("table", "//tmp/input")

        testing_options = {"scheduling_delay": 250}

        job_count = 1000
        original_data = [{"index": i} for i in xrange(job_count)]
        write_table("//tmp/input", original_data)

        create("table", "//tmp/output")
        with pytest.raises(YtError):
            map(in_="//tmp/input",
                out="//tmp/output",
                command="sleep 0.250; exit 1",
                spec={"data_size_per_job": 1, "max_failed_job_count": 10, "testing": testing_options})

    @authors("ignat")
    @unix_only
    def test_YT_5629(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        data = [{"a": i} for i in xrange(5)]
        write_table("//tmp/t1", data)

        map(in_="//tmp/t1", out="//tmp/t2", command="sleep 1; cat /proc/self/fd/0")

        assert read_table("//tmp/t2") == data

    @authors("ignat")
    def test_range_count_limit(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", {"key": "a", "value": "value"})

        def gen_table(range_count):
            return "<ranges=[" + ("{exact={row_index=0}};" * range_count) + "]>//tmp/in"

        map(in_=[gen_table(20)],
            out="//tmp/out",
            command="cat")

        with pytest.raises(YtError):
            map(in_=[gen_table(2000)],
                out="//tmp/out",
                command="cat")

    @authors("ignat")
    def test_complete_op(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in xrange(5):
            write_table("<append=true>//tmp/t1", {"key": str(i), "value": "foo"})

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("echo job_index=$YT_JOB_INDEX ; BREAKPOINT"),
            spec={
                "mapper": {
                    "format": "dsv"
                },
                "data_size_per_job": 1,
                "max_failed_job_count": 1
            })
        jobs = wait_breakpoint(job_count=5)

        for job_id in jobs[:3]:
            release_breakpoint(job_id=job_id)

        assert op.get_state() != "completed"
        wait(lambda: op.get_job_count("completed") >= 3)

        op.complete()
        assert op.get_state() == "completed"
        op.track()
        assert len(read_table("//tmp/t2")) == 3
        assert "operation_completed_by_user_request" in op.get_alerts()

    @authors("ignat")
    def test_abort_op(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"foo": "bar"})

        op = map(track=False,
            in_="//tmp/t",
            out="//tmp/t",
            command="sleep 1")

        op.abort()
        assert op.get_state() == "aborted"

    @authors("ignat")
    def test_input_with_custom_transaction(self):
        custom_tx = start_transaction(timeout=30000)

        create("table", "//tmp/in", tx=custom_tx)
        write_table("//tmp/in", {"foo": "bar"}, tx=custom_tx)

        create("table", "//tmp/out")

        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/in", out="//tmp/out")

        map(command="cat", in_='<transaction_id="{}">//tmp/in'.format(custom_tx), out="//tmp/out")

        assert list(read_table("//tmp/out")) == [{"foo": "bar"}]

    @authors("babenko")
    def test_input_created_in_user_transaction(self):
        custom_tx = start_transaction()
        create("table", "//tmp/in", tx=custom_tx)
        write_table("//tmp/in", {"foo": "bar"}, tx=custom_tx)
        create("table", "//tmp/out")
        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/in", out="//tmp/out")

    @authors("ignat")
    def test_nested_input_transactions(self):
        custom_tx = start_transaction(timeout=60000)

        create("table", "//tmp/in", tx=custom_tx)
        write_table("//tmp/in", {"foo": "bar"}, tx=custom_tx)

        create("table", "//tmp/out")

        op = map(
            track=False,
            command=with_breakpoint("BREAKPOINT; sleep 100"),
            in_='<transaction_id="{}">//tmp/in'.format(custom_tx),
            out="//tmp/out")

        wait_breakpoint()

        nested_input_transaction_ids = get(op.get_path() + "/@nested_input_transaction_ids")
        assert len(nested_input_transaction_ids) == 1
        nested_tx = nested_input_transaction_ids[0]

        assert list(read_table("//tmp/in", tx=nested_tx)) == [{"foo": "bar"}]
        assert get("#{}/@parent_id".format(nested_tx)) == custom_tx

        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        op.ensure_running()
        assert get(op.get_path() + "/@nested_input_transaction_ids") == [nested_tx]

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            abort_transaction(nested_tx)

        op.ensure_running()
        new_nested_input_transaction_ids = get(op.get_path() + "/@nested_input_transaction_ids")
        assert len(new_nested_input_transaction_ids) == 1
        assert new_nested_input_transaction_ids[0] != nested_tx

    @authors("ignat")
    def test_nested_input_transaction_duplicates(self):
        custom_tx = start_transaction(timeout=60000)

        create("table", "//tmp/in", tx=custom_tx)
        write_table("//tmp/in", {"foo": "bar"}, tx=custom_tx)

        create("table", "//tmp/out")

        op = map(
            track=False,
            command=with_breakpoint("BREAKPOINT; sleep 100"),
            in_=['<transaction_id="{}">//tmp/in'.format(custom_tx)] * 2,
            out="//tmp/out")

        wait_breakpoint()

        nested_input_transaction_ids = get(op.get_path() + "/@nested_input_transaction_ids")
        assert len(nested_input_transaction_ids) == 2
        assert nested_input_transaction_ids[0] == nested_input_transaction_ids[1]

        nested_tx = nested_input_transaction_ids[0]
        assert list(read_table("//tmp/in", tx=nested_tx)) == [{"foo": "bar"}]
        assert get("#{}/@parent_id".format(nested_tx)) == custom_tx

        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        op.ensure_running()
        assert get(op.get_path() + "/@nested_input_transaction_ids") == [nested_tx, nested_tx]

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            abort_transaction(nested_tx)

        op.ensure_running()
        new_nested_input_transaction_ids = get(op.get_path() + "/@nested_input_transaction_ids")
        assert len(new_nested_input_transaction_ids) == 2
        assert new_nested_input_transaction_ids[0] == new_nested_input_transaction_ids[1]
        assert new_nested_input_transaction_ids[0] != nested_tx

    @authors("babenko")
    def test_update_lock_transaction_timeout(self):
        lock_tx = get("//sys/scheduler/lock/@locks/0/transaction_id")
        new_timeout = get("#{}/@timeout".format(lock_tx)) + 1234
        set("//sys/scheduler/config/lock_transaction_timeout", new_timeout, recursive=True)
        wait(lambda: get("#{}/@timeout".format(lock_tx)) == new_timeout)

class TestSchedulerCommonMulticell(TestSchedulerCommon):
    NUM_SECONDARY_MASTER_CELLS = 2

##################################################################

class TestMultipleSchedulers(YTEnvSetup, PrepareTables):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 2

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 1000,
            "fair_share_update_period": 100,
            "profiling_update_period": 100,
            "testing_options": {
                "master_disconnect_delay": 3000,
            },
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
        }
    }

    def _get_scheduler_transation(self):
        while True:
            scheduler_locks = get("//sys/scheduler/lock/@locks", verbose=False)
            if len(scheduler_locks) > 0:
                scheduler_transaction = scheduler_locks[0]["transaction_id"]
                return scheduler_transaction
            time.sleep(0.01)

    @authors("ignat")
    def test_hot_standby(self):
        self._prepare_tables()

        op = map(track=False, in_="//tmp/t_in", out="//tmp/t_out", command="cat; sleep 5")

        op.wait_for_fresh_snapshot()

        transaction_id = self._get_scheduler_transation()

        def get_transaction_title(transaction_id):
            return get("#{0}/@title".format(transaction_id), verbose=False)

        title = get_transaction_title(transaction_id)

        while True:
            abort_transaction(transaction_id)

            new_transaction_id = self._get_scheduler_transation()
            new_title = get_transaction_title(new_transaction_id)
            if title != new_title:
                break

            title = new_title
            transaction_id = new_transaction_id
            time.sleep(0.3)

        op.track()

        assert read_table("//tmp/t_out") == [{"foo": "bar"}]

##################################################################

class TestSchedulerMaxChunkPerJob(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "map_operation_options": {
                "max_data_slices_per_job": 1,
            },
            "ordered_merge_operation_options": {
                "max_data_slices_per_job": 1,
            },
            "sorted_merge_operation_options": {
                "max_data_slices_per_job": 1,
            },
            "reduce_operation_options": {
                "max_data_slices_per_job": 1,
            },
        }
    }

    @authors("ignat")
    def test_max_data_slices_per_job(self):
        data = [{"foo": i} for i in xrange(5)]
        create("table", "//tmp/in1")
        create("table", "//tmp/in2")
        create("table", "//tmp/out")
        write_table("//tmp/in1", data, sorted_by="foo")
        write_table("//tmp/in2", data, sorted_by="foo")



        op = merge(mode="ordered", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out", spec={"force_transform": True})
        assert data + data == read_table("//tmp/out")

        # Must be 2 jobs since input has 2 chunks.
        assert get(op.get_path() + "/@progress/jobs/total") == 2

        op = map(command="cat >/dev/null", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out")
        assert get(op.get_path() + "/@progress/jobs/total") == 2

        op = merge(mode="sorted", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out")
        assert get(op.get_path() + "/@progress/jobs/total") == 2

        op = reduce(command="cat >/dev/null", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out", reduce_by=["foo"])
        assert get(op.get_path() + "/@progress/jobs/total") == 2

##################################################################

class TestSchedulerMaxChildrenPerAttachRequest(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "max_children_per_attach_request": 1,
        }
    }

    @authors("ignat")
    def test_max_children_per_attach_request(self):
        data = [{"foo": i} for i in xrange(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        map(command="cat", in_="//tmp/in", out="//tmp/out", spec={"data_size_per_job": 1})

        assert sorted(read_table("//tmp/out")) == sorted(data)
        assert get("//tmp/out/@row_count") == 3

    @authors("ignat")
    def test_max_children_per_attach_request_in_live_preview(self):
        data = [{"foo": i} for i in xrange(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        op = map(
            track=False,
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_="//tmp/in",
            out="//tmp/out",
            spec={"data_size_per_job": 1})

        jobs = wait_breakpoint(job_count=3)

        for job_id in jobs[:2]:
            release_breakpoint(job_id=job_id)

        for iter in xrange(100):
            jobs_exist = exists(op.get_path() + "/@progress/jobs")
            if jobs_exist:
                completed_jobs = get(op.get_path() + "/@progress/jobs/completed/total")
                if completed_jobs == 2:
                    break
            time.sleep(0.1)

        transaction_id = get(op.get_path() + "/@async_scheduler_transaction_id")
        wait(lambda: get(op.get_path() + "/output_0/@row_count", tx=transaction_id) == 2)

        release_breakpoint()
        op.track()

##################################################################

class TestSchedulerConfig(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "event_log": {
                "retry_backoff_time": 7,
                "flush_period": 5000
            },
        },
        "addresses": [
            ("ipv4", "127.0.0.1"),
            ("ipv6", "::1")
        ]
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "event_log": {
                "retry_backoff_time": 7,
                "flush_period": 5000
            },
            "operation_options": {
                "spec_template": {
                    "data_weight_per_job": 1000
                }
            },
            "map_operation_options": {
                "spec_template": {
                    "data_weight_per_job": 2000,
                    "max_failed_job_count": 10
                }
            },
            "environment": {
                "TEST_VAR": "10"
            },
        },
        "addresses": [
            ("ipv4", "127.0.0.1"),
            ("ipv6", "::1")
        ],
    }

    @authors("ignat")
    def test_basic(self):
        orchid_scheduler_config = "//sys/scheduler/orchid/scheduler/config"
        assert get("{0}/event_log/flush_period".format(orchid_scheduler_config)) == 5000
        assert get("{0}/event_log/retry_backoff_time".format(orchid_scheduler_config)) == 7

        set("//sys/scheduler/config", {"event_log": {"flush_period": 10000}})
        time.sleep(2)

        assert get("{0}/event_log/flush_period".format(orchid_scheduler_config)) == 10000
        assert get("{0}/event_log/retry_backoff_time".format(orchid_scheduler_config)) == 7

        set("//sys/scheduler/config", {})
        time.sleep(2)

        assert get("{0}/event_log/flush_period".format(orchid_scheduler_config)) == 5000
        assert get("{0}/event_log/retry_backoff_time".format(orchid_scheduler_config)) == 7

    @authors("ignat")
    def test_adresses(self):
        adresses = get("//sys/scheduler/@addresses")
        assert adresses["ipv4"].startswith("127.0.0.1:")
        assert adresses["ipv6"].startswith("::1:")

    @authors("ignat")
    def test_specs(self):
        create("table", "//tmp/t_in")
        write_table("<append=true;sorted_by=[foo]>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")

        op = map(command="sleep 1000", in_=["//tmp/t_in"], out="//tmp/t_out", track=False)
        wait(lambda: exists(op.get_path() + "/@full_spec"))
        # XXX(ignat)
        for spec_type in ("full_spec",):
            assert get(op.get_path() + "/@{}/data_weight_per_job".format(spec_type)) == 2000
            assert get("//sys/scheduler/orchid/scheduler/operations/{0}/{1}/data_weight_per_job".format(op.id, spec_type)) == 2000
            assert get("//sys/scheduler/orchid/scheduler/operations/{0}/{1}/max_failed_job_count".format(op.id, spec_type)) == 10
        op.abort()

        op = reduce(command="sleep 1000", in_=["//tmp/t_in"], out="//tmp/t_out", reduce_by=["foo"], track=False)
        wait(lambda: op.get_state() == "running")
        time.sleep(1)
        # XXX(ignat)
        for spec_type in ("full_spec",):
            assert get(op.get_path() + "/@{}/data_weight_per_job".format(spec_type)) == 1000
            assert get("//sys/scheduler/orchid/scheduler/operations/{0}/{1}/data_weight_per_job".format(op.id, spec_type)) == 1000
            assert get("//sys/scheduler/orchid/scheduler/operations/{0}/{1}/max_failed_job_count".format(op.id, spec_type)) == 10

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        op.ensure_running()

        # XXX(ignat)
        for spec_type in ("full_spec",):
            assert get(op.get_path() + "/@{}/data_weight_per_job".format(spec_type)) == 1000
            assert get("//sys/scheduler/orchid/scheduler/operations/{0}/{1}/data_weight_per_job".format(op.id, spec_type)) == 1000
            assert get("//sys/scheduler/orchid/scheduler/operations/{0}/{1}/max_failed_job_count".format(op.id, spec_type)) == 10

        op.abort()

    @authors("ignat")
    def test_unrecognized_spec(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"a": "b"}])
        create("table", "//tmp/t_out")
        op = map(command="sleep 1000", in_=["//tmp/t_in"], out="//tmp/t_out", track=False, spec={"xxx": "yyy"})

        wait(lambda: exists(op.get_path() + "/@unrecognized_spec"))
        assert get(op.get_path() + "/@unrecognized_spec") == {"xxx": "yyy"}

    @authors("ignat")
    def test_brief_progress(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"a": "b"}])
        create("table", "//tmp/t_out")
        op = map(command="sleep 1000", in_=["//tmp/t_in"], out="//tmp/t_out", track=False)

        wait(lambda: exists(op.get_path() + "/@brief_progress"))
        assert "jobs" in list(get(op.get_path() + "/@brief_progress"))

    @authors("ignat")
    def test_cypress_config(self):
        create("table", "//tmp/t_in")
        write_table("<append=true>//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")

        op = map(command="cat", in_=["//tmp/t_in"], out="//tmp/t_out")
        assert get(op.get_path() + "/@full_spec/data_weight_per_job") == 2000
        assert get(op.get_path() + "/@full_spec/max_failed_job_count") == 10

        set("//sys/controller_agents/config", {
            "map_operation_options": {"spec_template": {"max_failed_job_count": 50}},
            "environment": {"OTHER_VAR": "20"},
        })

        instances = ls("//sys/controller_agents/instances")
        for instance in instances:
            config_path = "//sys/controller_agents/instances/{0}/orchid/controller_agent/config".format(instance)
            wait(lambda: exists(config_path + "/environment/OTHER_VAR") and get(config_path + "/environment/OTHER_VAR") == "20")

            environment = get(config_path + "/environment")
            assert environment["TEST_VAR"] == "10"
            assert environment["OTHER_VAR"] == "20"

            assert get(config_path + "/map_operation_options/spec_template/max_failed_job_count".format(instance)) == 50

        op = map(command="cat", in_=["//tmp/t_in"], out="//tmp/t_out")
        assert get(op.get_path() + "/@full_spec/data_weight_per_job") == 2000
        assert get(op.get_path() + "/@full_spec/max_failed_job_count") == 50

##################################################################

class TestSchedulerOperationSnapshots(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "max_concurrent_controller_schedule_job_calls": 1,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
            "operation_controller_suspend_timeout": 2000,
        }
    }

    @authors("ignat")
    def test_snapshots(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])
        create("table", "//tmp/out")

        testing_options = {"scheduling_delay": 500}

        op = map(
            track=False,
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_="//tmp/in",
            out="//tmp/out",
            spec={"data_weight_per_job": 1, "testing": testing_options})

        snapshot_path = op.get_path() + "/snapshot"
        wait(lambda: exists(snapshot_path))

        # This is done to avoid read failures due to snapshot file rewriting.
        snapshot_backup_path = snapshot_path + ".backup"
        copy(snapshot_path, snapshot_backup_path)
        assert len(read_file(snapshot_backup_path, verbose=False)) > 0

        ts_str = get(op.get_path() + "/controller_orchid/progress/last_successful_snapshot_time")
        assert time.time() - date_string_to_timestamp(ts_str) < 60

        release_breakpoint()
        op.track()

    @authors("ignat")
    def test_parallel_snapshots(self):
        create("table", "//tmp/input")

        testing_options = {"scheduling_delay": 100}

        job_count = 1
        original_data = [{"index": i} for i in xrange(job_count)]
        write_table("//tmp/input", original_data)

        operation_count = 5
        ops = []
        for index in range(operation_count):
            output = "//tmp/output" + str(index)
            create("table", output)
            ops.append(
                map(track=False,
                    command=with_breakpoint("cat ; BREAKPOINT"),
                    in_="//tmp/input",
                    out=[output],
                    spec={"data_size_per_job": 1, "testing": testing_options}))

        for op in ops:
            snapshot_path = op.get_path() + "/snapshot"
            wait(lambda: exists(snapshot_path))

            snapshot_backup_path = snapshot_path + ".backup"
            copy(snapshot_path, snapshot_backup_path)
            assert len(read_file(snapshot_backup_path, verbose=False)) > 0

        # All our operations use 'default' breakpoint so we release it and all operations continue execution.
        release_breakpoint()

        for op in ops:
            op.track()

    @authors("ignat")
    def test_suspend_time_limit(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        create("table", "//tmp/out1")
        create("table", "//tmp/out2")

        while True:
            op2 = map(
                track=False,
                command="cat",
                in_="//tmp/in",
                out="//tmp/out2",
                spec={"data_size_per_job": 1, "testing": {"delay_inside_suspend": 15000}})

            time.sleep(2)
            snapshot_path2 = op2.get_path() + "/snapshot"
            if exists(snapshot_path2):
                op2.abort()
                continue
            else:
                break

        op1 = map(
            track=False,
            command="sleep 10; cat",
            in_="//tmp/in",
            out="//tmp/out1",
            spec={"data_size_per_job": 1})

        snapshot_path1 = op1.get_path() + "/snapshot"
        snapshot_path2 = op2.get_path() + "/snapshot"

        wait(lambda: exists(snapshot_path1))
        assert not exists(snapshot_path2)

        op1.track()
        op2.track()

##################################################################

class TestSchedulerHeterogeneousConfiguration(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @classmethod
    def modify_node_config(cls, config):
        if not hasattr(cls, "node_counter"):
            cls.node_counter = 0
        cls.node_counter += 1
        if cls.node_counter == 1:
            config["exec_agent"]["job_controller"]["resource_limits"]["user_slots"] = 0

    @authors("renadeen", "ignat")
    def test_job_count(self):
        data = [{"foo": i} for i in xrange(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        assert get("//sys/scheduler/orchid/scheduler/cell/resource_limits/user_slots") == 2
        assert get("//sys/scheduler/orchid/scheduler/cell/resource_usage/user_slots") == 0

        assert get("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/resource_limits/user_slots") == 2
        assert get("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/resource_usage/user_slots") == 0

        op = map(
            track=False,
            command="sleep 100",
            in_="//tmp/in",
            out="//tmp/out",
            spec={"data_size_per_job": 1, "locality_timeout": 0})

        time.sleep(2)

        assert get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/resource_usage/user_slots".format(op.id)) == 2
        assert get("//sys/scheduler/orchid/scheduler/cell/resource_limits/user_slots") == 2
        assert get("//sys/scheduler/orchid/scheduler/cell/resource_usage/user_slots") == 2

        assert get("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/resource_limits/user_slots") == 2
        assert get("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/resource_usage/user_slots") == 2

###############################################################################################

class TestSchedulerJobStatistics(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "scheduler_connector": {
                "heartbeat_period": 100  # 100 msec
            }
        }
    }

    def _create_table(self, table):
        create("table", table)
        set(table + "/@replication_factor", 1)

    @authors("ignat")
    def test_scheduler_job_by_id(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out")
        write_table("//tmp/in", [{"foo": i} for i in xrange(10)])

        op = map(
            track=False,
            label="scheduler_job_statistics",
            in_="//tmp/in",
            out="//tmp/out",
            command=with_breakpoint("BREAKPOINT ; cat"))

        wait_breakpoint()
        running_jobs = op.get_running_jobs()
        job_id = running_jobs.keys()[0]
        job_info = running_jobs.values()[0]

        # Check that /jobs is accessible only with direct job id.
        with pytest.raises(YtError):
            get("//sys/scheduler/orchid/scheduler/jobs")
        with pytest.raises(YtError):
            ls("//sys/scheduler/orchid/scheduler/jobs")

        job_info2 = get("//sys/scheduler/orchid/scheduler/jobs/{0}".format(job_id))
        # Check that job_info2 contains all the keys that are in job_info (do not check the same
        # for values because values could actually change between two get requests).
        for key in job_info:
            assert key in job_info2

    @authors("ignat")
    def test_scheduler_job_statistics(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out")
        write_table("//tmp/in", [{"foo": i} for i in xrange(10)])

        op = map(
            track=False,
            label="scheduler_job_statistics",
            in_="//tmp/in",
            out="//tmp/out",
            command=with_breakpoint("cat ; BREAKPOINT"))

        wait_breakpoint()
        running_jobs = op.get_running_jobs()
        job_id = running_jobs.keys()[0]

        statistics_appeared = False
        for iter in xrange(300):
            statistics = get("//sys/scheduler/orchid/scheduler/jobs/{0}/statistics".format(job_id))
            data = statistics.get("data", {})
            _input = data.get("input", {})
            row_count = _input.get("row_count", {})
            _sum = row_count.get("sum", 0)
            if _sum == 10:
                statistics_appeared = True
                break
            time.sleep(0.1)

        assert statistics_appeared

        traffic_statistics = statistics["job_proxy"]["traffic"]
        assert traffic_statistics["inbound"]["from_"]["sum"] > 0
        assert traffic_statistics["duration_ms"]["sum"] > 0
        assert traffic_statistics["_to_"]["sum"] > 0

        release_breakpoint()
        op.track()

##################################################################

class DISABLED_TestSchedulerOperationStorage(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500
        }
    }

    @authors("asaitgalin")
    def test_revive(self):
        create("table", "//tmp/t_input")
        write_table("//tmp/t_input", [{"x": "y"}, {"a": "b"}])

        cmd = """
if [ "$YT_JOB_INDEX" == "0"  ]; then
    exit 1
else
    sleep 1000; cat
fi
"""

        ops = []
        for i in xrange(2):
            create("table", "//tmp/t_output_" + str(i))

            op = map(
                command=cmd,
                in_="//tmp/t_input",
                out="//tmp/t_output_" + str(i),
                spec={
                    "data_size_per_job": 1,
                    "enable_compatible_storage_mode": i == 0
                },
                track=False)

            state_path = "//sys/scheduler/orchid/scheduler/operations/{0}/state".format(op.id)
            wait(lambda: get(state_path) == "running")

            ops.append(op)

        for op in ops:
            wait(lambda: op.get_job_count("failed") == 1)

        for op in ops:
            op.wait_for_fresh_snapshot()

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        for op in ops:
            wait(lambda: get("//sys/scheduler/orchid/scheduler/operations/{}/state".format(op.id)) == "running")
            wait(lambda: get(op.get_path() + "/@state") == "running")
            assert op.get_job_count("failed") == 1

    @authors("asaitgalin")
    @pytest.mark.parametrize("use_owners", [False, True])
    def test_attributes(self, use_owners):
        create_user("u")
        create("table", "//tmp/t_input")
        write_table("//tmp/t_input", [{"x": "y"}, {"a": "b"}])

        create("table", "//tmp/t_output")

        cmd = """
if [ "$YT_JOB_INDEX" == "0"  ]; then
    exit 1
else
    sleep 1000; cat
fi
"""

        spec = {"data_size_per_job": 1}
        if use_owners:
            spec["owners"] = ["u"]
        else:
            spec["acl"] = [make_ace("allow", "u", ["read", "manage"])]

        op = map(
            command=cmd,
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec=spec,
            track=False,
        )

        state_path = "//sys/scheduler/orchid/scheduler/operations/{0}/state".format(op.id)
        wait(lambda: get(state_path) == "running", ignore_exceptions=True)
        time.sleep(1.0)  # Give scheduler some time to dump attributes to cypress.

        assert get(op.get_path() + "/@state") == "running"
        assert get("//sys/operations/" + op.id + "/@state") == "running"
        complete_op(op.id, authenticated_user="u")
        # NOTE: This attribute is moved to hash buckets unconditionally in all modes.
        assert not exists("//sys/operations/" + op.id + "/@committed")
        assert exists(op.get_path() + "/@committed")

    @authors("asaitgalin")
    def test_inner_operation_nodes(self):
        create("table", "//tmp/t_input")
        write_table("<append=%true>//tmp/t_input", [{"key": "value"} for i in xrange(2)])
        create("table", "//tmp/t_output")

        cmd = """
if [ "$YT_JOB_INDEX" == "0"  ]; then
    python -c "import os; os.read(0, 1)"
    echo "Oh no!" >&2
    exit 1
else
    echo "abacaba"
    sleep 1000
fi
"""

        def _run_op():
            op = map(
                command=cmd,
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "data_size_per_job": 1
                },
                track=False)

            wait(lambda: op.get_job_count("failed") == 1 and op.get_job_count("running") >= 1)

            time.sleep(1.0)

            return op

        get_async_scheduler_tx_path = lambda op: "//sys/operations/" + op.id + "/@async_scheduler_transaction_id"
        get_async_scheduler_tx_path_new = lambda op: op.get_path() + "/@async_scheduler_transaction_id"

        get_output_path_new = lambda op: op.get_path() + "/output_0"

        get_stderr_path = lambda op, job_id: "//sys/operations/" + op.id + "/jobs/" + job_id + "/stderr"
        get_stderr_path_new = lambda op, job_id: op.get_path() + "/jobs/" + job_id + "/stderr"

        get_fail_context_path = lambda op, job_id: "//sys/operations/" + op.id + "/jobs/" + job_id + "/fail_context"
        get_fail_context_path_new = lambda op, job_id: op.get_path() + "/jobs/" + job_id + "/fail_context"

        # Compatible mode or simple hash buckets mode.
        op = _run_op()
        assert exists(get_async_scheduler_tx_path(op))
        assert exists(get_async_scheduler_tx_path_new(op))
        async_tx_id = get(get_async_scheduler_tx_path(op))
        assert exists(get_output_path_new(op), tx=async_tx_id)

        jobs = ls("//sys/operations/" + op.id + "/jobs")
        assert len(jobs) == 1
        assert exists(get_fail_context_path_new(op, jobs[0]))
        assert exists(get_fail_context_path(op, jobs[0]))
        assert read_file(get_stderr_path(op, jobs[0])) == "Oh no!\n"
        assert read_file(get_stderr_path_new(op, jobs[0])) == "Oh no!\n"

    @authors("ignat")
    def test_rewrite_operation_path(self):
        get_stderr_path = lambda op, job_id: "//sys/operations/" + op.id + "/jobs/" + job_id + "/stderr"

        create("table", "//tmp/t_input")
        write_table("//tmp/t_input", [{"x": "y"}, {"a": "b"}])

        create("table", "//tmp/t_output")

        op = map(
            command="echo 'XYZ' >&2",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "enable_compatible_storage_mode": False,
            })

        assert not exists("//sys/operations/" + op.id + "/@")
        assert exists("//sys/operations/" + op.id + "/@", rewrite_operation_path=True)
        assert get("//sys/operations/" + op.id + "/@id", rewrite_operation_path=True) == \
            get(op.get_path() + "/@id", rewrite_operation_path=True)

        tx = start_transaction()
        assert lock("//sys/operations/" + op.id, rewrite_operation_path=True, mode="snapshot", tx=tx)

        jobs = ls("//sys/operations/" + op.id + "/jobs", rewrite_operation_path=True)
        assert read_file(get_stderr_path(op, jobs[0]), rewrite_operation_path=True) == "XYZ\n"

##################################################################

class TestNewLivePreview(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 3

    @authors("max42")
    def test_new_live_preview_simple(self):
        data = [{"foo": i} for i in range(3)]

        create("table", "//tmp/t1")
        write_table("//tmp/t1", data)

        create("table", "//tmp/t2")

        op = map(
            wait_for_jobs=True,
            track=False,
            command=with_breakpoint("BREAKPOINT ; cat"),
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"data_size_per_job": 1})

        jobs = wait_breakpoint(job_count=2)

        assert exists(op.get_path() + "/controller_orchid")

        release_breakpoint(job_id=jobs[0])
        release_breakpoint(job_id=jobs[1])
        wait(lambda: op.get_job_count("completed") == 2)

        live_preview_data = read_table(op.get_path() + "/controller_orchid/data_flow_graph/vertices/map/live_previews/0")
        assert len(live_preview_data) == 2

        assert all(record in data for record in live_preview_data)

    @authors("max42")
    def test_new_live_preview_intermediate_data_acl(self):
        create_user("u1")
        create_user("u2")

        data = [{"foo": i} for i in range(3)]

        create("table", "//tmp/t1")
        write_table("//tmp/t1", data)

        create("table", "//tmp/t2")

        op = map(
            wait_for_jobs=True,
            track=False,
            command=with_breakpoint("BREAKPOINT ; cat"),
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "data_size_per_job": 1,
                "acl": [make_ace("allow", "u1", "read")],
            },
        )

        jobs = wait_breakpoint(job_count=2)

        assert exists(op.get_path() + "/controller_orchid")

        release_breakpoint(job_id=jobs[0])
        release_breakpoint(job_id=jobs[1])
        wait(lambda: op.get_job_count("completed") == 2)

        read_table(op.get_path() + "/controller_orchid/data_flow_graph/vertices/map/live_previews/0", authenticated_user="u1")

        with pytest.raises(YtError):
            read_table(op.get_path() + "/controller_orchid/data_flow_graph/vertices/map/live_previews/0", authenticated_user="u2")

    @authors("max42")
    def test_new_live_preview_ranges(self):
        create("table", "//tmp/t1")
        for i in range(3):
            write_table("<append=%true>//tmp/t1", [{"a": i}])

        create("table", "//tmp/t2")

        op = map_reduce(
            wait_for_jobs=True,
            track=False,
            mapper_command='for ((i=0; i<3; i++)); do echo "{a=$(($YT_JOB_INDEX*3+$i))};"; done',
            reducer_command=with_breakpoint("cat; BREAKPOINT"),
            reduce_by="a",
            sort_by=["a"],
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"map_job_count": 3, "partition_count": 1})

        wait(lambda: op.get_job_count("completed") == 3)

        assert exists(op.get_path() + "/controller_orchid")

        live_preview_path = op.get_path() + "/controller_orchid/data_flow_graph/vertices/partition_map/live_previews/0"
        live_preview_data = read_table(live_preview_path)

        assert len(live_preview_data) == 9

        # We try all possible combinations of chunk and row index ranges and check that everything works as expected.
        expected_all_ranges_data = []
        all_ranges = []
        for lower_row_index in range(10) + [None]:
            for upper_row_index in range(10) + [None]:
                for lower_chunk_index in range(4) + [None]:
                    for upper_chunk_index in range(4) + [None]:
                        lower_limit = dict()
                        real_lower_index = 0
                        if not lower_row_index is None:
                            lower_limit["row_index"] = lower_row_index
                            real_lower_index = max(real_lower_index, lower_row_index)
                        if not lower_chunk_index is None:
                            lower_limit["chunk_index"] = lower_chunk_index
                            real_lower_index = max(real_lower_index, lower_chunk_index * 3)

                        upper_limit = dict()
                        real_upper_index = 9
                        if not upper_row_index is None:
                            upper_limit["row_index"] = upper_row_index
                            real_upper_index = min(real_upper_index, upper_row_index)
                        if not upper_chunk_index is None:
                            upper_limit["chunk_index"] = upper_chunk_index
                            real_upper_index = min(real_upper_index, upper_chunk_index * 3)

                        all_ranges.append({"lower_limit": lower_limit, "upper_limit": upper_limit})
                        expected_all_ranges_data += [live_preview_data[real_lower_index:real_upper_index]]

        all_ranges_path = "<" + yson.dumps({"ranges": all_ranges}, yson_type="map_fragment", yson_format="text") + ">" + live_preview_path

        all_ranges_data = read_table(all_ranges_path, verbose=False)

        position = 0
        for i, range_ in enumerate(expected_all_ranges_data):
            if all_ranges_data[position:position + len(range_)] != range_:
                print_debug("position =", position, ", range =", all_ranges[i])
                print_debug("expected:", range_)
                print_debug("actual:", all_ranges_data[position:position + len(range_)])
                assert all_ranges_data[position:position + len(range_)] == range_
            position += len(range_)

        release_breakpoint()
        op.track()

    @authors("max42")
    def test_disabled_live_preview(self):
        create_user("robot-root")
        add_member("robot-root", "superusers")

        data = [{"foo": i} for i in range(3)]

        create("table", "//tmp/t1")
        write_table("//tmp/t1", data)

        create("table", "//tmp/t2")

        # Run operation with given params and return a tuple (live preview created, suppression alert set)
        def check_live_preview(enable_legacy_live_preview=None, authenticated_user=None, index=None):
            op = map(
                wait_for_jobs=True,
                track=False,
                command=with_breakpoint("BREAKPOINT ; cat", breakpoint_name=str(index)),
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={"data_size_per_job": 1, "enable_legacy_live_preview": enable_legacy_live_preview},
                authenticated_user=authenticated_user)

            wait_breakpoint(job_count=2, breakpoint_name=str(index))

            async_transaction_id = get(op.get_path() + "/@async_scheduler_transaction_id")
            live_preview_created = exists(op.get_path() + "/output_0", tx=async_transaction_id)
            suppression_alert_set = "legacy_live_preview_suppressed" in op.get_alerts()

            op.abort()

            return (live_preview_created, suppression_alert_set)

        combinations = [
            (None, "root", True, False),
            (True, "root", True, False),
            (False, "root", False, False),
            (None, "robot-root", False, True),
            (True, "robot-root", True, False),
            (False, "robot-root", False, False)
        ]

        for i, combination in enumerate(combinations):
            enable_legacy_live_preview, authenticated_user, live_preview_created, suppression_alert_set = combination
            assert check_live_preview(enable_legacy_live_preview=enable_legacy_live_preview,
                                      authenticated_user=authenticated_user,
                                      index=i) == (live_preview_created, suppression_alert_set)

##################################################################

class TestConnectToMaster(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 0

    @authors("max42")
    def test_scheduler_doesnt_connect_to_master_in_safe_mode(self):
        set("//sys/@config/enable_safe_mode", True)
        self.Env.kill_schedulers()
        self.Env.start_schedulers(sync=False)
        time.sleep(1)

        wait(lambda: self.has_safe_mode_error_in_log())

    def has_safe_mode_error_in_log(self):
        for line in gzip.open(self.path_to_run + "/logs/scheduler-0.log.gz"):
            if "Error connecting to master" in line and "Cluster is in safe mode" in line:
                return True
        return False

##################################################################

class TestEventLog(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    REQUIRE_YTSERVER_ROOT_PRIVILEGES = True
    USE_PORTO_FOR_SERVERS = True

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "event_log": {
                "flush_period": 1000
            }
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "event_log": {
                "flush_period": 1000
            }
        }
    }

    DELTA_NODE_CONFIG = get_porto_delta_node_config()

    @authors("ignat")
    def test_scheduler_event_log(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"}])
        op = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command='cat; bash -c "for (( I=0 ; I<=100*1000 ; I++ )) ; do echo $(( I+I*I )); done; sleep 2" >/dev/null')

        statistics = get(op.get_path() + "/@progress/job_statistics")
        assert get_statistics(statistics, "user_job.cpu.user.$.completed.map.sum") > 0
        assert get_statistics(statistics, "user_job.block_io.bytes_read.$.completed.map.sum") is not None
        assert get_statistics(statistics, "user_job.current_memory.rss.$.completed.map.count") > 0
        assert get_statistics(statistics, "user_job.max_memory.$.completed.map.count") > 0
        assert get_statistics(statistics, "user_job.cumulative_memory_mb_sec.$.completed.map.count") > 0
        assert get_statistics(statistics, "job_proxy.cpu.user.$.completed.map.count") == 1
        assert get_statistics(statistics, "job_proxy.cpu.user.$.completed.map.count") == 1

        # wait for scheduler to dump the event log
        def check():
            res = read_table("//sys/scheduler/event_log")
            event_types = __builtin__.set()
            for item in res:
                event_types.add(item["event_type"])
                if item["event_type"] == "job_completed":
                    stats = item["statistics"]
                    user_time = get_statistics(stats, "user_job.cpu.user")
                    # our job should burn enough cpu
                    if  user_time == 0:
                        return False
                if item["event_type"] == "job_started":
                    limits = item["resource_limits"]
                    if limits["cpu"] == 0:
                        return False
                    if limits["user_memory"] == 0:
                        return False
                    if limits["user_slots"] == 0:
                        return False
            if "operation_started" not in event_types:
                return False
            return True
        wait(check)

    @authors("ignat")
    def test_scheduler_event_log_buffering(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"}])

        for node in ls("//sys/cluster_nodes"):
            set("//sys/cluster_nodes/{0}/@banned".format(node), True)

        time.sleep(2)
        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat")
        time.sleep(2)

        for node in ls("//sys/cluster_nodes"):
            set("//sys/cluster_nodes/{0}/@banned".format(node), False)

        op.track()

        def check():
            res = read_table("//sys/scheduler/event_log")
            event_types = __builtin__.set([item["event_type"] for item in res])
            for event in ["scheduler_started", "operation_started", "operation_completed"]:
                if event not in event_types:
                    return False
            return True
        wait(check)

    @authors("mrkastep")
    def test_structured_event_log(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"}])

        op = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat")

        # Let's wait until scheduler dumps the information on our map operation
        def check_event_log():
            event_log = read_table("//sys/scheduler/event_log")
            for event in event_log:
                if event["event_type"] == "operation_completed" and event["operation_id"] == op.id:
                    return True
            return False

        wait(check_event_log)

        event_log = read_table("//sys/scheduler/event_log")

        def check_structured():
            def extract_event_log(filename):
                with open(filename) as f:
                    items = [json.loads(line) for line in f]
                    events = list(filter(lambda e: "event_type" in e, items))
                    return events

            scheduler_log_file = self.path_to_run + "/logs/scheduler-0.json.log"
            controller_agent_log_file = self.path_to_run + "/logs/controller-agent-0.json.log"

            structured_log = extract_event_log(scheduler_log_file) + extract_event_log(controller_agent_log_file)

            for normal_event in event_log:
                flag = False
                for structured_event in structured_log:
                    def key(event):
                        return (event["timestamp"],
                                event["event_type"],
                                event["operation_id"] if "operation_id" in event else "")

                    if key(normal_event) == key(structured_event):
                        flag = True
                        break
                if not flag:
                    return False
            return True

        wait(check_structured)

##################################################################

@patch_porto_env_only(YTEnvSetup)
class TestJobStatisticsPorto(YTEnvSetup):
    NUM_SCHEDULERS = 1
    DELTA_NODE_CONFIG = get_porto_delta_node_config()
    USE_PORTO_FOR_SERVERS = True

    @authors("babenko")
    def test_statistics(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"}])
        op = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command='cat; bash -c "for (( I=0 ; I<=100*1000 ; I++ )) ; do echo $(( I+I*I )); done; sleep 2" >/dev/null')

        statistics = get(op.get_path() + "/@progress/job_statistics")

        for component in ["user_job", "job_proxy"]:
            print(component)
            assert get_statistics(statistics, component + ".cpu.user.$.completed.map.sum") > 0
            assert get_statistics(statistics, component + ".cpu.system.$.completed.map.sum") > 0
            assert get_statistics(statistics, component + ".cpu.context_switches.$.completed.map.sum") is not None
            assert get_statistics(statistics, component + ".cpu.wait.$.completed.map.sum") is not None
            assert get_statistics(statistics, component + ".cpu.throttled.$.completed.map.sum") is not None
            assert get_statistics(statistics, component + ".block_io.bytes_read.$.completed.map.sum") is not None
            assert get_statistics(statistics, component + ".max_memory.$.completed.map.sum") > 0

        assert get_statistics(statistics, "user_job.cumulative_memory_mb_sec.$.completed.map.sum") > 0

##################################################################

class TestResourceMetering(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 5
    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "default_abc_id": 42,
            "resource_metering_period": 1000,
        }
    }

    @authors("mrkastep")
    def test_resource_metering_log(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"}])

        create_pool_tree("yggdrasil", wait_for_orchid=False)
        set("//sys/pool_trees/default/@nodes_filter", "!other")
        set("//sys/pool_trees/yggdrasil/@nodes_filter", "other")

        nodes = ls("//sys/cluster_nodes")
        for node in nodes[:3]:
            set("//sys/cluster_nodes/" + node + "/@user_tags/end", "other")

        create_pool("pixies",
                    pool_tree="yggdrasil",
                    attributes={
                        "min_share_resources": {"cpu": 3},
                        "abc": {"id": 1, "slug": "pixies", "name": "pixies"}
                    },
                    wait_for_orchid=False)

        create_pool("francis",
                    pool_tree="yggdrasil",
                    attributes={
                        "min_share_resources": {"cpu": 1},
                        "abc": {"id": 2, "slug": "francis", "name": "pixies"}
                    },
                    parent_name="pixies",
                    wait_for_orchid=False)

        root_key = (42, "yggdrasil", "<Root>")

        desired_cpu_limits = {
            (1, "yggdrasil", "pixies"): 2,
            (2, "yggdrasil", "francis"): 1,
        }

        def check_structured():
            def extract_metering_log(filename):
                with open(filename) as f:
                    items = [json.loads(line) for line in f]
                    events = list(filter(lambda e: "event_type" not in e, items))
                    return events

            scheduler_log_file = os.path.join(self.path_to_run, "logs/scheduler-0.json.log")

            structured_log = extract_metering_log(scheduler_log_file)

            last_reports = {}

            for entry in structured_log:
                if "abc_id" not in entry:
                    continue

                key = (entry["abc_id"], entry["tree_id"], entry["pool"])
                last_reports[key] = entry["min_share_resources"]["cpu"]

            if root_key not in last_reports:
                return False

            for key, value in desired_cpu_limits.items():
                if key not in last_reports or last_reports[key] != value:
                    return False

            return True

        wait(check_structured)


