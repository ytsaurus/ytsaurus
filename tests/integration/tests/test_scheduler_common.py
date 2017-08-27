from yt_env_setup import YTEnvSetup, unix_only, patch_porto_env_only, wait
from yt_commands import *

from yt.yson import *
from yt.wrapper import JsonFormat

from flaky import flaky

import pytest
import time
import __builtin__
import sys

##################################################################

# This is a mix of options for 18.4 and 18.5
cgroups_delta_node_config = {
    "exec_agent": {
        "enable_cgroups": True,                                       # <= 18.4
        "supported_cgroups": ["cpuacct", "blkio", "memory", "cpu"],   # <= 18.4
        "slot_manager": {
            "enforce_job_control": True,                              # <= 18.4
            "job_environment": {
                "type": "cgroups",                                   # >= 18.5
                "supported_cgroups": [                                # >= 18.5
                    "cpuacct",
                    "blkio",
                    "memory",
                    "cpu"],
            },
        }
    }
}

porto_delta_node_config = {
    "exec_agent": {
        "slot_manager": {
            # <= 18.4
            "enforce_job_control": True,
            "job_environment": {
                # >= 19.2
                "type": "porto",
            },
        }
    }
}

##################################################################

def get_pool_metrics(metric_key):
    result = {}
    for entry in reversed(get("//sys/scheduler/orchid/profiling/scheduler/pools/metrics/{0}".format(metric_key))):
        pool = entry["tags"]["pool"]
        if pool not in result:
            result[pool] = entry["value"]
    return result

def get_cypress_metrics(operation_id, key):
    statistics = get("//sys/operations/{0}/@progress/job_statistics".format(operation_id))
    return get_statistics(statistics, "{0}.$.completed.map.sum".format(key))

##################################################################

class PrepareTables(object):
    def _create_table(self, table):
        create("table", table)
        set(table + "/@replication_factor", 1)

    def _prepare_tables(self):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        self._create_table("//tmp/t_out")

##################################################################

class TestEventLog(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "event_log": {
                "flush_period": 1000
            }
        }
    }

    DELTA_NODE_CONFIG = cgroups_delta_node_config

    def test_scheduler_event_log(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"}])
        op = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command='cat; bash -c "for (( I=0 ; I<=100*1000 ; I++ )) ; do echo $(( I+I*I )); done; sleep 2" >/dev/null')

        statistics = get("//sys/operations/{0}/@progress/job_statistics".format(op.id))
        assert get_statistics(statistics, "user_job.cpu.user.$.completed.map.sum") > 0
        assert get_statistics(statistics, "user_job.block_io.bytes_read.$.completed.map.sum") is not None
        assert get_statistics(statistics, "user_job.current_memory.rss.$.completed.map.count") > 0
        assert get_statistics(statistics, "user_job.max_memory.$.completed.map.count") > 0
        assert get_statistics(statistics, "user_job.cumulative_memory_mb_sec.$.completed.map.count") > 0
        assert get_statistics(statistics, "job_proxy.cpu.user.$.completed.map.count") == 1
        assert get_statistics(statistics, "job_proxy.cpu.user.$.completed.map.count") == 1

        # wait for scheduler to dump the event log
        time.sleep(2)
        res = read_table("//sys/scheduler/event_log")
        event_types = __builtin__.set()
        for item in res:
            event_types.add(item["event_type"])
            if item["event_type"] == "job_completed":
                stats = item["statistics"]
                user_time = get_statistics(stats, "user_job.cpu.user")
                # our job should burn enough cpu
                assert user_time > 0
            if item["event_type"] == "job_started":
                limits = item["resource_limits"]
                assert limits["cpu"] > 0
                assert limits["memory"] > 0
                assert limits["user_slots"] > 0
        assert "operation_started" in event_types

    def test_scheduler_event_log_buffering(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"}])

        for node in ls("//sys/nodes"):
            set("//sys/nodes/{0}/@banned".format(node), True)

        time.sleep(2)
        op = map(
            dont_track=True,
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat")
        time.sleep(2)

        for node in ls("//sys/nodes"):
            set("//sys/nodes/{0}/@banned".format(node), False)

        op.track()

        time.sleep(2)
        res = read_table("//sys/scheduler/event_log")
        event_types = __builtin__.set([item["event_type"] for item in res])
        for event in ["scheduler_started", "operation_started", "operation_completed"]:
            assert event in event_types


##################################################################

@patch_porto_env_only(TestEventLog)
class TestEventLogPorto(YTEnvSetup):
    DELTA_NODE_CONFIG = porto_delta_node_config
    USE_PORTO_FOR_SERVERS = True

##################################################################

class TestSchedulerControllerThrottling(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "schedule_job_time_limit": 100,
            "operations_update_period": 10
        }
    }

    def test_time_based_throttling(self):
        create("table", "//tmp/input")

        testing_options = {"scheduling_delay": 200}

        data = [{"foo": i} for i in range(5)]
        write_table("//tmp/input", data)

        create("table", "//tmp/output")
        op = map(
            dont_track=True,
            in_="//tmp/input",
            out="//tmp/output",
            command="cat",
            spec={"testing": testing_options})

        while True:
            try:
                jobs = get("//sys/operations/{0}/@progress/jobs".format(op.id), verbose=False)
                assert jobs["running"] == 0
                assert jobs["completed"]["total"] == 0
                if jobs["aborted"]["non_scheduled"]["scheduling_timeout"] > 0:
                    break
            except:
                pass
            time.sleep(1)

        op.abort()

##################################################################

@unix_only
class TestJobStderr(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 16
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_jobs_update_period": 10,
            "map_operation_options": {
                "job_splitter": {
                    "min_job_time": 5000,
                    "min_total_data_size": 1024,
                    "update_period": 100,
                    "median_excess_duration": 3000,
                    "candidate_percentile": 0.8,
                    "max_jobs_per_split": 3,
                },
            },
        }
    }

    DELTA_NODE_CONFIG = {
        "tablet_manager": {
            "error_backoff_time": 0
        }
    }

    def test_stderr_ok(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        command = """cat > /dev/null; echo stderr 1>&2; echo {operation='"'$YT_OPERATION_ID'"'}';'; echo {job_index=$YT_JOB_INDEX};"""

        op = map(in_="//tmp/t1", out="//tmp/t2", command=command)

        assert read_table("//tmp/t2") == [{"operation": op.id}, {"job_index": 0}]
        check_all_stderrs(op, "stderr\n", 1)

    def test_stderr_failed(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        command = """echo "{x=y}{v=};{a=b}"; while echo xxx 2>/dev/null; do false; done; echo stderr 1>&2; cat > /dev/null;"""

        op = map(dont_track=True, in_="//tmp/t1", out="//tmp/t2", command=command)

        # If all jobs failed then operation is also failed
        with pytest.raises(YtError):
            op.track()

        check_all_stderrs(op, "stderr\n", 10)

    def test_stderr_limit(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            dont_track=True,
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat > /dev/null; echo stderr 1>&2; exit 125",
            spec={"max_failed_job_count": 5})

        # If all jobs failed then operation is also failed
        with pytest.raises(YtError):
            op.track()

        check_all_stderrs(op, "stderr\n", 5)

    def test_stderr_max_size(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat > /dev/null; python -c 'print \"head\" + \"0\" * 10000000; print \"1\" * 10000000 + \"tail\"' >&2;",
            spec={"max_failed_job_count": 1, "mapper": {"max_stderr_size": 1000000}})

        jobs_path = "//sys/operations/{0}/jobs".format(op.id)
        assert get(jobs_path + "/@count") == 1
        stderr_path = "{0}/{1}/stderr".format(jobs_path, ls(jobs_path)[0])
        stderr = read_file(stderr_path, verbose=False).strip()

        # Stderr buffer size is equal to 1000000, we should add it to limit
        assert len(stderr) <= 4000000
        assert stderr[:1004] == "head" + "0" * 1000
        assert stderr[-1004:] == "1" * 1000 + "tail"
        assert "skipped" in stderr

    def test_stderr_chunks_not_created_for_completed_jobs(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"row_id": "row_" + str(i)} for i in xrange(100)])

        # One job hangs, so that we can poke into transaction.
        command = """
                if [ "$YT_JOB_INDEX" -eq 1 ]; then
                    sleep 1000
                else
                    cat > /dev/null; echo message > /dev/stderr
                fi;"""

        op = map(
            dont_track=True,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=command,
            spec={"job_count": 10, "max_stderr_count": 0})

        def enough_jobs_completed():
            progress = get("//sys/operations/{0}/@brief_progress".format(op.id))
            if "jobs" in progress and "completed" in progress["jobs"]:
                return progress["jobs"]["completed"]["total"] > 8
            return False

        wait(enough_jobs_completed)

        stderr_tx = get("//sys/operations/{}/@async_scheduler_transaction_id".format(op.id))
        staged_objects = get("//sys/transactions/{}/@staged_object_ids".format(stderr_tx))
        assert sum(len(ids) for ids in staged_objects.values()) == 0, str(staged_objects)

        op.abort()

    def test_stderr_of_failed_jobs(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"row_id": "row_" + str(i)} for i in xrange(110)])

        # All jobs with index < 109 will successfuly finish on "exit 0;"
        # The job with index 109 will be waiting because of wait_for_jobs=True
        # until it is manualy resumed.
        command = """grep -v row_109 > /dev/null;
                IS_FAILING_JOB=$?;
                echo stderr 1>&2;
                if [ $IS_FAILING_JOB -eq 1 ]; then
                    trap "exit 125" EXIT
                else
                    exit 0;
                fi;"""

        op = map(
            dont_track=True,
            wait_timeout=120,
            wait_for_jobs=True,
            label="stderr_of_failed_jobs",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=command,
            spec={"max_failed_job_count": 1, "max_stderr_count": 100, "job_count": 110})

        with pytest.raises(YtError):
            op.resume_jobs()
            op.track()

        # The default number of stderr is 100. We check that we have 101-st stderr of failed job,
        # that is last one.
        check_all_stderrs(op, "stderr\n", 101)

    def test_stderr_with_missing_tmp_quota(self):
        create_account("test_account")

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"} for _ in xrange(5)])

        op = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat > /dev/null; echo 'stderr' >&2;",
            spec={"max_failed_job_count": 1, "job_node_account": "test_account"})
        check_all_stderrs(op, "stderr\n", 1)

        multicell_sleep()
        resource_usage = get("//sys/accounts/test_account/@resource_usage")
        assert resource_usage["node_count"] >= 2
        assert resource_usage["chunk_count"] >= 1
        assert resource_usage["disk_space_per_medium"].get("default", 0) > 0

        jobs = ls("//sys/operations/{0}/jobs".format(op.id))
        assert get("//sys/operations/{0}/jobs/{1}/@recursive_resource_usage".format(op.id, jobs[0])) == resource_usage

        set("//sys/accounts/test_account/@resource_limits/chunk_count", 0)
        set("//sys/accounts/test_account/@resource_limits/node_count", 0)
        op = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat > /dev/null; echo 'stderr' >&2;",
            spec={"max_failed_job_count": 1, "job_node_account": "test_account"})
        check_all_stderrs(op, "stderr\n", 0)

##################################################################

class TestJobStderrMulticell(TestJobStderr):
    NUM_SECONDARY_MASTER_CELLS = 2

##################################################################

@patch_porto_env_only(TestJobStderr)
class TestJobStderrPorto(YTEnvSetup):
    DELTA_NODE_CONFIG = porto_delta_node_config
    USE_PORTO_FOR_SERVERS = True

##################################################################

class TestUserFiles(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 16
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_jobs_update_period": 10,
            "map_operation_options": {
                "job_splitter": {
                    "min_job_time": 5000,
                    "min_total_data_size": 1024,
                    "update_period": 100,
                    "median_excess_duration": 3000,
                    "candidate_percentile": 0.8,
                    "max_jobs_per_split": 3,
                },
            },
        }
    }

    DELTA_NODE_CONFIG = {
        "tablet_manager": {
            "error_backoff_time": 0
        }
    }

    def test_file_with_integer_name(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("//tmp/t_input", [{"hello": "world"}])

        file = "//tmp/1000"
        create("file", file)
        write_file(file, "{value=42};\n")

        map(in_="//tmp/t_input",
            out=["//tmp/t_output"],
            command="cat 1000 >&2; cat",
            file=[file],
            verbose=True)

        assert read_table("//tmp/t_output") == [{"hello": "world"}]

    def test_file_with_subdir(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("//tmp/t_input", [{"hello": "world"}])

        file = "//tmp/test_file"
        create("file", file)
        write_file(file, "{value=42};\n")

        map(in_="//tmp/t_input",
            out=["//tmp/t_output"],
            command="cat dir/my_file >&2; cat",
            file=[to_yson_type("//tmp/test_file", attributes={"file_name": "dir/my_file"})],
            verbose=True)

        with pytest.raises(YtError):
            map(in_="//tmp/t_input",
                out=["//tmp/t_output"],
                command="cat dir/my_file >&2; cat",
                file=[to_yson_type("//tmp/test_file", attributes={"file_name": "../dir/my_file"})],
                spec={"max_failed_job_count": 1},
                verbose=True)

        assert read_table("//tmp/t_output") == [{"hello": "world"}]

    @unix_only
    def test_with_user_files(self):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        file1 = "//tmp/some_file.txt"
        file2 = "//tmp/renamed_file.txt"
        file3 = "//tmp/link_file.txt"

        create("file", file1)
        create("file", file2)

        write_file(file1, "{value=42};\n")
        write_file(file2, "{a=b};\n")
        link(file2, file3)

        create("table", "//tmp/table_file")
        write_table("//tmp/table_file", {"text": "info", "other": "trash"})

        map(in_="//tmp/input",
            out="//tmp/output",
            command="cat > /dev/null; cat some_file.txt; cat my_file.txt; cat table_file;",
            file=[file1, "<file_name=my_file.txt>" + file2, "<format=yson; columns=[text]>//tmp/table_file"])

        assert read_table("//tmp/output") == [{"value": 42}, {"a": "b"}, {"text": "info"}]

        map(in_="//tmp/input",
            out="//tmp/output",
            command="cat > /dev/null; cat link_file.txt; cat my_file.txt;",
            file=[file3, "<file_name=my_file.txt>" + file3])

        assert read_table("//tmp/output") == [{"a": "b"}, {"a": "b"}]

        with pytest.raises(YtError):
            map(in_="//tmp/input",
                out="//tmp/output",
                command="cat",
                file=["<format=invalid_format>//tmp/table_file"])

        # missing format
        with pytest.raises(YtError):
            map(in_="//tmp/input",
                out="//tmp/output",
                command="cat",
                file=["//tmp/table_file"])


    @unix_only
    def test_empty_user_files(self):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        file1 = "//tmp/empty_file.txt"
        create("file", file1)

        table_file = "//tmp/table_file"
        create("table", table_file)

        command = "cat > /dev/null; cat empty_file.txt; cat table_file"

        map(in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=[file1, "<format=yamr>" + table_file])

        assert read_table("//tmp/output") == []

    @unix_only
    def test_multi_chunk_user_files(self):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        file1 = "//tmp/regular_file"
        create("file", file1)
        write_file(file1, "{value=42};\n")
        set(file1 + "/@compression_codec", "lz4")
        write_file("<append=true>" + file1, "{a=b};\n")

        table_file = "//tmp/table_file"
        create("table", table_file)
        write_table(table_file, {"text": "info"})
        set(table_file + "/@compression_codec", "snappy")
        write_table("<append=true>" + table_file, {"text": "info"})

        command = "cat > /dev/null; cat regular_file; cat table_file"

        map(in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=[file1, "<format=yson>" + table_file])

        assert read_table("//tmp/output") == [{"value": 42}, {"a": "b"}, {"text": "info"}, {"text": "info"}]

    def test_erasure_user_files(self):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        create("file", "//tmp/regular_file", attributes={"erasure_coded": "lrc_12_2_2"})
        write_file("<append=true>//tmp/regular_file", "{value=42};\n")
        write_file("<append=true>//tmp/regular_file", "{a=b};\n")

        create("table", "//tmp/table_file", attributes={"erasure_codec": "reed_solomon_6_3"})
        write_table("<append=true>//tmp/table_file", {"text1": "info1"})
        write_table("<append=true>//tmp/table_file", {"text2": "info2"})

        command = "cat > /dev/null; cat regular_file; cat table_file"

        map(in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=["//tmp/regular_file", "<format=yson>//tmp/table_file"])

        assert read_table("//tmp/output") == [{"value": 42}, {"a": "b"}, {"text1": "info1"}, {"text2": "info2"}]

##################################################################

class TestUserFilesMulticell(TestUserFiles):
    NUM_SECONDARY_MASTER_CELLS = 2

##################################################################

@patch_porto_env_only(TestUserFiles)
class TestUserFilesPorto(YTEnvSetup):
    DELTA_NODE_CONFIG = porto_delta_node_config
    USE_PORTO_FOR_SERVERS = True

##################################################################

class TestSchedulerOperationNodeFlush(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10
        }
    }

    @unix_only
    def test_stderr_flush(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", {"foo": "bar"})

        ops = []
        for i in range(20):
            create("table", "//tmp/out" + str(i))
            op = map(
                dont_track=True,
                in_="//tmp/in",
                out="//tmp/out" + str(i),
                command="cat > /dev/null; echo stderr 1>&2; exit 125",
                spec={"max_failed_job_count": 1})
            ops += [op]

        for op in ops:
            # if all jobs failed then operation is also failed
            with pytest.raises(YtError):
                op.track()
            check_all_stderrs(op, "stderr\n", 1)

##################################################################

class TestSchedulerCommon(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 16
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_jobs_update_period": 10,
            "map_operation_options": {
                "job_splitter": {
                    "min_job_time": 5000,
                    "min_total_data_size": 1024,
                    "update_period": 100,
                    "median_excess_duration": 3000,
                    "candidate_percentile": 0.8,
                    "max_jobs_per_split": 3,
                },
            },
        }
    }

    DELTA_NODE_CONFIG = cgroups_delta_node_config

    def test_failed_jobs_twice(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"} for _ in xrange(200)])

        op = map(
            dont_track=True,
            in_="//tmp/t1",
            out="//tmp/t2",
            command='trap "" HUP; bash -c "sleep 60" &; sleep $[( $RANDOM % 5 )]s; exit 42;',
            spec={"max_failed_job_count": 1, "job_count": 200})

        with pytest.raises(YtError):
            op.track()

        for job_desc in ls("//sys/operations/{0}/jobs".format(op.id), attributes=["error"]):
            print >>sys.stderr, job_desc.attributes
            print >>sys.stderr, job_desc.attributes["error"]["inner_errors"][0]["message"]
            assert "Process exited with code " in job_desc.attributes["error"]["inner_errors"][0]["message"]

    def test_job_progress(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"} for _ in xrange(10)])

        op = map(
            dont_track=True,
            wait_for_jobs=True,
            label="job_progress",
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat",
            spec={"test_flag": to_yson_type("value", attributes={"attr": 0})})

        progress = get("//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs/{1}/progress".format(op.id, op.jobs[0]))
        assert progress >= 0

        test_flag = get("//sys/scheduler/orchid/scheduler/operations/{0}/spec/test_flag".format(op.id))
        assert str(test_flag) == "value"
        assert test_flag.attributes == {"attr": 0}

        op.resume_jobs()
        op.track()

    def test_estimated_statistics(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"key": i} for i in xrange(5)])

        sort(in_="//tmp/t1", out="//tmp/t1", sort_by="key")
        op = map(command="cat", in_="//tmp/t1[:1]", out="//tmp/t2")

        statistics = get("//sys/operations/{0}/@progress/estimated_input_statistics".format(op.id))
        for key in ["uncompressed_data_size", "compressed_data_size", "row_count", "data_weight"]:
            assert statistics[key] > 0
        assert statistics["unavailable_chunk_count"] == 0
        assert statistics["chunk_count"] == 1

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

    @unix_only
    def test_fail_context(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            dont_track=True,
            in_="//tmp/t1",
            out="//tmp/t2",
            command='python -c "import os; os.read(0, 1);"',
            spec={"mapper": {"input_format": "dsv", "check_input_fully_consumed": True}})

        # If all jobs failed then operation is also failed
        with pytest.raises(YtError):
            op.track()

        jobs_path = "//sys/operations/" + op.id + "/jobs"
        for job_id in ls(jobs_path):
            assert len(read_file(jobs_path + "/" + job_id + "/fail_context")) > 0

    def test_dump_job_context(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            dont_track=True,
            wait_for_jobs=True,
            label="dump_job_context",
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat",
            spec={
                "mapper": {
                    "input_format": "json",
                    "output_format": "json"
                }
            })

        # Wait till job starts reading input
        progress_path = "//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs/{1}/progress".format(op.id, op.jobs[0])
        while get(progress_path) < 0.5:
            time.sleep(1)

        dump_job_context(op.jobs[0], "//tmp/input_context")

        op.resume_jobs()
        op.track()

        context = read_file("//tmp/input_context")
        assert get("//tmp/input_context/@description/type") == "input_context"
        assert JsonFormat(process_table_index=True).loads_row(context)["foo"] == "bar"

    def test_large_spec(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", [{"a": "b"}])

        with pytest.raises(YtError):
            map(in_="//tmp/t1", out="//tmp/t2", command="cat", spec={"attribute": "really_large" * (2 * 10 ** 6)}, verbose=False)

    # NB(psushin): remove flaky flag from 19.2 and further.
    @flaky(max_runs=5)
    def test_job_with_exit_immediately_flag(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            dont_track=True,
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command='set -e; /non_existed_command; echo stderr >&2;',
            spec={
                "max_failed_job_count": 1
            })

        with pytest.raises(YtError):
            op.track()

        jobs_path = "//sys/operations/" + op.id + "/jobs"
        assert get(jobs_path + "/@count") == 1
        for job_id in ls(jobs_path):
            assert read_file(jobs_path + "/" + job_id + "/stderr") == \
                "/bin/bash: /non_existed_command: No such file or directory\n"

    def test_pipe_statistics(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output")

        statistics = get("//sys/operations/{0}/@progress/job_statistics".format(op.id))
        assert get_statistics(statistics, "user_job.pipes.input.bytes.$.completed.map.sum") == 15
        assert get_statistics(statistics, "user_job.pipes.output.0.bytes.$.completed.map.sum") == 15

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

        chunks = get("//tmp/t_out/@chunk_ids")
        assert len(chunks) == 1
        assert get("#" + chunks[0] + "/@compressed_data_size") > 1024 * 10
        assert get("#" + chunks[0] + "/@max_block_size") < 1024 * 2

    def test_invalid_schema_in_path(self):
        create("table", "//tmp/input")
        create("table", "//tmp/output")

        with pytest.raises(YtError):
            map(in_="//tmp/input",
                out="<schema=[{name=key; type=int64}; {name=key;type=string}]>//tmp/output",
                command="cat")

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
                    dont_track=True))

        failed_ops = []
        for index in range(operation_count):
            output = "//tmp/failed_output" + str(index)
            create("table", output)
            failed_ops.append(
                map(in_="//tmp/input",
                    out=[output],
                    command="sleep 0.1; exit 1",
                    spec={"data_size_per_job": 1, "max_failed_job_count": 1, "testing": testing_options},
                    dont_track=True))

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
        operation_objects = ["NYT::NScheduler::TOperationElement", "NYT::NScheduler::TOperation"]
        records = [record for record in statistics if record["name"] in operation_objects]
        assert len(records) == 2
        assert records[0]["objects_alive"] == 0
        assert records[1]["objects_alive"] == 0

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

    @unix_only
    def test_YT_5629(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        data = [{"a": i} for i in xrange(5)]
        write_table("//tmp/t1", data)

        map(in_="//tmp/t1", out="//tmp/t2", command="sleep 1; cat /proc/self/fd/0")

        assert read_table("//tmp/t2") == data

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

    def test_complete_op(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in xrange(5):
            write_table("<append=true>//tmp/t1", {"key": str(i), "value": "foo"})

        op = map(
            wait_for_jobs=True,
            dont_track=True,
            in_="//tmp/t1",
            out="//tmp/t2",
            command="echo job_index=$YT_JOB_INDEX",
            spec={
                "mapper": {
                    "format": "dsv"
                },
                "data_size_per_job": 1,
                "max_failed_job_count": 1
            })

        for job_id in op.jobs[:3]:
            op.resume_job(job_id)

        path = "//sys/operations/{0}/@state".format(op.id)
        assert get(path) != "completed"
        while op.get_job_count("completed") < 3:
            time.sleep(0.3)

        op.complete()
        assert get(path) == "completed"
        op.track()
        assert len(read_table("//tmp/t2")) == 3

    def test_abort_op(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"foo": "bar"})

        op = map(dont_track=True,
            in_="//tmp/t",
            out="//tmp/t",
            command="sleep 1")

        path = "//sys/operations/%s/@state" % op.id
        # check running
        op.abort()
        assert get(path) == "aborted"

##################################################################

class TestSchedulerCommonMulticell(TestSchedulerCommon):
    NUM_SECONDARY_MASTER_CELLS = 2

##################################################################

@patch_porto_env_only(TestSchedulerCommon)
class TestSchedulerCommonPorto(YTEnvSetup):
    DELTA_NODE_CONFIG = porto_delta_node_config
    USE_PORTO_FOR_SERVERS = True

##################################################################

class TestPreserveSlotIndexAfterRevive(YTEnvSetup, PrepareTables):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operation_time_limit_check_period": 100,
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "profiling_update_period": 100,
            "fair_share_profiling_period": 100,
        }
    }

    def test_preserve_slot_index_after_revive(self):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", [{"x": "y"}])

        get_slot_index = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/slot_index".format(op_id))

        for i in xrange(3):
            self._create_table("//tmp/t_out_" + str(i))

        op1 = map(command="sleep 1000; cat", in_="//tmp/t_in", out="//tmp/t_out_0", dont_track=True)
        op2 = map(command="sleep 2; cat", in_="//tmp/t_in", out="//tmp/t_out_1", dont_track=True)
        op3 = map(command="sleep 1000; cat", in_="//tmp/t_in", out="//tmp/t_out_2", dont_track=True)

        assert get_slot_index(op1.id) == 0
        assert get_slot_index(op2.id) == 1
        assert get_slot_index(op3.id) == 2

        op2.track()  # this makes slot index 1 available again since operation is completed

        self.Env.kill_schedulers()
        self.Env.start_schedulers()

        time.sleep(2.0)

        assert get_slot_index(op1.id) == 0
        assert get_slot_index(op3.id) == 2

        op2 = map(command="sleep 1000; cat", in_="//tmp/t_in", out="//tmp/t_out_1", dont_track=True)

        assert get_slot_index(op2.id) == 1

##################################################################

class TestSchedulerRevive(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operation_time_limit_check_period": 100,
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "testing_options": {
                "enable_random_master_disconnection": False,
                "random_master_disconnection_max_backoff": 10000,
                "finish_operation_transition_delay": 1000,
            }
        }
    }

    OP_COUNT = 10

    def _create_table(self, table):
        create("table", table)
        set(table + "/@replication_factor", 1)

    def _prepare_tables(self):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        for index in xrange(self.OP_COUNT):
            self._create_table("//tmp/t_out" + str(index))
            self._create_table("//tmp/t_err" + str(index))

    def test_many_operations(self):
        self._prepare_tables()

        ops = []
        for index in xrange(self.OP_COUNT):
            op = map(
                dont_track=True,
                command="sleep 1; echo 'AAA' >&2; cat",
                in_="//tmp/t_in",
                out="//tmp/t_out" + str(index),
                spec={
                    "stderr_table_path": "//tmp/t_err" + str(index),
                })
            ops.append(op)

        try:
            set("//sys/scheduler/config", {"testing_options": {"enable_random_master_disconnection": True}})
            for index, op in enumerate(ops):
                try:
                    op.track()
                    assert read_table("//tmp/t_out" + str(index)) == [{"foo": "bar"}]
                except YtError:
                    assert get("//sys/operations/{0}/@state".format(op.id)) == "failed"
        finally:
            set("//sys/scheduler/config", {"testing_options": {"enable_random_master_disconnection": False}})
            time.sleep(5)

##################################################################

class TestMultipleSchedulers(YTEnvSetup, PrepareTables):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 2

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 1000,
            "fair_share_update_period": 100,
            "profiling_update_period": 100,
            "snapshot_period": 500,
            "testing_options": {
                "master_disconnect_delay": 3000,
            },
        }
    }

    def _get_scheduler_transation(self):
        while True:
            scheduler_locks = get("//sys/scheduler/lock/@locks", verbose=False)
            if len(scheduler_locks) > 0:
                scheduler_transaction = scheduler_locks[0]["transaction_id"]
                return scheduler_transaction
            time.sleep(0.01)

    def test_hot_standby(self):
        self._prepare_tables()

        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat; sleep 5")

        # Wait till snapshot is written
        time.sleep(1)

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
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
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
        assert get("//sys/operations/{0}/@progress/jobs/total".format(op.id)) == 2

        op = map(command="cat >/dev/null", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out")
        assert get("//sys/operations/{0}/@progress/jobs/total".format(op.id)) == 2

        op = merge(mode="sorted", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out")
        assert get("//sys/operations/{0}/@progress/jobs/total".format(op.id)) == 2

        op = reduce(command="cat >/dev/null", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out", reduce_by=["foo"])
        assert get("//sys/operations/{0}/@progress/jobs/total".format(op.id)) == 2

##################################################################

class TestSchedulerMaxChildrenPerAttachRequest(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "max_children_per_attach_request": 1,
        }
    }

    def test_max_children_per_attach_request(self):
        data = [{"foo": i} for i in xrange(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        map(command="cat", in_="//tmp/in", out="//tmp/out", spec={"data_size_per_job": 1})

        assert sorted(read_table("//tmp/out")) == sorted(data)
        assert get("//tmp/out/@row_count") == 3

    def test_max_children_per_attach_request_in_live_preview(self):
        data = [{"foo": i} for i in xrange(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        op = map(
            wait_for_jobs=True,
            dont_track=True,
            command="cat",
            in_="//tmp/in",
            out="//tmp/out",
            spec={"data_size_per_job": 1})

        op.resume_job(op.jobs[0])
        op.resume_job(op.jobs[1])

        operation_path = "//sys/operations/{0}".format(op.id)
        for iter in xrange(100):
            jobs_exist = exists(operation_path + "/@brief_progress/jobs")
            if jobs_exist:
                completed_jobs = get(operation_path + "/@brief_progress/jobs/completed/total")
                if completed_jobs == 2:
                    break
            time.sleep(0.1)

        operation_path = "//sys/operations/{0}".format(op.id)
        transaction_id = get(operation_path + "/@async_scheduler_transaction_id")
        assert len(read_table(operation_path + "/output_0", tx=transaction_id)) == 2
        assert get(operation_path + "/output_0/@row_count", tx=transaction_id) == 2

        op.resume_jobs()
        op.track()

##################################################################

class TestSchedulingTags(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "event_log": {
                "flush_period": 300,
                "retry_backoff_time": 300
            },
            "safe_scheduler_online_time": 1000,
        }
    }

    def _prepare(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")

        self.node = list(get("//sys/nodes"))[0]
        set("//sys/nodes/{0}/@user_tags".format(self.node), ["tagA", "tagB"])
        # Wait applying scheduling tags.
        time.sleep(0.1)

    def test_tag_filters(self):
        self._prepare()

        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out")
        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagC"})

        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagA"})
        assert read_table("//tmp/t_out") == [{"foo": "bar"}]

        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out",
            spec={"scheduling_tag_filter": "tagA & !tagC"})
        assert read_table("//tmp/t_out") == [{"foo": "bar"}]
        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_in", out="//tmp/t_out",
                spec={"scheduling_tag_filter": "tagA & !tagB"})

        set("//sys/nodes/{0}/@user_tags".format(self.node), [])
        time.sleep(1.0)
        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagA"})


    def test_pools(self):
        self._prepare()

        create("map_node", "//sys/pools/test_pool", attributes={"node_tag": "tagA"})
        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"pool": "test_pool"})
        assert read_table("//tmp/t_out") == [{"foo": "bar"}]

    def test_tag_correctness(self):
        def get_job_nodes(op):
            nodes = __builtin__.set()
            for row in read_table("//sys/scheduler/event_log"):
                if row.get("event_type") == "job_started" and row.get("operation_id") == op.id:
                    nodes.add(row["node_address"])
            return nodes

        self._prepare()
        write_table("//tmp/t_in", [{"foo": "bar"} for _ in xrange(20)])

        set("//sys/nodes/{0}/@user_tags".format(self.node), ["tagB"])
        time.sleep(1.2)
        op = map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagB", "job_count": 20})
        time.sleep(0.8)
        assert get_job_nodes(op) == __builtin__.set([self.node])


        op = map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"job_count": 20})
        time.sleep(0.8)
        assert len(get_job_nodes(op)) <= 2

##################################################################

class TestSchedulerConfig(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
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
        ]
    }

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

    def test_compat(self):
        orchid_scheduler_config = "//sys/scheduler/orchid/scheduler/config"

        set("//sys/scheduler/config", {"max_running_operation_count_per_pool": 666})
        time.sleep(3)
        assert get("{0}/max_running_operation_count_per_pool".format(orchid_scheduler_config)) == 666

        set("//sys/scheduler/config", {})
        time.sleep(3)
        assert get("{0}/max_running_operation_count_per_pool".format(orchid_scheduler_config)) == 50

        # COMPAT(acid): Remove this when max_running_operations_per_pool is removed.
        set("//sys/scheduler/config", {"max_running_operations_per_pool": 999})
        time.sleep(3)
        assert get("{0}/max_running_operation_count_per_pool".format(orchid_scheduler_config)) == 999

    def test_adresses(self):
        adresses = get("//sys/scheduler/@addresses")
        assert adresses["ipv4"].startswith("127.0.0.1:")
        assert adresses["ipv6"].startswith("::1:")

    def test_specs(self):
        create("table", "//tmp/t_in")
        write_table("<append=true;sorted_by=[foo]>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")

        op = map(command="sleep 1000", in_=["//tmp/t_in"], out="//tmp/t_out", dont_track=True)
        time.sleep(1)
        for spec_type in ("spec", "full_spec"):
            assert get("//sys/operations/{0}/@{1}/data_weight_per_job".format(op.id, spec_type)) == 2000
            assert get("//sys/scheduler/orchid/scheduler/operations/{0}/{1}/data_weight_per_job".format(op.id, spec_type)) == 2000
            assert get("//sys/scheduler/orchid/scheduler/operations/{0}/{1}/max_failed_job_count".format(op.id, spec_type)) == 10
        op.abort()

        op = reduce(command="sleep 1000", in_=["//tmp/t_in"], out="//tmp/t_out", reduce_by=["foo"], dont_track=True)
        time.sleep(1)
        for spec_type in ("spec", "full_spec"):
            assert get("//sys/operations/{0}/@{1}/data_weight_per_job".format(op.id, spec_type)) == 1000
            assert get("//sys/scheduler/orchid/scheduler/operations/{0}/{1}/data_weight_per_job".format(op.id, spec_type)) == 1000
            assert get("//sys/scheduler/orchid/scheduler/operations/{0}/{1}/max_failed_job_count".format(op.id, spec_type)) == 10
        op.abort()

    def test_cypress_config(self):
        create("table", "//tmp/t_in")
        write_table("<append=true>//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")

        op = map(command="cat", in_=["//tmp/t_in"], out="//tmp/t_out")
        assert get("//sys/operations/{0}/@spec/data_weight_per_job".format(op.id)) == 2000
        assert get("//sys/operations/{0}/@spec/max_failed_job_count".format(op.id)) == 10

        set("//sys/scheduler/config", {
            "map_operation_options": {"spec_template": {"max_failed_job_count": 50}},
            "environment": {"OTHER_VAR": "20"},
        })
        time.sleep(0.5)

        op = map(command="cat", in_=["//tmp/t_in"], out="//tmp/t_out")
        assert get("//sys/operations/{0}/@spec/data_weight_per_job".format(op.id)) == 2000
        assert get("//sys/operations/{0}/@spec/max_failed_job_count".format(op.id)) == 50

        environment = get("//sys/scheduler/orchid/scheduler/config/environment")
        assert environment["TEST_VAR"] == "10"
        assert environment["OTHER_VAR"] == "20"

##################################################################

class TestSchedulerSnapshots(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "snapshot_period": 500,
            "operation_controller_suspend_timeout": 2000,
            "max_concurrent_controller_schedule_job_calls": 1,
        }
    }

    def test_snapshots(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])
        create("table", "//tmp/out")

        testing_options = {"scheduling_delay": 500}

        op = map(
            dont_track=True,
            wait_for_jobs=True,
            command="cat",
            in_="//tmp/in",
            out="//tmp/out",
            spec={"data_weight_per_job": 1, "testing": testing_options})

        snapshot_path = "//sys/operations/{0}/snapshot".format(op.id)
        track_path(snapshot_path, 10)

        # This is done to avoid read failures due to snapshot file rewriting.
        snapshot_backup_path = snapshot_path + ".backup"
        copy(snapshot_path, snapshot_backup_path)
        assert len(read_file(snapshot_backup_path, verbose=False)) > 0

        op.resume_jobs()
        op.track()

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
                map(dont_track=True,
                    wait_for_jobs=True,
                    command="cat",
                    in_="//tmp/input",
                    out=[output],
                    spec={"data_size_per_job": 1, "testing": testing_options}))

        for op in ops:
            snapshot_path = "//sys/operations/{0}/snapshot".format(op.id)
            track_path(snapshot_path, 10)

            snapshot_backup_path = snapshot_path + ".backup"
            copy(snapshot_path, snapshot_backup_path)
            assert len(read_file(snapshot_backup_path, verbose=False)) > 0
            op.resume_jobs()

        for op in ops:
            op.track()

    def test_suspend_time_limit(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        create("table", "//tmp/out1")
        create("table", "//tmp/out2")

        while True:
            op2 = map(
                dont_track=True,
                command="cat",
                in_="//tmp/in",
                out="//tmp/out2",
                spec={"data_size_per_job": 1, "testing": {"scheduling_delay": 15000}})

            time.sleep(2)

            snapshot_path2 = "//sys/operations/{0}/snapshot".format(op2.id)
            if exists(snapshot_path2):
                op2.abort()
                continue
            else:
                break

        op1 = map(
            dont_track=True,
            command="sleep 10; cat",
            in_="//tmp/in",
            out="//tmp/out1",
            spec={"data_size_per_job": 1})

        time.sleep(8)

        snapshot_path1 = "//sys/operations/{0}/snapshot".format(op1.id)
        snapshot_path2 = "//sys/operations/{0}/snapshot".format(op2.id)

        assert exists(snapshot_path1)
        assert not exists(snapshot_path2)

        op1.abort()
        op2.abort()

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

    def test_job_count(self):
        data = [{"foo": i} for i in xrange(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        assert get("//sys/scheduler/orchid/scheduler/cell/resource_limits/user_slots") == 2
        assert get("//sys/scheduler/orchid/scheduler/cell/resource_usage/user_slots") == 0

        op = map(
            dont_track=True,
            command="sleep 100",
            in_="//tmp/in",
            out="//tmp/out",
            spec={"data_size_per_job": 1, "locality_timeout": 0})

        time.sleep(2)

        assert get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/resource_usage/user_slots".format(op.id)) == 2
        assert get("//sys/scheduler/orchid/scheduler/cell/resource_limits/user_slots") == 2
        assert get("//sys/scheduler/orchid/scheduler/cell/resource_usage/user_slots") == 2

        op.abort()

##################################################################

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

    def test_scheduler_job_by_id(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out")
        write_table("//tmp/in", [{"foo": i} for i in xrange(10)])
        op = map(
            dont_track=True,
            wait_for_jobs=True,
            label="scheduler_job_statistics",
            in_="//tmp/in",
            out="//tmp/out",
            command="cat")

        running_jobs = get("//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op.id))
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

    def test_scheduler_job_statistics(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out")
        write_table("//tmp/in", [{"foo": i} for i in xrange(10)])

        op = map(
            dont_track=True,
            wait_for_jobs=True,
            label="scheduler_job_statistics",
            in_="//tmp/in",
            out="//tmp/out",
            command="cat")

        running_jobs = get("//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op.id))
        job_id = running_jobs.keys()[0]

        statistics_appeared = False
        for iter in xrange(30):
            statistics = get("//sys/scheduler/orchid/scheduler/jobs/{0}/statistics".format(job_id))
            data = statistics.get("data", {})
            _input = data.get("input", {})
            row_count = _input.get("row_count", {})
            _sum = row_count.get("sum", 0)
            if _sum == 10:
                statistics_appeared = True
                break
            time.sleep(1.0)

        assert statistics_appeared

        op.resume_jobs()
        op.track()

##################################################################

class TestSchedulerCaching(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "get_exec_nodes_information_delay": 3000,
        }
    }

    def test_exec_node_descriptors_caching(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        write_table("//tmp/t_in", [{"foo": i} for i in xrange(10)])

        op = map(dont_track=True, command='cat', in_="//tmp/t_in", out="//tmp/t_out")
        op.track()

##################################################################

class TestSecureVault(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    secure_vault = {
        "int64": 42424243,
        "uint64": yson.YsonUint64(1234),
        "string": "penguin",
        "boolean": True,
        "double": 3.14,
        "composite": {"token1": "SeNsItIvE", "token2": "InFo"},
    }

    def run_map_with_secure_vault(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")
        op = map(
            dont_track=True,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"secure_vault": self.secure_vault, "max_failed_job_count": 1},
            command="""
                echo {YT_SECURE_VAULT=$YT_SECURE_VAULT}\;;
                echo {YT_SECURE_VAULT_int64=$YT_SECURE_VAULT_int64}\;;
                echo {YT_SECURE_VAULT_uint64=$YT_SECURE_VAULT_uint64}\;;
                echo {YT_SECURE_VAULT_string=$YT_SECURE_VAULT_string}\;;
                echo {YT_SECURE_VAULT_boolean=$YT_SECURE_VAULT_boolean}\;;
                echo {YT_SECURE_VAULT_double=$YT_SECURE_VAULT_double}\;;
                echo {YT_SECURE_VAULT_composite=\\"$YT_SECURE_VAULT_composite\\"}\;;
           """)
        return op

    def check_content(self, res):
        assert len(res) == 7
        assert res[0] == {"YT_SECURE_VAULT": self.secure_vault}
        assert res[1] == {"YT_SECURE_VAULT_int64": self.secure_vault["int64"]}
        assert res[2] == {"YT_SECURE_VAULT_uint64": self.secure_vault["uint64"]}
        assert res[3] == {"YT_SECURE_VAULT_string": self.secure_vault["string"]}
        # Boolean values are represented with 0/1.
        assert res[4] == {"YT_SECURE_VAULT_boolean": 1}
        assert res[5] == {"YT_SECURE_VAULT_double": self.secure_vault["double"]}
        # Composite values are not exported as separate environment variables.
        assert res[6] == {"YT_SECURE_VAULT_composite": ""}


    def test_secure_vault_not_visible(self):
        op = self.run_map_with_secure_vault()
        cypress_info = str(get("//sys/operations/{0}/@".format(op.id)))
        scheduler_info = str(get("//sys/scheduler/orchid/scheduler/operations/{0}".format(op.id)))
        op.track()

        # Check that secure environment variables is neither presented in the Cypress node of the
        # operation nor in scheduler Orchid representation of the operation.
        for info in [cypress_info, scheduler_info]:
            for sensible_text in ["42424243", "SeNsItIvE", "InFo"]:
                assert info.find(sensible_text) == -1

    def test_secure_vault_simple(self):
        op = self.run_map_with_secure_vault()
        op.track()
        res = read_table("//tmp/t_out")
        self.check_content(res)

    def test_secure_vault_with_revive(self):
        op = self.run_map_with_secure_vault()
        self.Env.kill_schedulers()
        self.Env.start_schedulers()
        op.track()
        res = read_table("//tmp/t_out")
        self.check_content(res)

    def test_allowed_variable_names(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")
        with pytest.raises(YtError):
            map(dont_track=True,
                in_="//tmp/t_in",
                out="//tmp/t_out",
                spec={"secure_vault": {"=_=": 42}},
                command="cat")
        with pytest.raises(YtError):
            map(dont_track=True,
                in_="//tmp/t_in",
                out="//tmp/t_out",
                spec={"secure_vault": {"x" * (2**16 + 1): 42}},
                command="cat")

##################################################################

class TestSafeAssertionsMode(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "enable_controller_failure_spec_option": True,
        },
        "core_dumper": {
            "component_name": "",
            "path": "/dev/null",
        }
    }

    @unix_only
    def test_assertion_failure(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")

        op = map(
            dont_track=True,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"testing": {"controller_failure": "assertion_failure_in_prepare"}},
            command="cat")
        with pytest.raises(YtError):
            op.track()

        assert len(get("//sys/scheduler/orchid/profiling/controller_agent/assertions_failed")) == 1

        op = map(
            dont_track=True,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"testing": {"controller_failure": "exception_thrown_in_on_job_completed"}},
            command="cat")
        with pytest.raises(YtError):
            op.track()

        # Note that exception in on job completed is not a failed assertion, so it doesn't affect this counter.
        assert len(get("//sys/scheduler/orchid/profiling/controller_agent/assertions_failed")) == 1

        op = map(
            dont_track=True,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"testing": {"controller_failure": "assertion_failure_in_prepare"}},
            command="cat")
        with pytest.raises(YtError):
            op.track()

        assert len(get("//sys/scheduler/orchid/profiling/controller_agent/assertions_failed")) == 2

##################################################################

class TestMaxTotalSliceCount(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "max_total_slice_count": 3,
        }
    }

    @unix_only
    def test_hit_limit(self):
        create("table", "//tmp/t_primary")
        write_table("//tmp/t_primary", [
            {"key": 0},
            {"key": 10}], sorted_by=['key'])

        create("table", "//tmp/t_foreign")
        write_table("<append=true; sorted_by=[key]>//tmp/t_foreign", [{"key": 0}, {"key": 1}, {"key": 2}])
        write_table("<append=true; sorted_by=[key]>//tmp/t_foreign", [{"key": 3}, {"key": 4}, {"key": 5}])
        write_table("<append=true; sorted_by=[key]>//tmp/t_foreign", [{"key": 6}, {"key": 7}, {"key": 8}])

        create("table", "//tmp/t_out")
        with pytest.raises(YtError):
            join_reduce(
                in_=["//tmp/t_primary", "<foreign=true>//tmp/t_foreign"],
                out="//tmp/t_out",
                join_by=["key"],
                command="cat > /dev/null")

##################################################################

class TestNewPoolMetrics(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "job_metrics_batch_interval": 3000,  # 3 sec
            "fair_share_update_period": 100,
            "profiling_update_period": 100,
            "fair_share_profiling_period": 100,
        },
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "enable_cgroups": True,
            "supported_cgroups": ["cpuacct", "blkio", "memory", "cpu"],
            "slot_manager": {
                "enforce_job_control": True,
                "job_environment": {
                    "type": "cgroups",
                    "supported_cgroups": [
                        "cpuacct",
                        "blkio",
                        "memory",
                        "cpu"],
                },
            },
            "scheduler_connector": {
                "heartbeat_period": 100,  # 100 msec
            },
        }
    }

    @unix_only
    def test_map(self):
        create("map_node", "//sys/pools/parent")
        create("map_node", "//sys/pools/parent/child1")
        create("map_node", "//sys/pools/parent/child2")

        # Give scheduler some time to apply new pools.
        time.sleep(1)

        create("table", "//t_input")
        create("table", "//t_output")

        # write table of 2 chunks because we want 2 jobs
        write_table("//t_input", [{"key": i} for i in xrange(0, 100)])
        write_table("<append=%true>//t_input", [{"key": i} for i in xrange(100, 500)])

        # our command does the following
        # - writes (and syncs) something to disk
        # - works for some time (to ensure that it sends several heartbeats
        # - writes something to stderr because we want to find our jobs in //sys/operations later
        map_cmd = """for i in $(seq 10) ; do echo 5 > foo$i ; sync ; sleep 0.5 ; done ; cat ; echo done > /dev/stderr"""

        op11 = map(
            in_="//t_input",
            out="//t_output",
            wait_for_jobs=False,
            command=map_cmd,
            spec={"job_count": 2, "pool": "child1"},
        )
        op12 = map(
            in_="//t_input",
            out="//t_output",
            wait_for_jobs=False,
            command=map_cmd,
            spec={"job_count": 2, "pool": "child1"},
        )

        op2 = map(
            in_="//t_input",
            out="//t_output",
            wait_for_jobs=False,
            command=map_cmd,
            spec={"job_count": 2, "pool": "child2"},
        )
        # Give scheduler some time to update metrics in the orchid.
        time.sleep(1)

        pool_metrics = get_pool_metrics("disk_writes")

        op11_writes = get_cypress_metrics(op11.id, "user_job.block_io.io_write")
        op12_writes = get_cypress_metrics(op12.id, "user_job.block_io.io_write")
        op2_writes = get_cypress_metrics(op2.id, "user_job.block_io.io_write")

        assert pool_metrics["child1"] == op11_writes + op12_writes > 0
        assert pool_metrics["child2"] == op2_writes > 0
        assert pool_metrics["parent"] == op11_writes + op12_writes + op2_writes > 0

        jobs_11 = ls("//sys/operations/{0}/jobs".format(op11.id))
        assert len(jobs_11) >= 2

    def test_time_metrics(self):
        create("map_node", "//sys/pools/parent")
        create("map_node", "//sys/pools/parent/child")

        # Give scheduler some time to apply new pools.
        time.sleep(1)

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("<append=%true>//tmp/t_input", [{"key": i} for i in xrange(2)])

        op = map(
            command='cat; if [ "$YT_JOB_INDEX" = "0" ]; then sleep 1; else sleep 100500; fi',
            in_="//tmp/t_input",
            out="//tmp/t_output",
            dont_track=True,
            spec={"data_size_per_job": 1, "pool": "child"}
        )

        orchid_path = "//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op.id)

        def running_jobs_exists():
            return exists(orchid_path) and len(ls(orchid_path)) == 2

        wait(running_jobs_exists)

        # Give jobs some time to run.
        time.sleep(3.0)

        running_jobs = ls(orchid_path)
        assert len(running_jobs) == 1
        abort_job(running_jobs[0])

        # Give scheduler some time to update metrics in the orchid.
        time.sleep(1)

        completed_metrics = get_pool_metrics("time_completed")
        aborted_metrics = get_pool_metrics("time_aborted")

        for p in ("parent", "child"):
            assert completed_metrics[p] > 0
            assert aborted_metrics[p] > 0

        assert completed_metrics["parent"] == completed_metrics["child"]
        assert aborted_metrics["parent"] == aborted_metrics["child"]
