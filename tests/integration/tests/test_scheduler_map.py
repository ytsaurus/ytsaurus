from yt_env_setup import YTEnvSetup, unix_only, porto_env_only
from yt_commands import *

from yt.yson import *
from yt.wrapper import JsonFormat
from yt.environment.helpers import assert_items_equal, assert_almost_equal
from yt.wrapper.operation_commands import add_failed_operation_stderrs_to_error_message
from yt.common import date_string_to_timestamp_mcs

import yt.environment.init_operation_archive as init_operation_archive

from flaky import flaky

import pytest
import time
import __builtin__
import os
import re
import sys
from collections import defaultdict

##################################################################

def id_to_parts(id):
    id_parts = id.split("-")
    id_hi = long(id_parts[2], 16) << 32 | int(id_parts[3], 16)
    id_lo = long(id_parts[0], 16) << 32 | int(id_parts[1], 16)
    return id_hi, id_lo

##################################################################

def check_all_stderrs(op, expected_content, expected_count, substring=False):
    jobs_path = "//sys/operations/{0}/jobs".format(op.id)
    assert get(jobs_path + "/@count") == expected_count
    for job_id in ls(jobs_path):
        stderr_path = "{0}/{1}/stderr".format(jobs_path, job_id)
        if is_multicell:
            assert get(stderr_path + "/@external")
        actual_content = read_file(stderr_path)
        assert get(stderr_path + "/@uncompressed_data_size") == len(actual_content)
        if substring:
            assert expected_content in actual_content
        else:
            assert actual_content == expected_content

##################################################################

# This is a mix of options for 18.4 and 18.5
cgroups_delta_node_config = {
    "exec_agent": {
        "enable_cgroups": True,                                       # <= 18.4
        "supported_cgroups": ["cpuacct", "blkio", "memory", "cpu"],   # <= 18.4
        "slot_manager": {
            "enforce_job_control": True,                              # <= 18.4
            "job_environment" : {
                "type" : "cgroups",                                   # >= 18.5
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
            "job_environment" : {
                # >= 19.2
                "type" : "porto",
            },
        }
    }
}

##################################################################

class TestCGroups(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = cgroups_delta_node_config

    def test_failed_jobs_twice(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"} for i in xrange(200)])

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


class TestJobProber(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = cgroups_delta_node_config

    @unix_only
    def test_strace_job(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            dont_track=True,
            wait_for_jobs=True,
            label="strace_job",
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat")

        result = strace_job(op.jobs[0])

        for pid, trace in result["traces"].iteritems():
            if trace["process_name"] != "sleep" and "No such process" not in trace["trace"]:
                assert trace["trace"].startswith("Process {0} attached".format(pid))
            assert "process_command_line" in trace
            assert "process_name" in trace

        op.resume_jobs()
        op.track()

    @unix_only
    def test_signal_job_with_no_job_restart(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            dont_track=True,
            wait_for_jobs=True,
            label="signal_job_with_no_job_restart",
            in_="//tmp/t1",
            out="//tmp/t2",
            precommand='trap "echo got=SIGUSR1" USR1\ntrap "echo got=SIGUSR2" USR2\n',
            command="cat\n",
            spec={
                "mapper": {
                    "format": "dsv"
                },
                "max_failed_job_count": 1
            })

        signal_job(op.jobs[0], "SIGUSR1")
        signal_job(op.jobs[0], "SIGUSR2")

        op.resume_jobs()
        op.track()

        assert get("//sys/operations/{0}/@progress/jobs/aborted/total".format(op.id)) == 0
        assert get("//sys/operations/{0}/@progress/jobs/failed".format(op.id)) == 0
        assert read_table("//tmp/t2") == [{"foo": "bar"}, {"got": "SIGUSR1"}, {"got": "SIGUSR2"}]

    @unix_only
    @flaky(max_runs=5)
    def test_signal_job_with_job_restart(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            dont_track=True,
            wait_for_jobs=True,
            label="signal_job_with_job_restart",
            in_="//tmp/t1",
            out="//tmp/t2",
            precommand='trap "echo got=SIGUSR1; echo stderr >&2; exit 1" USR1\n',
            command='cat\n',
            spec={
                "mapper": {
                    "format": "dsv"
                },
                "max_failed_job_count": 1
            })

        # Send signal and wait for a new job
        signal_job(op.jobs[0], "SIGUSR1")
        op.resume_job(op.jobs[0])
        op.ensure_jobs_running()

        op.resume_jobs()
        op.track()

        assert get("//sys/operations/{0}/@progress/jobs/aborted/total".format(op.id)) == 1
        assert get("//sys/operations/{0}/@progress/jobs/aborted/scheduled/user_request".format(op.id)) == 1
        assert get("//sys/operations/{0}/@progress/jobs/aborted/scheduled/other".format(op.id)) == 0
        assert get("//sys/operations/{0}/@progress/jobs/failed".format(op.id)) == 0
        assert read_table("//tmp/t2") == [{"foo": "bar"}]
        # Can get two stderr here, either "User defined signal 1\nstderr\n" or "stderr\n"
        check_all_stderrs(op, "stderr\n", 1, substring=True)

    @unix_only
    def test_abandon_job(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in xrange(5):
            write_table("<append=true>//tmp/t1", {"key": str(i), "value": "foo"})

        op = map(
            dont_track=True,
            wait_for_jobs=True,
            label="abandon_job",
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat",
            spec={
                "data_size_per_job": 1
            })

        abandon_job(op.jobs[3])

        op.resume_jobs()
        op.track()
        assert len(read_table("//tmp/t2")) == 4

    @unix_only
    def test_abandon_job_sorted_empty_output(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("<append=true>//tmp/t1", {"key": "foo", "value": "bar"})

        op = map(
            dont_track=True,
            wait_for_jobs=True,
            label="abandon_job",
            in_="//tmp/t1",
            out="<sorted_by=[key]>//tmp/t2",
            command="sleep 5; cat")

        abandon_job(op.jobs[0])

        op.resume_jobs()
        op.track()
        assert len(read_table("//tmp/t2")) == 0

    @unix_only
    def test_abandon_job_permissions(self):
        create_user("u1")
        create_user("u2")

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in xrange(5):
            write_table("<append=true>//tmp/t1", {"key": str(i), "value": "foo"})

        op = map(
            dont_track=True,
            wait_for_jobs=True,
            label="abandon_job",
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat",
            spec={
                "data_size_per_job": 1
            },
            authenticated_user="u1")

        with pytest.raises(YtError):
            abandon_job(op.jobs[3], authenticated_user="u2")

        op.resume_jobs()
        op.track()
        assert len(read_table("//tmp/t2")) == 5

    def _poll_until_prompt(self, job_id, shell_id):
        output = ""
        while len(output) < 4 or output[-4:] != ":~$ ":
            r = poll_job_shell(job_id, operation="poll", shell_id=shell_id)
            output += r["output"]
        return output

    def _poll_until_shell_exited(self, job_id, shell_id):
        output = ""
        try:
            while True:
                r = poll_job_shell(job_id, operation="poll", shell_id=shell_id)
                output += r["output"]
        except YtResponseError as e:
            if e.is_shell_exited():
                return output
            raise

    def _send_keys(self, job_id, shell_id, keys, input_offset):
        poll_job_shell(
            job_id,
            operation="update",
            shell_id=shell_id,
            keys=keys.encode("hex"),
            input_offset=input_offset)

    def test_poll_job_shell(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"key": "foo"})

        op = map(
            dont_track=True,
            wait_for_jobs=True,
            label="poll_job_shell",
            in_="//tmp/t1",
            out="//tmp/t2",
            command="sleep 10; cat")

        job_id = op.jobs[0]
        r = poll_job_shell(job_id, operation="spawn", term="screen-256color", height=50, width=132)
        shell_id = r["shell_id"]
        output = self._poll_until_prompt(job_id, shell_id)

        command = "echo $TERM; tput lines; tput cols; env | grep -c YT_OPERATION_ID\r"
        self._send_keys(job_id, shell_id, command, 0)
        output = self._poll_until_prompt(job_id, shell_id)

        expected = "{0}\nscreen-256color\r\n50\r\n132\r\n1".format(command)
        assert output.startswith(expected) == True

        r = poll_job_shell(job_id, operation="terminate", shell_id=shell_id)
        with pytest.raises(YtError):
            output = self._poll_until_prompt(job_id, shell_id)

        abandon_job(job_id)

        op.resume_jobs()
        op.track()
        assert len(read_table("//tmp/t2")) == 0

    def test_poll_job_shell_command(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"key": "foo"})

        op = map(
            dont_track=True,
            wait_for_jobs=True,
            label="poll_job_shell",
            in_="//tmp/t1",
            out="//tmp/t2",
            command="sleep 10; cat")

        job_id = op.jobs[0]
        r = poll_job_shell(job_id, operation="spawn", command="echo $TERM; tput lines; tput cols; env | grep -c YT_OPERATION_ID")
        shell_id = r["shell_id"]
        output = self._poll_until_shell_exited(job_id, shell_id)

        expected = "xterm\r\n24\r\n80\r\n1\r\n"
        assert output == expected

        r = poll_job_shell(job_id, operation="terminate", shell_id=shell_id)
        with pytest.raises(YtError):
            output = self._poll_until_prompt(job_id, shell_id)

        abandon_job(job_id)

        op.resume_jobs()
        op.track()
        assert len(read_table("//tmp/t2")) == 0

    def test_poll_job_shell_permissions(self):
        create_user("u1")
        create_user("u2")

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"key": "foo"})

        op = map(
            dont_track=True,
            wait_for_jobs=True,
            label="poll_job_shell",
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat",
            authenticated_user="u1")

        job_id = op.jobs[0]
        with pytest.raises(YtError):
            poll_job_shell(
                job_id,
                operation="spawn",
                term="screen-256color",
                height=50,
                width=132,
                authenticated_user="u2")

        op.resume_jobs()
        op.track()

    def get_job_count_profiling(self):
        time.sleep(1.2)
        profiling_info = {}
        for value in reversed(get("//sys/scheduler/orchid/profiling/scheduler/job_count", verbose=False)):
            key = tuple(sorted(value["tags"].items()))
            if key not in profiling_info:
                profiling_info[key] = value["value"]

        job_count = {"state": defaultdict(int), "abort_reason": defaultdict(int)}
        print "profiling_info:", profiling_info
        for key, value in profiling_info.iteritems():
            state = dict(key)["state"]
            job_count["state"][state] += value

        for key, value in profiling_info.iteritems():
            state = dict(key)["state"]
            if state != "aborted":
                continue
            abort_reason = dict(key)["abort_reason"]
            job_count["abort_reason"][abort_reason] += value

        return job_count

    @unix_only
    def test_abort_job(self):
        start_profiling = self.get_job_count_profiling()

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in xrange(5):
            write_table("<append=true>//tmp/t1", {"key": str(i), "value": "foo"})

        op = map(
            dont_track=True,
            wait_for_jobs=True,
            label="abort_job",
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat",
            spec={
                "data_size_per_job": 1
            })

        abort_job(op.jobs[3])

        op.resume_jobs()
        op.track()

        assert len(read_table("//tmp/t2")) == 5
        assert get("//sys/operations/{0}/@progress/jobs/aborted/total".format(op.id)) == 1
        assert get("//sys/operations/{0}/@progress/jobs/aborted/scheduled/user_request".format(op.id)) == 1
        assert get("//sys/operations/{0}/@progress/jobs/aborted/scheduled/other".format(op.id)) == 0
        assert get("//sys/operations/{0}/@progress/jobs/failed".format(op.id)) == 0

        end_profiling = self.get_job_count_profiling()

        for state in end_profiling["state"]:
            print state, start_profiling["state"][state], end_profiling["state"][state]
            value = end_profiling["state"][state] - start_profiling["state"][state]
            count = 0
            if state == "aborted":
                count = 1
            if state == "completed":
                count = 5
            assert value == count

        for abort_reason in end_profiling["abort_reason"]:
            print abort_reason, start_profiling["abort_reason"][abort_reason], end_profiling["abort_reason"][abort_reason]
            value = end_profiling["abort_reason"][abort_reason] - start_profiling["abort_reason"][abort_reason]
            assert value == (1 if abort_reason == "user_request" else 0)

##################################################################

@porto_env_only
class TestEventLogPorto(TestEventLog):
    DELTA_NODE_CONFIG = porto_delta_node_config
    USE_PORTO_FOR_SERVERS = True

@porto_env_only
class TestJobProberPorto(TestJobProber):
    DELTA_NODE_CONFIG = porto_delta_node_config
    USE_PORTO_FOR_SERVERS = True

##################################################################

class TestSchedulerMapCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 16
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period" : 10,
            "running_jobs_update_period" : 10,
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
        "tablet_manager" : {
            "error_backoff_time" : 0
        }
    }

    def _create_simple_dynamic_table(self, path, sort_order="ascending", optimize_for="lookup"):
        create("table", path,
            attributes = {
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": sort_order},
                    {"name": "value", "type": "string"}
                ],
                "dynamic": True,
                "optimize_for": optimize_for
            })

    def test_empty_table(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        map(in_="//tmp/t1", out="//tmp/t2", command="cat")

        assert read_table("//tmp/t2") == []

    @unix_only
    def test_one_chunk(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})
        op = map(
            dont_track=True,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=r'cat; echo "{v1=\"$V1\"};{v2=\"$TMPDIR\"}"',
            spec={"mapper": {"environment": {"V1": "Some data", "TMPDIR": "$(SandboxPath)/mytmp"}},
                  "title": "MyTitle"})

        get("//sys/operations/%s/@spec" % op.id)
        op.track()

        res = read_table("//tmp/t2")
        assert len(res) == 3
        assert res[0] == {"a" : "b"}
        assert res[1] == {"v1" : "Some data"}
        assert res[2].has_key("v2")
        assert res[2]["v2"].endswith("/mytmp")
        assert res[2]["v2"].startswith("/")

    def test_big_input(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        count = 1000 * 1000
        original_data = [{"index": i} for i in xrange(count)]
        write_table("//tmp/t1", original_data)

        command = "cat"
        map(in_="//tmp/t1", out="//tmp/t2", command=command)

        new_data = read_table("//tmp/t2", verbose=False)
        assert sorted(row.items() for row in new_data) == [[("index", i)] for i in xrange(count)]

    def test_type_conversion(self):
        create("table", "//tmp/s")
        write_table("//tmp/s", {"foo": "42"})

        create("table", "//tmp/t",
            attributes={
                "schema": make_schema([
                    {"name": "int64", "type": "int64", "sort_order": "ascending"},
                    {"name": "uint64", "type": "uint64"},
                    {"name": "boolean", "type": "boolean"},
                    {"name": "double", "type": "double"},
                    {"name": "any", "type": "any"}],
                    strict=False)
            })

        row = '{int64=3u; uint64=42; boolean="false"; double=18; any={}; extra=qwe}'

        with pytest.raises(YtError):
            map(in_="//tmp/s",
                out="//tmp/t",
                command="echo '{0}'".format(row),
                spec={"max_failed_job_count": 1})

        yson_with_type_conversion = yson.loads("<enable_type_conversion=%true>yson")
        map(in_="//tmp/s",
            out="//tmp/t",
            command="echo '{0}'".format(row), format=yson_with_type_conversion,
            spec={"max_failed_job_count": 1, "mapper": {"output_format": yson_with_type_conversion}})


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

    def test_two_inputs_at_the_same_time(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output1")
        create("table", "//tmp/t_output2")

        count = 1000
        original_data = [{"index": i} for i in xrange(count)]
        write_table("//tmp/t_input", original_data)

        file = "//tmp/some_file.txt"
        create("file", file)
        write_file(file, "{value=42};\n")

        command = 'bash -c "cat <&0 & sleep 0.1; cat some_file.txt >&4; wait;"'
        map(in_="//tmp/t_input",
            out=["//tmp/t_output1", "//tmp/t_output2"],
            command=command,
            file=[file],
            verbose=True)

        assert read_table("//tmp/t_output2") == [{"value": 42}]
        assert sorted([row.items() for row in read_table("//tmp/t_output1")]) == [[("index", i)] for i in xrange(count)]

    def test_first_after_second(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output1")
        create("table", "//tmp/t_output2")

        count = 10000
        original_data = [{"index": i} for i in xrange(count)]
        write_table("//tmp/t_input", original_data)

        file1 = "//tmp/some_file.txt"
        create("file", file1)
        write_file(file1, "}}}}};\n")

        with pytest.raises(YtError):
            map(in_="//tmp/t_input",
                out=["//tmp/t_output1", "//tmp/t_output2"],
                command='cat some_file.txt >&4; cat >&4; echo "{value=42}"',
                file=[file1],
                verbose=True)

    @unix_only
    def test_in_equal_to_out(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"foo": "bar"})

        map(in_="//tmp/t1", out="<append=true>//tmp/t1", command="cat")

        assert read_table("//tmp/t1") == [{"foo": "bar"}, {"foo": "bar"}]

    @unix_only
    def test_stderr_ok(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        command = """cat > /dev/null; echo stderr 1>&2; echo {operation='"'$YT_OPERATION_ID'"'}';'; echo {job_index=$YT_JOB_INDEX};"""

        op = map(in_="//tmp/t1", out="//tmp/t2", command=command)

        assert read_table("//tmp/t2") == [{"operation" : op.id}, {"job_index" : 0}]
        check_all_stderrs(op, "stderr\n", 1)

    @unix_only
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

    @unix_only
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

    @unix_only
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

    @unix_only
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

    @unix_only
    def test_stderr_with_missing_tmp_quota(self):
        create_account("test_account")

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"} for i in xrange(5)])

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

    def test_job_progress(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"} for i in xrange(10)])

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
        write_table("//tmp/t1", [{"key" : i} for i in xrange(5)])

        sort(in_="//tmp/t1", out="//tmp/t1", sort_by="key")
        op = map(command="cat", in_="//tmp/t1[:1]", out="//tmp/t2")

        statistics = get("//sys/operations/{0}/@progress/estimated_input_statistics".format(op.id))
        for key in ["uncompressed_data_size", "compressed_data_size", "row_count", "data_weight"]:
            assert statistics[key] > 0
        assert statistics["unavailable_chunk_count"] == 0
        assert statistics["chunk_count"] == 1

    def test_input_row_count(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"key" : i} for i in xrange(5)])

        sort(in_="//tmp/t1", out="//tmp/t1", sort_by="key")
        op = map(command="cat", in_="//tmp/t1[:1]", out="//tmp/t2")

        assert get("//tmp/t2/@row_count") == 1

        row_count = get("//sys/operations/{0}/@progress/job_statistics/data/input/row_count/$/completed/map/sum".format(op.id))
        assert row_count == 1

    def test_multiple_output_row_count(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")
        write_table("//tmp/t1", [{"key" : i} for i in xrange(5)])

        op = map(command="cat; echo {hello=world} >&4", in_="//tmp/t1", out=["//tmp/t2", "//tmp/t3"])
        assert get("//tmp/t2/@row_count") == 5
        row_count = get("//sys/operations/{0}/@progress/job_statistics/data/output/0/row_count/$/completed/map/sum".format(op.id))
        assert row_count == 5
        row_count = get("//sys/operations/{0}/@progress/job_statistics/data/output/1/row_count/$/completed/map/sum".format(op.id))
        assert row_count == 1


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
            spec={ "mapper": { "input_format" : "dsv", "check_input_fully_consumed": True}})

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

    @unix_only
    def test_sorted_output(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in xrange(2):
            write_table("<append=true>//tmp/t1", {"key": "foo", "value": "ninja"})

        command = """cat >/dev/null;
           if [ "$YT_JOB_INDEX" = "0" ]; then
               k1=0; k2=1;
           else
               k1=0; k2=0;
           fi
           echo "{key=$k1; value=one}; {key=$k2; value=two}"
        """

        map(in_="//tmp/t1",
            out="<sorted_by=[key];append=true>//tmp/t2",
            command=command,
            spec={"job_count": 2})

        assert get("//tmp/t2/@sorted")
        assert get("//tmp/t2/@sorted_by") == ["key"]
        assert read_table("//tmp/t2") == [{"key":0 , "value":"one"}, {"key":0, "value":"two"}, {"key":0, "value":"one"}, {"key":1, "value":"two"}]

    def test_sorted_output_overlap(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in xrange(2):
            write_table("<append=true>//tmp/t1", {"key": "foo", "value": "ninja"})

        command = 'cat >/dev/null; echo "{key=1; value=one}; {key=2; value=two}"'

        with pytest.raises(YtError):
            map(in_="//tmp/t1",
                out="<sorted_by=[key]>//tmp/t2",
                command=command,
                spec={"job_count": 2})

    def test_sorted_output_job_failure(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in xrange(2):
            write_table("<append=true>//tmp/t1", {"key": "foo", "value": "ninja"})

        command = "cat >/dev/null; echo {key=2; value=one}; {key=1; value=two}"

        with pytest.raises(YtError):
            map(in_="//tmp/t1",
                out="<sorted_by=[key]>//tmp/t2",
                command=command,
                spec={"job_count": 2})

    @unix_only
    def test_job_count(self):
        create("table", "//tmp/t1")
        for i in xrange(5):
            write_table("<append=true>//tmp/t1", {"foo": "bar"})

        command = "cat > /dev/null; echo {hello=world}"

        def check(table_name, job_count, expected_num_records):
            create("table", table_name)
            map(in_="//tmp/t1",
                out=table_name,
                command=command,
                spec={"job_count": job_count})
            assert read_table(table_name) == [{"hello": "world"} for i in xrange(expected_num_records)]

        check("//tmp/t2", 3, 3)
        check("//tmp/t3", 10, 5) # number of jobs cannot be more than number of rows.


    @unix_only
    def test_skewed_rows(self):
        create("table", "//tmp/t1")
        # 5 small rows
        write_table("<append=true>//tmp/t1", [{"foo": "bar"}] * 5)
        # and one large row
        write_table("<append=true>//tmp/t1", {"foo": "".join(["r"] * 1024)})

        create("table", "//tmp/t2")
        map(in_="//tmp/t1",
            out="//tmp/t2",
            command="cat > /dev/null; echo {hello=world}",
            spec={"job_count": 6})
        assert read_table("//tmp/t2") == [{"hello": "world"} for i in xrange(6)]

    @unix_only
    def test_job_per_row(self):
        create("table", "//tmp/input")

        job_count = 976
        original_data = [{"index": str(i)} for i in xrange(job_count)]
        write_table("//tmp/input", original_data)

        create("table", "//tmp/output", ignore_existing=True)

        for job_count in xrange(976, 950, -1):
            op = map(dont_track=True,
                     in_="//tmp/input",
                     out="//tmp/output",
                     command="sleep 100",
                     spec={"job_count": job_count})
            time.sleep(1)
            assert op.get_job_count("total") == job_count
            op.abort()

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
        write_table("//tmp/table_file", {"text": "info", "other" : "trash"})

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

        command= "cat > /dev/null; cat empty_file.txt; cat table_file"

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

        command= "cat > /dev/null; cat regular_file; cat table_file"

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

        command= "cat > /dev/null; cat regular_file; cat table_file"

        map(in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=["//tmp/regular_file", "<format=yson>//tmp/table_file"])

        assert read_table("//tmp/output") == [{"value": 42}, {"a": "b"}, {"text1": "info1"}, {"text2": "info2"}]

    @unix_only
    def run_many_output_tables(self, yamr_mode=False):
        output_tables = ["//tmp/t%d" % i for i in range(3)]

        create("table", "//tmp/t_in")
        for table_path in output_tables:
            create("table", table_path)

        write_table("//tmp/t_in", {"a": "b"})

        if yamr_mode:
            mapper = "cat  > /dev/null; echo {v = 0} >&3; echo {v = 1} >&4; echo {v = 2} >&5"
        else:
            mapper = "cat  > /dev/null; echo {v = 0} >&1; echo {v = 1} >&4; echo {v = 2} >&7"

        create("file", "//tmp/mapper.sh")
        write_file("//tmp/mapper.sh", mapper)

        map(in_="//tmp/t_in",
            out=output_tables,
            command="bash mapper.sh",
            file="//tmp/mapper.sh",
            spec={"mapper": {"use_yamr_descriptors" : yamr_mode}})

        assert read_table(output_tables[0]) == [{"v": 0}]
        assert read_table(output_tables[1]) == [{"v": 1}]
        assert read_table(output_tables[2]) == [{"v": 2}]

    @unix_only
    def test_many_output_yt(self):
        self.run_many_output_tables()

    @unix_only
    def test_many_output_yamr(self):
        self.run_many_output_tables(True)

    @unix_only
    def test_output_tables_switch(self):
        output_tables = ["//tmp/t%d" % i for i in range(3)]

        create("table", "//tmp/t_in")
        for table_path in output_tables:
            create("table", table_path)

        write_table("//tmp/t_in", {"a": "b"})
        mapper = 'cat  > /dev/null; echo "<table_index=2>#;{v = 0};{v = 1};<table_index=0>#;{v = 2}"'

        create("file", "//tmp/mapper.sh")
        write_file("//tmp/mapper.sh", mapper)

        map(in_="//tmp/t_in",
            out=output_tables,
            command="bash mapper.sh",
            file="//tmp/mapper.sh")

        assert read_table(output_tables[0]) == [{"v": 2}]
        assert read_table(output_tables[1]) == []
        assert read_table(output_tables[2]) == [{"v": 0}, {"v": 1}]

    @unix_only
    def test_tskv_input_format(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n').split('\\t')
assert input == ['tskv', 'foo=bar']
print '{hello=world}'

"""
        create("file", "//tmp/mapper.sh")
        write_file("//tmp/mapper.sh", mapper)

        create("table", "//tmp/t_out")
        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python mapper.sh",
            file="//tmp/mapper.sh",
            spec={"mapper": {"input_format": yson.loads("<line_prefix=tskv>dsv")}})

        assert read_table("//tmp/t_out") == [{"hello": "world"}]

    @unix_only
    def test_tskv_output_format(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n')
assert input == '<"table_index"=0;>#;'
input = sys.stdin.readline().strip('\\n')
assert input == '{"foo"="bar";};'
print "tskv" + "\\t" + "hello=world"
"""
        create("file", "//tmp/mapper.sh")
        write_file("//tmp/mapper.sh", mapper)

        create("table", "//tmp/t_out")
        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python mapper.sh",
            file="//tmp/mapper.sh",
            spec={"mapper": {
                    "enable_input_table_index": True,
                    "input_format": yson.loads("<format=text>yson"),
                    "output_format": yson.loads("<line_prefix=tskv>dsv")
                }})

        assert read_table("//tmp/t_out") == [{"hello": "world"}]

    @unix_only
    def test_yamr_output_format(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n')
assert input == '{"foo"="bar";};'
print "key\\tsubkey\\tvalue"

"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        create("table", "//tmp/t_out")
        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python mapper.py",
            file="//tmp/mapper.py",
            spec={"mapper": {
                    "input_format": yson.loads("<format=text>yson"),
                    "output_format": yson.loads("<has_subkey=true>yamr")
                }})

        assert read_table("//tmp/t_out") == [{"key": "key", "subkey": "subkey", "value": "value"}]

    @unix_only
    def test_yamr_input_format(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"value": "value", "subkey": "subkey", "key": "key", "a": "another"})

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n').split('\\t')
assert input == ['key', 'subkey', 'value']
print '{hello=world}'

"""
        create("file", "//tmp/mapper.sh")
        write_file("//tmp/mapper.sh", mapper)

        create("table", "//tmp/t_out")
        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python mapper.sh",
            file="//tmp/mapper.sh",
            spec={"mapper": {"input_format": yson.loads("<has_subkey=true>yamr")}})

        assert read_table("//tmp/t_out") == [{"hello": "world"}]

    @unix_only
    def test_executable_mapper(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        mapper =  \
"""
#!/bin/bash
cat > /dev/null; echo {hello=world}
"""

        create("file", "//tmp/mapper.sh")
        write_file("//tmp/mapper.sh", mapper)

        set("//tmp/mapper.sh/@executable", True)

        create("table", "//tmp/t_out")
        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="./mapper.sh",
            file="//tmp/mapper.sh")

        assert read_table("//tmp/t_out") == [{"hello": "world"}]

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


    @unix_only
    def test_table_index(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/out")

        write_table("//tmp/t1", {"key": "a", "value": "value"})
        write_table("//tmp/t2", {"key": "b", "value": "value"})

        mapper = \
"""
import sys
table_index = sys.stdin.readline().strip()
row = sys.stdin.readline().strip()
print row + table_index

table_index = sys.stdin.readline().strip()
row = sys.stdin.readline().strip()
print row + table_index
"""

        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        map(in_=["//tmp/t1", "//tmp/t2"],
            out="//tmp/out",
            command="python mapper.py",
            file="//tmp/mapper.py",
            spec={"mapper": {"format": yson.loads("<enable_table_index=true>yamr")}})

        expected = [{"key": "a", "value": "value0"},
                    {"key": "b", "value": "value1"}]
        assert_items_equal(read_table("//tmp/out"), expected)

    @unix_only
    def test_range_index(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/out")

        for i in xrange(1, 3):
            write_table(
                "<append=true>//tmp/t_in",
                [
                    {"key": "%05d" % i, "value": "value"},
                ],
                sorted_by = ["key", "value"])

        t_in = '<ranges=[{lower_limit={key=["00002"]};upper_limit={key=["00003"]}};{lower_limit={key=["00002"]};upper_limit={key=["00003"]}}]>//tmp/t_in'

        op = map(
            dont_track=True,
            in_=[t_in],
            out="//tmp/out",
            command="cat >& 2",
            spec={
                "job_io": {
                    "control_attributes": {
                        "enable_range_index": True,
                        "enable_row_index": True,
                    }
                },
                "mapper": {
                    "input_format": yson.loads("<format=text>yson"),
                    "output_format": "dsv",
                }
            })

        op.track()
        check_all_stderrs(op, '"range_index"=0;', 1, substring=True)
        check_all_stderrs(op, '"range_index"=1;', 1, substring=True)

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

    def test_insane_demand(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", {"cool": "stuff"})

        with pytest.raises(YtError):
            map(in_="//tmp/t_in", out="//tmp/t_out", command="cat",
                spec={"mapper": {"memory_limit": 1000000000000}})

    @unix_only
    def test_YT_5629(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        data = [{"a": i} for i in xrange(5)]
        write_table("//tmp/t1", data)

        map(in_="//tmp/t1", out="//tmp/t2", command="sleep 1; cat /proc/self/fd/0")

        assert read_table("//tmp/t2") == data

    def test_check_input_fully_consumed(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        command = 'python -c "import os; os.read(0, 5);"'

        # If all jobs failed then operation is also failed
        with pytest.raises(YtError):
            map(in_="//tmp/t1",
                out="//tmp/t2",
                command=command,
                spec={ "mapper": { "input_format" : "dsv", "check_input_fully_consumed": True}})

        assert read_table("//tmp/t2") == []

    def test_check_input_not_fully_consumed(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        data = [{"foo": "bar"} for i in xrange(10000)]
        write_table("//tmp/t1", data)

        map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="head -1",
            spec={"mapper": {"input_format" : "dsv", "output_format" : "dsv"}})

        assert read_table("//tmp/t2") == [{"foo": "bar"}]

    def test_live_preview(self):
        create_user("u")

        data = [{"foo": i} for i in range(5)]

        create("table", "//tmp/t1")
        write_table("//tmp/t1", data)

        create("table", "//tmp/t2")
        set("//tmp/t2/@acl", [make_ace("allow", "u", "write")])
        effective_acl = get("//tmp/t2/@effective_acl")

        schema = make_schema([{"name": "foo", "type": "int64"}], strict=True, unique_keys=False)
        alter_table("//tmp/t2", schema=schema)

        op = map(
            wait_for_jobs=True,
            dont_track=True,
            command="cat",
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"data_size_per_job": 1})

        operation_path = "//sys/operations/{0}".format(op.id)

        async_transaction_id = get(operation_path + "/@async_scheduler_transaction_id")
        assert exists(operation_path + "/output_0", tx=async_transaction_id)
        assert effective_acl == get(operation_path + "/output_0/@acl", tx=async_transaction_id)
        assert schema == get(operation_path + "/output_0/@schema", tx=async_transaction_id)

        op.resume_job(op.jobs[0])
        op.resume_job(op.jobs[1])
        while op.get_job_count("completed") < 2:
            time.sleep(0.2)
        time.sleep(1)

        live_preview_data = read_table(operation_path + "/output_0", tx=async_transaction_id)
        assert len(live_preview_data) == 2
        assert all(record in data for record in live_preview_data)

        op.resume_jobs()
        op.track()
        assert read_table("//tmp/t2") == data

    def test_row_sampling(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")

        count = 1000
        original_data = [{"index": i} for i in xrange(count)]
        write_table("//tmp/t1", original_data)

        command = "cat"
        sampling_rate = 0.5
        spec = {"job_io": {"table_reader": {"sampling_seed": 42, "sampling_rate": sampling_rate}}}

        map(in_="//tmp/t1", out="//tmp/t2", command=command, spec=spec)
        map(in_="//tmp/t1", out="//tmp/t3", command=command, spec=spec)

        new_data_t2 = read_table("//tmp/t2", verbose=False)
        new_data_t3 = read_table("//tmp/t3", verbose=False)

        assert sorted(row.items() for row in new_data_t2) == sorted(row.items() for row in new_data_t3)

        actual_rate = len(new_data_t2) * 1.0 / len(original_data)
        variation = sampling_rate * (1 - sampling_rate)
        assert sampling_rate - variation <= actual_rate <= sampling_rate + variation

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

    def test_job_per_row(self):
        create("table", "//tmp/input")

        job_count = 976
        original_data = [{"index": str(i)} for i in xrange(job_count)]
        write_table("//tmp/input", original_data)

        create("table", "//tmp/output", ignore_existing=True)
        op = map(dont_track=True,
                 in_="//tmp/input",
                 out="//tmp/output",
                 command="sleep 100",
                 spec={"job_count": 976})
        time.sleep(1)
        assert op.get_job_count("total") == 976
        op.abort()

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
            output = "//tmp/failed_output" + str(index)
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

    @unix_only
    def test_map_row_count_limit(self):
        create("table", "//tmp/input")
        for i in xrange(5):
            write_table("<append=true>//tmp/input", {"key": "%05d"%i, "value": "foo"})

        create("table", "//tmp/output")
        op = map(
            wait_for_jobs=True,
            dont_track=True,
            in_="//tmp/input",
            out="<row_count_limit=3>//tmp/output",
            command="cat",
            spec={
                "mapper": {
                    "format": "dsv"
                },
                "data_size_per_job": 1,
                "max_failed_job_count": 1
            })

        for i in xrange(3):
            op.resume_job(op.jobs[0])

        op.track()
        assert len(read_table("//tmp/output")) == 3

    @unix_only
    def test_map_row_count_limit_second_output(self):
        create("table", "//tmp/input")
        for i in xrange(5):
            write_table("<append=true>//tmp/input", {"key": "%05d"%i, "value": "foo"})

        create("table", "//tmp/out_1")
        create("table", "//tmp/out_2")
        op = map(
            wait_for_jobs=True,
            dont_track=True,
            in_="//tmp/input",
            out=["//tmp/out_1", "<row_count_limit=3>//tmp/out_2"],
            command="cat >&4",
            spec={
                "mapper": {
                    "format": "dsv"
                },
                "data_size_per_job": 1,
                "max_failed_job_count": 1
            })

        for i in xrange(3):
            op.resume_job(op.jobs[0])

        op.track()
        assert len(read_table("//tmp/out_1")) == 0
        assert len(read_table("//tmp/out_2")) == 3

    @unix_only
    def test_ordered_map_row_count_limit(self):
        create("table", "//tmp/input")
        for i in xrange(5):
            write_table("<append=true>//tmp/input", {"key": "%05d"%i, "value": "foo"})

        create("table", "//tmp/output")
        op = map(
            wait_for_jobs=True,
            dont_track=True,
            in_="//tmp/input",
            out="<row_count_limit=3>//tmp/output",
            ordered=True,
            command="cat",
            spec={
                "mapper": {
                    "format": "dsv"
                },
                "data_size_per_job": 1,
                "max_failed_job_count": 1
            })

        for i in xrange(3):
            op.resume_job(op.jobs[0])

        op.track()
        assert len(read_table("//tmp/output")) == 3

    def test_multiple_row_count_limit(self):
        create("table", "//tmp/input")

        create("table", "//tmp/output")
        with pytest.raises(YtError):
            map(in_="//tmp/input",
                out=["<row_count_limit=1>//tmp/out_1", "<row_count_limit=1>//tmp/out_2"],
                command="cat")

    def test_invalid_schema_in_path(self):
        create("table", "//tmp/input")
        create("table", "//tmp/output")

        with pytest.raises(YtError):
            map(in_="//tmp/input",
                out="<schema=[{name=key; type=int64}; {name=key;type=string}]>//tmp/output",
                command="cat")

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_schema_validation(self, optimize_for):
        schema = make_schema([
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}])
        create("table", "//tmp/input")
        create("table", "//tmp/output", attributes={
            "optimize_for" : optimize_for,
            "schema": schema })
        create("table", "//tmp/output2")

        for i in xrange(10):
            write_table("<append=true>//tmp/input", {"key": i, "value": "foo"})

        map(in_="//tmp/input",
            out="//tmp/output",
            command="cat")

        assert get("//tmp/output/@schema_mode") == "strong"
        assert get("//tmp/output/@schema/@strict")
        assert [c for c in get("//tmp/output/@schema")] == [c for c in schema]
        assert_items_equal(read_table("//tmp/output"), [{"key": i, "value": "foo"} for i in xrange(10)])

        map(in_="//tmp/input",
            out="<schema=%s>//tmp/output2" % yson.dumps(schema),
            command="cat")

        assert get("//tmp/output2/@schema_mode") == "strong"
        assert get("//tmp/output2/@schema/@strict")
        assert [c for c in get("//tmp/output2/@schema")] == [c for c in schema]
        assert_items_equal(read_table("//tmp/output2"), [{"key": i, "value": "foo"} for i in xrange(10)])

        write_table("//tmp/input", {"key": "1", "value": "foo"})

        with pytest.raises(YtError):
            map(in_="//tmp/input",
                out="//tmp/output",
                command="cat")

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_unique_keys_validation(self, optimize_for):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2", attributes={
            "optimize_for" : optimize_for,
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

    @pytest.mark.parametrize("mode", ["unordered", "ordered"])
    def test_computed_columns(self, mode):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "k1", "type": "int64", "expression": "k2 * 2" },
                    {"name": "k2", "type": "int64"}]
            })

        write_table("//tmp/t1", [{"k2": i} for i in xrange(2)])

        map(
            mode=mode,
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat")

        assert get("//tmp/t2/@schema_mode") == "strong"
        assert read_table("//tmp/t2") == [{"k1": i * 2, "k2": i} for i in xrange(2)]

    @unix_only
    def test_map_input_paths_attr(self):
        create("table", "//tmp/in1")
        for i in xrange(0, 5, 2):
            write_table(
                "<append=true>//tmp/in1",
                [{"key": "%05d" % (i+j), "value": "foo"} for j in xrange(2)],
                sorted_by = ["key"])

        create("table", "//tmp/in2")
        for i in xrange(3, 24, 2):
            write_table(
                "<append=true>//tmp/in2",
                [{"key": "%05d" % ((i+j) / 4), "value": "bar"} for j in xrange(2)],
                sorted_by = ["key", "value"])

        create("table", "//tmp/out")
        in2 = '//tmp/in2["00001":"00004","00005":"00006"]'
        op = map(
            dont_track=True,
            in_=["//tmp/in1", in2],
            out="//tmp/out",
            command="exit 1",
            spec={
                "mapper": {
                    "format": "dsv"
                },
                "job_count": 1,
                "max_failed_job_count": 1
            })
        with pytest.raises(YtError):
            op.track();

        jobs_path = "//sys/operations/{0}/jobs".format(op.id)
        job_ids = ls(jobs_path)
        assert len(job_ids) == 1
        expected = yson.loads("""[
            <ranges=[{lower_limit={row_index=0};upper_limit={row_index=6}}]>"//tmp/in1";
            <ranges=[
                {lower_limit={row_index=0;key=["00001"]};upper_limit={row_index=14;key=["00004"]}};
                {lower_limit={row_index=16;key=["00005"]};upper_limit={row_index=22;key=["00006"]}}
            ]>"//tmp/in2"]""")
        actual = get("{0}/{1}/@input_paths".format(jobs_path, job_ids[0]))
        assert expected == actual

    def test_map_max_data_size_per_job(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            dont_track=True,
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command='cat',
            spec={
                "max_data_size_per_job": 1
            })

        with pytest.raises(YtError):
            op.track()

    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("sort_order", [None, "ascending"])
    @pytest.mark.parametrize("ordered", [False, True])
    def test_map_on_dynamic_table(self, ordered, sort_order, optimize_for):
        self.sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t", sort_order=sort_order, optimize_for=optimize_for)
        set("//tmp/t/@min_compaction_store_count", 5)
        create("table", "//tmp/t_out")

        rows = [{"key": i, "value": str(i)} for i in range(10)]
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)
        self.sync_unmount_table("//tmp/t")

        map(
            in_="//tmp/t",
            out="//tmp/t_out",
            ordered=ordered,
            command="cat")

        assert_items_equal(read_table("//tmp/t_out"), rows)

        rows1 = [{"key": i, "value": str(i+1)} for i in range(3)]
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows1)
        self.sync_unmount_table("//tmp/t")

        rows2 = [{"key": i, "value": str(i+2)} for i in range(2,6)]
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows2)
        self.sync_unmount_table("//tmp/t")

        rows3 = [{"key": i, "value": str(i+3)} for i in range(7,8)]
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows3)
        self.sync_unmount_table("//tmp/t")

        assert len(get("//tmp/t/@chunk_ids")) == 4

        def update(new):
            def update_row(row):
                if sort_order == "ascending":
                    for r in rows:
                        if r["key"] == row["key"]:
                            r["value"] = row["value"]
                            return
                rows.append(row)
            for row in new:
                update_row(row)

        update(rows1)
        update(rows2)
        update(rows3)

        map(
            in_="//tmp/t",
            out="//tmp/t_out",
            ordered=ordered,
            command="cat")

        assert_items_equal(read_table("//tmp/t_out"), rows)

    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_sorted_dynamic_table_as_user_file(self, optimize_for):
        self.sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t", optimize_for=optimize_for)
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        rows = [{"key": i, "value": str(i)} for i in range(5)]
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)
        self.sync_unmount_table("//tmp/t")

        rows1 = [{"key": i, "value": str(i+1)} for i in range(3,8)]
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows1)
        self.sync_unmount_table("//tmp/t")

        write_table("//tmp/t_in", [{"a": "b"}])

        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            file=["<format=<format=text>yson>//tmp/t"],
            command="cat t",
            spec={
                "mapper": {
                    "format": yson.loads("<format=text>yson")
                }
            })

        def update(new):
            def update_row(row):
                for r in rows:
                    if r["key"] == row["key"]:
                        r["value"] = row["value"]
                        return
                rows.append(row)
            for row in new:
                update_row(row)

        update(rows1)
        rows = sorted(rows, key = lambda r: r["key"])
        assert read_table("//tmp/t_out") == rows

    def test_ordered_dynamic_table_as_user_file(self):
        self.sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t", sort_order=None)
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        rows = [{"key": i, "value": str(i)} for i in range(5)]
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)
        self.sync_unmount_table("//tmp/t")

        rows1 = [{"key": i, "value": str(i+1)} for i in range(3,8)]
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows1)
        self.sync_unmount_table("//tmp/t")

        write_table("//tmp/t_in", [{"a": "b"}])

        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            file=["<format=<format=text>yson>//tmp/t"],
            command="cat t",
            spec={
                "mapper": {
                    "format": yson.loads("<format=text>yson")
                }
            })

        assert read_table("//tmp/t_out") == rows + rows1

    def test_dynamic_table_timestamp(self):
        self.sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t")
        create("table", "//tmp/t_out")

        rows = [{"key": i, "value": str(i)} for i in range(2)]
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)

        time.sleep(1)
        ts = generate_timestamp()

        self.sync_flush_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i, "value": str(i+1)} for i in range(2)])
        self.sync_flush_table("//tmp/t")
        self.sync_compact_table("//tmp/t")

        map(
            in_="<timestamp=%s>//tmp/t" % ts,
            out="//tmp/t_out",
            command="cat")

        assert_items_equal(read_table("//tmp/t_out"), rows)

        with pytest.raises(YtError):
            map(
                in_="<timestamp=%s>//tmp/t" % MinTimestamp,
                out="//tmp/t_out",
                command="cat")

        insert_rows("//tmp/t", rows)

        with pytest.raises(YtError):
            map(
                in_="<timestamp=%s>//tmp/t" % generate_timestamp(),
                out="//tmp/t_out",
                command="cat")

    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_dynamic_table_input_data_statistics(self, optimize_for):
        self.sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t", optimize_for=optimize_for)
        create("table", "//tmp/t_out")

        rows = [{"key": i, "value": str(i)} for i in range(2)]
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)
        self.sync_unmount_table("//tmp/t")

        op = map(
            in_="//tmp/t",
            out="//tmp/t_out",
            command="cat")

        statistics = get("//sys/operations/{0}/@progress/job_statistics".format(op.id))
        assert get_statistics(statistics, "data.input.chunk_count.$.completed.map.sum") == 1
        assert get_statistics(statistics, "data.input.row_count.$.completed.map.sum") == 2
        assert get_statistics(statistics, "data.input.uncompressed_data_size.$.completed.map.sum") > 0
        assert get_statistics(statistics, "data.input.compressed_data_size.$.completed.map.sum") > 0
        assert get_statistics(statistics, "data.input.data_weight.$.completed.map.sum") > 0

    def test_dynamic_table_column_filter(self):
        self.sync_create_cells(1)
        create("table", "//tmp/t",
            attributes={
                "schema": make_schema([
                    {"name": "k", "type": "int64", "sort_order": "ascending"},
                    {"name": "u", "type": "int64"},
                    {"name": "v", "type": "int64"}],
                    unique_keys=True),
                "optimize_for": "scan",
                "external": False
            })
        create("table", "//tmp/t_out")

        row = {"k": 0, "u": 1, "v": 2}
        write_table("//tmp/t", [row])
        alter_table("//tmp/t", dynamic=True)

        def get_data_size(statistics):
            return {
                "uncompressed_data_size": get_statistics(statistics, "data.input.uncompressed_data_size.$.completed.map.sum"),
                "compressed_data_size": get_statistics(statistics, "data.input.compressed_data_size.$.completed.map.sum")
            }

        op = map(
            in_="//tmp/t",
            out="//tmp/t_out",
            command="cat")
        stat1 = get_data_size(get("//sys/operations/{0}/@progress/job_statistics".format(op.id)))
        assert read_table("//tmp/t_out") == [row]

        # FIXME(savrus) investigate test flapping
        print get("//tmp/t/@compression_statistics")

        for columns in (["k"], ["u"], ["v"], ["k", "u"], ["k", "v"], ["u", "v"]):
            op = map(
                in_="<columns=[{0}]>//tmp/t".format(";".join(columns)),
                out="//tmp/t_out",
                command="cat")
            stat2 = get_data_size(get("//sys/operations/{0}/@progress/job_statistics".format(op.id)))
            assert read_table("//tmp/t_out") == [{c: row[c] for c in columns}]

            if columns == ["u", "v"]:
                assert stat1["uncompressed_data_size"] == stat2["uncompressed_data_size"]
                assert stat1["compressed_data_size"] == stat2["compressed_data_size"]
            else:
                assert stat1["uncompressed_data_size"] > stat2["uncompressed_data_size"]
                assert stat1["compressed_data_size"] > stat2["compressed_data_size"]

    def test_output_to_dynamic_table_fails(self):
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_input",
                out="//tmp/t_output",
                command="cat")

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
        assert get_statistics(statistics, "user_job.pipes.output.0.bytes.$.completed.map.sum") ==15

    def test_writer_config(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out",
            attributes={
                "chunk_writer": {"block_size": 1024},
                "compression_codec": "none"
            })

        write_table("//tmp/t_in", [{"value": "A"*1024} for i in xrange(10)])

        map(
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"job_count": 1})

        chunks = get("//tmp/t_out/@chunk_ids")
        assert len(chunks) == 1
        assert get("#" + chunks[0] + "/@compressed_data_size") > 1024 * 10
        assert get("#" + chunks[0] + "/@max_block_size") < 1024 * 2

    @pytest.mark.parametrize("ordered", [False, True])
    def test_map_interrupt_job(self, ordered):
        create("table", "//tmp/in_1")
        write_table(
            "//tmp/in_1",
            [{"key": "%08d" % i, "value": "(t_1)", "data": "a" * (2 * 1024 * 1024)} for i in range(3)],
            table_writer = {
                "block_size": 1024,
                "desired_chunk_size": 1024})

        output = "//tmp/output"
        job_type = "map"
        if ordered:
            output = "<sorted_by=[key]>" + output
            job_type = "ordered_map"
        create("table", output)

        op = map(
            ordered=ordered,
            dont_track=True,
            wait_for_jobs=True,
            label="interrupt_job",
            in_="//tmp/in_1",
            out=output,
            precommand='read; echo "${REPLY/(???)/(job)}"; echo "$REPLY"',
            command="true",
            postcommand="cat",
            spec={
                "mapper": {
                    "format": "dsv"
                },
                "max_failed_job_count": 1,
                "job_io" : {
                    "buffer_row_count" : 1,
                },
                "enable_job_splitting": False,
            })

        interrupt_job(op.jobs[0])
        op.resume_jobs()
        op.track()

        result = read_table("//tmp/output", verbose=False)
        for row in result:
            print "key:", row["key"], "value:", row["value"]
        assert len(result) == 5
        if not ordered:
            result.sort()
        row_index = 0
        job_indexes = []
        for row in result:
            assert row["key"] == "%08d" % row_index
            if row["value"] == "(job)":
                job_indexes.append(int(row["key"]))
            else:
                row_index += 1
        assert job_indexes[1] > 0 and job_indexes[1] < 99999
        assert get("//sys/operations/{0}/@progress/job_statistics/data/input/row_count/$/completed/{1}/sum".format(op.id, job_type)) == len(result) - 2

    # YT-6324: false job interrupt when it does not consume any input data.
    @pytest.mark.parametrize("ordered", [False, True])
    def test_map_no_consumption(self, ordered):
        create("table", "//tmp/in_1")
        write_table(
            "//tmp/in_1",
            [{"key": "%08d" % i, "value": "(t_1)", "data": "a" * (2 * 1024 * 1024)} for i in range(3)],
            table_writer = {
                "block_size": 1024,
                "desired_chunk_size": 1024})

        output = "//tmp/output"
        if ordered:
            output = "<sorted_by=[key]>" + output
        create("table", output)

        op = map(
            ordered=ordered,
            dont_track=True,
            label="interrupt_job",
            in_="//tmp/in_1",
            out=output,
            command="true",
            spec={
                "mapper": {
                    "format": "dsv"
                },
                "max_failed_job_count": 1,
                "job_io" : {
                    "buffer_row_count" : 1,
                },
                "enable_job_splitting": False,
            })
        op.track()

        assert get("//sys/operations/{0}/@progress/jobs/completed/total".format(op.id)) == 1
        assert get("//sys/operations/{0}/@progress/jobs/completed/non-interrupted".format(op.id)) == 1

    def test_ordered_map_many_jobs(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        original_data = [{"index": i} for i in xrange(10)]
        for row in original_data:
            write_table("<append=true>//tmp/t_input", row)

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="cat; echo stderr 1>&2",
            ordered=True,
            spec={"data_size_per_job": 1})

        assert get("//sys/operations/" + op.id + "/jobs/@count") == 10
        assert read_table("//tmp/t_output") == original_data

    def test_ordered_map_remains_sorted(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        original_data = [{"key": i} for i in xrange(1000)]
        for i in xrange(10):
            write_table("<append=true>//tmp/t_input", original_data[100*i:100*(i+1)])

        op = map(
            in_="//tmp/t_input",
            out="<sorted_by=[key]>//tmp/t_output",
            command="cat; echo stderr 1>&2",
            ordered=True,
            spec={"job_count": 5})

        jobs = get("//sys/operations/" + op.id + "/jobs/@count")

        assert jobs == 5
        assert get("//tmp/t_output/@sorted")
        assert get("//tmp/t_output/@sorted_by") == ["key"]
        assert read_table("//tmp/t_output") == original_data

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

    def test_large_spec(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", [{"a": "b"}])

        with pytest.raises(YtError):
            map(in_="//tmp/t1", out="//tmp/t2", command="cat", spec={"attribute": "really_large" * (2 * 10 ** 6)}, verbose=False)

    # ToDo(psushin): uncomment and use parameter after YT-7064.
    # @pytest.mark.parametrize("ordered", [False, True])
    def test_map_job_splitter(self):
        create("table", "//tmp/in_1")
        write_table(
            "//tmp/in_1",
            [{"key": "%08d" % i, "value": "(t_1)", "data": "a" * (1024 * 1024)} for i in range(20)])

        input_ = ["//tmp/in_1"] * 5
        output = "//tmp/output"
        create("table", output)

        command="""
while read ROW; do
    if [ "$YT_JOB_INDEX" == 0 ]; then
        sleep 1
    else
        sleep 0.1
    fi
    echo "$ROW"
done
"""

        op = map(
            ordered=False,
            dont_track=True,
            label="split_job",
            in_=input_,
            out=output,
            command=command,
            spec={
                "mapper": {
                    "format": "dsv",
                },
                "data_size_per_job": 21 * 1024 * 1024,
                "max_failed_job_count": 1,
                "job_io": {
                    "buffer_row_count": 1,
                },
            })

        op.track()

        completed = get("//sys/operations/{0}/@progress/jobs/completed".format(op.id))
        interrupted = completed["interrupted"]
        assert completed["total"] >= 6
        assert interrupted["job_split"] >= 1
        expected = read_table("//tmp/in_1", verbose=False)
        for row in expected:
            del row["data"]
        got = read_table(output, verbose=False)
        for row in got:
            del row["data"]
        assert sorted(got) == sorted(expected * 5)

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



##################################################################

@porto_env_only
class TestSchedulerMapCommandsPorto(TestSchedulerMapCommands):
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
            "operations_update_period" : 10
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

class TestSchedulerOperationNodeFlush(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler" : {
            "watchers_update_period" : 100,
            "operations_update_period" : 10
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

@unix_only
class TestJobQuery(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "udf_registry_path": "//tmp/udfs"
        }
    }

    def _init_udf_registry(self):
        registry_path =  "//tmp/udfs"
        create("map_node", registry_path)

        abs_path = os.path.join(registry_path, "abs_udf")
        create(
            "file", abs_path,
            attributes={"function_descriptor": {
                "name": "abs_udf",
                "argument_types": [{
                    "tag": "concrete_type",
                    "value": "int64"}],
                "result_type": {
                    "tag": "concrete_type",
                    "value": "int64"},
                "calling_convention": "simple"}})

        abs_impl_path = self._find_ut_file("test_udfs.bc")
        write_local_file(abs_path, abs_impl_path)

    def test_query_simple(self):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "string"}]
        })
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        map(in_="//tmp/t1", out="//tmp/t2", command="cat",
            spec={"input_query": "a"})

        assert read_table("//tmp/t2") == [{"a": "b"}]

    def test_query_two_input_tables(self):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "string"},
                       {"name": "b", "type": "string"}]
        })
        create("table", "//tmp/t2", attributes={
            "schema": [{"name": "a", "type": "string"},
                       {"name": "c", "type": "string"}]
        })
        create("table", "//tmp/t_out")
        write_table("//tmp/t1", {"a": "1", "b": "1"})
        write_table("//tmp/t2", {"a": "2", "c": "2"})

        map(in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t_out", command="cat",
            spec={"input_query": "*"})

        expected = [{"a": "1", "b": "1", "c": None}, {"a": "2", "b": None, "c": "2"}]
        assert_items_equal(read_table("//tmp/t_out"), expected)

    def test_query_reader_projection(self):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "string"}, {"name": "c", "type": "string"}]
        })
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b", "c": "d"})

        map(in_="//tmp/t1", out="//tmp/t2", command="cat",
            spec={"input_query": "a"})

        assert read_table("//tmp/t2") == [{"a": "b"}]

    @pytest.mark.parametrize("mode", ["ordered", "unordered"])
    def test_query_filtering(self, mode):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": i} for i in xrange(2)])

        map(in_="//tmp/t1", out="//tmp/t2", command="cat", mode=mode,
            spec={"input_query": "a where a > 0"})

        assert read_table("//tmp/t2") == [{"a": 1}]

    def test_query_asterisk(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        rows = [
            {"a": 1, "b": 2, "c": 3},
            {"b": 5, "c": 6},
            {"a": 7, "c": 8}]
        write_table("//tmp/t1", rows)

        schema = [{"name": "z", "type": "int64"},
            {"name": "a", "type": "int64"},
            {"name": "y", "type": "int64"},
            {"name": "b", "type": "int64"},
            {"name": "x", "type": "int64"},
            {"name": "c", "type": "int64"},
            {"name": "u", "type": "int64"}]

        for row in rows:
            for column in schema:
                if column["name"] not in row.keys():
                    row[column["name"]] = None

        map(in_="//tmp/t1", out="//tmp/t2", command="cat",
            spec={
                "input_query": "* where a > 0 or b > 0",
                "input_schema": schema})

        assert_items_equal(read_table("//tmp/t2"), rows)

    def test_query_schema_in_spec(self):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "string"}]
        })
        create("table", "//tmp/t2", attributes={
            "schema": [{"name": "a", "type": "string"}, {"name": "b", "type": "string"}]
        })
        create("table", "//tmp/t_out")
        write_table("//tmp/t1", {"a": "b"})
        write_table("//tmp/t2", {"a": "b"})

        map(in_="//tmp/t1", out="//tmp/t_out", command="cat",
            spec={"input_query": "*", "input_schema": [{"name": "a", "type": "string"}, {"name": "b", "type": "string"}]})

        assert read_table("//tmp/t_out") == [{"a": "b", "b": None}]

        map(in_="//tmp/t2", out="//tmp/t_out", command="cat",
            spec={"input_query": "*", "input_schema": [{"name": "a", "type": "string"}]})

        assert read_table("//tmp/t_out") == [{"a": "b"}]

    def test_query_udf(self):
        self._init_udf_registry()

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": i} for i in xrange(-1,1)])

        map(in_="//tmp/t1", out="//tmp/t2", command="cat",
            spec={"input_query": "a where abs_udf(a) > 0", "input_schema": [{"name": "a", "type": "int64"}]})

        assert read_table("//tmp/t2") == [{"a": -1}]

    def test_query_wrong_schema(self):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "string"}]
        })
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        with pytest.raises(YtError):
            map(in_="//tmp/t1", out="//tmp/t2", command="cat",
                spec={"input_query": "a", "input_schema": [{"name": "a", "type": "int64"}]})

    def test_query_range_inference(self):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}]
        })
        create("table", "//tmp/t_out")
        for i in range(3):
            write_table("<append=%true>//tmp/t", [{"a": i * 10 + j} for j in xrange(3)])
        assert get("//tmp/t/@chunk_count") == 3

        def _test(selector, query, rows, chunk_count):
            op = map(
                in_="//tmp/t" + selector,
                out="//tmp/t_out",
                command="cat",
                spec={"input_query": query})

            assert_items_equal(read_table("//tmp/t_out"), rows)
            statistics = get("//sys/operations/{0}/@progress/job_statistics".format(op.id))
            assert get_statistics(statistics, "data.input.chunk_count.$.completed.map.sum") == chunk_count

        _test("", "a where a between 5 and 15", [{"a": i} for i in xrange(10, 13)], 1)
        _test("[#0:]", "a where a between 5 and 15", [{"a": i} for i in xrange(10, 13)], 1)
        _test("[11:12]", "a where a between 5 and 15", [{"a": i} for i in xrange(11, 12)], 1)
        _test("[9:20]", "a where a between 5 and 15", [{"a": i} for i in xrange(10, 13)], 1)
        _test("[#2:#4]", "a where a <= 10", [{"a": 2}, {"a": 10}], 2)
        _test("[10]", "a where a > 0", [{"a": 10}], 1)


##################################################################

class TestSchedulerMapCommandsMulticell(TestSchedulerMapCommands):
    NUM_SECONDARY_MASTER_CELLS = 2

    def test_multicell_input_fetch(self):
        create("table", "//tmp/t1", attributes={"external_cell_tag": 1})
        write_table("//tmp/t1", [{"a": 1}])
        create("table", "//tmp/t2", attributes={"external_cell_tag": 2})
        write_table("//tmp/t2", [{"a": 2}])

        create("table", "//tmp/t_in", attributes={"external": False})
        merge(mode="ordered",
              in_=["//tmp/t1", "//tmp/t2"],
              out="//tmp/t_in")

        create("table", "//tmp/t_out")
        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="cat")

        assert_items_equal(read_table("//tmp/t_out"), [{"a": 1}, {"a": 2}])


##################################################################

class TestSandboxTmpfs(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    cgroups_delta_node_config = {
        "exec_agent": {
            "slot_manager": {
                "enforce_job_control": True,           # <= 18.3
                "job_environment" : {                  # >= 18.4
                    "enfoce_job_control" : True
                }
            }
        }
    }

    def test_simple(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="cat; echo 'content' > tmpfs/file; ls tmpfs/ >&2; cat tmpfs/file >&2;",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": "tmpfs",
                }
            })

        jobs_path = "//sys/operations/" + op.id + "/jobs"
        assert get(jobs_path + "/@count") == 1
        words = read_file(jobs_path + "/" + ls(jobs_path)[0] + "/stderr").strip().split()
        assert ["file", "content"] == words

    def test_custom_tmpfs_path(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="cat; echo 'content' > my_dir/file; ls my_dir/ >&2; cat my_dir/file >&2;",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": "my_dir",
                }
            })

        jobs_path = "//sys/operations/" + op.id + "/jobs"
        assert get(jobs_path + "/@count") == 1
        words = read_file(jobs_path + "/" + ls(jobs_path)[0] + "/stderr").strip().split()
        assert ["file", "content"] == words

    def test_dot_tmpfs_path(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="cat; mkdir my_dir; echo 'content' > my_dir/file; ls my_dir/ >&2; cat my_dir/file >&2;",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": ".",
                }
            })

        jobs_path = "//sys/operations/" + op.id + "/jobs"
        assert get(jobs_path + "/@count") == 1
        words = read_file(jobs_path + "/" + ls(jobs_path)[0] + "/stderr").strip().split()
        assert ["file", "content"] == words

        create("file", "//tmp/test_file")
        write_file("//tmp/test_file", "".join(["0"] * (1024 * 1024 + 1)))
        map(command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": ".",
                    "file_paths": ["//tmp/test_file"]
                }
            })

        map(command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": "./",
                    "file_paths": ["//tmp/test_file"]
                }
            })

        script = "#!/usr/bin/env python\n"\
                 "import sys\n"\
                 "sys.stdout.write(sys.stdin.read())\n"\
                 "with open('test_file', 'w') as f: f.write('Hello world!')"
        create("file", "//tmp/script")
        write_file("//tmp/script", script)
        set("//tmp/script/@executable", True)

        map(command="./script.py",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 100 * 1024 * 1024,
                    "tmpfs_path": ".",
                    "copy_files": True,
                    "file_paths": ["//tmp/test_file", to_yson_type("//tmp/script", attributes={"file_name": "script.py"})]
                }
            })

        with pytest.raises(YtError):
            map(command="cat; cp test_file local_file;",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": ".",
                        "file_paths": ["//tmp/test_file"]
                    },
                    "max_failed_job_count": 1,
                })

        op = map(command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024 + 10000,
                    "tmpfs_path": ".",
                    "file_paths": ["//tmp/test_file"],
                    "copy_files": True,
                },
                "max_failed_job_count": 1,
            })

        statistics = get("//sys/operations/{0}/@progress/job_statistics".format(op.id))
        tmpfs_size = get_statistics(statistics, "user_job.tmpfs_size.$.completed.map.sum")
        assert 0.9 * 1024 * 1024 <= tmpfs_size <= 1.1 * 1024 * 1024

        with pytest.raises(YtError):
            map(command="cat",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": ".",
                        "file_paths": ["//tmp/test_file"],
                        "copy_files": True,
                    },
                    "max_failed_job_count": 1,
                })

    def test_incorrect_tmpfs_path(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_input", out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": "../",
                    }
                })

        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_input", out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": "/tmp",
                    }
                })


    def test_tmpfs_remove_failed(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        with pytest.raises(YtError):
            map(command="cat; rm -rf tmpfs",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": "tmpfs",
                    },
                    "max_failed_job_count": 1
                })

    def test_tmpfs_size_limit(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        with pytest.raises(YtError):
            map(command="set -e; cat; dd if=/dev/zero of=tmpfs/file bs=1100000 count=1",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024
                    },
                    "max_failed_job_count": 1
                })

    def test_memory_reserve_and_tmpfs(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(command="python -c 'import time; x = \"0\" * (200 * 1000 * 1000); time.sleep(2)'",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_path": "tmpfs",
                    "memory_limit": 250 * 1000 * 1000
                },
                "max_failed_job_count": 1
            })

        assert get("//sys/operations/{0}/@progress/jobs/aborted/total".format(op.id)) == 0

##################################################################

@porto_env_only
class TestSandboxTmpfsPorto(TestSandboxTmpfs):
    DELTA_NODE_CONFIG = porto_delta_node_config
    USE_PORTO_FOR_SERVERS = True

##################################################################

class TestDisabledSandboxTmpfs(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "slot_manager": {
                "enable_tmpfs": False
            }
        }
    }

    def test_simple(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="cat; echo 'content' > tmpfs/file; ls tmpfs/ >&2; cat tmpfs/file >&2;",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": "tmpfs",
                }
            })

        jobs_path = "//sys/operations/" + op.id + "/jobs"
        assert get(jobs_path + "/@count") == 1
        words = read_file(jobs_path + "/" + ls(jobs_path)[0] + "/stderr").strip().split()
        assert ["file", "content"] == words


##################################################################

class TestFilesInSandbox(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "static_orchid_cache_update_period": 100,
        }
    }

    def test_operation_abort_with_lost_file(self):
        create("file", "//tmp/script", attributes={"replication_factor": 1, "executable": True})
        write_file("//tmp/script", "#!/bin/bash\ncat")

        chunk_ids = get("//tmp/script/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        replicas = get("#{0}/@stored_replicas".format(chunk_id))
        assert len(replicas) == 1
        replica_to_ban = str(replicas[0]) # str() is for attribute stripping.

        banned = False
        for node in ls("//sys/nodes"):
            if node == replica_to_ban:
                set("//sys/nodes/{0}/@banned".format(node), True)
                banned = True
        assert banned

        time.sleep(1)
        assert get("#{0}/@replication_status/default/lost".format(chunk_id))

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})
        op = map(dont_track=True,
                 command="./script",
                 in_="//tmp/t_input",
                 out="//tmp/t_output",
                 spec={
                     "mapper": {
                         "file_paths": ["//tmp/script"]
                     }
                 })

        while True:
            if op.get_job_count("running") == 1:
                break
            time.sleep(0.5)

        time.sleep(1)
        op.abort()

        time.sleep(1)
        assert op.get_state() == "aborted"
        assert assert_almost_equal(get("//sys/scheduler/orchid/scheduler/cell/resource_usage/cpu"), 0)


##################################################################

class TestJobSizeAdjuster(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
      "scheduler" : {
        "map_operation_options" : {
          "data_size_per_job" : 1
        }
      }
    }

    @flaky(max_runs=5)
    def test_map_job_size_adjuster_boost(self):
        create("table", "//tmp/t_input")
        original_data = [{"index": "%05d" % i} for i in xrange(31)]
        for row in original_data:
            write_table("<append=true>//tmp/t_input", row, verbose=False)

        create("table", "//tmp/t_output")

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="echo lines=`wc -l`",
            spec={
                "mapper": {"format": "dsv"},
                "resource_limits": {"user_slots": 1}
            })

        expected = [{"lines": str(2**i)} for i in xrange(5)]
        actual = read_table("//tmp/t_output")
        assert_items_equal(actual, expected)
        estimated = get("//sys/operations/{0}/@progress/estimated_input_data_size_histogram".format(op.id))
        histogram = get("//sys/operations/{0}/@progress/input_data_size_histogram".format(op.id))
        assert estimated == histogram
        assert histogram["max"]/histogram["min"] == 16
        assert histogram["count"][0] == 1
        assert sum(histogram["count"]) == 5

    def test_map_job_size_adjuster_max_limit(self):
        create("table", "//tmp/t_input")
        original_data = [{"index": "%05d" % i} for i in xrange(31)]
        for row in original_data:
            write_table("<append=true>//tmp/t_input", row, verbose=False)
        chunk_ids = get("//tmp/t_input/@chunk_ids")
        chunk_id = chunk_ids[0]
        chunk_size = get("#{0}/@uncompressed_data_size".format(chunk_id))
        create("table", "//tmp/t_output")

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="echo lines=`wc -l`",
            spec={
                "mapper": {"format": "dsv"},
                "max_data_size_per_job": chunk_size * 4,
                "resource_limits": {"user_slots": 3}
            })

        for row in read_table("//tmp/t_output"):
            assert int(row["lines"]) < 5

    def test_map_unavailable_chunk(self):
        create("table", "//tmp/t_input", attributes={"replication_factor": 1})
        original_data = [{"index": "%05d" % i} for i in xrange(20)]
        write_table("<append=true>//tmp/t_input", original_data[0], verbose=False)
        chunk_ids = get("//tmp/t_input/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        chunk_size = get("#{0}/@uncompressed_data_size".format(chunk_id))
        replicas = get("#{0}/@stored_replicas".format(chunk_id))
        assert len(replicas) == 1
        replica_to_ban = str(replicas[0]) # str() is for attribute stripping.

        banned = False
        for node in ls("//sys/nodes"):
            if node == replica_to_ban:
                set("//sys/nodes/{0}/@banned".format(node), True)
                banned = True
        assert banned

        time.sleep(1)
        assert get("#{0}/@replication_status/default/lost".format(chunk_id))

        for row in original_data[1:]:
            write_table("<append=true>//tmp/t_input", row, verbose=False)
        chunk_ids = get("//tmp/t_input/@chunk_ids")
        assert len(chunk_ids) == len(original_data)

        create("table", "//tmp/t_output")
        op = map(dont_track=True,
                 command="sleep $YT_JOB_INDEX; cat",
                 in_="//tmp/t_input",
                 out="//tmp/t_output",
                 spec={
                     "data_size_per_job": chunk_size * 2
                 })

        while True:
            time.sleep(0.2)
            if op.get_job_count("completed") > 3:
                break

        unbanned = False
        for node in ls("//sys/nodes"):
            if node == replica_to_ban:
                set("//sys/nodes/{0}/@banned".format(node), False)
                unbanned = True
        assert unbanned

        op.track()
        assert op.get_state() == "completed"
