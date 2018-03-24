from yt_env_setup import YTEnvSetup, unix_only, patch_porto_env_only, wait
from yt_commands import *

from flaky import flaky

import pytest
import time
from collections import defaultdict

##################################################################

# This is a mix of options for 18.4 and 18.5
cgroups_delta_node_config = {
    "exec_agent": {
        "enable_cgroups": True,                                       # <= 18.4
        "supported_cgroups": ["cpuacct", "blkio", "cpu"],             # <= 18.4
        "slot_manager": {
            "enforce_job_control": True,                              # <= 18.4
            "job_environment": {
                "type": "cgroups",                                   # >= 18.5
                "supported_cgroups": [                                # >= 18.5
                    "cpuacct",
                    "blkio",
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
            label="strace_job",
            in_="//tmp/t1",
            out="//tmp/t2",
            command="{notify_running} ; sleep 5000".format(notify_running=events_on_fs().notify_event_cmd("job_is_running")))

        events_on_fs().wait_event("job_is_running")
        jobs = ls("//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op.id))
        time.sleep(1.0) # give job proxy some time to send a heartbeat
        result = strace_job(jobs[0])

        assert len(result) > 0
        for pid, trace in result["traces"].iteritems():
            assert trace["trace"].startswith("Process {0} attached".format(pid))
            assert "process_command_line" in trace
            assert "process_name" in trace

    @unix_only
    def test_signal_job_with_no_job_restart(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            dont_track=True,
            label="signal_job_with_no_job_restart",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("""(trap "echo got=SIGUSR1" USR1 ; trap "echo got=SIGUSR2" USR2 ; cat ; BREAKPOINT)"""),
            spec={
                "mapper": {
                    "format": "dsv"
                },
                "max_failed_job_count": 1
            })

        jobs = wait_breakpoint()

        time.sleep(1.0) # give job proxy some time to send a heartbeat
        signal_job(jobs[0], "SIGUSR1")
        signal_job(jobs[0], "SIGUSR2")

        release_breakpoint()
        op.track()

        assert get("//sys/operations/{0}/@progress/jobs/aborted/total".format(op.id)) == 0
        assert get("//sys/operations/{0}/@progress/jobs/failed".format(op.id)) == 0
        assert read_table("//tmp/t2") == [{"foo": "bar"}, {"got": "SIGUSR1"}, {"got": "SIGUSR2"}]

    @unix_only
    def test_signal_job_with_job_restart(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            dont_track=True,
            label="signal_job_with_job_restart",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("""(trap "echo got=SIGUSR1; echo stderr >&2; exit 1" USR1 ; cat ; BREAKPOINT)"""),
            spec={
                "mapper": {
                    "format": "dsv"
                },
                "max_failed_job_count": 1
            })

        jobs = wait_breakpoint()

        time.sleep(1.0) # give job proxy some time to send a heartbeat
        signal_job(jobs[0], "SIGUSR1")
        release_breakpoint()

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
            label="abandon_job",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={
                "data_size_per_job": 1
            })

        jobs = wait_breakpoint(job_count=5)
        abandon_job(jobs[0])

        release_breakpoint()
        op.track()
        assert len(read_table("//tmp/t2")) == 4

    @unix_only
    def test_abandon_job_sorted_empty_output(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("<append=true>//tmp/t1", {"key": "foo", "value": "bar"})

        op = map(
            dont_track=True,
            label="abandon_job",
            in_="//tmp/t1",
            out="<sorted_by=[key]>//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"))

        jobs = wait_breakpoint()
        abandon_job(jobs[0])

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
            label="abandon_job",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={
                "data_size_per_job": 1
            },
            authenticated_user="u1")
        jobs = wait_breakpoint(job_count=5)

        with pytest.raises(YtError):
            abandon_job(jobs[0], authenticated_user="u2")

        release_breakpoint()
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
            label="poll_job_shell",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("BREAKPOINT ; cat"))
        job_id = wait_breakpoint()[0]

        r = poll_job_shell(job_id, operation="spawn", term="screen-256color", height=50, width=132)
        shell_id = r["shell_id"]
        self._poll_until_prompt(job_id, shell_id)

        command = "echo $TERM; tput lines; tput cols; env | grep -c YT_OPERATION_ID\r"
        self._send_keys(job_id, shell_id, command, 0)
        output = self._poll_until_prompt(job_id, shell_id)

        expected = "{0}\nscreen-256color\r\n50\r\n132\r\n1".format(command)
        assert output.startswith(expected)

        poll_job_shell(job_id, operation="terminate", shell_id=shell_id)
        with pytest.raises(YtError):
            self._poll_until_prompt(job_id, shell_id)

        abandon_job(job_id)

        op.track()
        assert len(read_table("//tmp/t2")) == 0

    def test_poll_job_shell_command(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"key": "foo"})

        op = map(
            dont_track=True,
            label="poll_job_shell",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"))
        job_id = wait_breakpoint()[0]

        r = poll_job_shell(job_id, operation="spawn", command="echo $TERM; tput lines; tput cols; env | grep -c YT_OPERATION_ID")
        shell_id = r["shell_id"]
        output = self._poll_until_shell_exited(job_id, shell_id)

        expected = "xterm\r\n24\r\n80\r\n1\r\n"
        assert output == expected

        poll_job_shell(job_id, operation="terminate", shell_id=shell_id)
        with pytest.raises(YtError):
            self._poll_until_prompt(job_id, shell_id)

        abandon_job(job_id)

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
            label="poll_job_shell",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            authenticated_user="u1")

        job_id = wait_breakpoint()[0]
        with pytest.raises(YtError):
            poll_job_shell(
                job_id,
                operation="spawn",
                term="screen-256color",
                height=50,
                width=132,
                authenticated_user="u2")

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
            label="abort_job",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={
                "data_size_per_job": 1
            })

        jobs = wait_breakpoint(job_count=5)
        abort_job(jobs[0])

        release_breakpoint()
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

@patch_porto_env_only(TestJobProber)
class TestJobProberPorto(YTEnvSetup):
    DELTA_NODE_CONFIG = porto_delta_node_config
    USE_PORTO_FOR_SERVERS = True
