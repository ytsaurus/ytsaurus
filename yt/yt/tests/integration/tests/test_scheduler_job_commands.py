from yt_env_setup import (
    YTEnvSetup, unix_only, porto_avaliable, wait,
    get_porto_delta_node_config
)
from yt_commands import *

from flaky import flaky

import yt.common

import pytest
import time

##################################################################

@pytest.mark.skip_if('not porto_avaliable()')
class TestJobProber(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = yt.common.update(
        get_porto_delta_node_config(),
        {
            "exec_agent": {
                "test_poll_job_shell": True,
            },
        }
    )

    USE_PORTO_FOR_SERVERS = True
    REQUIRE_YTSERVER_ROOT_PRIVILEGES = True

    @authors("ignat")
    @unix_only
    def test_abandon_job(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in xrange(5):
            write_table("<append=true>//tmp/t1", {"key": str(i), "value": "foo"})

        op = map(
            track=False,
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

    @authors("ignat")
    @unix_only
    def test_abandon_job_sorted_empty_output(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("<append=true>//tmp/t1", {"key": "foo", "value": "bar"})

        op = map(
            track=False,
            label="abandon_job",
            in_="//tmp/t1",
            out="<sorted_by=[key]>//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"))

        jobs = wait_breakpoint()
        abandon_job(jobs[0])

        op.track()
        assert len(read_table("//tmp/t2")) == 0

    @authors("ignat")
    @unix_only
    def test_abandon_job_permissions(self):
        create_user("u1")
        create_user("u2")

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in xrange(5):
            write_table("<append=true>//tmp/t1", {"key": str(i), "value": "foo"})

        op = map(
            track=False,
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

    @authors("ignat")
    def test_poll_job_shell(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"key": "foo"})

        op = map(
            track=False,
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

    @authors("ignat")
    def test_poll_job_shell_command(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"key": "foo"})

        op = map(
            track=False,
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

    @authors("gritukan")
    def test_poll_job_shell_command_large_output(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"key": "foo"})

        op = map(
            track=False,
            label="poll_job_shell",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"))
        job_id = wait_breakpoint()[0]

        r = poll_job_shell(job_id, operation="spawn", command="for((i=0;i<100000;i++)); do echo A; done")
        shell_id = r["shell_id"]
        output = self._poll_until_shell_exited(job_id, shell_id)

        expected = "A\r\n" * 10**5
        assert output == expected

    @authors("ignat")
    def test_poll_job_shell_permissions(self):
        create_user("u1")
        create_user("u2")

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"key": "foo"})

        op = map(
            track=False,
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

    @authors("gritukan")
    @pytest.mark.parametrize("enable_secure_vault_variables_in_job_shell", [False, True])
    def test_poll_job_shell_secret_vault(self, enable_secure_vault_variables_in_job_shell):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"key": "foo"})

        op = map(
            track=False,
            label="poll_job_shell",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={
                "mapper": {
                    "environment": {
                        "YT_NOT_SECRET": "not_secret_data",
                    },
                },
                "secure_vault": {
                    "MY_SECRET": "super_secret_data",
                },
                "enable_secure_vault_variables_in_job_shell": enable_secure_vault_variables_in_job_shell,
            })
        job_id = wait_breakpoint()[0]

        r = poll_job_shell(job_id, operation="spawn", command="echo $YT_NOT_SECRET; echo $YT_SECURE_VAULT_MY_SECRET")
        shell_id = r["shell_id"]
        output = self._poll_until_shell_exited(job_id, shell_id)

        if enable_secure_vault_variables_in_job_shell:
            expected = "not_secret_data\r\nsuper_secret_data\r\n"
        else:
            expected = "not_secret_data\r\n\r\n"

        assert output == expected

    @authors("ignat")
    @unix_only
    def test_abort_job(self):
        time.sleep(2)
        start_profiling = get_job_count_profiling()

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in xrange(5):
            write_table("<append=true>//tmp/t1", {"key": str(i), "value": "foo"})

        op = map(
            track=False,
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
        assert get(op.get_path() + "/@progress/jobs/aborted/total") == 1
        assert get(op.get_path() + "/@progress/jobs/aborted/scheduled/user_request") == 1
        assert get(op.get_path() + "/@progress/jobs/aborted/scheduled/other") == 0
        assert get(op.get_path() + "/@progress/jobs/failed") == 0

        def check():
            end_profiling = get_job_count_profiling()

            for state in end_profiling["state"]:
                print_debug(state, start_profiling["state"][state], end_profiling["state"][state])
                value = end_profiling["state"][state] - start_profiling["state"][state]
                count = 0
                if state == "aborted":
                    count = 1
                if state == "completed":
                    count = 5
                if value != count:
                    return False

            for abort_reason in end_profiling["abort_reason"]:
                print_debug(abort_reason, start_profiling["abort_reason"][abort_reason], end_profiling["abort_reason"][abort_reason])
                value = end_profiling["abort_reason"][abort_reason] - start_profiling["abort_reason"][abort_reason]
                if value != (1 if abort_reason == "user_request" else 0):
                    return False
            return True
        wait(check)

##################################################################

class TestJobProberRpcProxy(TestJobProber):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True

