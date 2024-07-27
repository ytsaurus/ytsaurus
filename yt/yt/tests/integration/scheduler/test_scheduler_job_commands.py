from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, print_debug, wait, wait_no_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    create, get, create_user,
    create_group, add_member, read_table, write_table, map, merge,
    run_test_vanilla, abort_job, abandon_job, update_op_parameters,
    poll_job_shell, raises_yt_error)

from yt_helpers import get_job_count_profiling

import yt_error_codes

from yt.common import YtError, YtResponseError

import pytest

import binascii
import time

##################################################################


class TestJobProber(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "node_shard_count": 1,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "test_poll_job_shell": True,
            },
        },
    }

    @authors("ignat")
    def test_abandon_job(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in range(5):
            write_table("<append=true>//tmp/t1", {"key": str(i), "value": "foo"})

        op = map(
            track=False,
            label="abandon_job",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"data_size_per_job": 1},
        )

        jobs = wait_breakpoint(job_count=5)
        abandon_job(jobs[0])

        release_breakpoint()
        op.track()
        assert len(read_table("//tmp/t2")) == 4

    @authors("ignat")
    def test_abandon_job_sorted_empty_output(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("<append=true>//tmp/t1", {"key": "foo", "value": "bar"})

        op = map(
            track=False,
            label="abandon_job",
            in_="//tmp/t1",
            out="<sorted_by=[key]>//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
        )

        jobs = wait_breakpoint()
        abandon_job(jobs[0])

        op.track()
        assert len(read_table("//tmp/t2")) == 0

    @authors("pogorelov")
    def test_abandon_job_failure(self):
        create(
            "table",
            "//tmp/t_in",
            attributes={"schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}]},
        )
        create("table", "//tmp/t_out")

        rows = [{"a": i} for i in range(25)]
        for row in rows:
            write_table("<append=%true>//tmp/t_in", row)
        op = merge(
            track=False,
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            spec={
                "force_transform": True,
                "mode": "sorted",
                "job_io": {
                    "testing_options": {"pipe_delay": 250},
                    "buffer_row_count": 1,
                },
                "enable_job_splitting": False,
            },
        )
        wait(lambda: len(op.get_running_jobs()) > 0)
        jobs = list(op.get_running_jobs())
        assert len(jobs) >= 1
        job_id = jobs[0]

        try:
            abandon_job(job_id)
        except YtResponseError:
            pass

        op.track()

    @authors("ignat")
    def test_abandon_job_permissions(self):
        create_user("u1")
        create_user("u2")

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in range(5):
            write_table("<append=true>//tmp/t1", {"key": str(i), "value": "foo"})

        op = map(
            track=False,
            label="abandon_job",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"data_size_per_job": 1},
            authenticated_user="u1",
        )
        jobs = wait_breakpoint(job_count=5)

        with pytest.raises(YtError):
            abandon_job(jobs[0], authenticated_user="u2")

        release_breakpoint()
        op.track()
        assert len(read_table("//tmp/t2")) == 5

    def _poll_until_prompt(self, job_id, shell_id):
        output = ""
        while len(output) < 4 or (output[-4:] != ":~$ " and output[-4:] != ":~# "):
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
            keys=binascii.hexlify(keys.encode("ascii")),
            input_offset=input_offset,
        )

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
            command=with_breakpoint("BREAKPOINT ; cat"),
        )
        job_id = wait_breakpoint()[0]
        wait(lambda: op.get_job_phase(job_id) == "running")

        r = poll_job_shell(job_id, operation="spawn", term="dumb", height=50, width=132)
        shell_id = r["shell_id"]
        self._poll_until_prompt(job_id, shell_id)

        command = "echo $TERM; tput lines; tput cols; env | grep -c YT_OPERATION_ID\r"
        self._send_keys(job_id, shell_id, command, 0)
        output = self._poll_until_prompt(job_id, shell_id)

        expected = "{0}\ndumb\r\n50\r\n132\r\n1".format(command)
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
            command=with_breakpoint("cat ; BREAKPOINT"),
        )
        job_id = wait_breakpoint()[0]
        wait(lambda: op.get_job_phase(job_id) == "running")

        r = poll_job_shell(
            job_id,
            operation="spawn",
            command="echo $TERM; tput lines; tput cols; env | grep -c YT_OPERATION_ID",
        )
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
            command=with_breakpoint("cat ; BREAKPOINT"),
        )
        job_id = wait_breakpoint()[0]
        wait(lambda: op.get_job_phase(job_id) == "running")

        r = poll_job_shell(
            job_id,
            operation="spawn",
            command="for((i=0;i<100000;i++)); do echo A; done",
        )
        shell_id = r["shell_id"]
        output = self._poll_until_shell_exited(job_id, shell_id)

        expected = "A\r\n" * 10 ** 5
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
            authenticated_user="u1",
        )

        job_id = wait_breakpoint()[0]
        wait(lambda: op.get_job_phase(job_id) == "running")

        with pytest.raises(YtError):
            poll_job_shell(
                job_id,
                operation="spawn",
                term="screen-256color",
                height=50,
                width=132,
                authenticated_user="u2",
            )

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
            },
        )
        job_id = wait_breakpoint()[0]
        wait(lambda: op.get_job_phase(job_id) == "running")

        r = poll_job_shell(
            job_id,
            operation="spawn",
            command="echo $YT_NOT_SECRET; echo $YT_SECURE_VAULT_MY_SECRET",
        )
        shell_id = r["shell_id"]
        output = self._poll_until_shell_exited(job_id, shell_id)

        if enable_secure_vault_variables_in_job_shell:
            expected = "not_secret_data\r\nsuper_secret_data\r\n"
        else:
            expected = "not_secret_data\r\n\r\n"

        assert output == expected

    @authors("ignat")
    def test_abort_job(self):
        time.sleep(2)
        start_profiling = get_job_count_profiling()

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in range(5):
            write_table("<append=true>//tmp/t1", {"key": str(i), "value": "foo"})

        op = map(
            track=False,
            label="abort_job",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"data_size_per_job": 1},
        )

        jobs = wait_breakpoint(job_count=5)
        abort_job(jobs[0])

        release_breakpoint()
        op.track()

        assert len(read_table("//tmp/t2")) == 5
        assert get(op.get_path() + "/@progress/jobs/aborted/total") == 1
        assert get(op.get_path() + "/@progress/jobs/aborted/scheduled/user_request") == 1
        assert get(op.get_path() + "/@progress/jobs/aborted/scheduled/other") == 0
        assert get(op.get_path() + "/@progress/jobs/failed") == 0

        @wait_no_assert
        def check():
            end_profiling = get_job_count_profiling()

            for state in end_profiling["state"]:
                print_debug(
                    state,
                    start_profiling["state"][state],
                    end_profiling["state"][state],
                )
                value = end_profiling["state"][state] - start_profiling["state"][state]
                count = 0
                if state == "aborted":
                    count = 1
                if state == "completed":
                    count = 5
                assert value == count

            for abort_reason in end_profiling["abort_reason"]:
                print_debug(
                    abort_reason,
                    start_profiling["abort_reason"][abort_reason],
                    end_profiling["abort_reason"][abort_reason],
                )
                value = end_profiling["abort_reason"][abort_reason] - start_profiling["abort_reason"][abort_reason]
                assert value == (1 if abort_reason == "user_request" else 0)


class TestJobProberPorto(TestJobProber):
    USE_PORTO = True


##################################################################


class TestJobShellInSubcontainer(TestJobProber):
    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "test_poll_job_shell": True,
            },
        },
    }

    # NB(gritukan): Setting an arbitrary user to user job
    # will end up with problems with permissions during subsubcontainer start.
    # On real clusters all the slot and job proxy users are in the same group,
    # so these manipulations with containers are allowed. It's impossible to
    # make such a configuration in tests, so we simply run job proxy and user job
    # under the same user.
    USE_SLOT_USER_ID = False

    @authors("gritukan")
    def test_job_shell_in_subcontainer(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"key": "foo"})

        create_user("nirvana_boss")
        create_user("nirvana_dev")
        create_group("nirvana_devs")
        create_user("taxi_boss")
        create_user("taxi_dev")
        create_group("taxi_devs")
        create_user("yt_dev")

        add_member("nirvana_dev", "nirvana_devs")
        add_member("taxi_dev", "taxi_devs")
        add_member("yt_dev", "superusers")

        op = run_test_vanilla(
            with_breakpoint("portoctl create N && BREAKPOINT"),
            spec={
                "enable_porto": "isolate",
                "job_shells": [
                    {
                        "name": "default",
                        "subcontainer": "",
                        "owners": ["nirvana_boss", "nirvana_devs"],
                    },
                    {
                        "name": "nirvana",
                        "subcontainer": "/N",
                        "owners": [
                            "taxi_boss",
                            "taxi_devs",
                            "nirvana_boss",
                            "nirvana_devs",
                        ],
                    },
                    {
                        "name": "non_existent",
                        "subcontainer": "/Z",
                    },
                ],
            },
            task_patch={"enable_porto": "isolate"},
            authenticated_user="taxi_dev",
        )
        try:
            job_id = wait_breakpoint()[0]
        except YtError:
            op.track()
            assert False

        # Check that job shell starts in a proper container.
        def get_subcontainer_name(shell_name):
            r = poll_job_shell(
                job_id,
                shell_name=shell_name,
                operation="spawn",
                command="echo $PORTO_NAME",
            )
            output = self._poll_until_shell_exited(job_id, r["shell_id"])

            # /path/to/uj/N/js-1234
            uj = output.find("uj/")
            js = output.find("/js")
            return output[uj + 3: js]

        assert get_subcontainer_name("default") == ""
        assert get_subcontainer_name(None) == ""
        assert get_subcontainer_name("nirvana") == "N"

        with raises_yt_error(yt_error_codes.ContainerDoesNotExist):
            poll_job_shell(job_id, shell_name="non_existent", operation="spawn", command="echo hi")
        with raises_yt_error(yt_error_codes.NoSuchJobShell):
            poll_job_shell(job_id, shell_name="brrr", operation="spawn", command="echo hi")

        # Check job shell permissions.
        def check_job_shell_permission(shell_name, user, allowed):
            if allowed:
                r = poll_job_shell(
                    job_id,
                    shell_name=shell_name,
                    authenticated_user=user,
                    operation="spawn",
                    command="echo hi",
                )
                output = self._poll_until_shell_exited(job_id, r["shell_id"])
                assert output == "hi\r\n"
            else:
                with raises_yt_error(yt_error_codes.AuthorizationErrorCode):
                    poll_job_shell(
                        job_id,
                        shell_name=shell_name,
                        authenticated_user=user,
                        operation="spawn",
                        command="echo hi",
                    )

        check_job_shell_permission("default", "nirvana_boss", allowed=True)
        check_job_shell_permission("default", "nirvana_dev", allowed=True)
        check_job_shell_permission("default", "taxi_boss", allowed=False)
        check_job_shell_permission("default", "taxi_dev", allowed=False)
        check_job_shell_permission("default", "yt_dev", allowed=True)
        check_job_shell_permission("default", "root", allowed=True)
        check_job_shell_permission("nirvana", "nirvana_boss", allowed=True)
        check_job_shell_permission("nirvana", "nirvana_dev", allowed=True)
        check_job_shell_permission("nirvana", "taxi_boss", allowed=True)
        check_job_shell_permission("nirvana", "taxi_dev", allowed=True)
        check_job_shell_permission("nirvana", "yt_dev", allowed=True)
        check_job_shell_permission("nirvana", "root", allowed=True)

    @authors("gritukan")
    def test_job_shell_in_subcontainer_invalid(self):
        with pytest.raises(YtError):
            run_test_vanilla(
                with_breakpoint("portoctl create N ; BREAKPOINT"),
                spec={
                    "enable_porto": "isolate",
                    "job_shells": [
                        {
                            "name": "x",
                            "subcontainer": "/A",
                            "owners": [],
                        },
                        {
                            "name": "x",
                            "subcontainer": "/B",
                            "owners": [],
                        },
                    ],
                },
                task_patch={"enable_porto": "isolate"},
            )

    @authors("gritukan")
    def test_job_shell_owners_update(self):
        create_user("nirvana_dev")
        create_group("nirvana_devs")

        create_user("taxi_dev")
        create_group("taxi_devs")

        create_user("market_dev")
        create_group("market_devs")

        add_member("nirvana_dev", "nirvana_devs")
        add_member("taxi_dev", "taxi_devs")
        add_member("market_dev", "market_devs")

        op = run_test_vanilla(
            with_breakpoint("portoctl create N && BREAKPOINT"),
            spec={
                "enable_porto": "isolate",
                "job_shells": [
                    {
                        "name": "default",
                        "subcontainer": "",
                        "owners": ["nirvana_devs"],
                    },
                    {
                        "name": "nirvana",
                        "subcontainer": "/N",
                        "owners": [
                            "taxi_devs",
                            "nirvana_devs",
                        ],
                    },
                ],
            },
            task_patch={"enable_porto": "isolate"},
            authenticated_user="taxi_dev",
        )
        try:
            job_id = wait_breakpoint()[0]
        except YtError:
            op.track()
            assert False

        # Check that job shell starts in a proper container.
        def get_subcontainer_name(shell_name):
            r = poll_job_shell(
                job_id,
                shell_name=shell_name,
                operation="spawn",
                command="echo $PORTO_NAME",
            )
            output = self._poll_until_shell_exited(job_id, r["shell_id"])

            # /path/to/uj/N/js-1234
            uj = output.find("uj/")
            js = output.find("/js")
            return output[uj + 3: js]

        assert get_subcontainer_name("default") == ""
        assert get_subcontainer_name(None) == ""
        assert get_subcontainer_name("nirvana") == "N"

        # Check job shell permissions.
        def check_job_shell_permission(shell_name, user, allowed):
            if allowed:
                r = poll_job_shell(
                    job_id,
                    shell_name=shell_name,
                    authenticated_user=user,
                    operation="spawn",
                    command="echo hi",
                )
                output = self._poll_until_shell_exited(job_id, r["shell_id"])
                assert output == "hi\r\n"
            else:
                with raises_yt_error(yt_error_codes.AuthorizationErrorCode):
                    poll_job_shell(
                        job_id,
                        shell_name=shell_name,
                        authenticated_user=user,
                        operation="spawn",
                        command="echo hi",
                    )

        check_job_shell_permission("default", "nirvana_dev", allowed=True)
        check_job_shell_permission("default", "taxi_dev", allowed=False)
        check_job_shell_permission("nirvana", "nirvana_dev", allowed=True)
        check_job_shell_permission("nirvana", "taxi_dev", allowed=True)
        check_job_shell_permission("nirvana", "market_dev", allowed=False)

        update_op_parameters(
            op.id,
            parameters={
                "options_per_job_shell": {
                    "nirvana": {
                        "owners": ["nirvana_devs", "market_devs"],
                    },
                },
            })

        check_job_shell_permission("default", "nirvana_dev", allowed=True)
        check_job_shell_permission("default", "taxi_dev", allowed=False)
        check_job_shell_permission("nirvana", "nirvana_dev", allowed=True)
        check_job_shell_permission("nirvana", "taxi_dev", allowed=False)
        check_job_shell_permission("nirvana", "market_dev", allowed=True)

        with pytest.raises(YtError):
            update_op_parameters(
                op.id,
                parameters={
                    "options_per_job_shell": {
                        "nirvana": {
                            "owners": ["nirvana_devs", "market_devs"],
                        },
                    },
                },
                authenticated_user="taxi_dev")

##################################################################


class TestJobProberRpcProxy(TestJobProber):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True


##################################################################


class TestJobShellInSubcontainerRpcProxy(TestJobShellInSubcontainer):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True
