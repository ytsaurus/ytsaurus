from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors,
    create,
    create_user,
    release_breakpoint,
    run_test_vanilla,
    set,
    wait_breakpoint,
    with_breakpoint,
    write_table,
    merge,
    raises_yt_error,
)

from yt_helpers import wait_and_get_controller_incarnation

from time import sleep

import pytest


@pytest.mark.enabled_multidaemon
class TestControllerTransactions(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    @authors("coteeq")
    def test_consistent_locks(self):
        create(
            "table",
            "//tmp/in",
            attributes={"schema": [
                {"name": "x", "type": "int64", "sort_order": "ascending"},
                {"name": "y", "type": "int64"},
                ]
            },
        )

        def try_write(val):
            write_table(
                "<append=true>//tmp/in",
                [
                    {"x": val, "y": 2 * val}
                ]
            )
        try_write(val=0)

        op = merge(
            in_="//tmp/in",
            out="//tmp/in",
            mode="sorted",
            combine_chunks=True,
            spec={
                "force_transform": True,
                "testing": {
                    "delay_inside_prepare": "3s",
                },
            },
            track=False,
        )
        sleep(1)
        try_write(val=10)

        with raises_yt_error("has changed between taking input and output locks"):
            op.track()

    @authors("coteeq")
    def test_ban_user_during_operation(self):
        create_user("user")
        create("table", "//tmp/table")

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT; echo '{a=b}'"),
            job_count=2,
            task_patch={
                "output_table_paths": ["//tmp/table"],
            },
            authenticated_user="user",
        )

        (job_id_1, _) = wait_breakpoint(job_count=2)
        release_breakpoint(job_id=job_id_1)
        wait_breakpoint()

        assert op.get_state() == "running"

        incarnation = wait_and_get_controller_incarnation(op.get_controller_agent_address())

        set("//sys/users/user/@banned", True)

        # NB: release breakpoint, so operation will try to complete.
        release_breakpoint()
        with raises_yt_error():
            op.track()

        # Assert no disconnections happened.
        sleep(1)
        assert incarnation == wait_and_get_controller_incarnation(op.get_controller_agent_address())
