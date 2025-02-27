import os
from time import sleep
from yt_env_setup import CONTROLLER_AGENTS_SERVICE, SCHEDULERS_SERVICE, Restarter, YTEnvSetup

from yt_commands import (
    authors,
    create,
    events_on_fs,
    execute_command,
    exists,
    get,
    map,
    patch_op_spec,
    raises_yt_error,
    read_table,
    run_sleeping_vanilla,
    run_test_vanilla,
    update_controller_agent_config,
    wait,
    write_table,
)

import pytest

##################################################################


def delay(duration, type="async"):
    return {
        "duration": duration,
        "type": type,
    }


def make_patch(path: str, value):
    return {"path": path, "value": value}


class TestPatchSpec(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "operations_update_period": 10,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {"controller_agent": {"snapshot_period": 1000}}

    @authors("coteeq")
    def test_basic(self):
        op = run_sleeping_vanilla()
        sleep(1)

        patch_op_spec(
            op.id,
            patches=[
                {
                    "path": "/max_failed_job_count",
                    "value": 10,
                }
            ],
        )


class TestUpdateProtocolBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "operations_update_period": 10,
            "pool_change_is_allowed": True,
            "watchers_update_period": 100,  # Update pools configuration period
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {"controller_agent": {"snapshot_period": 500}}

    @classmethod
    def _maybe_disable_snapshots(cls, disable=True):
        update_controller_agent_config("enable_snapshot_building", not disable)


class TestUpdateProtocol(TestUpdateProtocolBase):
    NUM_TEST_PARTITIONS = 4

    @authors("coteeq")
    @pytest.mark.parametrize("direction", ["increase", "decrease"])
    @pytest.mark.parametrize(
        "restart_services",
        [
            [],
            [SCHEDULERS_SERVICE],
            [CONTROLLER_AGENTS_SERVICE],
            [SCHEDULERS_SERVICE, CONTROLLER_AGENTS_SERVICE],
        ],
    )
    @pytest.mark.parametrize("snapshot", [True, False])
    def test_change_max_failed_job_count(self, direction, restart_services, snapshot):
        create("table", "//tmp/t1")  # noqa: F821
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        command = ";".join(
            [
                events_on_fs().notify_event_cmd("job_started_${YT_TASK_JOB_INDEX}"),
                events_on_fs().wait_event_cmd("release_${YT_TASK_JOB_INDEX}"),
                # Fail first two jobs.
                """if [ ${YT_TASK_JOB_INDEX} -lt 2 ]; then exit 1; else cat; fi""",
            ]
        )

        self._maybe_disable_snapshots(not snapshot)

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=command,
            spec={"max_failed_job_count": 2},
        )

        if direction == "increase":
            events_on_fs().notify_event("release_0")
            wait(lambda: op.get_job_count("failed") == 1)

            patch_op_spec(op.id, patches=[make_patch("/max_failed_job_count", 3)])

            def written_to_cypress():
                path = op.get_path() + "/@cumulative_spec_patch/max_failed_job_count"
                return exists(path) and get(path) == 3

            wait(written_to_cypress, timeout=10)

            if restart_services:
                if snapshot:
                    op.wait_for_fresh_snapshot()
                with Restarter(self.Env, restart_services):
                    pass

            wait(lambda: op.get_state() == "running")

            # NB(coteeq): Although controller may have forgotten about the first job,
            # we do not need to release it, because it is already released.
            events_on_fs().notify_event("release_1")
            events_on_fs().notify_event("release_2")

            op.track()

            assert op.get_job_count("failed") == 2

            assert read_table("//tmp/t2") == [{"foo": "bar"}]
        else:
            events_on_fs().notify_event("release_0")
            wait(lambda: op.get_job_count("failed") == 1)

            events_on_fs().wait_event("job_started_1")
            if restart_services and snapshot:
                op.wait_for_fresh_snapshot()

            patch_op_spec(op.id, patches=[make_patch("/max_failed_job_count", 1)])

            if restart_services:
                with Restarter(self.Env, restart_services):
                    pass

            with raises_yt_error("max_failed_job_count"):
                op.track()

            assert op.get_job_count("failed"), op.get_job_count("aborted") == (1, 1)

    @authors("coteeq")
    @pytest.mark.parametrize(
        "fail_point",
        [
            "",
            "before_cypress_flush",
            "before_apply",
        ],
    )
    @pytest.mark.parametrize("enable_snapshots", [True, False])
    def test_with_failures(self, fail_point, enable_snapshots):
        self._maybe_disable_snapshots(not enable_snapshots)
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        testing_spec = {}
        if fail_point:
            testing_spec["patch_spec_protocol"] = {"delay_" + fail_point: delay("5s")}

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command="sleep 1000",
            spec={
                "max_failed_job_count": 2,
                "testing": testing_spec,
            },
        )

        op.wait_for_state("running")

        if enable_snapshots:
            op.wait_for_fresh_snapshot()

        # Do not wait for command to finish.
        response = execute_command(
            "patch_op_spec",
            {
                "operation_id": op.id,
                "patches": [make_patch("/max_failed_job_count", 3)],
            },
            return_response=True,
        )
        sleep(1)

        if fail_point:
            with Restarter(self.Env, SCHEDULERS_SERVICE):
                pass

        sleep(1)  # Wait some time so we do not see "running" from previous incarnation.
        op.wait_for_state("running")

        expected = 2 if fail_point == "before_cypress_flush" else 3
        orchid_path = op.get_path() + "/controller_orchid"
        assert get(orchid_path + "/testing/dynamic_spec/max_failed_job_count") == expected

        response.wait()
        if not fail_point:
            assert response.is_ok()
        else:
            assert not response.is_ok()

    @authors("coteeq")
    def test_no_concurrent_updates(self):
        op = run_sleeping_vanilla(
            spec={"testing": {"patch_spec_protocol": {"delay_before_cypress_flush": delay("2s")}}}
        )

        op.wait_for_state("running")

        response1 = execute_command(
            "patch_op_spec",
            {"operation_id": op.id, "patches": [make_patch("/max_failed_job_count", 3)]},
            return_response=True,
        )

        sleep(0.5)

        response2 = execute_command(
            "patch_op_spec",
            {"operation_id": op.id, "patches": [make_patch("/max_failed_job_count", 4)]},
            return_response=True,
        )

        response1.wait()
        assert response1.is_ok()
        response2.wait()
        assert not response2.is_ok()

    @authors("coteeq")
    def test_fail_validation(self):
        op = run_sleeping_vanilla(
            spec={"testing": {"patch_spec_protocol": {"fail_validate": True}}}
        )

        op.wait_for_state("running")

        with raises_yt_error("Failed to validate"):
            patch_op_spec(op.id, patches=[make_patch("/max_failed_job_count", 1)])

        # Wait for possible failure.
        sleep(1)
        assert op.get_state() == "running"

    @authors("coteeq")
    def test_very_deep_spec(self):
        op = run_sleeping_vanilla()
        op.wait_for_state("running")
        with raises_yt_error("Depth limit exceeded"):
            deep_path = "/a" * 128
            patch_op_spec(op.id, patches=[make_patch("/annotations" + deep_path, "value")])


class TestUpdateProtocolControllerCrashes(TestUpdateProtocolBase):
    @classmethod
    def modify_controller_agent_config(cls, config, cluster_index):
        cls.core_path = os.path.join(cls.path_to_run, "_cores")
        os.mkdir(cls.core_path)
        os.chmod(cls.core_path, 0o777)
        config["core_dumper"] = {
            "path": cls.core_path,
            # Pattern starts with the underscore to trick teamcity; we do not want it to
            # pay attention to the created core.
            "pattern": "_core.%CORE_PID.%CORE_SIG.%CORE_THREAD_NAME-%CORE_REASON",
        }

    @authors("coteeq")
    def test_controller_crashes_on_exception_in_apply(self):
        op = run_sleeping_vanilla(
            spec={
                "testing": {
                    "patch_spec_protocol": {
                        "fail_apply": True,
                    }
                }
            }
        )

        op.wait_for_state("running")

        with raises_yt_error("Failed to apply"):
            patch_op_spec(op.id, patches=[make_patch("/max_failed_job_count", 3)])

        # NB: This is kinda happy path: controller "promised" that it will fail operation without
        # the help from scheduler.
        with raises_yt_error("Exception thrown in operation controller that led to operation failure"):
            op.track()

    @authors("coteeq")
    @pytest.mark.parametrize("enable_snapshots", [True, False])
    def test_controller_crashes_on_exception_in_initialize_reviving(self, enable_snapshots):
        self._maybe_disable_snapshots(not enable_snapshots)
        op = run_test_vanilla(
            command=events_on_fs().with_breakpoint("BREAKPOINT"),
            spec={
                "testing": {
                    "patch_spec_protocol": {
                        "fail_revive": True,
                    }
                }
            },
        )

        op.wait_for_state("running")

        patch_op_spec(op.id, patches=[make_patch("/max_failed_job_count", 3)])

        if enable_snapshots:
            op.wait_for_fresh_snapshot()

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            # Force the revival
            pass

        events_on_fs().release_breakpoint()

        if enable_snapshots:
            with raises_yt_error("Operation has failed to revive"):
                op.track()
        else:
            # See comment in spec_manager.cpp
            op.track()
