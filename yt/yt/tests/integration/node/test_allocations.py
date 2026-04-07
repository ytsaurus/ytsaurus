from yt_env_setup import (YTEnvSetup, Restarter, CONTROLLER_AGENTS_SERVICE, SCHEDULERS_SERVICE)

from yt_commands import (
    authors, print_debug, vanilla, with_breakpoint, wait_breakpoint, release_breakpoint,
    get_allocation_id_from_job_id, get, ls, exists, wait,
)

import pytest


##################################################################


class TestSeveralJobsInAllocation(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 200,
            "snapshot_writer": {
                "upload_replication_factor": 1,
                "min_upload_replication_factor": 1,
            },
        }
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 1,
                "user_slots": 1,
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "allocation": {
                        "enable_multiple_jobs": True,
                    },
                },
            },
        },
    }

    @authors("pogorelov")
    @pytest.mark.parametrize("with_job_revival", [False, True])
    def test_vanilla_allocation_reused_within_task(self, with_job_revival):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 2,
                        "command": with_breakpoint("BREAKPOINT"),
                    },
                },
                "enable_multiple_jobs_in_allocation": True,
            },
        )

        job_id1, = wait_breakpoint(job_count=1)
        print_debug(f"job_id1: {job_id1}")

        if with_job_revival:
            op.wait_for_fresh_snapshot()

            with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
                pass

        release_breakpoint(job_id=job_id1)

        job_id2, = wait_breakpoint(job_count=1)
        print_debug(f"job_id2: {job_id2}")
        release_breakpoint(job_id=job_id2)

        print_debug(f"job_id1: {job_id1}, job_id2: {job_id2}")

        assert get_allocation_id_from_job_id(job_id1) == get_allocation_id_from_job_id(job_id2)

        op.track()

    @authors("pogorelov")
    def test_vanilla_allocation_not_reused_between_tasks(self):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT"),
                    },
                    "task_b": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT"),
                    },
                },
                "enable_multiple_jobs_in_allocation": True,
            },
        )
        job_id1, = wait_breakpoint(job_count=1)
        release_breakpoint(job_id=job_id1)

        job_id2, = wait_breakpoint(job_count=1)
        release_breakpoint(job_id=job_id2)

        print_debug(f"job_id1: {job_id1}, job_id2: {job_id2}")

        assert get_allocation_id_from_job_id(job_id1) != get_allocation_id_from_job_id(job_id2)

        op.track()

    @authors("pogorelov")
    def test_reusing_many_times(self):
        job_count = 10
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "command": with_breakpoint("BREAKPOINT"),
                        "job_count": 10,
                    },
                },
                "enable_multiple_jobs_in_allocation": True,
            },
        )

        allocation_id = None

        for i in range(job_count):
            job_id, = wait_breakpoint(job_count=1)
            release_breakpoint(job_id=job_id)

            if allocation_id is None:
                allocation_id = get_allocation_id_from_job_id(job_id)
            else:
                assert get_allocation_id_from_job_id(job_id) == allocation_id

        op.track()


##################################################################


class TestSuspendedOperationDoesNotSettleJobs(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 200,
            "snapshot_writer": {
                "upload_replication_factor": 1,
                "min_upload_replication_factor": 1,
            },
        }
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 1,
                "user_slots": 1,
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "allocation": {
                        "enable_multiple_jobs": True,
                    },
                },
            },
        },
    }

    def _get_orchid_paths(self):
        controller_agent_address = ls("//sys/controller_agents/instances")[0]
        base = f"//sys/controller_agents/instances/{controller_agent_address}/orchid/controller_agent/job_tracker"
        return (
            base + "/allocations",
            base + "/operations",
        )

    @authors("pogorelov")
    def test_suspended_operation_does_not_settle_jobs(self):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 2,
                        "command": with_breakpoint("BREAKPOINT"),
                    },
                },
                "enable_multiple_jobs_in_allocation": True,
            },
        )

        allocations_orchid_path, _ = self._get_orchid_paths()

        job_id1, = wait_breakpoint(job_count=1)
        allocation_id = get_allocation_id_from_job_id(job_id1)

        assert exists(allocations_orchid_path + f"/{allocation_id}")

        op.suspend()
        wait(lambda: get(op.get_path() + "/@suspended"))

        release_breakpoint(job_id=job_id1)
        wait(lambda: op.get_job_count("completed") == 1)

        wait(lambda: not exists(allocations_orchid_path + f"/{allocation_id}"))

        op.resume()
        wait(lambda: not get(op.get_path() + "/@suspended"))

        job_id2, = wait_breakpoint(job_count=1)
        release_breakpoint(job_id=job_id2)

        op.track()

        # After resume, job2 must run in a NEW allocation.
        assert get_allocation_id_from_job_id(job_id2) != allocation_id

    @authors("pogorelov")
    @pytest.mark.parametrize("service_to_restart", [CONTROLLER_AGENTS_SERVICE, SCHEDULERS_SERVICE])
    def test_suspended_operation_does_not_settle_jobs_after_revival(self, service_to_restart):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_a": {
                        "job_count": 2,
                        "command": with_breakpoint("BREAKPOINT"),
                    },
                },
                "enable_multiple_jobs_in_allocation": True,
            },
        )

        job_id1, = wait_breakpoint(job_count=1)
        allocation_id = get_allocation_id_from_job_id(job_id1)

        op.suspend()
        wait(lambda: get(op.get_path() + "/@suspended"))

        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, service_to_restart):
            pass

        allocations_orchid_path, operations_orchid_path = self._get_orchid_paths()

        wait(lambda: exists(allocations_orchid_path + f"/{allocation_id}"))

        assert get(operations_orchid_path + f"/{op.id}/suspended")

        release_breakpoint(job_id=job_id1)
        wait(lambda: op.get_job_count("completed") == 1)

        wait(lambda: not exists(allocations_orchid_path + f"/{allocation_id}"))

        op.resume()
        wait(lambda: not get(op.get_path() + "/@suspended"))

        job_id2, = wait_breakpoint(job_count=1)
        release_breakpoint(job_id=job_id2)

        op.track()

        # After resume, job2 must run in a NEW allocation.
        assert get_allocation_id_from_job_id(job_id2) != allocation_id
