from yt_env_setup import (YTEnvSetup, Restarter, CONTROLLER_AGENTS_SERVICE)

from yt_commands import (
    authors, print_debug, vanilla, with_breakpoint, wait_breakpoint, release_breakpoint,
    get_allocation_id_from_job_id,
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
