import time
from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, with_breakpoint, wait_breakpoint, release_breakpoint, run_test_vanilla,
    run_sleeping_vanilla, update_nodes_dynamic_config, ls, get, wait, abort_job)

from yt_helpers import JobCountProfiler, profiler_factory

import pytest


class TestUserMemoryUsageTracker(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    @authors("arkady-e1ppa")
    def test_completed_job_resource_usage(self):
        node = ls("//sys/cluster_nodes")[0]
        profiler = profiler_factory().at_node(node)
        user_jobs_used = profiler \
            .counter(
                name="cluster_node/memory_usage/used",
                tags={"category": "user_jobs"})

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))
        wait_breakpoint()

        wait(lambda: user_jobs_used.get_delta() > 0)

        release_breakpoint()
        op.track()

        wait(lambda: user_jobs_used.get_delta() == 0)

    @authors("arkady-e1ppa")
    def test_aborted_running_job_resource_usage(self):
        node = ls("//sys/cluster_nodes")[0]
        profiler = profiler_factory().at_node(node)
        user_jobs_used = profiler \
            .counter(
                name="cluster_node/memory_usage/used",
                tags={"category": "user_jobs"})

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))
        (job_id, ) = wait_breakpoint()

        wait(lambda: user_jobs_used.get_delta() > 0)

        abort_job(job_id)
        release_breakpoint()
        op.track()

        wait(lambda: user_jobs_used.get_delta() == 0)

    @authors("arkady-e1ppa")
    @pytest.mark.skip(reason="This test is way too racy to properly check")
    def test_aborted_created_job_resource_usage(self):
        node = ls("//sys/cluster_nodes")[0]
        profiler = profiler_factory().at_node(node)
        user_jobs_used = profiler \
            .counter(
                name="cluster_node/memory_usage/used",
                tags={"category": "user_jobs"})

        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "test_resource_acquisition_delay": 10000,
                },
                "controller_agent_connector": {
                    "heartbeat_executor": {
                        "period": 2 * 10000,
                    }
                },
            },
        })

        def wait_for_job_to_finish(op):
            if op.get_job_count("aborted") > 0:
                return True

            return op.get_job_count("completed") > 0

        op = run_test_vanilla("echo 1 > null")
        wait(lambda: get("//sys/cluster_nodes/{}/orchid/exec_node/job_controller/active_job_count".format(node)) > 0)
        op.abort()
        wait(lambda: wait_for_job_to_finish(op), timeout=90)

        op.track(raise_on_aborted=False)

        wait(lambda: user_jobs_used.get_delta() == 0)


class TestMemoryPressureDetector(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    FREE_MEMORY_WATERMARK = 1024**3

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "job_environment": {
                    "type": "testing",
                    "testing_job_environment_scenario": "increasing_major_page_fault_count",
                },
            },
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "job_resource_manager": {
                "free_memory_watermark": FREE_MEMORY_WATERMARK,
            }
        }
    }

    @authors("renadeen")
    def test_memory_pressure_detector_at_node(self):
        node = ls("//sys/cluster_nodes")[0]
        node_memory = "//sys/cluster_nodes/{}/orchid/exec_node/job_resource_manager/resource_limits/user_memory".format(node)

        memory_before_pressure = get(node_memory)

        update_nodes_dynamic_config({
            "job_resource_manager": {
                "memory_pressure_detector": {
                    "enabled": True,
                    "check_period": 1000,
                    "major_page_fault_count_threshold": 100,
                    "memory_watermark_multiplier_increase_step": 0.5,
                    "max_memory_watermark_multiplier": 5,
                }
            },
        })

        wait(lambda: get(node_memory) < memory_before_pressure - 3 * self.FREE_MEMORY_WATERMARK)


class TestMemoryPressureAtJobProxy(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 3

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "job_environment": {
                    "job_thrashing_detector": {
                        "enabled": True,
                        "check_period": 1000,
                        "major_page_fault_count_threshold": 100,
                        "limit_overflow_count_threshold_to_abort_job": 3,
                    },
                    "type": "testing",
                    "testing_job_environment_scenario": "increasing_major_page_fault_count",
                }
            },
        }
    }

    @authors("renadeen")
    def test_memory_pressure_detector_at_job_proxy(self):
        time.sleep(1)
        aborted_job_profiler = JobCountProfiler("aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "job_memory_thrashing"})

        op = run_sleeping_vanilla()

        wait(lambda: op.get_job_count(state="aborted") > 0, timeout=10)
        wait(lambda: aborted_job_profiler.get_job_count_delta() == 1)


class TestNodeJobResourceManagerProfiling(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 10,
                "cpu": 2,
            },
        }
    }

    @authors("ignat")
    def test_resource_usage(self):
        node = ls("//sys/cluster_nodes")[0]
        resource_usage_metric = profiler_factory().at_node(node).gauge("job_controller/resource_usage/user_slots")

        op1 = run_sleeping_vanilla()

        wait(lambda: resource_usage_metric.get({"state": "acquired"}) == 1)
        wait(lambda: resource_usage_metric.get({"state": "pending"}) == 0)
        wait(lambda: resource_usage_metric.get({"state": "releasing"}) == 0)

        op2 = run_sleeping_vanilla()

        wait(lambda: resource_usage_metric.get({"state": "acquired"}) == 2)
        wait(lambda: resource_usage_metric.get({"state": "pending"}) == 0)
        wait(lambda: resource_usage_metric.get({"state": "releasing"}) == 0)

        op1.abort()

        wait(lambda: resource_usage_metric.get({"state": "acquired"}) == 1)
        wait(lambda: resource_usage_metric.get({"state": "pending"}) == 0)
        wait(lambda: resource_usage_metric.get({"state": "releasing"}) == 0)

        op2.abort()


class TestWaitingAllocationsOnResourceAvailabilityChange(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "resource_limits": {
            "memory_limits": {
                "user_jobs": {
                    "type": "static",
                    "value": 1000 * 10 ** 6,
                },
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 2,
                "cpu": 2,
            },
        },
        "exec_node": {
            "job_proxy": {
                "check_user_job_memory_limit": False,
            },
        },
    }

    def _allocations_path(self, node):
        return f"//sys/cluster_nodes/{node}/orchid/exec_node/job_controller/allocations"

    def _wait_for_allocations(self, node, count):
        allocations_path = self._allocations_path(node)
        wait(lambda: len(ls(allocations_path)) == count)
        return set(ls(allocations_path))

    def _get_allocation_states(self, node, allocation_ids):
        allocations_path = self._allocations_path(node)
        present_ids = set(ls(allocations_path))
        assert present_ids == allocation_ids, \
            f"Allocation set changed: expected {allocation_ids}, got {present_ids}"
        return [
            get(f"{allocations_path}/{allocation_id}")["state"]
            for allocation_id in allocation_ids
        ]

    def _wait_for_waiting_allocations(self, node, allocation_ids):
        wait(lambda: all(
            state == "waiting"
            for state in self._get_allocation_states(node, allocation_ids)))

    def _wait_for_running_allocations(self, node, allocation_ids):
        wait(lambda: all(
            state == "running"
            for state in self._get_allocation_states(node, allocation_ids)))

    # Time the node waits before its first attempt to acquire resources for an
    # allocation, leaving the test a window to lower the node resource limits so that
    # the attempt fails and the allocation stays waiting.
    RESOURCE_ACQUISITION_DELAY = 3000

    def _enable_delayed_resource_acquisition(self):
        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "test_resource_acquisition_delay": self.RESOURCE_ACQUISITION_DELAY,
                },
            },
            "job_resource_manager": {
                "resource_availability_check_period": 100,
            },
        })

    def _wait_out_failed_acquisition_attempt(self):
        # Wait out the acquisition delay so that the node actually attempts to acquire
        # resources under the lowered limits and fails. Only a failed attempt arms the
        # retry that a subsequent resource limit increase triggers.
        time.sleep(self.RESOURCE_ACQUISITION_DELAY / 1000 + 2)

    @authors("pogorelov")
    def test_waiting_allocations_retried_after_user_jobs_limit_increase(self):
        node = ls("//sys/cluster_nodes")[0]
        self._enable_delayed_resource_acquisition()

        op = run_sleeping_vanilla(
            job_count=1,
            task_patch={
                "memory_limit": 400 * 10 ** 6,
            })

        try:
            allocation_ids = self._wait_for_allocations(node, count=1)

            # Lower the limit below the job's demand so that the allocation cannot
            # acquire resources, before the node's first (delayed) attempt.
            update_nodes_dynamic_config(
                value={
                    "user_jobs": {
                        "type": "static",
                        "value": 200 * 10 ** 6,
                    },
                },
                path="resource_limits/memory_limits")

            self._wait_out_failed_acquisition_attempt()
            self._wait_for_waiting_allocations(node, allocation_ids)

            update_nodes_dynamic_config(
                value={
                    "user_jobs": {
                        "type": "static",
                        "value": 1000 * 10 ** 6,
                    },
                },
                path="resource_limits/memory_limits")

            self._wait_for_running_allocations(node, allocation_ids)
        finally:
            op.abort()

    @authors("pogorelov")
    def test_waiting_allocations_retried_after_cpu_limit_increase(self):
        node = ls("//sys/cluster_nodes")[0]
        self._enable_delayed_resource_acquisition()

        op = run_sleeping_vanilla(
            job_count=1,
            task_patch={
                "cpu_limit": 1,
            })

        try:
            allocation_ids = self._wait_for_allocations(node, count=1)

            # cpu=0 leaves no cpu for the allocation (it demands cpu=1), so it stays waiting.
            update_nodes_dynamic_config({"resource_limits": {"overrides": {"cpu": 0}}})

            self._wait_out_failed_acquisition_attempt()
            self._wait_for_waiting_allocations(node, allocation_ids)

            update_nodes_dynamic_config({"resource_limits": {"overrides": {"cpu": 2}}})

            self._wait_for_running_allocations(node, allocation_ids)
        finally:
            op.abort()
