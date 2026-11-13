import time
from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, with_breakpoint, wait_breakpoint, release_breakpoint, run_test_vanilla,
    run_sleeping_vanilla, update_nodes_dynamic_config, ls, get, wait, abort_job, set as yt_set)

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
        wait_breakpoint(op)

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
        (job_id, ) = wait_breakpoint(op)

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
                    "testing": {
                        "resource_acquisition_delay": 10000,
                    },
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
                    "testing": {
                        "resource_acquisition_delay": self.RESOURCE_ACQUISITION_DELAY,
                    },
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


class TestExecNodeMasterCache(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 0
    NUM_CONTROLLER_AGENTS = 0
    USE_MASTER_CACHE = True

    DELTA_NODE_FLAVORS = [["exec"]]

    CACHE_CAPACITY = 1024**3
    SMALL_CACHE_CAPACITY = 1024**2
    BUDGET_TOLERANCE = 64 * 1024**2

    DELTA_NODE_CONFIG = {
        "exec_node_is_not_data_node": True,
        "tablet_node": {"versioned_chunk_meta_cache": {"capacity": 0}},
        "resource_limits": {
            "memory": 64 * 1024**3,
            "memory_limits": {
                "user_jobs": {"type": "dynamic"},
                # Stabilize allocator reserves while measuring the cache budget.
                "footprint": {"type": "static", "value": 16 * 1024**3},
                "alloc_fragmentation": {"type": "static", "value": 8 * 1024**3},
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "caching_object_service": {"capacity": CACHE_CAPACITY},
            "tablet_node": {"versioned_chunk_meta_cache": {"capacity": 0}},
            "resource_limits": {"use_instance_limits_tracker": False},
            "node_memory_tracker": {"check_per_category_limit_overcommit": True},
        },
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "node_tracker": {
            "master_cache_manager": {"peer_count": 1, "update_period": 100},
        },
    }

    @authors("pogorelov")
    def test_small_capacity_releases_job_memory(self):
        node, = ls("//sys/exec_nodes")
        orchid = f"//sys/cluster_nodes/{node}/orchid"
        cache_limit = f"{orchid}/node_resource_manager/memory_limit_per_category/master_cache"
        job_memory_limit = f"{orchid}/exec_node/job_resource_manager/resource_limits/user_memory"
        cache_used = profiler_factory().at_node(node).gauge(
            "cluster_node/memory_usage/used", {"category": "master_cache"})

        wait(lambda: get("//sys/cluster_nodes/@master_cache_nodes") == [node])
        wait(lambda: get(f"{orchid}/node_resource_manager/total_memory") == 64 * 1024**3)
        wait(lambda: get(cache_limit) == self.CACHE_CAPACITY)
        memory_before = get(job_memory_limit)

        path = "//tmp/master_cache_probe"
        yt_set(path, "before")
        wait(lambda: (
            get(path, read_from="cache", expire_after_successful_update_time=60000) == "before" and
            cache_used.get() > 0))

        update_nodes_dynamic_config({
            "caching_object_service": {"capacity": self.SMALL_CACHE_CAPACITY},
        })
        wait(lambda: get(cache_limit) == self.SMALL_CACHE_CAPACITY)
        wait(lambda: 0 < cache_used.get() <= self.SMALL_CACHE_CAPACITY)
        released_memory = self.CACHE_CAPACITY - self.SMALL_CACHE_CAPACITY
        wait(lambda: (
            abs(get(job_memory_limit) - memory_before - released_memory) < self.BUDGET_TOLERANCE))

        for value in ("after", "still_forwarding"):
            yt_set(path, value)
            assert get(path, read_from="cache", expire_after_successful_update_time=0) == value
            wait(lambda: 0 < cache_used.get() <= self.SMALL_CACHE_CAPACITY)

        update_nodes_dynamic_config({"caching_object_service": {"capacity": self.CACHE_CAPACITY}})
        wait(lambda: get(cache_limit) == self.CACHE_CAPACITY)
        wait(lambda: abs(get(job_memory_limit) - memory_before) < self.BUDGET_TOLERANCE)
        wait(lambda: (
            get(
                path,
                read_from="cache",
                expire_after_successful_update_time=60000) == "still_forwarding" and
            cache_used.get() > 0))
