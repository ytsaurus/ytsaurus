from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    SCHEDULERS_SERVICE,
    CONTROLLER_AGENTS_SERVICE,
    NODES_SERVICE,
    is_asan_build,
    is_debug_build,
)

from yt_commands import (
    authors, wait, wait_no_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    ls, get, create, set, write_table, remove, exists, create_pool, create_pool_tree,
    create_data_center, create_rack, make_batch_request,
    execute_batch, get_batch_error,
    vanilla, run_test_vanilla, run_sleeping_vanilla, update_scheduler_config, abort_job,
    update_controller_agent_config, update_pool_tree_config, update_pool_tree_config_option,
    print_debug, map)

from yt_scheduler_helpers import (
    scheduler_orchid_pool_path,
    scheduler_orchid_operation_path, scheduler_orchid_default_pool_tree_config_path,
    scheduler_orchid_path, scheduler_orchid_node_path, scheduler_new_orchid_pool_tree_path)

from yt_helpers import profiler_factory, read_structured_log, wait_and_get_controller_incarnation, write_log_barrier

from yt.test_helpers import are_almost_equal

from yt.common import YtError

import yt.yson as yson


import pytest

import time
import datetime
import builtins

from copy import deepcopy

from collections import defaultdict


##################################################################


def get_first_job_node(op):
    wait(lambda: len(op.get_running_jobs()) >= 1)
    jobs = op.get_running_jobs()
    job = jobs[list(jobs)[0]]
    return job["address"]


def get_persistent_node_segment_states_path(tree="default"):
    return "//sys/scheduler/strategy_state/tree_states/{}/scheduling_policy_state/scheduling_segments_state/node_states".format(tree)

##################################################################


@pytest.mark.skipif(
    is_asan_build() or is_debug_build(),
    reason="This test suite requires a genuine release build to fit into timeout"
)
class TestSchedulingSegments(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_TEST_PARTITIONS = 8
    NUM_MASTERS = 1
    NUM_NODES = 10
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "fair_share_update_period": 100,
            "fair_share_profiling_period": 100,
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "controller_agent_connector": {
                    "heartbeat_executor": {
                        "period": 500,  # 500 msec
                    },
                },
            },
        },
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "gpu_manager": {
                "testing": {
                    "test_resource": True,
                    "test_gpu_count": 8
                },
            },
            "job_proxy": {
                "job_proxy_heartbeat_period": 100,
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 10,
                "user_slots": 10,
            },
        }
    }

    SCHEDULING_SEGMENTS = [
        "default",
        "large_gpu",
    ]

    DATA_CENTER = "SAS"
    RACK = "SAS1"

    def _get_dominant_usage_share(self, op, tree="default"):
        return get(scheduler_orchid_operation_path(op, tree) + "/dominant_usage_share", default=0.0)

    def _get_dominant_fair_share(self, op, tree="default"):
        return get(scheduler_orchid_operation_path(op, tree) + "/detailed_dominant_fair_share/total", default=0.0)

    # NB(eshcherbin): This method always returns NO nodes for the default segment.
    def _get_nodes_for_segment_in_tree(self, segment, tree="default"):
        node_states = get(get_persistent_node_segment_states_path(tree), default={})
        return [node_state["address"] for _, node_state in node_states.items() if node_state["segment"] == segment]

    def setup_method(self, method):
        super(TestSchedulingSegments, self).setup_method(method)

        create_pool("cpu", attributes={"allow_normal_preemption": False}, wait_for_orchid=False)
        create_pool("small_gpu", attributes={"allow_normal_preemption": False}, wait_for_orchid=False)
        create_pool("large_gpu", attributes={"allow_normal_preemption": False})
        set("//sys/pool_trees/default/@config/scheduling_segments", {
            "mode": "large_gpu",
            "initialization_timeout": 10000,
            "manage_period": 100,
            "unsatisfied_segments_rebalancing_timeout": 1000,
            "data_centers": [TestSchedulingSegments.DATA_CENTER],
            "enable_detailed_logs": True,
        })
        set("//sys/pool_trees/default/@config/main_resource", "gpu")
        wait(lambda: get(scheduler_orchid_default_pool_tree_config_path() + "/scheduling_segments/mode") == "large_gpu")
        wait(
            lambda: get(
                scheduler_orchid_default_pool_tree_config_path()
                + "/scheduling_segments/unsatisfied_segments_rebalancing_timeout"
            )
            == 1000
        )
        update_pool_tree_config("default", {
            "preemptive_scheduling_backoff": 0,
            "fair_share_starvation_timeout": 100,
            "fair_share_starvation_tolerance": 0.95,
            "preemption_satisfaction_threshold": 0.99,
            "non_preemptible_resource_usage_threshold": {"user_slots": 0},
        })

        # NB(eshcherbin): This is done to reset node segments.
        with Restarter(self.Env, SCHEDULERS_SERVICE):
            requests = [
                make_batch_request("set", path=get_persistent_node_segment_states_path(), input={}),
            ]
            for node in ls("//sys/cluster_nodes"):
                requests.append(make_batch_request(
                    "set",
                    path="//sys/cluster_nodes/{}/@scheduling_options".format(node),
                    input={},
                ))
            for response in execute_batch(requests):
                assert not get_batch_error(response)

        create_data_center(TestSchedulingSegments.DATA_CENTER)
        create_rack(TestSchedulingSegments.RACK)
        set("//sys/racks/{}/@data_center".format(TestSchedulingSegments.RACK), TestSchedulingSegments.DATA_CENTER)
        for node in ls("//sys/cluster_nodes"):
            set("//sys/cluster_nodes/{}/@rack".format(node), TestSchedulingSegments.RACK)
        for node in ls("//sys/cluster_nodes"):
            wait(lambda: get(scheduler_orchid_node_path(node) + "/data_center") == TestSchedulingSegments.DATA_CENTER)

    @authors("eshcherbin")
    def test_large_gpu_segment_extended(self):
        blocking_op = run_sleeping_vanilla(
            job_count=20,
            spec={"pool": "small_gpu"},
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 1.0))

        op = run_sleeping_vanilla(
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.1))

    @authors("eshcherbin")
    def test_default_segment_extended_gpu(self):
        blocking_op = run_sleeping_vanilla(
            job_count=10,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 1.0))

        op = run_sleeping_vanilla(
            job_count=8,
            spec={"pool": "small_gpu"},
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.1))

    @pytest.mark.skip("There is no logic that reduces oversatisfied segments yet, "
                      "and operations with zero GPU demand do not change the default segment's fair resource amount")
    @authors("eshcherbin")
    def test_default_segment_extended_cpu(self):
        blocking_op = run_sleeping_vanilla(
            job_count=10,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 1.0))

        op = run_sleeping_vanilla(job_count=10, spec={"pool": "cpu"}, task_patch={"cpu_limit": 1})
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.1))

    @pytest.mark.skip("There is no logic that reduces oversatisfied segments yet, "
                      "and operations with zero GPU demand do not change the default segment's fair resource amount")
    @authors("eshcherbin")
    def test_default_segment_extended_gpu_and_cpu(self):
        blocking_op = run_sleeping_vanilla(
            job_count=10,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 1.0))

        op1 = run_sleeping_vanilla(job_count=10, spec={"pool": "cpu"}, task_patch={"cpu_limit": 1})
        op2 = run_sleeping_vanilla(
            job_count=8,
            spec={"pool": "small_gpu"},
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
        )

        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op1.id), 0.1))
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op2.id), 0.1))

    @authors("eshcherbin")
    def test_reserve_fair_resource_amount(self):
        # Just to check that it works with no core dump.
        update_pool_tree_config_option("default", "scheduling_segments/reserve_fair_resource_amount", {"default": 1.0}, wait_for_orchid=False)

        set("//sys/pool_trees/default/large_gpu/@strong_guarantee_resources", {"gpu": 80})
        update_pool_tree_config_option("default", "scheduling_segments/reserve_fair_resource_amount/large_gpu", {
            TestSchedulingSegments.DATA_CENTER: 8.0,
        })
        update_pool_tree_config_option("default", "allocation_preemption_timeout", 30000)

        filling_op = run_sleeping_vanilla(
            job_count=9,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(filling_op.id), 0.9))

        run_test_vanilla(
            """(trap "sleep 40; exit 0" SIGINT; sleep 1000)""",
            job_count=8,
            spec={"pool": "small_gpu"},
            task_patch={"interruption_signal": "SIGINT", "gpu_limit": 1, "enable_gpu_layers": False},
        )

        time.sleep(3)

        op = run_test_vanilla(
            "sleep 1",
            job_count=1,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: op.get_state() == "completed")

    @authors("eshcherbin")
    def test_rebalancing_heuristic(self):
        # A little hack to disable preemptible jobs. These are tested below.
        set("//sys/pool_trees/default/large_gpu/@non_preemptible_resource_usage_threshold", {"user_slots": 9})

        blocking_op1 = run_sleeping_vanilla(
            job_count=9,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op1.id), 0.9))

        # Need to spend some time to ensure the nodes where blocking_op1's jobs are running won't be moved.
        time.sleep(1.0)

        blocking_op2 = run_sleeping_vanilla(
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op2.id), 0.1))

        expected_node = get_first_job_node(blocking_op2)
        wait(lambda: get(scheduler_orchid_node_path(expected_node) + "/scheduling_segment", default=None) == "large_gpu")

        new_op = run_sleeping_vanilla(
            job_count=2,
            spec={"pool": "small_gpu"},
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(new_op.id), 0.1))

        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op2.id), 0.0))
        actual_node = get_first_job_node(new_op)
        assert actual_node == expected_node
        wait(lambda: get(scheduler_orchid_node_path(expected_node) + "/scheduling_segment", default=None) == "default")

    @authors("eshcherbin")
    def test_rebalancing_heuristic_choose_node_with_preemptible_job(self):
        set("//sys/pool_trees/default/@config/cached_allocation_preemption_statuses_update_period", 500)
        set("//sys/pool_trees/default/large_gpu/@strong_guarantee_resources", {"gpu": 72})
        set("//sys/pool_trees/default/small_gpu/@strong_guarantee_resources", {"gpu": 8})
        create_pool(
            "guaranteed_large",
            parent_name="large_gpu",
            attributes={
                "strong_guarantee_resources": {"gpu": 72},
                "allow_normal_preemption": False,
            },
        )
        create_pool("research_large", parent_name="large_gpu", attributes={"allow_normal_preemption": False})

        blocking_op1 = run_sleeping_vanilla(
            spec={"pool": "research_large"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op1.id), 0.1))

        # Need to spend some time to ensure the nodes where blocking_op1's jobs are running won't be moved.
        time.sleep(3.0)

        blocking_op2 = run_sleeping_vanilla(
            job_count=9,
            spec={"pool": "guaranteed_large"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op2.id), 0.9))

        def get_first_job_node(op):
            wait(lambda: len(op.get_running_jobs()) >= 1)
            jobs = op.get_running_jobs()
            job = jobs[list(jobs)[0]]
            return job["address"]

        expected_node = get_first_job_node(blocking_op1)
        wait(lambda: get(scheduler_orchid_node_path(expected_node) + "/scheduling_segment", default=None) == "large_gpu")

        timeout_attribute_path = "/scheduling_segments/unsatisfied_segments_rebalancing_timeout"
        set("//sys/pool_trees/default/@config" + timeout_attribute_path, 1000000000)
        wait(lambda: get(scheduler_orchid_default_pool_tree_config_path() + timeout_attribute_path) == 1000000000)

        new_op = run_sleeping_vanilla(
            job_count=8,
            spec={"pool": "small_gpu"},
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
        )

        wait(
            lambda: get(
                scheduler_orchid_node_path(expected_node) + "/running_job_statistics/preemptible_gpu_time", default=0.0
            )
            > 0.0
        )
        set("//sys/pool_trees/default/@config" + timeout_attribute_path, 1000)

        wait(lambda: are_almost_equal(self._get_dominant_usage_share(new_op.id), 0.1))

        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op1.id), 0.0))
        actual_node = get_first_job_node(new_op)
        assert actual_node == expected_node
        wait(lambda: get(scheduler_orchid_node_path(expected_node) + "/scheduling_segment", default=None) == "default")

    @authors("eshcherbin")
    def test_rebalancing_heuristic_choose_node_with_job_from_other_segment(self):
        update_pool_tree_config_option("default", "cached_allocation_preemption_statuses_update_period", 500)
        set("//sys/pool_trees/default/large_gpu/@strong_guarantee_resources", {"gpu": 44})
        set("//sys/pool_trees/default/small_gpu/@strong_guarantee_resources", {"gpu": 36})

        blocking_large_op = run_sleeping_vanilla(
            job_count=5,
            spec={"pool": "large_gpu", "scheduling_segment": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_large_op.id), 0.5))

        blocking_small_op = run_sleeping_vanilla(
            job_count=4,
            spec={"pool": "small_gpu", "scheduling_segment": "default"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_small_op.id), 0.4))

        time.sleep(2.0)

        sharing_small_op = run_sleeping_vanilla(
            spec={"pool": "small_gpu", "scheduling_segment": "default"},
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(sharing_small_op.id), 0.05))

        sharing_large_op = run_sleeping_vanilla(
            spec={"pool": "large_gpu", "scheduling_segment": "large_gpu"},
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(sharing_large_op.id), 0.05))

        time.sleep(2.0)
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(sharing_small_op.id), 0.05))

        def get_first_job_node(op):
            wait(lambda: len(op.get_running_jobs()) >= 1)
            jobs = op.get_running_jobs()
            job = jobs[list(jobs)[0]]
            return job["address"]

        # Two jobs from different segments share a node from large GPU segment.
        wait(lambda: get_first_job_node(sharing_large_op) == get_first_job_node(sharing_small_op))
        shared_node = get_first_job_node(sharing_large_op)
        wait(lambda: get(scheduler_orchid_node_path(shared_node) + "/scheduling_segment", default=None) == "large_gpu")

        # But the job from default segment is considered preemptible just for running in the wrong segment.
        wait(lambda: get(scheduler_orchid_node_path(shared_node) + "/running_job_statistics/preemptible_gpu_time") > 0)

        # NB: We need to disable rebalancing temporarily, because after blocking_large_op's jobs are aborted
        # there may be a moment when new demand has not been received from the controller yet
        # and large segment doesn't have enough fair share.
        update_pool_tree_config_option("default", "scheduling_segments/unsatisfied_segments_rebalancing_timeout", 1000000000)
        blocking_jobs = blocking_large_op.get_running_jobs()
        assert len(blocking_jobs) == 5
        for job in blocking_jobs.keys():
            abort_job(job)

        time.sleep(1.0)
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_large_op.id), 0.5))

        sharing_large_op.abort()

        def check():
            stats = get(scheduler_orchid_node_path(shared_node) + "/running_job_statistics")
            return are_almost_equal(stats["total_gpu_time"], stats["preemptible_gpu_time"])
        wait(check)

        update_pool_tree_config_option("default", "scheduling_segments/unsatisfied_segments_rebalancing_timeout", 100)
        wait(lambda: get(scheduler_orchid_node_path(shared_node) + "/scheduling_segment") == "default")

    @authors("eshcherbin")
    def test_mixed_operation(self):
        op = vanilla(
            spec={
                "tasks": {
                    "small": {
                        "job_count": 1,
                        "command": "sleep 1000",
                        "gpu_limit": 1,
                        "enable_gpu_layers": False,
                    },
                    "large": {
                        "job_count": 1,
                        "command": "sleep 1000",
                        "gpu_limit": 8,
                        "enable_gpu_layers": False,
                    },
                }
            },
            track=False,
        )

        wait(
            lambda: get(
                scheduler_orchid_operation_path(op.id) + "/scheduling_segment",
                default="",
            )
            == "default"
        )

    @authors("eshcherbin")
    def test_specified_segment(self):
        small_but_large_op = run_sleeping_vanilla(
            spec={"pool": "small_gpu", "scheduling_segment": "large_gpu"},
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
        )
        large_but_small_op = run_sleeping_vanilla(
            spec={"pool": "large_gpu", "scheduling_segment": "default"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )

        wait(
            lambda: get(
                scheduler_orchid_operation_path(small_but_large_op.id) + "/scheduling_segment",
                default="",
            )
            == "large_gpu"
        )
        wait(
            lambda: get(
                scheduler_orchid_operation_path(large_but_small_op.id) + "/scheduling_segment",
                default="",
            )
            == "default"
        )

        with pytest.raises(YtError):
            run_sleeping_vanilla(
                spec={
                    "pool": "small_gpu",
                    "scheduling_segment": "my_cool_but_totally_invalid_segment",
                },
                task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
            )

    @authors("eshcherbin")
    def test_disabled(self):
        set("//sys/pool_trees/default/@config/scheduling_segments/mode", "disabled")
        wait(lambda: get(scheduler_orchid_default_pool_tree_config_path() + "/scheduling_segments/mode") == "disabled")

        blocking_op = run_sleeping_vanilla(
            job_count=20,
            spec={"pool": "small_gpu"},
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 1.0))

        op = run_sleeping_vanilla(
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_fair_share(op.id), 0.1))

        op_slot_index_path = scheduler_orchid_path() + "/scheduler/operations/{}/slot_index_per_pool_tree/default".format(op.id)
        wait(lambda: exists(op_slot_index_path))
        op_slot_index = get(op_slot_index_path)

        op_dominant_usage_share_sensor = profiler_factory()\
            .at_scheduler(fixed_tags={"tree": "default", "pool": "large_gpu", "slot_index": str(op_slot_index)})\
            .gauge("scheduler/operations_by_slot/dominant_usage_share")

        for _ in range(30):
            time.sleep(0.1)
            assert op_dominant_usage_share_sensor.get(default=0, verbose=False) == 0

    @authors("eshcherbin")
    def test_rebalancing_timeout_changed(self):
        timeout_attribute_path = "/scheduling_segments/unsatisfied_segments_rebalancing_timeout"
        set("//sys/pool_trees/default/@config" + timeout_attribute_path, 1000000000)
        wait(lambda: get(scheduler_orchid_default_pool_tree_config_path() + timeout_attribute_path) == 1000000000)

        blocking_op = run_sleeping_vanilla(
            job_count=20,
            spec={"pool": "small_gpu"},
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 1.0))

        op = run_sleeping_vanilla(
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_fair_share(op.id), 0.1))

        op_slot_index_path = scheduler_orchid_path() + "/scheduler/operations/{}/slot_index_per_pool_tree/default".format(op.id)
        wait(lambda: exists(op_slot_index_path))
        op_slot_index = get(op_slot_index_path)

        op_dominant_usage_share_sensor = profiler_factory()\
            .at_scheduler(fixed_tags={"tree": "default", "pool": "large_gpu", "slot_index": str(op_slot_index)}) \
            .gauge("scheduler/operations_by_slot/dominant_usage_share")

        for _ in range(30):
            time.sleep(0.1)
            assert op_dominant_usage_share_sensor.get(default=0, verbose=False) == 0

        set("//sys/pool_trees/default/@config" + timeout_attribute_path, 1000)
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.1))

    @authors("eshcherbin")
    def test_orchid(self):
        small_op = run_sleeping_vanilla(
            spec={"pool": "small_gpu"},
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
        )
        large_op = run_sleeping_vanilla(
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )

        wait(lambda: exists(scheduler_new_orchid_pool_tree_path("default") + "/scheduling_segments/operations/{}".format(large_op.id)))

        def check_segment(op, segment):
            wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/scheduling_segment", default="") == segment)
            wait(lambda: get(scheduler_new_orchid_pool_tree_path("default") + "/scheduling_segments/operations/{}/scheduling_segment".format(op.id)) == segment)

        check_segment(small_op, "default")
        check_segment(large_op, "large_gpu")

    @authors("eshcherbin")
    def test_update_operation_segment_on_reconfiguration(self):
        set("//sys/pool_trees/default/@config/scheduling_segments/mode", "disabled")
        wait(lambda: get(scheduler_orchid_default_pool_tree_config_path() + "/scheduling_segments/mode") == "disabled")

        blocking_op = run_sleeping_vanilla(
            job_count=20,
            spec={"pool": "small_gpu"},
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 1.0))

        op = run_sleeping_vanilla(
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_fair_share(op.id), 0.1))
        wait(
            lambda: get(
                scheduler_orchid_operation_path(op.id) + "/scheduling_segment",
                default="",
            )
            == "default"
        )

        set("//sys/pool_trees/default/@config/scheduling_segments/mode", "large_gpu")
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/scheduling_segment", default="") == "large_gpu")
        wait(lambda: get(scheduler_orchid_operation_path(blocking_op.id) + "/scheduling_segment", default="") == "default")
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.1))

        set("//sys/pool_trees/default/@config/scheduling_segments/mode", "disabled")
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/scheduling_segment", default="") == "default")
        wait(lambda: get(scheduler_orchid_operation_path(blocking_op.id) + "/scheduling_segment", default="") == "default")

        time.sleep(3.0)
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.1))

    @authors("eshcherbin")
    def test_profiling(self):
        set("//sys/pool_trees/default/@config/scheduling_segments/unsatisfied_segments_rebalancing_timeout", 1000000000)
        wait(lambda: get(scheduler_orchid_default_pool_tree_config_path() + "/scheduling_segments/unsatisfied_segments_rebalancing_timeout") == 1000000000)

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default"})
        fair_resource_amount_default_sensor = profiler.gauge("scheduler/segments/fair_resource_amount", fixed_tags={"segment": "default"})
        current_resource_amount_default_sensor = profiler.gauge("scheduler/segments/current_resource_amount", fixed_tags={"segment": "default"})
        fair_resource_amount_large_sensor = profiler.gauge("scheduler/segments/fair_resource_amount", fixed_tags={"segment": "large_gpu"})
        current_resource_amount_large_sensor = profiler.gauge("scheduler/segments/current_resource_amount", fixed_tags={"segment": "large_gpu"})

        wait(lambda: fair_resource_amount_default_sensor.get() == 0)
        wait(lambda: fair_resource_amount_large_sensor.get() == 0)
        wait(lambda: current_resource_amount_default_sensor.get() == 80)
        wait(lambda: current_resource_amount_large_sensor.get() == 0)

        blocking_op = run_sleeping_vanilla(
            job_count=20,
            spec={"pool": "small_gpu"},
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 1.0))

        wait(lambda: fair_resource_amount_default_sensor.get() == 80)
        wait(lambda: fair_resource_amount_large_sensor.get() == 0)
        wait(lambda: current_resource_amount_default_sensor.get() == 80)
        wait(lambda: current_resource_amount_large_sensor.get() == 0)

        op = run_sleeping_vanilla(
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_fair_share(op.id), 0.1))

        time.sleep(3.0)

        wait(lambda: fair_resource_amount_default_sensor.get() == 72)
        wait(lambda: fair_resource_amount_large_sensor.get() == 8)
        wait(lambda: current_resource_amount_default_sensor.get() == 80)
        wait(lambda: current_resource_amount_large_sensor.get() == 0)

        set("//sys/pool_trees/default/@config/scheduling_segments/unsatisfied_segments_rebalancing_timeout", 1000)

        wait(lambda: fair_resource_amount_default_sensor.get() == 72)
        wait(lambda: fair_resource_amount_large_sensor.get() == 8)
        wait(lambda: current_resource_amount_default_sensor.get() == 72)
        wait(lambda: current_resource_amount_large_sensor.get() == 8)

        op.abort()

        wait(lambda: fair_resource_amount_default_sensor.get() == 80)
        wait(lambda: fair_resource_amount_large_sensor.get() == 0)
        wait(lambda: current_resource_amount_default_sensor.get() == 80)
        wait(lambda: current_resource_amount_large_sensor.get() == 0)

    @authors("eshcherbin")
    def test_revive_operation_segments_from_scratch(self):
        small_op = run_sleeping_vanilla(
            job_count=20,
            spec={"pool": "small_gpu"},
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )
        large_op = run_sleeping_vanilla(
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(small_op.id), 0.9))
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(large_op.id), 0.1))

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        wait(
            lambda: get(
                scheduler_orchid_operation_path(small_op.id) + "/scheduling_segment",
                default="",
            )
            == "default"
        )
        wait(
            lambda: get(
                scheduler_orchid_operation_path(large_op.id) + "/scheduling_segment",
                default="",
            )
            == "large_gpu"
        )

        wait(lambda: are_almost_equal(self._get_dominant_usage_share(small_op.id), 0.9))
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(large_op.id), 0.1))

    @authors("eshcherbin")
    @pytest.mark.parametrize("service_to_restart", [SCHEDULERS_SERVICE, CONTROLLER_AGENTS_SERVICE])
    def test_revive_operation_segments_from_snapshot(self, service_to_restart):
        update_controller_agent_config("snapshot_period", 300)

        small_op = run_sleeping_vanilla(
            job_count=20,
            spec={"pool": "small_gpu"},
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )
        large_op = run_sleeping_vanilla(
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(small_op.id), 0.9))
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(large_op.id), 0.1))

        small_op.wait_for_fresh_snapshot()
        large_op.wait_for_fresh_snapshot()

        with Restarter(self.Env, service_to_restart):
            pass

        small_op.ensure_running()
        large_op.ensure_running()

        wait(
            lambda: get(
                scheduler_orchid_operation_path(large_op.id) + "/scheduling_segment",
                default="",
            )
            == "large_gpu"
        )
        wait(
            lambda: get(
                scheduler_orchid_operation_path(small_op.id) + "/scheduling_segment",
                default="",
            )
            == "default"
        )

        wait(lambda: are_almost_equal(self._get_dominant_usage_share(small_op.id), 0.9))
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(large_op.id), 0.1))

    @authors("eshcherbin")
    def test_persistent_segments_state(self):
        blocking_op = run_sleeping_vanilla(job_count=20, spec={"pool": "small_gpu"}, task_patch={"gpu_limit": 4, "enable_gpu_layers": False})
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 1.0))
        run_sleeping_vanilla(spec={"pool": "large_gpu"}, task_patch={"gpu_limit": 8, "enable_gpu_layers": False})

        wait(lambda: len(self._get_nodes_for_segment_in_tree("large_gpu")) == 1)
        wait(lambda: len(self._get_nodes_for_segment_in_tree("default")) == 0)

        node_segment_orchid_path = scheduler_orchid_path() + "/scheduler/nodes/{}/scheduling_segment"
        large_gpu_segment_nodes = self._get_nodes_for_segment_in_tree("large_gpu")
        assert len(large_gpu_segment_nodes) == 1
        for node in ls("//sys/cluster_nodes"):
            expected_segment = "large_gpu" \
                if node in large_gpu_segment_nodes \
                else "default"
            wait(lambda: get(node_segment_orchid_path.format(node), default="") == expected_segment)

    @authors("eshcherbin")
    def test_persistent_segments_state_revive(self):
        update_controller_agent_config("snapshot_period", 300)

        blocking_op = run_sleeping_vanilla(job_count=20, spec={"pool": "small_gpu"}, task_patch={"gpu_limit": 4, "enable_gpu_layers": False})
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 1.0))
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT; sleep 1000"),
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False}
        )

        wait_breakpoint()
        release_breakpoint()

        wait(lambda: len(self._get_nodes_for_segment_in_tree("large_gpu")) == 1)
        wait(lambda: len(self._get_nodes_for_segment_in_tree("default")) == 0)

        large_gpu_segment_nodes = self._get_nodes_for_segment_in_tree("large_gpu")
        assert len(large_gpu_segment_nodes) == 1
        expected_node = large_gpu_segment_nodes[0]

        jobs = list(op.get_running_jobs())
        assert len(jobs) == 1
        expected_job = jobs[0]

        op.wait_for_fresh_snapshot()

        agent_to_incarnation = {}
        for agent in ls("//sys/controller_agents/instances"):
            agent_to_incarnation[agent] = wait_and_get_controller_incarnation(agent)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            update_pool_tree_config_option("default", "scheduling_segments/initialization_timeout", 60000, wait_for_orchid=False)

        node_segment_orchid_path = scheduler_orchid_path() + "/scheduler/nodes/{}/scheduling_segment"
        for node in ls("//sys/cluster_nodes"):
            expected_segment = "large_gpu" \
                if node == expected_node \
                else "default"
            wait(lambda: get(node_segment_orchid_path.format(node), default="") == expected_segment)

        # NB(eshcherbin): See: YT-14796.
        for agent, old_incarnation in agent_to_incarnation.items():
            wait(lambda: old_incarnation != wait_and_get_controller_incarnation(agent))

        wait(lambda: len(list(op.get_running_jobs())) == 1)
        jobs = list(op.get_running_jobs())
        assert len(jobs) == 1
        assert jobs[0] == expected_job

    @authors("eshcherbin")
    def test_node_changes_trees(self):
        set("//sys/pool_trees/default/@config/node_tag_filter", "!other")
        create_pool_tree("other", config={"node_tag_filter": "other", "main_resource": "gpu"})

        op = run_sleeping_vanilla(spec={"pool": "large_gpu"}, task_patch={"gpu_limit": 8, "enable_gpu_layers": False})
        wait(lambda: len(op.get_running_jobs()) == 1)

        wait(lambda: len(self._get_nodes_for_segment_in_tree("large_gpu", tree="default")) > 0)
        large_nodes = self._get_nodes_for_segment_in_tree("large_gpu", tree="default")
        assert len(large_nodes) == 1
        node = large_nodes[0]

        set("//sys/cluster_nodes/{}/@user_tags/end".format(node), "other")
        wait(lambda: get(scheduler_orchid_pool_path("<Root>", tree="other") + "/resource_limits/cpu") > 0)

        wait(lambda: get(scheduler_orchid_node_path(node) + "/scheduling_segment") == "default")
        wait(lambda: len(self._get_nodes_for_segment_in_tree("large_gpu", tree="other")) == 0)
        wait(lambda: len(self._get_nodes_for_segment_in_tree("large_gpu", tree="default")) == 1)

    @authors("eshcherbin")
    def test_manual_move_node_from_segment_and_back(self):
        node = list(ls("//sys/cluster_nodes"))[0]
        set("//sys/cluster_nodes/{}/@scheduling_options/scheduling_segment".format(node), "large_gpu")
        wait(lambda: get(scheduler_orchid_path() + "/scheduler/nodes/{}/scheduling_segment".format(node), default="") == "large_gpu")
        set("//sys/cluster_nodes/{}/@scheduling_options/scheduling_segment".format(node), "default")
        wait(lambda: get(scheduler_orchid_path() + "/scheduler/nodes/{}/scheduling_segment".format(node), default="") == "default")

    @authors("eshcherbin")
    def test_freeze_node_segment(self):
        set("//sys/pools/large_gpu/@strong_guarantee_resources", {"gpu": 80})

        blocking_op = run_sleeping_vanilla(
            job_count=10,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 1.0))

        op = run_sleeping_vanilla(
            job_count=8,
            spec={"pool": "small_gpu"},
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
        )
        time.sleep(3.0)
        assert are_almost_equal(self._get_dominant_usage_share(op.id), 0.0)

        node = list(ls("//sys/cluster_nodes"))[0]
        wait(lambda: get(scheduler_orchid_path() + "/scheduler/nodes/{}/scheduling_segment".format(node), default="") == "large_gpu")
        set("//sys/cluster_nodes/{}/@scheduling_options/scheduling_segment".format(node), "default")
        wait(lambda: get(scheduler_orchid_path() + "/scheduler/nodes/{}/scheduling_segment".format(node), default="") == "default")

        set("//sys/pools/large_gpu/@strong_guarantee_resources", {"gpu": 72})
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.1))

        op.complete()
        time.sleep(3.0)
        wait(lambda: get(scheduler_orchid_path() + "/scheduler/nodes/{}/scheduling_segment".format(node), default="") == "default")
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 0.9))

        remove("//sys/cluster_nodes/{}/@scheduling_options/scheduling_segment".format(node))
        wait(lambda: get(scheduler_orchid_path() + "/scheduler/nodes/{}/scheduling_segment".format(node), default="") == "large_gpu")
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 1.0))

    @authors("eshcherbin")
    def test_invalid_config(self):
        with pytest.raises(YtError):
            set("//sys/pool_trees/default/@config/scheduling_segments/data_centers", ["SAS", "VLA", ""])
        with pytest.raises(YtError):
            set("//sys/pool_trees/default/@config/scheduling_segments/data_centers", ["SAS", "VLA", ""])
        with pytest.raises(YtError):
            set("//sys/pool_trees/default/@config/scheduling_segments/reserve_fair_resource_amount", {"default": "-3.0"})
        with pytest.raises(YtError):
            set("//sys/pool_trees/default/@config/scheduling_segments/reserve_fair_resource_amount", {"default": {"SAS": "3.0"}})
        with pytest.raises(YtError):
            set("//sys/pool_trees/default/@config/scheduling_segments/reserve_fair_resource_amount", {"large_gpu": {"VLA": "3.0"}})
        with pytest.raises(YtError):
            set("//sys/pool_trees/default/@config/scheduling_segments/reserve_fair_resource_amount", {"large_gpu": {"SAS": "-3.0"}})

    @authors("eshcherbin")
    def test_only_gang_operations_in_large_segment(self):
        update_pool_tree_config_option(
            "default",
            "scheduling_segments/allow_only_gang_operations_in_large_segment",
            True)

        gang_op = run_sleeping_vanilla(
            job_count=8,
            spec={"pool": "large_gpu", "is_gang": True},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        op = run_sleeping_vanilla(
            job_count=8,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: get(scheduler_orchid_operation_path(gang_op.id) + "/scheduling_segment", default=None) == "large_gpu")
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/scheduling_segment", default=None) == "default")

    @authors("eshcherbin")
    def test_initialization_timeout_prevents_rebalancing(self):
        with Restarter(self.Env, SCHEDULERS_SERVICE):
            update_pool_tree_config_option("default", "scheduling_segments/initialization_timeout", 1000000000, wait_for_orchid=False)

        op = run_sleeping_vanilla(
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.0))

        time.sleep(5.0)
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.0))

    @authors("omgronny")
    def test_gpu_event_log(self):
        before_start_time = datetime.datetime.utcnow()
        op = run_sleeping_vanilla(
            job_count=4,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: len(op.get_running_jobs()) > 0)

        def collect_allocations_info_event():
            scheduler_log_file = self.path_to_run + "/logs/scheduler-0.json.log"
            structured_log = read_structured_log(scheduler_log_file)
            operation_ids = builtins.set()
            allocation_ids = builtins.set()
            for event in structured_log:
                if "timestamp" not in event or \
                    datetime.datetime.strptime(event["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ") < before_start_time or \
                    "event_type" not in event or \
                        event["event_type"] != "scheduling_segments_info" or \
                        "nodes" not in event:
                    continue
                for info in event["nodes"].values():
                    for allocation_id, allocation in info["running_allocations"].items():
                        allocation_ids.add(allocation_id)
                        operation_ids.add(allocation["operation_id"])

            return allocation_ids, operation_ids

        @wait_no_assert
        def check_allocation_ids():
            allocation_ids, operation_ids = collect_allocations_info_event()
            assert len(allocation_ids) == 4
            assert len(operation_ids) == 1
            assert op.id in operation_ids

##################################################################


@pytest.mark.skipif(
    is_asan_build() or is_debug_build(),
    reason="This test suite requires a genuine release build to fit into timeout"
)
class BaseTestSchedulingSegmentsMultiModule(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 10
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "fair_share_update_period": 100,
            "fair_share_profiling_period": 100,
            "operations_update_period": 100,
            "operation_stuck_check": {
                "period": 100,
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "controller_agent_connector": {
                    "heartbeat_executor": {
                        "period": 500,  # 500 msec
                    },
                },
            },
        },
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "gpu_manager": {
                "testing": {
                    "test_resource": True,
                    "test_gpu_count": 8
                },
            },
            "job_proxy": {
                "job_proxy_heartbeat_period": 100,
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 10,
                "user_slots": 10,
            },
        }
    }

    SCHEDULING_SEGMENTS = [
        "default",
        "large_gpu",
    ]

    DATA_CENTERS = ["SAS", "VLA"]
    RACKS = ["SAS1", "VLA1"]
    INFINIBAND_CLUSTERS = ["IBC1", "IBC2"]

    def _get_dominant_usage_share(self, op, tree="default"):
        return get(scheduler_orchid_operation_path(op, tree) + "/dominant_usage_share", default=0.0)

    def _get_dominant_fair_share(self, op, tree="default"):
        return get(scheduler_orchid_operation_path(op, tree) + "/detailed_dominant_fair_share/total", default=0.0)

    def _get_operation_module(self, op, tree="default"):
        return get(scheduler_orchid_operation_path(op.id, tree) + "/scheduling_segment_module", default=None)

    def _setup_node_modules(self, node_count_per_module):
        raise NotImplementedError()

    def _get_module_type(self):
        raise NotImplementedError()

    def _get_all_modules(self):
        raise NotImplementedError()

    def _get_node_module(self, node_address):
        raise NotImplementedError()

    def _get_node_tag_from_module(self, module):
        raise NotImplementedError()

    def _setup_data_centers(self, node_count_per_data_center, ibc_to_dc=None):
        dc_to_rack = dict(zip(BaseTestSchedulingSegmentsMultiModule.DATA_CENTERS, BaseTestSchedulingSegmentsMultiModule.RACKS))
        data_center_index_per_node = sum([[i] * count for i, count in enumerate(node_count_per_data_center)], [])

        def get_node_rack(i, node):
            if ibc_to_dc is None:
                return BaseTestSchedulingSegmentsMultiModule.RACKS[data_center_index_per_node[i]]
            ibc = get("//sys/cluster_nodes/{}/@annotations/infiniband_cluster_tag".format(node))
            return dc_to_rack[ibc_to_dc[ibc]]

        nodes = list(ls("//sys/cluster_nodes"))
        for i, node in enumerate(nodes):
            set("//sys/cluster_nodes/{}/@rack".format(node), get_node_rack(i, node))

        rack_to_dc = dict(zip(BaseTestSchedulingSegmentsMultiModule.RACKS, BaseTestSchedulingSegmentsMultiModule.DATA_CENTERS))
        for i, node in enumerate(nodes):
            wait(lambda: get(scheduler_orchid_node_path(node) + "/data_center") == rack_to_dc[get_node_rack(i, node)])

    def _setup_infiniband_clusters(self, node_count_per_infiniband_cluster):
        infiniband_cluster_index_per_node = sum([[i] * count for i, count in enumerate(node_count_per_infiniband_cluster)], [])
        with Restarter(self.Env, NODES_SERVICE):
            for i, node_config in enumerate(self.Env.configs["node"]):
                config = deepcopy(node_config)
                annotations = config.pop("cypress_annotations", dict())
                annotations["infiniband_cluster_tag"] = BaseTestSchedulingSegmentsMultiModule.INFINIBAND_CLUSTERS[infiniband_cluster_index_per_node[i]]
                config["cypress_annotations"] = annotations

                config_path = self.Env.config_paths["node"][i]
                with open(config_path, "wb") as fout:
                    yson.dump(config, fout)

        for node in ls("//sys/cluster_nodes"):
            def is_infiniband_cluster():
                return (
                    get(scheduler_orchid_node_path(node) + "/infiniband_cluster", default=None)
                    in BaseTestSchedulingSegmentsMultiModule.INFINIBAND_CLUSTERS)

            wait(is_infiniband_cluster)
            ibc = get(scheduler_orchid_node_path(node) + "/infiniband_cluster")
            set("//sys/cluster_nodes/{}/@user_tags/end".format(node), "infiniband_cluster_tag:{}".format(ibc))

    def _get_persistent_operation_segment_states_path(self, tree="default"):
        return "//sys/scheduler/strategy_state/tree_states/{}/scheduling_policy_state/scheduling_segments_state/operation_states".format(tree)

    def setup_method(self, method):
        super(BaseTestSchedulingSegmentsMultiModule, self).setup_method(method)

        create_pool("cpu", attributes={"allow_normal_preemption": False}, wait_for_orchid=False)
        create_pool("small_gpu", attributes={"allow_normal_preemption": False}, wait_for_orchid=False)
        create_pool("large_gpu", attributes={"allow_normal_preemption": False})
        set("//sys/pool_trees/default/@config/scheduling_segments", {
            "mode": "large_gpu",
            "initialization_timeout": 10000,
            "manage_period": 100,
            "unsatisfied_segments_rebalancing_timeout": 1000,
            "data_centers": BaseTestSchedulingSegmentsMultiModule.DATA_CENTERS,
            "infiniband_clusters": BaseTestSchedulingSegmentsMultiModule.INFINIBAND_CLUSTERS,
            "module_type": self._get_module_type(),
            "enable_detailed_logs": True,
        })
        set("//sys/pool_trees/default/@config/main_resource", "gpu")
        wait(lambda: get(scheduler_orchid_default_pool_tree_config_path() + "/scheduling_segments/mode") == "large_gpu")
        wait(
            lambda: get(
                scheduler_orchid_default_pool_tree_config_path()
                + "/scheduling_segments/unsatisfied_segments_rebalancing_timeout"
            ) == 1000
        )
        update_pool_tree_config("default", {
            "preemptive_scheduling_backoff": 0,
            "fair_share_starvation_timeout": 100,
            "fair_share_starvation_tolerance": 0.95,
            "preemption_satisfaction_threshold": 0.99,
            "non_preemptible_resource_usage_threshold": {"user_slots": 0},
        })

        # NB(eshcherbin): This is done to reset node segments.
        with Restarter(self.Env, SCHEDULERS_SERVICE):
            requests = [make_batch_request("set", path=get_persistent_node_segment_states_path(), input={})]
            for node in ls("//sys/cluster_nodes"):
                requests.append(make_batch_request(
                    "set",
                    path="//sys/cluster_nodes/{}/@scheduling_options".format(node),
                    input={},
                ))
            for response in execute_batch(requests):
                assert not get_batch_error(response)

        dc_to_rack = dict(zip(BaseTestSchedulingSegmentsMultiModule.DATA_CENTERS, BaseTestSchedulingSegmentsMultiModule.RACKS))
        for dc, r in dc_to_rack.items():
            create_data_center(dc)
            create_rack(r)
            set("//sys/racks/{}/@data_center".format(r), dc)

        module_count = BaseTestSchedulingSegmentsMultiModule.NUM_NODES // 2
        self._setup_node_modules([module_count, module_count])

    def _prepare_for_module_preemption_test(self):
        update_pool_tree_config_option("default", "scheduling_segments/module_assignment_heuristic", "min_remaining_feasible_capacity")
        update_pool_tree_config_option("default", "scheduling_segments/priority_module_assignment_timeout", 0)

        create_pool(
            "priority_large_gpu",
            parent_pool="large_gpu",
            attributes={
                "allow_normal_preemption": False,
                "enable_priority_scheduling_segment_module_assignment": True,
            })

    def _run_large_gpu_operations(self, job_count_per_operation):
        operations = []
        for job_count in job_count_per_operation:
            op = run_sleeping_vanilla(
                job_count=job_count,
                spec={"pool": "large_gpu"},
                task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            )
            wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.1 * job_count))
            operations.append(op)

        return operations

    @authors("eshcherbin")
    def test_module_locality_for_large_multihost_operations(self):
        op = run_sleeping_vanilla(
            job_count=5,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )

        wait(lambda: self._get_operation_module(op) in self._get_all_modules())
        module = self._get_operation_module(op)

        wait(lambda: len(op.get_running_jobs()) == 5)
        jobs = op.get_running_jobs()
        for _, job in jobs.items():
            assert self._get_node_module(job["address"]) == module

    @authors("eshcherbin")
    def test_no_module_locality_for_small_multihost_operations(self):
        op = run_sleeping_vanilla(
            job_count=12,
            spec={"pool": "small_gpu"},
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 48.0 / 80.0))

    @authors("eshcherbin")
    def test_uniform_distribution_of_large_operations_to_modules_1(self):
        ops = []
        for i in range(10):
            op = run_sleeping_vanilla(
                spec={"pool": "large_gpu"},
                task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            )
            ops.append(op)
            wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.1))

        modules = [self._get_operation_module(op_) for op_ in ops]
        assert modules[0] != modules[1]
        for i in range(2, 10):
            assert modules[i] == modules[i - 2]

    @authors("eshcherbin")
    def test_uniform_distribution_of_large_operations_to_modules_2(self):
        set("//sys/pools/large_gpu/@strong_guarantee_resources", {"gpu": 80})

        blocking_op = run_sleeping_vanilla(
            job_count=4,
            spec={"pool": "small_gpu"},
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 0.2))

        big_op = run_sleeping_vanilla(
            job_count=4,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(big_op.id), 0.4))
        big_module = self._get_operation_module(big_op)

        for i in range(4):
            op = run_sleeping_vanilla(
                spec={"pool": "large_gpu"},
                task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            )
            wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.1))
            wait(lambda: big_module != self._get_operation_module(op))

        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 0.2))

    @authors("eshcherbin")
    def test_specified_module(self):
        big_op = run_sleeping_vanilla(
            job_count=4,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(big_op.id), 0.4))
        big_module = self._get_operation_module(big_op)

        op = run_sleeping_vanilla(
            spec={"pool": "large_gpu", "scheduling_segment_modules": [big_module]},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.1))
        wait(lambda: big_module == self._get_operation_module(op))

    @authors("ignat")
    def test_missing_specified_module(self):
        with pytest.raises(YtError):
            run_sleeping_vanilla(
                spec={"pool": "large_gpu", "scheduling_segment_modules": ["UNKNOWN"]},
                task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
                track=True,
            )

    @authors("eshcherbin")
    def test_reserve_fair_resource_amount(self):
        module = self._get_all_modules()[0]
        update_pool_tree_config_option("default", "scheduling_segments/reserve_fair_resource_amount", {"large_gpu": {module: 8.0}})

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default"})
        fair_resource_amount_default_sensor = profiler.gauge(
            "scheduler/segments/fair_resource_amount",
            fixed_tags={"segment": "large_gpu", "module": module},
        )
        wait(lambda: are_almost_equal(fair_resource_amount_default_sensor.get(), 8.0))

        big_op = run_sleeping_vanilla(
            job_count=4,
            spec={"pool": "large_gpu", "scheduling_segment_modules": [module]},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(big_op.id), 0.4))
        wait(lambda: self._get_operation_module(big_op) == module)

        wait(lambda: are_almost_equal(fair_resource_amount_default_sensor.get(), 40.0))

        op1 = run_sleeping_vanilla(
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op1.id), 0.1))
        wait(lambda: self._get_operation_module(op1) != module)

        update_pool_tree_config_option("default", "scheduling_segments/reserve_fair_resource_amount", {"large_gpu": {module: 0.0}})

        op2 = run_sleeping_vanilla(
            spec={"pool": "large_gpu", "scheduling_segment_modules": [module]},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op2.id), 0.1))
        wait(lambda: self._get_operation_module(op2) == module)

        update_pool_tree_config_option("default", "scheduling_segments/reserve_fair_resource_amount", {"large_gpu": {module: 8.0}})
        time.sleep(3.0)

        wait(lambda: are_almost_equal(fair_resource_amount_default_sensor.get(), 40.0))

    @authors("omgronny")
    def test_module_preemption(self):
        self._prepare_for_module_preemption_test()

        ops_in_module1 = self._run_large_gpu_operations([3, 1])
        module1 = self._get_operation_module(ops_in_module1[0])
        assert len(frozenset([self._get_operation_module(op) for op in ops_in_module1])) == 1

        ops_in_module2 = self._run_large_gpu_operations([2, 2])
        module2 = self._get_operation_module(ops_in_module2[0])
        assert len(frozenset([self._get_operation_module(op) for op in ops_in_module2])) == 1

        priority_op = run_sleeping_vanilla(
            job_count=2,
            spec={"pool": "priority_large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(priority_op.id), 0.2))

        wait(lambda: self._get_operation_module(priority_op) == module1)
        wait(lambda: len([op.id for op in ops_in_module1 if self._get_operation_module(op) == module1]) == 1)
        wait(lambda: len([op.id for op in ops_in_module2 if self._get_operation_module(op) == module2]) == 2)

    @authors("omgronny")
    def test_module_preemption_in_specified_module(self):
        self._prepare_for_module_preemption_test()

        ops_in_module1 = self._run_large_gpu_operations([3, 1])
        module1 = self._get_operation_module(ops_in_module1[0])
        assert len(frozenset([self._get_operation_module(op) for op in ops_in_module1])) == 1

        ops_in_module2 = self._run_large_gpu_operations([2, 2])
        module2 = self._get_operation_module(ops_in_module2[0])
        assert len(frozenset([self._get_operation_module(op) for op in ops_in_module2])) == 1

        priority_op = run_sleeping_vanilla(
            job_count=2,
            spec={"pool": "priority_large_gpu", "scheduling_segment_modules": [module2]},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(priority_op.id), 0.2))

        wait(lambda: self._get_operation_module(priority_op) == module2)
        wait(lambda: len([op.id for op in ops_in_module1 if self._get_operation_module(op) == module1]) == 2)
        wait(lambda: len([op.id for op in ops_in_module2 if self._get_operation_module(op) == module2]) == 1)

    @authors("omgronny")
    def test_module_preemption_does_not_overcommit_modules(self):
        self._prepare_for_module_preemption_test()

        self._setup_node_modules([3, BaseTestSchedulingSegmentsMultiModule.NUM_NODES - 3])

        op_in_small_module = run_sleeping_vanilla(
            job_count=1,
            spec={"pool": "priority_large_gpu", "scheduling_segment_modules": [self._get_all_modules()[0]]},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op_in_small_module.id), 0.1))
        wait(lambda: self._get_operation_module(op_in_small_module) == self._get_all_modules()[0])

        large_module = self._get_all_modules()[1]
        ops_in_large_module = []
        for job_count in [3, 2, 1]:
            op = run_sleeping_vanilla(
                job_count=job_count,
                spec={"pool": "large_gpu", "scheduling_segment_modules": [large_module]},
                task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            )
            wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.1 * job_count))
            ops_in_large_module.append(op)

        priority_op = run_sleeping_vanilla(
            job_count=3,
            spec={"pool": "priority_large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(priority_op.id), 0.3))
        wait(lambda: self._get_operation_module(priority_op) == large_module)

        wait(lambda: self._get_operation_module(ops_in_large_module[0]) == large_module)
        wait(lambda: self._get_operation_module(ops_in_large_module[2]) == large_module)
        wait(lambda: self._get_operation_module(ops_in_large_module[1]) != large_module)

    @authors("eshcherbin")
    def test_module_reset_on_zero_fair_share(self):
        update_pool_tree_config_option("default", "scheduling_segments/enable_module_reset_on_zero_fair_share_and_usage", True)

        op = run_sleeping_vanilla(
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.1))
        op_module = self._get_operation_module(op)

        op.suspend(abort_running_jobs=True)

        big_op = run_sleeping_vanilla(
            job_count=5,
            spec={"pool": "large_gpu", "scheduling_segment_modules": [op_module]},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(big_op.id), 0.5))
        wait(lambda: self._get_operation_module(big_op) == op_module)

        op.resume()
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.1))
        wait(lambda: self._get_operation_module(op) != op_module)

    @authors("eshcherbin")
    def test_rebalance_large_gpu_segment_nodes_between_modules(self):
        create_pool("large_gpu_other", attributes={"allow_normal_preemption": False})
        set("//sys/pools/large_gpu/@strong_guarantee_resources", {"gpu": 40})
        set("//sys/pools/small_gpu/@strong_guarantee_resources", {"gpu": 40})

        blocking_op = run_sleeping_vanilla(
            job_count=10,
            spec={"pool": "small_gpu"},
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )

        big_op = run_sleeping_vanilla(
            job_count=3,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(big_op.id), 0.3))
        big_module = self._get_operation_module(big_op)

        opportunistic_op = run_sleeping_vanilla(
            job_count=2,
            spec={"pool": "large_gpu_other", "scheduling_segment_modules": [big_module]},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(opportunistic_op.id), 0.2))

        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 0.5))

        op = run_sleeping_vanilla(
            job_count=2,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.2))
        wait(lambda: big_module != self._get_operation_module(op))

        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 0.5))

    @authors("eshcherbin")
    def test_revive_operation_module(self):
        update_controller_agent_config("snapshot_period", 300)

        ops = []
        for i in range(10):
            op = run_sleeping_vanilla(
                spec={"pool": "large_gpu"},
                task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            )
            ops.append(op)

        modules = []
        for op in ops:
            op_module_path = self._get_persistent_operation_segment_states_path() + "/{}/module".format(op.id)
            wait(lambda: exists(op_module_path))
            modules.append(get(op_module_path))

        ops[-1].wait_for_fresh_snapshot()

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        for op, module in zip(ops, modules):
            wait(lambda: module == self._get_operation_module(op))

    @authors("eshcherbin")
    def test_profiling(self):
        set("//sys/pool_trees/default/@config/scheduling_segments/unsatisfied_segments_rebalancing_timeout", 1000000000)
        wait(lambda: get(scheduler_orchid_default_pool_tree_config_path() + "/scheduling_segments/unsatisfied_segments_rebalancing_timeout") == 1000000000)

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "default"})

        for module in self._get_all_modules():
            wait(lambda: profiler.gauge("scheduler/segments/module_capacity", fixed_tags={"module": module}).get() ==
                 8 * (BaseTestSchedulingSegmentsMultiModule.NUM_NODES / 2))

        fair_resource_amount_default_sensor = profiler.gauge("scheduler/segments/fair_resource_amount", fixed_tags={"segment": "default"})
        current_resource_amount_default_sensor = profiler.gauge("scheduler/segments/current_resource_amount", fixed_tags={"segment": "default"})
        fair_resource_amount_large_sensor = profiler.gauge("scheduler/segments/fair_resource_amount", fixed_tags={"segment": "large_gpu"})
        current_resource_amount_large_sensor = profiler.gauge("scheduler/segments/current_resource_amount", fixed_tags={"segment": "large_gpu"})

        wait(lambda: fair_resource_amount_default_sensor.get() == 0)
        wait(lambda: current_resource_amount_default_sensor.get() == 80)
        for module in self._get_all_modules():
            wait(lambda: fair_resource_amount_large_sensor.get(tags={"module": module}) == 0)
            wait(lambda: current_resource_amount_large_sensor.get(tags={"module": module}) == 0)

        blocking_op = run_sleeping_vanilla(
            job_count=20,
            spec={"pool": "small_gpu"},
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 1.0))

        wait(lambda: fair_resource_amount_default_sensor.get() == 80)
        wait(lambda: current_resource_amount_default_sensor.get() == 80)
        for module in self._get_all_modules():
            wait(lambda: fair_resource_amount_large_sensor.get(tags={"module": module}) == 0)
            wait(lambda: current_resource_amount_large_sensor.get(tags={"module": module}) == 0)

        op1 = run_sleeping_vanilla(
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_fair_share(op1.id), 0.1))
        wait(lambda: self._get_operation_module(op1) in self._get_all_modules())
        op1_module = self._get_operation_module(op1)

        time.sleep(3.0)

        wait(lambda: fair_resource_amount_default_sensor.get() == 72)
        wait(lambda: current_resource_amount_default_sensor.get() == 80)
        wait(lambda: fair_resource_amount_large_sensor.get(tags={"module": op1_module}) == 8)
        wait(lambda: current_resource_amount_large_sensor.get(tags={"module": op1_module}) == 0)

        set("//sys/pool_trees/default/@config/scheduling_segments/unsatisfied_segments_rebalancing_timeout", 1000)

        wait(lambda: fair_resource_amount_default_sensor.get() == 72)
        wait(lambda: current_resource_amount_default_sensor.get() == 72)
        wait(lambda: fair_resource_amount_large_sensor.get(tags={"module": op1_module}) == 8)
        wait(lambda: current_resource_amount_large_sensor.get(tags={"module": op1_module}) == 8)

        op2 = run_sleeping_vanilla(
            job_count=2,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_fair_share(op2.id), 0.2))
        wait(lambda: self._get_operation_module(op2) in self._get_all_modules())
        op2_module = self._get_operation_module(op2)
        assert op1_module != op2_module

        wait(lambda: fair_resource_amount_default_sensor.get() == 56)
        wait(lambda: current_resource_amount_default_sensor.get() == 56)
        wait(lambda: fair_resource_amount_large_sensor.get(tags={"module": op1_module}) == 8)
        wait(lambda: current_resource_amount_large_sensor.get(tags={"module": op1_module}) == 8)
        wait(lambda: fair_resource_amount_large_sensor.get(tags={"module": op2_module}) == 16)
        wait(lambda: current_resource_amount_large_sensor.get(tags={"module": op2_module}) == 16)

    @authors("eshcherbin")
    @pytest.mark.parametrize("allow_single_job", [True, False])
    def test_fail_large_gpu_operation_started_in_several_trees(self, allow_single_job):
        other_nodes = list(ls("//sys/cluster_nodes"))[:2]
        update_pool_tree_config("default", {
            "node_tag_filter": "!other",
            "allow_single_job_large_gpu_operations_in_multiple_trees": allow_single_job,
        })
        set("//sys/pool_trees/default/@config/node_tag_filter", "!other")
        create_pool_tree("other", config={"node_tag_filter": "other", "main_resource": "gpu"})
        for node in other_nodes:
            set("//sys/cluster_nodes/{}/@user_tags/end".format(node), "other")

        big_op = run_sleeping_vanilla(
            spec={"pool_trees": ["default", "other"]},
            job_count=2,
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: big_op.get_state() == "failed")

        big_op_single_tree = run_test_vanilla(
            "sleep 1",
            spec={"pool_trees": ["default", "other"], "schedule_in_single_tree": True},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        big_op_single_tree.track()

        big_op_single_job = run_test_vanilla(
            "sleep 1",
            spec={"pool_trees": ["default", "other"]},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        if allow_single_job:
            big_op_single_job.track()
        else:
            wait(lambda: big_op_single_job.get_state() == "failed")

        small_op = run_test_vanilla(
            "sleep 1",
            job_count=2,
            spec={"pool_trees": ["default", "other"]},
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )
        small_op.track()

    @authors("eshcherbin")
    def test_fail_operations_with_custom_tag_filter(self):
        blocking_op = run_sleeping_vanilla(
            job_count=5,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 0.5))
        wait(lambda: self._get_operation_module(blocking_op) in self._get_all_modules())
        module = self._get_operation_module(blocking_op)
        other_module = [module_ for module_ in self._get_all_modules() if module_ != module][0]

        op1 = run_sleeping_vanilla(
            spec={"pool": "large_gpu", "scheduling_tag_filter": self._get_node_tag_from_module(module)},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        op1.wait_for_state("failed")

        op2 = run_sleeping_vanilla(
            spec={"pool": "large_gpu", "scheduling_tag_filter": self._get_node_tag_from_module(other_module)},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op2.id), 0.1))

        op3 = run_sleeping_vanilla(
            spec={
                "pool": "large_gpu",
                "scheduling_tag_filter": "{} & !{}".format(
                    self._get_node_tag_from_module(module),
                    self._get_node_tag_from_module(other_module)
                )
            },
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_fair_share(op3.id), 0.1))
        time.sleep(1.0)
        op3.wait_for_state("running")
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op3.id), 0.0))

        op4 = run_sleeping_vanilla(
            spec={"pool": "large_gpu", "scheduling_tag_filter": self._get_node_tag_from_module(module)},
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_fair_share(op4.id), 0.05))
        time.sleep(1.0)
        op4.wait_for_state("running")
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op4.id), 0.0))

    @authors("eshcherbin")
    def test_min_remaining_feasible_capacity_assignment_heuristic(self):
        set("//sys/pool_trees/default/@config/scheduling_segments/module_assignment_heuristic", "min_remaining_feasible_capacity")
        wait(lambda: get(scheduler_orchid_default_pool_tree_config_path() + "/scheduling_segments/module_assignment_heuristic") ==
             "min_remaining_feasible_capacity")

        op1 = run_sleeping_vanilla(
            job_count=1,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: self._get_operation_module(op1) in self._get_all_modules())

        op2 = run_sleeping_vanilla(
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: self._get_operation_module(op2) in self._get_all_modules())

        op3 = run_sleeping_vanilla(
            job_count=4,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: self._get_operation_module(op3) in self._get_all_modules())

        op4 = run_sleeping_vanilla(
            job_count=3,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: self._get_operation_module(op4) in self._get_all_modules())

        assert self._get_operation_module(op1) == self._get_operation_module(op2) == self._get_operation_module(op4)
        assert self._get_operation_module(op1) != self._get_operation_module(op3)

    @authors("eshcherbin")
    @pytest.mark.parametrize("remove_all_module_nodes", [True, False])
    def test_module_reconsideration(self, remove_all_module_nodes):
        update_scheduler_config("running_allocations_update_period", 1000)
        update_pool_tree_config_option("default", "scheduling_segments/module_assignment_heuristic", "min_remaining_feasible_capacity")

        op = run_sleeping_vanilla(
            job_count=3,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.3))

        op_module = self._get_operation_module(op)

        module_nodes_by_segment = defaultdict(list)
        for node in ls("//sys/cluster_nodes"):
            if self._get_node_module(node) != op_module:
                continue
            node_segment = get(scheduler_orchid_path() + "/scheduler/nodes/{}/scheduling_segment".format(node), verbose=False)
            module_nodes_by_segment[node_segment].append(node)

        update_pool_tree_config_option("default", "node_tag_filter", "!other")
        create_pool_tree("other", config={"node_tag_filter": "other", "main_resource": "gpu"})

        nodes_to_move = module_nodes_by_segment["default"] + module_nodes_by_segment["large_gpu"] \
            if remove_all_module_nodes \
            else module_nodes_by_segment["default"][:2] + module_nodes_by_segment["large_gpu"][:1]
        for node in nodes_to_move:
            set("//sys/cluster_nodes/{}/@user_tags/end".format(node), "other")

        if remove_all_module_nodes:
            wait(lambda: self._get_dominant_usage_share(op.id) == 0.0)

        update_pool_tree_config_option("default", "scheduling_segments/module_reconsideration_timeout", 3000)
        wait(lambda: self._get_operation_module(op) != op_module)
        wait(lambda: op.get_job_count("aborted") >= 3)

        @wait_no_assert
        def check():
            jobs = op.get_running_jobs()
            assert len(jobs) == 3
            for _, job in jobs.items():
                assert self._get_node_module(job["address"]) != op_module

        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 3.0 / (10.0 - len(nodes_to_move))))

    @authors("eshcherbin")
    def test_module_reset_with_fail_on_job_restart(self):
        update_scheduler_config("operation_stuck_check", {"period": 1000000000})

        create_pool("large_gpu_other", attributes={"allow_normal_preemption": False})
        set("//sys/pools/large_gpu/@strong_guarantee_resources", {"gpu": 70})

        blocking_op = run_sleeping_vanilla(
            job_count=5,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(blocking_op.id), 0.5))

        other_op = run_sleeping_vanilla(
            job_count=4,
            spec={"pool": "large_gpu_other"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(other_op.id), 0.4))

        op = run_sleeping_vanilla(
            job_count=2,
            spec={
                "pool": "large_gpu",
                "fail_on_job_restart": True,
            },
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_fair_share(op.id), 0.2))
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.1))

        update_pool_tree_config_option("default", "scheduling_segments/module_reconsideration_timeout", 3000)
        blocking_op.abort()

        wait(lambda: op.get_job_count("aborted", from_orchid=False, verbose=True) > 0)
        op.wait_for_state("failed")

    @authors("eshcherbin")
    def test_rebalance_oversatisfied_segment(self):
        update_pool_tree_config_option("default", "enable_step_function_for_gang_operations", True)
        update_pool_tree_config_option("default", "enable_improved_fair_share_by_fit_factor_computation", True)
        update_pool_tree_config_option("default", "scheduling_segments/force_incompatible_segment_preemption", True)

        set("//sys/pool_trees/default/small_gpu/@strong_guarantee_resources", {"gpu": 8})
        set("//sys/pool_trees/default/large_gpu/@strong_guarantee_resources", {"gpu": 72})
        set("//sys/pool_trees/default/large_gpu/@mode", "fifo")
        wait(lambda: get(scheduler_orchid_pool_path("large_gpu") + "/mode", default=None) == "fifo")

        large_op1 = run_sleeping_vanilla(
            job_count=1,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        large_op2 = run_sleeping_vanilla(
            job_count=5,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        large_op3 = run_sleeping_vanilla(
            job_count=4,
            spec={"pool": "large_gpu", "is_gang": True},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_fair_share(large_op1.id), 0.1))
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(large_op1.id), 0.1))
        wait(lambda: are_almost_equal(self._get_dominant_fair_share(large_op2.id), 0.5))
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(large_op2.id), 0.5))
        wait(lambda: are_almost_equal(self._get_dominant_fair_share(large_op3.id), 0.4))
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(large_op3.id), 0.4))

        op = run_sleeping_vanilla(
            job_count=8,
            spec={
                "pool": "small_gpu",
            },
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )

        wait(lambda: are_almost_equal(self._get_dominant_fair_share(op.id), 0.2))
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.2))
        wait(lambda: are_almost_equal(self._get_dominant_fair_share(large_op3.id), 0.0))

        time.sleep(5.0)

        wait(lambda: are_almost_equal(self._get_dominant_fair_share(op.id), 0.2))
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.2))
        wait(lambda: are_almost_equal(self._get_dominant_fair_share(large_op3.id), 0.0))

        update_pool_tree_config_option("default", "scheduling_segments/module_oversatisfaction_threshold", 8.0)

        wait(lambda: are_almost_equal(self._get_dominant_fair_share(op.id), 0.2))
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.3))
        wait(lambda: are_almost_equal(self._get_dominant_fair_share(large_op3.id), 0.0))

        update_pool_tree_config_option("default", "scheduling_segments/module_oversatisfaction_threshold", 0.0)

        wait(lambda: are_almost_equal(self._get_dominant_fair_share(op.id), 0.2))
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(op.id), 0.4))
        wait(lambda: are_almost_equal(self._get_dominant_fair_share(large_op3.id), 0.0))


class TestSchedulingSegmentsMultiDataCenter(BaseTestSchedulingSegmentsMultiModule):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_TEST_PARTITIONS = 2

    def _setup_node_modules(self, node_count_per_module):
        self._setup_data_centers(node_count_per_module)

    def _get_module_type(self):
        return "data_center"

    def _get_all_modules(self):
        return BaseTestSchedulingSegmentsMultiModule.DATA_CENTERS

    def _get_node_module(self, node_address):
        return get("//sys/cluster_nodes/{}/@data_center".format(node_address), default="")

    def _get_node_tag_from_module(self, module):
        return module


class TestSchedulingSegmentsMultiInfinibandCluster(BaseTestSchedulingSegmentsMultiModule):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_TEST_PARTITIONS = 2

    def _setup_node_modules(self, node_count_per_module):
        self._setup_infiniband_clusters(node_count_per_module)

    def _get_module_type(self):
        return "infiniband_cluster"

    def _get_all_modules(self):
        return BaseTestSchedulingSegmentsMultiModule.INFINIBAND_CLUSTERS

    def _get_node_module(self, node_address):
        return get("//sys/cluster_nodes/{}/@annotations/infiniband_cluster_tag".format(node_address), default="")

    def _get_node_tag_from_module(self, module):
        return "infiniband_cluster_tag:{}".format(module)

    def setup_method(self, method):
        super(TestSchedulingSegmentsMultiInfinibandCluster, self).setup_method(method)
        set("//sys/pool_trees/default/@config/scheduling_segments/enable_infiniband_cluster_tag_validation", True)


class TestInfinibandClusterTagValidation(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
        },
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "gpu_manager": {
                "testing": {
                    "test_resource": True,
                    "test_gpu_count": 8
                },
            },
        },
    }

    SCHEDULING_SEGMENTS = [
        "default",
        "large_gpu",
    ]

    INFINIBAND_CLUSTERS = ["IBC1", "IBC2"]

    def _setup_infiniband_clusters(self, node_ibcs):
        assert len(node_ibcs) == TestInfinibandClusterTagValidation.NUM_NODES

        with Restarter(self.Env, NODES_SERVICE):
            for i, node_config in enumerate(self.Env.configs["node"]):
                config = deepcopy(node_config)
                annotations = config.pop("cypress_annotations", dict())
                if node_ibcs[i] is None:
                    if "infiniband_cluster_tag" in annotations:
                        del annotations["infiniband_cluster_tag"]
                else:
                    annotations["infiniband_cluster_tag"] = node_ibcs[i]
                config["cypress_annotations"] = annotations

                config_path = self.Env.config_paths["node"][i]
                with open(config_path, "wb") as fout:
                    yson.dump(config, fout)

        for node in ls("//sys/cluster_nodes"):
            ibc = get("//sys/cluster_nodes/{}/@annotations/infiniband_cluster_tag".format(node), default=yson.YsonEntity())
            wait(lambda: get(scheduler_orchid_node_path(node) + "/infiniband_cluster", default=None) == ibc)
            if ibc in TestInfinibandClusterTagValidation.INFINIBAND_CLUSTERS:
                tag = "infiniband_cluster_tag:{}".format(ibc)
                set("//sys/cluster_nodes/{}/@user_tags/end".format(node), tag)
                wait(lambda: tag in get(scheduler_orchid_node_path(node) + "/tags"))

    def _enable_ibc_tag_validation(self):
        set("//sys/pool_trees/default/@config/scheduling_segments/enable_infiniband_cluster_tag_validation", True)

    def _check_alert(self, message):
        wait(lambda: get("//sys/scheduler/@alerts"))

        @wait_no_assert
        def do_check():
            alert = get("//sys/scheduler/@alerts")[0]
            assert alert["attributes"]["alert_type"] == "manage_scheduling_segments"
            assert message in alert["inner_errors"][0]["inner_errors"][0]["message"]

    def setup_method(self, method):
        super(TestInfinibandClusterTagValidation, self).setup_method(method)

        set("//sys/pool_trees/default/@config/scheduling_segments", {
            "mode": "large_gpu",
            "initialization_timeout": 10000,
            "manage_period": 100,
            "unsatisfied_segments_rebalancing_timeout": 1000,
            "infiniband_clusters": TestInfinibandClusterTagValidation.INFINIBAND_CLUSTERS,
            "module_type": "infiniband_cluster",
            "enable_detailed_logs": True,
        })
        set("//sys/pool_trees/default/@config/main_resource", "gpu")
        wait(lambda: get(scheduler_orchid_default_pool_tree_config_path() + "/scheduling_segments/mode") == "large_gpu")
        wait(
            lambda: get(
                scheduler_orchid_default_pool_tree_config_path()
                + "/scheduling_segments/unsatisfied_segments_rebalancing_timeout"
            ) == 1000
        )

        # NB(eshcherbin): This is done to reset node segments.
        with Restarter(self.Env, SCHEDULERS_SERVICE):
            requests = [make_batch_request("set", path=get_persistent_node_segment_states_path(), input={})]
            for node in ls("//sys/cluster_nodes"):
                requests.append(make_batch_request(
                    "set",
                    path="//sys/cluster_nodes/{}/@scheduling_options".format(node),
                    input={},
                ))
            for response in execute_batch(requests):
                assert not get_batch_error(response)

        wait(lambda: not get("//sys/scheduler/@alerts"))

    @authors("eshcherbin")
    def test_valid(self):
        self._setup_infiniband_clusters(["IBC1", "IBC1", "IBC1"])
        self._enable_ibc_tag_validation()

        time.sleep(3.0)
        assert not get("//sys/scheduler/@alerts")

    @authors("eshcherbin")
    @pytest.mark.parametrize("invalid_ibc", [None, "IBC3"])
    def test_invalid_infiniband_cluster(self, invalid_ibc):
        self._setup_infiniband_clusters(["IBC1", "IBC2", invalid_ibc])
        self._enable_ibc_tag_validation()
        self._check_alert("Node's infiniband cluster is invalid or missing")

    @authors("eshcherbin")
    def test_missing_infiniband_cluster_tag(self):
        self._setup_infiniband_clusters(["IBC1", "IBC2", "IBC2"])
        self._enable_ibc_tag_validation()

        node = ls("//sys/cluster_nodes")[0]
        set("//sys/cluster_nodes/{}/@user_tags".format(node), [])

        self._check_alert("Node has no infiniband cluster tags")

    @authors("eshcherbin")
    def test_too_many_infiniband_cluster_tags(self):
        self._setup_infiniband_clusters(["IBC1", "IBC2", "IBC2"])
        self._enable_ibc_tag_validation()

        node = ls("//sys/cluster_nodes")[0]
        node_ibc = get(scheduler_orchid_node_path(node) + "/infiniband_cluster")
        other_ibc = [ibc for ibc in TestInfinibandClusterTagValidation.INFINIBAND_CLUSTERS if ibc != node_ibc][0]
        set("//sys/cluster_nodes/{}/@user_tags/end".format(node), "infiniband_cluster_tag:{}".format(other_ibc))

        self._check_alert("Node has more than one infiniband cluster tags")

    @authors("eshcherbin")
    def test_wrong_many_infiniband_cluster_tag(self):
        self._setup_infiniband_clusters(["IBC2", "IBC2", "IBC2"])
        self._enable_ibc_tag_validation()

        node = ls("//sys/cluster_nodes")[0]
        node_ibc = get(scheduler_orchid_node_path(node) + "/infiniband_cluster")
        other_ibc = [ibc for ibc in TestInfinibandClusterTagValidation.INFINIBAND_CLUSTERS if ibc != node_ibc][0]
        set("//sys/cluster_nodes/{}/@user_tags".format(node), ["infiniband_cluster_tag:{}".format(other_ibc)])

        wait(lambda: get("//sys/scheduler/@alerts"))
        alert = get("//sys/scheduler/@alerts")[0]
        assert alert["attributes"]["alert_type"] == "manage_scheduling_segments"
        self._check_alert("Node's infiniband cluster tag doesn't match its infiniband cluster from annotations")


##################################################################


class TestRunningJobStatistics(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "fair_share_update_period": 100,
            "fair_share_profiling_period": 100,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "gpu_manager": {
                "testing": {
                    "test_resource": True,
                    "test_gpu_count": 8
                },
            },
            "job_proxy": {
                "job_proxy_heartbeat_period": 100,
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 2,
                "user_slots": 2,
            },
        }
    }

    SCHEDULING_SEGMENTS = [
        "default",
        "large_gpu",
    ]

    DATA_CENTER = "SAS"
    RACK = "SAS1"

    def _get_dominant_usage_share(self, op, tree="default"):
        return get(scheduler_orchid_operation_path(op, tree) + "/dominant_usage_share", default=0.0)

    # TODO(eshcherbin): Do something with copy-paste in this long setup method.
    def setup_method(self, method):
        super(TestRunningJobStatistics, self).setup_method(method)

        create_pool("small_gpu", attributes={"allow_normal_preemption": False}, wait_for_orchid=False)
        create_pool("large_gpu", attributes={"allow_normal_preemption": False})
        set("//sys/pool_trees/default/@config/scheduling_segments", {
            "mode": "large_gpu",
            "initialization_timeout": 10000,
            "manage_period": 100,
            "unsatisfied_segments_rebalancing_timeout": 1000,
            "data_centers": [TestRunningJobStatistics.DATA_CENTER],
            "enable_detailed_logs": True,
        })
        set("//sys/pool_trees/default/@config/main_resource", "gpu")
        wait(lambda: get(scheduler_orchid_default_pool_tree_config_path() + "/scheduling_segments/mode") == "large_gpu")
        wait(
            lambda: get(
                scheduler_orchid_default_pool_tree_config_path()
                + "/scheduling_segments/unsatisfied_segments_rebalancing_timeout"
            ) == 1000
        )
        update_pool_tree_config("default", {
            "preemptive_scheduling_backoff": 0,
            "fair_share_starvation_timeout": 100,
            "fair_share_starvation_tolerance": 0.95,
            "preemption_satisfaction_threshold": 0.99,
            "non_preemptible_resource_usage_threshold": {"user_slots": 0},
        })

        # NB(eshcherbin): This is done to reset node segments.
        with Restarter(self.Env, SCHEDULERS_SERVICE):
            requests = [make_batch_request("set", path=get_persistent_node_segment_states_path(), input={})]
            for node in ls("//sys/cluster_nodes"):
                requests.append(make_batch_request(
                    "set",
                    path="//sys/cluster_nodes/{}/@scheduling_options".format(node),
                    input={},
                ))
            for response in execute_batch(requests):
                assert not get_batch_error(response)

        create_data_center(TestRunningJobStatistics.DATA_CENTER)
        create_rack(TestRunningJobStatistics.RACK)
        set("//sys/racks/{}/@data_center".format(TestRunningJobStatistics.RACK), TestRunningJobStatistics.DATA_CENTER)
        for node in ls("//sys/cluster_nodes"):
            set("//sys/cluster_nodes/{}/@rack".format(node), TestRunningJobStatistics.RACK)
        for node in ls("//sys/cluster_nodes"):
            wait(lambda: get(scheduler_orchid_node_path(node) + "/data_center") == TestRunningJobStatistics.DATA_CENTER)

    @authors("eshcherbin")
    def test_long_job_preparation(self):
        aux_op = run_sleeping_vanilla(
            job_count=2,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )

        for node in ls("//sys/cluster_nodes"):
            wait(lambda: get(scheduler_orchid_node_path(node) + "/scheduling_segment") == "large_gpu")
        aux_op.abort(wait_until_finished=True)
        for node in ls("//sys/cluster_nodes"):
            wait(lambda: get(scheduler_orchid_node_path(node) + "/running_job_statistics/total_gpu_time") == 0.0)

        op = run_sleeping_vanilla(
            job_count=1,
            spec={
                "pool": "large_gpu",
                "job_testing_options": {"delay_after_node_directory_prepared": 10000},
            },
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        good_node = get_first_job_node(op)

        bad_op = run_sleeping_vanilla(
            job_count=1,
            spec={"pool": "small_gpu"},
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
        )
        wait(lambda: are_almost_equal(self._get_dominant_usage_share(bad_op.id), 0.25))
        assert get(scheduler_orchid_node_path(good_node) + "/scheduling_segment") == "large_gpu"


##################################################################


class TestNetworkPriority(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "fair_share_update_period": 100,
            "fair_share_profiling_period": 100,
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "controller_agent_connector": {
                    "heartbeat_executor": {
                        "period": 500,
                    },
                },
                "gpu_manager": {
                    "enable_network_service_level": True,
                },
            },
        },
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "gpu_manager": {
                "testing": {
                    "test_resource": True,
                    "test_gpu_count": 8
                },
            },
            "job_proxy": {
                "job_proxy_heartbeat_period": 100,
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 10,
                "user_slots": 10,
            },
        },
        "slot_manager": {
            "job_environment": {
                "type": "porto",
            },
        },
    }

    USE_PORTO = True

    SCHEDULING_SEGMENTS = [
        "default",
        "large_gpu",
    ]

    DATA_CENTER = "SAS"
    RACK = "SAS1"

    def setup_method(self, method):
        super(TestNetworkPriority, self).setup_method(method)

        set("//sys/pool_trees/default/@config/scheduling_segments", {
            "mode": "large_gpu",
            "initialization_timeout": 10000,
            "manage_period": 100,
            "unsatisfied_segments_rebalancing_timeout": 1000,
            "data_centers": [TestNetworkPriority.DATA_CENTER],
            "enable_detailed_logs": True,
            "module_share_to_network_priority": [{
                "module_share": 0.1,
                "network_priority": 1,
            }, {
                "module_share": 0.4,
                "network_priority": 2,
            }, {
                "module_share": 0.8,
                "network_priority": 3,
            }]
        })
        set("//sys/pool_trees/default/@config/main_resource", "gpu")
        wait(lambda: get(scheduler_orchid_default_pool_tree_config_path() + "/scheduling_segments/mode") == "large_gpu")
        update_pool_tree_config("default", {
            "preemptive_scheduling_backoff": 0,
            "fair_share_starvation_timeout": 100,
            "fair_share_starvation_tolerance": 0.95,
            "preemption_satisfaction_threshold": 0.99,
            "non_preemptible_resource_usage_threshold": {"user_slots": 0},
        })

        # NB(eshcherbin): This is done to reset node segments.
        with Restarter(self.Env, SCHEDULERS_SERVICE):
            requests = [
                make_batch_request("set", path=get_persistent_node_segment_states_path(), input={}),
            ]
            for node in ls("//sys/cluster_nodes"):
                requests.append(make_batch_request(
                    "set",
                    path="//sys/cluster_nodes/{}/@scheduling_options".format(node),
                    input={},
                ))
            for response in execute_batch(requests):
                assert not get_batch_error(response)

        create_data_center(TestNetworkPriority.DATA_CENTER)
        create_rack(TestNetworkPriority.RACK)
        set("//sys/racks/{}/@data_center".format(TestNetworkPriority.RACK), TestNetworkPriority.DATA_CENTER)
        for node in ls("//sys/cluster_nodes"):
            set("//sys/cluster_nodes/{}/@rack".format(node), TestNetworkPriority.RACK)
        for node in ls("//sys/cluster_nodes"):
            wait(lambda: get(scheduler_orchid_node_path(node) + "/data_center") == TestNetworkPriority.DATA_CENTER)

    @authors("renadeen")
    def test_network_priority(self):
        # full module op, network priority is 3 from config
        from_barriers = self.write_log_barriers_on_all_nodes()
        full_module_op = run_sleeping_vanilla(
            job_count=5,
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: len(full_module_op.get_running_jobs()) == 5)

        self.assert_network_priority_of_all_jobs(full_module_op, from_barriers, 3)

        full_module_op.abort()

        # one node op, network priority is 1 from config
        from_barriers = self.write_log_barriers_on_all_nodes()
        one_node_op = run_sleeping_vanilla(
            job_count=1,
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: len(one_node_op.get_running_jobs()) == 1)

        self.assert_network_priority_of_all_jobs(one_node_op, from_barriers, 1)
        one_node_op.abort()

        # half module op, network priority is 2 from config
        from_barriers = self.write_log_barriers_on_all_nodes()
        half_module_op = run_sleeping_vanilla(
            job_count=3,
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: len(half_module_op.get_running_jobs()) == 3)

        self.assert_network_priority_of_all_jobs(half_module_op, from_barriers, 2)
        half_module_op.abort()

        # one gpu full module op, network priority is 0 as op is not from large segment
        from_barriers = self.write_log_barriers_on_all_nodes()
        four_gpu_full_module_op = run_sleeping_vanilla(
            job_count=10,
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )
        wait(lambda: len(four_gpu_full_module_op.get_running_jobs()) == 10)

        self.assert_network_priority_of_all_jobs(four_gpu_full_module_op, from_barriers, 0)
        four_gpu_full_module_op.abort()

    def write_log_barriers_on_all_nodes(self):
        barriers = []
        for i in range(TestNetworkPriority.NUM_NODES):
            barriers.append(write_log_barrier(self.get_node_address(i)))
        return barriers

    def assert_network_priority_of_all_jobs(self, op, from_barriers, expected_network_priority):
        address_to_node_index = self.get_address_to_node_index()
        jobs = op.get_running_jobs().values()
        node_indices = builtins.set([address_to_node_index[job["address"]] for job in jobs])
        wait(lambda: self.check_network_priority_on_nodes(node_indices, from_barriers, expected_network_priority))

    def check_network_priority_on_nodes(self, node_indices, from_barriers, expected_network_priority):
        for node_index in node_indices:
            log = read_structured_log(
                self.get_structured_log_path(node_index),
                from_barrier=from_barriers[node_index],
                row_filter=lambda row: "event_type" in row and row["event_type"] == "apply_network_priority_in_test",
            )
            if len(log) == 0:
                print_debug("log of node {} is empty".format(node_index))
                return False

            assert len(log) == 1, "log has more than one row: {}".format(log)
            assert log[0]["network_priority"] == expected_network_priority, "wrong network priority in log: {}, node_index: {}".format(log, node_index)
            return True

    def get_address_to_node_index(self):
        result = {}
        for i in range(TestNetworkPriority.NUM_NODES):
            result[self.get_node_address(i)] = i

        return result

    def get_structured_log_path(self, node_id):
        return "{}/logs/node-{}.json.log".format(self.path_to_run, node_id)

    def get_node_address(self, node_id):
        return "localhost:" + str(self.Env.configs["node"][node_id]["rpc_port"])


class TestDefaultGpuFullHostPreemption(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "fair_share_update_period": 100,
            "fair_share_profiling_period": 100,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "gpu_manager": {
                "testing": {
                    "test_resource": True,
                    "test_gpu_count": 8
                },
            },
            "job_proxy": {
                "job_proxy_heartbeat_period": 100,
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 20,
                "user_slots": 20,
            },
        }
    }

    SCHEDULING_SEGMENTS = [
        "default",
        "large_gpu",
    ]

    DATA_CENTER = "SAS"
    RACK = "SAS1"

    def setup_method(self, method):
        super().setup_method(method)
        update_pool_tree_config("default", {
            "main_resource": "gpu",
            "fair_share_starvation_timeout": 1000,
            "default_gpu_full_host_preemption": {
                "enable": True,
                "max_preemption_penalty": 200.0,
                "timeout": 5000,
            }
        })
        set("//sys/pool_trees/default/@config/scheduling_segments", {
            "mode": "large_gpu",
            "initialization_timeout": 10000,
            "manage_period": 100,
            "unsatisfied_segments_rebalancing_timeout": 1000,
            "data_centers": [TestDefaultGpuFullHostPreemption.DATA_CENTER],
            "enable_detailed_logs": True,
        })
        wait(lambda: get(scheduler_orchid_default_pool_tree_config_path() + "/scheduling_segments/mode") == "large_gpu")
        wait(
            lambda: get(
                scheduler_orchid_default_pool_tree_config_path()
                + "/scheduling_segments/unsatisfied_segments_rebalancing_timeout"
            )
            == 1000
        )

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            requests = [
                make_batch_request("set", path=get_persistent_node_segment_states_path(), input={}),
            ]
            for node in ls("//sys/cluster_nodes"):
                requests.append(make_batch_request(
                    "set",
                    path="//sys/cluster_nodes/{}/@scheduling_options".format(node),
                    input={},
                ))
            for response in execute_batch(requests):
                assert not get_batch_error(response)

        create_data_center(TestDefaultGpuFullHostPreemption.DATA_CENTER)
        create_rack(TestDefaultGpuFullHostPreemption.RACK)
        set("//sys/racks/{}/@data_center".format(TestDefaultGpuFullHostPreemption.RACK), TestDefaultGpuFullHostPreemption.DATA_CENTER)
        for node in ls("//sys/cluster_nodes"):
            set("//sys/cluster_nodes/{}/@rack".format(node), TestDefaultGpuFullHostPreemption.RACK)
        for node in ls("//sys/cluster_nodes"):
            wait(lambda: get(scheduler_orchid_node_path(node) + "/data_center") == TestDefaultGpuFullHostPreemption.DATA_CENTER)

    @authors("severovv")
    def test_preemption_basic(self):
        create_pool("pool1", attributes={"strong_guarantee_resources": {"gpu": 16}}, wait_for_orchid=False)
        create_pool("pool2", attributes={"strong_guarantee_resources": {"gpu": 8}})

        nodes = list(ls("//sys/cluster_nodes"))
        small_ops = []
        for node in nodes:
            op = run_sleeping_vanilla(
                job_count=2,
                task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
                pool="pool1",
                spec={"scheduling_tag_filter": node}
            )
            small_ops.append(op)
        wait(lambda: all(op.get_running_jobs() for op in small_ops))

        update_pool_tree_config_option("default", "default_gpu_full_host_preemption/max_preemption_penalty", 0.0)
        full_host_op = run_test_vanilla(
            "sleep 10",
            job_count=2,
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={"pool": "pool2", "scheduling_segment": "default"}
        )
        time.sleep(5)
        assert len(full_host_op.get_running_jobs()) == 0

        update_pool_tree_config_option("default", "default_gpu_full_host_preemption/max_preemption_penalty", 100.0)
        wait(lambda: full_host_op.get_running_jobs())

    @authors("severovv")
    def test_preemption_timeout(self):
        create_pool("pool1", attributes={"strong_guarantee_resources": {"gpu": 16}}, wait_for_orchid=False)
        create_pool("pool2", attributes={"strong_guarantee_resources": {"gpu": 8}})

        nodes = list(ls("//sys/cluster_nodes"))
        small_ops = []
        for node in nodes:
            op = run_sleeping_vanilla(
                job_count=2,
                task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
                pool="pool1",
                spec={"scheduling_tag_filter": node}
            )
            small_ops.append(op)
        wait(lambda: all(op.get_running_jobs() for op in small_ops))

        update_pool_tree_config_option("default", "default_gpu_full_host_preemption/max_preemption_penalty", 1000.0)
        update_pool_tree_config_option("default", "default_gpu_full_host_preemption/timeout", 1000000)
        full_host_op = run_test_vanilla(
            "sleep 10",
            job_count=2,
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={"pool": "pool2", "scheduling_segment": "default"}
        )
        time.sleep(5)
        assert len(full_host_op.get_running_jobs()) == 0

        update_pool_tree_config_option("default", "default_gpu_full_host_preemption/timeout", 100)
        wait(lambda: full_host_op.get_running_jobs())

    @authors("severovv")
    def test_preemption_priority(self):
        create_pool("pool1", attributes={"strong_guarantee_resources": {"gpu": 16}}, wait_for_orchid=False)
        create_pool("pool2", attributes={"strong_guarantee_resources": {"gpu": 0}}, wait_for_orchid=False)
        create_pool("pool3", attributes={"strong_guarantee_resources": {"gpu": 8}})

        nodes = list(ls("//sys/cluster_nodes"))
        small_ops = []
        for node in nodes[:-1]:
            op = run_sleeping_vanilla(
                job_count=2,
                task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
                pool="pool1",
                spec={"scheduling_tag_filter": node}
            )
            small_ops.append(op)

        preemptible_op = run_sleeping_vanilla(
            job_count=2,
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            pool="pool2",
            spec={"scheduling_tag_filter": nodes[-1]}
        )

        create("table", "//tmp/t_in")
        write_table("<append=true>//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out1")

        wait(lambda: all(op.get_running_jobs() for op in small_ops))
        wait(lambda: preemptible_op.get_running_jobs())

        big_mapper = map(
            command="sleep 100; cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out1",
            spec={"pool": "pool3", "job_count": 1, "mapper": {"gpu_limit": 8, "enable_gpu_layers": False}, "scheduling_segment": "default"},
            track=False,
        )
        wait(lambda: big_mapper.get_running_jobs())
        for op in small_ops:
            assert len(op.get_running_jobs()) == 2
        assert len(preemptible_op.get_running_jobs()) == 0

    @authors("severovv")
    def test_self_preemption(self):
        create_pool("pool1", attributes={"strong_guarantee_resources": {"gpu": 8}}, wait_for_orchid=False)
        create_pool("pool2", attributes={"strong_guarantee_resources": {"gpu": 8}}, wait_for_orchid=False)
        create_pool("pool3", attributes={"strong_guarantee_resources": {"gpu": 8}})

        nodes = list(ls("//sys/cluster_nodes"))
        small_ops = []
        for node in nodes[:2]:
            op = run_sleeping_vanilla(
                job_count=2,
                task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
                pool="pool1",
                spec={"scheduling_tag_filter": node}
            )
            small_ops.append(op)
        wait(lambda: all(op.get_running_jobs() for op in small_ops))

        create("table", "//tmp/t_in")
        write_table("<append=true>//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        other_mapper = map(
            command="sleep 100; cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out1",
            spec={"pool": "pool2", "job_count": 1, "mapper": {"gpu_limit": 8, "enable_gpu_layers": False}, "scheduling_segment": "default"},
            track=False,
        )
        wait(lambda: len(other_mapper.get_running_jobs()) > 0)
        assert all(len(job.get_running_jobs()) == 2 for job in small_ops)

        big_mapper = map(
            command="sleep 10; cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out2",
            spec={"pool": "pool3", "job_count": 1, "mapper": {"gpu_limit": 8, "enable_gpu_layers": False}, "scheduling_segment": "default"},
            track=False,
        )
        wait(lambda: big_mapper.get_running_jobs())
        assert len(other_mapper.get_running_jobs()) > 0

    @authors("severovv")
    def test_low_fair_share(self):
        create_pool("pool1", attributes={"strong_guarantee_resources": {"gpu": 20}}, wait_for_orchid=False)
        create_pool("pool2", attributes={"strong_guarantee_resources": {"gpu": 4}})
        op = run_sleeping_vanilla(
            job_count=5,
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            pool="pool1",
        )
        wait(lambda: op.get_running_jobs())

        create("table", "//tmp/t_in")
        write_table("<append=true>//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out1")

        big_mapper = map(
            command="sleep 100; cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out1",
            spec={"pool": "pool2", "job_count": 1, "mapper": {"gpu_limit": 8, "enable_gpu_layers": False}, "scheduling_segment": "default"},
            track=False,
        )

        deactivations = scheduler_orchid_operation_path(big_mapper.id) + "/deactivation_reasons/fair_share_limits_exceeded"
        wait(lambda: get(deactivations, default=0) > 0)
        assert len(big_mapper.get_running_jobs()) == 0

    @authors("severovv")
    def test_other_segment_preemption(self):
        create_pool("pool1", attributes={"strong_guarantee_resources": {"gpu": 8}}, wait_for_orchid=False)
        create_pool("pool2", attributes={"strong_guarantee_resources": {"gpu": 8}}, wait_for_orchid=False)
        create_pool("pool3", attributes={"strong_guarantee_resources": {"gpu": 8}})

        nodes = list(ls("//sys/cluster_nodes"))
        small_ops = []
        for node in nodes[:2]:
            op = run_sleeping_vanilla(
                job_count=2,
                task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
                pool="pool1",
                spec={"scheduling_tag_filter": node}
            )
            small_ops.append(op)
        wait(lambda: all(op.get_running_jobs() for op in small_ops))

        create("table", "//tmp/t_in")
        write_table("<append=true>//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        large_gpu_op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={"pool": "pool2", "job_count": 1},
            track=False,
        )
        wait(lambda: len(large_gpu_op.get_running_jobs()) > 0)
        assert all(len(job.get_running_jobs()) == 2 for job in small_ops)

        big_mapper = map(
            command="sleep 10; cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out2",
            spec={"pool": "pool3", "job_count": 1, "mapper": {"gpu_limit": 8, "enable_gpu_layers": False}, "scheduling_segment": "default"},
            track=False,
        )
        wait(lambda: big_mapper.get_running_jobs())
        assert len(large_gpu_op.get_running_jobs()) > 0

    @authors("severovv")
    def test_segments_rebalancing(self):
        create_pool("pool1", attributes={"strong_guarantee_resources": {"gpu": 16}}, wait_for_orchid=False)
        create_pool("pool2", attributes={"strong_guarantee_resources": {"gpu": 0}}, wait_for_orchid=False)
        create_pool("pool3", attributes={"strong_guarantee_resources": {"gpu": 8}})

        nodes = list(ls("//sys/cluster_nodes"))
        small_op = run_sleeping_vanilla(
            job_count=2,
            pool="pool1",
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            spec={"scheduling_tag_filter": nodes[0]},
            track=False,
        )
        wait(lambda: len(small_op.get_running_jobs()) == 2)

        large_gpu_op = run_sleeping_vanilla(
            job_count=2,
            pool="pool2",
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={"scheduling_segment": "large_gpu"},
            track=False,
        )
        wait(lambda: len(large_gpu_op.get_running_jobs()) == 2)

        real_job = run_sleeping_vanilla(
            job_count=1,
            pool="pool3",
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={"scheduling_segment": "default"},
            track=False,
        )
        wait(lambda: len(real_job.get_running_jobs()) == 1)
        assert len(small_op.get_running_jobs()) == 2
        assert len(large_gpu_op.get_running_jobs()) == 1

    @authors("severovv")
    def test_correct_scheduling_below_fair_share(self):
        nodes = list(ls("//sys/cluster_nodes"))

        only_one_runs = run_sleeping_vanilla(
            job_count=3,
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={"scheduling_segment": "default", "scheduling_tag_filter": nodes[0]},
        )
        wait(lambda: len(only_one_runs.get_running_jobs()) == 1)

        must_run = run_sleeping_vanilla(
            job_count=1,
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={"scheduling_segment": "default"},
        )
        wait(lambda: len(must_run.get_running_jobs()) == 1)
