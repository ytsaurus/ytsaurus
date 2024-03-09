from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    SCHEDULERS_SERVICE,
    CONTROLLER_AGENTS_SERVICE,
)

from yt_commands import (
    authors, wait, create, ls, get, set, remove,
    exists, create_user,
    create_pool, add_member, read_table, write_table, map, run_test_vanilla, run_sleeping_vanilla,
    update_op_parameters, create_test_tables, execute_command, make_ace)

from yt_scheduler_helpers import (
    scheduler_orchid_pool_path, scheduler_orchid_default_pool_tree_path,
    scheduler_orchid_operation_path)
from yt_helpers import create_custom_pool_tree_with_one_node
import yt_error_codes

from yt.test_helpers import are_almost_equal
from yt.common import YtError, YtResponseError

import io
import pytest
import random
import gzip
import zstandard as zstd
import time

##################################################################


class TestRuntimeParameters(YTEnvSetup):
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

    DELTA_CONTROLLER_AGENT_CONFIG = {"controller_agent": {"snapshot_period": 1000}}

    @authors("renadeen")
    def test_update_runtime_parameters(self):
        create_test_tables()

        op = map(
            command="sleep 100",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"weight": 5, "annotations": {"foo": "abc"}},
            track=False,
        )
        wait(lambda: op.get_state() == "running", iter=10)

        wait(lambda: op.get_runtime_progress("scheduling_info_per_pool_tree/default/weight", 0.0) == 5.0)

        annotations_path = op.get_path() + "/@runtime_parameters/annotations"
        if self.DELTA_SCHEDULER_CONFIG["scheduler"].get("enable_heavy_runtime_parameters", False):
            annotations_path = op.get_path() + "/@heavy_runtime_parameters/annotations"

        assert get(annotations_path) == {"foo": "abc"}

        update_op_parameters(
            op.id,
            parameters={
                "scheduling_options_per_pool_tree": {"default": {"weight": 3.0, "resource_limits": {"user_slots": 0}}},
                "annotations": {
                    "foo": "bar",
                },
            },
        )

        default_tree_parameters_path = op.get_path() + "/@runtime_parameters/scheduling_options_per_pool_tree/default"

        wait(lambda: are_almost_equal(get(default_tree_parameters_path + "/weight"), 3.0))
        wait(lambda: get(default_tree_parameters_path + "/resource_limits/user_slots") == 0)

        wait(lambda:
             are_almost_equal(op.get_runtime_progress("scheduling_info_per_pool_tree/default/weight", 0.0), 3.0))
        # wait() is essential since resource limits are copied from runtime parameters only during fair-share update.
        wait(lambda:
             op.get_runtime_progress("scheduling_info_per_pool_tree/default/resource_limits/user_slots", 0) == 0,
             iter=5)
        wait(lambda: get(annotations_path) == {"foo": "bar"})

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        op.ensure_running()

        wait(lambda:
             are_almost_equal(op.get_runtime_progress("scheduling_info_per_pool_tree/default/weight", 0.0), 3.0))
        # wait() is essential since resource limits are copied from runtime parameters only during fair-share update.
        wait(lambda:
             op.get_runtime_progress("scheduling_info_per_pool_tree/default/resource_limits/user_slots", None) == 0,
             iter=5)
        wait(lambda: get(annotations_path) == {"foo": "bar"})

    @authors("renadeen")
    def test_change_pool_of_default_pooltree(self):
        create_pool("initial_pool")
        create_pool("changed_pool")

        op = run_sleeping_vanilla(spec={"pool": "initial_pool"})

        wait(lambda: op.get_state() == "running", iter=10)

        update_op_parameters(op.id, parameters={"pool": "changed_pool"})

        wait(lambda: op.get_runtime_progress("scheduling_info_per_pool_tree/default/pool") == "changed_pool")

    @authors("renadeen", "ignat")
    def test_running_operation_counts_on_change_pool(self):
        create_pool("initial_pool")
        create_pool("changed_pool")

        op = run_sleeping_vanilla(spec={"pool": "initial_pool"})
        wait(lambda: op.get_state() == "running", iter=10)

        wait(lambda: get(scheduler_orchid_pool_path("initial_pool") + "/running_operation_count") == 1)
        wait(lambda: get(scheduler_orchid_pool_path("changed_pool") + "/running_operation_count") == 0)

        update_op_parameters(op.id, parameters={"pool": "changed_pool"})

        wait(lambda: get(scheduler_orchid_pool_path("initial_pool") + "/running_operation_count") == 0)
        wait(lambda: get(scheduler_orchid_pool_path("changed_pool") + "/running_operation_count") == 1)

    @authors("renadeen")
    def test_change_pool_of_multitree_operation(self):
        create_custom_pool_tree_with_one_node("custom")
        create_pool("default_pool")
        create_pool("custom_pool1", pool_tree="custom")
        create_pool("custom_pool2", pool_tree="custom")

        op = run_sleeping_vanilla(
            spec={
                "pool_trees": ["default", "custom"],
                "scheduling_options_per_pool_tree": {
                    "default": {"pool": "default_pool"},
                    "custom": {"pool": "custom_pool1"},
                },
            }
        )

        wait(lambda: op.get_state() == "running", iter=10)

        update_op_parameters(
            op.id,
            parameters={"scheduling_options_per_pool_tree": {"custom": {"pool": "custom_pool2"}}},
        )

        wait(lambda: op.get_runtime_progress("scheduling_info_per_pool_tree/custom/pool") == "custom_pool2")

    @authors("renadeen")
    def test_operation_count_validation_on_change_pool(self):
        create_pool("initial_pool")
        create_pool("full_pool", attributes={"max_running_operation_count": 0})

        op = run_sleeping_vanilla(spec={"pool": "initial_pool"})
        wait(lambda: op.get_state() == "running")

        with pytest.raises(YtError):
            update_op_parameters(op.id, parameters={"pool": "full_pool"})

        wait(lambda: op.get_runtime_progress("scheduling_info_per_pool_tree/default/pool") == "initial_pool")

    @authors("renadeen")
    def test_pool_name_validation_on_change_pool_to_ephemeral(self):
        op = run_sleeping_vanilla(spec={"pool": "ephemeral"})
        wait(lambda: op.get_state() == "running")

        with pytest.raises(YtError):
            update_op_parameters(op.id, parameters={"scheduling_options_per_pool_tree": {
                "default": {"pool": "ephemeral$subpool"}}
            })

        wait(lambda: op.get_runtime_progress("scheduling_info_per_pool_tree/default/pool") == "ephemeral")

    @authors("renadeen")
    def test_change_pool_during_prepare_phase_bug(self):
        create_pool("source")
        create_pool("target")
        op = run_test_vanilla(":", spec={"pool": "source", "testing": {"delay_inside_prepare": 3000}})
        wait(lambda: op.get_state() == "preparing", sleep_backoff=0.1)
        update_op_parameters(op.id, parameters={"pool": "target"})
        assert op.get_state() == "preparing"
        # YT-11311: core was in MaterializeOperation.
        op.track()

    @authors("renadeen")
    def test_change_pool_of_pending_operation_crash(self):
        # YT-12147:
        # 1. There are two pools: parent and child.
        # 2. Parent reached running_operation_count limit.
        # 3. Run operation in child pool. It became pending due to the limit at parent.
        # 4. Issue command to move operation to parent pool.
        # 5. Validation was obliged to check that operation can be instantly run at new pool
        # (i.e. there is no operation count violation at new pool).
        # 6. But validation skipped common prefix of pools due to expectation
        #    that operation counts won't change on common prefix.
        # 7. After performing pool change crash is caused by YT_VERIFY
        #    which enforces that operation will immediately become running.

        create_pool("parent", attributes={"max_running_operation_count": 0})
        create_pool("child", parent_name="parent")

        op = run_test_vanilla(":", spec={"pool": "child"})
        op.wait_for_state("pending")

        with pytest.raises(YtError):
            # core was in TFairShareTree::ChangeOperationPool.
            update_op_parameters(op.id, parameters={"pool": "parent"})

    @authors("renadeen")
    def test_change_pool_of_pending_operation_hang(self):
        # YT-11479:
        # 1. Operation is pending due to running operation count limit in pool.
        # 2. Change operation pool to pool with available running operation count.
        # 3. Operation is removed from violating pool and from queue of waiting operations.
        # 4. Operation attached to new pool but nobody bothers to activate operation (call OnOperationReadyInTree).
        # 5. Operation is hung forever.

        create_pool("free")
        create_pool("busy", attributes={"max_running_operation_count": 0})

        op = run_test_vanilla(":", spec={"pool": "busy"})
        op.wait_for_state("pending")

        update_op_parameters(op.id, parameters={"pool": "free"})
        op.track()

    @authors("eshcherbin")
    def test_change_pool_max_operation_count_exceeded(self):
        # YT-14547:
        # 1. Operation is running in pool.
        # 2. Change operation pool to pool with available running operation count.
        # 3. At the same time change max_running_operation_count in target pool to 0.
        # 4. Due to WaitFor`s after validation we can attach operation to pool with 0 limit.
        # 5. Later in the TFairShareTree::ChangeOperationPool we crash as operation is supposed to run after change pool
        # but cannot due to limit violation.

        create_pool("free")
        create_pool("busy", attributes={"max_running_operation_count": 1})

        op = run_sleeping_vanilla(spec={
            "pool": "free",
            "testing": {
                "delay_inside_validate_runtime_parameters": 5000
            }
        })
        op.wait_for_state("running")

        response = execute_command(
            "update_op_parameters",
            {
                "operation_id": op.id,
                "parameters": {"pool": "busy"}
            },
            return_response=True)

        time.sleep(1.5)
        set("//sys/pools/busy/@max_running_operation_count", 0)

        response.wait()
        assert not response.is_ok()
        error = YtResponseError(response.error())
        assert error.contains_text("Max running operation count of pool \"busy\" violated")

    @authors("omgronny")
    def test_change_pool_slot_index_in_cypress(self):
        create_pool("first")
        create_pool("second")

        first_ops = []
        second_ops = []
        for _ in range(2):
            first_ops.append(run_sleeping_vanilla(spec={"pool": "first"}))
        for _ in range(5):
            second_ops.append(run_sleeping_vanilla(spec={"pool": "second"}))
        second_ops[-1].wait_for_state("running")

        def get_slot_index(op):
            return get(op.get_path() + "/@slot_index_per_pool_tree/default")

        op = run_sleeping_vanilla(spec={"pool": "first"})
        op.wait_for_state("running")
        wait(lambda: get_slot_index(op) == 2)

        update_op_parameters(op.id, parameters={"pool": "second"})
        wait(lambda: get_slot_index(op) == 5)

        update_op_parameters(op.id, parameters={"pool": "first"})
        wait(lambda: get_slot_index(op) == 2)

    @authors("eshcherbin")
    def test_change_pool_slot_index_conflict(self):
        # YT-15199

        create_pool("first")
        create_pool("second")

        first_ops = []
        second_ops = []
        for _ in range(2):
            first_ops.append(run_sleeping_vanilla(spec={"pool": "first"}))
        for _ in range(5):
            second_ops.append(run_sleeping_vanilla(spec={"pool": "second"}))
        second_ops[-1].wait_for_state("running")

        def get_slot_index(op):
            return op.get_runtime_progress("scheduling_info_per_pool_tree/default/slot_index")

        op1 = run_sleeping_vanilla(spec={"pool": "first"})
        op1.wait_for_state("running")
        wait(lambda: get_slot_index(op1) == 2)

        op2 = run_sleeping_vanilla(spec={"pool": "first"})
        op2.wait_for_state("running")
        wait(lambda: get_slot_index(op2) == 3)

        update_op_parameters(op1.id, parameters={"pool": "second"})
        wait(lambda: get_slot_index(op1) == 5)

        update_op_parameters(op2.id, parameters={"pool": "second"})
        wait(lambda: get_slot_index(op2) == 6)

        update_op_parameters(op2.id, parameters={"pool": "first"})
        wait(lambda: get_slot_index(op2) in [2, 3])

        update_op_parameters(op1.id, parameters={"pool": "first"})
        wait(lambda: get_slot_index(op1) in [2, 3] and get_slot_index(op1) != get_slot_index(op2))

    @authors("renadeen")
    def test_no_pool_validation_on_change_weight(self):
        create_pool("test_pool")
        op = run_sleeping_vanilla(spec={"pool": "test_pool"})
        op.wait_for_state("running")

        set("//sys/pools/test_pool/@max_operation_count", 0)
        set("//sys/pools/test_pool/@max_running_operation_count", 0)

        orchid_pools = scheduler_orchid_default_pool_tree_path() + "/pools"
        wait(lambda: get(orchid_pools + "/test_pool/max_running_operation_count") == 0)

        # assert this doesn't fail
        update_op_parameters(op.id, parameters={"weight": 2})

    @authors("eshcherbin")
    def test_schedule_in_single_tree(self):
        create_custom_pool_tree_with_one_node("other")
        create_pool("pool1", pool_tree="other")
        create_pool("pool2", pool_tree="other")
        create_pool("pool1")
        create_pool("pool2")

        op = run_sleeping_vanilla(
            spec={
                "pool_trees": ["default", "other"],
                "scheduling_options_per_pool_tree": {
                    "default": {"pool": "pool1"},
                    "custom": {"pool": "pool1"},
                },
                "schedule_in_single_tree": True,
            }
        )

        op.wait_for_state("running")

        erased_tree = get(op.get_path() + "/@runtime_parameters/erased_trees")[0]
        chosen_tree = "default" if erased_tree == "other" else "other"
        parameters = {"scheduling_options_per_pool_tree": {chosen_tree: {"pool": "pool2"}}}
        update_op_parameters(op.id, parameters=parameters)

        wait(lambda: op.get_runtime_progress("scheduling_info_per_pool_tree/default/pool") == "pool2")

    @authors("eshcherbin")
    def test_forbidden_during_materialization(self):
        create_pool("initial_pool")
        create_pool("changed_pool")

        op = run_sleeping_vanilla(
            spec={
                "pool": "initial_pool",
                "testing": {"delay_inside_materialize": 10000},
            }
        )

        wait(lambda: op.get_state() == "materializing", iter=10)

        with pytest.raises(YtError):
            update_op_parameters(op.id, parameters={"pool": "changed_pool"})

        op.abort()

    @authors("eshcherbin")
    def test_forbidden_during_revival(self):
        create_pool("initial_pool")
        create_pool("changed_pool")

        op = run_sleeping_vanilla(
            spec={
                "pool": "initial_pool",
                "testing": {"delay_inside_register_jobs_from_revived_operation": 10000},
            }
        )

        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        wait(lambda: op.get_state() in ["reviving_jobs"], iter=10)

        with pytest.raises(YtError):
            update_op_parameters(op.id, parameters={"pool": "changed_pool"})

        op.abort()

    @authors("ignat")
    def test_tree_removal(self):
        create_custom_pool_tree_with_one_node("other")
        create_pool("my_pool", pool_tree="other")
        create_pool("my_pool")
        create_pool("other_pool")

        op = run_sleeping_vanilla(
            spec={
                "pool_trees": ["default", "other"],
                "pool": "my_pool",
            }
        )

        op.wait_for_state("running")

        scheduling_options_path = op.get_path() + "/@runtime_parameters/scheduling_options_per_pool_tree"
        wait(lambda: sorted(ls(scheduling_options_path)) == ["default", "other"])

        remove("//sys/pool_trees/other")
        wait(lambda: sorted(ls(scheduling_options_path)) == ["default"])
        wait(lambda: get(op.get_path() + "/@runtime_parameters/erased_trees") == ["other"])

        parameters = {"scheduling_options_per_pool_tree": {"default": {"pool": "other_pool"}}}
        update_op_parameters(op.id, parameters=parameters)

        wait(lambda: op.get_runtime_progress("scheduling_info_per_pool_tree/default/pool") == "other_pool")

    @authors("eshcherbin")
    def test_initial_resource_limits_per_tree(self):
        create_custom_pool_tree_with_one_node("other")
        op = run_sleeping_vanilla(
            job_count=4,
            task_patch={
                "cpu_limit": 0.5,
            },
            spec={
                "pool_trees": ["default", "other"],
                "scheduling_options_per_pool_tree": {
                    "default": {
                        "resource_limits": {
                            "cpu": 1.0,
                        },
                    },
                    "other": {
                        "resource_limits": {
                            "cpu": 0.5,
                        },
                    },
                },
            },
        )

        wait(lambda: exists(scheduler_orchid_operation_path(op.id, tree="default")))
        wait(lambda: exists(scheduler_orchid_operation_path(op.id, tree="other")))
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="default") + "/resource_limits/cpu") == 1.0)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="default") + "/resource_usage/cpu") == 1.0)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="other") + "/resource_limits/cpu") == 0.5)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="other") + "/resource_usage/cpu") == 0.5)

    @authors("renadeen")
    def test_scheduling_options_priority(self):
        create_custom_pool_tree_with_one_node("other")
        op = run_sleeping_vanilla(
            spec={
                "pool": "pool1",
                "weight": 2,
                "scheduling_options_per_pool_tree": {
                    "default": {
                        "pool": "pool2",
                        "weight": 3,
                    },
                },
            },
        )

        wait(lambda: exists(scheduler_orchid_operation_path(op.id, tree="default")))
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="default") + "/pool") == "pool2")
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="default") + "/weight") == 3.0)

    @authors("dakovalkov")
    def test_acl_change(self):
        create_user("u1")
        create_user("u2")

        op = run_sleeping_vanilla()
        op.ensure_running()

        acl_path = op.get_path() + "/@runtime_parameters/acl"
        root_ace = make_ace("allow", "root", ["read", "manage"])
        u1_ace = make_ace("allow", "u1", "read")
        u2_ace = make_ace("allow", "u1", "read")

        assert get(acl_path) == [root_ace]

        update_op_parameters(op.id, parameters={"acl": [u1_ace]})
        assert get(acl_path) == [u1_ace, root_ace]

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass
        op.ensure_running()

        assert get(acl_path) == [u1_ace, root_ace]

        update_op_parameters(op.id, parameters={"acl": [u2_ace]})
        assert get(acl_path) == [u2_ace, root_ace]


class TestRuntimeParametersWithRecentResourceUsage(TestRuntimeParameters):
    def setup_method(self, method):
        super(TestRuntimeParametersWithRecentResourceUsage, self).setup_method(method)
        set("//sys/pool_trees/default/@config/use_recent_resource_usage_for_local_satisfaction", True)


class TestRuntimeParametersWithHeavyRuntimeParameters(TestRuntimeParameters):
    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "operations_update_period": 10,
            "pool_change_is_allowed": True,
            "watchers_update_period": 100,  # Update pools configuration period
            "enable_heavy_runtime_parameters": True,
        }
    }

##################################################################


class TestSchedulerResourceUsageStrategySwitches(YTEnvSetup):
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

    @authors("ignat")
    def test_switches(self):
        OP_COUNT = 20

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"foo": "bar"} for _ in range(3)])

        for index in range(OP_COUNT):
            create("table", "//tmp/t_out" + str(index))
            create_pool("pool_" + str(index))

        ops = []
        for index in range(OP_COUNT):
            op = map(
                track=False,
                command="sleep 1; echo 'AAA' >&2; cat",
                in_="//tmp/t_in",
                out="//tmp/t_out" + str(index),
                spec={"job_count": 3, "pool": "pool_" + str(index)},
            )
            ops.append(op)

        for index in range(40):
            set("//sys/pool_trees/default/@config/use_recent_resource_usage_for_local_satisfaction", index % 2 == 0)
            if random.randint(0, 1):
                set("//sys/pools/pool_{}/@resource_limits".format(random.randint(0, OP_COUNT - 1)), {})
            else:
                set("//sys/pools/pool_{}/@resource_limits".format(random.randint(0, OP_COUNT - 1)), {"user_slots": 1})
            time.sleep(0.5)

        for index, op in enumerate(ops):
            op.track()
            assert read_table("//tmp/t_out" + str(index)) == [{"foo": "bar"}] * 3


class TestJobsAreScheduledAfterPoolChange(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "operations_update_period": 10,
            "pool_change_is_allowed": True,
        }
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 10,
                "cpu": 10,
                "memory": 10 * 1024 ** 3,
            }
        }
    }

    @authors("renadeen", "antonkikh")
    def test_jobs_are_scheduled_after_pool_change(self):
        create_pool("initial_pool")
        create_pool("changed_pool")
        op = run_test_vanilla(":", job_count=100000, spec={"pool": "initial_pool"})
        wait(lambda: op.get_job_count("running") > 5, iter=10)

        update_op_parameters(op.id, parameters={"pool": "changed_pool"})
        time.sleep(0.1)

        scheduled = op.get_job_count("running") + op.get_job_count("completed")
        wait(lambda: op.get_job_count("running") + op.get_job_count("completed") > scheduled + 10)


class TestOperationDetailedLogs(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 2,
                "cpu": 2,
                "memory": 2 * 1024 ** 3,
            }
        }
    }

    def get_scheduled_allocation_log_entries(self):
        scheduler_debug_logs_filename = self.Env.configs["scheduler"][0]["logging"]["writers"]["debug"]["file_name"]

        if scheduler_debug_logs_filename.endswith(".zst"):
            compressed_file = open(scheduler_debug_logs_filename, "rb")
            decompressor = zstd.ZstdDecompressor()
            binary_reader = decompressor.stream_reader(compressed_file, read_size=8192)
            logfile = io.TextIOWrapper(binary_reader, encoding="utf-8", errors="ignore")
        elif scheduler_debug_logs_filename.endswith(".gz"):
            logfile = gzip.open(scheduler_debug_logs_filename, "b")
        else:
            logfile = open(scheduler_debug_logs_filename, "b")

        return [line for line in logfile if "Scheduled an allocation" in line]

    @authors("antonkikh")
    def test_enable_detailed_logs(self):
        create_pool("fake_pool")
        set("//sys/pool_trees/default/fake_pool/@resource_limits", {"user_slots": 1})

        op = run_sleeping_vanilla(job_count=6, spec={"pool": "fake_pool"})
        wait(lambda: len(op.get_running_jobs()) == 1)

        # Check that there are no detailed logs by default.

        assert len(self.get_scheduled_allocation_log_entries()) == 0

        # Enable detailed logging and check that expected the expected log entries are produced.

        update_op_parameters(
            op.id,
            parameters={
                "scheduling_options_per_pool_tree": {
                    "default": {
                        "enable_detailed_logs": True,
                    }
                }
            },
        )

        set("//sys/pool_trees/default/fake_pool/@resource_limits/user_slots", 3)
        wait(lambda: len(op.get_running_jobs()) == 3)

        wait(lambda: len(self.get_scheduled_allocation_log_entries()) == 2)
        log_entries = self.get_scheduled_allocation_log_entries()
        for log_entry in log_entries:
            assert "OperationId: {}".format(op.id) in log_entry
            assert "TreeId: default" in log_entry

        # Disable detailed logging and check that no new log entries are produced.
        update_op_parameters(
            op.id,
            parameters={
                "scheduling_options_per_pool_tree": {
                    "default": {
                        "enable_detailed_logs": False,
                    }
                }
            },
        )

        assert len(op.get_running_jobs()) == 3
        set("//sys/pool_trees/default/fake_pool/@resource_limits/user_slots", 4)
        wait(lambda: len(op.get_running_jobs()) == 4)

        log_entries = self.get_scheduled_allocation_log_entries()
        assert len(log_entries) == 2

        op.abort()

    @authors("antonkikh")
    def test_enable_detailed_logs_requires_administer_permission(self):
        create_user("u1")
        op = run_sleeping_vanilla(job_count=5, authenticated_user="u1")
        op.wait_for_state("running")

        def update_enable_detailed_logs():
            update_op_parameters(
                op.id,
                parameters={
                    "scheduling_options_per_pool_tree": {
                        "default": {
                            "enable_detailed_logs": False,
                        }
                    },
                },
                authenticated_user="u1",
            )

        with pytest.raises(YtError) as excinfo:
            update_enable_detailed_logs()
        if not excinfo.value.contains_code(yt_error_codes.AuthorizationErrorCode):
            raise excinfo.value

        add_member("u1", "superusers")
        update_enable_detailed_logs()
