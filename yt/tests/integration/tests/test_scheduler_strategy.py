import pytest

from yt_commands import *
from yt_helpers import *

from yt_env_setup import YTEnvSetup, wait, Restarter, SCHEDULERS_SERVICE, require_ytserver_root_privileges, get_porto_delta_node_config
from yt.test_helpers import are_almost_equal
from yt.common import date_string_to_timestamp
import yt.common

from flaky import flaky

import os
import time
import datetime

import __builtin__

##################################################################

def get_scheduling_options(user_slots):
    return {
        "scheduling_options_per_pool_tree": {
            "default": {
                "resource_limits": {
                    "user_slots": user_slots
                }
            }
        }
    }

def get_from_tree_orchid(tree, path, **kwargs):
    return get("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/{}/{}".format(tree, path), **kwargs)

##################################################################

class BaseTestResourceUsage(YTEnvSetup, PrepareTables):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    BASE_DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "fair_share_profiling_period": 100,
            "alerts_update_period": 100,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operation_time_limit_check_period": 100,
        }
    }

    DELTA_NODE_CONFIG = {
        "resource_limits": {
            "user_jobs": {
                "type": "static",
                "value": 10**9,
            },
        }
    }

    def setup_method(self, method):
        super(BaseTestResourceUsage, self).setup_method(method)
        set("//sys/pool_trees/default/@preemptive_scheduling_backoff", 0)
        set("//sys/pool_trees/default/@max_unpreemptable_running_job_count", 0)
        time.sleep(0.5)

    def _check_running_jobs(self, op, desired_running_jobs):
        success_iter = 0
        min_success_iteration = 10
        for i in xrange(100):
            running_jobs = op.get_running_jobs()
            if running_jobs:
                assert len(running_jobs) <= desired_running_jobs
                success_iter += 1
                if success_iter == min_success_iteration:
                    return
            time.sleep(0.1)
        assert False

    @authors("ignat")
    def test_root_pool(self):
        wait(lambda: are_almost_equal(get(scheduler_orchid_default_pool_tree_path() + "/pools/<Root>/fair_share_ratio"), 0.0))

    @authors("ignat")
    def test_scheduler_guaranteed_resources_ratio(self):
        total_resource_limits = get("//sys/scheduler/orchid/scheduler/cell/resource_limits")

        create_pool("big_pool", attributes={"min_share_resources": {"cpu": total_resource_limits["cpu"]}})
        create_pool("subpool_1", parent_name="big_pool", attributes={"weight": 1.0})
        create_pool("subpool_2", parent_name="big_pool", attributes={"weight": 3.0})
        create_pool("small_pool", attributes={"weight": 100.0})
        create_pool("subpool_3", parent_name="small_pool")
        create_pool("subpool_4", parent_name="small_pool")


        # Wait for fair share update.
        time.sleep(1)

        pools_orchid = scheduler_orchid_default_pool_tree_path() + "/pools"

        get_pool_guaranteed_resources = lambda pool: \
            get("{0}/{1}/guaranteed_resources".format(pools_orchid, pool))

        get_pool_guaranteed_resources_ratio = lambda pool: \
            get("{0}/{1}/guaranteed_resources_ratio".format(pools_orchid, pool))

        assert are_almost_equal(get_pool_guaranteed_resources_ratio("big_pool"), 1.0)
        assert get_pool_guaranteed_resources("big_pool") == total_resource_limits

        assert are_almost_equal(get_pool_guaranteed_resources_ratio("small_pool"), 0)
        assert are_almost_equal(get_pool_guaranteed_resources_ratio("subpool_3"), 0)
        assert are_almost_equal(get_pool_guaranteed_resources_ratio("subpool_4"), 0)

        assert are_almost_equal(get_pool_guaranteed_resources_ratio("subpool_1"), 1.0 / 4.0)
        assert are_almost_equal(get_pool_guaranteed_resources_ratio("subpool_2"), 3.0 / 4.0)

        self._prepare_tables()

        get_operation_guaranteed_resources_ratio = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/guaranteed_resources_ratio".format(op_id))

        op = map(
            track=False,
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            spec={"pool": "big_pool"})
        wait_breakpoint()

        # Wait for fair share update.
        time.sleep(1)

        assert are_almost_equal(get_operation_guaranteed_resources_ratio(op.id), 1.0 / 5.0)
        assert are_almost_equal(get_pool_guaranteed_resources_ratio("subpool_1"), 1.0 / 5.0)
        assert are_almost_equal(get_pool_guaranteed_resources_ratio("subpool_2"), 3.0 / 5.0)

        release_breakpoint()
        op.track()

    @authors("ignat")
    def test_resource_limits(self):
        resource_limits = {"cpu": 1, "memory": 1000 * 1024 * 1024, "network": 10}
        create_pool("test_pool", attributes={"resource_limits": resource_limits})

        wait(lambda: "test_pool" in get(scheduler_orchid_default_pool_tree_path() + "/pools"))

        # TODO(renadeen): make better, I know you can
        def check_limits():
            stats = get(scheduler_orchid_default_pool_tree_path())
            pool_resource_limits = stats["pools"]["test_pool"]["resource_limits"]
            for resource, limit in resource_limits.iteritems():
                resource_name = "user_memory" if resource == "memory" else resource
                if not are_almost_equal(pool_resource_limits[resource_name], limit):
                    return False
            return True

        wait(check_limits)

        self._prepare_tables()
        data = [{"foo": i} for i in xrange(3)]
        write_table("//tmp/t_in", data)

        memory_limit = 30 * 1024 * 1024

        testing_options = {"scheduling_delay": 500, "scheduling_delay_type": "async"}

        op = map(
            track=False,
            command="sleep 100",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"job_count": 3, "pool": "test_pool", "mapper": {"memory_limit": memory_limit}, "testing": testing_options})
        self._check_running_jobs(op, 1)
        op.abort()

        op = map(
            track=False,
            command="sleep 5",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"job_count": 3, "resource_limits": resource_limits, "mapper": {"memory_limit": memory_limit}, "testing": testing_options})
        self._check_running_jobs(op, 1)
        op_limits = get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/resource_limits".format(op.id))
        for resource, limit in resource_limits.iteritems():
            resource_name = "user_memory" if resource == "memory" else resource
            assert are_almost_equal(op_limits[resource_name], limit)

    @authors("ignat")
    def test_resource_limits_preemption(self):
        create_pool("test_pool2")
        wait(lambda: "test_pool2" in get(scheduler_orchid_default_pool_tree_path() + "/pools"))

        self._prepare_tables()
        data = [{"foo": i} for i in xrange(3)]
        write_table("//tmp/t_in", data)

        op = map(
            track=False,
            command="sleep 100",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"job_count": 3, "pool": "test_pool2"})
        wait(lambda: len(op.get_running_jobs()) == 3)

        resource_limits = {"cpu": 2}
        set("//sys/pools/test_pool2/@resource_limits", resource_limits)

        wait(lambda: are_almost_equal(get(scheduler_orchid_default_pool_tree_path() + "/pools/test_pool2/resource_limits/cpu"), 2))

        wait(lambda: len(op.get_running_jobs()) == 2)

    # Remove flaky after YT-8784.
    @authors("ignat")
    @flaky(max_runs=5)
    def test_resource_limits_runtime(self):
        self._prepare_tables()
        data = [{"foo": i} for i in xrange(3)]
        write_table("//tmp/t_in", data)

        op = map(
            track=False,
            command="sleep 100",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"job_count": 3, "resource_limits": {"user_slots": 1}})
        self._check_running_jobs(op, 1)

        set(op.get_path() + "/@resource_limits", {"user_slots": 2})
        self._check_running_jobs(op, 2)

    @authors("ignat")
    def test_max_possible_resource_usage(self):
        create_pool("low_cpu_pool", attributes={"resource_limits": {"cpu": 1}})
        create_pool("subpool_1", parent_name="low_cpu_pool")
        create_pool("subpool_2", parent_name="low_cpu_pool", attributes={"resource_limits": {"cpu": 0}})
        create_pool("high_cpu_pool")

        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out_1")
        self._create_table("//tmp/t_out_2")
        self._create_table("//tmp/t_out_3")
        data = [{"foo": i} for i in xrange(3)]
        write_table("//tmp/t_in", data)

        get_pool_fair_share_ratio = lambda pool: \
            get("{0}/pools/{1}/fair_share_ratio".format(scheduler_orchid_default_pool_tree_path(), pool))
        get_pool_max_possible_resource_usage = lambda pool: \
            get("{0}/pools/{1}/max_possible_usage_ratio".format(scheduler_orchid_default_pool_tree_path(), pool))

        command_with_breakpoint = with_breakpoint("cat ; BREAKPOINT")
        op1 = map(
            track=False,
            command=command_with_breakpoint,
            in_="//tmp/t_in",
            out="//tmp/t_out_1",
            spec={"job_count": 1, "pool": "subpool_1"})

        op2 = map(
            track=False,
            command=command_with_breakpoint,
            in_="//tmp/t_in",
            out="//tmp/t_out_2",
            spec={"job_count": 2, "pool": "high_cpu_pool"})

        wait_breakpoint()

        wait(lambda: are_almost_equal(get_pool_fair_share_ratio("subpool_1"), 1.0 / 3.0))
        wait(lambda: are_almost_equal(get_pool_fair_share_ratio("subpool_2"), 0))
        wait(lambda: are_almost_equal(get_pool_fair_share_ratio("low_cpu_pool"), 1.0 / 3.0))
        wait(lambda: are_almost_equal(get_pool_fair_share_ratio("high_cpu_pool"), 2.0 / 3.0))

        op3 = map(
            track=False,
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out_3",
            spec={"job_count": 1, "pool": "subpool_2", "mapper": {"cpu_limit": 0}})

        time.sleep(1)

        if self.DELTA_SCHEDULER_CONFIG["scheduler"].get("use_classic_scheduler", True):
            wait(lambda: are_almost_equal(get_pool_fair_share_ratio("low_cpu_pool"), 1.0 / 2.0))
            wait(lambda: are_almost_equal(get_pool_fair_share_ratio("high_cpu_pool"), 1.0 / 2.0))
        else:
            wait(lambda: are_almost_equal(get_pool_fair_share_ratio("subpool_1"), 1.0 / 3.0))
            wait(lambda: are_almost_equal(get_pool_fair_share_ratio("subpool_2"), 0))
            wait(lambda: are_almost_equal(get_pool_fair_share_ratio("low_cpu_pool"), 1.0 / 3.0))
            wait(lambda: are_almost_equal(get_pool_fair_share_ratio("high_cpu_pool"), 2.0 / 3.0))

        release_breakpoint()
        op1.track()
        op2.track()
        op3.track()

    @authors("renadeen", "ignat")
    def test_fractional_cpu_usage(self):
        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        data = [{"foo": i} for i in xrange(3)]
        write_table("//tmp/t_in", data)

        op = map(
            track=False,
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"job_count": 3, "mapper": {"cpu_limit": 0.87}})
        wait_breakpoint()

        get_resource_usage = lambda op: get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/resource_usage".format(op.id))
        wait(lambda: are_almost_equal(get_resource_usage(op)["cpu"], 3 * 0.87))

        release_breakpoint()
        op.track()


class TestResourceUsageClassic(BaseTestResourceUsage):
    DELTA_SCHEDULER_CONFIG = BaseTestResourceUsage.BASE_DELTA_SCHEDULER_CONFIG

class TestResourceUsageVector(BaseTestResourceUsage):
    DELTA_SCHEDULER_CONFIG = yt.common.update(
        BaseTestResourceUsage.BASE_DELTA_SCHEDULER_CONFIG,
        {"use_classic_scheduler": False}
    )

##################################################################


class TestStrategyWithSlowController(YTEnvSetup, PrepareTables):
    NUM_MASTERS = 1
    NUM_NODES = 10
    NUM_SCHEDULERS = 1

    CONCURRENT_HEARTBEAT_LIMIT = 2

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "node_shard_count": 1,
            "soft_concurrent_heartbeat_limit": CONCURRENT_HEARTBEAT_LIMIT,
            "hard_concurrent_heartbeat_limit": CONCURRENT_HEARTBEAT_LIMIT
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "scheduler_connector": {
                "heartbeat_period": 100
            }
        }
    }

    @authors("renadeen", "ignat")
    def test_strategy_with_slow_controller(self):
        slow_spec = {
            "testing": {
                "scheduling_delay": 1000,
                "scheduling_delay_type": "async"
            }
        }

        # Occupy the cluster
        op0 = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=10)
        wait_breakpoint(job_count=10)

        # Run operations
        op1 = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=20)
        op2 = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=20, spec=slow_spec)

        wait(lambda: op1.get_state() == "running")
        wait(lambda: op2.get_state() == "running")

        enable_op_detailed_logs(op1)
        enable_op_detailed_logs(op2)

        assert op1.get_job_count("running") == 0
        assert op2.get_job_count("running") == 0

        # Free up the cluster
        for j in op0.get_running_jobs():
            release_breakpoint(job_id=j)

        # Check the resulting allocation
        wait(lambda: op1.get_job_count("running") + op2.get_job_count("running") == 10)
        assert abs(op1.get_job_count("running") - op2.get_job_count("running")) <= self.CONCURRENT_HEARTBEAT_LIMIT


class TestStrategies(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    def _prepare_tables(self):
        create("table", "//tmp/t_in")
        set("//tmp/t_in/@replication_factor", 1)
        write_table("//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")
        set("//tmp/t_out/@replication_factor", 1)

    def _get_table_chunk_node(self, table):
        chunk_id = get_first_chunk_id(table)
        replicas = get("#{0}/@stored_replicas".format(chunk_id))
        assert len(replicas) == 1

        return replicas[0]

    @authors("ignat")
    def test_strategies(self):
        self._prepare_tables()

        node = self._get_table_chunk_node("//tmp/t_in")
        set_banned_flag(True, [node])

        print_debug("Fail strategy")
        with pytest.raises(YtError):
            map(in_="//tmp/t_in", out="//tmp/t_out", command="cat", spec={"unavailable_chunk_strategy": "fail"})

        print_debug("Skip strategy")
        map(in_="//tmp/t_in", out="//tmp/t_out", command="cat", spec={"unavailable_chunk_strategy": "skip"})
        assert read_table("//tmp/t_out") == []

        print_debug("Wait strategy")
        op = map(track=False, in_="//tmp/t_in", out="//tmp/t_out", command="cat",  spec={"unavailable_chunk_strategy": "wait"})

        set_banned_flag(False, [node])
        op.track()

        assert read_table("//tmp/t_out") == [{"foo": "bar"}]

    @authors("ignat")
    def test_strategies_in_sort(self):
        v1 = {"key": "aaa"}
        v2 = {"key": "bb"}
        v3 = {"key": "bbxx"}
        v4 = {"key": "zfoo"}
        v5 = {"key": "zzz"}

        create("table", "//tmp/t_in")
        set("//tmp/t_in/@replication_factor", 1)
        write_table("//tmp/t_in", [v3, v5, v1, v2, v4])  # some random order

        create("table", "//tmp/t_out")
        set("//tmp/t_out/@replication_factor", 1)

        set_banned_flag(True)

        print_debug("Fail strategy")
        with pytest.raises(YtError):
            sort(in_="//tmp/t_in", out="//tmp/t_out", sort_by="key", spec={"unavailable_chunk_strategy": "fail"})

        print_debug("Skip strategy")
        sort(in_="//tmp/t_in", out="//tmp/t_out", sort_by="key", spec={"unavailable_chunk_strategy": "skip"})
        assert read_table("//tmp/t_out") == []

        print_debug("Wait strategy")
        op = sort(track=False, in_="//tmp/t_in", out="//tmp/t_out", sort_by="key", spec={"unavailable_chunk_strategy": "wait"})

        # Give a chance to scraper to work
        time.sleep(1.0)
        set_banned_flag(False)
        op.track()

        assert read_table("//tmp/t_out") == [v1, v2, v3, v4, v5]
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["key"]

    @authors("ignat")
    def test_strategies_in_merge(self):
        create("table", "//tmp/t1")
        set("//tmp/t1/@replication_factor", 1)
        write_table("<append=true>//tmp/t1", [{"a": 0}, {"a": 2}], sorted_by="a")
        write_table("<append=true>//tmp/t1", [{"a": 4}, {"a": 6}], sorted_by="a")

        create("table", "//tmp/t2")
        set("//tmp/t2/@replication_factor", 1)
        write_table("<append=true>//tmp/t2", [{"a": 1}, {"a": 3}], sorted_by="a")
        write_table("<append=true>//tmp/t2", [{"a": 5}, {"a": 7}], sorted_by="a")

        create("table", "//tmp/t_out")
        set("//tmp/t_out/@replication_factor", 1)

        set_banned_flag(True)

        print_debug("Fail strategy")
        with pytest.raises(YtError):
            merge(mode="sorted", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t_out", spec={"unavailable_chunk_strategy": "fail"})

        print_debug("Skip strategy")
        merge(mode="sorted", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t_out", spec={"unavailable_chunk_strategy": "skip"})
        assert read_table("//tmp/t_out") == []

        print_debug("Wait strategy")
        op = merge(track=False, mode="sorted", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t_out", spec={"unavailable_chunk_strategy": "wait"})

        # Give a chance for scraper to work
        time.sleep(1.0)
        set_banned_flag(False)
        op.track()

        assert read_table("//tmp/t_out") == [{"a": i} for i in range(8)]
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["a"]

##################################################################

class TestSchedulerOperationLimits(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "static_orchid_cache_update_period": 100
        }
    }

    def setup_method(self, method):
        super(TestSchedulerOperationLimits, self).setup_method(method)
        set("//sys/pool_trees/default/@max_running_operation_count_per_pool", 1)
        set("//sys/pool_trees/default/@default_parent_pool", "default_pool")

    def _run_operations(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        create("table", "//tmp/out3")
        write_table("//tmp/in", [{"foo": "bar"}])

        command = with_breakpoint("cat > /dev/null && BREAKPOINT")
        op1 = map(
            track=False,
            command=command,
            in_=["//tmp/in"],
            out="//tmp/out1",
            spec={"pool": "test_pool_1"})

        op2 = map(
            track=False,
            command=command,
            in_=["//tmp/in"],
            out="//tmp/out2",
            spec={"pool": "test_pool_1"})

        op3 = map(
            track=False,
            command=command,
            in_=["//tmp/in"],
            out="//tmp/out3",
            spec={"pool": "test_pool_2"})

        wait_breakpoint(job_count=2)

        # We sleep some time to make sure that op2 will not start.
        time.sleep(1)

        assert op1.get_state() == "running"
        assert op2.get_state() == "pending"
        assert op3.get_state() == "running"

        release_breakpoint()

        op1.track()
        op2.track()
        op3.track()

        assert read_table("//tmp/out1") == []
        assert read_table("//tmp/out2") == []
        assert read_table("//tmp/out3") == []

    @authors("ignat")
    def test_operations_pool_limit(self):
        create_pool("test_pool_1")
        create_pool("test_pool_2")
        self._run_operations()

    @authors("ignat")
    def test_operations_recursive_pool_limit(self):
        create_pool("research")
        set("//sys/pools/research/@max_running_operation_count", 2)
        create_pool("test_pool_1", parent_name="research")
        create_pool("test_pool_2", parent_name="research")
        self._run_operations()

    @authors("asaitgalin")
    def test_operation_count(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        attrs = {"max_running_operation_count": 3}
        create_pool("research", attributes=attrs)
        create_pool("subpool", parent_name="research", attributes=attrs)
        create_pool("other_subpool", parent_name="subpool", attributes=attrs)

        # give time to scheduler for pool reloading
        time.sleep(0.2)

        ops = []
        for i in xrange(3):
            create("table", "//tmp/out_" + str(i))
            op = map(command="sleep 1000; cat >/dev/null",
                in_=["//tmp/in"],
                out="//tmp/out_" + str(i),
                spec={"pool": "other_subpool"},
                track=False)
            ops.append(op)

        wait(lambda: get(scheduler_orchid_default_pool_tree_path() + "/pools/research/operation_count") == 3)
        wait(lambda: get(scheduler_orchid_default_pool_tree_path() + "/pools/research/running_operation_count") == 3)

    @authors("ignat")
    def test_pending_operations_after_revive(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        data = [{"foo": i} for i in xrange(5)]
        write_table("//tmp/in", data)

        op1 = map(track=False, command="sleep 5.0; cat", in_=["//tmp/in"], out="//tmp/out1")
        op2 = map(track=False, command="cat", in_=["//tmp/in"], out="//tmp/out2")

        time.sleep(1.5)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        op1.track()
        op2.track()

        assert sorted(read_table("//tmp/out1")) == sorted(data)
        assert sorted(read_table("//tmp/out2")) == sorted(data)

    @authors("ermolovd", "ignat")
    def test_abort_of_pending_operation(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        create("table", "//tmp/out3")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        command = with_breakpoint("cat > /dev/null ; BREAKPOINT")
        op1 = map(track=False, command=command, in_=["//tmp/in"], out="//tmp/out1")
        op2 = map(track=False, command=command, in_=["//tmp/in"], out="//tmp/out2")
        op3 = map(track=False, command=command, in_=["//tmp/in"], out="//tmp/out3")

        wait_breakpoint()

        # Sleep some time to make sure that op2 and op3 will not start.
        time.sleep(1)
        assert op1.get_state() == "running"
        assert op2.get_state() == "pending"
        assert op3.get_state() == "pending"

        op2.abort()
        release_breakpoint()
        op1.track()
        op3.track()

        assert op1.get_state() == "completed"
        assert op2.get_state() == "aborted"
        assert op3.get_state() == "completed"

    @authors("ignat")
    def test_reconfigured_pools_operations_limit(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        create_pool("test_pool_1")
        create_pool("test_pool_2")
        # TODO(eshcherbin): Add wait_for_orchid to create_pool.
        wait(lambda: exists(scheduler_orchid_default_pool_tree_path() + "/pools/test_pool_1"))

        op1 = map(
            track=False,
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_=["//tmp/in"],
            out="//tmp/out1",
            spec={"pool": "test_pool_1"})
        wait_breakpoint()

        # TODO(ignat): Stabilize this part.
        remove("//sys/pools/test_pool_1")
        create_pool("test_pool_1", parent_name="test_pool_2")
        time.sleep(0.5)

        op2 = map(
            track=False,
            command="cat",
            in_=["//tmp/in"],
            out="//tmp/out2",
            spec={"pool": "test_pool_2"})

        assert op1.get_state() == "running"
        wait(lambda: op2.get_state() == "pending")

        release_breakpoint()
        op1.track()
        op2.track()

    @authors("ignat")
    def test_total_operations_limit(self):
        create_pool("research")
        create_pool("research_subpool", parent_name="research")
        create_pool("production")
        set("//sys/pools/research/@max_operation_count", 3)

        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        for i in xrange(5):
            create("table", "//tmp/out" + str(i))

        ops = []
        def run(index, pool, should_raise):
            def execute(track):
                return map(
                    track=track,
                    command="sleep 1000; cat",
                    in_=["//tmp/in"],
                    out="//tmp/out" + str(index),
                    spec={"pool": pool})

            if should_raise:
                with pytest.raises(YtError):
                    execute(track=True)
            else:
                op = execute(track=False)
                wait(lambda: op.get_state() in ("pending", "running"))
                ops.append(op)

        for i in xrange(3):
            run(i, "research", False)

        for i in xrange(3, 5):
            run(i, "research", True)

        for i in xrange(3, 5):
            run(i, "research_subpool", True)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        for i in xrange(3, 5):
            run(i, "research", True)

        for i in xrange(3, 5):
            run(i, "production", False)

        pools_path = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/fair_share_info/pools"
        wait(lambda: get(pools_path + "/production/running_operation_count") == 1)
        wait(lambda: get(pools_path + "/production/operation_count") == 2)
        wait(lambda: get(pools_path + "/research/running_operation_count") == 1)
        wait(lambda: get(pools_path + "/research/operation_count") == 3)
        wait(lambda: get(pools_path + "/<Root>/running_operation_count") == 2)
        wait(lambda: get(pools_path + "/<Root>/operation_count") == 5)

        for op in ops:
            op.abort()


    @authors("ignat")
    def test_pool_changes(self):
        create_pool("research")
        create_pool("subpool", parent_name="research")
        create_pool("production")

        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        for i in xrange(5):
            create("table", "//tmp/out" + str(i))

        ops = []
        def run(index, pool):
            ops.append(map(
                track=False,
                command="sleep 1000; cat",
                in_=["//tmp/in"],
                out="//tmp/out" + str(index),
                spec={"pool": pool}))

        for i in xrange(1, 4):
            run(i, "subpool")

        time.sleep(0.5)

        pools_path = scheduler_orchid_default_pool_tree_path() + "/pools"
        wait(lambda: get(pools_path + "/subpool/running_operation_count") == 1)
        wait(lambda: get(pools_path + "/subpool/operation_count") == 3)

        wait(lambda: get(pools_path + "/research/running_operation_count") == 1)
        wait(lambda: get(pools_path + "/research/operation_count") == 3)

        assert get(pools_path + "/production/running_operation_count") == 0
        assert get(pools_path + "/production/operation_count") == 0

        move("//sys/pools/research/subpool", "//sys/pools/production/subpool")

        time.sleep(0.5)

        assert get(pools_path + "/subpool/running_operation_count") == 1
        assert get(pools_path + "/subpool/operation_count") == 3

        wait(lambda: get(pools_path + "/research/running_operation_count") == 0)
        wait(lambda: get(pools_path + "/research/operation_count") == 0)

        wait(lambda: get(pools_path + "/production/running_operation_count") == 1)
        wait(lambda: get(pools_path + "/production/operation_count") == 3)

    def _test_pool_acl_prologue(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        create_user("u")

    def _test_pool_acl_core(self, pool, acl_path):
        def _run_op():
            map(command="cat",
                in_="//tmp/t_in",
                out="//tmp/t_out",
                authenticated_user="u",
                spec={"pool": pool})
        _run_op()
        set("//sys/pools{0}/@acl/0/action".format(acl_path), "deny")
        check_permission("u", "use", "//sys/pools" + acl_path)
        with pytest.raises(YtError):
            _run_op()

    @authors("ignat")
    def test_global_pool_acl(self):
        self._test_pool_acl_prologue()
        create_pool("p", attributes={
            "inherit_acl": False,
            "acl": [make_ace("allow", "u", "use")]
        })
        self._test_pool_acl_core("p", "/p")

    @authors("ignat")
    def test_inner_pool_acl(self):
        self._test_pool_acl_prologue()
        create_pool("p1", attributes={
            "inherit_acl": False,
            "acl": [make_ace("allow", "u", "use")]
        })
        create_pool("p2", parent_name="p1")
        self._test_pool_acl_core("p2", "/p1")

    @authors("ignat")
    def test_forbid_immediate_operations(self):
        self._test_pool_acl_prologue()

        create_pool("p1", attributes={"forbid_immediate_operations": True})
        create_pool("p2", parent_name="p1")
        create_pool("default_pool", attributes={"forbid_immediate_operations": True})

        time.sleep(0.5)

        with pytest.raises(YtError):
            map(command="cat",
                in_="//tmp/t_in",
                out="//tmp/t_out",
                user="u",
                spec={"pool": "p1"})

        map(command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            user="u",
            spec={"pool": "p2"})

        map(command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            user="u",
            spec={"pool": "p3"})

##################################################################

class BaseTestSchedulerPreemption(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    BASE_DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "graceful_preemption_job_interrupt_timeout": 2000,
            "event_log": {
                "flush_period": 300,
                "retry_backoff_time": 300
            }
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "event_log": {
                "flush_period": 300,
                "retry_backoff_time": 300
            }
        }
    }

    def setup_method(self, method):
        super(BaseTestSchedulerPreemption, self).setup_method(method)
        set("//sys/pool_trees/default/@preemption_satisfaction_threshold", 0.99)
        set("//sys/pool_trees/default/@fair_share_starvation_tolerance", 0.7)
        set("//sys/pool_trees/default/@fair_share_starvation_tolerance_limit", 0.9)
        set("//sys/pool_trees/default/@min_share_preemption_timeout", 100)
        set("//sys/pool_trees/default/@max_unpreemptable_running_job_count", 0)
        set("//sys/pool_trees/default/@preemptive_scheduling_backoff", 0)
        time.sleep(0.5)

    @authors("ignat")
    def test_preemption(self):
        set("//sys/pool_trees/default/@max_ephemeral_pools_per_user", 2)
        create("table", "//tmp/t_in")
        for i in xrange(3):
            write_table("<append=true>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        op1 = map(track=False, command="sleep 1000; cat", in_=["//tmp/t_in"], out="//tmp/t_out1",
                  spec={"pool": "fake_pool", "job_count": 3, "locality_timeout": 0})
        enable_op_detailed_logs(op1)
        time.sleep(3)

        pools_path = scheduler_orchid_default_pool_tree_path() + "/pools"
        assert get(pools_path + "/fake_pool/fair_share_ratio") >= 0.999
        assert get(pools_path + "/fake_pool/usage_ratio") >= 0.999

        total_cpu_limit = get("//sys/scheduler/orchid/scheduler/cell/resource_limits/cpu")
        create_pool("test_pool", attributes={"min_share_resources": {"cpu": total_cpu_limit}})
        op2 = map(track=False, command="cat", in_=["//tmp/t_in"], out="//tmp/t_out2", spec={"pool": "test_pool"})
        enable_op_detailed_logs(op2)
        op2.track()

        op1.abort()

    @authors("ignat")
    @pytest.mark.parametrize("interruptible", [False, True])
    @pytest.mark.skipif(True, reason="YT-11083")
    def test_interrupt_job_on_preemption(self, interruptible):
        set("//sys/pool_trees/default/@max_ephemeral_pools_per_user", 2)
        create("table", "//tmp/t_in")
        write_table(
            "//tmp/t_in",
            [{"key": "%08d" % i, "value": "(foo)", "data": "a" * (2 * 1024 * 1024)} for i in range(6)],
            table_writer={
                "block_size": 1024,
                "desired_chunk_size": 1024})

        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        spec = {
            "pool": "fake_pool",
            "locality_timeout": 0,
            "enable_job_splitting": False,
            "max_failed_job_count": 1,
        }
        if interruptible:
            data_size_per_job = get("//tmp/t_in/@uncompressed_data_size")
            spec["data_size_per_job"] = data_size_per_job / 3 + 1
        else:
            spec["job_count"] = 3

        mapper = " ; ".join([
            events_on_fs().breakpoint_cmd(),
            "sleep 7",
            "cat"])
        op1 = map(
            track=False,
            command=mapper,
            in_=["//tmp/t_in"],
            out="//tmp/t_out1",
            spec=spec)

        time.sleep(3)

        pools_path = scheduler_orchid_default_pool_tree_path() + "/pools"
        assert get(pools_path + "/fake_pool/fair_share_ratio") >= 0.999
        assert get(pools_path + "/fake_pool/usage_ratio") >= 0.999

        total_cpu_limit = get("//sys/scheduler/orchid/scheduler/cell/resource_limits/cpu")
        create_pool("test_pool", attributes={"min_share_resources": {"cpu": total_cpu_limit}})

        # Ensure that all three jobs have started.
        events_on_fs().wait_breakpoint(timeout=datetime.timedelta(1000), job_count=3)
        events_on_fs().release_breakpoint()

        op2 = map(
            track=False,
            command="cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out2",
            spec={"pool": "test_pool",
                  "max_failed_job_count": 1})
        op2.track()
        op1.track()
        assert get(op1.get_path() + "/@progress/jobs/completed/total") == (4 if interruptible else 3)

    @authors("dakovalkov")
    @require_ytserver_root_privileges
    def test_graceful_preemption(self):
        create_test_tables(row_count=1)

        command = """(trap "sleep 1; echo '{interrupt=42}'; exit 0" SIGINT; BREAKPOINT)"""

        op = map(
            track=False,
            command=with_breakpoint(command),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "mapper": {
                    "interruption_signal": "SIGINT",
                },
                "preemption_mode": "graceful",
            }
        )

        enable_op_detailed_logs(op)
        wait_breakpoint()

        update_op_parameters(op.id, parameters=get_scheduling_options(user_slots=0))
        wait(lambda: op.get_job_count("running") == 0)
        op.track()
        assert op.get_job_count("completed", from_orchid=False) == 1
        assert op.get_job_count("total", from_orchid=False) == 1
        assert read_table("//tmp/t_out") == [{"interrupt": 42}]

    @authors("dakovalkov")
    @require_ytserver_root_privileges
    def test_graceful_preemption_timeout(self):
        create_test_tables(row_count=1)

        command = "BREAKPOINT ; sleep 100"

        op = map(
            track=False,
            command=with_breakpoint(command),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "preemption_mode": "graceful",
            }
        )

        jobs = wait_breakpoint()
        release_breakpoint()
        update_op_parameters(op.id, parameters=get_scheduling_options(user_slots=0))
        wait(lambda: op.get_job_count("aborted") == 1, iter=20)
        assert op.get_job_count("total") == 1
        assert op.get_job_count("aborted") == 1

    @authors("ignat")
    def test_min_share_ratio(self):
        create_pool("test_min_share_ratio_pool", attributes={"min_share_resources": {"cpu": 3}})

        get_operation_min_share_ratio = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/adjusted_min_share_ratio".format(op_id))

        min_share_settings = [
            {"cpu": 3},
            {"cpu": 1, "user_slots": 3}
        ]

        for min_share_spec in min_share_settings:
            reset_events_on_fs()
            op = run_test_vanilla(
                with_breakpoint("BREAKPOINT"),
                spec={
                    "pool": "test_min_share_ratio_pool",
                    "min_share_resources": min_share_spec
                },
                job_count=3)
            wait_breakpoint()

            wait(lambda: get_operation_min_share_ratio(op.id) == 1.0)

            release_breakpoint()
            op.track()

    @authors("ignat")
    def test_recursive_preemption_settings(self):
        create_pool("p1", attributes={"fair_share_starvation_tolerance_limit": 0.6})
        create_pool("p2", parent_name="p1")
        create_pool("p3", parent_name="p1", attributes={"fair_share_starvation_tolerance": 0.5})
        create_pool("p4", parent_name="p1", attributes={"fair_share_starvation_tolerance": 0.9})
        create_pool("p5", attributes={"fair_share_starvation_tolerance": 0.8})
        create_pool("p6", parent_name="p5")
        time.sleep(1)

        get_pool_tolerance = lambda pool: \
            get(scheduler_orchid_default_pool_tree_path() + "/pools/{0}/adjusted_fair_share_starvation_tolerance".format(pool))

        assert get_pool_tolerance("p1") == 0.7
        assert get_pool_tolerance("p2") == 0.6
        assert get_pool_tolerance("p3") == 0.5
        assert get_pool_tolerance("p4") == 0.6
        assert get_pool_tolerance("p5") == 0.8
        assert get_pool_tolerance("p6") == 0.8

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")
        create("table", "//tmp/t_out3")
        create("table", "//tmp/t_out4")

        op1 = map(
            track=False,
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out1",
            spec={"pool": "p2", "fair_share_starvation_tolerance": 0.4})

        op2 = map(
            track=False,
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out2",
            spec={"pool": "p2", "fair_share_starvation_tolerance": 0.8})

        op3 = map(
            track=False,
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out3",
            spec={"pool": "p6"})

        op4 = map(
            track=False,
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out4",
            spec={"pool": "p6", "fair_share_starvation_tolerance": 0.9})

        time.sleep(1)

        get_operation_tolerance = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/adjusted_fair_share_starvation_tolerance".format(op_id))

        assert get_operation_tolerance(op1.id) == 0.4
        assert get_operation_tolerance(op2.id) == 0.6
        assert get_operation_tolerance(op3.id) == 0.8
        assert get_operation_tolerance(op4.id) == 0.9

        op1.abort()
        op2.abort()

    @authors("asaitgalin", "ignat")
    def test_preemption_of_jobs_excessing_resource_limits(self):
        create("table", "//tmp/t_in")
        for i in xrange(3):
            write_table("<append=%true>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")

        op = map(
            track=False,
            command="sleep 1000; cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            spec={"data_size_per_job": 1})

        wait(lambda: len(op.get_running_jobs()) == 3)

        update_op_parameters(op.id, parameters={
            "scheduling_options_per_pool_tree": {
                "default": {
                    "resource_limits": {
                        "user_slots": 1
                    }
                }
            }
        })

        wait(lambda: len(op.get_running_jobs()) == 1)

        update_op_parameters(op.id, parameters={
            "scheduling_options_per_pool_tree": {
                "default": {
                    "resource_limits": {
                        "user_slots": 0
                    }
                }
            }
        })

        wait(lambda: len(op.get_running_jobs()) == 0)

    @authors("mrkastep")
    def test_preemptor_event_log(self):
        set("//sys/pool_trees/default/@max_ephemeral_pools_per_user", 2)
        total_cpu_limit = get("//sys/scheduler/orchid/scheduler/cell/resource_limits/cpu")
        create_pool("pool1", attributes={"min_share_resources": {"cpu": total_cpu_limit}})

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out0")
        create("table", "//tmp/t_out1")

        for i in xrange(3):
            write_table("<append=%true>//tmp/t_in", {"foo": "bar"})

        op0 = map(
            track=False,
            command=with_breakpoint("BREAKPOINT; sleep 1000; cat", breakpoint_name="b0"),
            in_="//tmp/t_in",
            out="//tmp/t_out0",
            spec={"pool": "pool0", "job_count": 3})

        wait_breakpoint(breakpoint_name="b0", job_count=3)
        release_breakpoint(breakpoint_name="b0")

        op1 = map(
            track=False,
            command=with_breakpoint("cat; BREAKPOINT", breakpoint_name="b1"),
            in_="//tmp/t_in",
            out="//tmp/t_out1",
            spec={"pool": "pool1", "job_count": 1})

        preemptor_job_id = wait_breakpoint(breakpoint_name="b1")[0]
        release_breakpoint(breakpoint_name="b1")

        def check_events():
            for row in read_table("//sys/scheduler/event_log"):
                event_type = row["event_type"]
                if event_type == "job_aborted" and row["operation_id"] == op0.id:
                    assert row["preempted_for"]["operation_id"] == op1.id
                    assert row["preempted_for"]["job_id"] == preemptor_job_id
                    return True
            return False
        wait(lambda: check_events())

        op0.abort()

class TestSchedulerPreemptionClassic(BaseTestSchedulerPreemption):
    DELTA_SCHEDULER_CONFIG = BaseTestSchedulerPreemption.BASE_DELTA_SCHEDULER_CONFIG

class TestSchedulerPreemptionVector(BaseTestSchedulerPreemption):
    DELTA_SCHEDULER_CONFIG = yt.common.update(
        BaseTestSchedulerPreemption.BASE_DELTA_SCHEDULER_CONFIG,
        {"use_classic_scheduler": False}
    )

class TestInferWeightFromMinShare(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100
        }
    }

    @authors("ignat")
    def test_infer_weight_from_min_share(self):
        total_cpu_limit = get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/cpu")
        set("//sys/pool_trees/default/@infer_weight_from_min_share_ratio_multiplier", 10)

        create_pool("test_pool1", pool_tree="default", attributes={"min_share_resources": {"cpu": 0.3*total_cpu_limit}})
        create_pool("test_pool2", pool_tree="default", attributes={"min_share_resources": {"cpu": 0.4*total_cpu_limit}})
        create_pool("test_pool3", pool_tree="default")
        create_pool("subpool1", pool_tree="default", parent_name="test_pool2", attributes={"min_share_resources": {"cpu": 0.4*0.3*total_cpu_limit}})
        create_pool("subpool2", pool_tree="default", parent_name="test_pool2", attributes={"min_share_resources": {"cpu": 0.4*0.4*total_cpu_limit}})

        pools_path = scheduler_orchid_default_pool_tree_path() + "/pools"
        wait(lambda: exists(pools_path + "/subpool2"))

        wait(lambda: are_almost_equal(get(pools_path + "/test_pool1/weight"), 3.0))
        wait(lambda: are_almost_equal(get(pools_path + "/test_pool2/weight"), 4.0))
        wait(lambda: are_almost_equal(get(pools_path + "/test_pool3/weight"), 1.0))

        wait(lambda: are_almost_equal(get(pools_path + "/subpool1/weight"), 3.0))
        wait(lambda: are_almost_equal(get(pools_path + "/subpool2/weight"), 4.0))


##################################################################

class BaseTestResourceLimitsOverdraftPreemption(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    BASE_DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "graceful_preemption_job_interrupt_timeout": 2000,
            "job_interrupt_timeout": 600000,
            "allowed_node_resources_overcommit_duration": 1000,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "resource_limits": {
                    "cpu": 2,
                    "user_slots": 2
                }
            }
        }
    }

    def teardown(self):
        remove("//sys/scheduler/config", force=True)

    @authors("ignat")
    def test_scheduler_preempt_overdraft_resources(self):
        set("//sys/scheduler/config", {"job_interrupt_timeout": 1000})

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) > 0

        set("//sys/cluster_nodes/{}/@resource_limits_overrides".format(nodes[0]), {"cpu": 0})
        wait(lambda: get("//sys/cluster_nodes/{}/orchid/job_controller/resource_limits/cpu".format(nodes[0])) == 0.0)

        create("table", "//tmp/t_in")
        for i in xrange(1):
            write_table("<append=%true>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        op1 = map(
            track=False,
            command=with_breakpoint("BREAKPOINT; sleep 1000; cat"),
            in_=["//tmp/t_in"],
            out="//tmp/t_out1")
        wait_breakpoint()

        op2 = map(
            track=False,
            command=with_breakpoint("BREAKPOINT; sleep 1000; cat"),
            in_=["//tmp/t_in"],
            out="//tmp/t_out2")
        wait_breakpoint()

        wait(lambda: op1.get_job_count("running") == 1)
        wait(lambda: op2.get_job_count("running") == 1)
        wait(lambda: get("//sys/cluster_nodes/{}/orchid/job_controller/resource_usage/cpu".format(nodes[1])) == 2.0)

        # TODO(ignat): add check that jobs are not preemptable.

        set("//sys/cluster_nodes/{}/@resource_limits_overrides".format(nodes[0]), {"cpu": 2})
        wait(lambda: get("//sys/cluster_nodes/{}/orchid/job_controller/resource_limits/cpu".format(nodes[0])) == 2.0)

        set("//sys/cluster_nodes/{}/@resource_limits_overrides".format(nodes[1]), {"cpu": 0})
        wait(lambda: get("//sys/cluster_nodes/{}/orchid/job_controller/resource_limits/cpu".format(nodes[1])) == 0.0)

        wait(lambda: op1.get_job_count("aborted") == 1)
        wait(lambda: op2.get_job_count("aborted") == 1)

        wait(lambda: op1.get_job_count("running") == 1)
        wait(lambda: op2.get_job_count("running") == 1)

    @authors("ignat")
    def test_scheduler_force_abort(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) >= 2

        set("//sys/cluster_nodes/{}/@disable_scheduler_jobs".format(nodes[0]), True)
        wait(lambda: get("//sys/cluster_nodes/{}/orchid/job_controller/resource_limits/user_slots".format(nodes[0])) == 0)

        create("table", "//tmp/t_in")
        for i in xrange(1):
            write_table("<append=%true>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        op1 = map(
            track=False,
            command="sleep 1000; cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out1")
        op2 = map(
            track=False,
            command="sleep 1000; cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out2")

        wait(lambda: op1.get_job_count("running") == 1)
        wait(lambda: op2.get_job_count("running") == 1)
        wait(lambda: get("//sys/cluster_nodes/{}/orchid/job_controller/resource_usage/user_slots".format(nodes[1])) == 2)

        # TODO(ignat): add check that jobs are not preemptable.

        set("//sys/cluster_nodes/{}/@disable_scheduler_jobs".format(nodes[0]), False)
        wait(lambda: get("//sys/cluster_nodes/{}/orchid/job_controller/resource_limits/user_slots".format(nodes[0])) == 2)

        set("//sys/cluster_nodes/{}/@disable_scheduler_jobs".format(nodes[1]), True)
        wait(lambda: get("//sys/cluster_nodes/{}/orchid/job_controller/resource_limits/user_slots".format(nodes[1])) == 0)

        wait(lambda: op1.get_job_count("aborted") == 1)
        wait(lambda: op2.get_job_count("aborted") == 1)

        wait(lambda: op1.get_job_count("running") == 1)
        wait(lambda: op2.get_job_count("running") == 1)

class TestResourceLimitsOverdraftPreemptionClassic(BaseTestResourceLimitsOverdraftPreemption):
    DELTA_SCHEDULER_CONFIG = BaseTestResourceLimitsOverdraftPreemption.BASE_DELTA_SCHEDULER_CONFIG

class TestResourceLimitsOverdraftPreemptionVector(BaseTestResourceLimitsOverdraftPreemption):
    DELTA_SCHEDULER_CONFIG = yt.common.update(
        BaseTestResourceLimitsOverdraftPreemption.BASE_DELTA_SCHEDULER_CONFIG,
        {"use_classic_scheduler": False}
    )

##################################################################

class TestSchedulerUnschedulableOperations(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "operation_unschedulable_check_period": 100,
            "operation_unschedulable_safe_timeout": 5000,
            "operation_unschedulable_min_schedule_job_attempts": 10,
        }
    }

    @classmethod
    def modify_node_config(cls, config):
        config["exec_agent"]["job_controller"]["resource_limits"]["cpu"] = 2
        config["exec_agent"]["job_controller"]["resource_limits"]["user_slots"] = 2

    @authors("ignat")
    def test_unschedulable_operations(self):
        create("table", "//tmp/t_in")
        write_table("<append=true>//tmp/t_in", {"foo": "bar"})

        ops = []
        for i in xrange(5):
            table = "//tmp/t_out" + str(i)
            create("table", table)
            op = map(track=False, command="sleep 1000; cat", in_=["//tmp/t_in"], out=table,
                     spec={"pool": "fake_pool", "locality_timeout": 0, "mapper": {"cpu_limit": 0.8}})
            ops.append(op)

        for op in ops:
            wait(lambda: len(op.get_running_jobs()) == 1)

        table = "//tmp/t_out_other"
        create("table", table)
        op = map(track=False, command="sleep 1000; cat", in_=["//tmp/t_in"], out=table,
                 spec={"pool": "fake_pool", "locality_timeout": 0, "mapper": {"cpu_limit": 1.5}})

        wait(lambda: op.get_state() == "failed")

        assert "unschedulable" in str(get(op.get_path() + "/@result"))


##################################################################

class BaseTestSchedulerAggressivePreemption(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    BASE_DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100
        }
    }

    def setup_method(self, method):
        super(BaseTestSchedulerAggressivePreemption, self).setup_method(method)
        set("//sys/pool_trees/default/@aggressive_preemption_satisfaction_threshold", 0.2)
        set("//sys/pool_trees/default/@max_unpreemptable_running_job_count", 0)
        set("//sys/pool_trees/default/@fair_share_preemption_timeout", 100)
        set("//sys/pool_trees/default/@min_share_preemption_timeout", 100)
        set("//sys/pool_trees/default/@preemptive_scheduling_backoff", 0)
        set("//sys/pool_trees/default/@max_ephemeral_pools_per_user", 5)
        time.sleep(0.5)

    @classmethod
    def modify_node_config(cls, config):
        for resource in ["cpu", "user_slots"]:
            config["exec_agent"]["job_controller"]["resource_limits"][resource] = 2

    @authors("ignat")
    def test_aggressive_preemption(self):
        create("table", "//tmp/t_in")
        for i in xrange(3):
            write_table("<append=true>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")

        create_pool("special_pool")
        set("//sys/pools/special_pool/@aggressive_starvation_enabled", True)
        scheduling_info_path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/"

        def get_fair_share_ratio(op_id):
            return get(scheduling_info_path.format(op_id) + "fair_share_ratio")

        def get_usage_ratio(op_id):
            return get(scheduling_info_path.format(op_id) + "usage_ratio")

        ops = []
        for index in xrange(2):
            create("table", "//tmp/t_out" + str(index))
            op = map(track=False, command="sleep 1000; cat", in_=["//tmp/t_in"], out="//tmp/t_out" + str(index),
                    spec={"pool": "fake_pool" + str(index), "job_count": 3, "locality_timeout": 0, "mapper": {"memory_limit": 10 * 1024 * 1024}})
            ops.append(op)
        time.sleep(3)

        for op in ops:
            wait(lambda: are_almost_equal(get_fair_share_ratio(op.id), 1.0 / 2.0))
            wait(lambda: are_almost_equal(get_usage_ratio(op.id), 1.0 / 2.0))
            wait(lambda: len(op.get_running_jobs()) == 3)

        op = map(track=False, command="sleep 1000; cat", in_=["//tmp/t_in"], out="//tmp/t_out",
                 spec={"pool": "special_pool", "job_count": 1, "locality_timeout": 0, "mapper": {"cpu_limit": 2}})
        time.sleep(3)

        wait(lambda: are_almost_equal(get_fair_share_ratio(op.id), 1.0 / 3.0))
        wait(lambda: are_almost_equal(get_usage_ratio(op.id), 1.0 / 3.0))
        wait(lambda: len(op.get_running_jobs()) == 1)

class TestSchedulerAggressivePreemptionClassic(BaseTestSchedulerAggressivePreemption):
    DELTA_SCHEDULER_CONFIG = BaseTestSchedulerAggressivePreemption.BASE_DELTA_SCHEDULER_CONFIG

class TestSchedulerAggressivePreemptionVector(BaseTestSchedulerAggressivePreemption):
    DELTA_SCHEDULER_CONFIG = yt.common.update(
        BaseTestSchedulerAggressivePreemption.BASE_DELTA_SCHEDULER_CONFIG,
        {"use_classic_scheduler": False}
    )

##################################################################

class BaseTestSchedulerAggressiveStarvationPreemption(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 6
    NUM_SCHEDULERS = 1

    BASE_DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100
        }
    }

    def setup_method(self, method):
        super(BaseTestSchedulerAggressiveStarvationPreemption, self).setup_method(method)
        set("//sys/pool_trees/default/@aggressive_preemption_satisfaction_threshold", 0.35)
        set("//sys/pool_trees/default/@preemption_satisfaction_threshold", 0.75)
        set("//sys/pool_trees/default/@min_share_preemption_timeout", 100)
        set("//sys/pool_trees/default/@fair_share_preemption_timeout", 100)
        set("//sys/pool_trees/default/@max_unpreemptable_running_job_count", 0)
        set("//sys/pool_trees/default/@preemptive_scheduling_backoff", 0)
        time.sleep(0.5)

    @classmethod
    def modify_node_config(cls, config):
        for resource in ["cpu", "user_slots"]:
            config["exec_agent"]["job_controller"]["resource_limits"][resource] = 2

    @authors("ignat")
    def test_allow_aggressive_starvation_preemption(self):
        create("table", "//tmp/t_in")
        for i in xrange(3):
            write_table("<append=true>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")

        create_pool("special_pool")
        set("//sys/pools/special_pool/@aggressive_starvation_enabled", True)

        for index in xrange(4):
            create_pool("pool" + str(index))

        get_fair_share_ratio = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/fair_share_ratio".format(op_id))

        get_usage_ratio = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/usage_ratio".format(op_id))

        ops = []
        for index in xrange(4):
            create("table", "//tmp/t_out" + str(index))

            spec = {
                "pool": "pool" + str(index),
                "job_count": 3,
                "locality_timeout": 0,
                "mapper": {"memory_limit": 10 * 1024 * 1024},
            }
            if index == 0:
                spec["allow_aggressive_starvation_preemption"] = False

            op = map(
                command="sleep 1000; cat",
                in_=["//tmp/t_in"],
                out="//tmp/t_out" + str(index),
                spec=spec,
                track=False)
            ops.append(op)

        for op in ops:
            wait(lambda: are_almost_equal(get_fair_share_ratio(op.id), 1.0 / 4.0))
            wait(lambda: are_almost_equal(get_usage_ratio(op.id), 1.0 / 4.0))
            wait(lambda: len(op.get_running_jobs()) == 3)

        special_op = ops[0]
        special_op_jobs = [
            {
                "id": key,
                "start_time": date_string_to_timestamp(value["start_time"])
            }
            for key, value in special_op.get_running_jobs().iteritems()]

        special_op_jobs.sort(key=lambda x: x["start_time"])
        # There is no correct method to determine last started job by the opinion of scheduler.
        #preemtable_job_id = special_op_jobs[-1]["id"]

        op = map(
            command="sleep 1000; cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            spec={
                "pool": "special_pool",
                "job_count": 1,
                "locality_timeout": 0,
                "mapper": {"cpu_limit": 2}
            },
            track=False)

        wait(lambda: len(op.get_running_jobs()) == 1)

        special_op_running_job_count = len(special_op.get_running_jobs())
        assert special_op_running_job_count >= 2
        #if special_op_running_job_count == 2:
        #    assert preemtable_job_id not in special_op.get_running_jobs()

class TestSchedulerAggressiveStarvationPreemptionClassic(BaseTestSchedulerAggressiveStarvationPreemption):
    DELTA_SCHEDULER_CONFIG = BaseTestSchedulerAggressiveStarvationPreemption.BASE_DELTA_SCHEDULER_CONFIG

class TestSchedulerAggressiveStarvationPreemptionVector(BaseTestSchedulerAggressiveStarvationPreemption):
    DELTA_SCHEDULER_CONFIG = yt.common.update(
        BaseTestSchedulerAggressiveStarvationPreemption.BASE_DELTA_SCHEDULER_CONFIG,
        {"use_classic_scheduler": False}
    )

##################################################################

class TestSchedulerPoolsCommon(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "fair_share_update_period": 300,
            "profiling_update_period": 300,
            "fair_share_profiling_period": 300,
            "event_log": {
                "flush_period": 300,
                "retry_backoff_time": 300
            }
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "event_log": {
                "flush_period": 300,
                "retry_backoff_time": 300
            }
        }
    }

    @authors("ignat")
    def test_pools_reconfiguration(self):
        create_test_tables(attributes={"replication_factor": 1})

        testing_options = {"scheduling_delay": 1000}

        create_pool("test_pool_1")
        create_pool("test_pool_2")

        time.sleep(0.2)

        op = map(
            track=False,
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"pool": "test_pool_1", "testing": testing_options})
        time.sleep(1)

        remove("//sys/pools/test_pool_1")
        create_pool("test_pool_1", parent_name="test_pool_2")

        op.track()

    @authors("ignat")
    def test_default_parent_pool(self):
        create("table", "//tmp/t_in")
        set("//tmp/t_in/@replication_factor", 1)
        write_table("//tmp/t_in", {"foo": "bar"})

        for output in ["//tmp/t_out1", "//tmp/t_out2"]:
            create("table", output)
            set(output + "/@replication_factor", 1)

        create_pool("default_pool")
        set("//sys/pool_trees/default/@default_parent_pool", "default_pool")
        set("//sys/pool_trees/default/@max_ephemeral_pools_per_user", 2)
        time.sleep(0.2)

        command = with_breakpoint("cat ; BREAKPOINT")
        op1 = map(
            track=False,
            command=command,
            in_="//tmp/t_in",
            out="//tmp/t_out1")

        op2 = map(
            track=False,
            command=command,
            in_="//tmp/t_in",
            out="//tmp/t_out2",
            spec={"pool": "my_pool"})
        # Each operation has one job.
        wait_breakpoint(job_count=2)

        pools_path = scheduler_orchid_default_pool_tree_path() + "/pools"
        pool = get(pools_path + "/root")
        assert pool["parent"] == "default_pool"

        pool = get(pools_path + "/my_pool")
        assert pool["parent"] == "default_pool"

        scheduling_info_per_pool_tree = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"
        assert __builtin__.set(["root", "my_pool"]) == \
               __builtin__.set(get(scheduling_info_per_pool_tree + "/default/user_to_ephemeral_pools/root"))

        remove("//sys/pools/default_pool")

        release_breakpoint()
        for op in [op1, op2]:
            op.track()

    @authors("renadeen")
    def test_ephemeral_flag(self):
        create_pool("real_pool")
        op = run_sleeping_vanilla(spec={"pool": "ephemeral_pool"})
        op.wait_for_state("running")
        assert get(scheduler_orchid_default_pool_tree_path() + "/pools/ephemeral_pool/is_ephemeral")
        assert not get(scheduler_orchid_default_pool_tree_path() + "/pools/real_pool/is_ephemeral")
        assert not get(scheduler_orchid_default_pool_tree_path() + "/pools/<Root>/is_ephemeral")

    @authors("renadeen")
    def test_ephemeral_pool_in_custom_pool_simple(self):
        create_pool("custom_pool")
        set("//sys/pools/custom_pool/@create_ephemeral_subpools", True)
        time.sleep(0.2)

        op = run_sleeping_vanilla(spec={"pool": "custom_pool"})
        wait(lambda: len(list(op.get_running_jobs())) == 1)

        pool = get(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool$root")
        assert pool["parent"] == "custom_pool"
        assert pool["mode"] == "fair_share"
        assert pool["is_ephemeral"]

    @authors("renadeen")
    def test_custom_ephemeral_pool_persists_after_pool_update(self):
        create_pool("custom_pool")
        set("//sys/pools/custom_pool/@create_ephemeral_subpools", True)
        time.sleep(0.2)

        op = run_sleeping_vanilla(spec={"pool": "custom_pool"})
        wait(lambda: len(list(op.get_running_jobs())) == 1)
        assert get(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool$root/parent") == "custom_pool"

        create_pool("trigger_pool_update")
        wait(lambda: exists(scheduler_orchid_default_pool_tree_path() + "/pools/trigger_pool_update"))
        time.sleep(0.5)  # wait orchid update
        # after pools update all ephemeral pools where mistakenly moved to default pool
        assert get(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool$root/parent") == "custom_pool"

    @authors("renadeen")
    def test_ephemeral_pool_parent_is_removed_after_operation_complete(self):
        create_pool("custom_pool")
        set("//sys/pools/custom_pool/@create_ephemeral_subpools", True)
        time.sleep(0.2)

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), spec={"pool": "custom_pool"})
        wait_breakpoint()

        remove("//sys/pools/custom_pool")
        wait(lambda: exists(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool"))
        wait(lambda: exists(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool$root"))

        release_breakpoint()
        op.track()
        wait(lambda: not exists(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool"))
        wait(lambda: not exists(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool$root"))

    @authors("renadeen")
    def test_custom_ephemeral_pool_scheduling_mode(self):
        create_pool("custom_pool_fifo")
        set("//sys/pools/custom_pool_fifo/@create_ephemeral_subpools", True)
        set("//sys/pools/custom_pool_fifo/@ephemeral_subpool_config", {"mode": "fifo"})
        time.sleep(0.2)

        op = run_sleeping_vanilla(spec={"pool": "custom_pool_fifo"})
        wait(lambda: len(list(op.get_running_jobs())) == 1)

        pool_fifo = get(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool_fifo$root")
        assert pool_fifo["parent"] == "custom_pool_fifo"
        assert pool_fifo["mode"] == "fifo"

    @authors("renadeen")
    def test_custom_ephemeral_pool_max_operation_count(self):
        create_pool("custom_pool")
        set("//sys/pools/custom_pool/@create_ephemeral_subpools", True)
        set("//sys/pools/custom_pool/@ephemeral_subpool_config", {"max_operation_count": 1})
        time.sleep(0.2)

        op = run_sleeping_vanilla(spec={"pool": "custom_pool"})
        wait(lambda: len(list(op.get_running_jobs())) == 1)

        with pytest.raises(YtError):
            run_test_vanilla(command="", spec={"pool": "custom_pool"}, track=True)

    @authors("ignat")
    def test_ephemeral_pools_limit(self):
        create("table", "//tmp/t_in")
        set("//tmp/t_in/@replication_factor", 1)
        write_table("//tmp/t_in", {"foo": "bar"})

        for i in xrange(1, 5):
            output = "//tmp/t_out" + str(i)
            create("table", output)
            set(output + "/@replication_factor", 1)

        create_pool("default_pool")
        set("//sys/pool_trees/default/@default_parent_pool", "default_pool")
        set("//sys/pool_trees/default/@max_ephemeral_pools_per_user", 3)
        time.sleep(0.2)

        ops = []
        breakpoints = []
        for i in xrange(1, 4):
            breakpoint_name = "breakpoint{0}".format(i)
            breakpoints.append(breakpoint_name)
            ops.append(map(
                track=False,
                command="cat ; {breakpoint_cmd}".format(breakpoint_cmd=events_on_fs().breakpoint_cmd(breakpoint_name)),
                in_="//tmp/t_in",
                out="//tmp/t_out" + str(i),
                spec={"pool": "pool" + str(i)}))
            wait_breakpoint(breakpoint_name)

        scheduling_info_per_pool_tree = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"
        assert __builtin__.set(["pool" + str(i) for i in xrange(1, 4)]) == \
               __builtin__.set(get(scheduling_info_per_pool_tree + "/default/user_to_ephemeral_pools/root"))

        with pytest.raises(YtError):
            map(track=False,
                command="cat",
                in_="//tmp/t_in",
                out="//tmp/t_out4",
                spec={"pool": "pool4"})

        remove("//sys/pools/default_pool")
        remove("//sys/pool_trees/default/@default_parent_pool")
        remove("//sys/pool_trees/default/@max_ephemeral_pools_per_user")

        for breakpoint_name in breakpoints:
            release_breakpoint(breakpoint_name)

        for op in ops:
            op.track()

    @authors("renadeen", "babenko")
    def test_event_log(self):
        create_test_tables(attributes={"replication_factor": 1})

        create_pool("event_log_test_pool", attributes={"min_share_resources": {"cpu": 1}})
        op = map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"pool": "event_log_test_pool"})

        def check_events():
            events = []
            for row in read_table("//sys/scheduler/event_log"):
                event_type = row["event_type"]
                if event_type.startswith("operation_") and \
                    event_type != "operation_prepared" and \
                    event_type != "operation_materialized" and \
                    row["operation_id"] == op.id:
                    events.append(row["event_type"])
                    if event_type == "operation_started":
                        assert row["pool"]
                    if event_type == "operation_completed":
                        assert row["progress"]["job_statistics"]
            return events == ["operation_started", "operation_completed"]
        wait(lambda: check_events())

        def check_pools():
            pools_info = [row for row in read_table("//sys/scheduler/event_log")
                          if row["event_type"] == "pools_info" and "event_log_test_pool" in row["pools"]["default"]]
            if len(pools_info) != 1:
                return False
            custom_pool_info = pools_info[-1]["pools"]["default"]["event_log_test_pool"]
            assert are_almost_equal(custom_pool_info["min_share_resources"]["cpu"], 1.0)
            assert custom_pool_info["mode"] == "fair_share"
            return True
        wait(lambda: check_pools())

    @authors("eshcherbin")
    def test_pool_count(self):
        def get_orchid_pool_count():
            return get_from_tree_orchid("default", "fair_share_info/pool_count")

        pool_count_last = Metric.at_scheduler(
            "scheduler/pool_count",
            with_tags={"tree": "default"},
            aggr_method="last")

        def check_pool_count(expected_pool_count):
            wait(lambda: get_orchid_pool_count() == expected_pool_count)
            wait(lambda: pool_count_last.update().get(verbose=True) == expected_pool_count)

        check_pool_count(0)

        create_pool("first_pool")
        create_pool("second_pool")
        check_pool_count(2)

        op = run_sleeping_vanilla(spec={"pool": "first_pool"})
        check_pool_count(2)
        op.abort()

        # Ephemeral pool.
        op = run_sleeping_vanilla()
        check_pool_count(3)
        op.abort()
        check_pool_count(2)

class TestSchedulerPoolsReconfigurationOld(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0
    NUM_SCHEDULERS = 1

    POOL_TREES_ROOT = "//sys/map_node_pool_trees_root"
    POOL_TREE_PATH = POOL_TREES_ROOT + "/tree"

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,  # Update pools configuration period
            "pool_trees_root": POOL_TREES_ROOT
        }
    }

    orchid_pools = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/tree/fair_share_info/pools"

    def setup_method(self, method):
        super(TestSchedulerPoolsReconfigurationOld, self).setup_method(method)
        if exists(self.POOL_TREES_ROOT):
            remove(self.POOL_TREES_ROOT)

        create("map_node", self.POOL_TREES_ROOT)
        create("map_node", self.POOL_TREE_PATH)
        time.sleep(0.2)

        wait(lambda: exists(self.orchid_pools), sleep_backoff=0.1)
        wait(lambda: len(ls(self.orchid_pools)) == 1, sleep_backoff=0.1)  # <Root> is always in orchid
        wait(lambda: not get("//sys/scheduler/@alerts"), sleep_backoff=0.1)

    @authors("renadeen")
    def test_add_nested_pool(self):
        set(self.POOL_TREE_PATH + "/test_parent", {"test_pool": {}})

        self.wait_pool_exists("test_parent")
        self.wait_pool_exists("test_pool")

        assert self.get_pool_parent("test_parent") == "<Root>"
        assert self.get_pool_parent("test_pool") == "test_parent"

    @authors("renadeen")
    def test_move_to_existing_pool(self):
        create("map_node", self.POOL_TREE_PATH + "/test_parent")
        create("map_node", self.POOL_TREE_PATH + "/test_pool")
        self.wait_pool_exists("test_pool")
        wait(lambda: self.get_pool_parent("test_pool") == "<Root>")

        move(self.POOL_TREE_PATH + "/test_pool", self.POOL_TREE_PATH + "/test_parent/test_pool")
        wait(lambda: self.get_pool_parent("test_pool") == "test_parent")

    @authors("renadeen")
    def test_move_to_new_pool(self):
        create("map_node", self.POOL_TREE_PATH + "/test_pool")
        self.wait_pool_exists("test_pool")

        tx = start_transaction()
        create("map_node", self.POOL_TREE_PATH + "/new_pool", tx=tx)
        move(self.POOL_TREE_PATH + "/test_pool", self.POOL_TREE_PATH + "/new_pool/test_pool", tx=tx)
        commit_transaction(tx)

        self.wait_pool_exists("new_pool")
        wait(lambda: self.get_pool_parent("test_pool") == "new_pool")

    @authors("renadeen")
    def test_move_to_root_pool(self):
        set(self.POOL_TREE_PATH + "/test_parent", {"test_pool": {}})
        self.wait_pool_exists("test_pool")
        wait(lambda: self.get_pool_parent("test_pool") == "test_parent")

        move(self.POOL_TREE_PATH + "/test_parent/test_pool", self.POOL_TREE_PATH + "/test_pool")

        wait(lambda: self.get_pool_parent("test_pool") == "<Root>")

    @authors("renadeen")
    def test_remove_big_hierarchy(self):
        # Test bug when some pools weren't removed due to wrong removal order and inability to remove nonempty pools
        path = self.POOL_TREE_PATH
        for i in range(10):
            path = path + "/pool" + str(i)
            create("map_node", path)

        self.wait_pool_exists("pool9")

        remove(self.POOL_TREE_PATH + "/pool0")
        wait(lambda: len(ls(self.orchid_pools)) == 1)  # only <Root> must remain

    @authors("renadeen")
    def test_parent_child_swap_is_allowed(self):
        set(self.POOL_TREE_PATH + "/test_parent", {"test_pool": {}})
        self.wait_pool_exists("test_pool")
        wait(lambda: self.get_pool_parent("test_pool") == "test_parent")

        tx = start_transaction()
        move(self.POOL_TREE_PATH + "/test_parent/test_pool", self.POOL_TREE_PATH + "/test_pool", tx=tx)
        move(self.POOL_TREE_PATH + "/test_parent", self.POOL_TREE_PATH + "/test_pool/test_parent", tx=tx)
        commit_transaction(tx)

        wait(lambda: self.get_pool_parent("test_parent") == "test_pool")
        assert get("//sys/scheduler/@alerts") == []

    # Now this behaviour is prevented in master
    @authors("renadeen")
    def test_duplicate_pools_are_forbidden(self):
        tx = start_transaction()
        set(self.POOL_TREE_PATH + "/test_parent1", {"test_pool": {}}, tx=tx)
        set(self.POOL_TREE_PATH + "/test_parent2", {"test_pool": {}}, tx=tx)
        commit_transaction(tx)

        alert_message = self.wait_and_get_inner_alert_message()
        assert "Duplicate poolId test_pool found in new configuration" == alert_message
        assert ls(self.orchid_pools) == ['<Root>']

    # Now this behaviour is prevented in master
    @authors("renadeen")
    def test_root_id_are_forbidden(self):
        set(self.POOL_TREE_PATH + "/test_parent", {"<Root>": {}})

        alert_message = self.wait_and_get_inner_alert_message()
        assert "Pool name cannot be equal to root pool name" == alert_message
        assert ls(self.orchid_pools) == ['<Root>']

    # Now this behaviour is prevented in master
    @authors("renadeen")
    def test_invalid_pool_attributes(self):
        create("map_node", self.POOL_TREE_PATH + "/test_pool", attributes={"max_operation_count": "trash"})

        alert_message = self.wait_and_get_inner_alert_message()
        assert "Parsing configuration of pool \"test_pool\" failed" == alert_message
        assert ls(self.orchid_pools) == ['<Root>']

    # Now this behaviour is prevented in master
    @authors("renadeen")
    def test_invalid_pool_node(self):
        set(self.POOL_TREE_PATH + "/test_pool", 0)

        alert_message = self.wait_and_get_inner_alert_message()
        assert "Found node with type Int64, but only Map is allowed" == alert_message
        assert ls(self.orchid_pools) == ['<Root>']

    # Now this behaviour is prevented in master
    @authors("renadeen")
    def test_operation_count_validation_on_pool(self):
        create("map_node", self.POOL_TREE_PATH + "/test_pool", attributes={
            "max_operation_count": 1,
            "max_running_operation_count": 2
        })

        alert_message = self.wait_and_get_inner_alert_message()
        assert "Parsing configuration of pool \"test_pool\" failed" == alert_message
        assert ls(self.orchid_pools) == ['<Root>']

    # Now this behaviour is prevented in master
    @authors("renadeen")
    def test_subpools_of_fifo_pools_are_forbidden(self):
        tx = start_transaction()
        create("map_node", self.POOL_TREE_PATH + "/test_pool", attributes={"mode": "fifo"}, tx=tx)
        create("map_node", self.POOL_TREE_PATH + "/test_pool/test_child", tx=tx)
        commit_transaction(tx)

        alert_message = self.wait_and_get_inner_alert_message()
        assert "Pool \"test_pool\" cannot have subpools since it is in fifo mode" == alert_message
        assert ls(self.orchid_pools) == ['<Root>']

    # @authors("renadeen", "ignat")
    # def test_ephemeral_to_explicit_pool_transformation(self):
    #     create("map_node", self.POOL_TREE_PATH + "/default_pool")
    #     set(self.POOL_TREE_PATH + "/@default_parent_pool", "default_pool")
    #     self.wait_pool_exists("default_pool")
    #
    #     run_sleeping_vanilla(spec={"pool": "test_pool"})
    #     self.wait_pool_exists("test_pool")
    #
    #     create("map_node", self.POOL_TREE_PATH + "/test_pool")
    #
    #     wait(lambda: self.get_pool_parent("test_pool") == "<Root>")
    #     ephemeral_pools = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/tree/user_to_ephemeral_pools/root"
    #     assert get(ephemeral_pools) == []

    def wait_pool_exists(self, pool):
        wait(lambda: exists(self.orchid_pools + "/" + pool), sleep_backoff=0.1)

    def get_pool_parent(self, pool):
        return get(self.orchid_pools + "/" + pool + "/parent")

    def wait_and_get_inner_alert_message(self):
        wait(lambda: get("//sys/scheduler/@alerts"))
        return get("//sys/scheduler/@alerts")[0]["inner_errors"][0]["inner_errors"][0]["message"]


class TestSchedulerPoolsReconfigurationNew(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,  # Update pools configuration period
        }
    }

    orchid_pools = scheduler_orchid_default_pool_tree_path() + "/pools"

    def setup_method(self, method):
        super(TestSchedulerPoolsReconfigurationNew, self).setup_method(method)
        wait(lambda: len(ls(self.orchid_pools)) == 1, sleep_backoff=0.1)  # <Root> is always in orchid
        wait(lambda: not get("//sys/scheduler/@alerts"), sleep_backoff=0.1)

    @authors("renadeen")
    def test_add_nested_pool(self):
        create_pool("test_parent")
        create_pool("test_pool", parent_name="test_parent")

        self.wait_pool_exists("test_parent")
        self.wait_pool_exists("test_pool")

        assert self.get_pool_parent("test_parent") == "<Root>"
        assert self.get_pool_parent("test_pool") == "test_parent"

    @authors("renadeen")
    def test_move_to_existing_pool(self):
        create_pool("test_parent")
        create_pool("test_pool")
        self.wait_pool_exists("test_pool")
        wait(lambda: self.get_pool_parent("test_pool") == "<Root>")

        move("//sys/pools/test_pool", "//sys/pools/test_parent/test_pool")
        wait(lambda: self.get_pool_parent("test_pool") == "test_parent")

    @authors("renadeen")
    def test_move_to_new_pool(self):
        create_pool("test_pool")
        self.wait_pool_exists("test_pool")

        # We'd like to execute these two commands atomically
        create_pool("new_pool")
        move("//sys/pools/test_pool", "//sys/pools/new_pool/test_pool")

        self.wait_pool_exists("new_pool")
        wait(lambda: self.get_pool_parent("test_pool") == "new_pool")

    @authors("renadeen")
    def test_move_to_root_pool(self):
        create_pool("test_parent")
        create_pool("test_pool", parent_name="test_parent")
        self.wait_pool_exists("test_pool")
        wait(lambda: self.get_pool_parent("test_pool") == "test_parent")

        move("//sys/pools/test_parent/test_pool", "//sys/pools/test_pool")

        wait(lambda: self.get_pool_parent("test_pool") == "<Root>")

    @authors("renadeen")
    def test_remove_big_hierarchy(self):
        # Test bug when some pools weren't removed due to wrong removal order and inability to remove nonempty pools
        parent = None
        for i in range(10):
            pool = "pool" + str(i)
            create_pool(pool, parent_name=parent)
            parent = pool

        self.wait_pool_exists("pool9")

        remove("//sys/pools/pool0")
        wait(lambda: len(ls(self.orchid_pools)) == 1)  # only <Root> must remain

    @authors("renadeen")
    def test_subtle_reconfiguration_crash(self):
        # 1. there are two pools - parent (max_running_operation_count=1) and child (max_running_operation_count=2)
        # 2. launch first operation in child - it runs
        # 3. launch second operation in child - it will wait at parent as parent reached its max_running_operation_count
        # 4. set child's max_running_operation_count to 0
        # 5. first operation completes
        # 6. scheduler wakes up second operation
        # 7. operation is stuck in child - the pool where operation just finished - scheduler crash here

        create_pool("parent", attributes={"max_running_operation_count": 1})
        create_pool("child", parent_name="parent", attributes={"max_running_operation_count": 2})
        self.wait_pool_exists("child")
        running_op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=1, spec={"pool": "child"})
        wait_breakpoint()
        waiting_op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=1, spec={"pool": "child"})
        waiting_op.wait_for_state("pending")
        set("//sys/pools/parent/child/@max_running_operation_count", 0)
        release_breakpoint()
        running_op.track()

    @authors("renadeen", "ignat")
    def test_ephemeral_to_explicit_pool_transformation(self):
        create_pool("default_pool")
        set("//sys/pool_trees/default/@default_parent_pool", "default_pool")
        self.wait_pool_exists("default_pool")

        run_sleeping_vanilla(spec={"pool": "test_pool"})
        self.wait_pool_exists("test_pool")

        create_pool("test_pool")

        wait(lambda: self.get_pool_parent("test_pool") == "<Root>")
        ephemeral_pools = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/user_to_ephemeral_pools/root"
        wait(lambda: get(ephemeral_pools) == [])

    def wait_pool_exists(self, pool):
        wait(lambda: exists(self.orchid_pools + "/" + pool), sleep_backoff=0.1)

    def get_pool_parent(self, pool):
        return get(self.orchid_pools + "/" + pool + "/parent")

    def wait_and_get_inner_alert_message(self):
        wait(lambda: get("//sys/scheduler/@alerts"))
        return get("//sys/scheduler/@alerts")[0]["inner_errors"][0]["inner_errors"][0]["message"]

##################################################################

@pytest.mark.skip_if('not porto_avaliable()')
class TestSchedulerSuspiciousJobs(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    REQUIRE_YTSERVER_ROOT_PRIVILEGES = True
    USE_PORTO_FOR_SERVERS = True

    DELTA_NODE_CONFIG = yt.common.update(
        get_porto_delta_node_config(),
        {
            "exec_agent": {
                "scheduler_connector": {
                    "heartbeat_period": 100  # 100 msec
                },
                "job_proxy_heartbeat_period": 100,  # 100 msec
                "job_controller": {
                    "resource_limits": {
                        "user_slots": 2,
                        "cpu": 2
                    }
                }
            }
        })

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "running_jobs_update_period": 100,  # 100 msec
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "suspicious_jobs": {
                "inactivity_timeout": 2000,  # 2 sec
                "update_period": 100,  # 100 msec
            },
        }
    }

    @authors("ignat")
    def test_false_suspicious_jobs(self):
        create("table", "//tmp/t", attributes={"replication_factor": 1})
        create("table", "//tmp/t1", attributes={"replication_factor": 1})
        create("table", "//tmp/t2", attributes={"replication_factor": 1})
        write_table("//tmp/t", [{"foo": i} for i in xrange(10)])

        # Jobs below are not suspicious, they are just stupid.
        op1 = map(
            track=False,
            command='echo -ne "x = 1\nwhile True:\n    x = (x * x + 1) % 424243" | python',
            in_="//tmp/t",
            out="//tmp/t1")

        op2 = map(
            track=False,
            command='sleep 1000',
            in_="//tmp/t",
            out="//tmp/t2")

        for i in xrange(200):
            running_jobs1 = op1.get_running_jobs()
            running_jobs2 = op2.get_running_jobs()
            print_debug("running_jobs1:", len(running_jobs1), "running_jobs2:", len(running_jobs2))
            if not running_jobs1 or not running_jobs2:
                time.sleep(0.1)
            else:
                break

        if not running_jobs1 or not running_jobs2:
            assert False, "Failed to have running jobs in both operations"

        time.sleep(5)

        job1_id = running_jobs1.keys()[0]
        job2_id = running_jobs2.keys()[0]

        time.sleep(1)

        suspicious1 = get("//sys/scheduler/orchid/scheduler/jobs/{0}/suspicious".format(job1_id))
        suspicious2 = get("//sys/scheduler/orchid/scheduler/jobs/{0}/suspicious".format(job2_id))

        if suspicious1 or suspicious2:
            print_debug("Some of jobs considered suspicious, their brief statistics are:")
            for i in range(50):
                if suspicious1 and exists("//sys/scheduler/orchid/scheduler/jobs/{0}/brief_statistics".format(job1_id)):
                    print_debug("job1 brief statistics:",
                        get("//sys/scheduler/orchid/scheduler/jobs/{0}/brief_statistics".format(job1_id)))
                if suspicious2 and exists("//sys/scheduler/orchid/scheduler/jobs/{0}/brief_statistics".format(job2_id)):
                    print_debug("job2 brief statistics:",
                        get("//sys/scheduler/orchid/scheduler/jobs/{0}/brief_statistics".format(job2_id)))

        assert not suspicious1
        assert not suspicious2

        op1.abort()
        op2.abort()

    @authors("max42")
    def test_true_suspicious_jobs(self):
        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1})

        for i in range(15):
            write_table("<append=%true>//tmp/t_in", {"a": i})
        op = merge(
            track=False,
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            spec={
                "force_transform": True,
                "mode": "ordered",
                "job_io": {
                    "testing_options": {"pipe_delay": 4000},
                    "buffer_row_count": 1,
                },
                "enable_job_splitting": False,
            })

        running_jobs = None
        for i in xrange(200):
            running_jobs = op.get_running_jobs()
            print_debug("running_jobs:", len(running_jobs))
            if not running_jobs:
                time.sleep(0.1)
            else:
                break

        if not running_jobs:
            assert False, "Failed to have running jobs"

        time.sleep(5)

        job_id = running_jobs.keys()[0]

        # Most part of the time we should be suspicious, let's check that
        for i in range(30):
            suspicious = get("//sys/scheduler/orchid/scheduler/jobs/{0}/suspicious".format(job_id))
            if suspicious:
                break
            time.sleep(0.1)

        if not suspicious:
            print_debug("Job is not considered suspicious, its brief statistics are:")
            for i in range(50):
                if suspicious and exists("//sys/scheduler/orchid/scheduler/jobs/{0}/brief_statistics".format(job_id)):
                    print_debug("job brief statistics:",
                        get("//sys/scheduler/orchid/scheduler/jobs/{0}/brief_statistics".format(job_id)))

        assert suspicious

    @authors("max42")
    def DISABLED_test_true_suspicious_jobs_old(self):
        # This test involves dirty hack to make lots of retries for fetching feasible
        # seeds from master making the job suspicious (as it doesn't give the input for the
        # user job for a long time).
        #
        # We create a table consisting of the only chunk, temporarily set cpu = 0 to prevent
        # the map from running via @resource_limits_overrides, then we remove the chunk from
        # the chunk_store via the filesystem and return cpu back to the normal state.

        create("table", "//tmp/t", attributes={"replication_factor": 1})
        create("table", "//tmp/d", attributes={"replication_factor": 1})
        write_table("//tmp/t", {"a": 2})

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node = nodes[0]
        set("//sys/cluster_nodes/{0}/@resource_limits_overrides".format(node), {"cpu": 0})

        op = map(
            track=False,
            command='cat',
            in_="//tmp/t",
            out="//tmp/d")

        chunk_id = get_singular_chunk_id("//tmp/t")

        chunk_store_path = self.Env.configs["node"][0]["data_node"]["store_locations"][0]["path"]
        chunk_path = os.path.join(chunk_store_path, chunk_id[-2:], chunk_id)
        os.remove(chunk_path)
        os.remove(chunk_path + ".meta")

        set("//sys/cluster_nodes/{0}/@resource_limits_overrides".format(node), {"cpu": 1})

        while True:
            if exists("//sys/scheduler/orchid/scheduler/operations/{0}".format(op.id)):
                running_jobs = op.get_running_jobs()
                if len(running_jobs) > 0:
                    break

            time.sleep(1.0)

        assert len(running_jobs) == 1
        job_id = running_jobs.keys()[0]

        for i in xrange(20):
            suspicious = get("//sys/scheduler/orchid/scheduler/jobs/{0}/suspicious".format(job_id))
            if not suspicious:
                time.sleep(1.0)

            if exists("//sys/scheduler/orchid/scheduler/jobs/{0}/brief_statistics".format(job_id)):
                print_debug(get("//sys/scheduler/orchid/scheduler/jobs/{0}/brief_statistics".format(job_id)))

        assert suspicious

##################################################################

class TestMinNeededResources(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "min_needed_resources_update_period": 200
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "safe_scheduler_online_time": 500,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "resource_limits": {
                    "memory": 10 * 1024 * 1024 * 1024,
                    "cpu": 3
                }
            }
        },
        "resource_limits": {
            "memory": 20 * 1024 * 1024 * 1024
        }
    }

    DELTA_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1
        }
    }

    @authors("ignat")
    def test_min_needed_resources(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": 1}])
        create("table", "//tmp/t_out")

        op1 = map(
            command="sleep 100; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "mapper": {
                    "memory_limit": 8 * 1024 * 1024 * 1024,
                    "memory_reserve_factor": 1.0,
                    "cpu_limit": 1
                }
            },
            track=False)

        op1_path = "//sys/scheduler/orchid/scheduler/operations/" + op1.id
        wait(lambda: exists(op1_path) and get(op1_path + "/state") == "running")

        time.sleep(3.0)

        assert get(op1.get_path() + "/controller_orchid/progress/schedule_job_statistics/count") > 0

        create("table", "//tmp/t2_in")
        write_table("//tmp/t2_in", [{"x": 1}])
        create("table", "//tmp/t2_out")

        op2 = map(
            command="cat",
            in_="//tmp/t2_in",
            out="//tmp/t2_out",
            spec={
                "mapper": {
                    "memory_limit": 3 * 1024 * 1024 * 1024,
                    "memory_reserve_factor": 1.0,
                    "cpu_limit": 1
                }
            },
            track=False)

        op2_path = "//sys/scheduler/orchid/scheduler/operations/" + op2.id
        wait(lambda: exists(op2_path) and get(op2_path + "/state") == "running")

        time.sleep(3.0)
        assert get(op2.get_path() + "/controller_orchid/progress/schedule_job_statistics/count") == 0

        abort_op(op1.id)

        op2.track()

##################################################################

class TestSchedulerInferChildrenWeightsFromHistoricUsage(YTEnvSetup):
    NUM_CPUS_PER_NODE = 10
    NUM_SLOTS_PER_NODE = 10

    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "fair_share_profiling_period": 100,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "resource_limits": {
                    "cpu": NUM_CPUS_PER_NODE,
                    "user_slots": NUM_SLOTS_PER_NODE
                }
            }
        }
    }

    def setup_method(self, method):
        super(TestSchedulerInferChildrenWeightsFromHistoricUsage, self).setup_method(method)
        create_pool("parent")
        wait(lambda: "parent" in get(scheduler_orchid_default_pool_tree_path() + "/pools"))
        set("//sys/pools/parent/@infer_children_weights_from_historic_usage", True)
        set("//sys/pools/parent/@historic_usage_config", {
            "aggregation_mode": "exponential_moving_average",
            "ema_alpha": 1.0
        })
        wait(lambda: get("//sys/pools/parent/@infer_children_weights_from_historic_usage"))
        wait(lambda: "historic_usage_config" in get("//sys/pools/parent/@"))

    def _init_children(self, num_children=2):
        for i in xrange(num_children):
            create_pool("child" + str(i + 1), parent_name="parent")
            wait(lambda: ("child" + str(i + 1)) in get(scheduler_orchid_default_pool_tree_path() + "/pools"))

    def _get_pool_fair_share_ratio(self, pool):
        try:
            return get(scheduler_orchid_default_pool_tree_path() + "/pools/{}/fair_share_ratio".format(pool))
        except YtError:
            return 0.0

    def _get_pool_usage_ratio(self, pool):
        try:
            return get(scheduler_orchid_default_pool_tree_path() + "/pools/{}/usage_ratio".format(pool))
        except YtError:
            return 0.0

    def _get_pool_profiling(self, sensor_name, start_time, reduce="none"):
        assert reduce in ("none", "last", "max")
        result = {}
        for entry in get("//sys/scheduler/orchid/profiling/scheduler/pools/" + sensor_name,
                         from_time=int(start_time * 1000000), verbose=False):
            pool = entry["tags"]["pool"]
            if pool in result:
                result[pool].append(entry["value"])
            else:
                result[pool] = [entry["value"]]
        for pool in result:
            if reduce == "last":
                result[pool] = result[pool][-1]
            elif reduce == "max":
                result[pool] = max(result[pool])
        print_debug("Pool profiling (reduce='{}'):".format(reduce), result)
        return result

    def _test_more_fair_share_for_new_operation_base(self, num_jobs_op1, num_jobs_op2):
        self._init_children()

        op1_tasks_spec = {
            "task": {
                "job_count": num_jobs_op1,
                "command": "sleep 100;"
            }
        }
        op1 = vanilla(
            spec={
                "pool": "child1",
                "tasks": op1_tasks_spec
            },
            track=False)

        wait(lambda: are_almost_equal(self._get_pool_usage_ratio("child1"),
                                      min(num_jobs_op1 / self.NUM_SLOTS_PER_NODE, 1.0)))

        # give some time for historic usage to accumulate
        time.sleep(2)

        op2_tasks_spec = {
            "task": {
                "job_count": num_jobs_op2,
                "command": "sleep 100;"
            }
        }

        fair_share_ratio_max = Metric.at_scheduler(
            "scheduler/pools/fair_share_ratio_x100000",
            with_tags={"pool": "child2"},
            aggr_method="max")

        op2 = vanilla(
            spec={
                "pool": "child2",
                "tasks": op2_tasks_spec
            },
            track=False)

        # it's hard to estimate historic usage for all children, because run time can vary and jobs
        # can spuriously abort and restart; so we don't set the threshold any greater than 0.5
        wait(lambda: fair_share_ratio_max.update().get(verbose=True) is not None)
        wait(lambda: fair_share_ratio_max.update().get(verbose=True) > 0.5 * 100000)

        op1.complete()
        op2.complete()

    @authors("eshcherbin")
    def test_more_fair_share_for_new_operation_equal_demand(self):
        self._test_more_fair_share_for_new_operation_base(10, 10)

    @authors("eshcherbin")
    def test_more_fair_share_for_new_operation_bigger_demand(self):
        self._test_more_fair_share_for_new_operation_base(5, 10)

    @authors("eshcherbin")
    def test_more_fair_share_for_new_operation_smaller_demand(self):
        self._test_more_fair_share_for_new_operation_base(10, 6)

    # NB(eshcherbin): this test works only if new config effectively disables historic usage aggregation
    def _test_equal_fair_share_after_disabling_config_change_base(self, new_config):
        self._init_children()

        op1_tasks_spec = {
            "task": {
                "job_count": self.NUM_SLOTS_PER_NODE,
                "command": "sleep 100;"
            }
        }
        op1 = vanilla(
            spec={
                "pool": "child1",
                "tasks": op1_tasks_spec
            },
            track=False)

        wait(lambda: are_almost_equal(self._get_pool_usage_ratio("child1"), 1.0))

        # give some time for historic usage to accumulate
        time.sleep(2)

        # change config and wait for it to be applied
        set("//sys/pools/parent/@infer_children_weights_from_historic_usage",
            new_config["infer_children_weights_from_historic_usage"])
        wait(lambda: get("//sys/pools/parent/@infer_children_weights_from_historic_usage")
             == new_config["infer_children_weights_from_historic_usage"])
        set("//sys/pools/parent/@historic_usage_config", new_config["historic_usage_config"])
        wait(lambda: get("//sys/pools/parent/@historic_usage_config") == new_config["historic_usage_config"])
        time.sleep(0.5)

        op2_tasks_spec = {
            "task": {
                "job_count": self.NUM_SLOTS_PER_NODE,
                "command": "sleep 100;"
            }
        }

        fair_share_ratio_max = Metric.at_scheduler(
            "scheduler/pools/fair_share_ratio_x100000",
            with_tags={"pool": "child2"},
            aggr_method="max")

        op2 = vanilla(
            spec={
                "pool": "child2",
                "tasks": op2_tasks_spec
            },
            track=False)

        wait(lambda: fair_share_ratio_max.update().get(verbose=True) is not None)
        wait(lambda: fair_share_ratio_max.update().get(verbose=True) in (49999, 50000, 50001))

        op1.complete()
        op2.complete()

    @authors("eshcherbin")
    def test_equal_fair_share_after_disabling_historic_usage(self):
        self._test_equal_fair_share_after_disabling_config_change_base({
            "infer_children_weights_from_historic_usage": False,
            "historic_usage_config": {
                "aggregation_mode": "none",
                "ema_alpha": 0.0
            }
        })

    @authors("eshcherbin")
    def test_equal_fair_share_after_disabling_historic_usage_but_keeping_parameters(self):
        self._test_equal_fair_share_after_disabling_config_change_base({
            "infer_children_weights_from_historic_usage": False,
            "historic_usage_config": {
                "aggregation_mode": "exponential_moving_average",
                "ema_alpha": 1.0
            }
        })

    @authors("eshcherbin")
    def test_equal_fair_share_after_setting_none_mode(self):
        self._test_equal_fair_share_after_disabling_config_change_base({
            "infer_children_weights_from_historic_usage": True,
            "historic_usage_config": {
                "aggregation_mode": "none",
                "ema_alpha": 1.0
            }
        })

    @authors("eshcherbin")
    def test_equal_fair_share_after_setting_zero_alpha(self):
        self._test_equal_fair_share_after_disabling_config_change_base({
            "infer_children_weights_from_historic_usage": True,
            "historic_usage_config": {
                "aggregation_mode": "exponential_moving_average",
                "ema_alpha": 0.0
            }
        })

