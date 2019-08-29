import pytest

from yt_env_setup import YTEnvSetup, require_ytserver_root_privileges, wait, Restarter, SCHEDULERS_SERVICE
from yt.test_helpers import are_almost_equal
from yt_commands import *
from yt_helpers import ProfileMetric

from yt.common import date_string_to_timestamp

from flaky import flaky

import os
import sys
import time
import datetime

import __builtin__

##################################################################

class PrepareTables(object):
    def _create_table(self, table):
        create("table", table)
        set(table + "/@replication_factor", 1)

    def _prepare_tables(self):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        self._create_table("//tmp/t_out")

##################################################################

class TestResourceUsage(YTEnvSetup, PrepareTables):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
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

    def setup_method(self, method):
        super(TestResourceUsage, self).setup_method(method)
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
        create("map_node", "//sys/pools/big_pool", attributes={"min_share_ratio": 1.0})
        create("map_node", "//sys/pools/big_pool/subpool_1", attributes={"weight": 1.0})
        create("map_node", "//sys/pools/big_pool/subpool_2", attributes={"weight": 3.0})
        create("map_node", "//sys/pools/small_pool", attributes={"weight": 100.0})
        create("map_node", "//sys/pools/small_pool/subpool_3")
        create("map_node", "//sys/pools/small_pool/subpool_4")

        total_resource_limit = get("//sys/scheduler/orchid/scheduler/cell/resource_limits")

        # Wait for fair share update.
        time.sleep(1)

        assert not get("//sys/scheduler/@alerts")

        set("//sys/pools/small_pool/subpool_3/@min_share_resources", {"cpu": 1})

        # Wait for fair share update.
        time.sleep(1)

        assert get("//sys/scheduler/@alerts")

        remove("//sys/pools/small_pool/subpool_3/@min_share_resources")

        # Wait for fair share update.
        time.sleep(1)

        pools_orchid = scheduler_orchid_default_pool_tree_path() + "/pools"

        get_pool_guaranteed_resources = lambda pool: \
            get("{0}/{1}/guaranteed_resources".format(pools_orchid, pool))

        get_pool_guaranteed_resources_ratio = lambda pool: \
            get("{0}/{1}/guaranteed_resources_ratio".format(pools_orchid, pool))

        assert are_almost_equal(get_pool_guaranteed_resources_ratio("big_pool"), 1.0)
        assert get_pool_guaranteed_resources("big_pool") == total_resource_limit

        assert are_almost_equal(get_pool_guaranteed_resources_ratio("small_pool"), 0)
        assert are_almost_equal(get_pool_guaranteed_resources_ratio("subpool_3"), 0)
        assert are_almost_equal(get_pool_guaranteed_resources_ratio("subpool_4"), 0)

        assert are_almost_equal(get_pool_guaranteed_resources_ratio("subpool_1"), 1.0 / 4.0)
        assert are_almost_equal(get_pool_guaranteed_resources_ratio("subpool_2"), 3.0 / 4.0)

        self._prepare_tables()

        get_operation_guaranteed_resources_ratio = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/guaranteed_resources_ratio".format(op_id))

        op = map(
            dont_track=True,
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
        create("map_node", "//sys/pools/test_pool", attributes={"resource_limits": resource_limits})

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
            dont_track=True,
            command="sleep 100",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"job_count": 3, "pool": "test_pool", "mapper": {"memory_limit": memory_limit}, "testing": testing_options})
        self._check_running_jobs(op, 1)
        op.abort()

        op = map(
            dont_track=True,
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
        create("map_node", "//sys/pools/test_pool2")
        wait(lambda: "test_pool2" in get(scheduler_orchid_default_pool_tree_path() + "/pools"))

        self._prepare_tables()
        data = [{"foo": i} for i in xrange(3)]
        write_table("//tmp/t_in", data)

        op = map(
            dont_track=True,
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
            dont_track=True,
            command="sleep 100",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"job_count": 3, "resource_limits": {"user_slots": 1}})
        self._check_running_jobs(op, 1)

        set(op.get_path() + "/@resource_limits", {"user_slots": 2})
        self._check_running_jobs(op, 2)

    @authors("ignat")
    def test_max_possible_resource_usage(self):
        create("map_node", "//sys/pools/low_cpu_pool", attributes={"resource_limits": {"cpu": 1}})
        create("map_node", "//sys/pools/low_cpu_pool/subpool_1")
        create("map_node", "//sys/pools/low_cpu_pool/subpool_2", attributes={"resource_limits": {"cpu": 0}})
        create("map_node", "//sys/pools/high_cpu_pool")

        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out_1")
        self._create_table("//tmp/t_out_2")
        self._create_table("//tmp/t_out_3")
        data = [{"foo": i} for i in xrange(3)]
        write_table("//tmp/t_in", data)

        get_pool_fair_share_ratio = lambda pool: \
            get("{0}/pools/{1}/fair_share_ratio".format(scheduler_orchid_default_pool_tree_path(), pool))

        command_with_breakpoint = with_breakpoint("cat ; BREAKPOINT")
        op1 = map(
            dont_track=True,
            command=command_with_breakpoint,
            in_="//tmp/t_in",
            out="//tmp/t_out_1",
            spec={"job_count": 1, "pool": "subpool_1"})

        op2 = map(
            dont_track=True,
            command=command_with_breakpoint,
            in_="//tmp/t_in",
            out="//tmp/t_out_2",
            spec={"job_count": 2, "pool": "high_cpu_pool"})

        wait_breakpoint()

        wait(lambda: are_almost_equal(get_pool_fair_share_ratio("subpool_1"), 1.0 / 3.0))
        wait(lambda: are_almost_equal(get_pool_fair_share_ratio("low_cpu_pool"), 1.0 / 3.0))
        wait(lambda: are_almost_equal(get_pool_fair_share_ratio("high_cpu_pool"), 2.0 / 3.0))

        op3 = map(
            dont_track=True,
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out_3",
            spec={"job_count": 1, "pool": "subpool_2", "mapper": {"cpu_limit": 0}})

        time.sleep(1)

        wait(lambda: are_almost_equal(get_pool_fair_share_ratio("low_cpu_pool"), 1.0 / 2.0))
        wait(lambda: are_almost_equal(get_pool_fair_share_ratio("high_cpu_pool"), 1.0 / 2.0))

        release_breakpoint()
        op1.track()
        op2.track()
        op3.track()

    @authors("renadeen", "ignat")
    def test_recursive_fair_share_when_lack_of_resources(self):
        create("map_node", "//sys/pools/parent_pool", attributes={"min_share_ratio": 0.1})
        create("map_node", "//sys/pools/parent_pool/subpool1", attributes={"min_share_ratio": 1})
        create("map_node", "//sys/pools/parent_pool/subpool2", attributes={"min_share_ratio": 1})

        def check():
            ratio_path = scheduler_orchid_default_pool_tree_path() + "/pools/{0}/recursive_min_share_ratio"
            try:
                return \
                    get(ratio_path.format("subpool1")) == 0.05 and \
                    get(ratio_path.format("subpool2")) == 0.05
            except:
                return False

        wait(check)

    @authors("renadeen", "ignat")
    def test_fractional_cpu_usage(self):
        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        data = [{"foo": i} for i in xrange(3)]
        write_table("//tmp/t_in", data)

        op = map(
            dont_track=True,
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"job_count": 3, "mapper": {"cpu_limit": 0.87}})
        wait_breakpoint()

        resource_usage = get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/resource_usage".format(op.id))
        wait(lambda: are_almost_equal(resource_usage["cpu"], 3 * 0.87))

        release_breakpoint()
        op.track()

##################################################################


class TestStrategyWithSlowController(YTEnvSetup, PrepareTables):
    NUM_MASTERS = 1
    NUM_NODES = 10
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "node_shard_count": 1,
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
        spec = {
            "testing": {
                "scheduling_delay": 1000,
                "scheduling_delay_type": "async"
            }
        }
        op1 = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=20, spec=spec)
        op2 = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=20, spec=spec)

        wait_breakpoint(job_count=10)

        for j in op1.get_running_jobs().keys():
            release_breakpoint(job_id=j)
        for j in op2.get_running_jobs().keys():
            release_breakpoint(job_id=j)

        wait_breakpoint(job_count=10)

        wait(lambda: op1.get_job_count("running") == 5)
        wait(lambda: op2.get_job_count("running") == 5)


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
        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat",  spec={"unavailable_chunk_strategy": "wait"})

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
        op = sort(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", sort_by="key", spec={"unavailable_chunk_strategy": "wait"})

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
        op = merge(dont_track=True, mode="sorted", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t_out", spec={"unavailable_chunk_strategy": "wait"})

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
            dont_track=True,
            command=command,
            in_=["//tmp/in"],
            out="//tmp/out1",
            spec={"pool": "test_pool_1"})

        op2 = map(
            dont_track=True,
            command=command,
            in_=["//tmp/in"],
            out="//tmp/out2",
            spec={"pool": "test_pool_1"})

        op3 = map(
            dont_track=True,
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
        create("map_node", "//sys/pools/test_pool_1")
        create("map_node", "//sys/pools/test_pool_2")
        self._run_operations()

    @authors("ignat")
    def test_operations_recursive_pool_limit(self):
        create("map_node", "//sys/pools/research")
        set("//sys/pools/research/@max_running_operation_count", 2)
        create("map_node", "//sys/pools/research/test_pool_1")
        create("map_node", "//sys/pools/research/test_pool_2")
        self._run_operations()

    @authors("asaitgalin")
    def test_operation_count(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        attrs = {"max_running_operation_count": 3}
        create("map_node", "//sys/pools/research", attributes=attrs)
        create("map_node", "//sys/pools/research/subpool", attributes=attrs)
        create("map_node", "//sys/pools/research/subpool/other_subpool", attributes=attrs)

        # give time to scheduler for pool reloading
        time.sleep(0.2)

        ops = []
        for i in xrange(3):
            create("table", "//tmp/out_" + str(i))
            op = map(command="sleep 1000; cat >/dev/null",
                in_=["//tmp/in"],
                out="//tmp/out_" + str(i),
                spec={"pool": "other_subpool"},
                dont_track=True)
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

        op1 = map(dont_track=True, command="sleep 5.0; cat", in_=["//tmp/in"], out="//tmp/out1")
        op2 = map(dont_track=True, command="cat", in_=["//tmp/in"], out="//tmp/out2")

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
        op1 = map(dont_track=True, command=command, in_=["//tmp/in"], out="//tmp/out1")
        op2 = map(dont_track=True, command=command, in_=["//tmp/in"], out="//tmp/out2")
        op3 = map(dont_track=True, command=command, in_=["//tmp/in"], out="//tmp/out3")

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

        create("map_node", "//sys/pools/test_pool_1")
        create("map_node", "//sys/pools/test_pool_2")

        op1 = map(
            dont_track=True,
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_=["//tmp/in"],
            out="//tmp/out1",
            spec={"pool": "test_pool_1"})
        wait_breakpoint()

        remove("//sys/pools/test_pool_1")
        create("map_node", "//sys/pools/test_pool_2/test_pool_1")
        time.sleep(0.5)

        op2 = map(
            dont_track=True,
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
        create("map_node", "//sys/pools/research")
        create("map_node", "//sys/pools/research/research_subpool")
        create("map_node", "//sys/pools/production")
        set("//sys/pools/research/@max_operation_count", 3)

        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        for i in xrange(5):
            create("table", "//tmp/out" + str(i))

        ops = []
        def run(index, pool, should_raise):
            def execute(dont_track):
                return map(
                    dont_track=dont_track,
                    command="sleep 1000; cat",
                    in_=["//tmp/in"],
                    out="//tmp/out" + str(index),
                    spec={"pool": pool})

            if should_raise:
                with pytest.raises(YtError):
                    execute(dont_track=False)
            else:
                op = execute(dont_track=True)
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

    @authors("mrkastep")
    def test_ignoring_tentative_pool_operation_limit(self):
        nodes = ls("//sys/cluster_nodes")
        for normal_node in nodes[:2]:
            set("//sys/cluster_nodes/{0}/@user_tags".format(normal_node), ["normal"])
        for tentative_node in nodes[2:]:
            set("//sys/cluster_nodes/{0}/@user_tags".format(tentative_node), ["tentative"])

        set("//sys/pool_trees/default/@nodes_filter", "!(normal|tentative)")
        create("map_node", "//sys/pool_trees/normal", attributes={"nodes_filter": "normal"})
        create("map_node", "//sys/pool_trees/normal/pool", attributes={"max_operation_count": 5})
        create("map_node", "//sys/pool_trees/tentative", attributes={"nodes_filter": "tentative"})
        create("map_node", "//sys/pool_trees/tentative/pool", attributes={"max_operation_count": 3})

        pool_path = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/{}/fair_share_info/pools/pool"
        wait(lambda: exists(pool_path.format("normal")))
        wait(lambda: exists(pool_path.format("tentative")))

        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        for i in xrange(6):
            create("table", "//tmp/out" + str(i))

        ops = []
        def run(index, trees, tentative_trees, should_raise):
            def execute(dont_track):
                return map(
                    dont_track=dont_track,
                    command="sleep 1000; cat",
                    in_=["//tmp/in"],
                    out="//tmp/out" + str(index),
                    spec={
                        "pool_trees": list(trees),
                        "tentative_pool_trees": list(tentative_trees),
                        "scheduling_options_per_pool_tree": {tree : {"pool" : "pool"} for tree in trees | tentative_trees}
                    })

            if should_raise:
                with pytest.raises(YtError):
                    execute(dont_track=False)
            else:
                op = execute(dont_track=True)
                wait(lambda: op.get_state() in ("pending", "running"))
                ops.append(op)

        for i in xrange(3):
            run(i, {"normal", "tentative"}, frozenset(), False)

        for i in xrange(3, 5):
            run(i, {"normal", "tentative"}, frozenset(), True)

        for i in xrange(3, 5):
            run(i, {"normal"}, {"tentative"}, False)

        for i in xrange(5, 6):
            run(i, {"normal"}, {"tentative"}, True)

        wait(lambda: get(pool_path.format("normal") + "/operation_count") == 5)
        wait(lambda: get(pool_path.format("tentative") + "/operation_count") == 3)

        for op in ops:
            op.abort()

        set("//sys/pool_trees/default/@nodes_filter", "")


    @authors("ignat")
    def test_pool_changes(self):
        create("map_node", "//sys/pools/research")
        create("map_node", "//sys/pools/research/subpool")
        create("map_node", "//sys/pools/production")

        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        for i in xrange(5):
            create("table", "//tmp/out" + str(i))

        ops = []
        def run(index, pool):
            ops.append(map(
                dont_track=True,
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
        with pytest.raises(YtError):
            _run_op()

    @authors("ignat")
    def test_global_pool_acl(self):
        self._test_pool_acl_prologue()
        create("map_node", "//sys/pools/p", attributes={
            "inherit_acl": False,
            "acl": [make_ace("allow", "u", "use")]
        })
        self._test_pool_acl_core("p", "/p")

    @authors("ignat")
    def test_inner_pool_acl(self):
        self._test_pool_acl_prologue()
        create("map_node", "//sys/pools/p1", attributes={
            "inherit_acl": False,
            "acl": [make_ace("allow", "u", "use")]
        })
        create("map_node", "//sys/pools/p1/p2")
        self._test_pool_acl_core("p2", "/p1")

    @authors("ignat")
    def test_forbid_immediate_operations(self):
        self._test_pool_acl_prologue()

        create("map_node", "//sys/pools/p1", attributes={"forbid_immediate_operations": True})
        create("map_node", "//sys/pools/p1/p2")
        create("map_node", "//sys/pools/default_pool", attributes={"forbid_immediate_operations": True})

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

class TestSchedulerPreemption(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
        }
    }

    def setup_method(self, method):
        super(TestSchedulerPreemption, self).setup_method(method)
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

        op1 = map(dont_track=True, command="sleep 1000; cat", in_=["//tmp/t_in"], out="//tmp/t_out1",
                  spec={"pool": "fake_pool", "job_count": 3, "locality_timeout": 0})
        time.sleep(3)

        pools_path = scheduler_orchid_default_pool_tree_path() + "/pools"
        assert get(pools_path + "/fake_pool/fair_share_ratio") >= 0.999
        assert get(pools_path + "/fake_pool/usage_ratio") >= 0.999

        create("map_node", "//sys/pools/test_pool", attributes={"min_share_ratio": 1.0})
        op2 = map(dont_track=True, command="cat", in_=["//tmp/t_in"], out="//tmp/t_out2", spec={"pool": "test_pool"})
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
            dont_track=True,
            command=mapper,
            in_=["//tmp/t_in"],
            out="//tmp/t_out1",
            spec=spec)

        time.sleep(3)

        pools_path = scheduler_orchid_default_pool_tree_path() + "/pools"
        assert get(pools_path + "/fake_pool/fair_share_ratio") >= 0.999
        assert get(pools_path + "/fake_pool/usage_ratio") >= 0.999

        create("map_node", "//sys/pools/test_pool", attributes={"min_share_ratio": 1.0})

        # Ensure that all three jobs have started.
        events_on_fs().wait_breakpoint(timeout=datetime.timedelta(1000), job_count=3)
        events_on_fs().release_breakpoint()

        op2 = map(
            dont_track=True,
            command="cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out2",
            spec={"pool": "test_pool",
                  "max_failed_job_count": 1})
        op2.track()
        op1.track()
        assert get(op1.get_path() + "/@progress/jobs/completed/total") == (4 if interruptible else 3)

    @authors("ignat")
    def test_min_share_ratio(self):
        create("map_node", "//sys/pools/test_min_share_ratio_pool", attributes={"min_share_ratio": 1.0})

        create("table", "//tmp/t_in")
        for i in xrange(3):
            write_table("<append=true>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")

        get_operation_min_share_ratio = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/adjusted_min_share_ratio".format(op_id))

        min_share_settings = [
            {"min_share_ratio": 0.5},
            {"min_share_resources": {"cpu": 3}},
            {"min_share_resources": {"cpu": 1, "user_slots": 3}},
            {"min_share_ratio": 0.5, "min_share_resources": {"cpu": 3}},
        ]

        total_resource_limit = get("//sys/scheduler/orchid/scheduler/cell/resource_limits")

        def compute_min_share_ratio(spec):
            min_share_ratio = spec.get("min_share_ratio", 0.0)
            if "min_share_resources" in spec:
                for resource, value in spec["min_share_resources"].iteritems():
                    min_share_ratio = max(min_share_ratio, value * 1.0 / total_resource_limit[resource])
            return min_share_ratio

        for min_share_spec in min_share_settings:
            spec = {"job_count": 3, "pool": "test_min_share_ratio_pool"}
            spec.update(min_share_spec)
            reset_events_on_fs()
            op = map(
                dont_track=True,
                command=with_breakpoint("cat ; BREAKPOINT"),
                in_=["//tmp/t_in"],
                out="//tmp/t_out",
                spec=spec)
            wait_breakpoint()

            wait(lambda: get_operation_min_share_ratio(op.id) == compute_min_share_ratio(min_share_spec))

            release_breakpoint()
            op.track()

    @authors("ignat")
    def test_infer_weight_from_min_share(self):
        create("map_node", "//sys/pool_trees/custom_pool_tree", attributes={"infer_weight_from_min_share_ratio_multiplier": 10, "nodes_filter": "missing"})
        create("map_node", "//sys/pool_trees/custom_pool_tree/test_pool1", attributes={"min_share_ratio": 0.3})
        create("map_node", "//sys/pool_trees/custom_pool_tree/test_pool2", attributes={"min_share_ratio": 0.4})
        create("map_node", "//sys/pool_trees/custom_pool_tree/test_pool3")
        create("map_node", "//sys/pool_trees/custom_pool_tree/test_pool2/subpool1", attributes={"min_share_ratio": 0.3})
        create("map_node", "//sys/pool_trees/custom_pool_tree/test_pool2/subpool2", attributes={"min_share_ratio": 0.4})

        pools_path = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/custom_pool_tree/fair_share_info/pools"
        wait(lambda: exists(pools_path + "/subpool2"))
        get(pools_path)
        wait(lambda: are_almost_equal(get(pools_path + "/test_pool1/weight"), 3.0))
        wait(lambda: are_almost_equal(get(pools_path + "/test_pool2/weight"), 4.0))
        wait(lambda: are_almost_equal(get(pools_path + "/test_pool3/weight"), 1.0))

        wait(lambda: are_almost_equal(get(pools_path + "/subpool1/weight"), 3.0))
        wait(lambda: are_almost_equal(get(pools_path + "/subpool2/weight"), 4.0))

    @authors("ignat")
    def test_recursive_preemption_settings(self):
        create("map_node", "//sys/pools/p1", attributes={"fair_share_starvation_tolerance_limit": 0.6})
        create("map_node", "//sys/pools/p1/p2")
        create("map_node", "//sys/pools/p1/p3", attributes={"fair_share_starvation_tolerance": 0.5})
        create("map_node", "//sys/pools/p1/p4", attributes={"fair_share_starvation_tolerance": 0.9})
        create("map_node", "//sys/pools/p5", attributes={"fair_share_starvation_tolerance": 0.8})
        create("map_node", "//sys/pools/p5/p6")
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
            dont_track=True,
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out1",
            spec={"pool": "p2", "fair_share_starvation_tolerance": 0.4})

        op2 = map(
            dont_track=True,
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out2",
            spec={"pool": "p2", "fair_share_starvation_tolerance": 0.8})

        op3 = map(
            dont_track=True,
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out3",
            spec={"pool": "p6"})

        op4 = map(
            dont_track=True,
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
            dont_track=True,
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
            op = map(dont_track=True, command="sleep 1000; cat", in_=["//tmp/t_in"], out=table,
                     spec={"pool": "fake_pool", "locality_timeout": 0, "mapper": {"cpu_limit": 0.8}})
            ops.append(op)

        for op in ops:
            wait(lambda: len(op.get_running_jobs()) == 1)

        table = "//tmp/t_out_other"
        create("table", table)
        op = map(dont_track=True, command="sleep 1000; cat", in_=["//tmp/t_in"], out=table,
                 spec={"pool": "fake_pool", "locality_timeout": 0, "mapper": {"cpu_limit": 1.5}})

        wait(lambda: op.get_state() == "failed")

        assert "unschedulable" in str(get(op.get_path() + "/@result"))


##################################################################

class TestSchedulerAggressivePreemption(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100
        }
    }

    def setup_method(self, method):
        super(TestSchedulerAggressivePreemption, self).setup_method(method)
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

        create("map_node", "//sys/pools/special_pool")
        set("//sys/pools/special_pool/@aggressive_starvation_enabled", True)
        scheduling_info_path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/"

        def get_fair_share_ratio(op_id):
            return get(scheduling_info_path.format(op_id) + "fair_share_ratio")

        def get_usage_ratio(op_id):
            return get(scheduling_info_path.format(op_id) + "usage_ratio")

        ops = []
        for index in xrange(2):
            create("table", "//tmp/t_out" + str(index))
            op = map(dont_track=True, command="sleep 1000; cat", in_=["//tmp/t_in"], out="//tmp/t_out" + str(index),
                    spec={"pool": "fake_pool" + str(index), "job_count": 3, "locality_timeout": 0, "mapper": {"memory_limit": 10 * 1024 * 1024}})
            ops.append(op)
        time.sleep(3)

        for op in ops:
            wait(lambda: are_almost_equal(get_fair_share_ratio(op.id), 1.0 / 2.0))
            wait(lambda: are_almost_equal(get_usage_ratio(op.id), 1.0 / 2.0))
            wait(lambda: len(op.get_running_jobs()) == 3)

        op = map(dont_track=True, command="sleep 1000; cat", in_=["//tmp/t_in"], out="//tmp/t_out",
                 spec={"pool": "special_pool", "job_count": 1, "locality_timeout": 0, "mapper": {"cpu_limit": 2}})
        time.sleep(3)

        wait(lambda: are_almost_equal(get_fair_share_ratio(op.id), 1.0 / 3.0))
        wait(lambda: are_almost_equal(get_usage_ratio(op.id), 1.0 / 3.0))
        wait(lambda: len(op.get_running_jobs()) == 1)

##################################################################

class TestSchedulerAggressiveStarvationPreemption(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 6
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100
        }
    }

    def setup_method(self, method):
        super(TestSchedulerAggressiveStarvationPreemption, self).setup_method(method)
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

        create("map_node", "//sys/pools/special_pool")
        set("//sys/pools/special_pool/@aggressive_starvation_enabled", True)

        for index in xrange(4):
            create("map_node", "//sys/pools/pool" + str(index))

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
                dont_track=True)
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
            dont_track=True)

        wait(lambda: len(op.get_running_jobs()) == 1)

        special_op_running_job_count = len(special_op.get_running_jobs())
        assert special_op_running_job_count >= 2
        #if special_op_running_job_count == 2:
        #    assert preemtable_job_id not in special_op.get_running_jobs()

##################################################################

class TestSchedulerPools(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
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

        create("map_node", "//sys/pools/test_pool_1")
        create("map_node", "//sys/pools/test_pool_2")
        time.sleep(0.2)

        op = map(
            dont_track=True,
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"pool": "test_pool_1", "testing": testing_options})
        time.sleep(1)

        remove("//sys/pools/test_pool_1")
        create("map_node", "//sys/pools/test_pool_2/test_pool_1")

        op.track()

    @authors("ignat")
    def test_default_parent_pool(self):
        create("table", "//tmp/t_in")
        set("//tmp/t_in/@replication_factor", 1)
        write_table("//tmp/t_in", {"foo": "bar"})

        for output in ["//tmp/t_out1", "//tmp/t_out2"]:
            create("table", output)
            set(output + "/@replication_factor", 1)

        create("map_node", "//sys/pools/default_pool")
        set("//sys/pool_trees/default/@default_parent_pool", "default_pool")
        set("//sys/pool_trees/default/@max_ephemeral_pools_per_user", 2)
        time.sleep(0.2)

        command = with_breakpoint("cat ; BREAKPOINT")
        op1 = map(
            dont_track=True,
            command=command,
            in_="//tmp/t_in",
            out="//tmp/t_out1")

        op2 = map(
            dont_track=True,
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
    def test_ephemeral_pool_in_custom_pool_simple(self):
        create("map_node", "//sys/pools/custom_pool")
        set("//sys/pools/custom_pool/@create_ephemeral_subpools", True)
        time.sleep(0.2)

        op = run_sleeping_vanilla(spec={"pool": "custom_pool"})
        wait(lambda: len(list(op.get_running_jobs())) == 1)

        pool = get(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool$root")
        assert pool["parent"] == "custom_pool"
        assert pool["mode"] == "fair_share"

    @authors("renadeen")
    def test_ephemeral_pool_scheduling_mode(self):
        create("map_node", "//sys/pools/custom_pool_fifo")
        set("//sys/pools/custom_pool_fifo/@create_ephemeral_subpools", True)
        set("//sys/pools/custom_pool_fifo/@ephemeral_subpool_config", {"mode": "fifo"})
        time.sleep(0.2)

        op = run_sleeping_vanilla(spec={"pool": "custom_pool_fifo"})
        wait(lambda: len(list(op.get_running_jobs())) == 1)

        pool_fifo = get(scheduler_orchid_default_pool_tree_path() + "/pools/custom_pool_fifo$root")
        assert pool_fifo["parent"] == "custom_pool_fifo"
        assert pool_fifo["mode"] == "fifo"

    @authors("renadeen")
    def test_ephemeral_pool_max_operation_count(self):
        create("map_node", "//sys/pools/custom_pool")
        set("//sys/pools/custom_pool/@create_ephemeral_subpools", True)
        set("//sys/pools/custom_pool/@ephemeral_subpool_config", {"max_operation_count": 1})
        time.sleep(0.2)

        op = run_sleeping_vanilla(spec={"pool": "custom_pool"})
        wait(lambda: len(list(op.get_running_jobs())) == 1)

        with pytest.raises(YtError):
            run_test_vanilla(command="", spec={"pool": "custom_pool"}, dont_track=False)

    @authors("ignat")
    def test_ephemeral_pools_limit(self):
        create("table", "//tmp/t_in")
        set("//tmp/t_in/@replication_factor", 1)
        write_table("//tmp/t_in", {"foo": "bar"})

        for i in xrange(1, 5):
            output = "//tmp/t_out" + str(i)
            create("table", output)
            set(output + "/@replication_factor", 1)

        create("map_node", "//sys/pools/default_pool")
        set("//sys/pool_trees/default/@default_parent_pool", "default_pool")
        set("//sys/pool_trees/default/@max_ephemeral_pools_per_user", 3)
        time.sleep(0.2)

        ops = []
        breakpoints = []
        for i in xrange(1, 4):
            breakpoint_name = "breakpoint{0}".format(i)
            breakpoints.append(breakpoint_name)
            ops.append(map(
                dont_track=True,
                command="cat ; {breakpoint_cmd}".format(breakpoint_cmd=events_on_fs().breakpoint_cmd(breakpoint_name)),
                in_="//tmp/t_in",
                out="//tmp/t_out" + str(i),
                spec={"pool": "pool" + str(i)}))
            wait_breakpoint(breakpoint_name)

        scheduling_info_per_pool_tree = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"
        assert __builtin__.set(["pool" + str(i) for i in xrange(1, 4)]) == \
               __builtin__.set(get(scheduling_info_per_pool_tree + "/default/user_to_ephemeral_pools/root"))

        with pytest.raises(YtError):
            map(dont_track=True,
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

        create("map_node", "//sys/pools/event_log_test_pool", attributes={"min_share_resources": {"cpu": 1}})
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


class TestSchedulerPoolsReconfiguration(YTEnvSetup):
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
        super(TestSchedulerPoolsReconfiguration, self).setup_method(method)
        wait(lambda: len(ls(self.orchid_pools)) == 1, sleep_backoff=0.1)  # <Root> is always in orchid
        wait(lambda: not get("//sys/scheduler/@alerts"), sleep_backoff=0.1)

    @authors("renadeen")
    def test_add_nested_pool(self):
        set("//sys/pools/test_parent", {"test_pool": {}})

        self.wait_pool_exists("test_parent")
        self.wait_pool_exists("test_pool")

        assert self.get_pool_parent("test_parent") == "<Root>"
        assert self.get_pool_parent("test_pool") == "test_parent"

    @authors("renadeen")
    def test_move_to_existing_pool(self):
        create("map_node", "//sys/pools/test_parent")
        create("map_node", "//sys/pools/test_pool")
        self.wait_pool_exists("test_pool")
        wait(lambda: self.get_pool_parent("test_pool") == "<Root>")

        move("//sys/pools/test_pool", "//sys/pools/test_parent/test_pool")
        wait(lambda: self.get_pool_parent("test_pool") == "test_parent")

    @authors("renadeen")
    def test_move_to_new_pool(self):
        create("map_node", "//sys/pools/test_pool")
        self.wait_pool_exists("test_pool")

        tx = start_transaction()
        create("map_node", "//sys/pools/new_pool", tx=tx)
        move("//sys/pools/test_pool", "//sys/pools/new_pool/test_pool", tx=tx)
        commit_transaction(tx)

        self.wait_pool_exists("new_pool")
        wait(lambda: self.get_pool_parent("test_pool") == "new_pool")

    @authors("renadeen")
    def test_move_to_root_pool(self):
        set("//sys/pools/test_parent", {"test_pool": {}})
        self.wait_pool_exists("test_pool")
        wait(lambda: self.get_pool_parent("test_pool") == "test_parent")

        move("//sys/pools/test_parent/test_pool", "//sys/pools/test_pool")

        wait(lambda: self.get_pool_parent("test_pool") == "<Root>")

    @authors("renadeen")
    def test_parent_child_swap_is_forbidden(self):
        set("//sys/pools/test_parent", {"test_pool": {}})
        self.wait_pool_exists("test_pool")
        wait(lambda: self.get_pool_parent("test_pool") == "test_parent")

        tx = start_transaction()
        move("//sys/pools/test_parent/test_pool", "//sys/pools/test_pool", tx=tx)
        move("//sys/pools/test_parent", "//sys/pools/test_pool/test_parent", tx=tx)
        commit_transaction(tx)

        wait(lambda: get("//sys/scheduler/@alerts"))
        alert_message = get("//sys/scheduler/@alerts")[0]["inner_errors"][0]["inner_errors"][0]["message"]
        assert "Path to pool \"test_parent\" changed in more than one place; make pool tree changes more gradually" == alert_message
        wait(lambda: self.get_pool_parent("test_pool") == "test_parent")

    @authors("renadeen")
    def test_duplicate_pools_are_forbidden(self):
        tx = start_transaction()
        set("//sys/pools/test_parent1", {"test_pool": {}}, tx=tx)
        set("//sys/pools/test_parent2", {"test_pool": {}}, tx=tx)
        commit_transaction(tx)

        alert_message = self.wait_and_get_inner_alert_message()
        assert "Duplicate poolId test_pool found in new configuration" == alert_message
        assert ls(self.orchid_pools) == ['<Root>']

    @authors("renadeen")
    def test_root_id_are_forbidden(self):
        set("//sys/pools/test_parent", {"<Root>": {}})

        alert_message = self.wait_and_get_inner_alert_message()
        assert "Use of root element id is forbidden" == alert_message
        assert ls(self.orchid_pools) == ['<Root>']

    @authors("renadeen")
    def test_invalid_pool_attributes(self):
        create("map_node", "//sys/pools/test_pool", attributes={"max_operation_count": "trash"})

        alert_message = self.wait_and_get_inner_alert_message()
        assert "Parsing configuration of pool \"test_pool\" failed" == alert_message
        assert ls(self.orchid_pools) == ['<Root>']

    @authors("renadeen")
    def test_invalid_pool_node(self):
        set("//sys/pools/test_pool", 0)

        alert_message = self.wait_and_get_inner_alert_message()
        assert "Found node with type Int64, but only Map is allowed" == alert_message
        assert ls(self.orchid_pools) == ['<Root>']

    @authors("renadeen")
    def test_operation_count_validation_on_pool(self):
        create("map_node", "//sys/pools/test_pool", attributes={
            "max_operation_count": 1,
            "max_running_operation_count": 2
        })

        alert_message = self.wait_and_get_inner_alert_message()
        assert "Parsing configuration of pool \"test_pool\" failed" == alert_message
        assert ls(self.orchid_pools) == ['<Root>']

    @authors("renadeen")
    def test_subpools_of_fifo_pools_are_forbidden(self):
        tx = start_transaction()
        create("map_node", "//sys/pools/test_pool", attributes={"mode": "fifo"}, tx=tx)
        create("map_node", "//sys/pools/test_pool/test_child", tx=tx)
        commit_transaction(tx)

        alert_message = self.wait_and_get_inner_alert_message()
        assert "Pool \"test_pool\" cannot have subpools since it is in fifo mode" == alert_message
        assert ls(self.orchid_pools) == ['<Root>']

    @authors("renadeen", "ignat")
    def test_ephemeral_to_explicit_pool_transformation(self):
        create("map_node", "//sys/pools/default_pool")
        set("//sys/pool_trees/default/@default_parent_pool", "default_pool")
        self.wait_pool_exists("default_pool")

        run_sleeping_vanilla(spec={"pool": "test_pool"})
        self.wait_pool_exists("test_pool")

        create("map_node", "//sys/pools/test_pool")

        wait(lambda: self.get_pool_parent("test_pool") == "<Root>")
        ephemeral_pools = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/user_to_ephemeral_pools/root"
        assert get(ephemeral_pools) == []

    def wait_pool_exists(self, pool):
        wait(lambda: exists(self.orchid_pools + "/" + pool), sleep_backoff=0.1)

    def get_pool_parent(self, pool):
        return get(self.orchid_pools + "/" + pool + "/parent")

    def wait_and_get_inner_alert_message(self):
        wait(lambda: get("//sys/scheduler/@alerts"))
        return get("//sys/scheduler/@alerts")[0]["inner_errors"][0]["inner_errors"][0]["message"]

##################################################################

@require_ytserver_root_privileges
class TestSchedulerSuspiciousJobs(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    # This is a mix of options for 18.4 and 18.5
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "slot_manager": {
                "job_environment": {
                    "type": "cgroups",
                    "memory_watchdog_period": 100,
                    "supported_cgroups": [
                        "cpuacct",
                        "blkio",
                        "cpu"],
                },
            },
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
    }

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
            dont_track=True,
            command='echo -ne "x = 1\nwhile True:\n    x = (x * x + 1) % 424243" | python',
            in_="//tmp/t",
            out="//tmp/t1")

        op2 = map(
            dont_track=True,
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
            dont_track=True,
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

    @authors("ignat")
    @pytest.mark.xfail(reason="TODO(max42)")
    def test_true_suspicious_jobs_old(self):
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
            dont_track=True,
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
            dont_track=True)

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
            dont_track=True)

        op2_path = "//sys/scheduler/orchid/scheduler/operations/" + op2.id
        wait(lambda: exists(op2_path) and get(op2_path + "/state") == "running")

        time.sleep(3.0)
        assert get(op2.get_path() + "/controller_orchid/progress/schedule_job_statistics/count") == 0

        abort_op(op1.id)

        op2.track()

##################################################################

class TestPoolTreesReconfiguration(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            # Unrecognized alert often interferes with the alerts that
            # are tested in this test suite.
            "enable_unrecognized_alert": False,
            "alerts_update_period": 100
        }
    }

    def teardown_method(self, method):
        for node in ls("//sys/cluster_nodes"):
            set("//sys/cluster_nodes/{}/@resource_limits_overrides".format(node), {})
        remove("//sys/pool_trees/*")
        create("map_node", "//sys/pool_trees/default")
        set("//sys/pool_trees/@default_tree", "default")
        time.sleep(0.5)  # Give scheduler some time to reload trees
        super(TestPoolTreesReconfiguration, self).teardown_method(method)

    @authors("asaitgalin")
    def test_basic_sanity(self):
        assert exists("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/fair_share_info")

        create("map_node", "//sys/pool_trees/other", attributes={"nodes_filter": "other"})

        wait(lambda: exists("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/other/fair_share_info"))
        wait(lambda: not get("//sys/scheduler/@alerts"))

        # This tree intersects with default pool tree by nodes, should not be added
        create("map_node", "//sys/pool_trees/other_intersecting", attributes={"nodes_filter": ""})
        time.sleep(1.0)
        assert not exists("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/other_intersecting/fair_share_info")
        assert get("//sys/scheduler/@alerts")

        remove("//sys/pool_trees/other_intersecting")
        wait(lambda: "update_pools" not in get("//sys/scheduler/@alerts"))

    @authors("asaitgalin")
    def test_abort_orphaned_operations(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": 1}])
        create("table", "//tmp/t_out")

        op = map(
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            dont_track=True)

        wait(lambda: op.get_state() == "running")

        remove("//sys/pool_trees/@default_tree")
        remove("//sys/pool_trees/default")

        wait(lambda: op.get_state() in ["aborted", "aborting"])

    @authors("ignat")
    def test_abort_many_orphaned_operations(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": 1}])

        node = ls("//sys/cluster_nodes")[0]
        set("//sys/cluster_nodes/{}/@resource_limits_overrides".format(node), {"cpu": 10, "user_slots": 10})

        ops = []
        for i in xrange(10):
            create("table", "//tmp/t_out" + str(i))
            ops.append(map(
                command="sleep 1000; cat",
                in_="//tmp/t_in",
                out="//tmp/t_out" + str(i),
                dont_track=True))

        for op in ops:
            wait(lambda: op.get_state() == "running")

        remove("//sys/pool_trees/@default_tree")
        remove("//sys/pool_trees/default")

        for op in reversed(ops):
            try:
                op.abort()
            except YtError:
                pass

        for op in ops:
            wait(lambda: op.get_state() in ["aborted", "aborting"])

    @authors("asaitgalin")
    def test_multitree_operations(self):
        create("table", "//tmp/t_in")
        for i in xrange(15):
            write_table("<append=%true>//tmp/t_in", [{"x": i}])
        create("table", "//tmp/t_out")

        self.create_custom_pool_tree_with_one_node(pool_tree="other")

        time.sleep(1.0)

        op = map(
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"pool_trees": ["default", "other"]},
            dont_track=True)

        op.track()

    @authors("asaitgalin")
    def test_revive_multitree_operation(self):
        create("table", "//tmp/t_in")
        for i in xrange(6):
            write_table("<append=%true>//tmp/t_in", [{"x": i}])
        create("table", "//tmp/t_out")

        self.create_custom_pool_tree_with_one_node(pool_tree="other")

        time.sleep(1.0)

        op = map(
            command="sleep 4; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"pool_trees": ["default", "other"], "data_size_per_job": 1},
            dont_track=True)

        wait(lambda: len(op.get_running_jobs()) > 2)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            time.sleep(0.5)

        wait(lambda: op.get_state() == "running")
        op.track()

    @authors("asaitgalin", "ignat")
    def test_incorrect_node_tags(self):
        create("map_node", "//sys/pool_trees/supertree1", attributes={"nodes_filter": "x|y"})
        create("map_node", "//sys/pool_trees/supertree2", attributes={"nodes_filter": "y|z"})
        wait(lambda: "supertree1" in ls("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"))
        wait(lambda: "supertree2" in ls("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"))

        node = ls("//sys/cluster_nodes")[0]
        assert get("//sys/scheduler/orchid/scheduler/nodes/" + node + "/scheduler_state") == "online"
        assert get("//sys/scheduler/orchid/scheduler/cell/resource_limits/user_slots") == 3

        assert not get("//sys/scheduler/@alerts")

        set("//sys/cluster_nodes/" + node + "/@user_tags/end", "y")

        wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/" + node + "/scheduler_state") == "offline")
        assert get("//sys/scheduler/orchid/scheduler/cell/resource_limits/user_slots") == 2
        wait(lambda: get("//sys/scheduler/@alerts"))
        assert get("//sys/scheduler/@alerts")[0]

    @authors("asaitgalin")
    def test_default_tree_manipulations(self):
        assert get("//sys/pool_trees/@default_tree") == "default"

        remove("//sys/pool_trees/@default_tree")
        time.sleep(0.5)

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": 1}])
        create("table", "//tmp/t_out")

        error_occured = False
        try:
            op = map(command="cat", in_="//tmp/t_in", out="//tmp/t_out")
        except YtResponseError:
            error_occured = True

        assert error_occured or op.get_state() == "failed"

        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"pool_trees": ["default"]})

        set("//sys/pool_trees/@default_tree", "unexisting")
        wait(lambda: get("//sys/scheduler/@alerts"))
        wait(lambda: not exists("//sys/scheduler/orchid/scheduler/default_fair_share_tree"))

        set("//sys/pool_trees/@default_tree", "default")
        wait(lambda: exists("//sys/scheduler/orchid/scheduler/default_fair_share_tree"))
        assert get("//sys/scheduler/orchid/scheduler/default_fair_share_tree") == "default"

    @authors("asaitgalin")
    def test_fair_share(self):
        create("table", "//tmp/t_in")
        for i in xrange(3):
            write_table("<append=%true>//tmp/t_in", [{"x": i}])

        node = self.create_custom_pool_tree_with_one_node(pool_tree="other")

        orchid_root = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"
        wait(lambda: get(orchid_root + "/default/node_count") == 2)
        wait(lambda: get(orchid_root + "/other/node_count") == 1)
        assert are_almost_equal(get(orchid_root + "/default/resource_limits")["cpu"], 2)
        assert are_almost_equal(get(orchid_root + "/other/resource_limits")["cpu"], 1)
        assert node in get(orchid_root + "/other/node_addresses")
        assert node not in get(orchid_root + "/default/node_addresses")

        create("table", "//tmp/t_out_1")
        op1 = map(
            command="sleep 100; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out_1",
            spec={"pool_trees": ["default", "other"], "data_size_per_job": 1},
            dont_track=True)

        create("table", "//tmp/t_out_2")
        op2 = map(
            command="sleep 100; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out_2",
            spec={"pool_trees": ["other"], "data_size_per_job": 1},
            dont_track=True)

        def get_fair_share(tree, op_id):
            try:
                return get("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/{0}/fair_share_info/operations/{1}/fair_share_ratio"
                           .format(tree, op_id))
            except YtError:
                return 0.0

        wait(lambda: are_almost_equal(get_fair_share("default", op1.id), 1.0))
        wait(lambda: are_almost_equal(get_fair_share("other", op1.id), 0.5))
        wait(lambda: are_almost_equal(get_fair_share("other", op2.id), 0.5))

    @authors("asaitgalin", "shakurov")
    def test_default_tree_update(self):
        self.create_custom_pool_tree_with_one_node(pool_tree="other")
        time.sleep(0.5)

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": 1}])

        create("table", "//tmp/t_out_1")
        op1 = map(
            command="sleep 100; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out_1",
            dont_track=True)
        wait(lambda: op1.get_state() == "running")

        set("//sys/pool_trees/@default_tree", "other")
        wait(lambda: get("//sys/scheduler/orchid/scheduler/default_fair_share_tree") == "other")
        assert op1.get_state() == "running"

        create("table", "//tmp/t_out_2")
        op2 = map(
            command="sleep 100; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out_2",
            dont_track=True)
        wait(lambda: op2.get_state() == "running")

        operations_path = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/{}/fair_share_info/operations"
        default_operations_path = operations_path.format("default")
        other_operations_path = operations_path.format("other")

        wait(lambda: len(ls(default_operations_path)) == 1)
        wait(lambda: len(ls(other_operations_path)) == 1)

        default_tree_operations = get(default_operations_path)
        other_tree_operations = get(other_operations_path)
        assert op1.id in default_tree_operations
        assert op1.id not in other_tree_operations
        assert op2.id in other_tree_operations
        assert op2.id not in default_tree_operations

    def create_custom_pool_tree_with_one_node(self, pool_tree):
        tag = pool_tree
        node = ls("//sys/cluster_nodes")[0]
        set("//sys/cluster_nodes/" + node + "/@user_tags/end", tag)
        create("map_node", "//sys/pool_trees/" + pool_tree, attributes={"nodes_filter": tag})
        set("//sys/pool_trees/default/@nodes_filter", "!" + tag)
        return node

class TestTentativePoolTrees(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 6
    NUM_SCHEDULERS = 1

    TENTATIVE_TREE_ELIGIBILITY_SAMPLE_JOB_COUNT = 5
    MAX_TENTATIVE_TREE_JOB_DURATION_RATIO = 2
    TENTATIVE_TREE_ELIGIBILITY_MIN_JOB_DURATION = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "orchid_keys_update_period": 100,
            "static_orchid_cache_update_period": 100
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "check_tentative_tree_eligibility_period": 1000000,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent" : {
            "job_controller" : {
                "resource_limits" : {
                    "user_slots" : 2,
                    "cpu" : 2,
                },
            },
        },
    }

    def teardown_method(self, method):
        if exists("//sys/controller_agents/config"):
            set("//sys/controller_agents/config/check_tentative_tree_eligibility_period", 100 * 1000)
        remove("//sys/pool_trees/other")
        super(TestTentativePoolTrees, self).teardown_method(method)

    # Creates and additional pool tree called "other", configures tag filters,
    # tags some nodes as "other" and returns a list of those nodes.
    def _prepare_pool_trees(self):
        other_nodes = ls("//sys/cluster_nodes")[:3]
        for node in other_nodes:
            set("//sys/cluster_nodes/" + node + "/@user_tags/end", "other")

        set("//sys/pool_trees/default/@nodes_filter", "!other")
        create("map_node", "//sys/pool_trees/other", attributes={"nodes_filter": "other"})
        set("//sys/pool_trees/default/@nodes_filter", "!other")
        time.sleep(0.5)

        return other_nodes

    def _create_spec(self):
        spec = {
            "pool_trees": ["default"],
            "tentative_pool_trees": ["other"],
            "scheduling_options_per_pool_tree": {
                "default": {
                    "min_share_ratio": 0.37
                },
                "other": {
                    "pool": "superpool"
                }
            },
            "tentative_tree_eligibility": {
                "sample_job_count": TestTentativePoolTrees.TENTATIVE_TREE_ELIGIBILITY_SAMPLE_JOB_COUNT,
                "max_tentative_job_duration_ratio": TestTentativePoolTrees.MAX_TENTATIVE_TREE_JOB_DURATION_RATIO,
                "min_job_duration": TestTentativePoolTrees.TENTATIVE_TREE_ELIGIBILITY_MIN_JOB_DURATION,
            },
            "data_size_per_job": 1,
        }
        return spec

    def _iter_running_jobs(self, op, tentative_nodes):
        jobs_path = op.get_path() + "/controller_orchid/running_jobs"

        try:
            jobs = ls(jobs_path)
        except YtError:
            return []

        result = []
        for job_id in jobs:
            try:
                job_node = get("{0}/{1}".format(jobs_path, job_id))["address"]
            except YtError:
                continue # The job has already completed, Orchid is lagging.

            job_is_tentative = job_node in tentative_nodes
            result.append((job_id, job_is_tentative))
        return result

    # It's just flapping sheet YT-11156
    @flaky(max_runs=5)
    @authors("ignat")
    def test_tentative_pool_tree_sampling(self):
        other_nodes = self._prepare_pool_trees()
        spec = self._create_spec()

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": i} for i in xrange(20)])
        create("table", "//tmp/t_out")

        op = map(
            command="sleep 100; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec=spec,
            dont_track=True)

        jobs_path = op.get_path() + "/controller_orchid/running_jobs"

        dummy = {"jobs": [], "stability_count": 0} # no "nonlocal" support in python 2
        def all_jobs_running():
            try:
                old_job_count = len(dummy["jobs"])
                dummy["jobs"] = ls(jobs_path)
                new_job_count = len(dummy["jobs"])
                if new_job_count == old_job_count:
                    dummy["stability_count"] += 1

                return dummy["stability_count"] > 5
            except:
                return False

        wait(all_jobs_running)

        tentative_job_count = 0
        for job_id in dummy["jobs"]:
            job_node = get("{0}/{1}".format(jobs_path, job_id))["address"]
            if job_node in other_nodes:
                tentative_job_count += 1

        assert tentative_job_count == TestTentativePoolTrees.TENTATIVE_TREE_ELIGIBILITY_SAMPLE_JOB_COUNT

        # Check that tentative tree saturated and we have proper deactivation reasons about that.
        orchid_other_operations_path = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/other/fair_share_info/operations"
        assert get("{}/{}/deactivation_reasons/saturated_in_tentative_tree".format(orchid_other_operations_path, op.id)) > 0

    @authors("ignat")
    def test_tentative_pool_tree_not_supported(self):
        self._prepare_pool_trees()
        spec = self._create_spec()

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": i} for i in xrange(30)])
        create("table", "//tmp/t_out")

        events = events_on_fs()

        op2 = map_reduce(
            mapper_command=events.wait_event_cmd("continue_job_${YT_JOB_ID}"),
            reducer_command=events.wait_event_cmd("continue_job_${YT_JOB_ID}"),
            sort_by=["x"],
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec=spec,
            dont_track=True)

        op1 = map(
            command=events.wait_event_cmd("continue_job_${YT_JOB_ID}"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec=spec,
            dont_track=True)


        op1_pool_trees_path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/".format(op1.id)
        op2_pool_trees_path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/".format(op2.id)

        wait(lambda: exists(op1_pool_trees_path + "default"))
        wait(lambda: exists(op1_pool_trees_path + "other"))
        wait(lambda: exists(op2_pool_trees_path + "default"))
        wait(lambda: not exists(op2_pool_trees_path + "other"))

    @authors("ignat")
    def test_tentative_pool_tree_banning(self):
        other_node_list = self._prepare_pool_trees()
        other_nodes = frozenset(other_node_list)

        spec = self._create_spec()

        set("//sys/controller_agents/config", {"check_tentative_tree_eligibility_period": 500})

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": i} for i in xrange(30)])
        create("table", "//tmp/t_out")

        events = events_on_fs()

        op = map(
            command=events.wait_event_cmd("continue_job_${YT_JOB_ID}"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec=spec,
            dont_track=True)

        op_pool_trees_path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/".format(op.id)

        def operations_failed_or_aborted():
            return op.get_state() in ["failed", "aborted"]

        def has_all_tentative_jobs():
            assert not operations_failed_or_aborted()
            tentative_job_count = 0
            for job_id, tentative in self._iter_running_jobs(op, other_nodes):
                if tentative:
                    tentative_job_count += 1
            return tentative_job_count == TestTentativePoolTrees.TENTATIVE_TREE_ELIGIBILITY_SAMPLE_JOB_COUNT

        wait(has_all_tentative_jobs)

        # Sleep to make job durations long enonugh.
        time.sleep(5)

        wait(lambda: exists(op_pool_trees_path + "other"))

        def complete_non_tentative_jobs(context):
            assert not operations_failed_or_aborted()
            for job_id, tentative in self._iter_running_jobs(op, other_nodes):
                if not tentative and job_id not in context["completed_jobs"] and len(context["completed_jobs"]) < 20:
                    context["completed_jobs"].add(job_id)
                    events.notify_event("continue_job_{0}".format(job_id))
            return len(context["completed_jobs"]) == 20

        # We have 30 jobs overall, 5 should be tentative, 20 regular jobs we complete fast. It must be enough to ban tentative tree.
        context = {"completed_jobs": __builtin__.set()}
        wait(lambda: complete_non_tentative_jobs(context))

        wait(lambda: op.get_job_count("completed") == 20)

        op_pool_trees_path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/".format(op.id)
        wait(lambda: not exists(op_pool_trees_path + "other"))
        assert exists(op_pool_trees_path + "default")

    @authors("ignat")
    def test_missing_tentative_pool_trees(self):
        self._prepare_pool_trees()
        spec = self._create_spec()

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": i} for i in xrange(7)])
        create("table", "//tmp/t_out")

        spec["tentative_pool_trees"] = ["missing"]
        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec=spec)

        spec["tentative_tree_eligibility"]["ignore_missing_pool_trees"] = True
        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec=spec)

    @authors("ignat")
    def test_tentative_pool_tree_aborted_jobs(self):
        other_node_list = self._prepare_pool_trees()
        other_nodes = frozenset(other_node_list)

        spec = self._create_spec()

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": i} for i in xrange(30)])
        create("table", "//tmp/t_out")

        events = events_on_fs()

        op = map(
            command=events.wait_event_cmd("continue_job_${YT_JOB_ID}"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec=spec,
            dont_track=True)

        job_aborted = False
        for iter in xrange(20):
            time.sleep(0.5)

            for job_id, tentative in self._iter_running_jobs(op, other_nodes):
                if tentative:
                    try:
                        abort_job(job_id)
                        job_aborted = True
                        break
                    # Job can be published by controller agent but still be missing in scheduler.
                    except YtError:
                        pass

            if job_aborted:
                break

        assert job_aborted

        tentative_job_count = 0
        while tentative_job_count + 1 < TestTentativePoolTrees.TENTATIVE_TREE_ELIGIBILITY_SAMPLE_JOB_COUNT:
            time.sleep(0.5)
            for job_id, tentative in self._iter_running_jobs(op, other_nodes):
                if tentative:
                    events.notify_event("continue_job_{0}".format(job_id))
                    tentative_job_count += 1

    @authors("ignat")
    def test_use_default_tentative_pool_trees(self):
        other_node_list = self._prepare_pool_trees()

        set("//sys/scheduler/config/default_tentative_pool_trees", ["other"], recursive=True)
        wait(lambda: exists("//sys/scheduler/orchid/scheduler/config/default_tentative_pool_trees"))

        try:
            spec = self._create_spec()
            spec["use_default_tentative_pool_trees"] = True

            create("table", "//tmp/t_in")
            write_table("//tmp/t_in", [{"x": i} for i in xrange(30)])
            create("table", "//tmp/t_out")

            events = events_on_fs()
            op = map(
                command=events.wait_event_cmd("continue_job_${YT_JOB_ID}"),
                in_="//tmp/t_in",
                out="//tmp/t_out",
                spec=spec,
                dont_track=True)

            op_pool_trees_path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/".format(op.id)
            wait(lambda: exists(op_pool_trees_path + "other"))
            assert get(op_pool_trees_path + "other/tentative")
        finally:
            remove("//sys/scheduler/config/default_tentative_pool_trees")

class TestSchedulingTagFilterOnPerPoolTreeConfiguration(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "spec_template": {
                "scheduling_options_per_pool_tree": {
                    "default": {"scheduling_tag_filter": "default_tag"},
                    "custom_pool_tree": {"scheduling_tag_filter": "runnable_tag"}
                }
            }
        }
    }

    @authors("renadeen")
    def test_scheduling_tag_filter_applies_from_per_pool_tree_config(self):
        all_nodes = ls("//sys/cluster_nodes")
        default_node = all_nodes[0]
        custom_node = all_nodes[1]
        runnable_custom_node = all_nodes[2]
        set("//sys/cluster_nodes/" + default_node + "/@user_tags/end", "default_tag")
        set("//sys/cluster_nodes/" + custom_node + "/@user_tags/end", "custom_tag")
        set("//sys/cluster_nodes/" + runnable_custom_node + "/@user_tags", ["custom_tag", "runnable_tag"])

        set("//sys/pool_trees/default/@nodes_filter", "default_tag")
        create("map_node", "//sys/pool_trees/custom_pool_tree", attributes={"nodes_filter": "custom_tag"})

        time.sleep(0.5)

        create_test_tables()

        op = map(
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"pool_trees": ["custom_pool_tree"]},
            dont_track=True)

        wait_breakpoint()

        jobs = op.get_running_jobs()
        assert len(jobs) == 1
        assert jobs[jobs.keys()[0]]["address"] == runnable_custom_node

        release_breakpoint()

    def teardown_method(self, method):
        remove("//sys/pool_trees/custom_pool_tree")
        super(TestSchedulingTagFilterOnPerPoolTreeConfiguration, self).teardown_method(method)

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
        create("map_node", "//sys/pools/parent")
        wait(lambda: "parent" in get(scheduler_orchid_default_pool_tree_path() + "/pools"))
        set("//sys/pools/parent/@infer_children_weights_from_historic_usage", True)
        set("//sys/pools/parent/@historic_usage_config", {
            "aggregation_mode": "exponential_moving_average",
            "ema_alpha": 1
        })
        wait(lambda: get("//sys/pools/parent/@infer_children_weights_from_historic_usage"))
        wait(lambda: "historic_usage_config" in get("//sys/pools/parent/@"))

    def _init_children(self, num_children=2):
        for i in xrange(num_children):
            create("map_node", "//sys/pools/parent/child" + str(i + 1))
            wait(lambda: ("child" + str(i + 1)) in get(scheduler_orchid_default_pool_tree_path() + "/pools"))

    def _get_pool_fair_share_ratio(self, pool):
        try:
            return get(scheduler_orchid_default_pool_tree_path() + "/pools/{}/fair_share_ratio".format(pool))
        except YtError:
            return defaultdict(lambda: 0.0)

    def _get_pool_usage_ratio(self, pool):
        try:
            return get(scheduler_orchid_default_pool_tree_path() + "/pools/{}/usage_ratio".format(pool))
        except YtError:
            return defaultdict(lambda: 0.0)

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
            dont_track=True)

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

        with ProfileMetric.at_scheduler("scheduler/pools/fair_share_ratio_x100000").with_tag("pool", "child2") as fair_share_ratio:
            op2 = vanilla(
                spec={
                    "pool": "child2",
                    "tasks": op2_tasks_spec
                },
                dont_track=True)

        # it's hard to estimate historic usage for all children, because run time can vary and jobs
        # can spuriously abort and restart; so we don't set the threshold any greater than 0.5
        wait(lambda: fair_share_ratio.update() and fair_share_ratio.update().max(verbose=True) > 0.5 * 100000)

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
            dont_track=True)

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

        with ProfileMetric.at_scheduler("scheduler/pools/fair_share_ratio_x100000").with_tag("pool", "child2") as fair_share_ratio:
            op2 = vanilla(
                spec={
                    "pool": "child2",
                    "tasks": op2_tasks_spec
                },
                dont_track=True)

        wait(lambda: fair_share_ratio.update() and fair_share_ratio.update().max(verbose=True) in (49999, 50000, 50001))

        op1.complete()
        op2.complete()

    @authors("eshcherbin")
    def test_equal_fair_share_after_disabling_historic_usage(self):
        self._test_equal_fair_share_after_disabling_config_change_base({
            "infer_children_weights_from_historic_usage": False,
            "historic_usage_config": {
                "aggregation_mode": "none",
                "ema_alpha": 0
            }
        })

    @authors("eshcherbin")
    def test_equal_fair_share_after_disabling_historic_usage_but_keeping_parameters(self):
        self._test_equal_fair_share_after_disabling_config_change_base({
            "infer_children_weights_from_historic_usage": False,
            "historic_usage_config": {
                "aggregation_mode": "exponential_moving_average",
                "ema_alpha": 1
            }
        })

    @authors("eshcherbin")
    def test_equal_fair_share_after_setting_none_mode(self):
        self._test_equal_fair_share_after_disabling_config_change_base({
            "infer_children_weights_from_historic_usage": True,
            "historic_usage_config": {
                "aggregation_mode": "none",
                "ema_alpha": 1
            }
        })

    @authors("eshcherbin")
    def test_equal_fair_share_after_setting_zero_alpha(self):
        self._test_equal_fair_share_after_disabling_config_change_base({
            "infer_children_weights_from_historic_usage": True,
            "historic_usage_config": {
                "aggregation_mode": "exponential_moving_average",
                "ema_alpha": 0
            }
        })
