import pytest

from yt_env_setup import YTEnvSetup, require_ytserver_root_privileges, wait
from yt.test_helpers import are_almost_equal
from yt_commands import *

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
    NUM_MASTERS = 3
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

        get_pool_guaranteed_resources = lambda pool: \
            get("//sys/scheduler/orchid/scheduler/pools/{0}/guaranteed_resources".format(pool))

        get_pool_guaranteed_resources_ratio = lambda pool: \
            get("//sys/scheduler/orchid/scheduler/pools/{0}/guaranteed_resources_ratio".format(pool))

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


    def test_resource_limits(self):
        resource_limits = {"cpu": 1, "memory": 1000 * 1024 * 1024, "network": 10}
        create("map_node", "//sys/pools/test_pool", attributes={"resource_limits": resource_limits})

        wait(lambda: "test_pool" in get("//sys/scheduler/orchid/scheduler/pools"))

        stats = get("//sys/scheduler/orchid/scheduler")
        pool_resource_limits = stats["pools"]["test_pool"]["resource_limits"]
        for resource, limit in resource_limits.iteritems():
            resource_name = "user_memory" if resource == "memory" else resource
            assert are_almost_equal(pool_resource_limits[resource_name], limit)

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

    def test_resource_limits_preemption(self):
        create("map_node", "//sys/pools/test_pool2")
        wait(lambda: "test_pool2" in get("//sys/scheduler/orchid/scheduler/pools"))

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

        wait(lambda: are_almost_equal(get("//sys/scheduler/orchid/scheduler/pools/test_pool2/resource_limits/cpu"), 2))

        wait(lambda: len(op.get_running_jobs()) == 2)

    # Remove flaky after YT-8784.
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

        set("//sys/operations/{0}/@resource_limits".format(op.id), {"user_slots": 2})
        self._check_running_jobs(op, 2)

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
            get("//sys/scheduler/orchid/scheduler/pools/{0}/fair_share_ratio".format(pool))

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

        assert are_almost_equal(get_pool_fair_share_ratio("subpool_1"), 1.0 / 3.0)
        assert are_almost_equal(get_pool_fair_share_ratio("low_cpu_pool"), 1.0 / 3.0)
        assert are_almost_equal(get_pool_fair_share_ratio("high_cpu_pool"), 2.0 / 3.0)

        op3 = map(
            dont_track=True,
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out_3",
            spec={"job_count": 1, "pool": "subpool_2", "mapper": {"cpu_limit": 0}})

        time.sleep(1)

        assert are_almost_equal(get_pool_fair_share_ratio("low_cpu_pool"), 1.0 / 2.0)
        assert are_almost_equal(get_pool_fair_share_ratio("high_cpu_pool"), 1.0 / 2.0)

        release_breakpoint()
        op1.track()
        op2.track()
        op3.track()

    def test_recursive_fair_share_when_lack_of_resources(self):
        create("map_node", "//sys/pools/parent_pool", attributes={"min_share_ratio": 0.1})
        create("map_node", "//sys/pools/parent_pool/subpool1", attributes={"min_share_ratio": 1})
        create("map_node", "//sys/pools/parent_pool/subpool2", attributes={"min_share_ratio": 1})

        time.sleep(0.2)
        ratio_path = "//sys/scheduler/orchid/scheduler/pools/{0}/recursive_min_share_ratio"

        assert get(ratio_path.format("subpool1")) == 0.05
        assert get(ratio_path.format("subpool2")) == 0.05

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
        assert are_almost_equal(resource_usage["cpu"], 3 * 0.87)

        release_breakpoint()
        op.track()

##################################################################

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
        chunk_ids = get(table + "/@chunk_ids")
        chunk_id = chunk_ids[0]
        replicas = get("#{0}/@stored_replicas".format(chunk_id))
        assert len(replicas) == 1

        return replicas[0]

    def test_strategies(self):
        self._prepare_tables()

        node = self._get_table_chunk_node("//tmp/t_in")
        set_banned_flag(True, [node])

        print >>sys.stderr,  "Fail strategy"
        with pytest.raises(YtError):
            map(in_="//tmp/t_in", out="//tmp/t_out", command="cat", spec={"unavailable_chunk_strategy": "fail"})

        print >>sys.stderr,  "Skip strategy"
        map(in_="//tmp/t_in", out="//tmp/t_out", command="cat", spec={"unavailable_chunk_strategy": "skip"})
        assert read_table("//tmp/t_out") == []

        print >>sys.stderr,  "Wait strategy"
        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat",  spec={"unavailable_chunk_strategy": "wait"})

        set_banned_flag(False, [node])
        op.track()

        assert read_table("//tmp/t_out") == [{"foo": "bar"}]

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

        print >>sys.stderr, "Fail strategy"
        with pytest.raises(YtError):
            sort(in_="//tmp/t_in", out="//tmp/t_out", sort_by="key", spec={"unavailable_chunk_strategy": "fail"})

        print >>sys.stderr, "Skip strategy"
        sort(in_="//tmp/t_in", out="//tmp/t_out", sort_by="key", spec={"unavailable_chunk_strategy": "skip"})
        assert read_table("//tmp/t_out") == []

        print >>sys.stderr, "Wait strategy"
        op = sort(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", sort_by="key", spec={"unavailable_chunk_strategy": "wait"})

        # Give a chance to scraper to work
        time.sleep(1.0)
        set_banned_flag(False)
        op.track()

        assert read_table("//tmp/t_out") == [v1, v2, v3, v4, v5]
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["key"]

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

        print >>sys.stderr, "Fail strategy"
        with pytest.raises(YtError):
            merge(mode="sorted", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t_out", spec={"unavailable_chunk_strategy": "fail"})

        print >>sys.stderr, "Skip strategy"
        merge(mode="sorted", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t_out", spec={"unavailable_chunk_strategy": "skip"})
        assert read_table("//tmp/t_out") == []

        print >>sys.stderr, "Wait strategy"
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
    NUM_MASTERS = 3
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

    def test_operations_pool_limit(self):
        create("map_node", "//sys/pools/test_pool_1")
        create("map_node", "//sys/pools/test_pool_2")
        self._run_operations()

    def test_operations_recursive_pool_limit(self):
        create("map_node", "//sys/pools/research")
        set("//sys/pools/research/@max_running_operation_count", 2)
        create("map_node", "//sys/pools/research/test_pool_1")
        create("map_node", "//sys/pools/research/test_pool_2")
        self._run_operations()

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

        wait(lambda: get("//sys/scheduler/orchid/scheduler/pools/research/operation_count") == 3)
        wait(lambda: get("//sys/scheduler/orchid/scheduler/pools/research/running_operation_count") == 3)

    def test_pending_operations_after_revive(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        data = [{"foo": i} for i in xrange(5)]
        write_table("//tmp/in", data)

        op1 = map(dont_track=True, command="sleep 5.0; cat", in_=["//tmp/in"], out="//tmp/out1")
        op2 = map(dont_track=True, command="cat", in_=["//tmp/in"], out="//tmp/out2")

        time.sleep(1.5)

        self.Env.kill_schedulers()
        self.Env.start_schedulers()

        op1.track()
        op2.track()

        assert sorted(read_table("//tmp/out1")) == sorted(data)
        assert sorted(read_table("//tmp/out2")) == sorted(data)

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

        self.Env.kill_schedulers()
        self.Env.start_schedulers()

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

        wait(lambda: get("//sys/scheduler/orchid/scheduler/pools/subpool/running_operation_count") == 1)
        wait(lambda: get("//sys/scheduler/orchid/scheduler/pools/subpool/operation_count") == 3)

        wait(lambda: get("//sys/scheduler/orchid/scheduler/pools/research/running_operation_count") == 1)
        wait(lambda: get("//sys/scheduler/orchid/scheduler/pools/research/operation_count") == 3)

        assert get("//sys/scheduler/orchid/scheduler/pools/production/running_operation_count") == 0
        assert get("//sys/scheduler/orchid/scheduler/pools/production/operation_count") == 0

        move("//sys/pools/research/subpool", "//sys/pools/production/subpool")

        time.sleep(0.5)

        assert get("//sys/scheduler/orchid/scheduler/pools/subpool/running_operation_count") == 1
        assert get("//sys/scheduler/orchid/scheduler/pools/subpool/operation_count") == 3

        wait(lambda: get("//sys/scheduler/orchid/scheduler/pools/research/running_operation_count") == 0)
        wait(lambda: get("//sys/scheduler/orchid/scheduler/pools/research/operation_count") == 0)

        wait(lambda: get("//sys/scheduler/orchid/scheduler/pools/production/running_operation_count") == 1)
        wait(lambda: get("//sys/scheduler/orchid/scheduler/pools/production/operation_count") == 3)

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

    def test_global_pool_acl(self):
        self._test_pool_acl_prologue()
        create("map_node", "//sys/pools/p", attributes={
            "inherit_acl": False,
            "acl": [make_ace("allow", "u", "use")]
        })
        self._test_pool_acl_core("p", "/p")

    def test_inner_pool_acl(self):
        self._test_pool_acl_prologue()
        create("map_node", "//sys/pools/p1", attributes={
            "inherit_acl": False,
            "acl": [make_ace("allow", "u", "use")]
        })
        create("map_node", "//sys/pools/p1/p2")
        self._test_pool_acl_core("p2", "/p1")

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

    def test_preemption(self):
        create("table", "//tmp/t_in")
        for i in xrange(3):
            write_table("<append=true>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        op1 = map(dont_track=True, command="sleep 1000; cat", in_=["//tmp/t_in"], out="//tmp/t_out1",
                  spec={"pool": "fake_pool", "job_count": 3, "locality_timeout": 0})
        time.sleep(3)

        assert get("//sys/scheduler/orchid/scheduler/pools/fake_pool/fair_share_ratio") >= 0.999
        assert get("//sys/scheduler/orchid/scheduler/pools/fake_pool/usage_ratio") >= 0.999

        create("map_node", "//sys/pools/test_pool", attributes={"min_share_ratio": 1.0})
        op2 = map(dont_track=True, command="cat", in_=["//tmp/t_in"], out="//tmp/t_out2", spec={"pool": "test_pool"})
        op2.track()

        op1.abort()

    @pytest.mark.parametrize("interruptible", [False, True])
    def test_interrupt_job_on_preemption(self, interruptible):
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
        }
        if interruptible:
            data_size_per_job = get("//tmp/t_in/@uncompressed_data_size")
            spec["data_size_per_job"] = data_size_per_job / 3 + 1
        else:
            spec["job_count"] = 3

        mapper = " ; ".join([
            events_on_fs().notify_event_cmd("mapper_started_$YT_JOB_INDEX"),
            "sleep 7",
            "cat"])
        op1 = map(
            dont_track=True,
            command=mapper,
            in_=["//tmp/t_in"],
            out="//tmp/t_out1",
            spec=spec)

        time.sleep(3)

        assert get("//sys/scheduler/orchid/scheduler/pools/fake_pool/fair_share_ratio") >= 0.999
        assert get("//sys/scheduler/orchid/scheduler/pools/fake_pool/usage_ratio") >= 0.999

        create("map_node", "//sys/pools/test_pool", attributes={"min_share_ratio": 1.0})

        # Ensure that all three jobs have started.
        events_on_fs().wait_event("mapper_started_0", timeout=datetime.timedelta(1000))
        events_on_fs().wait_event("mapper_started_1", timeout=datetime.timedelta(1000))
        events_on_fs().wait_event("mapper_started_2", timeout=datetime.timedelta(1000))

        op2 = map(
            dont_track=True,
            command="cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out2",
            spec={"pool": "test_pool"})
        op2.track()
        op1.track()
        assert get("//sys/operations/" + op1.id + "/@progress/jobs/completed/total") == (4 if interruptible else 3)

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

            # Wait for fair share update.
            time.sleep(0.2)

            assert get_operation_min_share_ratio(op.id) == compute_min_share_ratio(min_share_spec)

            release_breakpoint()
            op.track()

    def test_recursive_preemption_settings(self):
        create("map_node", "//sys/pools/p1", attributes={"fair_share_starvation_tolerance_limit": 0.6})
        create("map_node", "//sys/pools/p1/p2")
        create("map_node", "//sys/pools/p1/p3", attributes={"fair_share_starvation_tolerance": 0.5})
        create("map_node", "//sys/pools/p1/p4", attributes={"fair_share_starvation_tolerance": 0.9})
        create("map_node", "//sys/pools/p5", attributes={"fair_share_starvation_tolerance": 0.8})
        create("map_node", "//sys/pools/p5/p6")
        time.sleep(1)

        get_pool_tolerance = lambda pool: \
            get("//sys/scheduler/orchid/scheduler/pools/{0}/adjusted_fair_share_starvation_tolerance".format(pool))

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

        assert "unschedulable" in str(get(get_operation_cypress_path(op.id) + "/@result"))


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
        time.sleep(0.5)

    @classmethod
    def modify_node_config(cls, config):
        for resource in ["cpu", "user_slots"]:
            config["exec_agent"]["job_controller"]["resource_limits"][resource] = 2

    def test_aggressive_preemption(self):
        create("table", "//tmp/t_in")
        for i in xrange(3):
            write_table("<append=true>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")

        create("map_node", "//sys/pools/special_pool")
        set("//sys/pools/special_pool/@aggressive_starvation_enabled", True)

        get_fair_share_ratio = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/fair_share_ratio".format(op_id))

        get_usage_ratio = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/usage_ratio".format(op_id))

        ops = []
        for index in xrange(2):
            create("table", "//tmp/t_out" + str(index))
            op = map(dont_track=True, command="sleep 1000; cat", in_=["//tmp/t_in"], out="//tmp/t_out" + str(index),
                    spec={"pool": "fake_pool" + str(index), "job_count": 3, "locality_timeout": 0, "mapper": {"memory_limit": 10 * 1024 * 1024}})
            ops.append(op)
        time.sleep(3)

        for op in ops:
            assert are_almost_equal(get_fair_share_ratio(op.id), 1.0 / 2.0)
            assert are_almost_equal(get_usage_ratio(op.id), 1.0 / 2.0)
            assert len(op.get_running_jobs()) == 3

        op = map(dont_track=True, command="sleep 1000; cat", in_=["//tmp/t_in"], out="//tmp/t_out",
                 spec={"pool": "special_pool", "job_count": 1, "locality_timeout": 0, "mapper": {"cpu_limit": 2}})
        time.sleep(3)

        assert are_almost_equal(get_fair_share_ratio(op.id), 1.0 / 3.0)
        assert are_almost_equal(get_usage_ratio(op.id), 1.0 / 3.0)
        assert len(op.get_running_jobs()) == 1

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

    def test_allow_aggressive_starvation_preemption(self):
        create("table", "//tmp/t_in")
        for i in xrange(3):
            write_table("<append=true>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")

        create("map_node", "//sys/pools/special_pool")
        set("//sys/pools/special_pool/@aggressive_starvation_enabled", True)

        for index in xrange(4):
            create("map_node", "//sys/pools/pool" + str(index))

        set("//sys/pools/pool0/@allow_aggressive_starvation_preemption", False)

        get_fair_share_ratio = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/fair_share_ratio".format(op_id))

        get_usage_ratio = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/usage_ratio".format(op_id))

        ops = []
        for index in xrange(4):
            create("table", "//tmp/t_out" + str(index))
            op = map(
                command="sleep 1000; cat",
                in_=["//tmp/t_in"],
                out="//tmp/t_out" + str(index),
                spec={
                    "pool": "pool" + str(index),
                    "job_count": 3,
                    "locality_timeout": 0,
                    "mapper": {"memory_limit": 10 * 1024 * 1024}
                },
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
    NUM_MASTERS = 3
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

    def setup_method(self, method):
        super(TestSchedulerPools, self).setup_method(method)
        set("//sys/pool_trees/default/@max_ephemeral_pools_per_user", 3)
        set("//sys/pool_trees/default/@default_parent_pool", "default_pool")
        time.sleep(0.5)

    def _prepare(self):
        create("table", "//tmp/t_in")
        set("//tmp/t_in/@replication_factor", 1)
        write_table("//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")
        set("//tmp/t_out/@replication_factor", 1)

    def test_pools_reconfiguration(self):
        self._prepare()

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

    def test_default_parent_pool(self):
        create("table", "//tmp/t_in")
        set("//tmp/t_in/@replication_factor", 1)
        write_table("//tmp/t_in", {"foo": "bar"})

        for output in ["//tmp/t_out1", "//tmp/t_out2"]:
            create("table", output)
            set(output + "/@replication_factor", 1)

        create("map_node", "//sys/pools/default_pool")
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

        pool = get("//sys/scheduler/orchid/scheduler/pools/root")
        assert pool["parent"] == "default_pool"

        pool = get("//sys/scheduler/orchid/scheduler/pools/my_pool")
        assert pool["parent"] == "default_pool"

        scheduling_info_per_pool_tree = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"
        assert __builtin__.set(["root", "my_pool"]) == \
               __builtin__.set(get(scheduling_info_per_pool_tree + "/default/user_to_ephemeral_pools/root"))

        remove("//sys/pools/default_pool")

        release_breakpoint()
        for op in [op1, op2]:
            op.track()

    def test_ephemeral_pool_in_custom_pool(self):
        create("map_node", "//sys/pools/custom_pool")
        create("map_node", "//sys/pools/custom_pool_fifo")
        set("//sys/pools/custom_pool/@create_ephemeral_subpools", True)
        set("//sys/pools/custom_pool_fifo/@create_ephemeral_subpools", True)
        set("//sys/pools/custom_pool_fifo/@ephemeral_subpools_mode", "fifo")

        time.sleep(0.2)

        op1 = self.run_vanilla_with_sleep(spec={"pool": "custom_pool"})
        op2 = self.run_vanilla_with_sleep(spec={"pool": "custom_pool_fifo"})
        wait(lambda: len(list(op1.get_running_jobs())) == 1)
        wait(lambda: len(list(op2.get_running_jobs())) == 1)

        pools_path = "//sys/scheduler/orchid/scheduler/pools/"

        pool = get(pools_path + "custom_pool$root")
        assert pool["parent"] == "custom_pool"
        assert pool["mode"] == "fair_share"

        pool_fifo = get(pools_path + "custom_pool_fifo$root")
        assert pool_fifo["parent"] == "custom_pool_fifo"
        assert pool_fifo["mode"] == "fifo"

        remove("//sys/pools/custom_pool")
        remove("//sys/pools/custom_pool_fifo")

    def test_ephemeral_pools_limit(self):
        create("table", "//tmp/t_in")
        set("//tmp/t_in/@replication_factor", 1)
        write_table("//tmp/t_in", {"foo": "bar"})

        for i in xrange(1, 5):
            output = "//tmp/t_out" + str(i)
            create("table", output)
            set(output + "/@replication_factor", 1)

        create("map_node", "//sys/pools/default_pool")
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

        for breakpoint_name in breakpoints:
            release_breakpoint(breakpoint_name)

        for op in ops:
            op.track()

    def test_event_log(self):
        self._prepare()

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

    def run_vanilla_with_sleep(self, spec):
        spec["tasks"] = {
            "task": {
                "job_count": 1,
                "command": "sleep 1000"
            },
        }
        return vanilla(spec=spec, dont_track=True)

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
                        "memory",
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
            print >>sys.stderr, "running_jobs1:", len(running_jobs1), "running_jobs2:", len(running_jobs2)
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
            print >>sys.stderr, "Some of jobs considered suspicious, their brief statistics are:"
            for i in range(50):
                if suspicious1 and exists("//sys/scheduler/orchid/scheduler/jobs/{0}/brief_statistics".format(job1_id)):
                    print >>sys.stderr, "job1 brief statistics:", \
                        get("//sys/scheduler/orchid/scheduler/jobs/{0}/brief_statistics".format(job1_id))
                if suspicious2 and exists("//sys/scheduler/orchid/scheduler/jobs/{0}/brief_statistics".format(job2_id)):
                    print >>sys.stderr, "job2 brief statistics:", \
                        get("//sys/scheduler/orchid/scheduler/jobs/{0}/brief_statistics".format(job2_id))

        assert not suspicious1
        assert not suspicious2

        op1.abort()
        op2.abort()

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
            print >>sys.stderr, "running_jobs:", len(running_jobs)
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
            print >>sys.stderr, "Job is not considered suspicious, its brief statistics are:"
            for i in range(50):
                if suspicious and exists("//sys/scheduler/orchid/scheduler/jobs/{0}/brief_statistics".format(job_id)):
                    print >>sys.stderr, "job brief statistics:", \
                        get("//sys/scheduler/orchid/scheduler/jobs/{0}/brief_statistics".format(job_id))

        assert suspicious

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

        nodes = ls("//sys/nodes")
        assert len(nodes) == 1
        node = nodes[0]
        set("//sys/nodes/{0}/@resource_limits_overrides".format(node), {"cpu": 0})

        op = map(
            dont_track=True,
            command='cat',
            in_="//tmp/t",
            out="//tmp/d")

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        chunk_store_path = self.Env.configs["node"][0]["data_node"]["store_locations"][0]["path"]
        chunk_path = os.path.join(chunk_store_path, chunk_id[-2:], chunk_id)
        os.remove(chunk_path)
        os.remove(chunk_path + ".meta")

        set("//sys/nodes/{0}/@resource_limits_overrides".format(node), {"cpu": 1})

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
                print >>sys.stderr, get("//sys/scheduler/orchid/scheduler/jobs/{0}/brief_statistics".format(job_id))

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

class TestFairShareTreesReconfiguration(YTEnvSetup):
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
        for node in ls("//sys/nodes"):
            set("//sys/nodes/{}/@resource_limits_overrides".format(node), {})
        remove("//sys/pool_trees/*")
        create("map_node", "//sys/pool_trees/default")
        set("//sys/pool_trees/@default_tree", "default")
        time.sleep(0.5)  # Give scheduler some time to reload trees
        super(TestFairShareTreesReconfiguration, self).teardown_method(method)

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

    def test_abort_many_orphaned_operations(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": 1}])

        node = ls("//sys/nodes")[0]
        set("//sys/nodes/{}/@resource_limits_overrides".format(node), {"cpu": 10, "user_slots": 10})

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

        self.Env.kill_schedulers()
        time.sleep(0.5)
        self.Env.start_schedulers()

        wait(lambda: op.get_state() == "running")
        op.track()

    def test_incorrect_node_tags(self):
        create("map_node", "//sys/pool_trees/supertree1", attributes={"nodes_filter": "x|y"})
        create("map_node", "//sys/pool_trees/supertree2", attributes={"nodes_filter": "y|z"})
        wait(lambda: "supertree1" in ls("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"))
        wait(lambda: "supertree2" in ls("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree"))

        node = ls("//sys/nodes")[0]
        assert get("//sys/scheduler/orchid/scheduler/nodes/" + node + "/state") == "online"
        assert get("//sys/scheduler/orchid/scheduler/cell/resource_limits/user_slots") == 3

        set("//sys/nodes/" + node + "/@user_tags/end", "y")

        wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/" + node + "/state") == "offline")
        assert get("//sys/scheduler/orchid/scheduler/cell/resource_limits/user_slots") == 2

    def test_default_tree_manipulations(self):
        assert get("//sys/pool_trees/@default_tree") == "default"
        assert exists("//sys/scheduler/orchid/scheduler/pools")

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

        assert not exists("//sys/scheduler/orchid/scheduler/pools")

        set("//sys/pool_trees/@default_tree", "unexisting")
        wait(lambda: get("//sys/scheduler/@alerts"))
        wait(lambda: not exists("//sys/scheduler/orchid/scheduler/default_fair_share_tree"))

        set("//sys/pool_trees/@default_tree", "default")
        wait(lambda: exists("//sys/scheduler/orchid/scheduler/default_fair_share_tree"))
        assert get("//sys/scheduler/orchid/scheduler/default_fair_share_tree") == "default"
        assert exists("//sys/scheduler/orchid/scheduler/pools")
        assert exists("//sys/scheduler/orchid/scheduler/fair_share_info")

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
        node = ls("//sys/nodes")[0]
        set("//sys/nodes/" + node + "/@user_tags/end", tag)
        create("map_node", "//sys/pool_trees/" + pool_tree, attributes={"nodes_filter": tag})
        set("//sys/pool_trees/default/@nodes_filter", "!" + tag)
        return node

class TestSchedulingOptionsPerTree(YTEnvSetup):
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
        remove("//sys/pool_trees/other")
        super(TestSchedulingOptionsPerTree, self).teardown_method(method)

    # Creates and additional pool tree called "other", configures tag filters,
    # tags some nodes as "other" and returns a list of those nodes.
    def _prepare_pool_trees(self):
        other_nodes = ls("//sys/nodes")[:3]
        for node in other_nodes:
            set("//sys/nodes/" + node + "/@user_tags/end", "other")

        set("//sys/pool_trees/default/@nodes_filter", "!other")
        create("map_node", "//sys/pool_trees/other", attributes={"nodes_filter": "other"})
        set("//sys/pool_trees/default/@nodes_filter", "!other")
        time.sleep(0.5)

        return other_nodes

    def _create_spec(self):
        return {
            "pool_trees": ["default", "other"],
            "scheduling_options_per_pool_tree": {
                "default": {
                    "max_share_ratio": 0.4,
                    "min_share_ratio": 0.37
                },
                "other": {
                    "max_share_ratio": 2./3,
                    "pool": "superpool"
                }
            },
            "max_share_ratio": 1./3,  # You had one job!
            "data_size_per_job": 1
        }

    def _patch_spec_for_tentativeness(self, spec):
        spec["pool_trees"].remove("other")
        spec["tentative_pool_trees"] = ["other"]
        spec["tentative_tree_eligibility"] = {
            "sample_job_count": TestSchedulingOptionsPerTree.TENTATIVE_TREE_ELIGIBILITY_SAMPLE_JOB_COUNT,
            "max_tentative_job_duration_ratio": TestSchedulingOptionsPerTree.MAX_TENTATIVE_TREE_JOB_DURATION_RATIO,
            "min_job_duration": TestSchedulingOptionsPerTree.TENTATIVE_TREE_ELIGIBILITY_MIN_JOB_DURATION,
        }

    @pytest.mark.xfail(run = True, reason = "asaitgalin should fix ratios")
    def test_scheduling_options_per_tree(self):
        self._prepare_pool_trees()
        spec = self._create_spec()

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": i} for i in xrange(7)])
        create("table", "//tmp/t_out")

        op = map(
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec=spec,
            dont_track=True)

        wait_breakpoint()

        def get_value(tree, op_id, value):
            return get("//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/{0}/fair_share_info/operations/{1}/{2}"
                       .format(tree, op_id, value))

        assert are_almost_equal(get_value("default", op.id, "min_share_ratio"), 0.37)
        assert are_almost_equal(get_value("default", op.id, "max_share_ratio"), 0.4)
        assert are_almost_equal(get_value("default", op.id, "fair_share_ratio"), 1./3)
        assert are_almost_equal(get_value("default", op.id, "usage_ratio"), 1./3)
        assert get_value("default", op.id, "pool") == "root"

        assert are_almost_equal(get_value("other", op.id, "max_share_ratio"), 2./3)
        assert are_almost_equal(get_value("other", op.id, "fair_share_ratio"), 2./3)
        assert are_almost_equal(get_value("other", op.id, "usage_ratio"), 2./3)
        assert are_almost_equal(get_value("other", op.id, "usage_ratio"), 2./3)
        assert get_value("other", op.id, "pool") == "superpool"
        assert get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/other/pool"
            .format(op.id)) == "superpool"

    def test_tentative_pool_tree_sampling(self):
        other_nodes = self._prepare_pool_trees()
        spec = self._create_spec()
        self._patch_spec_for_tentativeness(spec)

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

        assert tentative_job_count == TestSchedulingOptionsPerTree.TENTATIVE_TREE_ELIGIBILITY_SAMPLE_JOB_COUNT

    def test_tentative_pool_tree_not_supported(self):
        self._prepare_pool_trees()
        spec = self._create_spec()
        self._patch_spec_for_tentativeness(spec)

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

    def test_tentative_pool_tree_banning(self):
        other_node_list = self._prepare_pool_trees()
        spec = self._create_spec()
        self._patch_spec_for_tentativeness(spec)
        other_nodes = frozenset(other_node_list)

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

        jobs_path = op.get_path() + "/controller_orchid/running_jobs"

        def iter_running_jobs():
            try:
                jobs = ls(jobs_path)
            except:
                return # Operation completed.

            for job_id in jobs:
                try:
                    job_node = get("{0}/{1}".format(jobs_path, job_id))["address"]
                except YtError:
                    continue # The job has already completed, Orchid is lagging.

                job_is_tentative = job_node in other_nodes
                yield job_id, job_is_tentative

        def operation_completed():
            return get("//sys/operations/{0}/@state".format(op.id)) == "completed"

        def operations_failed_or_aborted():
            return get("//sys/operations/{0}/@state".format(op.id)) in ["failed", "aborted"]

        time.sleep(5)

        non_tentative_job_count = 0
        time_passed = 0
        completion_time = None
        while not operation_completed():
            time.sleep(0.5)
            time_passed += 0.5

            assert not operations_failed_or_aborted()

            if non_tentative_job_count >= TestSchedulingOptionsPerTree.TENTATIVE_TREE_ELIGIBILITY_SAMPLE_JOB_COUNT:
                completion_time = time_passed
                break

            for job_id, tentative in iter_running_jobs():
                if not tentative:
                    non_tentative_job_count += 1
                    events.notify_event("continue_job_{0}".format(job_id))

        wait(lambda: op.get_job_count("completed") >= 5)

        # Tentative jobs should now be "slow" enough, it's time to start completing them.
        while not operation_completed() and time_passed < 5.0 * completion_time:
            time.sleep(0.5)
            time_passed += 0.5

        op_pool_trees_path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/".format(op.id)
        tentative_job_count = 0
        while not operation_completed():
            time.sleep(0.5)

            assert not operations_failed_or_aborted()

            for job_id, tentative in iter_running_jobs():
                events.notify_event("continue_job_{0}".format(job_id))

                if tentative:
                    tentative_job_count += 1

                    if tentative_job_count == TestSchedulingOptionsPerTree.TENTATIVE_TREE_ELIGIBILITY_SAMPLE_JOB_COUNT:
                        time.sleep(0.3)
                        # Tentative tree should've been banned by now.
                        wait(lambda: not exists(op_pool_trees_path + "other"))
                        wait(lambda: exists(op_pool_trees_path + "default") or operation_completed())
                        break

        while not operation_completed():
            time.sleep(0.5)

            assert not operations_failed_or_aborted()

            for job_id, tentative in iter_running_jobs():
                events.notify_event("continue_job_{0}".format(job_id))

                if tentative:
                    tentative_job_count += 1


        assert tentative_job_count == TestSchedulingOptionsPerTree.TENTATIVE_TREE_ELIGIBILITY_SAMPLE_JOB_COUNT

    def test_missing_tentative_pool_trees(self):
        self._prepare_pool_trees()
        spec = self._create_spec()
        self._patch_spec_for_tentativeness(spec)

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": i} for i in xrange(7)])
        create("table", "//tmp/t_out")

        spec["tentative_pool_trees"] = ["missing"]
        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec=spec)

        spec["tentative_tree_eligibility"]["ignore_missing_pool_trees"] = True
        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec=spec)

    def test_tentative_pool_tree_aborted_jobs(self):
        other_node_list = self._prepare_pool_trees()
        spec = self._create_spec()
        self._patch_spec_for_tentativeness(spec)
        other_nodes = frozenset(other_node_list)

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

        jobs_path = op.get_path() + "/controller_orchid/running_jobs"

        def iter_running_jobs():
            try:
                jobs = ls(jobs_path)
            except:
                return # Operation completed.

            for job_id in jobs:
                try:
                    job_node = get("{0}/{1}".format(jobs_path, job_id))["address"]
                except YtError:
                    continue # The job has already completed, Orchid is lagging.

                job_is_tentative = job_node in other_nodes
                yield job_id, job_is_tentative

        def operation_completed():
            return get("//sys/operations/{0}/@state".format(op.id)) == "completed"

        job_aborted = False
        for iter in xrange(20):
            time.sleep(0.5)

            for job_id, tentative in iter_running_jobs():
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
        while tentative_job_count + 1 < TestSchedulingOptionsPerTree.TENTATIVE_TREE_ELIGIBILITY_SAMPLE_JOB_COUNT:
            time.sleep(0.5)
            for job_id, tentative in iter_running_jobs():
                if tentative:
                    events.notify_event("continue_job_{0}".format(job_id))
                    tentative_job_count += 1

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

    def test_scheduling_tag_filter_applies_from_per_pool_tree_config(self):
        all_nodes = ls("//sys/nodes")
        default_node = all_nodes[0]
        custom_node = all_nodes[1]
        runnable_custom_node = all_nodes[2]
        set("//sys/nodes/" + default_node + "/@user_tags/end", "default_tag")
        set("//sys/nodes/" + custom_node + "/@user_tags/end", "custom_tag")
        set("//sys/nodes/" + runnable_custom_node + "/@user_tags", ["custom_tag", "runnable_tag"])

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
