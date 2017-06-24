import pytest
from flaky import flaky

from yt_env_setup import YTEnvSetup, unix_only, require_ytserver_root_privileges, wait
from yt.environment.helpers import assert_almost_equal
from yt_commands import *

import string
import time
from datetime import datetime, timedelta
import __builtin__

import os

##################################################################

def set_banned_flag(value, nodes=None):
    if value:
        flag = True
        state = "offline"
    else:
        flag = False
        state = "online"

    if not nodes:
        nodes = get("//sys/nodes").keys()

    for address in nodes:
        set("//sys/nodes/{0}/@banned".format(address), flag)

    # Give it enough time to register or unregister the node
    time.sleep(1.0)

    for address in nodes:
        assert get("//sys/nodes/{0}/@state".format(address)) == state
        print >>sys.stderr, "Node {0} is {1}".format(address, state)

##################################################################

def get_pool_metrics(metric_key):
    result = {}
    for entry in reversed(get("//sys/scheduler/orchid/profiling/scheduler/pools/metrics/{0}".format(metric_key))):
        pool = entry["tags"]["pool"]
        if pool not in result:
            result[pool] = entry["value"]
    return result

def get_cypress_metrics(operation_id, key):
    statistics = get("//sys/operations/{0}/@progress/job_statistics".format(operation_id))
    return get_statistics(statistics, "{0}.$.completed.map.sum".format(key))

##################################################################

class PrepareTables(object):
    def _create_table(self, table):
        create("table", table)
        set(table + "/@replication_factor", 1)

    def _prepare_tables(self):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        self._create_table("//tmp/t_out")


class TestSchedulerFunctionality(YTEnvSetup, PrepareTables):
    NUM_MASTERS = 3
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operation_time_limit_check_period" : 100,
            "operation_fail_timeout": 3000,
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "profiling_update_period": 100,
            "fair_share_profiling_period": 100,
            "alerts_update_period": 100,
        }
    }

    def test_revive(self):
        def get_connection_time():
            return datetime.strptime(get("//sys/scheduler/@connection_time"), "%Y-%m-%dT%H:%M:%S.%fZ")

        self._prepare_tables()

        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat; sleep 4")

        time.sleep(3)

        assert datetime.utcnow() - get_connection_time() > timedelta(seconds=3)

        self.Env.kill_schedulers()
        self.Env.start_schedulers()

        assert datetime.utcnow() - get_connection_time() < timedelta(seconds=3)

        op.track()

        assert read_table("//tmp/t_out") == [ {"foo" : "bar"} ]

    def test_disconnect_during_revive(self):
        op_count = 20

        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        for i in xrange(1, op_count + 1):
            self._create_table("//tmp/t_out" + str(i))

        ops = []
        for i in xrange(1, op_count):
            ops.append(
                map(dont_track=True,
                    # Sleep is necessary since we not support revive for completing operations.
                    command="sleep 3; cat",
                    in_=["//tmp/t_in"],
                    out="//tmp/t_out" + str(i)))

        for i in range(10):
            while True:
                scheduler_locks = get("//sys/scheduler/lock/@locks", verbose=False)
                if len(scheduler_locks) > 0:
                    scheduler_transaction = scheduler_locks[0]["transaction_id"]
                    abort_transaction(scheduler_transaction)
                    break
                time.sleep(0.01)

        for op in ops:
            op.track()

        for i in xrange(1, op_count):
            assert read_table("//tmp/t_out" + str(i)) == [ {"foo" : "bar"} ]

    def test_user_transaction_abort_when_scheduler_is_down(self):
        self._prepare_tables()

        transaction_id = start_transaction(timeout=300 * 1000)
        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat; sleep 3", transaction_id=transaction_id)

        time.sleep(2)
        self.Env.kill_schedulers()

        abort_transaction(transaction_id)

        self.Env.start_schedulers()

        with pytest.raises(YtError):
            op.track()

    def test_scheduler_transaction_abort_when_scheduler_is_down(self):
        self._prepare_tables()

        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat; sleep 3")

        time.sleep(2)
        self.Env.kill_schedulers()

        abort_transaction(get("//sys/operations/{0}/@input_transaction_id".format(op.id)))
        abort_transaction(get("//sys/operations/{0}/@output_transaction_id".format(op.id)))

        self.Env.start_schedulers()

        op.track()

        assert read_table("//tmp/t_out") == [ {"foo" : "bar"} ]

    def test_operation_time_limit(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out1")
        self._create_table("//tmp/out2")

        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        # Default infinite time limit.
        op1 = map(dont_track=True,
            command="sleep 1.0; cat >/dev/null",
            in_=["//tmp/in"],
            out="//tmp/out1")

        # Operation specific time limit.
        op2 = map(dont_track=True,
            command="sleep 3.0; cat >/dev/null",
            in_=["//tmp/in"],
            out="//tmp/out2",
            spec={'time_limit': 1000})

        # Have to wait for process termination, job proxy can't kill user process when cgroups are not enabled.
        time.sleep(3.2)
        assert get("//sys/operations/{0}/@state".format(op1.id)) not in ["failing", "failed"]
        assert get("//sys/operations/{0}/@state".format(op2.id)) in ["failing", "failed"]

        op1.track()
        with pytest.raises(YtError):
            op2.track()

    def test_operation_suspend_with_account_limit_exceeded(self):
        create_account("limited")
        set("//sys/accounts/limited/@resource_limits/chunk_count", 1)

        self._create_table("//tmp/in")
        self._create_table("//tmp/out")
        set("//tmp/out/@account", "limited")
        write_table("//tmp/in", [{"foo": i} for i in xrange(3)])

        op = map(dont_track=True,
            command="sleep $YT_JOB_INDEX; cat",
            in_=["//tmp/in"],
            out="//tmp/out",
            spec={
                "data_size_per_job": 1,
                "suspend_operation_if_account_limit_exceeded": True
            })

        wait(lambda: get("//sys/operations/{0}/@suspended".format(op.id)), iter=20)

        time.sleep(0.5)

        assert get("//sys/operations/{0}/@state".format(op.id)) == "running"

        alerts = get("//sys/operations/{0}/@alerts".format(op.id))
        assert list(alerts) == ["operation_suspended"]

        set("//sys/accounts/limited/@resource_limits/chunk_count", 10)
        op.resume()
        op.track()

        assert get("//sys/operations/{0}/@state".format(op.id)) == "completed"
        assert not get("//sys/operations/{0}/@suspended".format(op.id))
        assert not get("//sys/operations/{0}/@alerts".format(op.id))

    def test_fail_context_saved_on_time_limit(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out")

        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        op = map(dont_track=True,
            command="sleep 2.0; cat >/dev/null",
            in_=["//tmp/in"],
            out="//tmp/out",
            spec={'time_limit': 1000})

        wait(lambda: get("//sys/operations/{0}/@state".format(op.id)) == "failed")

        time.sleep(1)
        jobs_path = "//sys/operations/{0}/jobs".format(op.id)
        jobs = ls(jobs_path)
        assert len(jobs) > 0

        for job_id in jobs:
            assert len(read_file(jobs_path + "/" + job_id + "/fail_context")) > 0

    def test_fifo_default(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out1")
        self._create_table("//tmp/out2")
        self._create_table("//tmp/out3")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        create("map_node", "//sys/pools/fifo_pool", ignore_existing=True)
        set("//sys/pools/fifo_pool/@mode", "fifo")

        # Waiting for updating pool settings.
        time.sleep(0.6)

        ops = []
        for i in xrange(1, 4):
            ops.append(
                map(dont_track=True,
                    command="sleep 0.3; cat >/dev/null",
                    in_=["//tmp/in"],
                    out="//tmp/out" + str(i),
                    spec={"pool": "fifo_pool"}))

        for op in ops:
            op.track()

        finish_times = [get("//sys/operations/{0}/@finish_time".format(op.id)) for op in ops]
        for cur, next in zip(finish_times, finish_times[1:]):
            assert cur < next

    def test_fifo_by_pending_job_count(self):
        op_count = 3

        for i in xrange(1, op_count + 1):
            self._create_table("//tmp/in" + str(i))
            self._create_table("//tmp/out" + str(i))
            write_table("//tmp/in" + str(i), [{"foo": j} for j in xrange(op_count * (op_count + 1 - i))])

        create("map_node", "//sys/pools/fifo_pool", ignore_existing=True)
        set("//sys/pools/fifo_pool/@mode", "fifo")
        set("//sys/pools/fifo_pool/@fifo_sort_parameters", ["pending_job_count"])

        # Wait until pools tree would be updated
        time.sleep(0.6)

        ops = []
        for i in xrange(1, op_count + 1):
            ops.append(
                map(dont_track=True,
                    command="sleep 2.0; cat >/dev/null",
                    in_=["//tmp/in" + str(i)],
                    out="//tmp/out" + str(i),
                    spec={"pool": "fifo_pool", "data_size_per_job": 1}))

        time.sleep(1.0)
        for index, op in enumerate(ops):
            assert get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/fifo_index".format(op.id)) == 2 - index

        for op in ops:
            op.track()

        finish_times = [get("//sys/operations/{0}/@finish_time".format(op.id)) for op in ops]
        for cur, next in zip(finish_times, finish_times[1:]):
            assert cur > next

    def test_fifo_subpools(self):
        assert not get("//sys/scheduler/@alerts")

        create("map_node", "//sys/pools/fifo_pool", attributes={"mode": "fifo"})
        create("map_node", "//sys/pools/fifo_pool/fifo_subpool", attributes={"mode": "fifo"})

        time.sleep(1.5)

        assert get("//sys/scheduler/@alerts")
        assert get("//sys/scheduler/@alerts")[0]

    def test_preparing_operation_transactions(self):
        self._prepare_tables()

        set_banned_flag(True)
        op = sort(
            dont_track=True,
            in_="//tmp/t_in",
            out="//tmp/t_in",
            sort_by=["foo"])
        time.sleep(2)

        for tx in ls("//sys/transactions", attributes=["operation_id"]):
            if tx.attributes.get("operation_id", "") == op.id:
                abort_transaction(tx)

        with pytest.raises(YtError):
            op.track()

        set_banned_flag(False)

    def test_abort_custom_error_message(self):
        self._prepare_tables()

        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat; sleep 3")
        op.abort(abort_message="Test abort")

        assert op.get_state() == "aborted"
        assert get("//sys/operations/{0}/@result/error/inner_errors/0/message".format(op.id)) == "Test abort"

    def test_operation_pool_attributes(self):
        self._prepare_tables()

        op = map(in_="//tmp/t_in", out="//tmp/t_out", command="cat")
        assert get("//sys/operations/{0}/@pool".format(op.id)) == "root"
        assert get("//sys/operations/{0}/@brief_spec/pool".format(op.id)) == "root"

    def test_operation_events_attribute(self):
        self._prepare_tables()

        op = map(in_="//tmp/t_in", out="//tmp/t_out", command="cat")
        events = get("//sys/operations/{0}/@events".format(op.id))
        assert ["initializing", "preparing", "pending", "materializing", "running", "completing", "completed"] == [event["state"] for event in events]

    def test_exceed_job_time_limit(self):
        self._prepare_tables()

        op = map(
            dont_track=True,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="sleep 2 ; cat",
            spec={"max_failed_job_count": 1, "mapper": {"job_time_limit": 2000}})

        # if all jobs failed then operation is also failed
        with pytest.raises(YtError):
            op.track()

        jobs_path = "//sys/operations/" + op.id + "/jobs"
        for job_id in ls(jobs_path):
            inner_errors = get(jobs_path + "/" + job_id + "/@error/inner_errors")
            assert "Job time limit exceeded" in inner_errors[0]["message"]

    def test_within_job_time_limit(self):
        self._prepare_tables()
        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="sleep 1 ; cat",
            spec={"max_failed_job_count": 1, "mapper": {"job_time_limit": 2000}})

    def _get_metric_maximum_value(self, metric_key, pool):
        result = 0.0
        for value in reversed(get("//sys/scheduler/orchid/profiling/scheduler/pools/" + metric_key, verbose=False)):
            if value["tags"]["pool"] != pool:
                continue
            result = max(result, value["value"])
        return result

    def _get_operation_last_metric_value(self, metric_key, pool, slot_index):
        results = []
        for value in reversed(get("//sys/scheduler/orchid/profiling/scheduler/operations/" + metric_key, verbose=False)):
            if value["tags"]["pool"] != pool or value["tags"]["slot_index"] != str(slot_index):
                continue
            results.append((value["value"], value["time"]))
        last_metric = sorted(results, key=lambda x: x[1])[-1]
        return last_metric[0]

    def test_pool_profiling(self):
        self._prepare_tables()
        create("map_node", "//sys/pools/unique_pool")
        map(command="sleep 1; cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"pool": "unique_pool"})

        assert self._get_metric_maximum_value("fair_share_ratio_x100000", "unique_pool") == 100000
        assert self._get_metric_maximum_value("usage_ratio_x100000", "unique_pool") == 100000
        assert self._get_metric_maximum_value("demand_ratio_x100000", "unique_pool") == 100000
        assert self._get_metric_maximum_value("guaranteed_resource_ratio_x100000", "unique_pool") == 100000
        assert self._get_metric_maximum_value("resource_usage/cpu", "unique_pool") == 1
        assert self._get_metric_maximum_value("resource_usage/user_slots", "unique_pool") == 1
        assert self._get_metric_maximum_value("resource_demand/cpu", "unique_pool") == 1
        assert self._get_metric_maximum_value("resource_demand/user_slots", "unique_pool") == 1

    def test_operations_profiling(self):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", [{"x": "y"}])
        for i in xrange(2):
            self._create_table("//tmp/t_out_" + str(i + 1))

        create("map_node", "//sys/pools/some_pool")
        op1 = map(command="sleep 1000; cat", in_="//tmp/t_in", out="//tmp/t_out_1", spec={"pool": "some_pool"}, dont_track=True)
        op2 = map(command="sleep 1000; cat", in_="//tmp/t_in", out="//tmp/t_out_2", spec={"pool": "some_pool"}, dont_track=True)

        time.sleep(1.0)

        assert op1.get_state() == "running"
        assert op2.get_state() == "running"

        get_slot_index = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/slot_index".format(op_id))

        assert get_slot_index(op1.id) == 0
        assert get_slot_index(op2.id) == 1

        range_ = (49999, 50000, 50001)

        assert self._get_operation_last_metric_value("fair_share_ratio_x100000", "some_pool", 0) in range_
        assert self._get_operation_last_metric_value("usage_ratio_x100000", "some_pool", 0) == 100000
        assert self._get_operation_last_metric_value("demand_ratio_x100000", "some_pool", 0) == 100000
        assert self._get_operation_last_metric_value("guaranteed_resource_ratio_x100000", "some_pool", 0) in range_
        assert self._get_operation_last_metric_value("resource_usage/cpu", "some_pool", 0) == 1
        assert self._get_operation_last_metric_value("resource_usage/user_slots", "some_pool", 0) == 1
        assert self._get_operation_last_metric_value("resource_demand/cpu", "some_pool", 0) == 1
        assert self._get_operation_last_metric_value("resource_demand/user_slots", "some_pool", 0) == 1

        assert self._get_operation_last_metric_value("fair_share_ratio_x100000", "some_pool", 1) in range_
        assert self._get_operation_last_metric_value("usage_ratio_x100000", "some_pool", 1) == 0
        assert self._get_operation_last_metric_value("demand_ratio_x100000", "some_pool", 1) == 100000
        assert self._get_operation_last_metric_value("guaranteed_resource_ratio_x100000", "some_pool", 1) in range_
        assert self._get_operation_last_metric_value("resource_usage/cpu", "some_pool", 1) == 0
        assert self._get_operation_last_metric_value("resource_usage/user_slots", "some_pool", 1) == 0
        assert self._get_operation_last_metric_value("resource_demand/cpu", "some_pool", 1) == 1
        assert self._get_operation_last_metric_value("resource_demand/user_slots", "some_pool", 1) == 1

        op1.abort()

        time.sleep(2.0)

        assert self._get_operation_last_metric_value("fair_share_ratio_x100000", "some_pool", 1) == 100000
        assert self._get_operation_last_metric_value("usage_ratio_x100000", "some_pool", 1) == 100000
        assert self._get_operation_last_metric_value("demand_ratio_x100000", "some_pool", 1) == 100000
        assert self._get_operation_last_metric_value("guaranteed_resource_ratio_x100000", "some_pool", 1) == 100000

    def test_suspend_resume(self):
        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        write_table("//tmp/t_in", [{"foo": i} for i in xrange(10)])

        op = map(
            dont_track=True,
            command="sleep 1; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"data_size_per_job": 1})

        for i in xrange(5):
            time.sleep(0.5)
            op.suspend(abort_running_jobs=True)
            time.sleep(0.5)
            op.resume()

        for i in xrange(5):
            op.suspend()
            op.resume()

        for i in xrange(5):
            op.suspend(abort_running_jobs=True)
            op.resume()

        op.track()

        assert sorted(read_table("//tmp/t_out")) == [{"foo": i} for i in xrange(10)]

class TestPreserveSlotIndexAfterRevive(YTEnvSetup, PrepareTables):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operation_time_limit_check_period" : 100,
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "profiling_update_period": 100,
            "fair_share_profiling_period": 100,
        }
    }

    def test_preserve_slot_index_after_revive(self):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", [{"x": "y"}])

        get_slot_index = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/slot_index".format(op_id))

        for i in xrange(3):
            self._create_table("//tmp/t_out_" + str(i))

        op1 = map(command="sleep 1000; cat", in_="//tmp/t_in", out="//tmp/t_out_0", dont_track=True)
        op2 = map(command="sleep 2; cat", in_="//tmp/t_in", out="//tmp/t_out_1", dont_track=True)
        op3 = map(command="sleep 1000; cat", in_="//tmp/t_in", out="//tmp/t_out_2", dont_track=True)

        assert get_slot_index(op1.id) == 0
        assert get_slot_index(op2.id) == 1
        assert get_slot_index(op3.id) == 2

        op2.track()  # this makes slot index 1 available again since operation is completed

        self.Env.kill_schedulers()
        self.Env.start_schedulers()

        time.sleep(2.0)

        assert get_slot_index(op1.id) == 0
        assert get_slot_index(op3.id) == 2

        op2 = map(command="sleep 1000; cat", in_="//tmp/t_in", out="//tmp/t_out_1", dont_track=True)

        assert get_slot_index(op2.id) == 1

class TestSchedulerFunctionality2(YTEnvSetup, PrepareTables):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operation_time_limit_check_period" : 100,
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "fair_share_profiling_period": 100,
        }
    }

    def _check_running_jobs(self, op_id, desired_running_jobs):
        success_iter = 0
        min_success_iteration = 10
        for i in xrange(100):
            running_jobs = get("//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op_id))
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
        create("map_node", "//sys/pools/small_pool/subpool_3", attributes={"min_share_ratio": 1.0})
        create("map_node", "//sys/pools/small_pool/subpool_4", attributes={"min_share_ratio": 1.0})

        total_resource_limit = get("//sys/scheduler/orchid/scheduler/cell/resource_limits")

        # Wait for fair share update.
        time.sleep(1)

        get_pool_guaranteed_resources = lambda pool: \
            get("//sys/scheduler/orchid/scheduler/pools/{0}/guaranteed_resources".format(pool))

        get_pool_guaranteed_resources_ratio = lambda pool: \
            get("//sys/scheduler/orchid/scheduler/pools/{0}/guaranteed_resources_ratio".format(pool))

        assert assert_almost_equal(get_pool_guaranteed_resources_ratio("big_pool"), 1.0)
        assert get_pool_guaranteed_resources("big_pool") == total_resource_limit

        assert assert_almost_equal(get_pool_guaranteed_resources_ratio("small_pool"), 0)
        assert assert_almost_equal(get_pool_guaranteed_resources_ratio("subpool_3"), 0)
        assert assert_almost_equal(get_pool_guaranteed_resources_ratio("subpool_4"), 0)

        assert assert_almost_equal(get_pool_guaranteed_resources_ratio("subpool_1"), 1.0 / 4.0)
        assert assert_almost_equal(get_pool_guaranteed_resources_ratio("subpool_2"), 3.0 / 4.0)

        self._prepare_tables()

        get_operation_guaranteed_resources_ratio = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/guaranteed_resources_ratio".format(op_id))

        op = map(
            dont_track=True,
            wait_for_jobs=True,
            command="cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            spec={"pool": "big_pool"})

        # Wait for fair share update.
        time.sleep(1)

        assert assert_almost_equal(get_operation_guaranteed_resources_ratio(op.id), 1.0 / 5.0)
        assert assert_almost_equal(get_pool_guaranteed_resources_ratio("subpool_1"), 1.0 / 5.0)
        assert assert_almost_equal(get_pool_guaranteed_resources_ratio("subpool_2"), 3.0 / 5.0)

        op.resume_jobs()
        op.track()


    def test_resource_limits(self):
        resource_limits = {"cpu": 1, "memory": 1000 * 1024 * 1024, "network": 10}
        create("map_node", "//sys/pools/test_pool", attributes={"resource_limits": resource_limits})

        while True:
            pools = get("//sys/scheduler/orchid/scheduler/pools")
            if "test_pool" in pools:
                break
            time.sleep(0.1)

        stats = get("//sys/scheduler/orchid/scheduler")
        pool_resource_limits = stats["pools"]["test_pool"]["resource_limits"]
        for resource, limit in resource_limits.iteritems():
            assert assert_almost_equal(pool_resource_limits[resource], limit)

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
        self._check_running_jobs(op.id, 1)
        op.abort()

        op = map(
            dont_track=True,
            command="sleep 5",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"job_count": 3, "resource_limits": resource_limits, "mapper": {"memory_limit": memory_limit}, "testing": testing_options})
        self._check_running_jobs(op.id, 1)
        op_limits = get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/resource_limits".format(op.id))
        for resource, limit in resource_limits.iteritems():
            assert assert_almost_equal(op_limits[resource], limit)
        op.abort()

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
        self._check_running_jobs(op.id, 1)

        set("//sys/operations/{0}/@resource_limits".format(op.id), {"user_slots": 2})
        self._check_running_jobs(op.id, 2)

        op.abort()

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
        write_table("//tmp/t_in", data);

        get_pool_fair_share_ratio = lambda pool: \
            get("//sys/scheduler/orchid/scheduler/pools/{0}/fair_share_ratio".format(pool))

        op1 = map(
            wait_for_jobs=True,
            dont_track=True,
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out_1",
            spec={"job_count": 1, "pool": "subpool_1"})

        op2 = map(
            wait_for_jobs=True,
            dont_track=True,
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out_2",
            spec={"job_count": 2, "pool": "high_cpu_pool"})

        assert assert_almost_equal(get_pool_fair_share_ratio("subpool_1"), 1.0 / 3.0)
        assert assert_almost_equal(get_pool_fair_share_ratio("low_cpu_pool"), 1.0 / 3.0)
        assert assert_almost_equal(get_pool_fair_share_ratio("high_cpu_pool"), 2.0 / 3.0)

        op3 = map(
            dont_track=True,
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out_3",
            spec={"job_count": 1, "pool": "subpool_2", "mapper": {"cpu_limit": 0}})

        time.sleep(1)

        assert assert_almost_equal(get_pool_fair_share_ratio("low_cpu_pool"), 1.0 / 2.0)
        assert assert_almost_equal(get_pool_fair_share_ratio("high_cpu_pool"), 1.0 / 2.0)

        op1.resume_jobs()
        op1.track()
        op2.resume_jobs()
        op2.track()
        op3.track()


    def test_fractional_cpu_usage(self):
        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        data = [{"foo": i} for i in xrange(3)]
        write_table("//tmp/t_in", data);

        op = map(
            wait_for_jobs=True,
            dont_track=True,
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"job_count": 3, "mapper": {"cpu_limit": 0.87}})

        resource_usage = get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/resource_usage".format(op.id))
        assert_almost_equal(resource_usage["cpu"], 3 * 0.87)

        op.resume_jobs()
        op.track()


class TestSchedulerRevive(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operation_time_limit_check_period" : 100,
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "operation_build_progress_period": 100,
            "snapshot_period": 500,
            "testing_options": {
                "finish_operation_transition_delay": 2000,
            },
        }
    }

    def _create_table(self, table):
        create("table", table)
        set(table + "/@replication_factor", 1)

    def _prepare_tables(self):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        remove("//tmp/t_out", force=True)
        self._create_table("//tmp/t_out")

    def _wait_state(self, op, state):
        iter = 0
        backoff = 0.1
        while True:
            if state == get("//sys/operations/" + op.id + "/@state"):
                break
            time.sleep(backoff)

            iter += 1
            assert iter < 50, "Operation %s did not come to %s state after %f seconds" % (op.id, state, iter * backoff)

    def test_missing_transactions(self):
        self._prepare_tables()

        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat; sleep 10")

        for iter in xrange(5):
            self._wait_state(op, "running")
            self.Env.kill_schedulers()
            set("//sys/operations/" + op.id + "/@input_transaction_id", "0-0-0-0")
            self.Env.start_schedulers()
            time.sleep(1)

        op.track()

        assert "completed" == get("//sys/operations/" + op.id + "/@state")

    def test_aborting(self):
        self._prepare_tables()

        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat; sleep 10")

        self._wait_state(op, "running")

        op.abort(ignore_result=True)

        self._wait_state(op, "aborting")

        self.Env.kill_schedulers()

        assert "aborting" == get("//sys/operations/" + op.id + "/@state")

        self.Env.start_schedulers()

        with pytest.raises(YtError):
            op.track()

        assert "aborted" == get("//sys/operations/" + op.id + "/@state")

    def test_completing(self):
        self._prepare_tables()

        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat; sleep 10")

        self._wait_state(op, "running")

        op.complete(ignore_result=True)

        self._wait_state(op, "completing")

        self.Env.kill_schedulers()

        assert "completing" == get("//sys/operations/" + op.id + "/@state")

        self.Env.start_schedulers()

        op.track()

        assert "completed" == get("//sys/operations/" + op.id + "/@state")

        assert read_table("//tmp/t_out") == []

    @pytest.mark.parametrize("stage", ["stage" + str(index) for index in xrange(1, 8)])
    def test_completing_with_sleep(self, stage):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", [{"foo": "bar"}] * 2)

        remove("//tmp/t_out", force=True)
        self._create_table("//tmp/t_out")

        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat; if [ \"$YT_JOB_INDEX\" != \"0\" ]; then sleep 10; fi;",
                 spec={
                     "testing": {
                         "delay_inside_operation_commit": 4000,
                         "delay_inside_operation_commit_stage": stage,
                     },
                     "job_count": 2
                 })

        self._wait_state(op, "running")

        # Wait for snapshot and job completion.
        time.sleep(3)

        op.complete(ignore_result=True)

        self._wait_state(op, "completing")

        # Wait to perform complete before sleep.
        time.sleep(2)

        self.Env.kill_schedulers()

        assert "completing" == get("//sys/operations/" + op.id + "/@state")

        self.Env.start_schedulers()

        op.track()

        events = get("//sys/operations/{0}/@events".format(op.id))

        events_prefix = ["initializing", "preparing", "materializing", "running", "completing"]
        if stage <= "stage5":
            correct_events = events_prefix + ["reviving", "running", "completing", "completed"]
        else:
            correct_events = events_prefix + ["reviving", "completed"]

        assert correct_events == [event["state"] for event in events if event["state"] != "pending"]

        assert "completed" == get("//sys/operations/" + op.id + "/@state")

        assert read_table("//tmp/t_out") == [{"foo": "bar"}]

    def test_abort_during_complete(self):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", [{"foo": "bar"}] * 2)

        remove("//tmp/t_out", force=True)
        self._create_table("//tmp/t_out")

        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat; if [ \"$YT_JOB_INDEX\" != \"0\" ]; then sleep 10; fi;",
                 spec={
                     "testing": {
                         "delay_inside_operation_commit": 4000,
                         "delay_inside_operation_commit_stage": "stage4",
                     },
                     "job_count": 2
                 })

        self._wait_state(op, "running")

        # Wait for snapshot and job completion.
        time.sleep(3)

        op.complete(ignore_result=True)

        self._wait_state(op, "completing")

        # Wait to perform complete before sleep.
        time.sleep(2)

        op.abort()
        op.track()

        assert "completed" == get("//sys/operations/" + op.id + "/@state")

    def test_failing(self):
        self._prepare_tables()

        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="exit 1", spec={"max_failed_job_count": 1})

        self._wait_state(op, "failing")

        self.Env.kill_schedulers()

        assert "failing" == get("//sys/operations/" + op.id + "/@state")

        self.Env.start_schedulers()

        with pytest.raises(YtError):
            op.track()

        assert "failed" == get("//sys/operations/" + op.id + "/@state")

    def test_revive_failed_jobs(self):
        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        op = map(
            command="sleep 1; false",
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            spec={"max_failed_job_count": 10000},
            dont_track=True)

        self._wait_state(op, "running")

        failed_jobs_path = "//sys/scheduler/orchid/scheduler/operations/" + op.id + "/progress/jobs/failed"

        def failed_jobs_exist():
            return exists(failed_jobs_path) and get(failed_jobs_path) >= 3

        wait(failed_jobs_exist)

        suspend_op(op.id)

        # Waiting until snapshot is built.
        time.sleep(2.0)

        self.Env.kill_schedulers()
        self.Env.start_schedulers()

        # Waiting until orchid is built.
        time.sleep(1.0)
        assert exists(failed_jobs_path) and get(failed_jobs_path) >= 3

        abort_op(op.id)

class TestSchedulerRevive2(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operation_time_limit_check_period" : 100,
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "testing_options": {
                "enable_random_master_disconnection": False,
                "random_master_disconnection_max_backoff": 10000,
                "finish_operation_transition_delay": 1000,
            }
        }
    }

    OP_COUNT = 10

    def _create_table(self, table):
        create("table", table)
        set(table + "/@replication_factor", 1)

    def _prepare_tables(self):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        for index in xrange(self.OP_COUNT):
            self._create_table("//tmp/t_out" + str(index))
            self._create_table("//tmp/t_err" + str(index))

    def test_many_operations(self):
        self._prepare_tables()

        ops = []
        for index in xrange(self.OP_COUNT):
            op = map(
                dont_track=True,
                command="sleep 1; echo 'AAA' >&2; cat",
                in_="//tmp/t_in",
                out="//tmp/t_out" + str(index),
                spec={
                    "stderr_table_path": "//tmp/t_err" + str(index),
                })
            ops.append(op)

        try:
            set("//sys/scheduler/config", {"testing_options": {"enable_random_master_disconnection": True}})
            for index, op in enumerate(ops):
                try:
                    op.track()
                    assert read_table("//tmp/t_out" + str(index)) == [{"foo": "bar"}]
                except YtError:
                    assert get("//sys/operations/{0}/@state".format(op.id)) == "failed"
        finally:
            set("//sys/scheduler/config", {"testing_options": {"enable_random_master_disconnection": False}})
            time.sleep(5)

class TestMultipleSchedulers(YTEnvSetup, PrepareTables):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 2

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 1000,
            "fair_share_update_period": 100,
            "profiling_update_period": 100,
            "snapshot_period": 500,
            "testing_options": {
                "master_disconnect_delay": 3000,
            },
        }
    }

    def _get_scheduler_transation(self):
        while True:
            scheduler_locks = get("//sys/scheduler/lock/@locks", verbose=False)
            if len(scheduler_locks) > 0:
                scheduler_transaction = scheduler_locks[0]["transaction_id"]
                return scheduler_transaction
            time.sleep(0.01)

    def test_hot_standby(self):
        self._prepare_tables()

        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat; sleep 5")

        # Wait till snapshot is written
        time.sleep(1)

        transaction_id = self._get_scheduler_transation()

        def get_transaction_title(transaction_id):
            return get("#{0}/@title".format(transaction_id), verbose=False)

        title = get_transaction_title(transaction_id)

        while True:
            abort_transaction(transaction_id)

            new_transaction_id = self._get_scheduler_transation()
            new_title = get_transaction_title(new_transaction_id)
            if title != new_title:
                break

            title = new_title
            transaction_id = new_transaction_id
            time.sleep(0.3)

        op.track()

        assert read_table("//tmp/t_out") == [ {"foo" : "bar"} ]


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
        set_banned_flag(True, [ node ])

        print >>sys.stderr,  "Fail strategy"
        with pytest.raises(YtError):
            op = map(in_="//tmp/t_in", out="//tmp/t_out", command="cat", spec={"unavailable_chunk_strategy": "fail"})

        print >>sys.stderr,  "Skip strategy"
        map(in_="//tmp/t_in", out="//tmp/t_out", command="cat", spec={"unavailable_chunk_strategy": "skip"})
        assert read_table("//tmp/t_out") == []

        print >>sys.stderr,  "Wait strategy"
        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat",  spec={"unavailable_chunk_strategy": "wait"})

        set_banned_flag(False, [ node ])
        op.track()

        assert read_table("//tmp/t_out") == [ {"foo" : "bar"} ]

    def test_strategies_in_sort(self):
        v1 = {"key" : "aaa"}
        v2 = {"key" : "bb"}
        v3 = {"key" : "bbxx"}
        v4 = {"key" : "zfoo"}
        v5 = {"key" : "zzz"}

        create("table", "//tmp/t_in")
        set("//tmp/t_in/@replication_factor", 1)
        write_table("//tmp/t_in", [v3, v5, v1, v2, v4]) # some random order

        create("table", "//tmp/t_out")
        set("//tmp/t_out/@replication_factor", 1)

        set_banned_flag(True)

        print >>sys.stderr, "Fail strategy"
        with pytest.raises(YtError):
            op = sort(in_="//tmp/t_in", out="//tmp/t_out", sort_by="key", spec={"unavailable_chunk_strategy": "fail"})

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
        assert get("//tmp/t_out/@sorted") == True
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
            op = merge(mode="sorted", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t_out", spec={"unavailable_chunk_strategy": "fail"})

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
        assert get("//tmp/t_out/@sorted") == True
        assert get("//tmp/t_out/@sorted_by") == ["a"]

class TestSchedulerMaxChunkPerJob(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "map_operation_options" : {
                "max_data_slices_per_job" : 1,
            },
            "ordered_merge_operation_options" : {
                "max_data_slices_per_job" : 1,
            },
            "sorted_merge_operation_options" : {
                "max_data_slices_per_job" : 1,
            },
            "reduce_operation_options" : {
                "max_data_slices_per_job" : 1,
            },
        }
    }

    def test_max_data_slices_per_job(self):
        data = [{"foo": i} for i in xrange(5)]
        create("table", "//tmp/in1")
        create("table", "//tmp/in2")
        create("table", "//tmp/out")
        write_table("//tmp/in1", data, sorted_by="foo")
        write_table("//tmp/in2", data, sorted_by="foo")



        op = merge(mode="ordered", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out", spec={"force_transform": True})
        assert data + data == read_table("//tmp/out")

        # Must be 2 jobs since input has 2 chunks.
        assert get("//sys/operations/{0}/@progress/jobs/total".format(op.id)) == 2

        op = map(command="cat >/dev/null", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out")
        assert get("//sys/operations/{0}/@progress/jobs/total".format(op.id)) == 2

        op = merge(mode="sorted", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out")
        assert get("//sys/operations/{0}/@progress/jobs/total".format(op.id)) == 2

        op = reduce(command="cat >/dev/null", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out", reduce_by=["foo"])
        assert get("//sys/operations/{0}/@progress/jobs/total".format(op.id)) == 2


class TestSchedulerMaxChildrenPerAttachRequest(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "max_children_per_attach_request": 1,
        }
    }

    def test_max_children_per_attach_request(self):
        data = [{"foo": i} for i in xrange(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        map(command="cat", in_="//tmp/in", out="//tmp/out", spec={"data_size_per_job": 1})

        assert sorted(read_table("//tmp/out")) == sorted(data)
        assert get("//tmp/out/@row_count") == 3

    def test_max_children_per_attach_request_in_live_preview(self):
        data = [{"foo": i} for i in xrange(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        op = map(
            wait_for_jobs=True,
            dont_track=True,
            command="cat",
            in_="//tmp/in",
            out="//tmp/out",
            spec={"data_size_per_job": 1})

        op.resume_job(op.jobs[0])
        op.resume_job(op.jobs[1])

        operation_path = "//sys/operations/{0}".format(op.id)
        for iter in xrange(100):
            jobs_exist = exists(operation_path + "/@brief_progress/jobs")
            if jobs_exist:
                completed_jobs = get(operation_path + "/@brief_progress/jobs/completed/total")
                if completed_jobs == 2:
                    break
            time.sleep(0.1)

        operation_path = "//sys/operations/{0}".format(op.id)
        transaction_id = get(operation_path + "/@async_scheduler_transaction_id")
        assert len(read_table(operation_path + "/output_0", tx=transaction_id)) == 2
        assert get(operation_path + "/output_0/@row_count", tx=transaction_id) == 2

        op.resume_jobs()
        op.track()


class TestSchedulerOperationLimits(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "max_running_operation_count_per_pool" : 1,
            "static_orchid_cache_update_period": 100,
            "default_parent_pool": "default_pool",
        }
    }

    def teardown(self):
        set("//sys/pools", {})

    def _run_operations(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        create("table", "//tmp/out3")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        op1 = map(
            dont_track=True,
            wait_for_jobs=True,
            command="cat >/dev/null",
            in_=["//tmp/in"],
            out="//tmp/out1",
            spec={"pool": "test_pool_1"})

        op2 = map(
            dont_track=True,
            command="cat >/dev/null",
            in_=["//tmp/in"],
            out="//tmp/out2",
            spec={"pool": "test_pool_1"})

        op3 = map(
            dont_track=True,
            wait_for_jobs=True,
            command="cat >/dev/null",
            in_=["//tmp/in"],
            out="//tmp/out3",
            spec={"pool": "test_pool_2"})

        op1.ensure_running()
        with pytest.raises(TimeoutError):
            op2.ensure_running(timeout=1.0)
        op3.ensure_running()

        op1.resume_jobs()
        op3.resume_jobs()

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

        op1 = map(dont_track=True, wait_for_jobs=True, command="cat >/dev/null", in_=["//tmp/in"], out="//tmp/out1")
        op2 = map(dont_track=True, command="cat >/dev/null", in_=["//tmp/in"], out="//tmp/out2")
        op3 = map(dont_track=True, command="cat >/dev/null", in_=["//tmp/in"], out="//tmp/out3")

        time.sleep(1.5)
        assert op1.get_state() == "running"
        assert op2.get_state() == "pending"
        assert op3.get_state() == "pending"

        op2.abort()
        op1.resume_jobs()
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
            wait_for_jobs=True,
            command="cat",
            in_=["//tmp/in"],
            out="//tmp/out1",
            spec={"pool": "test_pool_1"})

        remove("//sys/pools/test_pool_1")
        create("map_node", "//sys/pools/test_pool_2/test_pool_1")
        time.sleep(0.5)

        op2 = map(
            dont_track=True,
            command="cat",
            in_=["//tmp/in"],
            out="//tmp/out2",
            spec={"pool": "test_pool_2"})

        op1.ensure_running()
        with pytest.raises(TimeoutError):
            op2.ensure_running(timeout=1.0)

        op1.resume_jobs()
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
                    execute(False)
            else:
                ops.append(execute(True))

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

        assert get("//sys/scheduler/orchid/scheduler/pools/subpool/running_operation_count") == 1
        assert get("//sys/scheduler/orchid/scheduler/pools/subpool/operation_count") == 3

        assert get("//sys/scheduler/orchid/scheduler/pools/research/running_operation_count") == 1
        assert get("//sys/scheduler/orchid/scheduler/pools/research/operation_count") == 3

        assert get("//sys/scheduler/orchid/scheduler/pools/production/running_operation_count") == 0
        assert get("//sys/scheduler/orchid/scheduler/pools/production/operation_count") == 0

        move("//sys/pools/research/subpool", "//sys/pools/production/subpool")

        time.sleep(0.5)

        assert get("//sys/scheduler/orchid/scheduler/pools/subpool/running_operation_count") == 1
        assert get("//sys/scheduler/orchid/scheduler/pools/subpool/operation_count") == 3

        assert get("//sys/scheduler/orchid/scheduler/pools/research/running_operation_count") == 0
        assert get("//sys/scheduler/orchid/scheduler/pools/research/operation_count") == 0

        assert get("//sys/scheduler/orchid/scheduler/pools/production/running_operation_count") == 1
        assert get("//sys/scheduler/orchid/scheduler/pools/production/operation_count") == 3

        for op in ops:
            op.abort()

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


class TestSchedulingTags(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler" : {
            "event_log" : {
                "flush_period" : 300,
                "retry_backoff_time": 300
            },
            "safe_scheduler_online_time": 1000,
        }
    }

    def _prepare(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")

        self.node = list(get("//sys/nodes"))[0]
        set("//sys/nodes/{0}/@user_tags".format(self.node), ["tagA", "tagB"])
        # Wait applying scheduling tags.
        time.sleep(0.1)

    def test_tag_filters(self):
        self._prepare()

        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out")
        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagC"})

        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagA"})
        assert read_table("//tmp/t_out") == [ {"foo" : "bar"} ]

        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out",
            spec={"scheduling_tag_filter": "tagA & !tagC"})
        assert read_table("//tmp/t_out") == [ {"foo" : "bar"} ]
        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_in", out="//tmp/t_out",
                spec={"scheduling_tag_filter": "tagA & !tagB"})

        set("//sys/nodes/{0}/@user_tags".format(self.node), [])
        time.sleep(1.0)
        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagA"})


    def test_pools(self):
        self._prepare()

        create("map_node", "//sys/pools/test_pool", attributes={"node_tag": "tagA"})
        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"pool": "test_pool"})
        assert read_table("//tmp/t_out") == [ {"foo" : "bar"} ]

    def test_tag_correctness(self):
        def get_job_nodes(op):
            nodes = __builtin__.set()
            for row in read_table("//sys/scheduler/event_log"):
                if row.get("event_type") == "job_started" and row.get("operation_id") == op.id:
                    nodes.add(row["node_address"])
            return nodes

        self._prepare()
        write_table("//tmp/t_in", [{"foo": "bar"} for _ in xrange(20)])

        set("//sys/nodes/{0}/@user_tags".format(self.node), ["tagB"])
        time.sleep(1.2)
        op = map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagB", "job_count": 20})
        time.sleep(0.8)
        assert get_job_nodes(op) == __builtin__.set([self.node])


        op = map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"job_count": 20})
        time.sleep(0.8)
        assert len(get_job_nodes(op)) <= 2

##################################################################

class TestSchedulerConfig(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler" : {
            "event_log" : {
                "retry_backoff_time" : 7,
                "flush_period" : 5000
            },
            "operation_options": {
                "spec_template": {
                    "data_size_per_job": 1000
                }
            },
            "map_operation_options": {
                "spec_template": {
                    "data_size_per_job": 2000,
                    "max_failed_job_count": 10
                }
            },
            "environment": {
                "TEST_VAR": "10"
            },
        },
        "addresses": [
            ("ipv4", "127.0.0.1"),
            ("ipv6", "::1")
        ]
    }

    def test_basic(self):
        orchid_scheduler_config = "//sys/scheduler/orchid/scheduler/config"
        assert get("{0}/event_log/flush_period".format(orchid_scheduler_config)) == 5000
        assert get("{0}/event_log/retry_backoff_time".format(orchid_scheduler_config)) == 7

        set("//sys/scheduler/config", { "event_log" : { "flush_period" : 10000 } })
        time.sleep(2)

        assert get("{0}/event_log/flush_period".format(orchid_scheduler_config)) == 10000
        assert get("{0}/event_log/retry_backoff_time".format(orchid_scheduler_config)) == 7

        set("//sys/scheduler/config", {})
        time.sleep(2)

        assert get("{0}/event_log/flush_period".format(orchid_scheduler_config)) == 5000
        assert get("{0}/event_log/retry_backoff_time".format(orchid_scheduler_config)) == 7

    def test_compat(self):
        orchid_scheduler_config = "//sys/scheduler/orchid/scheduler/config"

        set("//sys/scheduler/config", { "max_running_operation_count_per_pool" : 666 })
        time.sleep(3)
        assert get("{0}/max_running_operation_count_per_pool".format(orchid_scheduler_config)) == 666

        set("//sys/scheduler/config", {})
        time.sleep(3)
        assert get("{0}/max_running_operation_count_per_pool".format(orchid_scheduler_config)) == 50

        # COMPAT(acid): Remove this when max_running_operations_per_pool is removed.
        set("//sys/scheduler/config", { "max_running_operations_per_pool" : 999 })
        time.sleep(3)
        assert get("{0}/max_running_operation_count_per_pool".format(orchid_scheduler_config)) == 999

    def test_adresses(self):
        adresses = get("//sys/scheduler/@addresses")
        assert adresses["ipv4"].startswith("127.0.0.1:")
        assert adresses["ipv6"].startswith("::1:")

    def test_specs(self):
        create("table", "//tmp/t_in")
        write_table("<append=true>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")

        op = map(command="cat", in_=["//tmp/t_in"], out="//tmp/t_out")
        assert get("//sys/operations/{0}/@spec/data_size_per_job".format(op.id)) == 2000

        op = merge(in_=["//tmp/t_in"], out="//tmp/t_out")
        assert get("//sys/operations/{0}/@spec/data_size_per_job".format(op.id)) == 1000

    def test_cypress_config(self):
        create("table", "//tmp/t_in")
        write_table("<append=true>//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")

        op = map(command="cat", in_=["//tmp/t_in"], out="//tmp/t_out")
        assert get("//sys/operations/{0}/@spec/data_size_per_job".format(op.id)) == 2000
        assert get("//sys/operations/{0}/@spec/max_failed_job_count".format(op.id)) == 10

        set("//sys/scheduler/config", {
            "map_operation_options": {"spec_template": {"max_failed_job_count": 50}},
            "environment": {"OTHER_VAR": "20"},
        })
        time.sleep(0.5)

        op = map(command="cat", in_=["//tmp/t_in"], out="//tmp/t_out")
        assert get("//sys/operations/{0}/@spec/data_size_per_job".format(op.id)) == 2000
        assert get("//sys/operations/{0}/@spec/max_failed_job_count".format(op.id)) == 50

        environment = get("//sys/scheduler/orchid/scheduler/config/environment")
        assert environment["TEST_VAR"] == "10"
        assert environment["OTHER_VAR"] == "20"

class TestSchedulerPools(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "default_parent_pool": "default_pool",
            "event_log" : {
                "flush_period" : 300,
                "retry_backoff_time": 300
            },
            "max_ephemeral_pools_per_user": 3,
        }
    }

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

        op1 = map(
            dont_track=True,
            wait_for_jobs=True,
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out1")

        op2 = map(
            dont_track=True,
            wait_for_jobs=True,
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out2",
            spec={"pool": "my_pool"})

        pool = get("//sys/scheduler/orchid/scheduler/pools/root")
        assert pool["parent"] == "default_pool"

        pool = get("//sys/scheduler/orchid/scheduler/pools/my_pool")
        assert pool["parent"] == "default_pool"

        assert __builtin__.set(["root", "my_pool"]) == \
               __builtin__.set(get("//sys/scheduler/orchid/scheduler/user_to_ephemeral_pools/root"))

        remove("//sys/pools/default_pool")
        time.sleep(0.2)

        for op in [op1, op2]:
            op.resume_jobs()
            op.track()

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
        for i in xrange(1, 4):
            ops.append(map(
                dont_track=True,
                wait_for_jobs=True,
                command="cat",
                in_="//tmp/t_in",
                out="//tmp/t_out" + str(i),
                spec={"pool": "pool" + str(i)}))

        assert __builtin__.set(["pool" + str(i) for i in xrange(1, 4)]) == \
               __builtin__.set(get("//sys/scheduler/orchid/scheduler/user_to_ephemeral_pools/root"))

        with pytest.raises(YtError):
            map(command="cat",
                in_="//tmp/t_in",
                out="//tmp/t_out4",
                spec={"pool": "pool4"})

        remove("//sys/pools/default_pool")
        time.sleep(0.2)

        for op in ops:
            op.resume_jobs()
            op.track()

    def test_event_log(self):
        self._prepare()

        create("map_node", "//sys/pools/custom_pool")
        op = map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"pool": "custom_pool"})

        time.sleep(2.0)

        events = []
        for row in read_table("//sys/scheduler/event_log"):
            event_type = row["event_type"]
            if event_type.startswith("operation_") and event_type != "operation_prepared" and row["operation_id"] == op.id:
                events.append(row["event_type"])
                assert row["pool"]

        assert events == ["operation_started", "operation_completed"]


class TestSchedulerSnapshots(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "snapshot_period": 500,
            "operation_controller_suspend_timeout": 2000,
            "max_concurrent_controller_schedule_job_calls": 1,
        }
    }

    def test_snapshots(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])
        create("table", "//tmp/out")

        testing_options = {"scheduling_delay": 500}

        op = map(
            dont_track=True,
            wait_for_jobs=True,
            command="cat",
            in_="//tmp/in",
            out="//tmp/out",
            spec={"data_size_per_job": 1, "testing": testing_options})

        snapshot_path = "//sys/operations/{0}/snapshot".format(op.id)
        track_path(snapshot_path, 10)

        # This is done to avoid read failures due to snapshot file rewriting.
        snapshot_backup_path = snapshot_path + ".backup"
        copy(snapshot_path, snapshot_backup_path)
        assert len(read_file(snapshot_backup_path, verbose=False)) > 0

        op.resume_jobs()
        op.track()

    def test_parallel_snapshots(self):
        create("table", "//tmp/input")

        testing_options = {"scheduling_delay": 100}

        job_count = 1
        original_data = [{"index": i} for i in xrange(job_count)]
        write_table("//tmp/input", original_data)

        operation_count = 5
        ops = []
        for index in range(operation_count):
            output = "//tmp/output" + str(index)
            create("table", output)
            ops.append(
                map(dont_track=True,
                    wait_for_jobs=True,
                    command="cat",
                    in_="//tmp/input",
                    out=[output],
                    spec={"data_size_per_job": 1, "testing": testing_options}))

        for op in ops:
            snapshot_path = "//sys/operations/{0}/snapshot".format(op.id)
            track_path(snapshot_path, 10)

            snapshot_backup_path = snapshot_path + ".backup"
            copy(snapshot_path, snapshot_backup_path)
            assert len(read_file(snapshot_backup_path, verbose=False)) > 0
            op.resume_jobs()

        for op in ops:
            op.track()

    def test_suspend_time_limit(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        create("table", "//tmp/out1")
        create("table", "//tmp/out2")

        while True:
            op2 = map(
                dont_track=True,
                command="cat",
                in_="//tmp/in",
                out="//tmp/out2",
                spec={"data_size_per_job": 1, "testing": {"scheduling_delay": 15000}})

            time.sleep(2)

            snapshot_path2 = "//sys/operations/{0}/snapshot".format(op2.id)
            if exists(snapshot_path2):
                op2.abort()
                continue
            else:
                break

        op1 = map(
            dont_track=True,
            command="sleep 10; cat",
            in_="//tmp/in",
            out="//tmp/out1",
            spec={"data_size_per_job": 1})

        time.sleep(8)

        snapshot_path1 = "//sys/operations/{0}/snapshot".format(op1.id)
        snapshot_path2 = "//sys/operations/{0}/snapshot".format(op2.id)

        assert exists(snapshot_path1)
        assert not exists(snapshot_path2)

        op1.abort()
        op2.abort()

class TestSchedulerPreemption(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "min_share_preemption_timeout": 100,
            "fair_share_starvation_tolerance": 0.7,
            "fair_share_starvation_tolerance_limit": 0.9,
            "fair_share_update_period": 100
        }
    }

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
            table_writer = {
                "block_size": 1024,
                "desired_chunk_size": 1024})

        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        spec={
            "pool": "fake_pool",
            "locality_timeout": 0,
            "enable_job_splitting": False,
        }
        if interruptible:
            data_size_per_job = get("//tmp/t_in/@uncompressed_data_size")
            spec["data_size_per_job"] = data_size_per_job / 3 + 1
        else:
            spec["job_count"] = 3
        op1 = map(
            dont_track=True,
            command="sleep 5; cat; echo stderr 1>&2",
            in_=["//tmp/t_in"],
            out="//tmp/t_out1",
            spec=spec)
        time.sleep(3)

        assert get("//sys/scheduler/orchid/scheduler/pools/fake_pool/fair_share_ratio") >= 0.999
        assert get("//sys/scheduler/orchid/scheduler/pools/fake_pool/usage_ratio") >= 0.999

        create("map_node", "//sys/pools/test_pool", attributes={"min_share_ratio": 1.0})
        op2 = map(
            dont_track=True,
            command="cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out2",
            spec={"pool": "test_pool"})
        op2.track()
        op1.track()
        assert get("//sys/operations/" + op1.id + "/jobs/@count") == (4 if interruptible else 3)

    def test_min_share_ratio(self):
        create("map_node", "//sys/pools/test_min_share_ratio_pool", attributes={"min_share_ratio": 1.0})

        create("table", "//tmp/t_in")
        for i in xrange(3):
            write_table("<append=true>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")

        get_operation_min_share_ratio = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/adjusted_min_share_ratio".format(op_id))

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
            op = map(
                dont_track=True,
                wait_for_jobs=True,
                command="cat",
                in_=["//tmp/t_in"],
                out="//tmp/t_out",
                spec=spec)

            # Wait for fair share update.
            time.sleep(0.2)

            assert get_operation_min_share_ratio(op.id) == compute_min_share_ratio(min_share_spec)

            op.resume_jobs()
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
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/adjusted_fair_share_starvation_tolerance".format(op_id))

        assert get_operation_tolerance(op1.id) == 0.4
        assert get_operation_tolerance(op2.id) == 0.6
        assert get_operation_tolerance(op3.id) == 0.8
        assert get_operation_tolerance(op4.id) == 0.9

        op1.abort();
        op2.abort();


class TestSchedulerAggressivePreemption(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_preemption_timeout": 100,
            "min_share_preemption_timeout": 100,
            "fair_share_update_period": 100,
            "aggressive_preemption_satisfaction_threshold": 0.2
        }
    }

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
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/fair_share_ratio".format(op_id))

        get_usage_ratio = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/usage_ratio".format(op_id))

        get_running_job_count = lambda op_id: \
            len(get("//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op_id)))

        ops = []
        for index in xrange(2):
            create("table", "//tmp/t_out" + str(index))
            op = map(dont_track=True, command="sleep 1000; cat", in_=["//tmp/t_in"], out="//tmp/t_out" + str(index),
                    spec={"pool": "fake_pool" + str(index), "job_count": 3, "locality_timeout": 0, "mapper": {"memory_limit": 10 * 1024 * 1024}})
            ops.append(op)
        time.sleep(3)

        for op in ops:
            assert assert_almost_equal(get_fair_share_ratio(op.id), 1.0 / 2.0)
            assert assert_almost_equal(get_usage_ratio(op.id), 1.0 / 2.0)
            assert get_running_job_count(op.id) == 3

        op = map(dont_track=True, command="sleep 1000; cat", in_=["//tmp/t_in"], out="//tmp/t_out",
                 spec={"pool": "special_pool", "job_count": 1, "locality_timeout": 0, "mapper": {"cpu_limit": 2}})
        time.sleep(3)

        assert assert_almost_equal(get_fair_share_ratio(op.id), 1.0 / 3.0)
        assert assert_almost_equal(get_usage_ratio(op.id), 1.0 / 3.0)
        assert get_running_job_count(op.id) == 1

class TestSchedulerAggressiveStarvationPreemption(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 6
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_preemption_timeout": 100,
            "min_share_preemption_timeout": 100,
            "fair_share_update_period": 100,
            "aggressive_preemption_satisfaction_threshold": 0.2
        }
    }

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
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/fair_share_ratio".format(op_id))

        get_usage_ratio = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/usage_ratio".format(op_id))

        get_running_jobs = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op_id))

        get_running_job_count = lambda op_id: len(get_running_jobs(op_id))

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

        time.sleep(3)

        for op in ops:
            assert assert_almost_equal(get_fair_share_ratio(op.id), 1.0 / 4.0)
            assert assert_almost_equal(get_usage_ratio(op.id), 1.0 / 4.0)
            assert get_running_job_count(op.id) == 3

        special_op = ops[0]
        special_op_jobs = [
            {
                "id": key,
                "start_time": datetime_str_to_ts(value["start_time"])
            }
            for key, value in get_running_jobs(special_op.id).iteritems()]

        special_op_jobs.sort(key=lambda x: x["start_time"])
        preemtable_job_id = special_op_jobs[-1]["id"]

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

        time.sleep(3)

        assert get_running_job_count(op.id) == 1

        special_op_running_job_count = get_running_job_count(special_op.id)
        assert special_op_running_job_count >= 2
        if special_op_running_job_count == 2:
            assert preemtable_job_id not in get_running_jobs(special_op.id)


class TestSchedulerHeterogeneousConfiguration(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @classmethod
    def modify_node_config(cls, config):
        if not hasattr(cls, "node_counter"):
            cls.node_counter = 0
        cls.node_counter += 1
        if cls.node_counter == 1:
            config["exec_agent"]["job_controller"]["resource_limits"]["user_slots"] = 0

    def test_job_count(self):
        data = [{"foo": i} for i in xrange(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        assert get("//sys/scheduler/orchid/scheduler/cell/resource_limits/user_slots") == 2
        assert get("//sys/scheduler/orchid/scheduler/cell/resource_usage/user_slots") == 0

        op = map(
            dont_track=True,
            command="sleep 100",
            in_="//tmp/in",
            out="//tmp/out",
            spec={"data_size_per_job": 1, "locality_timeout": 0})

        time.sleep(2)

        assert get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/resource_usage/user_slots".format(op.id)) == 2
        assert get("//sys/scheduler/orchid/scheduler/cell/resource_limits/user_slots") == 2
        assert get("//sys/scheduler/orchid/scheduler/cell/resource_usage/user_slots") == 2

        op.abort()

class TestSchedulerJobStatistics(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "scheduler_connector": {
                "heartbeat_period": 100 # 100 msec
            }
        }
    }

    def _create_table(self, table):
        create("table", table)
        set(table + "/@replication_factor", 1)

    def test_scheduler_job_by_id(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out")
        write_table("//tmp/in", [{"foo": i} for i in xrange(10)])
        op = map(
            dont_track=True,
            wait_for_jobs=True,
            label="scheduler_job_statistics",
            in_="//tmp/in",
            out="//tmp/out",
            command="cat")

        running_jobs = get("//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op.id))
        job_id = running_jobs.keys()[0]
        job_info = running_jobs.values()[0]

        # Check that /jobs is accessible only with direct job id.
        with pytest.raises(YtError):
            get("//sys/scheduler/orchid/scheduler/jobs")
        with pytest.raises(YtError):
            ls("//sys/scheduler/orchid/scheduler/jobs")

        job_info2 = get("//sys/scheduler/orchid/scheduler/jobs/{0}".format(job_id))
        # Check that job_info2 contains all the keys that are in job_info (do not check the same
        # for values because values could actually change between two get requests).
        for key in job_info:
            assert key in job_info2

    def test_scheduler_job_statistics(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out")
        write_table("//tmp/in", [{"foo": i} for i in xrange(10)])

        op = map(
            dont_track=True,
            wait_for_jobs=True,
            label="scheduler_job_statistics",
            in_="//tmp/in",
            out="//tmp/out",
            command="cat")

        running_jobs = get("//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op.id))
        job_id = running_jobs.keys()[0]

        statistics_appeared = False
        for iter in xrange(30):
            statistics = get("//sys/scheduler/orchid/scheduler/jobs/{0}/statistics".format(job_id))
            data = statistics.get("data", {})
            _input = data.get("input", {})
            row_count = _input.get("row_count", {})
            _sum = row_count.get("sum", 0)
            if _sum == 10:
                statistics_appeared = True
                break
            time.sleep(1.0)

        assert statistics_appeared

        op.resume_jobs()
        op.track()

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
                "heartbeat_period": 100 # 100 msec
            },
            "job_proxy_heartbeat_period": 100, # 100 msec
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
            "suspicious_inactivity_timeout": 2000, # 2 sec
            "running_jobs_update_period": 100 # 100 msec
        }
    }

    @require_ytserver_root_privileges
    def test_false_suspicious_jobs(self):
        def get_running_jobs(op_id):
            path = "//sys/scheduler/orchid/scheduler/operations/" + op_id
            if not exists(path, verbose=False):
                return []
            else:
                return get(path + "/running_jobs", verbose=False)

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
            running_jobs1 = get_running_jobs(op1.id)
            running_jobs2 = get_running_jobs(op2.id)
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

        assert not suspicious1
        assert not suspicious2

        op1.abort()
        op2.abort()

    @pytest.mark.xfail(reason="TODO(max42)")
    @require_ytserver_root_privileges
    def test_true_suspicious_job(self):
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
                running_jobs = get("//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op.id))
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


class TestSchedulerAlerts(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "alerts_update_period": 100,
            "watchers_update_period": 100,
            "fair_share_update_period": 100,
            "cluster_directory_synchronizer_check_period": 100,
        }
    }

    def test_pools(self):
        assert get("//sys/scheduler/@alerts") == []

        # Incorrect pool configuration.
        create("map_node", "//sys/pools/poolA", attributes={"min_share_ratio": 2.0})

        time.sleep(0.5)
        assert len(get("//sys/scheduler/@alerts")) == 1

        set("//sys/pools/poolA/@min_share_ratio", 0.8)

        time.sleep(0.5)
        assert get("//sys/scheduler/@alerts") == []

        # Total min_share_ratio > 1.
        create("map_node", "//sys/pools/poolB", attributes={"min_share_ratio": 0.8})

        time.sleep(0.5)
        assert len(get("//sys/scheduler/@alerts")) == 1

        set("//sys/pools/poolA/@min_share_ratio", 0.1)

        time.sleep(0.5)
        assert get("//sys/scheduler/@alerts") == []

    def test_config(self):
        assert get("//sys/scheduler/@alerts") == []

        set("//sys/scheduler/config", {"fair_share_update_period": -100})

        time.sleep(0.5)
        assert len(get("//sys/scheduler/@alerts")) == 1

        set("//sys/scheduler/config", {})

        time.sleep(0.5)
        assert get("//sys/scheduler/@alerts") == []

    def test_cluster_directory(self):
        assert get("//sys/scheduler/@alerts") == []

        set("//sys/clusters/banach", {})

        time.sleep(1.0)
        assert len(get("//sys/scheduler/@alerts")) == 1

        set("//sys/clusters", {})

        time.sleep(1.0)
        assert get("//sys/scheduler/@alerts") == []


class TestSchedulerCaching(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "get_exec_nodes_information_delay": 3000,
        }
    }

    def test_exec_node_descriptors_caching(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        write_table("//tmp/t_in", [{"foo": i} for i in xrange(10)])

        op = map(dont_track=True, command='cat', in_="//tmp/t_in", out="//tmp/t_out")
        op.track()

class TestSecureVault(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    secure_vault = {
        "int64": 42424243,
        "uint64": yson.YsonUint64(1234),
        "string": "penguin",
        "boolean": True,
        "double": 3.14,
        "composite": {"token1": "SeNsItIvE", "token2": "InFo"},
    }

    def run_map_with_secure_vault(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")
        op = map(
            dont_track=True,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"secure_vault": self.secure_vault, "max_failed_job_count": 1},
            command="""
                echo {YT_SECURE_VAULT=$YT_SECURE_VAULT}\;;
                echo {YT_SECURE_VAULT_int64=$YT_SECURE_VAULT_int64}\;;
                echo {YT_SECURE_VAULT_uint64=$YT_SECURE_VAULT_uint64}\;;
                echo {YT_SECURE_VAULT_string=$YT_SECURE_VAULT_string}\;;
                echo {YT_SECURE_VAULT_boolean=$YT_SECURE_VAULT_boolean}\;;
                echo {YT_SECURE_VAULT_double=$YT_SECURE_VAULT_double}\;;
                echo {YT_SECURE_VAULT_composite=\\"$YT_SECURE_VAULT_composite\\"}\;;
           """)
        return op

    def check_content(self, res):
        assert len(res) == 7
        assert res[0] == {"YT_SECURE_VAULT": self.secure_vault}
        assert res[1] == {"YT_SECURE_VAULT_int64": self.secure_vault["int64"]}
        assert res[2] == {"YT_SECURE_VAULT_uint64": self.secure_vault["uint64"]}
        assert res[3] == {"YT_SECURE_VAULT_string": self.secure_vault["string"]}
        # Boolean values are represented with 0/1.
        assert res[4] == {"YT_SECURE_VAULT_boolean": 1}
        assert res[5] == {"YT_SECURE_VAULT_double": self.secure_vault["double"]}
        # Composite values are not exported as separate environment variables.
        assert res[6] == {"YT_SECURE_VAULT_composite": ""}


    def test_secure_vault_not_visible(self):
        op = self.run_map_with_secure_vault()
        cypress_info = str(get("//sys/operations/{0}/@".format(op.id)))
        scheduler_info = str(get("//sys/scheduler/orchid/scheduler/operations/{0}".format(op.id)))
        op.track()

        # Check that secure environment variables is neither presented in the Cypress node of the
        # operation nor in scheduler Orchid representation of the operation.
        for info in [cypress_info, scheduler_info]:
            for sensible_text in ["42424243", "SeNsItIvE", "InFo"]:
                assert info.find(sensible_text) == -1

    def test_secure_vault_simple(self):
        op = self.run_map_with_secure_vault()
        op.track()
        res = read_table("//tmp/t_out")
        self.check_content(res)

    def test_secure_vault_with_revive(self):
        op = self.run_map_with_secure_vault()
        self.Env.kill_schedulers()
        self.Env.start_schedulers()
        op.track()
        res = read_table("//tmp/t_out")
        self.check_content(res)

    def test_allowed_variable_names(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")
        with pytest.raises(YtError):
            map(dont_track=True,
                in_="//tmp/t_in",
                out="//tmp/t_out",
                spec={"secure_vault": {"=_=": 42}},
                command="cat")
        with pytest.raises(YtError):
            map(dont_track=True,
                in_="//tmp/t_in",
                out="//tmp/t_out",
                spec={"secure_vault": {"x" * (2**16 + 1): 42}},
                command="cat")

class TestSafeAssertionsMode(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "enable_controller_failure_spec_option": True,
        },
        "core_dumper": {
            "component_name": "",
            "path": "/dev/null",
        }
    }

    @unix_only
    def test_assertion_failure(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")

        op = map(
            dont_track=True,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"testing": {"controller_failure": "assertion_failure_in_prepare"}},
            command="cat")
        with pytest.raises(YtError) as excinfo:
            op.track()

        assert len(get("//sys/scheduler/orchid/profiling/controller_agent/assertions_failed")) == 1

        op = map(
            dont_track=True,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"testing": {"controller_failure": "exception_thrown_in_on_job_completed"}},
            command="cat")
        with pytest.raises(YtError) as excinfo:
            op.track()

        # Note that exception in on job completed is not a failed assertion, so it doesn't affect this counter.
        assert len(get("//sys/scheduler/orchid/profiling/controller_agent/assertions_failed")) == 1

        op = map(
            dont_track=True,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"testing": {"controller_failure": "assertion_failure_in_prepare"}},
            command="cat")
        with pytest.raises(YtError) as excinfo:
            op.track()

        assert len(get("//sys/scheduler/orchid/profiling/controller_agent/assertions_failed")) == 2

class TestMaxTotalSliceCount(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "max_total_slice_count": 3,
        }
    }

    @unix_only
    def test_hit_limit(self):
        create("table", "//tmp/t_primary")
        write_table("//tmp/t_primary", [
            {"key": 0},
            {"key": 10}], sorted_by=['key'])

        create("table", "//tmp/t_foreign")
        write_table("<append=true; sorted_by=[key]>//tmp/t_foreign", [{"key": 0}, {"key": 1}, {"key": 2}])
        write_table("<append=true; sorted_by=[key]>//tmp/t_foreign", [{"key": 3}, {"key": 4}, {"key": 5}])
        write_table("<append=true; sorted_by=[key]>//tmp/t_foreign", [{"key": 6}, {"key": 7}, {"key": 8}])

        create("table", "//tmp/t_out")
        with pytest.raises(YtError):
            join_reduce(
                in_=["//tmp/t_primary", "<foreign=true>//tmp/t_foreign"],
                out="//tmp/t_out",
                join_by=["key"],
                command="cat > /dev/null")

class TestSchedulerOperationAlerts(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 3

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "scheduler_connector": {
                "heartbeat_period": 200
            },
            "slot_manager": {
                "job_environment": {
                    "type": "cgroups",
                    "supported_cgroups": ["blkio"],
                    "block_io_watchdog_period": 100
                }
            }
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operation_progress_analysis_period": 200,
            "operations_update_period": 100,
            "operation_alerts": {
                "tmpfs_alert_min_unused_space_threshold": 200,
                "tmpfs_alert_max_unused_space_ratio": 0.3,
                "aborted_jobs_alert_max_aborted_time": 100,
                "aborted_jobs_alert_aborted_time_ratio": 0.05,
                "intermediate_data_skew_alert_min_interquartile_range": 50,
                "intermediate_data_skew_alert_min_partition_size": 50,
                "short_jobs_alert_min_job_count": 3,
                "short_jobs_alert_min_job_duration": 5000
            },
            "iops_threshold": 50,
            "map_reduce_operation_options": {
                "min_uncompressed_block_size": 1
            },
            "schedule_job_time_limit": 3000,
        }
    }

    @require_ytserver_root_privileges
    @unix_only
    def test_unused_tmpfs_size_alert(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", [{"x": "y"}])

        op = map(
            command="echo abcdef >local_file; sleep 1.5; cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 5 * 1024 * 1024,
                    "tmpfs_path": "."
                }
            })

        assert "unused_tmpfs_space" in get("//sys/operations/{0}/@alerts".format(op.id))

        op = map(
            command="printf '=%.0s' {1..768} >local_file; sleep 1.5; cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024,
                    "tmpfs_path": "."
                }
            })

        assert "unused_tmpfs_space" not in get("//sys/operations/{0}/@alerts".format(op.id))

    def test_missing_input_chunks_alert(self):
        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out")
        write_table("//tmp/t_in", [{"x": "y"}])

        chunk_ids = get("//tmp/t_in/@chunk_ids")
        assert len(chunk_ids) == 1

        replicas = get("#{0}/@stored_replicas".format(chunk_ids[0]))
        set_banned_flag(True, replicas)

        op = map(
            command="sleep 1.5; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "unavailable_chunk_strategy": "wait",
                "unavailable_chunk_tactics": "wait"
            },
            dont_track=True)

        time.sleep(1.0)
        assert "lost_input_chunks" in get("//sys/operations/{0}/@alerts".format(op.id))

        set_banned_flag(False, replicas)
        time.sleep(1.0)

        assert "lost_input_chunks" not in get("//sys/operations/{0}/@alerts".format(op.id))

    @pytest.mark.skipif("True", reason="YT-6717")
    def test_woodpecker_jobs_alert(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": str(i)} for i in xrange(7)])
        create("table", "//tmp/t_out")

        cmd = "set -e; echo aaa >local_file; for i in {1..200}; do " \
              "dd if=./local_file of=/dev/null iflag=direct bs=1M count=1; done;"

        op = map(
            command=cmd,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "data_size_per_job": 1,
                "resource_limits": {
                    "user_slots": 1
                }
            })

        assert "excessive_disk_usage" in get("//sys/operations/{0}/@alerts".format(op.id))

    @flaky(max_runs=3)
    def test_long_aborted_jobs_alert(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": str(i)} for i in xrange(5)])
        create("table", "//tmp/t_out")

        op = map(
            command="sleep 100; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "data_size_per_job": 1,
            },
            dont_track=True)

        operation_orchid_path = "//sys/scheduler/orchid/scheduler/operations/" + op.id
        running_jobs_count_path = operation_orchid_path + "/progress/jobs/running"

        def running_jobs_exists():
            return exists(running_jobs_count_path) and get(running_jobs_count_path) >= 1

        wait(running_jobs_exists)

        for job in ls(operation_orchid_path + "/running_jobs"):
            abort_job(job)

        time.sleep(1.5)

        assert "long_aborted_jobs" in get("//sys/operations/{0}/@alerts".format(op.id))

        abort_op(op.id)

    def test_intermediate_data_skew_alert(self):
        create("table", "//tmp/t_in")

        mutliplier = 1
        data = []
        for letter in ["a", "b", "c", "d", "e"]:
            data.extend([{"x": letter} for _ in xrange(mutliplier)])
            mutliplier *= 10

        write_table("//tmp/t_in", data)

        create("table", "//tmp/t_out")

        op = map_reduce(
            mapper_command="cat",
            reducer_command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by=["x"],
            spec={"partition_count": 5})

        assert "intermediate_data_skew" in get("//sys/operations/{0}/@alerts".format(op.id))

    @flaky(max_runs=3)
    def test_short_jobs_alert(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": str(i)} for i in xrange(4)])
        create("table", "//tmp/t_out")

        op = map(
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "data_size_per_job": 1,
            })

        assert "short_jobs_duration" in get("//sys/operations/{0}/@alerts".format(op.id))

        op = map(
            command="sleep 5; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "data_size_per_job": 1,
            })

        assert "short_jobs_duration" not in get("//sys/operations/{0}/@alerts".format(op.id))

    def test_schedule_job_timed_out_alert(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"x": "y"}])
        create("table", "//tmp/t_out")

        testing_options = {"scheduling_delay": 3500, "scheduling_delay_type": "async"}

        op = map(
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "testing": testing_options,
            },
            dont_track=True)

        time.sleep(8)

        assert "schedule_job_timed_out" in get("//sys/operations/{0}/@alerts".format(op.id))

        op.abort()


class TestMainNodesFilter(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "main_nodes_filter": "internal",
        }
    }

    def test_main_nodes_resources_limits(self):
        nodes = ls("//sys/nodes")
        assert len(nodes) == 2
        set("//sys/nodes/{0}/@user_tags".format(nodes[0]), ["internal"])

        time.sleep(2)

        assert get("//sys/scheduler/orchid/scheduler/cell/resource_limits/user_slots", 2)
        assert get("//sys/scheduler/orchid/scheduler/cell/main_nodes_resource_limits/user_slots", 1)

        create("table", "//tmp/input", attributes={"replication_factor": 1})
        create("table", "//tmp/output", attributes={"replication_factor": 1})
        write_table("//tmp/input", [{"foo": i} for i in xrange(2)])
        op = map(
            dont_track=True,
            wait_for_jobs=True,
            command="sleep 10",
            in_="//tmp/input",
            out="//tmp/output",
            spec={"data_size_per_job": 1})

        time.sleep(1)

        assert get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/jobs/running".format(op.id)) == 2
        assert assert_almost_equal(get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/fair_share_ratio".format(op.id)), 1.0)
        assert assert_almost_equal(get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/usage_ratio".format(op.id)), 2.0)

class TestNewPoolMetrics(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "job_metrics_batch_interval": 3000, # 3 sec
            "fair_share_update_period": 100,
            "profiling_update_period": 100,
            "fair_share_profiling_period": 100,
        },
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "enable_cgroups": True,
            "supported_cgroups": ["cpuacct", "blkio", "memory", "cpu"],
            "slot_manager": {
                "enforce_job_control": True,
                "job_environment" : {
                    "type" : "cgroups",
                    "supported_cgroups": [
                        "cpuacct",
                        "blkio",
                        "memory",
                        "cpu"],
                },
            },
            "scheduler_connector": {
                "heartbeat_period": 100, # 100 msec
            },
        }
    }

    @unix_only
    def test_map(self):
        create("map_node", "//sys/pools/parent")
        create("map_node", "//sys/pools/parent/child1")
        create("map_node", "//sys/pools/parent/child2")

        # Give scheduler some time to apply new pools.
        time.sleep(1)

        create("table", "//t_input")
        create("table", "//t_output")

        # write table of 2 chunks because we want 2 jobs
        write_table("//t_input", [{"key": i} for i in xrange(0, 100)])
        write_table("<append=%true>//t_input", [{"key": i} for i in xrange(100, 500)])

        # our command does the following
        # - writes (and syncs) something to disk
        # - works for some time (to ensure that it sends several heartbeats
        # - writes something to stderr because we want to find our jobs in //sys/operations later
        map_cmd = """for i in $(seq 10) ; do echo 5 > foo$i ; sync ; sleep 0.5 ; done ; cat ; echo done > /dev/stderr"""

        op11 = map(
            in_="//t_input",
            out="//t_output",
            wait_for_jobs=False,
            command=map_cmd,
            spec={"job_count": 2, "pool": "child1"},
        )
        op12 = map(
            in_="//t_input",
            out="//t_output",
            wait_for_jobs=False,
            command=map_cmd,
            spec={"job_count": 2, "pool": "child1"},
        )

        op2 = map(
            in_="//t_input",
            out="//t_output",
            wait_for_jobs=False,
            command=map_cmd,
            spec={"job_count": 2, "pool": "child2"},
        )
        # Give scheduler some time to update metrics in the orchid.
        time.sleep(1)

        pool_metrics = get_pool_metrics("disk_writes")

        op11_writes = get_cypress_metrics(op11.id, "user_job.block_io.io_write")
        op12_writes = get_cypress_metrics(op12.id, "user_job.block_io.io_write")
        op2_writes = get_cypress_metrics(op2.id, "user_job.block_io.io_write")

        assert pool_metrics["child1"] == op11_writes + op12_writes > 0
        assert pool_metrics["child2"] == op2_writes > 0
        assert pool_metrics["parent"] == op11_writes + op12_writes + op2_writes > 0

        jobs_11 = ls("//sys/operations/{0}/jobs".format(op11.id))
        assert len(jobs_11) >= 2

class TestSchedulerJobSpecThrottlerOperationAlert(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operation_progress_analysis_period": 100,
            "operations_update_period": 100,
            "heavy_job_spec_slice_count_threshold": 1,
            "job_spec_slice_throttler": {
                "limit": 3,
                "period": 1000
            },
            "operation_alerts": {
                "job_spec_throttling_alert_activation_count_threshold": 1
            }
        }
    }

    def test_job_spec_throttler_operation_alert(self):
        create("table", "//tmp/t_in")
        for letter in string.ascii_lowercase:
            write_table("<append=%true>//tmp/t_in", [{"x": letter}])

        create("table", "//tmp/t_out")
        create("table", "//tmp/t_out2")

        op1 = map(
            command="sleep 100; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            dont_track=True)

        op2 = map(
            command="sleep 1; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out2",
            dont_track=True)

        time.sleep(1.5)

        assert "excessive_job_spec_throttling" in get("//sys/operations/{0}/@alerts".format(op2.id))

        op1.abort()
        op2.abort()

class TestMinNeededResources(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "safe_scheduler_online_time": 500,
            "min_needed_resources_update_period": 200
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

        assert get(op1_path + "/progress/schedule_job_statistics/count") > 0

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
        assert get(op2_path + "/progress/schedule_job_statistics/count") == 0

        abort_op(op1.id)

        op2.track()
