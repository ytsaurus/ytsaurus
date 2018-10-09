import pytest

from yt_env_setup import YTEnvSetup, wait
from yt_commands import *

from flaky import flaky

import time
from datetime import datetime, timedelta

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

class TestSchedulerFunctionality(YTEnvSetup, PrepareTables):
    NUM_MASTERS = 3
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "profiling_update_period": 100,
            "fair_share_profiling_period": 100,
            "alerts_update_period": 100,
            # Unrecognized alert often interferes with the alerts that
            # are tested in this test suite.
            "enable_unrecognized_alert": False
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operation_time_limit_check_period": 100,
            "operation_controller_fail_timeout": 3000,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "slot_manager": {
                "job_environment": {
                    "type": "cgroups",
                    "memory_watchdog_period": 100,
                    "supported_cgroups": ["cpuacct", "blkio", "cpu"],
                },
            }
        }
    }

    @flaky(max_runs=3)
    def test_revive(self):
        def get_connection_time():
            return datetime.strptime(get("//sys/scheduler/@connection_time"), "%Y-%m-%dT%H:%M:%S.%fZ")

        self._prepare_tables()

        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="echo '{foo=bar}'; sleep 4")

        time.sleep(3)

        assert datetime.utcnow() - get_connection_time() > timedelta(seconds=3)

        self.Env.kill_schedulers()
        self.Env.start_schedulers()

        assert datetime.utcnow() - get_connection_time() < timedelta(seconds=3)

        op.track()

        assert read_table("//tmp/t_out") == [{"foo": "bar"}]

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
            assert read_table("//tmp/t_out" + str(i)) == [{"foo": "bar"}]

    def test_user_transaction_abort_when_scheduler_is_down(self):
        self._prepare_tables()

        transaction_id = start_transaction(timeout=300 * 1000)
        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="echo '{foo=bar}'; sleep 10", transaction_id=transaction_id)

        time.sleep(2)
        self.Env.kill_schedulers()

        abort_transaction(transaction_id)

        self.Env.start_schedulers()

        with pytest.raises(YtError):
            op.track()

    def test_scheduler_transaction_abort_when_scheduler_is_down(self):
        self._prepare_tables()

        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="echo '{foo=bar}'; sleep 3")

        time.sleep(2)
        self.Env.kill_schedulers()

        abort_transaction(get("//sys/operations/{0}/@input_transaction_id".format(op.id)))
        abort_transaction(get("//sys/operations/{0}/@output_transaction_id".format(op.id)))

        self.Env.start_schedulers()

        op.track()

        assert read_table("//tmp/t_out") == [{"foo": "bar"}]

    def test_suspend_during_revive(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        op = map(dont_track=True,
            command="sleep 1000",
            in_=["//tmp/in"],
            out="//tmp/out")
        wait(lambda: op.get_state() == "running")

        op.suspend()
        wait(lambda: get(op.get_path() + "/@suspended"))

        self.Env.kill_schedulers()
        self.Env.start_schedulers()

        time.sleep(2)
        wait(lambda: op.get_state() == "running")
        wait(lambda: op.get_job_count("running") == 0)

        assert get(op.get_path() + "/@suspended")

        op.resume()
        wait(lambda: op.get_job_count("running") == 1)

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

        wait(lambda: get("//sys/operations/{0}/@suspended".format(op.id)), iter=100, sleep_backoff=0.6)

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

    def test_suspend_operation_after_materialization(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out")
        write_table("//tmp/in", [{"foo": 0}])

        op = map(dont_track=True,
                 command="cat",
                 in_="//tmp/in",
                 out="//tmp/out",
                 spec={
                     "data_size_per_job": 1,
                     "suspend_operation_after_materialization": True
                 })
        wait(lambda: get("//sys/operations/{0}/@suspended".format(op.id)))
        op.resume()
        op.track()

    def test_fail_context_saved_on_time_limit(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out")

        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        op = map(dont_track=True,
            command="sleep 1000.0; cat >/dev/null",
            in_=["//tmp/in"],
            out="//tmp/out",
            spec={"time_limit": 2000})

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
            assert get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/fifo_index".format(op.id)) == 2 - index

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
                for i in xrange(10):
                    try:
                        abort_transaction(tx)
                    except YtResponseError as err:
                        if err.is_no_such_transaction():
                            break
                        if i == 9:
                            raise

        with pytest.raises(YtError):
            op.track()

        set_banned_flag(False)

    def test_abort_custom_error_message(self):
        self._prepare_tables()

        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="echo '{foo=bar}'; sleep 3")
        op.abort(abort_message="Test abort")

        assert op.get_state() == "aborted"
        assert get("//sys/operations/{0}/@result/error/inner_errors/0/message".format(op.id)) == "Test abort"

    def test_operation_pool_attributes(self):
        self._prepare_tables()

        op = map(in_="//tmp/t_in", out="//tmp/t_out", command="cat")
        assert get("//sys/operations/{0}/@runtime_parameters/scheduling_options_per_pool_tree/default/pool".format(op.id)) == "root"

    def test_operation_events_attribute(self):
        self._prepare_tables()

        op = map(in_="//tmp/t_in", out="//tmp/t_out", command="cat")
        events = get("//sys/operations/{0}/@events".format(op.id))
        assert [
                   "starting",
                   "waiting_for_agent",
                   "initializing",
                   "preparing",
                   "pending",
                   "materializing",
                   "running",
                   "completing",
                   "completed"
               ] == [event["state"] for event in events]

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

    @flaky(max_runs=3)
    def test_within_job_time_limit(self):
        self._prepare_tables()
        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="sleep 1 ; cat",
            spec={"max_failed_job_count": 1, "mapper": {"job_time_limit": 3000}})

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
        wait(lambda: op1.get_job_count("running") == 1)
        op2 = map(command="sleep 1000; cat", in_="//tmp/t_in", out="//tmp/t_out_2", spec={"pool": "some_pool"}, dont_track=True)
        wait(lambda: op2.get_state() == "running")

        get_slot_index = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/slot_index".format(op_id))

        assert get_slot_index(op1.id) == 0
        assert get_slot_index(op2.id) == 1

        range_ = (49999, 50000, 50001)

        wait(lambda: self._get_operation_last_metric_value("fair_share_ratio_x100000", "some_pool", 0) in range_)
        wait(lambda: self._get_operation_last_metric_value("usage_ratio_x100000", "some_pool", 0) == 100000)
        wait(lambda: self._get_operation_last_metric_value("demand_ratio_x100000", "some_pool", 0) == 100000)
        wait(lambda: self._get_operation_last_metric_value("guaranteed_resource_ratio_x100000", "some_pool", 0) in range_)
        wait(lambda: self._get_operation_last_metric_value("resource_usage/cpu", "some_pool", 0) == 1)
        wait(lambda: self._get_operation_last_metric_value("resource_usage/user_slots", "some_pool", 0) == 1)
        wait(lambda: self._get_operation_last_metric_value("resource_demand/cpu", "some_pool", 0) == 1)
        wait(lambda: self._get_operation_last_metric_value("resource_demand/user_slots", "some_pool", 0) == 1)

        wait(lambda: self._get_operation_last_metric_value("fair_share_ratio_x100000", "some_pool", 1) in range_)
        wait(lambda: self._get_operation_last_metric_value("usage_ratio_x100000", "some_pool", 1) == 0)
        wait(lambda: self._get_operation_last_metric_value("demand_ratio_x100000", "some_pool", 1) == 100000)
        wait(lambda: self._get_operation_last_metric_value("guaranteed_resource_ratio_x100000", "some_pool", 1) in range_)
        wait(lambda: self._get_operation_last_metric_value("resource_usage/cpu", "some_pool", 1) == 0)
        wait(lambda: self._get_operation_last_metric_value("resource_usage/user_slots", "some_pool", 1) == 0)
        wait(lambda: self._get_operation_last_metric_value("resource_demand/cpu", "some_pool", 1) == 1)
        wait(lambda: self._get_operation_last_metric_value("resource_demand/user_slots", "some_pool", 1) == 1)

        op1.abort()

        time.sleep(2.0)

        wait(lambda: self._get_operation_last_metric_value("fair_share_ratio_x100000", "some_pool", 1) == 100000)
        wait(lambda: self._get_operation_last_metric_value("usage_ratio_x100000", "some_pool", 1) == 100000)
        wait(lambda: self._get_operation_last_metric_value("demand_ratio_x100000", "some_pool", 1) == 100000)
        wait(lambda: self._get_operation_last_metric_value("guaranteed_resource_ratio_x100000", "some_pool", 1) == 100000)

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

##################################################################

class SchedulerReviveBase(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "testing_options": {
                "finish_operation_transition_delay": 2000,
            },
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
            "operation_time_limit_check_period": 100,
            "operation_build_progress_period": 100,
        }
    }

    def _create_table(self, table):
        create("table", table, attributes={"replication_factor": 1})

    def _prepare_tables(self):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        self._create_table("//tmp/t_out")

    def _wait_for_state(self, op, state):
        wait(lambda: get("//sys/operations/" + op.id + "/@state") == state)

    def test_missing_transactions(self):
        self._prepare_tables()

        op = self._start_op(with_breakpoint("echo '{foo=bar}'; BREAKPOINT"), dont_track=True)

        for iter in xrange(5):
            self._wait_for_state(op, "running")
            self.Env.kill_schedulers()
            set("//sys/operations/" + op.id + "/@input_transaction_id", "0-0-0-0")
            self.Env.start_schedulers()
            time.sleep(1)

        release_breakpoint()
        op.track()

        assert "completed" == get("//sys/operations/" + op.id + "/@state")

    def test_aborting(self):
        self._prepare_tables()

        op = self._start_op("echo '{foo=bar}'; sleep 10", dont_track=True)

        self._wait_for_state(op, "running")

        op.abort(ignore_result=True)

        self._wait_for_state(op, "aborting")

        self.Env.kill_schedulers()

        assert "aborting" == get("//sys/operations/" + op.id + "/@state")

        self.Env.start_schedulers()

        with pytest.raises(YtError):
            op.track()

        assert "aborted" == get("//sys/operations/" + op.id + "/@state")

    # NB: we hope that complete finish first phase before we kill scheduler. But we cannot guarantee that this happen.
    @flaky(max_runs=3)
    def test_completing(self):
        self._prepare_tables()

        op = self._start_op("echo '{foo=bar}'; sleep 10", dont_track=True)

        self._wait_for_state(op, "running")

        op.complete(ignore_result=True)

        self._wait_for_state(op, "completing")

        self.Env.kill_schedulers()

        assert "completing" == get("//sys/operations/" + op.id + "/@state")

        self.Env.start_schedulers()

        op.track()

        assert "completed" == get("//sys/operations/" + op.id + "/@state")

        if self.OP_TYPE == "map":
            assert read_table("//tmp/t_out") == []

    # NB: test rely on timings and can flap if we hang at some point.
    @flaky(max_runs=3)
    @pytest.mark.parametrize("stage", ["stage" + str(index) for index in xrange(1, 8)])
    def test_completing_with_sleep(self, stage):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", [{"foo": "bar"}] * 2)

        self._create_table("//tmp/t_out")

        op = self._start_op("echo '{foo=bar}'; if [ \"$YT_JOB_INDEX\" != \"0\" ]; then sleep 100; fi;",
                 dont_track=True,
                 spec={
                     "testing": {
                         "delay_inside_operation_commit": 5000,
                         "delay_inside_operation_commit_stage": stage,
                     },
                     "job_count": 2
                 })

        self._wait_for_state(op, "running")

        wait(lambda: op.get_job_count("completed") == 1 and op.get_job_count("running") == 1)

        # Wait for snapshot after job completion.
        time.sleep(3)

        # This request will be retried with the new incarnation of the scheduler.
        op.complete(ignore_result=True)

        self._wait_for_state(op, "completing")

        # Wait to perform complete before sleep.
        time.sleep(1.5)

        self.Env.kill_schedulers()

        assert get("//sys/operations/" + op.id + "/@state") == "completing"

        self.Env.start_schedulers()

        # complete_operation retry may come when operation is in reviving state. In this case we should complete operation again.
        wait(lambda: op.get_state() in ("running", "completed"))

        if op.get_state() == "running":
            op.complete()

        op.track()

        events = get("//sys/operations/{0}/@events".format(op.id))

        events_prefix = [
            "starting",
            "waiting_for_agent",
            "initializing",
            "preparing",
            "pending",
            "materializing",
            "running",
            "completing",
            "orphaned"
        ]
        if stage <= "stage5":
            expected_events = events_prefix + ["waiting_for_agent", "reviving", "pending", "reviving_jobs", "running", "completing", "completed"]
        else:
            expected_events = events_prefix + ["completed"]

        actual_events = [event["state"] for event in events]

        print >>sys.stderr, "Expected: ", expected_events
        print >>sys.stderr, "Actual:   ", actual_events
        assert expected_events == actual_events

        assert get("//sys/operations/" + op.id + "/@state") == "completed"

        if self.OP_TYPE == "map":
            assert read_table("//tmp/t_out") == [{"foo": "bar"}]

    def test_abort_during_complete(self):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", [{"foo": "bar"}] * 2)

        remove("//tmp/t_out", force=True)
        self._create_table("//tmp/t_out")

        op = self._start_op("echo '{foo=bar}'; if [ \"$YT_JOB_INDEX\" != \"0\" ]; then sleep 100; fi;", dont_track=True,
                 spec={
                     "testing": {
                         "delay_inside_operation_commit": 4000,
                         "delay_inside_operation_commit_stage": "stage4",
                     },
                     "job_count": 2
                 })

        self._wait_for_state(op, "running")

        # Wait for snapshot and job completion.
        time.sleep(3)

        op.complete(ignore_result=True)

        self._wait_for_state(op, "completing")

        # Wait to perform complete before sleep.
        time.sleep(2)

        op.abort()
        op.track()

        assert get("//sys/operations/" + op.id + "/@state") == "completed"

    def test_failing(self):
        self._prepare_tables()

        op = self._start_op("exit 1", dont_track=True, spec={"max_failed_job_count": 1})

        self._wait_for_state(op, "failing")

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

        op = self._start_op(
            "sleep 1; false",
            spec={"max_failed_job_count": 10000},
            dont_track=True)

        self._wait_for_state(op, "running")

        def failed_jobs_exist():
            return op.get_job_count("failed") >= 3

        wait(failed_jobs_exist)

        suspend_op(op.id)

        # Waiting until snapshot is built.
        time.sleep(2.0)

        self.Env.kill_schedulers()
        self.Env.start_schedulers()

        wait(lambda: op.get_job_count("failed") >= 3)

class TestSchedulerReviveMap(SchedulerReviveBase):
    OP_TYPE = "map"

    def _start_op(self, command, **kwargs):
        return map(command=command, in_=["//tmp/t_in"], out="//tmp/t_out", **kwargs)

class TestSchedulerReviveVanilla(SchedulerReviveBase):
    OP_TYPE = "vanilla"

    def _start_op(self, command, **kwargs):
        spec = kwargs.pop("spec", {})
        job_count = spec.pop("job_count", 1)
        spec["tasks"] = {"main": {"command": command, "job_count": job_count}}
        return vanilla(spec=spec, **kwargs)

class TestControllerAgent(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "controller_agent_tracker": {
                "heartbeat_timeout": 2000,
            },
            "testing_options": {
                "finish_operation_transition_delay": 2000,
            },
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
            "operation_time_limit_check_period": 100,
            "operation_build_progress_period": 100,
        }
    }

    def _create_table(self, table):
        create("table", table, attributes={"replication_factor": 1})

    def _wait_for_state(self, op, state):
        wait(lambda: get("//sys/operations/" + op.id + "/@state") == state)

    def test_abort_operation_without_controller_agent(self):
        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        for wait_transition_state in (False, True):
            for iter in xrange(2):
                op = map(
                    command="sleep 1000",
                    in_=["//tmp/t_in"],
                    out="//tmp/t_out",
                    dont_track=True)

                self._wait_for_state(op, "running")

                self.Env.kill_controller_agents()

                if wait_transition_state:
                    self._wait_for_state(op, "waiting_for_agent")

                op.abort()

                self.Env.start_controller_agents()

                self._wait_for_state(op, "aborted")

    def test_complete_operation_without_controller_agent(self):
        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        op = map(
            command="sleep 1000",
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            dont_track=True)
        self._wait_for_state(op, "running")

        self.Env.kill_controller_agents()

        with pytest.raises(YtError):
            op.complete()

        self.Env.start_controller_agents()

        self._wait_for_state(op, "running")
        op.complete()
        self._wait_for_state(op, "completed")

    def test_complete_operation_on_controller_agent_connection(self):
        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        op = map(
            command="sleep 1000",
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            spec={
                "testing": {
                    "delay_inside_revive": 10000,
                }
            },
            dont_track=True)
        self._wait_for_state(op, "running")

        snapshot_path = op.get_path() + "/snapshot"
        wait(lambda: exists(snapshot_path))

        self.Env.kill_controller_agents()
        self.Env.start_controller_agents()

        with pytest.raises(YtError):
            op.complete()

        self._wait_for_state(op, "running")
        op.complete()

        self._wait_for_state(op, "completed")

    def test_abort_operation_on_controller_agent_connection(self):
        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        op = map(
            command="sleep 1000",
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            spec={
                "testing": {
                    "delay_inside_revive": 10000,
                }
            },
            dont_track=True)
        self._wait_for_state(op, "running")

        self.Env.kill_controller_agents()
        self.Env.start_controller_agents()

        op.abort()
        self._wait_for_state(op, "aborted")
