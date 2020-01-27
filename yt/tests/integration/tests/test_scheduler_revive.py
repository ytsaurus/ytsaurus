from yt_env_setup import (
    YTEnvSetup, wait,
    Restarter, SCHEDULERS_SERVICE, CONTROLLER_AGENTS_SERVICE, NODES_SERVICE
)

from yt_commands import *
from yt_helpers import *

import pytest

import pprint
import random
import sys
import time


class TestSchedulerRevive(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "testing_options": {
                "enable_random_master_disconnection": False,
                "random_master_disconnection_max_backoff": 10000,
                "finish_operation_transition_delay": 1000,
            },
            "finished_job_storing_timeout": 15000,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operation_time_limit_check_period": 100,
            "snapshot_period": 3000,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "total_confirmation_period": 5000
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

    @authors("ignat")
    def test_many_operations(self):
        self._prepare_tables()

        ops = []
        for index in xrange(self.OP_COUNT):
            op = map(
                track=False,
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
                    assert op.get_state() == "failed"
        finally:
            set("//sys/scheduler/config", {"testing_options": {"enable_random_master_disconnection": False}})
            time.sleep(2)

    @authors("ignat")
    def test_many_operations_hard(self):
        self._prepare_tables()

        ops = []
        for index in xrange(self.OP_COUNT):
            op = map(
                track=False,
                command="sleep 20; echo 'AAA' >&2; cat",
                in_="//tmp/t_in",
                out="//tmp/t_out" + str(index),
                spec={
                    "stderr_table_path": "//tmp/t_err" + str(index),
                    "testing": {
                        "delay_inside_revive": 2000,
                    }
                })
            ops.append(op)

        try:
            set("//sys/scheduler/config", {
                "testing_options": {
                    "enable_random_master_disconnection": True,
                }
            })
            for index, op in enumerate(ops):
                try:
                    op.track()
                    assert read_table("//tmp/t_out" + str(index)) == [{"foo": "bar"}]
                except YtError:
                    assert op.get_state() == "failed"
        finally:
            set("//sys/scheduler/config", {"testing_options": {"enable_random_master_disconnection": False}})
            time.sleep(2)

    @authors("ignat")
    def test_many_operations_controller_disconnections(self):
        self._prepare_tables()

        ops = []
        for index in xrange(self.OP_COUNT):
            op = map(
                track=False,
                command="sleep 20; echo 'AAA' >&2; cat",
                in_="//tmp/t_in",
                out="//tmp/t_out" + str(index),
                spec={
                    "stderr_table_path": "//tmp/t_err" + str(index),
                    "testing": {
                        "delay_inside_revive": 2000,
                    }
                })
            ops.append(op)

        ok = False
        for iter in xrange(100):
            time.sleep(random.randint(5, 15) * 0.5)
            with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
                pass

            completed_count = 0
            for index, op in enumerate(ops):
                assert op.get_state() not in ("aborted", "failed")
                if op.get_state() == "completed":
                    completed_count += 1
            if completed_count == len(ops):
                ok = True
                break
        assert ok

    @authors("ignat")
    def test_live_preview(self):
        create_user("u")

        data = [{"foo": i} for i in range(3)]

        create("table", "//tmp/t1")
        write_table("//tmp/t1", data)

        create("table", "//tmp/t2")

        op = map(
            wait_for_jobs=True,
            track=False,
            command=with_breakpoint("BREAKPOINT ; cat"),
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"data_size_per_job": 1})

        jobs = wait_breakpoint(job_count=2)

        async_transaction_id = get(op.get_path() + "/@async_scheduler_transaction_id")
        assert exists(op.get_path() + "/output_0", tx=async_transaction_id)

        release_breakpoint(job_id=jobs[0])
        release_breakpoint(job_id=jobs[1])
        wait(lambda: op.get_job_count("completed") == 2)

        wait(lambda: len(read_table(op.get_path() + "/output_0", tx=async_transaction_id)) == 2)
        live_preview_data = read_table(op.get_path() + "/output_0", tx=async_transaction_id)
        assert all(record in data for record in live_preview_data)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            abort_transaction(async_transaction_id)

        jobs = wait_breakpoint(job_count=1)
        wait(lambda: op.get_state() == "running")

        new_async_transaction_id = get(op.get_path() + "/@async_scheduler_transaction_id")
        assert new_async_transaction_id != async_transaction_id

        async_transaction_id = new_async_transaction_id
        assert exists(op.get_path() + "/output_0", tx=async_transaction_id)
        live_preview_data = read_table(op.get_path() + "/output_0", tx=async_transaction_id)
        assert all(record in data for record in live_preview_data)

        release_breakpoint()
        op.track()
        assert sorted(read_table("//tmp/t2")) == sorted(data)

    @authors("max42", "ignat")
    def test_brief_spec(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", [{"foo": 0}])

        create("table", "//tmp/t2")

        op = map(
            wait_for_jobs=True,
            track=False,
            command=with_breakpoint("BREAKPOINT ; cat"),
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"data_size_per_job": 1})

        wait_breakpoint()

        op.wait_fresh_snapshot()

        brief_spec = get(op.get_path() + "/@brief_spec")

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        release_breakpoint()

        wait(lambda: op.get_state() == "completed")

        assert brief_spec == get(op.get_path() + "/@brief_spec")

################################################################################

class TestJobRevivalBase(YTEnvSetup):
    def _wait_for_single_job(self, op_id):
        path = get_operation_cypress_path(op_id) + "/controller_orchid"
        for i in xrange(500):
            time.sleep(0.1)
            if get(path + "/state", default=None) != "running":
                continue

            jobs = None
            try:
                jobs = ls(path + "/running_jobs")
            except YtError as err:
                if err.is_resolve_error():
                    continue
                raise

            if len(jobs) > 0:
                assert len(jobs) == 1
                return jobs[0]

        assert False, "Wait failed"

    def _kill_and_start(self, components):
        with Restarter(self.Env, components):
            pass

################################################################################

class TestJobRevival(TestJobRevivalBase):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "operations_update_period": 100,
            "static_orchid_cache_update_period": 100
        },
        "cluster_connection" : {
            "transaction_manager": {
                "default_transaction_timeout": 3000,
                "default_ping_period": 200,
            }
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operation_time_limit_check_period": 100,
            "snapshot_period": 500,
            "operations_update_period": 100,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "resource_limits": {
                    "user_slots": 5,
                    "cpu": 5
                },
                "total_confirmation_period": 5000
            }
        }
    }

    @authors("max42")
    @pytest.mark.parametrize("components_to_kill", [["schedulers"], ["controller_agents"], ["schedulers", "controller_agents"]])
    def test_job_revival_simple(self, components_to_kill):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"a": 0}])

        map_cmd = " ; ".join([
            "sleep 2",
            events_on_fs().notify_event_cmd("snapshot_written"),
            events_on_fs().wait_event_cmd("scheduler_reconnected"),
            "echo {a=1}"])
        op = map(
            track=False,
            command=map_cmd,
            in_="//tmp/t_in",
            out="//tmp/t_out")

        job_id = self._wait_for_single_job(op.id)

        events_on_fs().wait_event("snapshot_written")
        op.wait_fresh_snapshot()

        self._kill_and_start(components_to_kill)

        orchid_path = "//sys/scheduler/orchid/scheduler/operations/{0}".format(op.id)

        wait(lambda: exists(orchid_path), "Operation did not re-appear")

        assert self._wait_for_single_job(op.id) == job_id

        events_on_fs().notify_event("scheduler_reconnected")
        op.track()

        assert get("{0}/@progress/jobs/aborted/total".format(op.get_path())) == 0
        assert read_table("//tmp/t_out") == [{"a": 1}]

    @authors("max42")
    @pytest.mark.skipif("True", reason="YT-8635")
    @pytest.mark.timeout(600)
    def test_many_jobs_and_operations(self):
        create("table", "//tmp/t_in")

        row_count = 20
        op_count = 20

        output_tables = []
        for i in range(op_count):
            output_table = "//tmp/t_out{0:02d}".format(i)
            create("table", output_table)
            output_tables.append(output_table)

        for i in range(row_count):
            write_table("<append=%true>//tmp/t_in", [{"a": i}])

        ops = []

        for i in range(op_count):
            ops.append(map(
                track=False,
                command="sleep 0.$(($RANDOM)); cat",
                in_="//tmp/t_in",
                out=output_tables[i],
                spec={"data_size_per_job": 1}))

        def get_total_job_count(category):
            total_job_count = 0
            operations = get("//sys/operations", verbose=False)
            for key in operations:
                if len(key) != 2:
                    continue
                for op_id in operations[key]:
                    total_job_count += \
                        get(get_operation_cypress_path(op_id) + "/@progress/jobs/{}".format(category),
                            default=0,
                            verbose=False)
            return total_job_count

        # We will switch scheduler when there are 40, 80, 120, ..., 400 completed jobs.

        for switch_job_count in range(40, 400, 40):
            while True:
                completed_job_count = get_total_job_count("completed/total")
                aborted_job_count = get_total_job_count("aborted/total")
                aborted_on_revival_job_count = get_total_job_count("aborted/scheduled/revival_confirmation_timeout")
                print_debug("completed_job_count =", completed_job_count)
                print_debug("aborted_job_count =", aborted_job_count)
                print_debug("aborted_on_revival_job_count =", aborted_on_revival_job_count)
                if completed_job_count >= switch_job_count:
                    if (switch_job_count // 40) % 2 == 0:
                        with Restarter(self.Env, SCHEDULERS_SERVICE):
                            pass
                    else:
                        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
                            pass
                    if switch_job_count % 3 == 0:
                        with Restarter(self.Env, NODES_SERVICE):
                            pass
                    break
                time.sleep(1)

        for op in ops:
            op.track()

        if aborted_job_count != aborted_on_revival_job_count:
            print_debug("There were aborted jobs other than during the revival process:")
            for op in ops:
                pprint.pprint(dict(get(op.get_path() + "/@progress/jobs/aborted")), stream=sys.stderr)

        for output_table in output_tables:
            assert sorted(read_table(output_table, verbose=False)) == [{"a": i} for i in range(op_count)]

    @authors("max42", "ignat")
    @pytest.mark.parametrize("components_to_kill", [["schedulers"], ["controller_agents"], ["schedulers", "controller_agents"]])
    def test_user_slots_limit(self, components_to_kill):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        row_count = 20
        for i in range(row_count):
            write_table("<append=%true>//tmp/t_in", [{"a": i}])
        user_slots_limit = 10

        map_cmd = " ; ".join([
            "sleep 2",
            "echo '{a=1};'",
            events_on_fs().notify_event_cmd("ready_for_revival_${YT_JOB_INDEX}"),
            events_on_fs().wait_event_cmd("complete_operation"),
            "echo '{a=2};'"])

        op = map(track=False,
                 command=map_cmd,
                 in_="//tmp/t_in",
                 out="//tmp/t_out",
                 spec={
                     "data_size_per_job": 1,
                     "resource_limits": {"user_slots": user_slots_limit},
                     "auto_merge": {"mode": "manual", "chunk_count_per_merge_job": 3, "max_intermediate_chunk_count": 100}
                 })

        # Comment about '+10' - we need some additional room for jobs that can be non-scheduled aborted.
        wait(lambda: sum([events_on_fs().check_event("ready_for_revival_" + str(i)) for i in xrange(user_slots_limit + 10)]) == user_slots_limit)

        self._kill_and_start(components_to_kill)
        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        orchid_path = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/fair_share_info"
        wait(lambda: exists(orchid_path + "/operations/" + op.id))

        for i in xrange(1000):
            user_slots = None
            user_slots_path = orchid_path + "/operations/{0}/resource_usage/user_slots".format(op.id)
            try:
                user_slots = get(user_slots_path, verbose=False)
            except YtError:
                pass

            for j in xrange(10):
                try:
                    jobs = get(op.get_path() + "/@progress/jobs", verbose=False)
                    break
                except:
                    time.sleep(0.1)
                    continue
            else:
                assert False
            if i == 300:
                events_on_fs().notify_event("complete_operation")
            running = jobs["running"]
            aborted = jobs["aborted"]["total"]
            assert running <= user_slots_limit or user_slots is None or user_slots <= user_slots_limit
            assert aborted == 0

        op.track()

##################################################################

class TestDisabledJobRevival(TestJobRevivalBase):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "lock_transaction_timeout": 3000,
            "operations_update_period": 100
        },
        "cluster_connection" : {
            "transaction_manager": {
                "default_transaction_timeout": 3000,
                "default_ping_period": 200,
            }
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
            "operations_update_period": 100,
            "operation_time_limit_check_period": 100,
            "enable_job_revival": False,
        }
    }

    @authors("max42", "ignat")
    @pytest.mark.parametrize("components_to_kill", [["schedulers"], ["controller_agents"], ["schedulers", "controller_agents"]])
    def test_disabled_job_revival(self, components_to_kill):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"a": 0}])

        map_cmd = " ; ".join([
            events_on_fs().notify_event_cmd("snapshot_written"),
            events_on_fs().wait_event_cmd("scheduler_reconnected"),
            "echo {a=1}"])
        op = map(
            track=False,
            command=map_cmd,
            in_="//tmp/t_in",
            out="//tmp/t_out")

        orchid_path = "//sys/scheduler/orchid/scheduler/operations/{0}".format(op.id)

        job_id = self._wait_for_single_job(op.id)

        events_on_fs().wait_event("snapshot_written")
        op.wait_fresh_snapshot()

        self._kill_and_start(components_to_kill)

        wait(lambda: exists(orchid_path), "Operation did not re-appear")

        # Here is the difference from the test_job_revival_simple.
        assert self._wait_for_single_job(op.id) != job_id

        events_on_fs().notify_event("scheduler_reconnected")
        op.track()

        assert read_table("//tmp/t_out") == [{"a": 1}]

