from yt_env_setup import (
    YTEnvSetup, wait,
    Restarter, SCHEDULERS_SERVICE, CONTROLLER_AGENTS_SERVICE, NODES_SERVICE
)

from yt_commands import *
from yt_helpers import *

from yt.yson import YsonEntity

import pytest
from flaky import flaky

import pprint
import random
import time
from StringIO import StringIO

##################################################################

class TestSchedulerRandomMasterDisconnections(YTEnvSetup):
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

class TestSchedulerRestart(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
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

    # COMPAT(gritukan)
    @authors("gritukan")
    def test_description_after_revival(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", [{"foo": 0}])

        create("table", "//tmp/t2")

        op = map(
            wait_for_jobs=True,
            track=False,
            command=with_breakpoint("BREAKPOINT ; cat"),
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"annotations": {"abc": "def"}, "description": {"foo": "bar"}})

        wait_breakpoint()

        op.wait_fresh_snapshot()

        annotations_path = op.get_path() + "/@runtime_parameters/annotations"

        required_annotations = {
            "abc": "def",
            "description": {
                "foo": "bar",
            },
        }

        def check_cypress():
            return exists(annotations_path) and get(annotations_path) == required_annotations

        def check_get_operation():
            result = get_operation(op.id, attributes=["runtime_parameters"])["runtime_parameters"]
            return result.get("annotations", None) == required_annotations

        wait(check_cypress)
        wait(check_get_operation)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            remove(annotations_path)
            assert not exists(annotations_path)

        wait(check_cypress)
        wait(check_get_operation)

        release_breakpoint()
        op.track()

##################################################################

class TestControllerAgentReconnection(YTEnvSetup):
    NUM_MASTERS = 1
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
        wait(lambda: op.get_state() == state)

    @authors("ignat")
    @flaky(max_runs=3)
    def test_connection_time(self):
        def get_connection_time():
            controller_agents = ls("//sys/controller_agents/instances")
            assert len(controller_agents) == 1
            return datetime.strptime(get("//sys/controller_agents/instances/{}/@connection_time".format(controller_agents[0])), "%Y-%m-%dT%H:%M:%S.%fZ")

        time.sleep(3)

        assert datetime.utcnow() - get_connection_time() > timedelta(seconds=3)

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        assert datetime.utcnow() - get_connection_time() < timedelta(seconds=3)

    @authors("ignat")
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
                    track=False)

                self._wait_for_state(op, "running")

                with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
                    if wait_transition_state:
                        self._wait_for_state(op, "waiting_for_agent")
                    op.abort()

                self._wait_for_state(op, "aborted")

    @authors("ignat")
    def test_complete_operation_without_controller_agent(self):
        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        op = map(
            command="sleep 1000",
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            track=False)
        self._wait_for_state(op, "running")

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            with pytest.raises(YtError):
                op.complete()

        self._wait_for_state(op, "running")
        op.complete()
        self._wait_for_state(op, "completed")

    @authors("ignat")
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
            track=False)
        self._wait_for_state(op, "running")

        snapshot_path = op.get_path() + "/snapshot"
        wait(lambda: exists(snapshot_path))

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        with pytest.raises(YtError):
            op.complete()

        self._wait_for_state(op, "running")
        op.complete()

        self._wait_for_state(op, "completed")

    @authors("ignat")
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
            track=False)
        self._wait_for_state(op, "running")

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        op.abort()
        self._wait_for_state(op, "aborted")


@authors("levysotsky")
class TestControllerAgentZombieOrchids(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "zombie_operation_orchids": {
                "clean_period": 15 * 1000,
            },
        }
    }

    def _create_table(self, table):
        create("table", table, attributes={"replication_factor": 1})

    def _get_operation_orchid_path(self, op):
        controller_agent = get(op.get_path() + "/@controller_agent_address")
        return "//sys/controller_agents/instances/{}/orchid/controller_agent/operations/{}"\
            .format(controller_agent, op.id)

    def test_zombie_operation_orchids(self):
        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        op = map(
            command="cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out")

        orchid_path = self._get_operation_orchid_path(op)
        wait(lambda: exists(orchid_path))
        assert get(orchid_path + "/state") == "completed"
        wait(lambda: not exists(orchid_path))

    def test_retained_finished_jobs(self):
        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        write_table("//tmp/t_in", [{"foo": "bar1"}, {"foo": "bar2"}])

        op = map(
            command='if [[ "$YT_JOB_INDEX" == "0" ]] ; then exit 1; fi; cat',
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            spec={
                "data_size_per_job": 1,
            })

        orchid_path = self._get_operation_orchid_path(op)
        wait(lambda: exists(orchid_path))
        retained_finished_jobs = get(orchid_path + "/retained_finished_jobs")
        assert len(retained_finished_jobs) == 1
        (job_id, attributes), = retained_finished_jobs.items()
        assert attributes["job_type"] == "map"
        assert attributes["state"] == "failed"

##################################################################

class TestRaceBetweenShardAndStrategy(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 2   # snapshot upload replication factor is 2; unable to configure
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
            "operation_time_limit_check_period": 100,
            "operation_build_progress_period": 100,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "scheduler_connector": {
                "heartbeat_period": 100
            }
        }
    }

    @authors("renadeen")
    def test_race_between_shard_and_strategy(self):
        # Scenario:
        # 1. operation is running
        # 2. controller fails, scheduler disables operation and it disappears from tree snapshot
        # 3. job completed event arrives to node shard but is not processed until job revival
        # 4. controller returns, scheduler revives operation
        # 5. node shard revives job and sets JobsReady=true
        # 6. scheduler should enable operation but a bit delayed
        # 7. node shard manages to process job completed event cause job is revived
        #    but operation still is not present at tree snapshot (due to not being enabled)
        #    and strategy discards job completed event telling node to remove it forever
        # 8. job resource usage is stuck in scheduler and next job won't be scheduled ever

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=2,
            spec={
                "testing": {"delay_after_materialize": 1000},
                "resource_limits": {"user_slots": 1}
            })
        wait_breakpoint()
        op.wait_fresh_snapshot()

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            release_breakpoint()

        wait(lambda: op.get_state() == "completed")


class TestRaceBetweenPoolTreeRemovalAndRegisterOperation(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 2   # snapshot upload replication factor is 2; unable to configure
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100   # pool trees config update period
        }
    }

    @authors("renadeen")
    def test_race_between_pool_tree_removal_and_register_operation(self):
        # Scenario:
        # 1. operation is running
        # 2. user updates node_filter of pool tree
        # 3. scheduler removes and adds that tree
        # 4. scheduler unregisters and aborts all operations of removed tree before publishing new trees
        # 5. abort of operation causes fiber switch
        # 6. new operation registers in old tree that is being removed
        # 7. all aborts are completed, scheduler publishes new tree structure (without new operation)
        # 8. operation tries to complete scheduler doesn't know this operation and crashes

        set("//sys/cluster_nodes/{}/@user_tags/end".format(ls("//sys/cluster_nodes")[0]), "my_tag")
        time.sleep(0.5)

        run_test_vanilla(
            "sleep 1000",
            job_count=1,
            spec={"testing": {"delay_inside_abort": 1000}}
        )

        set("//sys/pool_trees/default/@nodes_filter", "my_tag")
        time.sleep(0.2)
        try:
            run_test_vanilla(":", job_count=1)
            assert False
        except YtError as err:
            assert err.contains_text("tree \"default\" is being removed")

##################################################################

class OperationReviveBase(YTEnvSetup):
    NUM_MASTERS = 1
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
        wait(lambda: op.get_state() == state)

    @authors("ignat")
    def test_missing_transactions(self):
        self._prepare_tables()

        op = self._start_op(with_breakpoint("echo '{foo=bar}'; BREAKPOINT"), track=False)

        for iter in xrange(5):
            self._wait_for_state(op, "running")
            with Restarter(self.Env, SCHEDULERS_SERVICE):
                set(op.get_path() + "/@input_transaction_id", "0-0-0-0")
            time.sleep(1)

        release_breakpoint()
        op.track()

        assert op.get_state() == "completed"

    # NB: we hope that we check aborting state before operation comes to aborted state but we cannot guarantee that this happen.
    @authors("ignat")
    @flaky(max_runs=3)
    def test_aborting(self):
        self._prepare_tables()

        op = self._start_op("echo '{foo=bar}'; sleep 10", track=False)

        self._wait_for_state(op, "running")

        op.abort(ignore_result=True)

        self._wait_for_state(op, "aborting")

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            assert op.get_state() == "aborting"

        with pytest.raises(YtError):
            op.track()

        assert op.get_state() == "aborted"

    # NB: we hope that complete finish first phase before we kill scheduler. But we cannot guarantee that this happen.
    @authors("ignat")
    @flaky(max_runs=3)
    def test_completing(self):
        self._prepare_tables()

        op = self._start_op("echo '{foo=bar}'; sleep 10", track=False)

        self._wait_for_state(op, "running")

        op.complete(ignore_result=True)

        self._wait_for_state(op, "completing")

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            assert op.get_state() == "completing"

        op.track()

        assert op.get_state() == "completed"

        if self.OP_TYPE == "map":
            assert read_table("//tmp/t_out") == []

    # NB: test rely on timings and can flap if we hang at some point.
    @authors("ignat")
    @flaky(max_runs=3)
    @pytest.mark.parametrize("stage", ["stage" + str(index) for index in xrange(1, 8)])
    def test_completing_with_sleep(self, stage):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", [{"foo": "bar"}] * 2)

        self._create_table("//tmp/t_out")

        op = self._start_op(
            "echo '{foo=bar}'; " + events_on_fs().execute_once("sleep 100"),
            track=False,
            spec={
                "testing": {
                    "delay_inside_operation_commit": 60000,
                    "no_delay_on_second_entrance_to_commit": True,
                    "delay_inside_operation_commit_stage": stage,
                },
                "job_count": 2
            })

        self._wait_for_state(op, "running")

        wait(lambda: op.get_job_count("completed") == 1 and op.get_job_count("running") == 1)

        op.wait_fresh_snapshot()

        # This request will be retried with the new incarnation of the scheduler.
        op.complete(ignore_result=True)

        self._wait_for_state(op, "completing")
        wait(lambda: get(op.get_path() + "/controller_orchid/testing/commit_sleep_started"))

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            assert op.get_state() == "completing"
            set(op.get_path() + "/@testing", {"commit_sleep_started": True})

        # complete_operation retry may come when operation is in reviving state. In this case we should complete operation again.
        wait(lambda: op.get_state() in ("running", "completed"))

        if op.get_state() == "running":
            op.complete()

        op.track()

        events = get(op.get_path() + "/@events")

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
            expected_events = events_prefix + ["waiting_for_agent", "revive_initializing", "reviving", "pending", "reviving_jobs", "running", "completing", "completed"]
        else:
            expected_events = events_prefix + ["completed"]

        actual_events = [event["state"] for event in events]

        print_debug("Expected: ", expected_events)
        print_debug("Actual:   ", actual_events)
        assert expected_events == actual_events

        assert op.get_state() == "completed"

        if self.OP_TYPE == "map":
            assert read_table("//tmp/t_out") == [{"foo": "bar"}]

    @authors("ignat")
    def test_abort_during_complete(self):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", [{"foo": "bar"}] * 2)

        remove("//tmp/t_out", force=True)
        self._create_table("//tmp/t_out")

        op = self._start_op(
            "echo '{foo=bar}'; " + events_on_fs().execute_once("sleep 100"),
            track=False,
            spec={
                "testing": {
                    "delay_inside_operation_commit": 4000,
                    "delay_inside_operation_commit_stage": "stage4",
                },
                "job_count": 2
            })

        self._wait_for_state(op, "running")

        op.wait_fresh_snapshot()

        op.complete(ignore_result=True)

        self._wait_for_state(op, "completing")

        # Wait to perform complete before sleep.
        time.sleep(2)

        op.abort()
        op.track()

        assert op.get_state() == "completed"

    @authors("ignat")
    @flaky(max_runs=3)
    def test_failing(self):
        self._prepare_tables()

        op = self._start_op("exit 1", track=False, spec={"max_failed_job_count": 1})

        self._wait_for_state(op, "failing")

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            assert op.get_state() == "failing"

        with pytest.raises(YtError):
            op.track()

        assert op.get_state() == "failed"

    @authors("ignat")
    def test_revive_failed_jobs(self):
        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        op = self._start_op(
            "sleep 1; false",
            spec={"max_failed_job_count": 10000},
            track=False)

        self._wait_for_state(op, "running")

        def failed_jobs_exist():
            return op.get_job_count("failed") >= 3

        wait(failed_jobs_exist)

        suspend_op(op.id)

        op.wait_fresh_snapshot()

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        wait(lambda: op.get_job_count("failed") >= 3)

class TestSchedulerReviveForMap(OperationReviveBase):
    OP_TYPE = "map"

    def _start_op(self, command, **kwargs):
        return map(command=command, in_=["//tmp/t_in"], out="//tmp/t_out", **kwargs)

class TestSchedulerReviveForVanilla(OperationReviveBase):
    OP_TYPE = "vanilla"

    def _start_op(self, command, **kwargs):
        spec = kwargs.pop("spec", {})
        job_count = spec.pop("job_count", 1)
        spec["tasks"] = {"main": {"command": command, "job_count": job_count}}
        return vanilla(spec=spec, **kwargs)

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
                output = StringIO()
                pprint.pprint(dict(get(op.get_path() + "/@progress/jobs/aborted")), stream=output)
                print_debug(output.getvalue())

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

##################################################################

class TestPreserveSlotIndexAfterRevive(YTEnvSetup, PrepareTables):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "profiling_update_period": 100,
            "fair_share_profiling_period": 100,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operation_time_limit_check_period": 100,
        }
    }

    @authors("ignat")
    def test_preserve_slot_index_after_revive(self):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", [{"x": "y"}])

        def get_slot_index(op_id):
            path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/slot_index".format(op_id)
            wait(lambda: exists(path) and get(path) != YsonEntity())
            return get(path)

        for i in xrange(3):
            self._create_table("//tmp/t_out_" + str(i))

        op1 = map(command="sleep 1000; cat", in_="//tmp/t_in", out="//tmp/t_out_0", track=False)
        op2 = map(command="sleep 2; cat", in_="//tmp/t_in", out="//tmp/t_out_1", track=False)
        op3 = map(command="sleep 1000; cat", in_="//tmp/t_in", out="//tmp/t_out_2", track=False)

        assert get_slot_index(op1.id) == 0
        assert get_slot_index(op2.id) == 1
        assert get_slot_index(op3.id) == 2

        op2.track()  # this makes slot index 1 available again since operation is completed

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        time.sleep(2.0)

        assert get_slot_index(op1.id) == 0
        assert get_slot_index(op3.id) == 2

        op2 = map(command="sleep 1000; cat", in_="//tmp/t_in", out="//tmp/t_out_1", track=False)

        assert get_slot_index(op2.id) == 1

##################################################################

