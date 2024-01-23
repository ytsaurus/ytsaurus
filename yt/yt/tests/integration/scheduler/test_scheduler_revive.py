from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    SCHEDULERS_SERVICE,
    CONTROLLER_AGENTS_SERVICE,
    NODES_SERVICE,
    MASTERS_SERVICE,
)

from yt_commands import (
    authors, map_reduce, print_debug, update_op_parameters,
    wait, wait_no_assert, wait_breakpoint, release_breakpoint, with_breakpoint, events_on_fs,
    create, ls, get, set, remove, exists, create_user, create_pool, create_pool_tree, abort_transaction, read_table,
    write_table, start_transaction, map, vanilla, run_test_vanilla, suspend_op,
    get_operation, get_operation_cypress_path, PrepareTables, sorted_dicts,
    update_scheduler_config, update_controller_agent_config)

from yt_helpers import get_current_time, parse_yt_time

from yt.common import YtError

import pytest
from flaky import flaky

import pprint
import random
import time
from datetime import timedelta
from io import BytesIO

##################################################################


class TestSchedulerRandomMasterDisconnections(YTEnvSetup):
    NUM_TEST_PARTITIONS = 2
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "testing_options": {
                "enable_random_master_disconnection": False,
                "random_master_disconnection_max_backoff": 10000,
                "finish_operation_transition_delay": {
                    "duration": 1000,
                },
            },
            "controller_agent_tracker": {
                "tag_to_alive_controller_agent_thresholds": {
                    "default": {
                        "absolute": 0,
                        "relative": 1.0,
                    }
                },
                "min_agent_count": 0,
            },
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operation_time_limit_check_period": 100,
            "snapshot_period": 3000,
        }
    }

    OP_COUNT = 8

    def _create_table(self, table):
        create("table", table)
        set(table + "/@replication_factor", 1)

    def _prepare_tables(self):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        for index in range(self.OP_COUNT):
            self._create_table("//tmp/t_out" + str(index))
            self._create_table("//tmp/t_err" + str(index))

    @authors("ignat")
    def test_many_operations(self):
        self._prepare_tables()

        ops = []
        for index in range(self.OP_COUNT):
            op = map(
                track=False,
                command="sleep 1; echo 'AAA' >&2; cat",
                in_="//tmp/t_in",
                out="//tmp/t_out" + str(index),
                spec={
                    "stderr_table_path": "//tmp/t_err" + str(index),
                },
            )
            ops.append(op)

        try:
            update_scheduler_config("testing_options/enable_random_master_disconnection", True)
            for index, op in enumerate(ops):
                try:
                    op.track()
                    assert read_table("//tmp/t_out" + str(index)) == [{"foo": "bar"}]
                except YtError:
                    assert op.get_state() == "failed"
        finally:
            update_scheduler_config("testing_options/enable_random_master_disconnection", False)
            time.sleep(2)

    @authors("ignat")
    @pytest.mark.timeout(150)
    def test_many_operations_hard(self):
        self._prepare_tables()

        ops = []
        for index in range(self.OP_COUNT):
            op = map(
                track=False,
                command="sleep 15; echo 'AAA' >&2; cat",
                in_="//tmp/t_in",
                out="//tmp/t_out" + str(index),
                spec={
                    "stderr_table_path": "//tmp/t_err" + str(index),
                    "testing": {
                        "delay_inside_revive": 2000,
                    },
                },
            )
            ops.append(op)

        try:
            update_scheduler_config("testing_options/enable_random_master_disconnection", True)
            for index, op in enumerate(ops):
                try:
                    op.track()
                    assert read_table("//tmp/t_out" + str(index)) == [{"foo": "bar"}]
                except YtError:
                    assert op.get_state() == "failed"
        finally:
            update_scheduler_config("testing_options/enable_random_master_disconnection", False)
            time.sleep(2)

    @authors("ignat")
    @pytest.mark.timeout(300)
    def test_many_operations_controller_disconnections(self):
        self._prepare_tables()

        ops = []
        for index in range(self.OP_COUNT):
            op = map(
                track=False,
                command="sleep 15; echo 'AAA' >&2; cat",
                in_="//tmp/t_in",
                out="//tmp/t_out" + str(index),
                spec={
                    "stderr_table_path": "//tmp/t_err" + str(index),
                    "testing": {
                        "delay_inside_revive": 2000,
                    },
                },
            )
            ops.append(op)

        ok = False
        for iter in range(100):
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
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operation_time_limit_check_period": 100,
            "snapshot_period": 3000,
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
            spec={"data_size_per_job": 1},
        )

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

        op.wait_for_fresh_snapshot()

        wait(lambda: op.get_state() == "running")

        new_async_transaction_id = get(op.get_path() + "/@async_scheduler_transaction_id")
        assert new_async_transaction_id != async_transaction_id

        async_transaction_id = new_async_transaction_id
        assert exists(op.get_path() + "/output_0", tx=async_transaction_id)
        live_preview_data = read_table(op.get_path() + "/output_0", tx=async_transaction_id)
        assert all(record in data for record in live_preview_data)

        release_breakpoint()
        op.track()
        assert sorted_dicts(read_table("//tmp/t2")) == sorted_dicts(data)

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
            spec={"data_size_per_job": 1},
        )

        wait_breakpoint()

        op.wait_for_fresh_snapshot()

        brief_spec = get(op.get_path() + "/@brief_spec")

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        release_breakpoint()

        wait(lambda: op.get_state() == "completed")

        assert brief_spec == get(op.get_path() + "/@brief_spec")

    @authors("gritukan", "gepardo")
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
            spec={"annotations": {"abc": "def"}, "description": {"foo": "bar"}},
        )

        wait_breakpoint()

        op.wait_for_fresh_snapshot()

        annotations_path = op.get_path() + "/@runtime_parameters/annotations"

        required_annotations = {
            "abc": "def",
            "description": {
                "foo": "bar",
            },
        }

        def check_cypress():
            assert exists(annotations_path) and get(annotations_path) == required_annotations

        def check_get_operation():
            result = get_operation(op.id, attributes=["runtime_parameters"])["runtime_parameters"]
            assert result.get("annotations", None) == required_annotations

        wait_no_assert(check_cypress)
        wait_no_assert(check_get_operation)

        update_op_parameters(op.id, parameters={
            "annotations": {
                "abc": "ghi",
            }
        })
        required_annotations["abc"] = "ghi"

        wait_no_assert(check_cypress)
        wait_no_assert(check_get_operation)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        wait_no_assert(check_cypress)
        wait_no_assert(check_get_operation)

        release_breakpoint()
        op.track()

    # COMPAT(levysotsky): Remove when commit with this line is on every cluster.
    @authors("levysotsky")
    def test_bad_acl_during_revival(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", [{"foo": 0}])

        create("table", "//tmp/t2")

        op = map(
            wait_for_jobs=True,
            track=False,
            command=with_breakpoint("BREAKPOINT ; cat"),
            in_="//tmp/t1",
            out="//tmp/t2",
        )
        wait_breakpoint()
        op.wait_for_fresh_snapshot()

        bad_acl = [
            {
                "permission": ["read", "manage"],
                "subject": ["harold_finch"],
                "action": "allow",
            },
        ]

        acl_path = op.get_path() + "/@spec/acl"

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            set(acl_path, bad_acl)

        release_breakpoint()

        op.track()

    @authors("eshcherbin")
    def test_restart_during_materialization_and_revive_from_snapshot(self):
        update_scheduler_config("operations_update_period", 10000)
        update_controller_agent_config("snapshot_period", 500)

        op = run_test_vanilla(
            "sleep 1",
            job_count=2,
            spec={
                "testing": {"delay_inside_materialize_scheduler": {"duration": 10000, "type": "async"}},
            },
        )
        op.wait_for_fresh_snapshot()

        time.sleep(3.0)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        op.wait_for_state("completed")


##################################################################


class TestControllerAgentReconnection(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "testing_options": {
                "finish_operation_transition_delay": {
                    "duration": 2000,
                },
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
            return parse_yt_time(
                get("//sys/controller_agents/instances/{}/@connection_time".format(controller_agents[0]))
            )

        time.sleep(3)

        assert get_current_time() - get_connection_time() > timedelta(seconds=3)

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        assert get_current_time() - get_connection_time() < timedelta(seconds=3)

    @authors("ignat")
    @pytest.mark.parametrize("wait_transition_state", [False, True])
    def test_abort_operation_without_controller_agent(self, wait_transition_state):
        def get_agent_states():
            return [agent_info["state"]
                    for agent_info in get("//sys/scheduler/orchid/scheduler/controller_agents").values()]

        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        for iter in range(2):
            op = map(
                command="sleep 1000",
                in_=["//tmp/t_in"],
                out="//tmp/t_out",
                track=False,
            )

            self._wait_for_state(op, "running")

            with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
                wait(lambda: all(state == "unregistered" for state in get_agent_states()),
                     iter=30, sleep_backoff=1.0)
                if wait_transition_state:
                    self._wait_for_state(op, "waiting_for_agent")
                op.abort()

            self._wait_for_state(op, "aborted")

    @authors("ignat")
    def test_complete_operation_without_controller_agent(self):
        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        op = map(command="sleep 1000", in_=["//tmp/t_in"], out="//tmp/t_out", track=False)
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
            track=False,
        )
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

    @authors("alexkolodezny")
    def test_complete_map_reduce_operation_on_controller_agent_connection(self):
        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        op = map_reduce(
            map_command="sleep 1000",
            reduce_command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by=["foo"],
            spec={
                "testing": {
                    "delay_inside_revive": 10000,
                },
                "stderr_table_path": "<create=true>//tmp/stderr_table",
            },
            track=False,
        )
        self._wait_for_state(op, "running")

        snapshot_path = op.get_path() + "/snapshot"
        wait(lambda: exists(snapshot_path))

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        with pytest.raises(YtError):
            op.complete()

        self._wait_for_state(op, "running")
        op.complete()

        wait(lambda: op.get_state() in ["completed", "failed"])
        print_debug("stderr: {}".format(read_table("//tmp/stderr_table")))
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
            track=False,
        )
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
        return "//sys/controller_agents/instances/{}/orchid/controller_agent/operations/{}".format(
            controller_agent, op.id
        )

    def test_zombie_operation_orchids(self):
        self._create_table("//tmp/t_in")
        self._create_table("//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        op = map(command="cat", in_=["//tmp/t_in"], out="//tmp/t_out")

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
            },
            fail_fast=False
        )

        orchid_path = self._get_operation_orchid_path(op)
        wait(lambda: exists(orchid_path))
        retained_finished_jobs = get(orchid_path + "/retained_finished_jobs")
        assert len(retained_finished_jobs) == 1
        ((job_id, attributes),) = list(retained_finished_jobs.items())
        assert attributes["job_type"] == "map"
        assert attributes["state"] == "failed"


##################################################################


class TestRaceBetweenShardAndStrategy(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 2  # snapshot upload replication factor is 2; unable to configure
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
                "testing": {"delay_inside_materialize_scheduler": {"duration": 1000, "type": "async"}},
                "resource_limits": {"user_slots": 1},
            },
        )
        wait_breakpoint()
        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            release_breakpoint()

        wait(lambda: op.get_state() == "completed")


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
                "finish_operation_transition_delay": {
                    "duration": 2000,
                },
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

        for iter in range(5):
            self._wait_for_state(op, "running")
            with Restarter(self.Env, SCHEDULERS_SERVICE):
                set(op.get_path() + "/@input_transaction_id", "0-0-0-0")
            time.sleep(1)

        release_breakpoint()
        op.track()

        assert op.get_state() == "completed"

    # NB: we hope that we check aborting state before operation comes to aborted state but
    # we cannot guarantee that this happen.
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
    @authors("kvk1920", "ignat")
    @flaky(max_runs=3)
    @pytest.mark.parametrize("mode", ["simple", "cypress_tx_action", "system_tx_action"])  # COMPAT(kvk1920)
    def test_completing(self, mode):
        update_controller_agent_config(
            "set_committed_attribute_via_transaction_action",
            mode == "cypress_tx_action")
        update_controller_agent_config(
            "commit_operation_cypress_node_changes_via_system_transaction",
            mode == "system_tx_action")

        if mode == "cypress_tx_action":
            # COMPAT(kvk1920)
            set("//sys/@config/transaction_manager/forbid_transaction_actions_for_cypress_transactions", False)

        self._prepare_tables()

        op = self._start_op("echo '{foo=bar}'; sleep 15", track=False)

        self._wait_for_state(op, "running")

        op.complete(ignore_result=True)

        self._wait_for_state(op, "completing")

        get(f"{op.get_path()}/@id")
        wait(lambda: exists(op.get_path() + "/@committed"))

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            assert op.get_state() == "completing"

        op.track()

        assert op.get_state() == "completed"

        if self.OP_TYPE == "map":
            assert read_table("//tmp/t_out") == []

    @authors("kvk1920")
    @pytest.mark.parametrize("use_tx", [False, True])
    def test_operation_committed_attribute(self, use_tx):
        self._prepare_tables()

        if use_tx:
            tx = start_transaction(timeout=10 * 60 * 1000)
            op = self._start_op("echo '{foo=bar}'; sleep 15", track=False, transaction_id=tx)
        else:
            op = self._start_op("echo '{foo=bar}'; sleep 15", track=False)

        self._wait_for_state(op, "running")

        op.complete(ignore_result=True)

        self._wait_for_state(op, "completing")

        # NB: Since operation node branch is merged manually via tx action all
        # changes should be visible outside of user transaction.
        wait(lambda: exists(op.get_path() + "/@committed"))

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        assert get(op.get_path() + "/@committed")

    # NB: test rely on timings and can flap if we hang at some point.
    @authors("ignat")
    @flaky(max_runs=3)
    @pytest.mark.parametrize("stage", ["stage" + str(index) for index in range(1, 8)])
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
                "job_count": 2,
            },
        )

        self._wait_for_state(op, "running")

        wait(lambda: op.get_job_count("completed") == 1 and op.get_job_count("running") == 1)

        op.wait_for_fresh_snapshot()

        # This request will be retried with the new incarnation of the scheduler.
        op.complete(ignore_result=True)

        self._wait_for_state(op, "completing")
        wait(lambda: get(op.get_path() + "/controller_orchid/testing/commit_sleep_started"))

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            assert op.get_state() == "completing"
            set(op.get_path() + "/@testing", {"commit_sleep_started": True})

        # complete_operation retry may come when operation is in reviving state.
        # In this case we should complete operation again.
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
            "orphaned",
        ]
        if stage <= "stage5":
            expected_events = events_prefix + [
                "waiting_for_agent",
                "revive_initializing",
                "reviving",
                "pending",
                "reviving_jobs",
                "running",
                "completing",
                "completed",
            ]
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
                "job_count": 2,
            },
        )

        self._wait_for_state(op, "running")

        op.wait_for_fresh_snapshot()

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

        op = self._start_op("sleep 1; false", spec={"max_failed_job_count": 10000}, track=False)

        self._wait_for_state(op, "running")

        def failed_jobs_exist():
            assert op.get_job_count("failed") >= 3

        wait_no_assert(failed_jobs_exist)

        suspend_op(op.id)

        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        wait_no_assert(failed_jobs_exist)


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
        for i in range(500):
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
            "static_orchid_cache_update_period": 100,
        },
        "cluster_connection": {
            "transaction_manager": {
                "default_transaction_timeout": 3000,
                "default_ping_period": 200,
            }
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operation_time_limit_check_period": 100,
            "snapshot_period": 500,
            "operations_update_period": 100,
        }
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {"user_slots": 5, "cpu": 5},
        }
    }

    @authors("max42")
    @pytest.mark.parametrize(
        "components_to_kill",
        [["schedulers"], ["controller_agents"], ["schedulers", "controller_agents"]],
    )
    def test_job_revival_simple(self, components_to_kill):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"a": 0}])

        map_cmd = " ; ".join(
            [
                "sleep 2",
                events_on_fs().notify_event_cmd("snapshot_written"),
                events_on_fs().wait_event_cmd("scheduler_reconnected"),
                "echo {a=1}",
            ]
        )
        op = map(track=False, command=map_cmd, in_="//tmp/t_in", out="//tmp/t_out")

        job_id = self._wait_for_single_job(op.id)

        events_on_fs().wait_event("snapshot_written")
        op.wait_for_fresh_snapshot()

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
            ops.append(
                map(
                    track=False,
                    command="sleep 0.$(($RANDOM)); cat",
                    in_="//tmp/t_in",
                    out=output_tables[i],
                    spec={"data_size_per_job": 1},
                )
            )

        def get_total_job_count(category):
            total_job_count = 0
            operations = get("//sys/operations", verbose=False)
            for key in operations:
                if len(key) != 2:
                    continue
                for op_id in operations[key]:
                    total_job_count += get(
                        get_operation_cypress_path(op_id) + "/@progress/jobs/{}".format(category),
                        default=0,
                        verbose=False,
                    )
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
                output = BytesIO()
                pprint.pprint(dict(get(op.get_path() + "/@progress/jobs/aborted")), stream=output)
                print_debug(output.getvalue())

        for output_table in output_tables:
            assert sorted(read_table(output_table, verbose=False)) == [{"a": i} for i in range(op_count)]

    @authors("max42", "ignat")
    @pytest.mark.parametrize(
        "components_to_kill",
        [["schedulers"], ["controller_agents"], ["schedulers", "controller_agents"]],
    )
    def test_user_slots_limit(self, components_to_kill):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        row_count = 20
        for i in range(row_count):
            write_table("<append=%true>//tmp/t_in", [{"a": i}])
        user_slots_limit = 10

        map_cmd = " ; ".join(
            [
                "sleep 2",
                "echo '{a=1};'",
                events_on_fs().notify_event_cmd("ready_for_revival_${YT_JOB_INDEX}"),
                events_on_fs().wait_event_cmd("complete_operation"),
                "echo '{a=2};'",
            ]
        )

        op = map(
            track=False,
            command=map_cmd,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "data_size_per_job": 1,
                "resource_limits": {"user_slots": user_slots_limit},
                "auto_merge": {
                    "mode": "manual",
                    "chunk_count_per_merge_job": 3,
                    "max_intermediate_chunk_count": 100,
                },
            },
        )

        # Comment about '+10' - we need some additional room for jobs that can be non-scheduled aborted.
        wait(
            lambda: sum(
                [events_on_fs().check_event("ready_for_revival_" + str(i)) for i in range(user_slots_limit + 10)]
            )
            == user_slots_limit
        )

        self._kill_and_start(components_to_kill)
        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        orchid_path = "//sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/default/fair_share_info"
        wait(lambda: exists(orchid_path + "/operations/" + op.id))

        for i in range(1000):
            user_slots = None
            user_slots_path = orchid_path + "/operations/{0}/resource_usage/user_slots".format(op.id)
            try:
                user_slots = get(user_slots_path, verbose=False)
            except YtError:
                pass

            for j in range(10):
                try:
                    jobs = get(op.get_path() + "/@progress/jobs", verbose=False)
                    break
                except YtError:
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

    @authors("ignat", "ni-stoiko")
    def test_revival_of_pending_operation(self):
        def get_job_nodes(op):
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
                    continue  # The job has already completed, Orchid is lagging.

                result.append((job_id, job_node))
            return result

        set("//sys/pool_trees/default/@config/nodes_filter", "!other")

        nodes = ls("//sys/cluster_nodes")
        set("//sys/cluster_nodes/" + nodes[0] + "/@user_tags/end", "other")

        create_pool_tree("other", config={"nodes_filter": "other"})
        create_pool("my_pool", pool_tree="other")

        create_pool("my_pool", pool_tree="default")
        set("//sys/pool_trees/default/my_pool/@max_running_operation_count", 2)
        set("//sys/pool_trees/default/my_pool/@max_operation_count", 2)

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        row_count = 20
        for i in range(row_count):
            write_table("<append=%true>//tmp/t_in", [{"a": i}])

        op = map(
            track=False,
            command=with_breakpoint("BREAKPOINT"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "pool_trees": ["default", "other"],
                "pool": "my_pool",
                "data_size_per_job": 1,
            },
        )

        job_ids = wait_breakpoint(job_count=15)

        assert len(job_ids) == 15

        # There was often an error when checking for equality due to the race.
        # If it falls when checking for more or equal, I suggest you delete this assertion.
        # (ni-stoiko)
        assert op.get_job_count("running") >= 15

        op.wait_for_fresh_snapshot()

        create("table", "//tmp/t_out_concurrent")
        concurrent_op = map(
            track=False,
            command="sleep 1000",
            in_="//tmp/t_in",
            out="//tmp/t_out_concurrent",
            spec={
                "pool_trees": ["default"],
                "pool": "my_pool",
            },
        )

        wait(lambda: concurrent_op.get_state() == "running")

        set(concurrent_op.get_path() + "/@registration_index", 1)
        set(op.get_path() + "/@registration_index", 2)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            set("//sys/pool_trees/default/my_pool/@max_running_operation_count", 1)

        wait(lambda: concurrent_op.get_state() == "running")

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        wait(lambda: concurrent_op.get_state() == "running")

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        wait(lambda: concurrent_op.get_state() == "running")


##################################################################

class TestHasFailedJobsOnRevive(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "operations_update_period": 100,
            "static_orchid_cache_update_period": 100,
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operation_time_limit_check_period": 100,
            "snapshot_period": 500,
            "operations_update_period": 100,
        }
    }

    @authors("omgronny")
    @pytest.mark.parametrize("revive_from_snapshot", [True, False])
    def test_has_failed_jobs_after_revive(self, revive_from_snapshot):
        if not revive_from_snapshot:
            update_controller_agent_config("snapshot_period", 1000000000)

        failed_job_cmd = events_on_fs().wait_event_cmd("second_iteration_started", timeout=timedelta(seconds=1))
        op = vanilla(
            track=False,
            spec={
                "max_failed_job_count": 100,
                "tasks": {
                    "failed_task": {
                        "job_count": 1,
                        "command": failed_job_cmd
                    },
                    "task": {
                        "job_count": 1,
                        "command": "sleep 1000"
                    }
                }
            }
        )

        def get_has_failed_jobs(op):
            return get(op.get_path() + "/@has_failed_jobs", default=False)

        wait(lambda: get_has_failed_jobs(op))
        events_on_fs().notify_event("second_iteration_started")

        if revive_from_snapshot:
            op.wait_for_fresh_snapshot()

        assert get_has_failed_jobs(op)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        op.wait_for_state("running")
        wait(lambda: get_has_failed_jobs(op) == revive_from_snapshot)


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
            "operations_update_period": 100,
        },
        "cluster_connection": {
            "transaction_manager": {
                "default_transaction_timeout": 3000,
                "default_ping_period": 200,
            }
        },
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
    @pytest.mark.parametrize(
        "components_to_kill",
        [["schedulers"], ["controller_agents"], ["schedulers", "controller_agents"]],
    )
    def test_disabled_job_revival(self, components_to_kill):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"a": 0}])

        map_cmd = " ; ".join(
            [
                events_on_fs().notify_event_cmd("snapshot_written"),
                events_on_fs().wait_event_cmd("scheduler_reconnected"),
                "echo {a=1}",
            ]
        )
        op = map(track=False, command=map_cmd, in_="//tmp/t_in", out="//tmp/t_out")

        orchid_path = "//sys/scheduler/orchid/scheduler/operations/{0}".format(op.id)

        job_id = self._wait_for_single_job(op.id)

        events_on_fs().wait_event("snapshot_written")
        op.wait_for_fresh_snapshot()

        self._kill_and_start(components_to_kill)

        wait(lambda: exists(orchid_path), "Operation did not re-appear")

        # Here is the difference from the test_job_revival_simple.
        assert self._wait_for_single_job(op.id) != job_id

        events_on_fs().notify_event("scheduler_reconnected")
        op.track()

        assert read_table("//tmp/t_out") == [{"a": 1}]

    @authors("gritukan")
    def test_fail_on_job_restart(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"a": 0}])

        map_cmd = " ; ".join(
            [
                events_on_fs().notify_event_cmd("snapshot_written"),
                events_on_fs().wait_event_cmd("scheduler_reconnected"),
                "echo {a=1}",
            ]
        )
        op = map(
            track=False,
            command=map_cmd,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"fail_on_job_restart": True})

        self._wait_for_single_job(op.id)

        events_on_fs().wait_event("snapshot_written")
        op.wait_for_fresh_snapshot()

        self._kill_and_start("controller_agents")

        with pytest.raises(YtError):
            op.track()


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

        def get_slot_index(op):
            wait(lambda: op.get_runtime_progress("scheduling_info_per_pool_tree/default/slot_index") is not None)
            return op.get_runtime_progress("scheduling_info_per_pool_tree/default/slot_index")

        for i in range(3):
            self._create_table("//tmp/t_out_" + str(i))

        op1 = map(
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out_0",
            track=False,
        )
        op2 = map(command="sleep 2; cat", in_="//tmp/t_in", out="//tmp/t_out_1", track=False)
        op3 = map(
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out_2",
            track=False,
        )

        assert get_slot_index(op1) == 0
        assert get_slot_index(op2) == 1
        assert get_slot_index(op3) == 2

        op2.track()  # this makes slot index 1 available again since operation is completed

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        time.sleep(2.0)

        assert get_slot_index(op1) == 0
        assert get_slot_index(op3) == 2

        op2 = map(
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out_1",
            track=False,
        )

        assert get_slot_index(op2) == 1


##################################################################
