from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait, get, complete_op,
    lookup_rows, with_breakpoint, release_breakpoint,
    map, vanilla, get_singular_chunk_id, update_scheduler_config, set_nodes_banned,
    create_test_tables, sync_create_cells,
    list_operations, get_operation, update_op_parameters, make_ace)

import yt.environment.init_operations_archive as init_operations_archive

from yt.common import datetime_to_string, uuid_to_parts

from yt.yson import YsonEntity

import time
from datetime import datetime

##################################################################

CLEANER_ORCHID = "//sys/scheduler/orchid/scheduler/operations_cleaner"


def _lookup_ordered_by_id_row(op_id):
    id_hi, id_lo = uuid_to_parts(op_id)
    rows = lookup_rows(
        "//sys/operations_archive/ordered_by_id",
        [{"id_hi": id_hi, "id_lo": id_lo}],
        column_names=["alert_events"])
    assert len(rows) == 1
    return rows[0]


def _wait_for_alert_events(op, min_count):
    wait(
        lambda: len(_lookup_ordered_by_id_row(op.id)["alert_events"]) >= min_count,
        ignore_exceptions=True,
    )


def _run_op_with_input_chunks_alert(return_events=True, wait_until_set_event_sent=False, min_events=0):
    create_test_tables(attributes={"replication_factor": 1}, force=True)

    chunk_id = get_singular_chunk_id("//tmp/t_in")
    replicas = get("#{0}/@stored_replicas".format(chunk_id))
    set_nodes_banned(replicas, True)

    op = map(
        command="cat",
        in_="//tmp/t_in",
        out="//tmp/t_out",
        spec={
            "unavailable_chunk_strategy": "wait",
            "unavailable_chunk_tactics": "wait",
        },
        track=False,
    )

    wait(lambda: op.get_state() == "running")
    wait(lambda: "lost_input_chunks" in op.get_alerts())
    if wait_until_set_event_sent:
        _wait_for_alert_events(op, 1)

    set_nodes_banned(replicas, False)

    op.track()
    if return_events:
        _wait_for_alert_events(op, min_events)
        return _lookup_ordered_by_id_row(op.id)["alert_events"]
    else:
        return op


class TestSchedulerAlertHistoryBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 5

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "scheduler_connector": {
                    "heartbeat_executor": {
                        "period": 200,  # 200 msec
                    },
                },
                "controller_agent_connector": {"settle_jobs_timeout": 30000},  # 30 sec
            }
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {"job_environment": {"block_io_watchdog_period": 100}},
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operations_update_period": 100,
            "schedule_allocation_time_limit": 3000,
            "event_log": {"flush_period": 1000},
            "operations_cleaner": {
                "enable": True,
                "operation_alert_event_send_period": 100,
            },
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "iops_threshold": 50,
            "operations_update_period": 100,
            "alert_manager": {
                "period": 200,
            },
            "map_reduce_operation_options": {"min_uncompressed_block_size": 1},
            "event_log": {"flush_period": 1000},
        }
    }


class TestSchedulerAlertHistory(TestSchedulerAlertHistoryBase):
    def setup_method(self, method):
        super(TestSchedulerAlertHistory, self).setup_method(method)
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(),
            override_tablet_cell_bundle="default",
        )

    @authors("egor-gutrov")
    def test_missing_input_chunks_alert_on_off(self):
        alert_events = _run_op_with_input_chunks_alert(min_events=2)
        assert len(alert_events) == 2
        assert "operation_id" not in alert_events[0]
        assert alert_events[0]["alert_type"] == "lost_input_chunks"
        assert alert_events[0]["error"]["code"] != 0
        assert alert_events[0]["time"] == alert_events[0]["error"]["attributes"]["datetime"]

        assert alert_events[1]["alert_type"] == "lost_input_chunks"
        assert alert_events[1]["error"]["code"] == 0

    @authors("egor-gutrov")
    def test_events_limit_in_archive(self):
        update_scheduler_config("operations_cleaner/max_alert_event_count_per_alert_type", 1)

        alert_events = _run_op_with_input_chunks_alert(min_events=1)
        assert len(alert_events) == 1
        assert alert_events[0]["alert_type"] == "lost_input_chunks"
        assert alert_events[0]["error"]["code"] == 0

        update_scheduler_config("operations_cleaner/max_alert_event_count_per_alert_type", 1000)

    @authors("egor-gutrov")
    def test_events_limit_on_scheduler(self):
        update_scheduler_config("operations_cleaner/max_enqueued_operation_alert_event_count", 1)

        alert_events = _run_op_with_input_chunks_alert(wait_until_set_event_sent=True, min_events=2)
        assert len(alert_events) == 2
        assert alert_events[0]["alert_type"] == "lost_input_chunks"
        assert alert_events[0]["error"]["code"] != 0
        assert alert_events[0]["time"] == alert_events[0]["error"]["attributes"]["datetime"]

        assert alert_events[1]["alert_type"] == "lost_input_chunks"
        assert alert_events[1]["error"]["code"] == 0

        update_scheduler_config("operations_cleaner/operation_alert_event_send_period", 10**7)
        time.sleep(5)

        op = _run_op_with_input_chunks_alert(return_events=False)
        update_scheduler_config("operations_cleaner/operation_alert_event_send_period", 100)
        wait(lambda: get(CLEANER_ORCHID + "/enqueued_alert_events") == 0)

        alert_events = _lookup_ordered_by_id_row(op.id)["alert_events"]
        assert len(alert_events) == 0

        update_scheduler_config("operations_cleaner/max_enqueued_operation_alert_event_count", 1000)

    @authors("egor-gutrov")
    def test_disabled_alert_event_archivation(self):
        update_scheduler_config("operations_cleaner/enable_operation_alert_event_archivation", False)
        update_scheduler_config("operations_cleaner/max_enqueued_operation_alert_event_count", 2)

        op1 = _run_op_with_input_chunks_alert(return_events=False)
        op2 = _run_op_with_input_chunks_alert(return_events=False)

        update_scheduler_config("operations_cleaner/enable_operation_alert_event_archivation", True)
        update_scheduler_config("operations_cleaner/max_enqueued_operation_alert_event_count", 1000)
        wait(
            lambda: _lookup_ordered_by_id_row(op1.id)["alert_events"] == YsonEntity(),
            ignore_exceptions=False,
        )
        _wait_for_alert_events(op2, 2)


class TestUpdateAlertEventsSenderPeriodOnDisabledCleaner(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 3

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operations_update_period": 100,
            "schedule_allocation_time_limit": 3000,
            "event_log": {"flush_period": 1000},
            "operations_cleaner": {
                "enable": False,
            },
        }
    }

    @authors("egor-gutrov")
    def test_update_period_on_disabled_cleaner(self):
        # There was a bug when cleaner crashed on such update when it was disabled.
        update_scheduler_config("operations_cleaner/operation_alert_event_send_period", 1000)


class TestAlertsHistoryInApi(TestSchedulerAlertHistoryBase):
    def setup_method(self, method):
        super(TestAlertsHistoryInApi, self).setup_method(method)
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(),
            override_tablet_cell_bundle="default",
        )

    @authors("egor-gutrov")
    def test_get_operation(self):
        op = _run_op_with_input_chunks_alert(return_events=False, min_events=2)
        alert_events = get_operation(op.id, attributes=["alert_events"])["alert_events"]

        assert len(alert_events) == 2
        assert alert_events[0]["alert_type"] == "lost_input_chunks"
        assert alert_events[0]["error"]["code"] != 0
        assert alert_events[0]["time"] == alert_events[0]["error"]["attributes"]["datetime"]

        assert alert_events[1]["alert_type"] == "lost_input_chunks"
        assert alert_events[1]["error"]["code"] == 0

    @authors("egor-gutrov")
    def test_list_operations(self):
        _run_op_with_input_chunks_alert(return_events=False, min_events=2)
        _run_op_with_input_chunks_alert(return_events=False, min_events=2)
        operations = list_operations(
            attributes=["id", "alert_events"],
            include_archive=True,
            from_time=datetime_to_string(datetime.utcfromtimestamp(0)),
            to_time=datetime_to_string(datetime.utcnow()),
        )["operations"]

        for op_desc in operations:
            alert_events = op_desc["alert_events"]
            assert len(alert_events) == 2
            assert alert_events[0]["alert_type"] == "lost_input_chunks"
            assert alert_events[0]["error"]["code"] != 0
            assert alert_events[0]["time"] == alert_events[0]["error"]["attributes"]["datetime"]

            assert alert_events[1]["alert_type"] == "lost_input_chunks"
            assert alert_events[1]["error"]["code"] == 0

    @authors("egor-gutrov")
    def test_acl_in_history(self):
        create_test_tables(force=True)
        op = map(
            command=with_breakpoint("BREAKPOINT; cat"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            track=False,
        )
        wait(lambda: op.get_state() == "running")
        update_op_parameters(
            op.id,
            parameters={"acl": [make_ace("allow", "missing_user", ["read", "manage"])]},
        )
        _wait_for_alert_events(op, 1)
        time.sleep(0.1)  # make sure next invalid_acl occurs later
        update_op_parameters(
            op.id,
            parameters={"acl": [make_ace("allow", "another_missing_user", ["read", "manage"])]},
        )
        update_op_parameters(op.id, parameters={"acl": []})
        wait(lambda: not op.get_alerts())
        _wait_for_alert_events(op, 2)
        release_breakpoint()

        alert_events = get_operation(op.id, attributes=["alert_events"])["alert_events"]

        assert len(alert_events) == 2
        assert alert_events[0]["alert_type"] == "invalid_acl"
        assert alert_events[0]["error"]["code"] != 0
        assert alert_events[0]["time"] != alert_events[0]["error"]["attributes"]["datetime"]

        assert alert_events[1]["alert_type"] == "invalid_acl"
        assert alert_events[1]["error"]["code"] == 0

    @authors("egor-gutrov")
    def test_completed_by_user_request(self):
        op = vanilla(
            spec={"tasks": {"main": {"command": "sleep 1000", "job_count": 1}}},
            track=False,
        )
        wait(lambda: get(op.get_path() + "/@state") == "running")
        complete_op(op.id)
        assert get(op.get_path() + "/@state") == "completed"
        _wait_for_alert_events(op, 1)
        alert_events = get_operation(op.id, attributes=["alert_events"])["alert_events"]

        assert len(alert_events) == 1
        assert alert_events[0]["alert_type"] == "operation_completed_by_user_request"
        assert alert_events[0]["error"]["code"] != 0


class TestAlertsHistoryInApiRpcProxy(TestAlertsHistoryInApi):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True
