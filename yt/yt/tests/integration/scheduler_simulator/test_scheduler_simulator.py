from yt_env_setup import YTEnvSetup, search_binary_path

from yt_commands import (
    authors, wait, create, create_user, create_pool, read_table, write_table,
    map, PrepareTables)

import yt.yson as yson
from yt.common import date_string_to_datetime

import pytest

import os
import subprocess
import csv
import sys
from copy import deepcopy
from collections import defaultdict

##################################################################
# TODO: get rid of copy-paste: functions below are from scheduler simulator prepare script


def is_job_event(row):
    return row["event_type"].startswith("job_")


def is_operation_event(row):
    return row["event_type"].startswith("operation_")


def is_job_completion_event(row):
    return row["event_type"] not in ["job_started"]


def is_operation_completion_event(row):
    return row["event_type"] not in [
        "operation_started",
        "operation_prepared",
        "operation_materialized",
    ]


def is_operation_prepared_event(row):
    return row["event_type"] == "operation_prepared"


def is_operation_started_event(row):
    return row["event_type"] == "operation_started"


def has_resource_limits(row):
    return "resource_limits" in row


def to_seconds(delta):
    return delta.total_seconds()


def get_duration_in_timeframe(row):
    start_time = date_string_to_datetime(row["start_time"])
    finish_time = date_string_to_datetime(row["finish_time"])
    return to_seconds(finish_time - start_time)


def process_job_event(row):
    if not is_job_completion_event(row) or not has_resource_limits(row):
        return

    if row["event_type"] == "job_aborted" and row["reason"] == "preemption":
        return

    if "job_type" not in row:
        row["job_type"] = "unknown"

    yield {
        "operation_id": row["operation_id"],
        "duration": get_duration_in_timeframe(row),
        "resource_limits": row["resource_limits"],
        "job_id": row["job_id"],
        "job_type": row["job_type"],
        "state": row["event_type"].replace("job_", "", 1),
    }


def process_operation_event(row):
    if is_operation_prepared_event(row):
        yield row
    elif is_operation_completion_event(row):
        row["in_timeframe"] = True
        yield row


def process_event_log_row(row):
    if is_job_event(row):
        for item in process_job_event(row):
            yield item

    if is_operation_event(row):
        for item in process_operation_event(row):
            yield item


def group_by_operation(key, rows):
    job_descriptions = []
    operation = None
    prepare_time = None

    for row in rows:
        if "event_type" in row:
            if is_operation_completion_event(row):
                operation = deepcopy(row)
            elif is_operation_prepared_event(row):
                prepare_time = row["timestamp"]
        else:
            job_descriptions.append(
                (
                    row["duration"],
                    row["resource_limits"]["user_memory"],
                    row["resource_limits"]["cpu"],
                    row["resource_limits"]["user_slots"],
                    row["resource_limits"]["network"],
                    row["job_id"],
                    row["job_type"],
                    row["state"],
                )
            )

    if operation is not None and job_descriptions:
        result = deepcopy(operation)
        result["state"] = result["event_type"].replace("operation_", "", 1)
        result["job_descriptions"] = job_descriptions

        if prepare_time:
            result["start_time"] = prepare_time

        fields = (
            "operation_id",
            "job_descriptions",
            "start_time",
            "finish_time",
            "authenticated_user",
            "spec",
            "operation_type",
            "state",
            "error",
            "in_timeframe",
        )

        return {field: result[field] for field in fields}


##################################################################


def do_reduce(records, key_column, func):
    output = []
    records = sorted(records, key=lambda record: record.get(key_column))
    prev_key = None
    is_first_record = True
    rows = []
    for record in records:
        current_key = record.get(key_column)
        if current_key != prev_key and not is_first_record:
            output.append(func(prev_key, rows))
            rows = []
        is_first_record = False
        prev_key = current_key
        rows.append(record)
    output.append(func(prev_key, rows))
    return output


##################################################################
# TODO: get rid of copypaste: extract_pools_distribution is from analytics script


def extract_metric_distribution(row, metric, target):
    if row["event_type"] != "fair_share_info":
        return None
    timestamp = row["timestamp"]
    pools = row[target]
    result = {target: {}, "timestamp": timestamp}
    for pool in pools.items():
        name = pool[0]
        metric_value = pool[1][metric]
        if metric_value != 0:
            result[target][name] = metric_value
    return result


##################################################################


def resource_usage_sum(resource_usages):
    result = defaultdict(int)
    for resource_usage in resource_usages:
        for resource, value in resource_usage.items():
            result[resource] += value
    return dict(result)


def resources_equal(lhs, rhs):
    lhs = defaultdict(int, lhs)
    rhs = defaultdict(int, rhs)
    if "user_memory" in lhs:
        lhs["memory"] = lhs["user_memory"]
        del lhs["user_memory"]
    if "user_memory" in rhs:
        rhs["memory"] = rhs["user_memory"]
        del rhs["user_memory"]

    return all(
        lhs[resource] == rhs[resource] for resource in {resource for resource in lhs} | {resource for resource in rhs}
    )


##################################################################

SIMULATOR_BINARY = search_binary_path("scheduler_simulator")

ONE_GB = 1024 * 1024 * 1024

default_scheduler_simulator_config = {
    "heartbeat_period": 500,
    "pools_file": None,
    "operations_stats_file": None,
    "event_log_file": None,
    "enable_full_event_log": False,
    "default_tree": "test_pool",
    "scheduler_config_file": None,
    "node_groups_file": None,
    "cycles_per_flush": 1000000,
    "address_resolver": {
        "localhost_fqdn": "localhost",
    },
    "logging": {
        "flush_period": 1000,
        "rules": [
            {
                "min_level": "warning",
                "writers": ["stderr"],
            },
            {
                "min_level": "debug",
                "writers": ["debug"],
            },
        ],
        "writers": {
            "debug": {"file_name": None, "type": "file"},
            "stderr": {"type": "stderr"},
        },
    },
}

node_groups = [
    {
        "count": 1,
        "tags": ["internal"],
        "resource_limits": {
            "cpu": 2.0,
            "memory": 8 * ONE_GB,
            "user_slots": 40,
            "network": 100,
        },
    },
    {
        "count": 1,
        "tags": ["external"],
        "resource_limits": {
            "cpu": 2.0,
            "memory": 8 * ONE_GB,
            "user_slots": 40,
            "network": 100,
        },
    },
]

pools_config = yson.to_yson_type(
    {
        "default": yson.to_yson_type(
            {
                "test_pool": yson.to_yson_type(
                    {},
                    attributes={
                        "resource_limits": {
                            "user_slots": 10,
                            "cpu": 10,
                            "memory": 4 * ONE_GB,
                        },
                        "mode": "fifo",
                        "id": "10dd2-ac0ee-3ff03e9-8d975f74",
                    },
                )
            },
            attributes={
                "config": {
                    "nodes_filter": "internal",
                    "max_operation_count": 2000,
                    "max_operation_count_per_pool": 50,
                    "max_running_operation_count": 1000,
                    "max_running_operation_count_per_pool": 50,
                    "enable_scheduled_and_preempted_resources_profiling": False,
                },
            },
        )
    },
    attributes={
        "default_tree": "default",
    },
)


@authors("antonkikh")
@pytest.mark.enabled_multidaemon
class TestSchedulerSimulator(YTEnvSetup, PrepareTables):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "fair_share_profiling_period": 100,
            "event_log": {
                "enable": True,
                "flush_period": 100,
            },
            "nodes_info_logging_period": 100,
            "fair_share_log_period": 100,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "event_log": {
                "enable": True,
                "flush_period": 100,
            }
        }
    }

    def test_scheduler_simulator(self):
        resource_limits = {"cpu": 1, "memory": ONE_GB, "network": 10}
        create_pool("test_pool", attributes={"resource_limits": resource_limits})

        self._prepare_tables()
        data = [{"foo": i} for i in range(3)]
        write_table("//tmp/t_in", data)

        op = map(
            command="sleep 1;",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "job_count": 1,
                "max_failed_job_count": 1,
                "data_size_per_job": 1,
                "pool": "test_pool",
            },
        )

        def check_info_flushed():
            check_info_flushed.processed_rows = []
            event_log = read_table("//sys/scheduler/event_log", verbose=False)

            operation_completed = False
            job_completed = False

            for row in event_log:
                check_info_flushed.processed_rows.extend(list(process_event_log_row(row)))

                if "operation_id" in row and row["operation_id"] == op.id:
                    if is_operation_event(row):
                        operation_completed |= is_operation_completion_event(row)
                    elif is_job_event(row):
                        job_completed |= is_job_completion_event(row)

            return operation_completed and job_completed

        check_info_flushed.processed_rows = []
        wait(check_info_flushed)
        processed_rows = check_info_flushed.processed_rows

        simulator_data_dir = os.path.join(self.Env.path, "simulator_data")
        os.mkdir(simulator_data_dir)
        simulator_files_path = self._get_simulator_files_path(simulator_data_dir)

        output = do_reduce(processed_rows, "operation_id", group_by_operation)
        output.sort(key=lambda x: date_string_to_datetime(x["start_time"]))
        assert len(output) == 1
        operation_id = output[0]["operation_id"]
        with open(simulator_files_path["simulator_input_yson_file"], "wb") as fout:
            yson.dump(output, fout, yson_type="list_fragment")

        with open(simulator_files_path["simulator_input_yson_file"], "rb") as fin:
            subprocess.check_call(
                [SIMULATOR_BINARY, "convert-operations-to-binary-format", "--destination", simulator_files_path["simulator_input_bin_file"]],
                stdin=fin,
            )

        self._set_scheduler_simulator_config_params(simulator_files_path)
        with open(simulator_files_path["scheduler_simulator_config_yson_file"], "wb") as fout:
            fout.write(yson.dumps(self.scheduler_simulator_config))

        with open(simulator_files_path["node_groups_yson_file"], "wb") as fout:
            yson.dump(node_groups, fout)

        with open(simulator_files_path["pools_test_yson_file"], "wb") as fout:
            yson.dump(pools_config, fout)

        scheduler_config = self.Env.configs["scheduler"][0]["scheduler"]
        with open(simulator_files_path["scheduler_config_yson_file"], "wb") as fout:
            yson.dump(scheduler_config, fout)

        with open(simulator_files_path["simulator_input_bin_file"], "rb") as fin:
            subprocess.check_call(
                [
                    SIMULATOR_BINARY,
                    "--allow-debug-mode",
                    simulator_files_path["scheduler_simulator_config_yson_file"],
                ],
                stdin=fin,
                stderr=sys.stderr,
            )

        with open(simulator_files_path["operations_stats_file"]) as fin:
            reader = csv.DictReader(fin)
            total_row = 0
            for row in reader:
                total_row += 1
                assert row["id"] == operation_id
                assert row["operation_type"] == "Map"
                assert row["operation_state"] == "completed"
                assert row["job_count"] == "1"
            assert total_row == 1

        self.pool_and_operation_info_count = 0
        self.nodes_info_count = 0
        self.fair_share_info_error_count = 0
        self.nodes_info_error_count = 0
        self.operations_resource_usage = None

        for item in self._get_simulator_event_log():
            if item["event_type"] == "fair_share_info":
                self._parse_fair_share_info(item, operation_id)
            if item["event_type"] == "nodes_info":
                self._parse_nodes_info(item)

        assert self.pool_and_operation_info_count >= 5
        assert self.nodes_info_count >= 5

        # NB: Some explanation of possible non-zero error count:
        # 1. Scheduler simulator are running by 2 (default value) thread in this test.
        # 2. One thread may simulate job start, while another thread perform logging.
        # 3. Update of usage_ratio goes from bottom to up, some at some point we can have non-zero resource usage
        #    in operation but still have zero resource usage in pool while update is going on.
        assert self.fair_share_info_error_count <= 1

        # Up to two inconsistent situations might occur due to races in logging
        # One when the operation is started and one when it's finished.
        assert self.nodes_info_error_count <= 2

    def _get_simulator_files_path(self, simulator_data_dir):
        files = dict()
        files["simulator_input_yson_file"] = os.path.join(simulator_data_dir, "simulator_input.yson")
        files["simulator_input_bin_file"] = os.path.join(simulator_data_dir, "simulator_input.bin")
        files["scheduler_simulator_config_yson_file"] = os.path.join(
            simulator_data_dir, "self.scheduler_simulator_config.yson"
        )
        files["node_groups_yson_file"] = os.path.join(simulator_data_dir, "node_groups.yson")
        files["scheduler_config_yson_file"] = os.path.join(simulator_data_dir, "scheduler_config.yson")
        files["pools_test_yson_file"] = os.path.join(simulator_data_dir, "pools_test.yson")
        files["operations_stats_file"] = os.path.join(simulator_data_dir, "operations_stats_test.csv")
        files["scheduler_event_log_file"] = os.path.join(simulator_data_dir, "scheduler_event_log_test.txt")
        files["simulator_debug_logs"] = os.path.join(simulator_data_dir, "simulator_debug_logs.txt")
        return files

    def _set_scheduler_simulator_config_params(self, simulator_files_path):
        # global default_scheduler_simulator_config
        self.scheduler_simulator_config = deepcopy(default_scheduler_simulator_config)
        self.scheduler_simulator_config["pools_file"] = simulator_files_path["pools_test_yson_file"]
        self.scheduler_simulator_config["operations_stats_file"] = simulator_files_path["operations_stats_file"]
        self.scheduler_simulator_config["event_log_file"] = simulator_files_path["scheduler_event_log_file"]
        self.scheduler_simulator_config["node_groups_file"] = simulator_files_path["node_groups_yson_file"]
        self.scheduler_simulator_config["scheduler_config_file"] = simulator_files_path["scheduler_config_yson_file"]
        self.scheduler_simulator_config["logging"]["writers"]["debug"]["file_name"] = simulator_files_path[
            "simulator_debug_logs"
        ]

    def _parse_fair_share_info(self, item, operation_id):
        usage_pools = extract_metric_distribution(item, "usage_ratio", "pools")
        usage_operations = extract_metric_distribution(item, "usage_ratio", "operations")
        if (
            usage_pools is not None
            and "test_pool" in usage_pools["pools"]
            and usage_operations is not None
            and operation_id in usage_operations["operations"]
        ):
            self.pool_and_operation_info_count += 1
            if usage_pools["pools"]["test_pool"] != usage_operations["operations"][operation_id]:
                self.fair_share_info_error_count += 1
        self.operations_resource_usage = resource_usage_sum(
            operation["resource_usage"] for operation in item["operations"].values()
        )

    def _parse_nodes_info(self, item):
        assert "nodes" in item
        nodes = item["nodes"]

        total_node_count = sum(node_group["count"] for node_group in node_groups)
        assert len(nodes) == total_node_count

        for node_group in node_groups:
            node_group_count = 0
            for node_info in nodes.values():
                if node_info["tags"] == node_group["tags"] and resources_equal(
                    node_info["resource_limits"], node_group["resource_limits"]
                ):
                    node_group_count += 1

            assert node_group["count"] == node_group_count

        self.nodes_info_count += 1
        nodes_resource_usage = resource_usage_sum(node_info["resource_usage"] for node_info in nodes.values())
        if not resources_equal(nodes_resource_usage, self.operations_resource_usage):
            self.nodes_info_error_count += 1

    def _get_simulator_event_log(self):
        simulator_data_dir = os.path.join(self.Env.path, "simulator_data")
        simulator_files_path = self._get_simulator_files_path(simulator_data_dir)
        with open(simulator_files_path["scheduler_event_log_file"], "rb") as fin:
            return list(yson.load(fin, "list_fragment"))


@authors("ignat")
@pytest.mark.enabled_multidaemon
class TestSchedulerSimulatorWithRemoteEventLog(TestSchedulerSimulator):
    ENABLE_MULTIDAEMON = True
    # We are going to remove remote event log in scheduler simulator anyway. This is the only
    # thing in scheduler simulator that requires native authentication, so there's no reason to
    # add authentication into scheduler simulator.
    USE_NATIVE_AUTH = False

    def _set_scheduler_simulator_config_params(self, simulator_files_path):
        super(TestSchedulerSimulatorWithRemoteEventLog, self)._set_scheduler_simulator_config_params(
            simulator_files_path
        )

        connection = self._get_cluster_connection()
        create("table", "//tmp/event_log")

        create_user("simulator")

        self.scheduler_simulator_config["remote_event_log"] = {
            "connection": connection,
            "event_log_manager": {"path": "//tmp/event_log"},
            "user": "simulator",
        }

    def _get_simulator_event_log(self):
        return sorted(read_table("//tmp/event_log"), key=lambda r: r["timestamp"])

    @classmethod
    def _get_cluster_connection(cls):
        return cls.Env._cluster_configuration["scheduler"][0]["cluster_connection"]


##################################################################
