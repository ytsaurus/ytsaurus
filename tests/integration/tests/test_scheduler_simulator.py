from yt_env_setup import YTEnvSetup
from yt_commands import *
from yt.common import date_string_to_datetime
import yt.yson as yson

from copy import deepcopy
import time
import os
import subprocess
import csv

EPS = 1e-4

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
#TODO: get rid of copy-paste: functions below are from scheduler simulator prepare script

def is_job_event(row):
    return row["event_type"].startswith("job_")

def is_operation_event(row):
    return row["event_type"].startswith("operation_")

def is_job_completion_event(row):
    return row["event_type"] not in ["job_started"]

def is_operation_completion_event(row):
    return row["event_type"] not in ["operation_started", "operation_prepared"]

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
        "state": row["event_type"].replace("job_", "", 1)
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
            job_descriptions.append((
                row["duration"],
                row["resource_limits"]["memory"],
                row["resource_limits"]["cpu"],
                row["resource_limits"]["user_slots"],
                row["resource_limits"]["network"],
                row["job_id"],
                row["job_type"],
                row["state"]))

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
            "in_timeframe")

        return {field : result[field] for field in fields}

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
#TODO: get rid of copypaste: extract_pools_distribution is from analytics script

def extract_metric_distribution(row, metric, target):
    if row["event_type"] != "fair_share_info":
        return None
    timestamp = row["timestamp"]
    pools = row[target]
    result = {target: {}, "timestamp": timestamp}
    for pool in pools.iteritems():
        name = pool[0]
        metric_value = pool[1][metric]
        if metric_value != 0:
            result[target][name] = metric_value
    return result

##################################################################

ONE_GB = 1024 * 1024 * 1024

scheduler_simulator_config = {
    "heartbeat_period": 500,
    "pools_file": None,
    "operations_stats_file": None,
    "event_log_file": None,
    "enable_full_event_log": False,
    "default_tree": "test_pool",
    "scheduler": None,
    "node_groups": [{"count": 1, "tags": ["internal"],
                     "resource_limits": {"cpu": 2.0, "memory": 8 * ONE_GB, "user_slots": 40, "network": 100}},
                    {"count": 1, "tags": ["external"],
                     "resource_limits": {"cpu": 2.0, "memory": 8 * ONE_GB, "user_slots": 40, "network": 100}}],
    "cycles_per_flush": 1000000,
}

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
                    }
                )
            },
            attributes={
                "nodes_filter": "internal",
                "max_operation_count": 2000,
                "max_operation_count_per_pool": 50,
                "max_running_operation_count": 1000,
                "max_running_operation_count_per_pool": 50,
            }
        )
    },
    attributes={
        "default_tree": "default",
    }
)


class TestSchedulerSimulator(YTEnvSetup, PrepareTables):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
            "fair_share_profiling_period": 100,
            "event_log": {
                "flush_period": 100,
            }
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "event_log": {
                "flush_period": 100,
            }
        }
    }

    def test_scheduler_simulator(self):
        resource_limits = {"cpu": 1, "memory": ONE_GB, "network": 10}
        create("map_node", "//sys/pools/test_pool", attributes={"resource_limits": resource_limits})

        self._prepare_tables()
        data = [{"foo": i} for i in xrange(3)]
        write_table("//tmp/t_in", data)

        map(
            command="sleep 1;",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"job_count": 1,
                  "max_failed_job_count": 1,
                  "data_size_per_job": 1,
                  "pool": "test_pool"})

        time.sleep(5)

        event_log = read_table("//sys/scheduler/event_log")
        processed_rows = []
        for row in event_log:
            processed_rows.extend(list(process_event_log_row(row)))

        simulator_data_dir = os.path.join(self.Env.path, "simulator_data")
        os.mkdir(simulator_data_dir)
        simulator_files_path = self._get_simulator_files_path(simulator_data_dir)

        output = do_reduce(processed_rows, "operation_id", group_by_operation)
        output.sort(key=lambda x: date_string_to_datetime(x["start_time"]))
        assert len(output) == 1
        operation_id = output[0]["operation_id"]
        with open(simulator_files_path["simulator_input_yson_file"], "w") as fout:
            yson.dump(output, fout, yson_type="list_fragment")

        with open(simulator_files_path["simulator_input_yson_file"]) as fin:
            subprocess.check_call(["convert_operations_to_binary_format",
                                   simulator_files_path["simulator_input_bin_file"]],
                                  stdin=fin)

        self._set_scheduler_simulator_config_params(simulator_files_path)
        with open(simulator_files_path["scheduler_simulator_config_yson_file"], "w") as fout:
            fout.write(yson.dumps(scheduler_simulator_config))
        with open(simulator_files_path["pools_test_yson_file"], "w") as fout:
            yson.dump(pools_config, fout)

        with open(simulator_files_path["simulator_input_bin_file"]) as fin:
            subprocess.check_call(["scheduler_simulator",
                                   simulator_files_path["scheduler_simulator_config_yson_file"]],
                                  stdin=fin)

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

        error_count = 0
        pool_and_operations_validated = False
        with open(simulator_files_path["scheduler_event_log_file"]) as fin:
            for item in yson.load(fin, "list_fragment"):
                usage_pools = extract_metric_distribution(item, "usage_ratio", "pools")
                usage_operations = extract_metric_distribution(item, "usage_ratio", "operations")
                if usage_pools is not None and "test_pool" in usage_pools["pools"] and \
                   usage_operations is not None and operation_id in usage_operations["operations"]:
                    error_count += (usage_pools["pools"]["test_pool"] != usage_operations["operations"][operation_id])
                    pool_and_operations_validated = True
        # NB: some explanation of possible non-zero error count:
        # 1. Scheduler simulator are running by 2 (default value) thread in this test.
        # 2. One thread may simulate job start, while another thread perform logging.
        # 3. Update of usage_ratio goes from bottom to up, some at some point we can have non-zero resource usage
        #    in operation but still have zero resource usage in pool while update is going on.
        assert error_count <= 1
        assert pool_and_operations_validated

    def _get_simulator_files_path(self, simulator_data_dir):
        files = dict()
        files["simulator_input_yson_file"] = os.path.join(simulator_data_dir, "simulator_input.yson")
        files["simulator_input_bin_file"] = os.path.join(simulator_data_dir, "simulator_input.bin")
        files["scheduler_simulator_config_yson_file"] = os.path.join(simulator_data_dir, "scheduler_simulator_config.yson")
        files["pools_test_yson_file"] = os.path.join(simulator_data_dir, "pools_test.yson")
        files["operations_stats_file"] = os.path.join(simulator_data_dir, "operations_stats_test.csv")
        files["scheduler_event_log_file"] = os.path.join(simulator_data_dir, "scheduler_event_log_test.txt")
        return files

    def _set_scheduler_simulator_config_params(self, simulator_files_path):
        global scheduler_simulator_config
        scheduler_simulator_config["pools_file"] = simulator_files_path["pools_test_yson_file"]
        scheduler_simulator_config["operations_stats_file"] = simulator_files_path["operations_stats_file"]
        scheduler_simulator_config["event_log_file"] = simulator_files_path["scheduler_event_log_file"]
        scheduler_config = self.Env.configs["scheduler"][0]["scheduler"]
        scheduler_simulator_config["scheduler"] = scheduler_config

##################################################################
