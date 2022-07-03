from yt_env_setup import YTEnvSetup

from yt_commands import authors, create_pool, run_test_vanilla, ls

from yt_helpers import read_structured_log, write_log_barrier

import yt.yson as yson
from yt.environment import arcadia_interop

import os
import subprocess


PREPARE_SCHEDULING_USAGE_BINARY = arcadia_interop.search_binary_path("prepare_scheduling_usage")


@authors("ignat")
class TestPrepareSchedulingUsage(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    ENABLE_HTTP_PROXY = True

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "accumulated_usage_log_period": 1000,
            "accumulated_resource_usage_update_period": 100,
        }
    }

    def _run_script(self, rows):
        prepare_scheduling_usage_dir = os.path.join(self.Env.path, "prepare_scheduling_usage")
        if not os.path.exists(prepare_scheduling_usage_dir):
            os.mkdir(prepare_scheduling_usage_dir)

        input_filepath = os.path.join(prepare_scheduling_usage_dir, "input.yson")
        output_filepath = os.path.join(prepare_scheduling_usage_dir, "output.yson")

        with open(input_filepath, "wb") as fout:
            yson.dump(rows, fout, yson_type="list_fragment")

        subprocess.check_call([
            PREPARE_SCHEDULING_USAGE_BINARY,
            "--cluster", self.Env.get_proxy_address(),
            "--mode", "local",
            "--input-path", input_filepath,
            "--output-path", output_filepath
        ])

        with open(output_filepath, "rb") as fin:
            return list(yson.load(fin, yson_type="list_fragment"))

    def test_scheduler_simulator(self):
        create_pool("parent_pool", pool_tree="default")
        create_pool("test_pool", pool_tree="default", parent_name="parent_pool")

        scheduler_address = ls("//sys/scheduler/instances")[0]
        from_barrier = write_log_barrier(scheduler_address)

        op = run_test_vanilla(
            "sleep 5.2",
            pool="test_pool",
            spec={
                "annotations": {"my_key": "my_value"}
            },
            track=True)

        scheduler_log_file = self.path_to_run + "/logs/scheduler-0.json.log"
        to_barrier = write_log_barrier(scheduler_address)

        structured_log = read_structured_log(scheduler_log_file, from_barrier=from_barrier, to_barrier=to_barrier,
                                             row_filter=lambda e: "event_type" in e)

        for row in structured_log:
            row["cluster"] = "local_cluster"

        operation_event_indexes = []
        for index, row in enumerate(structured_log):
            if row["event_type"].startswith("operation_"):
                operation_event_indexes.append(index)

        assert len(operation_event_indexes) >= 2
        min_operation_event_index = operation_event_indexes[0]
        max_operation_event_index = operation_event_indexes[-1]

        mid_index = (min_operation_event_index + max_operation_event_index) // 2
        structured_log_part1 = structured_log[min_operation_event_index:mid_index]
        structured_log_part2 = structured_log[mid_index:max_operation_event_index + 1]

        rows1 = self._run_script(structured_log_part1)
        assert len(rows1) >= 1

        rows2 = self._run_script(structured_log_part2)
        assert len(rows2) >= 1

        rows = rows1 + rows2

        accumulated_resource_usage_cpu = 0.0
        for index, row in enumerate(rows):
            accumulated_resource_usage_cpu += row["accumulated_resource_usage_cpu"]
            assert row["operation_id"] == op.id
            assert row["cluster"] == "local_cluster"
            assert row["pool_path"] == "/parent_pool/test_pool"
            assert yson.loads(row["annotations"].encode("ascii")) == {"my_key": "my_value"}
            if index + 1 < len(rows):
                assert row["operation_state"] == "running"
            else:
                assert row["operation_state"] == "completed"
        assert 5.0 <= accumulated_resource_usage_cpu <= 10.0
