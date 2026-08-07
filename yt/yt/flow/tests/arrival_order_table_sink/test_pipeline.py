import os
from datetime import datetime, timedelta

import pytest
import yatest.common

from yt.common import wait
from yt.yt.flow.library.python.bullied_process import ProcessExitedNormallyException
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")
QUEUE_SCHEMA = [
    {"name": "partition_id", "type": "uint64"},
    {"name": "sequence", "type": "int64"},
    {"name": "data_weight", "type": "int64"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]


class TestArrivalOrderTableSink(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def setup_method(self, method):
        super().setup_method(method)
        run_yt_sync(
            self.primary_cluster_name,
            self.work_yt_path,
            tablet_cell_bundle=self.tablet_cell_bundle,
            primary_medium=self.primary_medium,
            add_input_queue_and_consumer=True,
            input_queue_schema=QUEUE_SCHEMA,
            input_queue_tablet_count=2,
        )
        self.input_queue = f"{self.work_yt_path}/input_queue"
        self.input_consumer = f"{self.work_yt_path}/consumer"
        self.output_root = f"{self.work_yt_path}/output"

    def prepare_config(self, *, max_row_count=100, output_cluster=None, crash_sentinel=None):
        output_cluster = output_cluster or self.primary_cluster_name
        self.sink_client = self.cluster_name_to_client[output_cluster]
        config = get_yson_config(PIPELINE_CONFIG_PATH)
        reader = config["spec"]["computations"]["reader"]
        reader["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster={self.primary_cluster_name}>{self.input_queue}",
                "consumer_path": f"<cluster={self.primary_cluster_name}>{self.input_consumer}",
            }
        )
        sink = reader["sinks"]["static"]
        sink["parameters"].update(
            {
                "output_directory": f"<cluster={output_cluster}>{self.output_root}",
            }
        )
        if crash_sentinel:
            sink["sink_class_name"] = "NTest::TCrashAfterExternalCommitSink"
            sink["parameters"].update(
                {
                    "crash_sentinel_path": crash_sentinel,
                    "table_period": "1h",
                }
            )
        config["dynamic_spec"]["computations"]["reader"]["sinks"]["static"]["parameters"][
            "max_row_count"
        ] = max_row_count
        self.patch_config(config)
        return self.dump_config_to_log_dir(config, "arrival_order_table_sink.yson")

    def write_rows(self, *rows):
        self.client.insert_rows(
            self.input_queue,
            [
                {
                    "partition_id": partition_id,
                    "sequence": sequence,
                    "data_weight": 1,
                    "$tablet_index": partition_id,
                }
                for partition_id, sequence in rows
            ],
        )

    def table_names(self):
        return self.sink_client.list(self.output_root) if self.sink_client.exists(self.output_root) else []

    def table_row_counts(self):
        return [self.sink_client.get(f"{self.output_root}/{name}/@row_count") for name in self.table_names()]

    def table_rows(self):
        rows = []
        for name in self.table_names():
            rows.extend(self.sink_client.read_table(f"{self.output_root}/{name}"))
        return rows

    def table_timestamps(self):
        result = []
        for name in self.table_names():
            value = self.sink_client.get(f"{self.output_root}/{name}/@table_timestamp")
            if isinstance(value, bytes):
                value = value.decode()
            if not isinstance(value, datetime):
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            result.append(value)
        return sorted(result)

    def source_persisted_offset(self):
        total = 0
        found = False
        for row in self.client.select_rows(f"* from [{self.pipeline_path}/states]"):
            computation_id = row["computation_id"]
            name = row["name"]
            if isinstance(computation_id, bytes):
                computation_id = computation_id.decode()
            if isinstance(name, bytes):
                name = name.decode()
            if computation_id != "reader" or name != "/$active_source/v0":
                continue
            found = True
            state = row["state"]
            persisted = state.get("persisted_offset_exclusive_v2", state.get(b"persisted_offset_exclusive_v2"))
            if persisted:
                total += int(persisted[0])
        return total if found else None

    @pytest.mark.authors(["pechatnov"])
    def test_gap_free_batches_and_empty_slots(self):
        self.write_rows((0, 0), (0, 1), (0, 2))
        config_path = self.prepare_config(max_row_count=2)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            wait(
                lambda: sum(self.table_row_counts()) == 3 and len(self.table_row_counts()) >= 3,
                timeout=180,
                ignore_exceptions=True,
            )
            assert sorted(row["sequence"] for row in self.table_rows()) == [0, 1, 2]
            assert max(self.table_row_counts()) == 2
            assert 0 in self.table_row_counts()
            timestamps = self.table_timestamps()
            assert all(right - left == timedelta(seconds=1) for left, right in zip(timestamps, timestamps[1:]))

    @pytest.mark.authors(["pechatnov"])
    def test_two_source_partitions_share_one_sequence(self):
        self.write_rows((0, 0), (1, 0))
        config_path = self.prepare_config(max_row_count=1)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            wait(lambda: sum(self.table_row_counts()) == 2, timeout=180, ignore_exceptions=True)
            assert {row["partition_id"] for row in self.table_rows()} == {0, 1}
            timestamps = self.table_timestamps()
            assert len(timestamps) == len(set(timestamps))
            assert all(right - left == timedelta(seconds=1) for left, right in zip(timestamps, timestamps[1:]))

            progress = self.sink_client.get(f"{self.output_root}/@progress")
            partitions = progress.get("partitions", progress.get(b"partitions"))
            assert len(partitions) == 2

    @pytest.mark.authors(["pechatnov"])
    def test_precreated_output_directory_is_adopted(self):
        self.client.create("map_node", self.output_root, recursive=True)
        self.write_rows((0, 7))
        config_path = self.prepare_config(max_row_count=1)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            wait(lambda: sum(self.table_row_counts()) == 1, timeout=180, ignore_exceptions=True)
            assert self.table_rows()[0]["sequence"] == 7
            assert self.sink_client.exists(f"{self.output_root}/@progress")

    @pytest.mark.authors(["pechatnov"])
    def test_output_can_use_another_cluster(self):
        self.write_rows((0, 42))
        config_path = self.prepare_config(
            max_row_count=1,
            output_cluster=self.remote_cluster_names[0],
        )

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            wait(lambda: sum(self.table_row_counts()) == 1, timeout=180, ignore_exceptions=True)
            assert self.table_rows()[0]["sequence"] == 42
            assert self.sink_client.exists(f"{self.output_root}/@progress")
            assert not self.client.exists(self.output_root)

    @pytest.mark.authors(["pechatnov"])
    def test_external_commit_crash_replays_exactly_once(self):
        self.write_rows((0, 7))
        sentinel = os.path.join(self.path_to_flow_logs, "external_commit_sentinel")
        config_path = self.prepare_config(max_row_count=1, crash_sentinel=sentinel)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": config_path},
            workers_count=1,
            controllers_count=1,
            start_watcher_thread=False,
            problems=False,
        ) as federation:
            wait(lambda: os.path.exists(sentinel), timeout=180)
            wait(lambda: not federation.workers[0].is_running(), timeout=30)
            assert [row["sequence"] for row in self.table_rows()] == [7]
            assert self.source_persisted_offset() == 0

            try:
                federation.workers[0].restart()
            except ProcessExitedNormallyException:
                federation.workers[0].restart()
            wait(lambda: self.source_persisted_offset() == 1, timeout=180)
            assert [row["sequence"] for row in self.table_rows()].count(7) == 1
