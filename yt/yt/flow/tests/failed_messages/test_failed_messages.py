import pytest

import yatest.common
import yt.wrapper

from yt.common import wait

from yt.yt.flow.library.python.bullied_process import ProblemsConfig
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync
from yt.yt.flow.library.python.queue import batching_write_rows

SCHEMA = [
    {"name": "event_id", "type": "int64"},
    {"name": "key", "type": "uint64"},
    {"name": "value", "type": "int64"},
    {"name": "event_timestamp", "type": "uint64"},
]
EVENT_COUNT = 240
EVENT_TIMESTAMP = 1700000000
TABLET_COUNT = 3


def input_rows(begin, end):
    return [
        {
            "event_id": event_id,
            "key": event_id % 5,
            "value": -1 if event_id % 3 == 0 else event_id,
            "event_timestamp": EVENT_TIMESTAMP + event_id,
            "$tablet_index": event_id % TABLET_COUNT,
        }
        for event_id in range(begin, end)
    ]


class TestFailedMessages(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")
    DRIVER_BACKEND = "rpc"

    def setup_method(self, method):
        super().setup_method(method)
        self.input_queue = f"{self.work_yt_path}/input_queue"
        self.output_queue = f"{self.work_yt_path}/output_queue"
        self.failed_messages = f"{self.pipeline_path}/failed_messages"

    def prepare_config(self):
        config = get_yson_config(
            yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")
        )
        config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.work_yt_path}/consumer",
            }
        )
        config["spec"]["computations"]["capture"]["sinks"]["queue"]["parameters"]["queue_path"] = self.output_queue
        self.patch_config(config)
        return self.dump_config_to_log_dir(config, "pipeline.yson")

    def write_input(self, begin, end):
        batching_write_rows(input_rows(begin, end), lambda batch: self.client.insert_rows(self.input_queue, batch), 100)

    def read_failures(self):
        return list(
            self.client.select_rows(
                f"* from [{self.failed_messages}]", format=yt.wrapper.format.YsonFormat(encoding=None)
            )
        )

    def check_results(self, event_count, stream_specs, flow_core_target):
        failures = self.read_failures()
        failed_ids = [row[b"diagnostic"][b"error"][b"attributes"][b"event_id"] for row in failures]
        assert sorted(failed_ids) == list(range(0, event_count, 3))
        for row, event_id in zip(failures, failed_ids):
            assert row[b"computation_id"] == b"capture"
            assert row[b"stream_id"] == b"input"
            assert row[b"key"][-1] == event_id % 5
            assert row[b"message_id"]
            assert row[b"payload"]
            assert row[b"event_timestamp"] == EVENT_TIMESTAMP + event_id
            assert row[b"failed_at"] > 0
            assert row[b"replay_attempts"] == 0
            assert row[b"diagnostic"][b"partition_id"]
            assert row[b"diagnostic"][b"worker_id"]
            assert row[b"diagnostic"][b"flow_core_target"] == flow_core_target.encode()
            assert row[b"diagnostic"][b"error"][b"message"] == b"Invalid input value"
            assert str(row[b"stream_spec_id"]) in stream_specs
            schema = stream_specs[str(row[b"stream_spec_id"])]["schema"]
            assert [(column["name"], column["type"]) for column in schema] == [
                (column["name"], column["type"]) for column in SCHEMA
            ]

        output = list(self.client.select_rows(f"event_id, key, value, event_timestamp from [{self.output_queue}]"))
        expected = [row for row in input_rows(0, event_count) if row["value"] >= 0]
        for row in expected:
            del row["$tablet_index"]
        assert sorted(output, key=lambda row: row["event_id"]) == expected

    @pytest.mark.authors(["timoninmaxim"])
    @pytest.mark.parametrize("problems", [pytest.param(False, id="stable"), pytest.param(True, id="unstable")])
    def test_capture(self, problems):
        run_yt_sync(
            "primary",
            self.work_yt_path,
            add_input_queue_and_consumer=True,
            input_queue_schema=SCHEMA
            + [
                {"name": "$timestamp", "type": "uint64"},
                {"name": "$cumulative_data_weight", "type": "int64"},
            ],
            input_queue_tablet_count=TABLET_COUNT,
            add_output_queue=True,
            output_queue_schema=SCHEMA,
        )
        self.write_input(0, EVENT_COUNT // 2)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": self.prepare_config()},
            workers_count=2,
            problems=problems,
            controller_problems_config=(
                ProblemsConfig(interval_seconds=10, problems_max_count=2, soft_restarts=True, start_delay=5)
                if problems
                else None
            ),
            worker_problems_config=(
                ProblemsConfig(interval_seconds=5, problems_max_count=4, soft_restarts=True, start_delay=5)
                if problems
                else None
            ),
        ) as federation:
            wait(lambda: len(self.read_failures()) == EVENT_COUNT // 6, timeout=120)
            if problems:
                wait(lambda: sum(worker.injected_problems_count for worker in federation.workers) > 0, timeout=120)
            self.write_input(EVENT_COUNT // 2, EVENT_COUNT)
            wait(lambda: len(self.read_failures()) == EVENT_COUNT // 3, timeout=180)
            wait(
                lambda: len(list(self.client.select_rows(f"* from [{self.output_queue}]"))) == EVENT_COUNT * 2 // 3,
                timeout=180,
            )
            stream_specs = None
            flow_core_target = None

            def read_specs_and_stop_pipeline():
                nonlocal stream_specs, flow_core_target
                stream_specs = self.client.get_flow_view(
                    self.pipeline_path,
                    view_path="/state/execution_spec/stream_spec_storage_state/value/stream_specs/input",
                    cache=False,
                )
                flow_core_target = self.client.get_flow_view(
                    self.pipeline_path,
                    view_path="/state/execution_spec/flow_core_target/value",
                    cache=False,
                )
                self.client.stop_pipeline(self.pipeline_path)
                return True

            wait(read_specs_and_stop_pipeline, timeout=120, ignore_exceptions=True)
            self.wait_pipeline_state("stopped", timeout=120)
            self.check_results(EVENT_COUNT, stream_specs, flow_core_target)
