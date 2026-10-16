import pytest

import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync
from yt.yt.flow.library.python.queue import batching_write_rows

from yt.common import wait

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")
FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

INPUT_TABLET_COUNT = 5
GROUP_KEY_COUNT = 10

INPUT_QUEUE_SCHEMA = [
    {"name": "event_id", "type": "int64"},
    {"name": "group_key", "type": "uint64"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

OUTPUT_QUEUE_SCHEMA = [
    {"name": "event_id", "type": "int64"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

if yatest.common.context.sanitize is not None:
    EVENT_COUNT = 200
else:
    EVENT_COUNT = 2000


def generate_data(event_count, tablet_count):
    return [
        {
            "event_id": event_id,
            "group_key": event_id % GROUP_KEY_COUNT,
            "$tablet_index": event_id % tablet_count,
        }
        for event_id in range(event_count)
    ]


INPUT_DATA = generate_data(EVENT_COUNT, INPUT_TABLET_COUNT)
EXPECTED_EVENT_IDS = set(range(EVENT_COUNT))

##################################################################


class TestSwiftMapBatching(FlowTestBase):
    FLOW_BINARY_PATH = FLOW_BINARY_PATH
    DRIVER_BACKEND = "rpc"

    def setup_method(self, method):
        super(TestSwiftMapBatching, self).setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.consumer = self.work_yt_path + "/consumer"
        self.output_queue = self.work_yt_path + "/output_queue"

    def prepare_environment(self):
        run_yt_sync(
            "primary",
            self.work_yt_path,
            add_input_queue_and_consumer=True,
            input_queue_schema=INPUT_QUEUE_SCHEMA,
            input_queue_tablet_count=INPUT_TABLET_COUNT,
            add_output_queue=True,
            output_queue_schema=OUTPUT_QUEUE_SCHEMA,
        )

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["Reader"]["source_streams"]["queue"]["parameters"].update({
            "queue_path": f"<cluster=primary>{self.input_queue}",
            "consumer_path": f"<cluster=primary>{self.consumer}",
        })

        pipeline_config["spec"]["computations"]["Writer"]["sinks"]["queue"]["parameters"].update({
            "queue_path": f"<cluster=primary>{self.output_queue}",
        })

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["pechatnov"])
    def test_all_events_delivered(self):
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config()

        batching_write_rows(
            INPUT_DATA, lambda batch: self.client.insert_rows(self.input_queue, batch), 100,
        )

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            expr = f"event_id from [{self.output_queue}]"
            wait(
                lambda: EXPECTED_EVENT_IDS.issubset({row["event_id"] for row in self.client.select_rows(expr)}),
                timeout=180,
            )

            event_ids = {row["event_id"] for row in self.client.select_rows(expr)}
            assert event_ids == EXPECTED_EVENT_IDS
