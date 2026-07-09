"""E2e test: a source emitting AddMessage(distribute=False) keeps that message out of the output yet still advances the watermark."""

import pytest

import yatest.common
import yt.wrapper

from yt.yt.flow.library.python.integration_test_base.yt_flow_python_base import (
    FlowTestPythonBase,
)
from yt.yt.flow.library.python.integration_test_base.helpers import (
    get_yson_config,
    nested_setdefault,
)
from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync
from yt.common import wait

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline.yson")

INPUT_QUEUE_SCHEMA = [
    {"name": "data", "type": "string"},
    {"name": "event_ts", "type": "uint64"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

# Distributed rows sit at a tiny event_ts; the lone distribute=False message of the
# "nodist_" row carries a much larger (sub-now) one, which the output watermark can only
# reach if non-distributed messages feed the generator.
NORMAL_EVENT_TS = 1000
NODIST_EVENT_TS = 1_700_000_000

OUTPUT_QUEUE_SCHEMA = [
    {"name": "data", "type": "string"},
    {"name": "event_ts", "type": "uint64"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

INPUT_ROWS = [
    {"data": "row_0", "event_ts": NORMAL_EVENT_TS, "$tablet_index": 0},
    {"data": "row_1", "event_ts": NORMAL_EVENT_TS, "$tablet_index": 0},
    {"data": "row_2", "event_ts": NORMAL_EVENT_TS, "$tablet_index": 0},
    # Produces only a distribute=False message carrying the high event_ts; the output
    # watermark must reach it even though the message is never published.
    {"data": "nodist_0", "event_ts": NODIST_EVENT_TS, "$tablet_index": 0},
]

##################################################################


class Test(FlowTestPythonBase):
    PYTHON_COMPANION_BINARY = yatest.common.binary_path(
        "yt/yt/flow/tests/add_message_distribute_flag/add_message_distribute_flag"
    )

    def _prepare_pipeline_config(self, input_queue, input_consumer, output_queue):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        cluster = self.primary_cluster_name
        nested_setdefault(
            pipeline_config, "spec", "computations", "reader", "source_streams", "queue", "parameters"
        ).update(
            {
                "queue_path": f"<cluster={cluster}>{input_queue}",
                "consumer_path": f"<cluster={cluster}>{input_consumer}",
                "finite": True,
            }
        )
        pipeline_config["spec"]["computations"]["reader"]["sinks"]["queue"]["parameters"]["queue_path"] = output_queue

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["pechatnov"])
    def test_distribute(self):
        run_yt_sync(
            self.primary_cluster_name,
            self.work_yt_path,
            tablet_cell_bundle=self.tablet_cell_bundle,
            primary_medium=self.primary_medium,
            add_input_queue_and_consumer=True,
            input_queue_schema=INPUT_QUEUE_SCHEMA,
            add_output_queue=True,
            output_queue_schema=OUTPUT_QUEUE_SCHEMA,
        )

        input_queue = f"{self.work_yt_path}/input_queue"
        input_consumer = f"{self.work_yt_path}/consumer"
        output_queue = f"{self.work_yt_path}/output_queue"

        self.client.insert_rows(input_queue, INPUT_ROWS)

        pipeline_config_path = self._prepare_pipeline_config(input_queue, input_consumer, output_queue)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
        ):
            # The watermark reaches NODIST_EVENT_TS only because the "nodist_" row's lone
            # distribute=False message contributes it; the distributed rows sit at NORMAL_EVENT_TS.
            wait(lambda: self.get_processing_watermark() >= NODIST_EVENT_TS, timeout=120)
            self.wait_pipeline_state("completed", timeout=240)

        rows = self.client.select_rows(
            f"* from [{output_queue}]",
            format=yt.wrapper.format.YsonFormat(encoding=None),
        )
        values = sorted(row[b"data"].decode() for row in rows)
        # Only the distributed originals are published; the "dup_" copies are dropped.
        assert values == ["row_0", "row_1", "row_2"]
