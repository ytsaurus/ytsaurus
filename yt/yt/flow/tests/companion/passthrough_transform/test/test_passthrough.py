"""E2e test: a transform declared as native passthrough forwards messages without a companion.

The "passthrough" transform uses the native C++ computation class
``NYT::NFlow::TPassthroughComputation`` in the pipeline spec and is therefore not registered
with the Python companion at all. The native computation must forward each input message to
its output stream without ever calling the companion -- if it called the companion the
pipeline would never complete because the "passthrough" computation is unknown there.
"""

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

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline.yson")

INPUT_QUEUE_SCHEMA = [
    {"name": "data", "type": "string"},
    {"name": "event_ts", "type": "uint64"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

OUTPUT_QUEUE_SCHEMA = [
    {"name": "data", "type": "string"},
    {"name": "event_ts", "type": "uint64"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

EVENT_TS = 1000

INPUT_ROWS = [{"data": f"row_{i}", "event_ts": EVENT_TS, "$tablet_index": 0} for i in range(8)]

##################################################################


class Test(FlowTestPythonBase):
    PYTHON_COMPANION_BINARY = yatest.common.binary_path(
        "yt/yt/flow/tests/companion/passthrough_transform/passthrough_transform"
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
        pipeline_config["spec"]["computations"]["passthrough"]["sinks"]["queue"]["parameters"][
            "queue_path"
        ] = output_queue

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["pechatnov"])
    def test_passthrough(self):
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
            # Completion is only possible if the passthrough transform never calls its
            # companion: the Python companion has no process function and would raise.
            self.wait_pipeline_state("completed", timeout=240)

        rows = self.client.select_rows(
            f"* from [{output_queue}]",
            format=yt.wrapper.format.YsonFormat(encoding=None),
        )
        values = sorted(row[b"data"].decode() for row in rows)
        # Every input row is forwarded unchanged through the no-process-function transform.
        assert values == sorted(row["data"] for row in INPUT_ROWS)
