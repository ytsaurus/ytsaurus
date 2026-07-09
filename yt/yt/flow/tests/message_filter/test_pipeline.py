import pytest

import yatest.common
import yt.wrapper

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import (
    get_yson_config,
    nested_setdefault,
)
from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline.yson")

QUEUE_SCHEMA = [
    {"name": "key", "type": "string"},
    {"name": "data", "type": "string"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

# A few distinct keys; "bad" is the one the filter must drop.
INPUT_ROWS = [
    {"key": "good_0", "data": "0", "$tablet_index": 0},
    {"key": "bad", "data": "1", "$tablet_index": 0},
    {"key": "good_1", "data": "2", "$tablet_index": 0},
    {"key": "bad", "data": "3", "$tablet_index": 0},
    {"key": "good_2", "data": "4", "$tablet_index": 0},
]

##################################################################


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path("yt/yt/flow/bin/flow_server/flow_server")

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
        nested_setdefault(pipeline_config, "spec", "computations", "writer", "sinks", "queue", "parameters")[
            "queue_path"
        ] = output_queue

        # Smoke-test the filter end-to-end: skip the "bad" key at the source (reader).
        pipeline_config["dynamic_spec"]["computations"]["reader"]["skip_if_expression"] = 'key = "bad"'

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["pechatnov"])
    def test_filter_drops_blacklisted_key(self):
        run_yt_sync(
            self.primary_cluster_name,
            self.work_yt_path,
            tablet_cell_bundle=self.tablet_cell_bundle,
            primary_medium=self.primary_medium,
            add_input_queue_and_consumer=True,
            input_queue_schema=QUEUE_SCHEMA,
            add_output_queue=True,
            output_queue_schema=QUEUE_SCHEMA,
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
            self.wait_pipeline_state("completed", timeout=240)

        rows = self.client.select_rows(
            f"* from [{output_queue}]",
            format=yt.wrapper.format.YsonFormat(encoding=None),
        )
        keys = sorted(row[b"key"].decode() for row in rows)
        assert keys == ["good_0", "good_1", "good_2"]
