from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

import yatest
import pytest
import os
import time
import logging

##################################################################

NODE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/../node_config.yson")

##################################################################


class TestBufferPlayground(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/../table_injector")

    def prepare_node_config(self):
        node_config = get_yson_config(NODE_CONFIG_PATH)

        node_config["logging"]["writers"]["BufferMetricsWriter"] = {
            "type": "file",
            "file_name": os.path.join(self.path_to_flow_logs, "buffer_metrics.log"),
            "accepted_message_format": "structured",
        }

        return node_config

    def prepare_pipeline_config(self, pipeline_config_path):
        pipeline_config = get_yson_config(pipeline_config_path)

        computation_specs = pipeline_config["spec"]["computations"]
        for computation_spec in computation_specs.values():
            if "table_name" not in computation_spec["parameters"]:
                continue

            table_name = f"//tmp/{computation_spec['parameters']['table_name']}"
            computation_spec["parameters"]["table_name"] = table_name

        self.patch_config(pipeline_config)

        run_dir_config_path = self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")
        return run_dir_config_path, pipeline_config

    def prepare_tables(self, pipeline_config):
        schema = [
            {"name": "data", "type": "string"},
            {"name": "key", "type": "string"},
            {"name": "$timestamp", "type": "uint64"},
            {"name": "$cumulative_data_weight", "type": "int64"},
        ]

        self.client.create("table", "//tmp/t_slow_input", attributes={"dynamic": True, "schema": schema})
        self.client.mount_table("//tmp/t_slow_input", sync=True)
        self.client.create("queue_consumer", "//tmp/t_input_consumer")
        self.client.mount_table("//tmp/t_input_consumer", sync=True)
        self.client.register_queue_consumer("//tmp/t_slow_input", "<cluster=primary>//tmp/t_input_consumer", vital=True)

        self.client.create("table", "//tmp/t_fast_input", attributes={"dynamic": True, "schema": schema})
        self.client.mount_table("//tmp/t_fast_input", sync=True)
        self.client.register_queue_consumer("//tmp/t_fast_input", "<cluster=primary>//tmp/t_input_consumer", vital=True)

        self.client.create("table", "//tmp/t_output", attributes={"dynamic": True, "schema": schema})
        self.client.mount_table("//tmp/t_output", sync=True)
        self.client.create("queue_producer", "//tmp/t_output_producer")
        self.client.mount_table("//tmp/t_output_producer", sync=True)

    @pytest.mark.authors(["gryzlov-ad"])
    @pytest.mark.skip(reason="Used to only collect buffer metrics")
    def test_simple(self):
        node_config = self.prepare_node_config()
        original_pipeline_config_path = yatest.common.source_path(
            f"{yatest.common.context.project_path}/../pipeline_two_injectors.yson"
        )
        pipeline_config_path, pipeline_config = self.prepare_pipeline_config(original_pipeline_config_path)
        self.prepare_tables(pipeline_config)

        slow_data = [{"data": f"slow_hello_{i}"} for i in range(10_000)]
        fast_data = [{"data": f"hello_{i}"} for i in range(100_000)]

        with self.start_flow_process_federation(
            node_config=node_config,
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.client.insert_rows("//tmp/t_slow_input", slow_data)
            self.client.insert_rows("//tmp/t_fast_input", fast_data)
            time.sleep(90)
            import logging

            logger = logging.getLogger()
            logger.debug(f'Written rows: {len(list(self.client.select_rows("* from [//tmp/t_output]")))}')

    @pytest.mark.authors(["gryzlov-ad"])
    @pytest.mark.skip(reason="Used to only collect buffer metrics")
    def test_unequal_load(self):
        node_config = self.prepare_node_config()
        original_pipeline_config_path = yatest.common.source_path(
            f"{yatest.common.context.project_path}/../pipeline_unequal_load.yson"
        )
        pipeline_config_path, pipeline_config = self.prepare_pipeline_config(original_pipeline_config_path)
        self.prepare_tables(pipeline_config)

        with self.start_flow_process_federation(
            node_config=node_config,
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            time.sleep(90)
            logger = logging.getLogger()
            logger.debug(f'Written rows: {len(list(self.client.select_rows("* from [//tmp/t_output]")))}')
