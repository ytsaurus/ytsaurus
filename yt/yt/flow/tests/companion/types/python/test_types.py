"""End-to-end type-checking test for the python companion: insert one row per
distinct wire value type, let the companion mirror every field, wait for the
finite pipeline to reach `completed`, then assert the output queue reproduces
every value byte-for-byte."""

import logging

import pytest
import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_python_base import FlowTestPythonBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync

from yt.yt.flow.tests.companion.types.common.type_samples import (
    QUEUE_SCHEMA,
    SAMPLE_ROWS,
    assert_roundtrip,
)

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")


class Test(FlowTestPythonBase):
    PYTHON_COMPANION_BINARY = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def setup_method(self, method):
        super().setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.input_consumer = self.work_yt_path + "/consumer"
        self.output_queue = self.work_yt_path + "/output_queue"

    def get_output(self):
        columns = "`key`, `f_string`, `f_int64`, `f_uint64`, `f_double`, `f_bool`"
        return list(self.client.select_rows(f"{columns} from [{self.output_queue}]"))

    def prepare_pipeline_config(self, finite=True):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        source_params = pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"]
        source_params.update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.input_consumer}",
                "finite": finite,
            }
        )

        sink_params = pipeline_config["spec"]["computations"]["mapper"]["sinks"]["queue"]["parameters"]
        sink_params.update({"queue_path": f"<cluster=primary>{self.output_queue}"})

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["sergeypozdeev"])
    def test_types(self):
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

        pipeline_config_path = self.prepare_pipeline_config(finite=True)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.client.insert_rows(self.input_queue, SAMPLE_ROWS)
            self.wait_pipeline_state("completed", timeout=240)

            assert_roundtrip(self.get_output())
            logging.info("python companion type-checking passed (rows=%d)", len(SAMPLE_ROWS))
