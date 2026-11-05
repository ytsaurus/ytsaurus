"""E2E test for the Java noop companion."""

import pytest
import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_java_base import FlowTestJavaBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/../pipeline.yson")

# Messages per partition the finite random source emits before the pipeline completes.
PARTITION_MESSAGE_COUNT = 200

#################################################################


class Test(FlowTestJavaBase):
    JAVA_RUNNER_BINARY_DIR = yatest.common.binary_path(f"{yatest.common.context.project_path}/../noop/")
    JAVA_MAIN_CLASS = "tech.ytsaurus.flow.examples.noop.PipelineMain"

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)
        # A finite source lets the test wait for the completed state.
        pipeline_config["spec"]["computations"]["reader"]["source_streams"]["random"]["parameters"]["finite"] = True
        pipeline_config["dynamic_spec"] = {
            "computations": {
                "reader": {
                    "source_streams": {
                        "random": {"parameters": {"partition_message_count": PARTITION_MESSAGE_COUNT}},
                    },
                },
            },
        }
        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["timoninmaxim"])
    @pytest.mark.parametrize("use_vanilla_jobs", [False, True], ids=["local", "vanilla"])
    def test_basic(self, use_vanilla_jobs):
        run_yt_sync("primary", self.work_yt_path)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": self.prepare_pipeline_config()},
            use_vanilla_jobs=use_vanilla_jobs,
        ):
            # Vanilla startup is slow: binary upload and a cold flow_server boot.
            self.wait_pipeline_state("completed", timeout=600)
