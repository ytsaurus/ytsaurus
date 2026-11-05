import logging
import pytest

import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")

##################################################################


class TestSecretEnv(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)
        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["mikari"])
    def test_secret_env_reaches_vanilla_job(self):
        run_yt_sync(self.primary_cluster_name, self.work_yt_path)
        pipeline_config_path = self.prepare_pipeline_config()

        # The random source is finite, so the pipeline completes only if YT_MY_SECRET reached the
        # job: TSecretChecker asserts it in DoProcessMessage and the job fails otherwise.
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            use_vanilla_jobs=True,
            additional_env={"YT_MY_SECRET": "5"},
            vanilla_secret_env=["YT_MY_SECRET"],
        ):
            self.wait_pipeline_state("completed", timeout=180)
            logging.info("pipeline completed: secret was visible in the vanilla job")
