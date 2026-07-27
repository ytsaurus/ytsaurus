import pytest
import yatest.common

from yt.common import wait

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")

WORKERS_COUNT = 2

##################################################################


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)
        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def get_published_value(self):
        """The `value` field of the spec payload the controller published for Counter."""
        return self.client.get_flow_view(
            self.pipeline_path,
            view_path="/state/execution_spec/resource_target_revisions/value/Counter/spec/value",
            cache=False,
        )

    def all_instances_applied(self, value):
        """Every worker and the controller-side instance decoded a spec-payload value at least
        `value`. The reported number is read out of the delivered payload, so this asserts the
        payload itself -- not just its revision id -- reached the instances. Monotone in time,
        so frequently published revisions cannot make it flap."""
        view = self.client.get_flow_view(
            self.pipeline_path,
            view_path="/ephemeral_state/resource_controller_views/Counter",
            cache=False,
        )
        applied = sum(
            count
            for decoded, count in view["workers_per_value"].items()
            if int(decoded) >= value
        )
        return applied >= WORKERS_COUNT and view.get("controller_value", 0) >= value

    @pytest.mark.authors(["mikari"])
    def test_target_revision_reaches_workers(self):
        """The spec payload published by the resource controller reaches every worker, and the
        values the workers decode from it keep catching up with the published one."""
        run_yt_sync("primary", self.work_yt_path)
        pipeline_config_path = self.prepare_pipeline_config()

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=WORKERS_COUNT,
            controllers_count=1,
        ):
            self.wait_pipeline_state("working")

            wait(lambda: self.get_published_value() >= 1, timeout=60, ignore_exceptions=True)
            first = self.get_published_value()
            wait(lambda: self.all_instances_applied(first), timeout=60, ignore_exceptions=True)

            # The counter keeps publishing new payloads and the workers keep catching up.
            wait(lambda: self.get_published_value() > first, timeout=60)
            second = self.get_published_value()
            wait(lambda: self.all_instances_applied(second), timeout=60, ignore_exceptions=True)
