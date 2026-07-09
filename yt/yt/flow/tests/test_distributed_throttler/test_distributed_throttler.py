import pytest
import yatest.common
import yt.wrapper

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

from yt.common import wait

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")

if yatest.common.context.sanitize:
    EVENT_COUNT = 50
else:
    EVENT_COUNT = 200

##################################################################


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def setup_method(self, method):
        super(Test, self).setup_method(method)
        self.input_queue = f"{self.work_yt_path}/input_queue"
        self.input_consumer = f"{self.work_yt_path}/consumer"
        self.output_queue = f"{self.work_yt_path}/output_queue"

    def write_input_data(self, count):
        rows = [{"value": i, "$tablet_index": 0} for i in range(count)]
        batching_write_rows(rows, lambda batch: self.client.insert_rows(self.input_queue, batch), 100)

    def prepare_pipeline_config(self, finite=True):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["Reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.input_consumer}",
                "finite": finite,
            }
        )
        pipeline_config["spec"]["computations"]["Throttled"]["sinks"]["queue"]["parameters"][
            "queue_path"
        ] = self.output_queue

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def count_output_rows(self):
        rows = self.client.select_rows(
            f"* from [{self.output_queue}]", format=yt.wrapper.format.YsonFormat(encoding=None)
        )
        return sum(1 for _ in rows)

    @pytest.mark.authors(["mikari"])
    def test_computation_uses_throttler(self):
        """Pipeline with a throttler-using computation completes and emits all messages."""
        run_yt_sync("primary", self.work_yt_path)

        self.write_input_data(EVENT_COUNT)
        pipeline_config_path = self.prepare_pipeline_config()

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
        ):
            self.wait_pipeline_state("completed", timeout=180)

        assert self.count_output_rows() == EVENT_COUNT

    @pytest.mark.authors(["mikari"])
    def test_throttler_survives_leader_switch(self):
        """A throttler-using computation keeps working after a controller leader switch.

        A new leader rebuilds the throttler host from scratch while the dynamic-spec
        version is unchanged, so the throttlers must be re-registered; otherwise the
        computation gets "Unknown throttler".
        """
        run_yt_sync("primary", self.work_yt_path)

        # Streaming source: the pipeline stays "working" so we can hand it over to a
        # fresh leader, and all input is processed only after the switch.
        pipeline_config_path = self.prepare_pipeline_config(finite=False)

        # First leader: reaching "working" persists the dynamic-spec version as equal
        # to the Cypress one, which is the condition that exposed the bug.
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
        ):
            self.wait_pipeline_state("working")

        self.write_input_data(EVENT_COUNT)

        # Second leader recovers the persisted "working" state without a dynamic-spec
        # change and must still serve throttler quota for the whole input.
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
            run_pipeline=False,
        ):
            wait(lambda: self.count_output_rows() == EVENT_COUNT, timeout=180)
