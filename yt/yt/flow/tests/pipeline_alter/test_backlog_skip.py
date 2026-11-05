import pytest

import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

from yt.common import wait

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")
FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

TABLET_COUNT = 5
# Both the greedy balancer and a large backlog are required: the old partitions' restarted jobs
# must still be mid-read when retirement lands, or the rows are delivered and the race is missed.
if yatest.common.context.sanitize is not None:
    ROWS_PER_TABLET = 200
else:
    ROWS_PER_TABLET = 1000


def generate_data(prefix):
    return [{"data": f"{prefix}_{i}", "$tablet_index": i % TABLET_COUNT} for i in range(TABLET_COUNT * ROWS_PER_TABLET)]


##################################################################


class TestBacklogSkip(FlowTestBase):
    FLOW_BINARY_PATH = FLOW_BINARY_PATH
    DRIVER_BACKEND = "rpc"

    def setup_method(self, method):
        super(TestBacklogSkip, self).setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.input_queue_alt = self.work_yt_path + "/input_queue_alt"
        self.consumer = self.work_yt_path + "/consumer"
        self.consumer_alt = self.work_yt_path + "/consumer_alt"
        self.output_queue = self.work_yt_path + "/output_queue"
        self.producer = self.work_yt_path + "/producer"

    @pytest.mark.authors(["timoninmaxim"])
    def test_backlog_skip(self):
        run_yt_sync("primary", self.work_yt_path, TABLET_COUNT)
        batching_write_rows(
            generate_data("payload"), lambda batch: self.client.insert_rows(self.input_queue, batch), 100
        )
        batching_write_rows(
            generate_data("alt"), lambda batch: self.client.insert_rows(self.input_queue_alt, batch), 100
        )

        config = get_yson_config(PIPELINE_CONFIG_PATH)
        source_params = config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"]
        source_params.update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.consumer}",
                "finite": False,
            }
        )
        config["spec"]["computations"]["reader"]["sinks"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.output_queue}",
                "producer_path": f"<cluster=primary>{self.producer}",
            }
        )
        # The greedy balancer restarts the old partitions' jobs immediately on start, which is what
        # lets them attach to the new queue before retirement.
        config["dynamic_spec"]["job_manager"]["use_cpu_aware_balancer"] = False
        self.patch_config(config)
        config_path = self.dump_config_to_log_dir(config, "pipeline.yson")

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": config_path},
            workers_count=1,
            controllers_count=1,
            problems=False,
        ):
            # The original queue is fully delivered, so every partition has committed and persisted
            # its end offset — the offsets a stale job would later push into the new consumer.
            expected_payload = {row["data"] for row in generate_data("payload")}
            wait(lambda: self._output_rows() == expected_payload, timeout=180)

            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=180)

            spec = self.client.get_pipeline_spec(self.pipeline_path)["spec"]
            reader_params = spec["computations"]["reader"]["source_streams"]["queue"]["parameters"]
            reader_params["queue_path"] = f"<cluster=primary>{self.input_queue_alt}"
            reader_params["consumer_path"] = f"<cluster=primary>{self.consumer_alt}"
            # The new source is finite: once it reports the alternative queue exhausted and the
            # final epoch commits, the pipeline completes, so at "completed" the output is final.
            reader_params["finite"] = True
            self.client.set_pipeline_spec(self.pipeline_path, spec)

            self.client.start_pipeline(self.pipeline_path)
            self.wait_pipeline_state(["working", "completed"], timeout=180)
            self.wait_pipeline_state("completed", timeout=180)

            missing = {row["data"] for row in generate_data("alt")} - self._output_rows()
            assert not missing, (
                f"{len(missing)}/{TABLET_COUNT * ROWS_PER_TABLET} alt-backlog rows skipped; "
                f"consumer_alt offsets {self._consumer_alt_offsets()} were advanced past data "
                "that was never delivered"
            )

    def _output_rows(self):
        return {row["data"] for row in self.client.select_rows(f"data from [{self.output_queue}]")}

    def _consumer_alt_offsets(self):
        return {
            row["partition_index"]: row["offset"]
            for row in self.client.select_rows(f"[partition_index], [offset] from [{self.consumer_alt}]")
        }
