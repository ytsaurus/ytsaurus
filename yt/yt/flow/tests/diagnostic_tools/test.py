import html
import os

import pytest
import yatest.common

from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync
from yt.yt.flow.library.python.queue import batching_write_rows

from yt.common import wait

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")
FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")
JOB_INVESTIGATION_BINARY_PATH = yatest.common.binary_path("yt/yt/flow/tools/job_investigation/job_investigation")
DRAW_PIPELINE_GRAPH_BINARY_PATH = yatest.common.binary_path("yt/yt/flow/tools/draw_pipeline_graph/draw_pipeline_graph")

QUEUE_SCHEMA = [
    {"name": "key", "type": "string"},
    {"name": "value", "type": "string"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]
ROWS = [{"key": f"key-{index % 17}", "value": "payload", "$tablet_index": 0} for index in range(200)]
# The sources read one row per second until the throttlers are released, so that their jobs stay alive.
THROTTLED_COMPUTATIONS = ("reader", "channel_reader")

##################################################################


class Test(FlowTestBase):
    """reader -> writer, whose sink writes channel_queue, which the source of channel_reader reads."""

    FLOW_BINARY_PATH = FLOW_BINARY_PATH

    def setup_method(self, method):
        super().setup_method(method)
        self.input_queue = f"{self.work_yt_path}/input_queue"
        self.channel_queue = f"{self.work_yt_path}/channel_queue"

    def _prepare_environment(self):
        run_yt_sync(
            "primary",
            self.work_yt_path,
            add_input_queue_and_consumer=True,
            input_queue_schema=QUEUE_SCHEMA,
            output_queues=[{"name": "channel_queue", "schema": QUEUE_SCHEMA}],
            producer_names=["channel_producer", "channel_producer_2"],
            extra_consumers={"channel_consumer": ["channel_queue"]},
        )
        for queue in (self.input_queue, self.channel_queue):
            batching_write_rows(ROWS, lambda batch, queue=queue: self.client.insert_rows(queue, batch), 10000)

    def _prepare_pipeline_config(self):
        config = get_yson_config(PIPELINE_CONFIG_PATH)
        computations = config["spec"]["computations"]
        computations["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.work_yt_path}/consumer",
                "finite": True,
            }
        )
        # Two sinks with names that a lossy node-id encoding would merge write the same queue.
        for sink_id, producer in (("channel_sink", "channel_producer"), ("channel-sink", "channel_producer_2")):
            computations["writer"]["sinks"][sink_id]["parameters"].update(
                {
                    "queue_path": f"<cluster=primary>{self.channel_queue}",
                    "producer_path": f"<cluster=primary>{self.work_yt_path}/{producer}",
                }
            )
        computations["channel_reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.channel_queue}",
                "consumer_path": f"<cluster=primary>{self.work_yt_path}/channel_consumer",
                "finite": True,
            }
        )

        dynamic_spec = config["dynamic_spec"]
        dynamic_spec["throttlers"] = {}
        for computation_id in THROTTLED_COMPUTATIONS:
            throttler_id = f"test_input_{computation_id}"
            dynamic_spec["computations"][computation_id].update(
                {"input_rows_throttler_id": throttler_id, "max_rows_per_batch": 1}
            )
            dynamic_spec["throttlers"][throttler_id] = {
                "limit": 1.0,
                "period": 1000,
                "request_period": 100,
                "max_grant_amount": 1,
            }

        self.patch_config(config)
        return self.dump_config_to_log_dir(config, "pipeline.yson")

    def _release_input_throttlers(self):
        dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
        for computation_id in THROTTLED_COMPUTATIONS:
            computation = dynamic_spec["spec"]["computations"][computation_id]
            computation.pop("input_rows_throttler_id")
            computation.pop("max_rows_per_batch")
        self.client.set_pipeline_dynamic_spec(
            self.pipeline_path,
            dynamic_spec["spec"],
            expected_version=dynamic_spec["version"],
        )

    def _get_current_job_id(self, computation_id):
        view = self.client.get_flow_view(self.pipeline_path, cache=False)
        partitions = view["state"]["execution_spec"]["layout"]["partitions"]
        statuses = view["feedback"]["partition_job_statuses"]
        for partition_id, partition in partitions.items():
            if partition["computation_id"] != computation_id:
                continue
            job_status = statuses.get(partition_id, {}).get("current_job_status", {})
            if job_status and not job_status.get("is_finished"):
                return job_status["job_id"]
        return None

    def _all_partitions_have_job_status(self):
        view = self.client.get_flow_view(self.pipeline_path, cache=False)
        partitions = view["state"]["execution_spec"]["layout"]["partitions"]
        statuses = view["feedback"].get("partition_job_statuses", {})
        return all(statuses.get(partition_id, {}).get("current_job_status") for partition_id in partitions)

    @pytest.mark.authors(["pechatnov"])
    def test_tools(self):
        self._prepare_environment()
        pipeline_config_path = self._prepare_pipeline_config()
        channel_label = f"yt:primary:{self.channel_queue}"
        with self.start_flow_process_federation(pipeline_binary_args={"--config": pipeline_config_path}):
            # Keep the finite source alive until both tools have observed complete job statuses.
            wait(lambda: self._get_current_job_id("reader") is not None)
            wait(lambda: self._get_current_job_id("channel_reader") is not None)
            # --check-is-full of the tools needs a job status of every partition.
            wait(self._all_partitions_have_job_status, timeout=120)
            assert (
                self.client.get_pipeline_state(self.pipeline_path) != "completed"
            ), "Can't complete so fast with current throttling options"
            throttled_job_id = self._get_current_job_id("reader")

            pipeline_full_path = f"primary:{self.pipeline_path}"

            job_investigation_output_path = os.path.join(self.path_to_flow_logs, "job_investigation.out")
            yatest.common.execute(
                [JOB_INVESTIGATION_BINARY_PATH, "--input", pipeline_full_path, "--check-is-full"],
                wait=True,
                stdout=job_investigation_output_path,
                stderr=os.path.join(self.path_to_flow_logs, "job_investigation.err"),
            )

            with open(job_investigation_output_path, "r") as f:
                assert throttled_job_id in f.read()

            draw_pipeline_graph_dot_output_path = os.path.join(self.path_to_flow_logs, "pipeline_graph.dot")
            yatest.common.execute(
                [
                    DRAW_PIPELINE_GRAPH_BINARY_PATH,
                    "--input",
                    pipeline_full_path,
                    "--check-is-full",
                    "--dot-output",
                    draw_pipeline_graph_dot_output_path,
                    "--output",
                    os.path.join(self.path_to_flow_logs, "pipeline_graph.svg"),
                    "--use-embedded-dot",
                ],
                wait=True,
                stdout=os.path.join(self.path_to_flow_logs, "draw_pipeline_graph.out"),
                stderr=os.path.join(self.path_to_flow_logs, "draw_pipeline_graph.err"),
            )
            with open(draw_pipeline_graph_dot_output_path, "r") as f:
                text = f.read()
                assert "writer" in text
                assert self.input_queue in text
                # The data-channel edge of each sink, in both clusters of the graph.
                assert text.count(f'label="{channel_label}"') == 4, text

            for mode in ("--mermaid", "--mermaid-cpp"):
                html_path = os.path.join(self.path_to_flow_logs, f"pipeline_graph{mode}.html")
                yatest.common.execute(
                    [DRAW_PIPELINE_GRAPH_BINARY_PATH, "--input", pipeline_full_path, mode, "--output", html_path],
                    wait=True,
                    stdout=os.path.join(self.path_to_flow_logs, f"draw_pipeline_graph{mode}.out"),
                    stderr=os.path.join(self.path_to_flow_logs, f"draw_pipeline_graph{mode}.err"),
                )
                with open(html_path, "r") as f:
                    text = f.read()
                    # Both diagrams of the page and their copies for the clipboard, HTML-escaped.
                    assert text.count(html.escape(f"-.->|{channel_label}|")) == 8, text
                    # The edges go from the sink nodes to the source node; the sinks stay distinct nodes.
                    for sink_node in ("e_snk_2dwriter_2fchannel__sink", "e_snk_2dwriter_2fchannel_2dsink"):
                        sink_to_source = f"{sink_node} -.->|{channel_label}| e_src_2dchannel__reader_2fqueue"
                        assert text.count(html.escape(sink_to_source)) == 4, text
                    assert "(source)" not in text, text

            # Remove the test gate and require ordinary finite-pipeline completion.
            self._release_input_throttlers()
            self.wait_pipeline_state("completed", timeout=180)
