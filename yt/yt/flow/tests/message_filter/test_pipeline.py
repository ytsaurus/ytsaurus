import copy
import json

from urllib.request import Request, urlopen

import pytest

import yatest.common
import yt.wrapper

from yt.common import wait

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

    def _prepare_pipeline_config(self, input_queue, input_consumer, output_queue, processor_class=None, skip_all=False):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        cluster = self.primary_cluster_name
        nested_setdefault(
            pipeline_config, "spec", "computations", "reader", "source_streams", "queue", "parameters"
        ).update(
            {
                "queue_path": f"<cluster={cluster}>{input_queue}",
                "consumer_path": f"<cluster={cluster}>{input_consumer}",
                "finite": processor_class is None,
            }
        )
        nested_setdefault(pipeline_config, "spec", "computations", "writer", "sinks", "queue", "parameters")[
            "queue_path"
        ] = output_queue

        if processor_class is None:
            pipeline_config["dynamic_spec"]["computations"]["reader"]["skip_if_expression"] = 'key = "bad"'
        else:
            computations = pipeline_config["spec"]["computations"]
            writer_spec = computations["writer"]
            sink_spec = copy.deepcopy(writer_spec)
            sink_spec["input_stream_ids"] = ["event_out"]
            sink_spec["output_stream_ids"] = ["event_sink"]
            sink_spec["sinks"]["queue"]["input_stream_ids"] = ["event_sink"]
            computations["sink"] = sink_spec
            pipeline_config["spec"]["streams"]["event_sink"] = copy.deepcopy(
                pipeline_config["spec"]["streams"]["event_out"]
            )
            del writer_spec["sinks"]
            writer_spec["computation_class_name"] = processor_class
            writer = pipeline_config["dynamic_spec"]["computations"]["writer"]
            writer["parameters"]["desired_partition_count"] = 1
            writer["skip_if_expression"] = "true" if skip_all else 'key = "bad"'

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
            node_config={"enable_porto_resource_tracker": False},
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

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize(
        "processor_class", ["NYT::NFlow::TPassthroughComputation", "NYT::NFlow::TSwiftPassthroughComputation"]
    )
    @pytest.mark.parametrize("skip_all", [False, True])
    def test_preprocess_preserves_input_statistics(self, processor_class, skip_all):
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
        config_path = self._prepare_pipeline_config(
            input_queue, input_consumer, output_queue, processor_class, skip_all
        )
        expected = [] if skip_all else ["0", "2", "4"]

        def output():
            return sorted(row["data"] for row in self.client.select_rows(f"data from [{output_queue}]"))

        with self.start_flow_process_federation(
            node_config={"enable_porto_resource_tracker": False},
            pipeline_binary_args={"--config": config_path},
            workers_count=1,
            controllers_count=1,
        ) as federation:

            def has_unfiltered_statistics():
                view = self.client.get_flow_view(self.pipeline_path, cache=False)
                for partition_id, partition in view["state"]["execution_spec"]["layout"]["partitions"].items():
                    if partition["computation_id"] != "writer":
                        continue
                    status = (
                        view["feedback"]["partition_job_statuses"].get(partition_id, {}).get("current_job_status", {})
                    )
                    metrics = status.get("input_metrics", {})
                    for stream_metrics in (metrics.get("global", {}), metrics.get("streams", {}).get("event_in", {})):
                        if not any(
                            key[-1] == "bad" and fraction > 0.2
                            for fraction, key in stream_metrics.get("heavy_hitters", [])
                        ):
                            return False
                    inflight = (
                        status.get("from_partition_traverse_data", {})
                        .get("node", {})
                        .get("streams", {})
                        .get("event_in", {})
                        .get("inflight_metrics", {})
                    )
                    return inflight.get("count") == 0
                return False

            # Dropped keys must remain visible to partitioning, including an entirely skipped batch.
            wait(has_unfiltered_statistics, timeout=120)
            wait(lambda: output() == expected, timeout=120)

            values = {}

            def checks_only_retained_input():
                nonlocal values
                # Read cumulative counters without Solomon's counter-to-rate conversion.
                request = Request(
                    f"http://localhost:{federation.workers[0].monitoring_port}/solomon_proxy/sensors",
                    headers={"X-YT-IsSolomonPull": "0"},
                )
                with urlopen(request, timeout=5) as response:
                    sensors = json.load(response)["sensors"]
                prefix = "yt.flow.worker.computation."
                names = {prefix + "input_streams.skipped_by_expression_count", prefix + "input_store.checked"}
                values = {
                    sensor["labels"]["sensor"]: sensor["value"]
                    for sensor in sensors
                    if sensor["labels"].get("computation_id") == "writer" and sensor["labels"]["sensor"] in names
                }
                skipped = values.get(prefix + "input_streams.skipped_by_expression_count")
                if skipped != len(INPUT_ROWS) - len(expected):
                    return False
                return values.get(prefix + "input_store.checked") == len(expected)

            wait(checks_only_retained_input, timeout=60, error_message=lambda: f"Unexpected input counters: {values}")
