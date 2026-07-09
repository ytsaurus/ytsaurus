import logging
import os
import time

import pytest
import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config, nested_setdefault

from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync


##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")
NODE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/node_config.yson")

KEY_COUNT = 100

if event_count_str := yatest.common.get_param("EVENT_COUNT"):
    EVENT_COUNT = int(float(event_count_str))
else:
    EVENT_COUNT = int(200e3)
    if yatest.common.context.sanitize is not None:
        EVENT_COUNT = EVENT_COUNT // 10

# Single-worker layout with everything on one worker, so CpuUsPerMessage is the clean
# per-worker CPU (matches pure_swift_high_throughput's test_completing).
PARTITION_COUNT = 10

##################################################################


def _worker_cpu_seconds(federation):
    """Total CPU time (utime+stime, seconds) consumed by all worker processes so far."""
    clk = os.sysconf("SC_CLK_TCK")
    total = 0.0
    for worker in federation.workers:
        pid = worker._proc.process.pid
        data = open(f"/proc/{pid}/stat").read()
        # Skip past "comm" (may contain spaces/parens): fields after the last ')'.
        fields = data[data.rfind(")") + 2:].split()
        total += (int(fields[11]) + int(fields[12])) / clk
    return total


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def prepare_pipeline_config(self, variant):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        source_parameters_path = ["computations", "Reader", "source_streams", "random_source", "parameters"]
        nested_setdefault(pipeline_config, "dynamic_spec", *source_parameters_path).update({
            "message_key_range": KEY_COUNT,
            "message_size_mean": 100,
            # Pin the per-batch generation count: its default has changed over time and a low
            # value silently throttles the source, hiding the engine's real throughput / CPU.
            "message_count_mean": 1000000,
            "partition_count": PARTITION_COUNT,
            "partition_message_count": EVENT_COUNT // PARTITION_COUNT,
        })

        nested_setdefault(pipeline_config, "dynamic_spec", "computations", "Reducer", "parameters").update({
            "desired_partition_count": PARTITION_COUNT,
        })

        # The "computation" variant uses the hand-written TReducer (the spec default); the
        # "function" variant hosts the equivalent process function under the swift-map adapter.
        if variant == "function":
            reducer = nested_setdefault(pipeline_config, "spec", "computations", "Reducer")
            reducer["computation_class_name"] = "NYT::NFlow::TProcessFunctionSwiftMapComputation"
            reducer["processing_function"] = "NExample::TReducerFunction"

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, f"pipeline_{variant}.yson")

    def run_variant(self, metrics, variant):
        run_yt_sync("primary", self.work_yt_path)
        pipeline_config_path = self.prepare_pipeline_config(variant)
        with self.start_flow_process_federation(
            node_config=get_yson_config(NODE_CONFIG_PATH),
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
        ) as federation:
            start_time = time.time()
            cpu_start = _worker_cpu_seconds(federation)
            self.wait_pipeline_state("completed", timeout=300)
            elapsed = time.time() - start_time
            cpu_used = _worker_cpu_seconds(federation) - cpu_start
            messages_per_second = EVENT_COUNT / elapsed
            cpu_us_per_message = cpu_used / EVENT_COUNT * 1e6
            metrics.set("ytflow_messages_per_second", messages_per_second)
            metrics.set("ytflow_cpu_us_per_message", cpu_us_per_message)
            logging.info(
                "Variant %s finished (MessagesPerSecond: %f, CpuUsPerMessage: %f)",
                variant, messages_per_second, cpu_us_per_message,
            )

    @pytest.mark.authors(["sergeypozdeev"])
    def test_computation(self, metrics):
        self.run_variant(metrics, "computation")

    @pytest.mark.authors(["sergeypozdeev"])
    def test_function(self, metrics):
        self.run_variant(metrics, "function")
