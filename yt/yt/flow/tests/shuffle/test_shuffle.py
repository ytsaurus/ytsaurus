import logging
import time

import pytest
import yatest.common

from yt.common import wait
from yt.wrapper.errors import YtResolveError

from yt.yt.flow.library.python.bullied_process import ProblemsConfig
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import (
    get_yson_config,
    nested_setdefault,
)
from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync
from yt.yt.flow.library.python.queue import batching_write_rows

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline.yson")

SHUFFLE_STAGES = ["shuffle_a", "shuffle_b", "shuffle_c", "shuffle_d"]

INPUT_QUEUE_TABLET_COUNT = 4

QUEUE_SCHEMA = [
    {"name": "key", "type": "string"},
    {"name": "data", "type": "string"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

if yatest.common.context.sanitize is not None:
    DEFAULT_TOTAL_EVENTS = 200
else:
    DEFAULT_TOTAL_EVENTS = 1500

# --test-param SHUFFLE_TOTAL_EVENTS=<n> overrides the input volume.
# Useful for perf evaluation against external clusters (e.g. zeno).
TOTAL_EVENTS = int(yatest.common.get_param("SHUFFLE_TOTAL_EVENTS", DEFAULT_TOTAL_EVENTS))

# Scale wait timeouts with input volume so larger perf-eval runs don't trip the
# in-test wait. Outer ya test timeout must also be disabled separately
# (--test-disable-timeout).
WAIT_TIMEOUT_MULTIPLIER = max(1.0, TOTAL_EVENTS / DEFAULT_TOTAL_EVENTS)


def generate_rows():
    return [
        {
            "key": f"key_{i % 1024}",
            "data": f"data_{i}",
            "$tablet_index": i % INPUT_QUEUE_TABLET_COUNT,
        }
        for i in range(TOTAL_EVENTS)
    ]


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path("yt/yt/flow/bin/flow_server/flow_server")

    def _prepare_pipeline_config(
        self,
        input_queue,
        input_consumer,
        output_queue,
        use_compact_partition_output,
        dynamic_spec_patch=None,
    ):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        cluster = self.primary_cluster_name
        nested_setdefault(
            pipeline_config,
            "spec",
            "computations",
            "reader",
            "source_streams",
            "queue",
            "parameters",
        ).update(
            {
                "queue_path": f"<cluster={cluster}>{input_queue}",
                "consumer_path": f"<cluster={cluster}>{input_consumer}",
                "finite": True,
            }
        )
        nested_setdefault(
            pipeline_config,
            "spec",
            "computations",
            "shuffle_d",
            "sinks",
            "queue",
            "parameters",
        )["queue_path"] = output_queue

        for stage in SHUFFLE_STAGES:
            pipeline_config["spec"]["computations"][stage][
                "use_compact_partition_output"
            ] = use_compact_partition_output

        if dynamic_spec_patch is not None:
            for key, patch in dynamic_spec_patch.items():
                nested_setdefault(pipeline_config, "dynamic_spec", key).update(patch)

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def _run_yt_sync_with_retries(self):
        # On real clusters yt_sync may race with master cross-cell replication;
        # retry on transient YtResolveError before giving up.
        last_error = None
        for attempt in range(5):
            try:
                run_yt_sync(
                    self.primary_cluster_name,
                    self.work_yt_path,
                    tablet_cell_bundle=self.tablet_cell_bundle,
                    primary_medium=self.primary_medium,
                    add_input_queue_and_consumer=True,
                    input_queue_schema=QUEUE_SCHEMA,
                    input_queue_tablet_count=INPUT_QUEUE_TABLET_COUNT,
                    add_output_queue=True,
                    output_queue_schema=QUEUE_SCHEMA,
                )
                return
            except YtResolveError as e:
                last_error = e
                logging.warning("run_yt_sync failed (attempt %d): %s", attempt + 1, e)
                time.sleep(5)
        raise last_error

    def _count_output_rows(self, output_queue):
        # Server-side aggregation: pulling all rows would blow past zeno's
        # default 1M input_row_limit at SHUFFLE_TOTAL_EVENTS >= 1M.
        rows = list(self.client.select_rows(f"sum(1) as cnt from [{output_queue}] group by 1"))
        return rows[0]["cnt"] if rows else 0

    @pytest.mark.authors(["pechatnov"])
    @pytest.mark.parametrize(
        "use_compact_partition_output",
        [
            pytest.param(True, id="compact_partition_output"),
            pytest.param(False, id="legacy_partition_output"),
        ],
    )
    def test_basic(self, use_compact_partition_output, metrics):
        self._run_yt_sync_with_retries()
        input_queue = f"{self.work_yt_path}/input_queue"
        input_consumer = f"{self.work_yt_path}/consumer"
        output_queue = f"{self.work_yt_path}/output_queue"

        batching_write_rows(generate_rows(), lambda batch: self.client.insert_rows(input_queue, batch), 1000)

        pipeline_config_path = self._prepare_pipeline_config(
            input_queue=input_queue,
            input_consumer=input_consumer,
            output_queue=output_queue,
            use_compact_partition_output=use_compact_partition_output,
        )

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=4,
            controllers_count=1,
            problems=True,
            controller_problems_config=ProblemsConfig(interval_seconds=60, problems_max_count=0),
        ):
            start_time = time.time()
            self.wait_pipeline_state("completed", timeout=300 * WAIT_TIMEOUT_MULTIPLIER)
            elapsed = time.time() - start_time

        messages_per_second = TOTAL_EVENTS / elapsed
        metrics.set("ytflow_messages_per_second", messages_per_second)
        logging.info(
            "Pipeline completed in %.2fs (%.0f msgs/s, mode=%s)",
            elapsed,
            messages_per_second,
            "compact" if use_compact_partition_output else "legacy",
        )

        wait(lambda: self._count_output_rows(output_queue) == TOTAL_EVENTS, timeout=60, ignore_exceptions=True)
        actual_rows = self._count_output_rows(output_queue)
        assert (
            actual_rows == TOTAL_EVENTS
        ), f"output_queue row count mismatch: got {actual_rows}, expected {TOTAL_EVENTS}"

    @pytest.mark.authors(["pechatnov"])
    def test_controller_loss_cancels_and_recreates_jobs(self):
        # YTFLOW-689: after controller_wait_timeout without heartbeats the workers must
        # cancel their jobs, not restart them in place with the same job ids.
        self._run_yt_sync_with_retries()
        input_queue = f"{self.work_yt_path}/input_queue"
        input_consumer = f"{self.work_yt_path}/consumer"
        output_queue = f"{self.work_yt_path}/output_queue"

        batching_write_rows(generate_rows(), lambda batch: self.client.insert_rows(input_queue, batch), 1000)

        pipeline_config_path = self._prepare_pipeline_config(
            input_queue=input_queue,
            input_consumer=input_consumer,
            output_queue=output_queue,
            use_compact_partition_output=True,
            dynamic_spec_patch={
                "controller_connector": {
                    "controller_wait_timeout": "3s",
                    "controller_heartbeat_rpc_timeout": "3s",
                    "controller_discover_period": "1s",
                },
            },
        )

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=4,
            controllers_count=1,
        ) as federation:
            # Restart the controller only after some output: the workers must lose it with
            # messages still in flight.
            wait(lambda: self._count_output_rows(output_queue) > 0, timeout=180, ignore_exceptions=True)
            controller = federation.controllers[0]
            controller.stop()
            time.sleep(5)
            controller.start()

            def controller_saw_dead_jobs():
                rows = self.client.select_rows(f"data from [{self.pipeline_path}/controller_logs]")
                # Either the workers' abandoned-job reports or the restarted controller's
                # lost-worker sweep wins the re-registration race; both mean the old jobs
                # are gone.
                return any(
                    "Job is abandoned" in row["data"] or "Job is lost because worker is lost" in row["data"]
                    for row in rows
                )

            wait(controller_saw_dead_jobs, timeout=180, ignore_exceptions=True)

            self.wait_pipeline_state("completed", timeout=300 * WAIT_TIMEOUT_MULTIPLIER)

        actual_rows = self._count_output_rows(output_queue)
        assert (
            actual_rows == TOTAL_EVENTS
        ), f"output_queue row count mismatch: got {actual_rows}, expected {TOTAL_EVENTS}"
