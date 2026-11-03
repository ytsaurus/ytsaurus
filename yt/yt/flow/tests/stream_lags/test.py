import json
import threading

from urllib.request import Request, urlopen

import pytest
import yatest.common
import yt.wrapper as yt_wrapper

from yt.common import wait
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")

# Global stream ids as published in the controller's `stream_id` sensor tag: source and timer streams are
# computation-scoped, internal streams keep their names.
SOURCE_STREAM = "reader/queue"
EVENTS_STREAM = "events"
NEAR_TIMER_STREAM = "scheduler/near_timer"
FAR_TIMER_STREAM = "scheduler/far_timer"
PROCESSED_STREAMS = (SOURCE_STREAM, EVENTS_STREAM, NEAR_TIMER_STREAM)

LAG_SENSORS = ("time_lag", "count_lag", "ready_time_lag", "ready_count_lag")
SENSOR_PREFIX = "yt.flow.controller.streams."

WRITE_ROWS_PER_SECOND = 50
WRITE_PERIOD = 0.1

# Lags are whole seconds measured against the host clock; thresholds are deliberately coarse.
NO_LAG_SECONDS = 10
LAG_SECONDS = 15
WAIT_TIMEOUT = 120

##################################################################


class QueueWriter:
    """Writes rows with distinct ids into the input queue in the background at a steady rate."""

    def __init__(self, client, queue_path):
        self._client = client
        self._queue_path = queue_path
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._next_id = 0

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *args):
        self._stop.set()
        self._thread.join()

    def _run(self):
        rows_per_period = max(1, int(WRITE_ROWS_PER_SECOND * WRITE_PERIOD))
        while not self._stop.wait(WRITE_PERIOD):
            rows = [{"id": self._next_id + index, "$tablet_index": 0} for index in range(rows_per_period)]
            try:
                self._client.insert_rows(self._queue_path, rows)
            except Exception:
                # A transient write failure only slows the input down; lags are checked by polling.
                continue
            self._next_id += rows_per_period


class TestStreamLags(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def setup_method(self, method):
        super().setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.consumer = self.work_yt_path + "/consumer"
        self.output_queue = self.work_yt_path + "/output_queue"
        self.last_lags = {}

    def prepare_pipeline_config(self):
        config = get_yson_config(PIPELINE_CONFIG_PATH)
        config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.consumer}",
            }
        )
        config["spec"]["computations"]["scheduler"]["sinks"]["queue"]["parameters"]["queue_path"] = self.output_queue
        self.patch_config(config)
        return self.dump_config_to_log_dir(config, "pipeline.yson")

    def read_lags(self, federation):
        request = Request(
            f"http://localhost:{federation.controllers[0].monitoring_port}/solomon_proxy/sensors",
            headers={"X-YT-IsSolomonPull": "0"},
        )
        with urlopen(request, timeout=5) as response:
            sensors = json.load(response)["sensors"]
        lags = {}
        for sensor in sensors:
            labels = sensor["labels"]
            name = labels.get("sensor", "")
            stream_id = labels.get("stream_id")
            if stream_id is None or not name.startswith(SENSOR_PREFIX):
                continue
            short_name = name.removeprefix(SENSOR_PREFIX)
            if short_name in LAG_SENSORS:
                lags[(short_name, stream_id)] = sensor["value"]
        self.last_lags = lags
        return lags

    def wait_lags(self, federation, predicate, description):
        def check():
            lags = self.read_lags(federation)
            return all(
                (sensor, stream_id) in lags
                for sensor in LAG_SENSORS
                for stream_id in PROCESSED_STREAMS + (FAR_TIMER_STREAM,)
            ) and predicate(lags)

        wait(
            check,
            timeout=WAIT_TIMEOUT,
            ignore_exceptions=True,
            error_message=lambda: f"Timed out waiting for {description}, last lags: {self.last_lags}",
        )

    def update_computation_dynamic_specs(self, updates):
        dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
        for computation_id, update in updates.items():
            update(dynamic_spec["spec"]["computations"][computation_id])
        self.client.set_pipeline_dynamic_spec(
            self.pipeline_path,
            dynamic_spec["spec"],
            expected_version=dynamic_spec["version"],
        )
        self.wait_dynamic_spec_sync()

    def wait_dynamic_spec_sync(self):
        version = self.client.get_pipeline_dynamic_spec(self.pipeline_path)["version"]

        def is_applied():
            execution_spec = self.client.get_flow_view(
                self.pipeline_path,
                view_path="/state/execution_spec",
                cache=False,
            )
            return execution_spec["dynamic_pipeline_spec"]["version"] == version

        wait(is_applied, timeout=WAIT_TIMEOUT)

    @staticmethod
    def has_no_lag(lags, stream_id):
        return 0 <= lags[("ready_time_lag", stream_id)] < NO_LAG_SECONDS

    @classmethod
    def far_timer_waits_without_ready_lag(cls, lags, min_time_lag):
        # Timers due in an hour hold `time_lag` from their creation, but none of them is ready.
        return (
            lags[("count_lag", FAR_TIMER_STREAM)] > 0
            and lags[("time_lag", FAR_TIMER_STREAM)] >= min_time_lag
            and cls.has_no_lag(lags, FAR_TIMER_STREAM)
            and lags[("ready_count_lag", FAR_TIMER_STREAM)] == 0
        )

    @pytest.mark.authors(["pechatnov"])
    def test_ready_lags(self):
        run_yt_sync("primary", self.work_yt_path)
        writer_client = yt_wrapper.YtClient(proxy=self.client.config["proxy"]["url"])
        with (
            QueueWriter(writer_client, self.input_queue),
            self.start_flow_process_federation(
                pipeline_binary_args={"--config": self.prepare_pipeline_config()},
            ) as federation,
        ):
            # No lags: only the timers far in the future hold the regular lags.
            self.wait_lags(
                federation,
                lambda lags: all(self.has_no_lag(lags, stream_id) for stream_id in PROCESSED_STREAMS)
                and self.far_timer_waits_without_ready_lag(lags, min_time_lag=NO_LAG_SECONDS),
                "no ready lags",
            )

            # Make lags: one row per batch cannot keep up with the input and the near timers.
            self.update_computation_dynamic_specs(
                {
                    computation_id: lambda spec: spec.update({"max_rows_per_batch": 1})
                    for computation_id in ("reader", "scheduler")
                }
            )
            self.wait_lags(
                federation,
                lambda lags: all(
                    lags[("ready_time_lag", stream_id)] > LAG_SECONDS and lags[("ready_count_lag", stream_id)] > 0
                    for stream_id in (SOURCE_STREAM, NEAR_TIMER_STREAM)
                )
                # Backpressure holds the internal stream within its buffer and moves the lag to the source,
                # so it is only checked to report its regular lags as ready ones.
                and lags[("ready_time_lag", EVENTS_STREAM)] == lags[("time_lag", EVENTS_STREAM)]
                and lags[("ready_count_lag", EVENTS_STREAM)] == lags[("count_lag", EVENTS_STREAM)]
                # A near timer lags from its due moment, not from its creation.
                and lags[("ready_time_lag", NEAR_TIMER_STREAM)] < lags[("time_lag", NEAR_TIMER_STREAM)]
                and self.far_timer_waits_without_ready_lag(lags, min_time_lag=NO_LAG_SECONDS),
                "ready lags",
            )

            # Remove lags.
            self.update_computation_dynamic_specs(
                {
                    computation_id: lambda spec: spec.pop("max_rows_per_batch", None)
                    for computation_id in ("reader", "scheduler")
                }
            )
            self.wait_lags(
                federation,
                lambda lags: all(self.has_no_lag(lags, stream_id) for stream_id in PROCESSED_STREAMS)
                and self.far_timer_waits_without_ready_lag(lags, min_time_lag=LAG_SECONDS),
                "ready lags to drain",
            )
