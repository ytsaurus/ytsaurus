import pytest
import collections

import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

from yt.common import wait

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_SWIFT_CONFIG_PATH = yatest.common.source_path(
    f"{yatest.common.context.project_path}/pipeline/pipeline_swift.yson"
)
PIPELINE_TRANSFORM_CONFIG_PATH = yatest.common.source_path(
    f"{yatest.common.context.project_path}/pipeline/pipeline_transform.yson"
)


def generate_data(event_count, tablet_count):
    result = []
    ts = 1750000000
    for i in range(event_count):
        result.append(
            {
                "data": f"payload_{i}",
                "repeat": (i % 13) + 1,
                "flow_queue_meta": {"event_timestamp": ts + i},
                "$tablet_index": i % tablet_count,
            }
        )
    return result


def compute_event_watermark(events):
    watermark = 0
    for row in events:
        watermark = max(watermark, row["flow_queue_meta"]["event_timestamp"])
    return watermark + 1


if yatest.common.context.sanitize is not None:
    EVENT_COUNT = 200
else:
    EVENT_COUNT = 1000

TABLET_COUNT = 5
INPUT_DATA = generate_data(EVENT_COUNT, TABLET_COUNT)
EVENT_WATERMARK = compute_event_watermark(INPUT_DATA)
EXPECTED_DATA = {row["data"]: row["repeat"] for row in INPUT_DATA}

##################################################################


class TestQueueConnector(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def setup_method(self, method):
        super(TestQueueConnector, self).setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.consumer = self.work_yt_path + "/consumer"
        self.output_queue = self.work_yt_path + "/output_queue"
        self.producer = self.work_yt_path + "/producer"

    def prepare_environment(self):

        run_yt_sync("primary", self.work_yt_path, TABLET_COUNT)

        batching_write_rows(INPUT_DATA, lambda batch: self.client.insert_rows(self.input_queue, batch), 100)

        self.client.insert_rows(
            self.input_queue,
            [
                {
                    "flow_queue_meta": {"event_watermark": EVENT_WATERMARK, "pure_heartbeat": True},
                    "$tablet_index": tablet,
                }
                for tablet in range(TABLET_COUNT)
            ],
        )

    def prepare_swift_pipeline_config(self, sync, source_filter=None):
        pipeline_config = get_yson_config(PIPELINE_SWIFT_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.consumer}",
                "finite": False,
                "partition_filter": source_filter,
            }
        )

        sink_spec = pipeline_config["spec"]["computations"]["reader"]["sinks"]["queue"]
        sink_spec["parameters"]["queue_path"] = f"<cluster=primary>{self.output_queue}"
        if sync:
            sink_spec["sink_class_name"] = "NYT::NFlow::TSyncQueueSink"
        else:
            sink_spec["sink_class_name"] = "NYT::NFlow::TAsyncQueueSink"
            sink_spec["parameters"]["producer_path"] = f"<cluster=primary>{self.producer}"

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def prepare_transform_pipeline_config(self, sync):
        pipeline_config = get_yson_config(PIPELINE_TRANSFORM_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.consumer}",
                "finite": False,
            }
        )

        sink_spec = pipeline_config["spec"]["computations"]["writer"]["sinks"]["queue"]
        sink_spec["parameters"]["queue_path"] = f"<cluster=primary>{self.output_queue}"
        if sync:
            sink_spec["sink_class_name"] = "NYT::NFlow::TSyncQueueSink"
        else:
            sink_spec["sink_class_name"] = "NYT::NFlow::TAsyncQueueSink"
            sink_spec["parameters"]["producer_path"] = f"<cluster=primary>{self.producer}"

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def get_heartbeat_watermark(self):
        expr = f"flow_queue_meta, [$tablet_index] from [{self.output_queue}]"
        rows = list(self.client.select_rows(expr))
        watermarks = {i: 0 for i in range(TABLET_COUNT)}
        for row in rows:
            tablet_index = row["$tablet_index"]
            watermarks[tablet_index] = max(
                watermarks.get(tablet_index, 0), row.get("flow_queue_meta", {}).get("event_watermark", 0)
            )

        return min(watermarks.values())

    def check(self, pipeline_config_path, workers_count, controllers_count, problems, expected=EXPECTED_DATA):
        self.prepare_environment()
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=workers_count,
            controllers_count=controllers_count,
            problems=problems,
        ):
            self.wait_pipeline_state("working", timeout=180)
            # wait until pipeline reach watermark
            wait(lambda: self.get_processing_watermark() >= EVENT_WATERMARK, timeout=180, ignore_exceptions=True)
            # wait until pipeline sent heartbeat, may be slow due too massive read
            wait(lambda: self.get_heartbeat_watermark() >= EVENT_WATERMARK, timeout=180, ignore_exceptions=True)

            expr = f"data, flow_queue_meta, [$tablet_index] from [{self.output_queue}]"
            rows = list(self.client.select_rows(expr))
            data = [row["data"] for row in rows if not row.get("flow_queue_meta", {}).get("pure_heartbeat", False)]
            watermarks = dict()
            for row in rows:
                tablet_index = row["$tablet_index"]
                watermarks[tablet_index] = max(
                    watermarks.get(tablet_index, 0), row.get("flow_queue_meta", {}).get("event_watermark", 0)
                )
                if not row.get("flow_queue_meta", {}).get("pure_heartbeat", False):
                    assert row.get("flow_queue_meta", {}).get("event_timestamp", 0) >= watermarks.get(tablet_index, 0)

            got = collections.Counter(data)
            assert got == expected

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count", "problems"),
        [
            # pytest.param(1, 1, False, id="1c_1w_stable"),
            pytest.param(4, 2, True, id="2c_4w_unstable"),
        ],
    )
    def test_swift_write_sync(self, workers_count, controllers_count, problems):
        pipeline_config_path = self.prepare_swift_pipeline_config(True)
        self.check(pipeline_config_path, workers_count, controllers_count, problems)

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count", "problems"),
        [
            # pytest.param(1, 1, False, id="1c_1w_stable"),
            pytest.param(4, 2, True, id="2c_4w_unstable"),
        ],
    )
    def test_swift_write_async(self, workers_count, controllers_count, problems):
        pipeline_config_path = self.prepare_swift_pipeline_config(False)
        self.check(pipeline_config_path, workers_count, controllers_count, problems)

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count", "problems"),
        [
            # pytest.param(1, 1, False, id="1c_1w_stable"),
            pytest.param(4, 2, True, id="2c_4w_unstable"),
        ],
    )
    def test_transform_write_sync(self, workers_count, controllers_count, problems):
        pipeline_config_path = self.prepare_transform_pipeline_config(True)
        self.check(pipeline_config_path, workers_count, controllers_count, problems)

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count", "problems"),
        [
            # pytest.param(1, 1, False, id="1c_1w_stable"),
            pytest.param(4, 2, True, id="2c_4w_unstable"),
        ],
    )
    def test_transform_write_async(self, workers_count, controllers_count, problems):
        pipeline_config_path = self.prepare_transform_pipeline_config(False)
        self.check(pipeline_config_path, workers_count, controllers_count, problems)

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count", "problems"),
        [
            # pytest.param(1, 1, False, id="1c_1w_stable"),
            pytest.param(4, 2, True, id="2c_4w_unstable"),
        ],
    )
    def test_partition_filter(self, workers_count, controllers_count, problems):
        pipeline_config_path = self.prepare_swift_pipeline_config(True, [[0, 1], [3, 5]])
        expected = {row["data"]: row["repeat"] for row in INPUT_DATA if row["$tablet_index"] in (0, 3, 4)}
        self.check(pipeline_config_path, workers_count, controllers_count, problems, expected)

    def check_trimmed(self, pipeline_config_path, workers_count, controllers_count, problems, trim_per_tablet):
        self.prepare_environment()

        # Trim the head of every partition before the consumer ever reads it (consumer offset stays at 0).
        # The source must derive its read lower bound from the queue's trim point, deliver every surviving row and
        # never skip a row that is still present. This guards the trim-point lower-bound logic in the queue source.
        for tablet in range(TABLET_COUNT):
            self.client.trim_rows(self.input_queue, tablet, trim_per_tablet)

        # Rows are written round-robin over tablets in input order, so the first `trim_per_tablet` rows of tablet `t`
        # are the input rows with `$tablet_index == t` taken in order; everything past the trim point must survive.
        survivors = collections.Counter()
        seen_per_tablet = {t: 0 for t in range(TABLET_COUNT)}
        for row in INPUT_DATA:
            tablet = row["$tablet_index"]
            if seen_per_tablet[tablet] >= trim_per_tablet:
                survivors[row["data"]] += row["repeat"]
            seen_per_tablet[tablet] += 1
        expected = dict(survivors)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=workers_count,
            controllers_count=controllers_count,
            problems=problems,
        ):
            self.wait_pipeline_state("working", timeout=180)
            wait(lambda: self.get_processing_watermark() >= EVENT_WATERMARK, timeout=180, ignore_exceptions=True)
            wait(lambda: self.get_heartbeat_watermark() >= EVENT_WATERMARK, timeout=180, ignore_exceptions=True)

            expr = f"data, flow_queue_meta, [$tablet_index] from [{self.output_queue}]"
            rows = list(self.client.select_rows(expr))
            data = [row["data"] for row in rows if not row.get("flow_queue_meta", {}).get("pure_heartbeat", False)]
            got = collections.Counter(data)
            assert got == expected

    @pytest.mark.authors(["pechatnov"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count", "problems"),
        [
            pytest.param(4, 2, True, id="2c_4w_unstable"),
        ],
    )
    def test_trimmed_queue_no_loss(self, workers_count, controllers_count, problems):
        pipeline_config_path = self.prepare_swift_pipeline_config(True)
        self.check_trimmed(pipeline_config_path, workers_count, controllers_count, problems, trim_per_tablet=3)
