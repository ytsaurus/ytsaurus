from collections import Counter
import time

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
    THROTTLER_LIMIT = 8.0
    PRIORITY_WINDOW = 120
else:
    EVENT_COUNT = 200
    THROTTLER_LIMIT = 15.0
    PRIORITY_WINDOW = 180

MAX_ROWS_PER_BATCH = 10

# Exact weighted ratios require every class to stay backlogged at the server.
# Flow readers briefly become inactive between prefetch RPCs and epochs, so the
# integration tests assert priority ordering and work conservation instead.
# Deterministic scheduler unit tests cover the exact ratios.

##################################################################


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def setup_method(self, method):
        super(Test, self).setup_method(method)
        self.input_queues = {
            "vip": f"{self.work_yt_path}/input_vip_queue",
            "regular": f"{self.work_yt_path}/input_regular_queue",
            "bulk": f"{self.work_yt_path}/input_bulk_queue",
        }
        self.input_consumers = {
            "vip": f"{self.work_yt_path}/consumer_vip",
            "regular": f"{self.work_yt_path}/consumer_regular",
            "bulk": f"{self.work_yt_path}/consumer_bulk",
        }
        self.output_queue = f"{self.work_yt_path}/output_queue"

    def write_input_data(self, count, source="vip", start=0, event_time_start=None):
        if event_time_start is None:
            event_time_start = start
        rows = [
            {
                "value": start + i,
                "source": source,
                "event_time": event_time_start + i,
                "$tablet_index": 0,
            }
            for i in range(count)
        ]
        batching_write_rows(
            rows,
            lambda batch: self.client.insert_rows(self.input_queues[source], batch),
            100,
        )

    def prepare_pipeline_config(self, finite=True, watermark_alignment=None):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        for source, computation in [
            ("vip", "ReaderVip"),
            ("regular", "ReaderRegular"),
            ("bulk", "ReaderBulk"),
        ]:
            pipeline_config["spec"]["computations"][computation]["source_streams"]["queue"]["parameters"].update(
                {
                    "queue_path": f"<cluster=primary>{self.input_queues[source]}",
                    "consumer_path": f"<cluster=primary>{self.input_consumers[source]}",
                    "finite": finite,
                }
            )
            if watermark_alignment is not None:
                watermark_strategy = {
                    "event_timestamp_assigner": {
                        "column": "event_time",
                        "limit_by_system_timestamp": True,
                    },
                    "watermark_generator": {
                        "out_of_orderness_bound": 0,
                    },
                }
                if watermark_alignment != "none":
                    watermark_strategy["watermark_alignment"] = {
                        "group_name": "source-priority-test",
                        "drift_bound": 1000,
                    }
                if watermark_alignment == "idle":
                    watermark_strategy["watermark_generator"]["idle_partitions"] = {
                        "duration": 500,
                        "max_ratio": 1.0,
                    }
                pipeline_config["spec"]["computations"][computation]["watermark_strategy"] = watermark_strategy
        pipeline_config["spec"]["computations"]["Throttled"]["sinks"]["queue"]["parameters"][
            "queue_path"
        ] = self.output_queue
        pipeline_config["dynamic_spec"]["throttlers"]["output_quota"]["limit"] = THROTTLER_LIMIT

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def count_output_rows(self):
        rows = self.client.select_rows(
            f"* from [{self.output_queue}]", format=yt.wrapper.format.YsonFormat(encoding=None)
        )
        return sum(1 for _ in rows)

    def count_output_by_source(self):
        rows = self.client.select_rows(
            f"source from [{self.output_queue}]",
            format=yt.wrapper.format.YsonFormat(encoding=None),
        )
        sources = []
        for row in rows:
            source = row[b"source"] if b"source" in row else row["source"]
            sources.append(source.decode() if isinstance(source, bytes) else source)
        return Counter(sources)

    @staticmethod
    def subtract_counts(after, before):
        return Counter({source: after[source] - before[source] for source in set(after) | set(before)})

    def wait_dynamic_spec_sync(self):
        dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)

        def is_applied():
            execution_spec = self.client.get_flow_view(
                self.pipeline_path,
                view_path="/state/execution_spec",
                cache=False,
            )
            return execution_spec["dynamic_pipeline_spec"]["version"] == dynamic_spec["version"]

        wait(is_applied, timeout=180)

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

    @pytest.mark.authors(["sergeypozdeev"])
    def test_weighted_split(self):
        run_yt_sync("primary", self.work_yt_path)
        for source in self.input_queues:
            self.write_input_data(max(EVENT_COUNT * 5, PRIORITY_WINDOW * 2), source)
        pipeline_config_path = self.prepare_pipeline_config(finite=False)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
        ):
            wait(lambda: self.count_output_rows() >= 90, timeout=180)
            before = self.count_output_by_source()
            wait(lambda: self.count_output_rows() >= sum(before.values()) + PRIORITY_WINDOW, timeout=180)
            after = self.count_output_by_source()
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=180)

        counts = self.subtract_counts(after, before)
        assert counts["vip"] > counts["bulk"], counts
        assert counts["regular"] > counts["bulk"], counts

    @pytest.mark.authors(["sergeypozdeev"])
    def test_idle_class_share_is_redistributed(self):
        run_yt_sync("primary", self.work_yt_path)
        backlog_size = max(EVENT_COUNT * 5, 500)
        self.write_input_data(backlog_size, "regular")
        self.write_input_data(backlog_size, "bulk")
        pipeline_config_path = self.prepare_pipeline_config(finite=False)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
        ):
            wait(lambda: self.count_output_rows() >= 60, timeout=180)
            before = self.count_output_by_source()
            started_at = time.monotonic()
            wait(lambda: self.count_output_rows() >= sum(before.values()) + PRIORITY_WINDOW, timeout=180)
            counts = self.subtract_counts(self.count_output_by_source(), before)
            elapsed = time.monotonic() - started_at
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=180)

        assert counts.get("vip", 0) == 0
        assert counts["regular"] > 0
        assert counts["bulk"] > 0
        # Exact active-class ratios are covered by scheduler unit tests. End-to-end
        # request boundaries can briefly make a reader inactive on the server, so
        # this test verifies non-starvation and work conservation instead. The two
        # backlogged classes together must consume the whole limit; a non-conserving
        # fixed split would cap them at 4/9 of it.
        assert sum(counts.values()) / elapsed >= THROTTLER_LIMIT * 0.7

    @pytest.mark.authors(["sergeypozdeev"])
    def test_rare_class_preempts_backlog(self):
        run_yt_sync("primary", self.work_yt_path)
        backlog_size = max(EVENT_COUNT * 10, 1000)
        self.write_input_data(backlog_size, "bulk")
        pipeline_config_path = self.prepare_pipeline_config(finite=False)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
        ):
            wait(lambda: self.count_output_rows() >= 20, timeout=180)
            started_at = time.monotonic()
            self.write_input_data(1, "vip", start=10_000_000)
            wait(lambda: self.count_output_by_source().get("vip", 0) == 1, timeout=30)
            wake_up_latency = time.monotonic() - started_at
            counts = self.count_output_by_source()
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=180)

        assert counts["bulk"] < backlog_size
        assert wake_up_latency < 10

    @pytest.mark.authors(["sergeypozdeev"])
    def test_class_weight_reconfigure(self):
        run_yt_sync("primary", self.work_yt_path)
        for source in self.input_queues:
            self.write_input_data(max(EVENT_COUNT * 10, 1000), source)
        pipeline_config_path = self.prepare_pipeline_config(finite=False)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
        ):
            wait(lambda: self.count_output_rows() >= 90, timeout=180)
            warm_before = self.count_output_by_source()
            wait(lambda: self.count_output_rows() >= sum(warm_before.values()) + PRIORITY_WINDOW, timeout=180)
            warm_counts = self.subtract_counts(self.count_output_by_source(), warm_before)

            dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
            classes = dynamic_spec["spec"]["throttlers"]["output_quota"]["classes"]
            classes["vip"]["weight"] = 1.0
            classes["bulk"]["weight"] = 5.0
            self.client.set_pipeline_dynamic_spec(
                self.pipeline_path,
                dynamic_spec["spec"],
                expected_version=dynamic_spec["version"],
            )
            self.wait_dynamic_spec_sync()

            transition_before = self.count_output_rows()
            wait(lambda: self.count_output_rows() >= transition_before + PRIORITY_WINDOW, timeout=180)
            after_transition = self.count_output_by_source()
            wait(lambda: self.count_output_rows() >= sum(after_transition.values()) + PRIORITY_WINDOW, timeout=180)
            final_counts = self.subtract_counts(self.count_output_by_source(), after_transition)
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=180)

        assert warm_counts["vip"] > warm_counts["bulk"], warm_counts
        assert final_counts["bulk"] > final_counts["vip"], final_counts

    @pytest.mark.authors(["sergeypozdeev"])
    @pytest.mark.parametrize(
        ("watermark_alignment", "continues"),
        [
            pytest.param("shared", False, id="shared_group"),
            pytest.param("none", True, id="no_group"),
            pytest.param("idle", True, id="idle_partitions"),
        ],
    )
    def test_silent_class_watermark_alignment(self, watermark_alignment, continues):
        run_yt_sync("primary", self.work_yt_path)
        self.write_input_data(
            EVENT_COUNT * 10,
            "bulk",
            event_time_start=int(time.time()),
        )
        pipeline_config_path = self.prepare_pipeline_config(
            finite=False,
            watermark_alignment=watermark_alignment,
        )

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
        ):
            wait(lambda: self.count_output_rows() >= 1, timeout=180)
            if continues:
                before = self.count_output_rows()
                started_at = time.monotonic()
                wait(lambda: self.count_output_rows() >= before + 6 * MAX_ROWS_PER_BATCH, timeout=30)
                observed_rate = (self.count_output_rows() - before) / (time.monotonic() - started_at)
            else:
                time.sleep(3)
                stalled_at = self.count_output_rows()
                time.sleep(2)
                stalled_delta = self.count_output_rows() - stalled_at

            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=180)

        if continues:
            assert observed_rate >= THROTTLER_LIMIT * 0.5
        else:
            assert stalled_delta <= MAX_ROWS_PER_BATCH

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
