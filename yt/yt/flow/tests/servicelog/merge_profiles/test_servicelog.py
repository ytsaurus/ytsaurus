import logging
import time

import pytest
import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.bullied_process import ProblemsConfig

from yt.common import wait

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")


def generate_data(event_count, is_another=False):
    result = []
    for i in range(event_count):
        if is_another and i % 10 == 0:
            continue
        result.append(
            {"key": i, "value": i + 1 + (2 if is_another else 0), "second_value": i + 2 + (2 if is_another else 0)}
        )
    return result


if yatest.common.context.sanitize is not None:
    ROW_COUNT = 150
    ROW_BATCH_SIZE = 15
else:
    ROW_COUNT = 1500
    ROW_BATCH_SIZE = 150

INPUT_DATA = generate_data(ROW_COUNT, False)
ANOTHER_INPUT_DATA = generate_data(ROW_COUNT, True)

##################################################################


class TestServicelogConnector(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def setup_method(self, method):
        super(TestServicelogConnector, self).setup_method(method)
        self.data_state = self.work_yt_path + "/data_state"
        self.profiles = self.work_yt_path + "/profiles"
        self.another_profiles = self.work_yt_path + "/another_profiles"
        self.nonexisting1 = self.work_yt_path + "/nonexisting1"
        self.nonexisting2 = self.work_yt_path + "/nonexisting2"

    def prepare_environment(self):
        run_yt_sync("primary", self.work_yt_path)
        self.client.insert_rows(self.profiles, INPUT_DATA)
        self.client.insert_rows(self.another_profiles, ANOTHER_INPUT_DATA)

    def prepare_pipeline_config(
        self,
        desired_cycle_time,
        finite=False,
        desired_partition_count=5,
        batch_timeout="15s",
        non_existing_tables=False,
        throttler_period="10s",
    ):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["dynamic_spec"]["computations"]["reader"]["source_streams"]["main_source"]["parameters"] = {
            "desired_partition_count": desired_partition_count,
            "desired_cycle_time": desired_cycle_time,
            "throttler_period": throttler_period,
        }

        reader_config = pipeline_config["spec"]["computations"]["reader"]

        reader_config["source_streams"]["main_source"]["parameters"]["finite"] = finite

        joiner_config = reader_config["source_streams"]["main_source"]["parameters"]["table_joiner"]
        if non_existing_tables:
            joiner_config["fetchers"][0]["table_path"] = f"<cluster=primary>{self.nonexisting1}"
            joiner_config["fetchers"][1]["table_path"] = f"<cluster=primary>{self.nonexisting2}"
        else:
            joiner_config["fetchers"][0]["table_path"] = f"<cluster=primary>{self.profiles}"
            joiner_config["fetchers"][1]["table_path"] = f"<cluster=primary>{self.another_profiles}"

        pipeline_config["dynamic_spec"]["computations"]["reader"]["max_rows_per_batch"] = ROW_BATCH_SIZE

        pipeline_config["spec"]["computations"]["state_keeper"]["external_state_managers"]["/state"]["parameters"][
            "path"
        ] = f"{self.data_state}"

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["vv-glazkov"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count"),
        [
            pytest.param(4, 2, id="2c_4w_unstable"),
        ],
    )
    def test_no_throttling(self, workers_count, controllers_count):
        pipeline_config_path = self.prepare_pipeline_config("1s")
        self.prepare_environment()

        def check_success():
            rows = list(self.client.select_rows(f"* from [{self.data_state}]"))
            if len(rows) != ROW_COUNT:
                return False
            if not all(row["count"] >= 5 for row in rows):
                return False
            return True

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=workers_count,
            controllers_count=controllers_count,
            worker_problems_config=ProblemsConfig(interval_seconds=60, problems_max_count=2),
        ):
            self.wait_pipeline_state("working", timeout=180)
            wait(check_success, timeout=300, ignore_exceptions=True)
            rows = list(self.client.select_rows(f"* from [{self.data_state}]"))
            assert max(row["count"] for row in rows) - min(row["count"] for row in rows) <= 1
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=180)

    @pytest.mark.authors(["vv-glazkov"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count"),
        [
            pytest.param(4, 2, id="2c_4w_unstable"),
        ],
    )
    def test_with_throttling(self, workers_count, controllers_count):
        # We have to be sure here that row count in every single partition is at least a few times greater that batch row count.
        # Otherwise, all partition rows might be fetched in one or two batches, thus throttler will be called just once or twice,
        # which is NOT enough for it to work correctly.
        # A small throttler_period keeps the throttler's initial burst (Period * Limit) tiny, so the actual
        # read time stays close to desired_cycle_time (~50s) instead of collapsing to the burst-reduced floor.
        # This restores real margin under the assertion below, even when the unstable variant restarts workers
        # (each restart re-arms the burst over the remaining range).
        pipeline_config_path = self.prepare_pipeline_config(
            "50s", finite=False, desired_partition_count=2, throttler_period="2s"
        )
        self.prepare_environment()

        def check_success():
            rows = list(self.client.select_rows(f"* from [{self.data_state}]"))
            if len(rows) != ROW_COUNT:
                return False
            return True

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=workers_count,
            controllers_count=controllers_count,
            worker_problems_config=ProblemsConfig(interval_seconds=60, problems_max_count=2),
        ):
            self.wait_pipeline_state("working", timeout=180)
            start_time = time.time()
            wait(check_success, timeout=300, ignore_exceptions=True)
            time_elapsed = time.time() - start_time
            # Lower bound: throttling must slow reading well past the unthrottled case (a few seconds).
            # Upper bound: guards against an unrelated stall being mistaken for "throttling works".
            assert 40 < time_elapsed < 120
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=180)

    @pytest.mark.authors(["vv-glazkov"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count"),
        [
            pytest.param(4, 2, id="2c_4w_unstable"),
        ],
    )
    def test_ensure_not_stuck_while_creating(self, workers_count, controllers_count):
        pipeline_config_path = self.prepare_pipeline_config(
            "1s", finite=False, desired_partition_count=1, batch_timeout="3s", non_existing_tables=True
        )

        self.prepare_environment()

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=workers_count,
            controllers_count=controllers_count,
            worker_problems_config=ProblemsConfig(interval_seconds=60, problems_max_count=2),
        ):
            self.wait_pipeline_state("working", timeout=180)
            report_time = 0
            for iteration in range(20):
                view_path = "/state/traverse_data/computations/reader/report_time"
                wait(
                    lambda: self.client.get_flow_view(self.pipeline_path, view_path=view_path) > report_time,
                    timeout=300,
                    ignore_exceptions=True,
                )
                report_time = self.client.get_flow_view(self.pipeline_path, view_path=view_path)
                logging.info("Test iteration completed (Iteration: %d, ReportTime: %s)", iteration, report_time)

    @pytest.mark.authors(["vv-glazkov"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count"),
        [
            pytest.param(4, 2, id="2c_4w_unstable"),
        ],
    )
    def test_finite(self, workers_count, controllers_count):
        pipeline_config_path = self.prepare_pipeline_config("10s", finite=True)
        self.prepare_environment()
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=workers_count,
            controllers_count=controllers_count,
            worker_problems_config=ProblemsConfig(interval_seconds=60, problems_max_count=2),
        ):
            self.wait_pipeline_state("working", timeout=180)
            self.wait_pipeline_state("completed", timeout=180)
            rows = list(self.client.select_rows(f"* from [{self.data_state}]"))
            assert len(rows) == ROW_COUNT
            assert all(row["count"] == 1 for row in rows)
