import pytest
import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")


def generate_data(event_count):
    result = []
    for i in range(event_count):
        # '\\' cause failure.
        result.append({"key": i, "text_key": f"key_{i}\\{i}", "value": i})
    return result


if yatest.common.context.sanitize is not None:
    ROW_COUNT = 150
    ROW_BATCH_SIZE = 15
else:
    ROW_COUNT = 1500
    ROW_BATCH_SIZE = 150

INPUT_DATA = generate_data(ROW_COUNT)

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
        self.client.insert_rows(self.data_state, INPUT_DATA)

    def prepare_pipeline_config(
        self,
        desired_cycle_time,
        finite=False,
        desired_partition_count=5,
        batch_timeout="15s",
        fetch_type: str = None,
    ):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["dynamic_spec"]["computations"]["reader"]["source_streams"]["main_source"]["parameters"] = {
            "desired_partition_count": desired_partition_count,
            "desired_cycle_time": desired_cycle_time,
        }

        reader_config = pipeline_config["spec"]["computations"]["reader"]

        reader_config["source_streams"]["main_source"]["parameters"]["finite"] = finite

        joiner_config = reader_config["source_streams"]["main_source"]["parameters"]["table_joiner"]
        joiner_config["fetchers"][0]["table_path"] = f"<cluster=primary>{self.data_state}"

        if fetch_type:
            joiner_config["fetchers"][0]["fetch_type"] = fetch_type

        pipeline_config["dynamic_spec"]["computations"]["reader"]["max_rows_per_batch"] = ROW_BATCH_SIZE

        pipeline_config["spec"]["computations"]["state_keeper"]["external_state_managers"]["/state"]["parameters"][
            "path"
        ] = f"{self.data_state}"

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["sergeypozdeev"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count", "fetch_type"),
        [
            pytest.param(1, 1, "select_rows", id="1c_1w_stable_select_rows"),
            pytest.param(1, 1, "table_reader", id="1c_1w_stable_table_reader"),
        ],
    )
    def test_clean(self, workers_count, controllers_count, fetch_type):
        pipeline_config_path = self.prepare_pipeline_config("10s", finite=True, fetch_type=fetch_type)
        self.prepare_environment()

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=workers_count,
            controllers_count=controllers_count,
            problems=False,
        ):
            self.wait_pipeline_state("completed", timeout=180)

            rows = list(self.client.select_rows(f"* from [{self.data_state}]"))
            assert len(rows) == (ROW_COUNT / 2)

            assert all(row["value"] % 2 != 0 for row in rows)
