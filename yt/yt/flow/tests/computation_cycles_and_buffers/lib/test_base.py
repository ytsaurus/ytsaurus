import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

from yt.common import wait

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(
    "yt/yt/flow/tests/computation_cycles_and_buffers/pipeline/pipeline.yson"
)

if yatest.common.context.sanitize is not None:
    EVENT_COUNT = 200
else:
    EVENT_COUNT = 1000


def generate_data(event_count, tablet_count):
    result = []
    for i in range(event_count):
        result.append({"data": "payload", "$tablet_index": i % tablet_count})
    return result


TABLET_COUNT = 1
INPUT_DATA = generate_data(EVENT_COUNT, TABLET_COUNT)
INPUT_THROTTLER_ID_PREFIX = "test_input"

##################################################################


class TestBase(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path("yt/yt/flow/tests/computation_cycles_and_buffers/pipeline/pipeline")

    def setup_method(self, method):
        super(TestBase, self).setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.consumer = self.work_yt_path + "/consumer"
        self.state = self.work_yt_path + "/state"

    def prepare_environment(self):
        run_yt_sync("primary", self.work_yt_path, TABLET_COUNT)
        batching_write_rows(INPUT_DATA, lambda batch: self.client.insert_rows(self.input_queue, batch), 10000)

    def prepare_pipeline_config(
        self,
        finite=True,
        processing_mode="exactly_once",
        throttled_computation=None,
        output_buffer_limit=None,
    ):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.consumer}",
                "finite": finite,
            }
        )

        pipeline_config["spec"]["computations"]["reducer"]["external_state_managers"]["/state"]["parameters"][
            "path"
        ] = self.state

        for computation in pipeline_config["spec"]["computations"].values():
            parameters = computation["parameters"]
            if "processing_mode" in parameters:
                parameters["processing_mode"] = processing_mode

        dynamic_spec = pipeline_config["dynamic_spec"]
        if throttled_computation is not None:
            throttler_id = f"{INPUT_THROTTLER_ID_PREFIX}_{throttled_computation}"
            computation = dynamic_spec["computations"].setdefault(throttled_computation, {})
            computation["input_rows_throttler_id"] = throttler_id
            computation["max_rows_per_batch"] = 1
            dynamic_spec["throttlers"] = {
                throttler_id: {
                    "limit": 1.0,
                    "period": 1000,
                    "request_period": 100,
                    "max_grant_amount": 1,
                }
            }

        if output_buffer_limit is not None:
            output_buffer = dynamic_spec["job_tracker"]["buffer_state_manager"]["output_buffer"]
            output_buffer["job_guarantee"] = output_buffer_limit
            output_buffer["job_limit"] = output_buffer_limit

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def set_output_buffer_limit(self, limit):
        dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
        output_buffer = dynamic_spec["spec"]["job_tracker"]["buffer_state_manager"]["output_buffer"]
        output_buffer["job_guarantee"] = limit
        output_buffer["job_limit"] = limit
        self.client.set_pipeline_dynamic_spec(
            self.pipeline_path,
            dynamic_spec["spec"],
            expected_version=dynamic_spec["version"],
        )
        self.wait_dynamic_spec_sync()

    def release_input_throttler(self, computation_id):
        dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
        computation = dynamic_spec["spec"]["computations"][computation_id]
        computation.pop("input_rows_throttler_id")
        computation.pop("max_rows_per_batch")
        self.client.set_pipeline_dynamic_spec(
            self.pipeline_path,
            dynamic_spec["spec"],
            expected_version=dynamic_spec["version"],
        )
        self.wait_dynamic_spec_sync()

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
