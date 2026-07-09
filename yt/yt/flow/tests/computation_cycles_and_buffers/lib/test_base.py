import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

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

    def prepare_pipeline_config(self, finite=True, processing_mode="exactly_once"):
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

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")
