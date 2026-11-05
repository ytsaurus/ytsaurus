import pytest
import time

import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

from yt.common import wait

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")
FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

if yatest.common.context.sanitize is not None:
    EVENT_COUNT = 200
else:
    EVENT_COUNT = 1000


def generate_data(event_count, tablet_count):
    result = []
    for i in range(event_count):
        result.append({"data": f"payload_{i}", "$tablet_index": i % tablet_count})
    return result


TABLET_COUNT = 5
INPUT_DATA = generate_data(EVENT_COUNT, TABLET_COUNT)
EXPECTED_DATA = [row["data"] for row in INPUT_DATA]
EXPECTED_DATA.sort()

##################################################################


class TestAtMostOnceSink(FlowTestBase):
    FLOW_BINARY_PATH = FLOW_BINARY_PATH
    DRIVER_BACKEND = "rpc"

    def setup_method(self, method):
        super(TestAtMostOnceSink, self).setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.consumer = self.work_yt_path + "/consumer"
        self.output_queue = self.work_yt_path + "/output_queue"
        self.control_output_queue = self.work_yt_path + "/control_output_queue"
        self.producer = self.work_yt_path + "/producer"

    def prepare_environment(self):
        run_yt_sync("primary", self.work_yt_path, TABLET_COUNT)

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.consumer}",
                "finite": False,
            }
        )

        pipeline_config["spec"]["computations"]["reader"]["sinks"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.output_queue}",
                "producer_path": f"<cluster=primary>{self.producer}",
            }
        )

        pipeline_config["spec"]["computations"]["reader"]["sinks"]["control_queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.control_output_queue}",
                "producer_path": f"<cluster=primary>{self.producer}",
            }
        )

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def _get_partitions_count(self):
        return len(self.client.get_flow_view(self.pipeline_path, view_path="/state/execution_spec/layout/partitions", cache=False))

    @pytest.mark.authors(["biwboris0"])
    def test_at_most_once_guarantee(self):
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config()

        # count pointers
        first_half = EVENT_COUNT // 2
        third_quarter = EVENT_COUNT * 3 // 4

        # step 0: Unmount output queue. Write half of the data. We start with write ttl = inf
        self.client.unmount_table(self.output_queue, sync=True)
        batching_write_rows(INPUT_DATA[:first_half], lambda batch: self.client.insert_rows(self.input_queue, batch), 100)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            expr = f"data from [{self.output_queue}]"
            control_expr = f"data from [{self.control_output_queue}]"

            wait(lambda: len(list(self.client.select_rows(control_expr))) == first_half, timeout=120)

            partitions_count = self._get_partitions_count()

            # step 1: Stop pipeline and mount output queue. Let data to be written completely asynchronously
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=60)
            self.client.mount_table(self.output_queue, sync=True)
            wait(lambda: len(list(self.client.select_rows(expr))) == first_half, timeout=120)

            # step 2: Set write ttl to almost zero. Unmount output queue. Write quarter. Wait some time > ttl. Data should be lost after ttl
            dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)["spec"]
            dynamic_spec["computations"]["reader"]["sinks"]["queue"]["parameters"]["at_most_once_strategy"][
                "suspend_destruction_duration"
            ] = 100

            self.client.set_pipeline_dynamic_spec(self.pipeline_path, dynamic_spec)
            self.client.unmount_table(self.output_queue, sync=True)
            batching_write_rows(
                INPUT_DATA[first_half:third_quarter], lambda batch: self.client.insert_rows(self.input_queue, batch), 100
            )

            self.client.start_pipeline(self.pipeline_path)
            wait(lambda: len(list(self.client.select_rows(control_expr))) == third_quarter, timeout=120)
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=60)
            time.sleep(2)

            # step 3: Set queue size limit to 1. Mount output queue. Write last quarter. We should lost all the data but one message per partition
            dynamic_spec["computations"]["reader"]["sinks"]["queue"]["parameters"]["at_most_once_strategy"][
                "total_queue_bytes_limit"
            ] = 1
            self.client.set_pipeline_dynamic_spec(self.pipeline_path, dynamic_spec)

            self.client.mount_table(self.output_queue, sync=True)
            batching_write_rows(INPUT_DATA[third_quarter:], lambda batch: self.client.insert_rows(self.input_queue, batch), 100)

            assert len(list(self.client.select_rows(expr))) == EVENT_COUNT // 2

            self.client.start_pipeline(self.pipeline_path)
            wait(lambda: len(list(self.client.select_rows(control_expr))) == EVENT_COUNT, timeout=120)
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=60)

            control_data = [row["data"] for row in self.client.select_rows(control_expr)]
            control_data.sort()
            assert control_data == EXPECTED_DATA

            assert len(list(self.client.select_rows(expr))) == EVENT_COUNT // 2 + partitions_count
