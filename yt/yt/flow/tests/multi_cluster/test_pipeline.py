import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

import pytest

from yt.common import wait

from .yt_sync import run_primary_yt_sync, run_remote_yt_sync

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


class TestMultiCluster(FlowTestBase):
    FLOW_BINARY_PATH = FLOW_BINARY_PATH
    DRIVER_BACKEND = "rpc"

    def setup_method(self, method):
        super(TestMultiCluster, self).setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.consumer = self.work_yt_path + "/consumer"
        self.output_queue_one = self.work_yt_path + "/output_queue"
        self.output_queue_multi = self.work_yt_path + "/output_queue_multi"
        self.producer = self.work_yt_path + "/producer"
        self.first_replica_cluster_name = self.remote_cluster_names[0]
        self.all_replica_cluster_names = self.remote_cluster_names[:2]

    def prepare_environment(self):
        run_primary_yt_sync("primary", self.work_yt_path, TABLET_COUNT)
        for replica_name in self.all_replica_cluster_names:
            run_remote_yt_sync(replica_name, self.work_yt_path, TABLET_COUNT)

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.consumer}",
                "finite": False,
            }
        )

        pipeline_config["spec"]["computations"]["reader"]["sinks"]["queue_one_cluster"]["parameters"].update(
            {
                "queue_path": f"<cluster={self.first_replica_cluster_name}>{self.output_queue_one}",
                "producer_path": f"<clusters=[{self.first_replica_cluster_name}]>{self.producer}",
            }
        )

        clusters_str = ";".join(self.all_replica_cluster_names)
        pipeline_config["spec"]["computations"]["reader"]["sinks"]["queue_multi_cluster"]["parameters"].update(
            {
                "queue_path": f"<clusters=[{clusters_str}]>{self.output_queue_multi}",
                "producer_path": f"<clusters=[{clusters_str}]>{self.producer}",
            }
        )

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["biwboris0"])
    def test_async_queue_sink_multi_cluster(self):
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config()

        remote_client_0 = self.cluster_name_to_client[self.all_replica_cluster_names[0]]

        # Step 0 - unmount all tables on remote clusters but one. Write half of data
        for i in range(1, len(self.all_replica_cluster_names)):
            self.cluster_name_to_client[self.all_replica_cluster_names[i]].unmount_table(self.output_queue_multi, sync=True)
        batching_write_rows(INPUT_DATA[: EVENT_COUNT // 2], lambda batch: self.client.insert_rows(self.input_queue, batch), 100)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            multi_expr = f"data from [{self.output_queue_multi}]"
            one_cluster_expr = f"data from [{self.output_queue_one}]"

            wait(
                lambda: len(list(remote_client_0.select_rows(multi_expr))) > 0,
                timeout=120,
            )
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=60)

            # Step 1 - mount all remote tables from step 0 and ummount other one. Write the other half of data
            for i in range(1, len(self.all_replica_cluster_names)):
                self.cluster_name_to_client[self.all_replica_cluster_names[i]].mount_table(self.output_queue_multi, sync=True)
            remote_client_0.unmount_table(self.output_queue_multi, sync=True)
            batching_write_rows(INPUT_DATA[EVENT_COUNT // 2 :], lambda batch: self.client.insert_rows(self.input_queue, batch), 100)
            self.client.start_pipeline(self.pipeline_path)

            # With finite=False we can't wait for "completed"
            first_replica_client = self.cluster_name_to_client[self.first_replica_cluster_name]
            wait(
                lambda: len(list(first_replica_client.select_rows(one_cluster_expr))) == EVENT_COUNT,
                timeout=60,
            )
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped", timeout=60)

            one_cluster_data = [
                row["data"]
                for row in list(first_replica_client.select_rows(one_cluster_expr))
            ]
            one_cluster_data.sort()
            assert one_cluster_data == EXPECTED_DATA

            # Step 2 - read data from all remote clusters. It should be written to several tables
            remote_client_0.mount_table(self.output_queue_multi, sync=True)
            data = []
            clusters_with_data = 0
            for cluster in self.all_replica_cluster_names:
                data_part = [row["data"] for row in list(self.cluster_name_to_client[cluster].select_rows(multi_expr))]
                clusters_with_data += len(data_part) > 0
                data += data_part
            assert clusters_with_data > 1
            data.sort()
            assert data == EXPECTED_DATA
