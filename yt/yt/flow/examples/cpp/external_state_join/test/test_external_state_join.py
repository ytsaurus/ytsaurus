import pytest

import yatest.common

from yt.common import wait
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from .yt_sync import run_yt_sync, build_reference_table, repoint_current

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/../pipeline.yson")

#################################################################


class TestExternalStateJoin(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/../external_state_join")

    def setup_method(self, method):
        super(TestExternalStateJoin, self).setup_method(method)
        self.reference_v1 = self.work_yt_path + "/reference.v1"
        self.reference_v2 = self.work_yt_path + "/reference.v2"
        # The "current version" symlink the joiner reads through; repointing it
        # atomically swaps the whole reference dataset under the pipeline.
        self.current = self.work_yt_path + "/current"
        self.event_queue = self.work_yt_path + "/event_queue"
        self.event_consumer = self.work_yt_path + "/event_consumer"
        self.output_queue = self.work_yt_path + "/output_queue"
        self.output_producer = self.work_yt_path + "/output_producer"

    def prepare_environment(self):
        run_yt_sync("primary", self.work_yt_path)

        build_reference_table(self.client, self.reference_v1, [{"key": 1, "name": "alice-v1"}])
        repoint_current(self.client, self.current, self.reference_v1)

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)
        computations = pipeline_config["spec"]["computations"]

        computations["event_reader"]["source_streams"]["event_queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.event_queue}",
                "consumer_path": f"<cluster=primary>{self.event_consumer}",
                "finite": False,
            }
        )
        computations["lookup_join"]["external_state_joiners"]["/reference"]["parameters"][
            "path"
        ] = f"<cluster=primary>{self.current}"
        computations["lookup_join"]["sinks"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.output_queue}",
                "producer_path": f"<cluster=primary>{self.output_producer}",
            }
        )

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def get_output(self):
        return list(self.client.select_rows(f"key, name from [{self.output_queue}]"))

    @pytest.mark.authors(["blinkov"])
    def test_join(self):
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config()
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
        ):
            self.client.insert_rows(self.event_queue, [{"key": 1}])
            wait(lambda: self.get_output() == [{"key": 1, "name": "alice-v1"}], timeout=180)

            # Build a second snapshot and atomically repoint the "current"
            # symlink; the joiner must pick up the new dataset without a restart.
            build_reference_table(self.client, self.reference_v2, [{"key": 1, "name": "alice-v2"}])
            repoint_current(self.client, self.current, self.reference_v2)

            self.client.insert_rows(self.event_queue, [{"key": 1}])
            wait(
                lambda: self.get_output() == [{"key": 1, "name": "alice-v1"}, {"key": 1, "name": "alice-v2"}],
                timeout=180,
            )
