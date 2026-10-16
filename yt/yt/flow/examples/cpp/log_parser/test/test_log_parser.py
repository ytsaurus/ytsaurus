import pytest

import yatest.common
import yt.wrapper

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/../pipeline.yson")

INPUT_LINES = [
    "info:started",
    "debug:noisy",
    "garbage",
    "info:a;debug:skip;error:b",
    "warning:w;info:x",
]

EXPECTED_RECORDS = [
    ("info", "started", "info"),
    ("info", "a", "info"),
    ("error", "b", "error"),
    ("warning", "w", "error"),
    ("info", "x", "error"),
]

##################################################################


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/../log_parser")

    def setup_method(self, method):
        super(Test, self).setup_method(method)
        self.input_queue = f"{self.work_yt_path}/input_queue"
        self.input_consumer = f"{self.work_yt_path}/consumer"
        self.output_queue = f"{self.work_yt_path}/output_queue"

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["parser"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.input_consumer}",
                "finite": True,
            }
        )
        pipeline_config["spec"]["computations"]["parser"]["sinks"]["queue"]["parameters"][
            "queue_path"
        ] = self.output_queue

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def check_result(self):
        queue_rows = self.client.select_rows(
            f"* from [{self.output_queue}]", format=yt.wrapper.format.YsonFormat(encoding=None)
        )
        actual = [
            (row[b"level"].decode(), row[b"text"].decode(), row[b"worst_level_so_far"].decode()) for row in queue_rows
        ]

        assert len(actual) == len(EXPECTED_RECORDS)
        assert sorted(actual) == sorted(EXPECTED_RECORDS)

    @pytest.mark.authors(["blinkov"])
    def test_parses_log_lines(self):
        run_yt_sync("primary", self.work_yt_path, queue_tablet_count=1)

        rows = [{"line": line, "$tablet_index": 0} for line in INPUT_LINES]
        batching_write_rows(rows, lambda batch: self.client.insert_rows(self.input_queue, batch), 100)

        pipeline_config_path = self.prepare_pipeline_config()

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
        ):
            self.wait_pipeline_state("completed", timeout=240)

        self.check_result()
