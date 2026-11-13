import pytest

import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/../pipeline.yson")

TABLET_COUNT = 2
ROWS_PER_TABLET = 50
WINDOW_PERIOD = 3

##################################################################


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/../queue_static_table_export")

    def setup_method(self, method):
        super(Test, self).setup_method(method)
        self.input_queue = f"{self.work_yt_path}/input_queue"
        self.input_consumer = f"{self.work_yt_path}/consumer"
        self.export_root = f"{self.work_yt_path}/export"

    def prepare_pipeline_config(self, parse):
        config = get_yson_config(PIPELINE_CONFIG_PATH)
        export = config["spec"]["computations"]["export"]
        if not parse:
            # Without a process function the columns of the schema are taken from the queue rows.
            del export["sinks"]["export"]["parameters"]["processing_function"]
            export["sinks"]["export"]["parameters"]["schema"] = [{"name": "line", "type": "string"}]
        export["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.input_consumer}",
                "finite": True,
            }
        )
        # Windows close only once the finished source stream completes, after the completing jobs claim.
        export["watermark_strategy"]["watermark_generator"]["idle_partitions"]["duration"] = "30s"
        export["watermark_strategy"]["watermark_alignment"]["drift_bound"] = "6s"
        export["sinks"]["export"]["parameters"].update(
            {
                "output_directory": self.export_root,
                "window_period": f"{WINDOW_PERIOD}s",
                "closure_delta": "1s",
                "max_unpublished_window_count": 6,
                "table_name_utc_offset": "+00:00",
            }
        )
        config["spec"]["resources"]["ExportWriter"]["parameters"].update(
            {
                "idle_flush_period": "200ms",
                "max_stream_age": "10s",
                "transaction_timeout": "15s",
                "commit_timeout": "20s",
                "io_timeout": "10s",
                "progress_refresh_period": "1s",
                "retry_backoff": "200ms",
            }
        )
        config["dynamic_spec"]["computations"]["export"]["sinks"] = {
            "export": {"parameters": {"publication_period": "200ms", "retry_backoff": "200ms"}}
        }
        self.patch_config(config)
        return self.dump_config_to_log_dir(config, "pipeline.yson")

    def check_result(self, columns, expected):
        names = sorted(str(name) for name in self.client.list(self.export_root) if str(name) != "_export_state")
        assert names
        next_offsets = {tablet: 0 for tablet in range(TABLET_COUNT)}
        exported = []
        previous_start = None
        for name in names:
            path = f"{self.export_root}/{name}"
            start = self.client.get(f"{path}/@window_start")
            # Windows are published one by one, empty ones included.
            assert previous_start is None or start == previous_start + WINDOW_PERIOD
            previous_start = start
            assert [column["name"] for column in self.client.get(f"{path}/@schema")] == columns
            metadata = self.client.get(f"{path}/@_queue_metadata")
            assert metadata["cluster"] == "primary"
            assert metadata["queue_path"] == self.input_queue
            parts = metadata["partitions"]
            assert [part["partition"] for part in parts] == list(range(TABLET_COUNT))
            # Consecutive tables cover consecutive offset ranges.
            for part in parts:
                assert part["first_offset"] == next_offsets[part["partition"]]
                next_offsets[part["partition"]] = part["next_offset"]
            exported += [tuple(row[column] for column in columns) for row in self.client.read_table(path)]
        assert next_offsets == {tablet: ROWS_PER_TABLET for tablet in range(TABLET_COUNT)}
        assert sorted(exported) == sorted(expected)

    @pytest.mark.authors(["pechatnov"])
    @pytest.mark.parametrize("parse", [True, False])
    def test_exports_queue(self, parse):
        run_yt_sync("primary", self.work_yt_path, queue_tablet_count=TABLET_COUNT)

        # Every third line has no records, so its offset is exported without table rows.
        lines = [
            (tablet, f"info:{tablet}-{index};error:{tablet}-{index}" if index % 3 else "garbage")
            for index in range(ROWS_PER_TABLET)
            for tablet in range(TABLET_COUNT)
        ]
        rows = [{"line": line, "$tablet_index": tablet} for tablet, line in lines]
        batching_write_rows(rows, lambda batch: self.client.insert_rows(self.input_queue, batch), 10)

        pipeline_config_path = self.prepare_pipeline_config(parse)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
        ):
            self.wait_pipeline_state("completed", timeout=300)

        if parse:
            expected = [
                tuple(record.split(":")) for _, line in lines if line != "garbage" for record in line.split(";")
            ]
            self.check_result(["level", "text"], expected)
        else:
            self.check_result(["line"], [(line,) for _, line in lines])
