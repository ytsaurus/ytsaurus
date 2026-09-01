import os
import time

import pytest
import yatest.common
import yt.yson as yson

from yt.common import wait

from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.queue import batching_write_rows

from .yt_sync import run_yt_sync

BLOB_TABLE_SCHEMA = yson.to_yson_type(
    [
        {"name": "filename", "type": "string", "sort_order": "ascending"},
        {"name": "part_index", "type": "int64", "sort_order": "ascending"},
        {"name": "data", "type": "string"},
    ],
    attributes={"strict": True, "unique_keys": True},
)


class TestFileResource(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/../file_resource")

    def setup_method(self, method):
        super().setup_method(method)
        self.input_queue = f"{self.work_yt_path}/input_queue"
        self.input_consumer = f"{self.work_yt_path}/consumer"
        self.output_queue = f"{self.work_yt_path}/output_queue"
        run_yt_sync("primary", self.work_yt_path)

    def prepare_pipeline(self, source_path):
        source = yatest.common.source_path(f"{yatest.common.context.project_path}/../pipeline-yt-file.yson")
        config = get_yson_config(source)
        reader = config["spec"]["computations"]["reader"]
        computation = config["spec"]["computations"]["enricher"]
        reader["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.input_consumer}",
                "finite": False,
            }
        )
        computation["sinks"]["queue"]["parameters"]["queue_path"] = f"<cluster=primary>{self.output_queue}"
        resource_spec = config["spec"]["resources"]["text"]
        resource_spec["file_sources"]["file"]["parameters"]["path"] = source_path
        config.setdefault("dynamic_spec", {}).setdefault("resources", {})["text"] = {
            "file_source_discover_period": 100,
            "file_source_update_retry_period": 100,
            "file_snapshot_min_creation_period": 100,
        }
        self.patch_config(config)
        return self.dump_config_to_log_dir(config, "pipeline-yt-file.yson")

    def write_input(self, value):
        batching_write_rows(
            [{"text": value, "$tablet_index": 0}],
            lambda rows: self.client.insert_rows(self.input_queue, rows),
            1,
        )

    def has_output(self, input_value, file_text):
        rows = self.client.select_rows(f"input, file_text from [{self.output_queue}]")
        return any(row["input"] == input_value and row["file_text"] == file_text for row in rows)

    def wait_output(self, input_value, file_text):
        wait(lambda: self.has_output(input_value, file_text), timeout=120, ignore_exceptions=True)

    def wait_for_updated_output(self, value, file_text):
        def probe():
            self.write_input(value)
            time.sleep(0.2)
            return self.has_output(value, file_text)

        wait(probe, timeout=120, ignore_exceptions=True)

    def write_blob_file(self, path, data):
        assert not self.client.exists(path)
        self.client.create("table", path, attributes={"schema": BLOB_TABLE_SCHEMA})
        self.client.write_table(
            path,
            [{"filename": "config-file", "part_index": 0, "data": data}],
        )

    def publish_blob_revision(self, link_path, revision, data):
        revision_root = f"{link_path}-revisions"
        if not self.client.exists(revision_root):
            self.client.create("map_node", revision_root, recursive=True)
        table_path = f"{revision_root}/{revision}"
        self.write_blob_file(table_path, data)
        self.client.link(table_path, link_path, force=True)

    def make_node_config(self):
        base_cache_path = os.path.abspath(os.path.join(self.path_to_flow_logs, "file-storage", "unused"))
        worker_cache_path = os.path.abspath(os.path.join(self.path_to_flow_logs, "file-storage", "worker-0"))
        os.makedirs(base_cache_path, exist_ok=True)
        os.makedirs(worker_cache_path, exist_ok=True)
        return (
            {
                "enable_porto_resource_tracker": False,
                "worker": {
                    "file_storage": {
                        "path": base_cache_path,
                        "soft_size_limit": 48 * 1024 * 1024,
                        "hard_size_limit": 64 * 1024 * 1024,
                        "cleanup_period": 100,
                    }
                },
            },
            [{"worker": {"file_storage": {"path": worker_cache_path}}}],
        )

    @pytest.mark.authors(["mikari"])
    def test_yt_file_update(self):
        file_path = f"{self.work_yt_path}/file"
        self.publish_blob_revision(file_path, "001", b"first")
        pipeline = self.prepare_pipeline(f"<cluster=primary>{file_path}")

        node_config, worker_overrides = self.make_node_config()
        with self.start_flow_process_federation(
            node_config=node_config,
            pipeline_binary_args={"--config": pipeline},
            worker_node_config_overrides=worker_overrides,
        ):
            self.write_input("before")
            self.wait_output("before", "first")
            self.publish_blob_revision(file_path, "002", b"second")
            self.wait_for_updated_output("updated-file", "second")
