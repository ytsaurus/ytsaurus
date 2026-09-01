import io
import os
import tarfile

import requests
import pytest
import yatest.common
import yt.yson as yson

from yt.common import wait

from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.queue import batching_write_rows

from .yt_sync import run_yt_sync

BLOB_PART_SIZE = 4 * 1024 * 1024
BLOB_TABLE_SCHEMA = yson.to_yson_type(
    [
        {"name": "filename", "type": "string", "sort_order": "ascending"},
        {"name": "part_index", "type": "int64", "sort_order": "ascending"},
        {"name": "data", "type": "string"},
    ],
    attributes={"strict": True, "unique_keys": True},
)


class TestFileResourceLifecycle(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(
        f"{yatest.common.context.project_path}/../file_resource_integration_test"
    )

    def setup_method(self, method):
        super().setup_method(method)
        self.input_queue = f"{self.work_yt_path}/input_queue"
        self.input_consumer = f"{self.work_yt_path}/consumer"
        self.output_queue = f"{self.work_yt_path}/output_queue"
        run_yt_sync("primary", self.work_yt_path)

    def prepare_pipeline(self, source_class_name, source_path, source_parameters=None):
        return self.prepare_named_pipeline({"file": (source_class_name, source_path, source_parameters or {})})

    def prepare_named_pipeline(self, file_sources):
        source = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline.yson")
        config = get_yson_config(source)
        computation = config["spec"]["computations"]["enricher"]
        computation["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.input_consumer}",
            }
        )
        computation["sinks"]["queue"]["parameters"]["queue_path"] = f"<cluster=primary>{self.output_queue}"
        configured_sources = {}
        for name, source in file_sources.items():
            source_class_name, source_path, *extra = source
            parameters = {"path": source_path}
            if extra:
                parameters.update(extra[0])
            configured_sources[name] = {
                "file_source_class_name": source_class_name,
                "parameters": parameters,
            }
        config["spec"]["resources"]["text"]["file_sources"] = configured_sources
        self.patch_config(config)
        return self.dump_config_to_log_dir(config, "pipeline.yson")

    def write_blob_table(self, path, files, part_size=BLOB_PART_SIZE):
        assert not self.client.exists(path)
        self.client.create(
            "table",
            path,
            attributes={"schema": BLOB_TABLE_SCHEMA},
        )
        rows = []
        for filename, data in sorted(files.items()):
            parts = [data[index : index + part_size] for index in range(0, len(data), part_size)] or [b""]
            rows.extend({"filename": filename, "part_index": index, "data": part} for index, part in enumerate(parts))
        self.client.write_table(path, rows)

    def write_blob_file(self, path, data, filename="file", part_size=BLOB_PART_SIZE):
        self.write_blob_table(path, {filename: data}, part_size=part_size)

    def publish_blob_revision(self, link_path, revision, files):
        revision_root = f"{link_path}-revisions"
        if not self.client.exists(revision_root):
            self.client.create("map_node", revision_root, recursive=True)
        table_path = f"{revision_root}/{revision}"
        self.write_blob_table(table_path, files)
        self.client.link(table_path, link_path, force=True)
        return table_path

    def write_cypress_file(self, path, data):
        if not self.client.exists(path):
            self.client.create("file", path)
        self.client.write_file(path, data)

    def make_node_config(
        self,
        soft_size_limit=48 * 1024 * 1024,
        hard_size_limit=64 * 1024 * 1024,
        workers_count=1,
    ):
        base_cache_path = os.path.abspath(os.path.join(self.path_to_flow_logs, "file-storage", "unused"))
        cache_paths = [
            os.path.abspath(os.path.join(self.path_to_flow_logs, "file-storage", f"worker-{index}"))
            for index in range(workers_count)
        ]
        for path in [base_cache_path, *cache_paths]:
            os.makedirs(path, exist_ok=True)
        worker_overrides = [{"worker": {"file_storage": {"path": path}}} for path in cache_paths]
        return (
            {
                "enable_porto_resource_tracker": False,
                "worker": {
                    "file_storage": {
                        "path": base_cache_path,
                        "soft_size_limit": soft_size_limit,
                        "hard_size_limit": hard_size_limit,
                        "cleanup_period": 100,
                    }
                },
            },
            cache_paths,
            worker_overrides,
        )

    def write_input(self, value):
        batching_write_rows(
            [{"text": value, "$tablet_index": 0}],
            lambda rows: self.client.insert_rows(self.input_queue, rows),
            1,
        )

    def find_output(self, input_value, file_text):
        rows = self.client.select_rows(
            f"input, file_text, resource_revision, file_snapshot_id from [{self.output_queue}]"
        )
        return next(
            (row for row in rows if row["input"] == input_value and row["file_text"] == file_text),
            None,
        )

    def wait_output(self, input_value, file_text):
        result = None

        def probe():
            nonlocal result
            result = self.find_output(input_value, file_text)
            return result is not None

        wait(probe, timeout=120, ignore_exceptions=True)
        return result

    def wait_for_updated_output(self, prefix, file_text):
        result = None
        counter = 0
        pending_value = None

        def probe():
            nonlocal counter, pending_value, result
            if pending_value is not None:
                result = self.find_output(pending_value, file_text)
                if result is not None:
                    return True

            pending_value = f"{prefix}-{counter}"
            counter += 1
            self.write_input(pending_value)
            return False

        wait(probe, timeout=120, ignore_exceptions=True, sleep_backoff=0.2)
        return result

    def resource_view(self):
        return self.client.get_flow_view(
            self.pipeline_path,
            view_path="/ephemeral_state/resource_controller_views/text",
            cache=False,
        )

    def wait_for_pipeline_description_error(self, error_substring):
        wait(
            lambda: error_substring
            in str(self.client.flow_execute(self.pipeline_path, flow_command="describe-pipeline")),
            timeout=60,
            ignore_exceptions=True,
        )

    @staticmethod
    def snapshot_metric_value(process, snapshot_id, state):
        response = requests.get(
            f"http://localhost:{process.monitoring_port}/solomon_proxy/sensors",
            timeout=10,
        )
        response.raise_for_status()
        sensors = response.json()["sensors"]
        for sensor in sensors:
            labels = sensor.get("labels", {})
            if (
                labels.get("sensor", "").endswith("file_snapshot_instance_count")
                and str(labels.get("file_snapshot_id")) == str(snapshot_id)
                and labels.get("state") == state
                and labels.get("resource") == "text"
            ):
                return sensor["value"]
        return None

    def active_snapshot_id(self, excluded_id=None):
        counts = self.snapshot_state_counts("active")
        active_ids = [snapshot_id for snapshot_id, count in counts.items() if count == 1]
        return next((snapshot_id for snapshot_id in active_ids if snapshot_id != excluded_id), None)

    def snapshot_state_counts(self, state):
        counts = self.resource_view().get("file_sources", {}).get("file_snapshot_state_counts", {})
        suffix = f"/{state}"
        return {int(key.split("/", 1)[0]): count for key, count in counts.items() if key.endswith(suffix)}

    @staticmethod
    def count_cached_objects(cache_path):
        return sum(1 for root, _, files in os.walk(cache_path) if "manifest.yson" in files and os.path.basename(root))

    @pytest.mark.authors(["mikari"])
    def test_yt_file_update_and_snapshot_metrics(self):
        file_path = f"{self.work_yt_path}/file"
        self.publish_blob_revision(file_path, "001", {"file": b"first"})
        pipeline = self.prepare_pipeline("NYT::NFlow::TYTFileSource", f"<cluster=primary>{file_path}")
        node_config, _, worker_overrides = self.make_node_config()

        with self.start_flow_process_federation(
            node_config=node_config,
            pipeline_binary_args={"--config": pipeline},
            worker_node_config_overrides=worker_overrides,
        ) as federation:
            self.write_input("before")
            first = self.wait_output("before", "first")
            wait(
                lambda: self.active_snapshot_id() is not None,
                timeout=120,
                ignore_exceptions=True,
            )
            first_snapshot_id = self.active_snapshot_id()
            wait(
                lambda: self.snapshot_metric_value(
                    federation.controllers[0],
                    first_snapshot_id,
                    "active",
                )
                == 1,
                timeout=120,
                ignore_exceptions=True,
            )

            self.publish_blob_revision(file_path, "002", {"file": b"second"})
            second = self.wait_for_updated_output("updated", "second")
            assert second["resource_revision"] > first["resource_revision"]
            wait(
                lambda: self.active_snapshot_id(first_snapshot_id) is not None,
                timeout=120,
                ignore_exceptions=True,
            )
            second_snapshot_id = self.active_snapshot_id(first_snapshot_id)
            wait(
                lambda: self.snapshot_metric_value(
                    federation.controllers[0],
                    second_snapshot_id,
                    "active",
                )
                == 1,
                timeout=120,
                ignore_exceptions=True,
            )

    @pytest.mark.authors(["mikari"])
    def test_cypress_file_update(self):
        file_path = f"{self.work_yt_path}/cypress-file"
        self.write_cypress_file(file_path, b"first")
        pipeline = self.prepare_pipeline("NYT::NFlow::TYTFileSource", f"<cluster=primary>{file_path}")
        node_config, _, worker_overrides = self.make_node_config()

        with self.start_flow_process_federation(
            node_config=node_config,
            pipeline_binary_args={"--config": pipeline},
            worker_node_config_overrides=worker_overrides,
        ):
            self.write_input("before")
            self.wait_output("before", "first")
            self.write_cypress_file(file_path, b"second")
            self.wait_for_updated_output("updated", "second")

    @pytest.mark.authors(["mikari"])
    def test_two_workers_report_independent_cache_and_rollout_state(self):
        file_path = f"{self.work_yt_path}/file"
        self.publish_blob_revision(file_path, "001", {"file": b"ok"})
        self.client.unmount_table(self.input_queue, sync=True)
        self.client.reshard_table(self.input_queue, tablet_count=2, sync=True)
        self.client.mount_table(self.input_queue, sync=True)
        pipeline = self.prepare_pipeline("NYT::NFlow::TYTFileSource", f"<cluster=primary>{file_path}")
        node_config, cache_paths, worker_overrides = self.make_node_config(workers_count=2)
        worker_overrides[1]["worker"]["file_storage"].update(
            {
                "soft_size_limit": 2,
                "hard_size_limit": 4,
            }
        )

        with self.start_flow_process_federation(
            node_config=node_config,
            workers_count=2,
            pipeline_binary_args={"--config": pipeline},
            worker_node_config_overrides=worker_overrides,
        ) as federation:
            wait(
                lambda: list(self.snapshot_state_counts("active").values()) == [2],
                timeout=120,
                ignore_exceptions=True,
            )
            first_snapshot_id = next(iter(self.snapshot_state_counts("active")))
            assert cache_paths[0] != cache_paths[1]
            for cache_path in cache_paths:
                wait(
                    lambda path=cache_path: self.count_cached_objects(path) == 1,
                    timeout=120,
                    ignore_exceptions=True,
                )
            wait(
                lambda: self.snapshot_metric_value(
                    federation.controllers[0],
                    first_snapshot_id,
                    "active",
                )
                == 2,
                timeout=120,
                ignore_exceptions=True,
            )

            self.publish_blob_revision(file_path, "002", {"file": b"too-large"})
            self.wait_for_pipeline_error("File storage hard size limit exceeded")

            def rollout_is_split():
                counts = self.snapshot_state_counts("active")
                return counts.get(first_snapshot_id) == 1 and any(
                    snapshot_id != first_snapshot_id and count == 1 for snapshot_id, count in counts.items()
                )

            wait(rollout_is_split, timeout=120, ignore_exceptions=True)
            active_counts = self.snapshot_state_counts("active")
            second_snapshot_id = next(snapshot_id for snapshot_id in active_counts if snapshot_id != first_snapshot_id)
            wait(
                lambda: self.snapshot_metric_value(
                    federation.controllers[0],
                    first_snapshot_id,
                    "active",
                )
                == 1
                and self.snapshot_metric_value(
                    federation.controllers[0],
                    second_snapshot_id,
                    "active",
                )
                == 1,
                timeout=120,
                ignore_exceptions=True,
            )

    @pytest.mark.authors(["mikari"])
    def test_two_named_yt_files_form_one_resource_snapshot(self):
        left_path = f"{self.work_yt_path}/left"
        right_path = f"{self.work_yt_path}/right"
        for path, value in ((left_path, b"left-v1"), (right_path, b"right-v1")):
            self.publish_blob_revision(path, "001", {"file": value})
        pipeline = self.prepare_named_pipeline(
            {
                "left": ("NYT::NFlow::TYTFileSource", f"<cluster=primary>{left_path}"),
                "right": ("NYT::NFlow::TYTFileSource", f"<cluster=primary>{right_path}"),
            }
        )
        node_config, _, worker_overrides = self.make_node_config()

        with self.start_flow_process_federation(
            node_config=node_config,
            pipeline_binary_args={"--config": pipeline},
            worker_node_config_overrides=worker_overrides,
        ):
            self.write_input("before")
            first = self.wait_output("before", "left-v1|right-v1")

            self.publish_blob_revision(right_path, "002", {"file": b"right-v2"})
            second = self.wait_for_updated_output("updated", "left-v1|right-v2")
            assert second["resource_revision"] > first["resource_revision"]

    @pytest.mark.authors(["mikari"])
    def test_large_yt_file_is_streamed_into_the_cache(self):
        payload_size = 64 * 1024 * 1024
        file_path = f"{self.work_yt_path}/large-file"
        self.write_blob_file(file_path, b"x" * payload_size)
        pipeline = self.prepare_pipeline("NYT::NFlow::TYTFileSource", f"<cluster=primary>{file_path}")
        node_config, _, worker_overrides = self.make_node_config(
            soft_size_limit=payload_size,
            hard_size_limit=payload_size + 16 * 1024 * 1024,
        )

        with self.start_flow_process_federation(
            node_config=node_config,
            pipeline_binary_args={"--config": pipeline},
            worker_node_config_overrides=worker_overrides,
        ):
            self.write_input("large")
            self.wait_output("large", f"size:{payload_size}")

    @pytest.mark.authors(["mikari"])
    def test_yt_file_source_materializes_all_blob_table_files(self):
        table_path = f"{self.work_yt_path}/files"
        self.write_blob_table(table_path, {"a": b"left", "b": b"right"})
        pipeline = self.prepare_pipeline(
            "NYT::NFlow::TYTFileSource",
            f"<cluster=primary>{table_path}",
        )
        node_config, _, worker_overrides = self.make_node_config()

        with self.start_flow_process_federation(
            node_config=node_config,
            pipeline_binary_args={"--config": pipeline},
            worker_node_config_overrides=worker_overrides,
        ):
            self.write_input("all")
            self.wait_output("all", "left|right")

    @pytest.mark.authors(["mikari"])
    def test_yt_directory_selects_greatest_blob_table(self):
        directory = f"{self.work_yt_path}/versions"
        self.client.create("map_node", directory)
        self.write_blob_table(f"{directory}/001", {"file": b"first"})
        pipeline = self.prepare_pipeline(
            "NYT::NFlow::TYTDirectoryLastFileSource",
            f"<cluster=primary>{directory}",
        )
        node_config, _, worker_overrides = self.make_node_config()

        with self.start_flow_process_federation(
            node_config=node_config,
            pipeline_binary_args={"--config": pipeline},
            worker_node_config_overrides=worker_overrides,
        ):
            self.write_input("before")
            self.wait_output("before", "first")
            self.write_blob_table(f"{directory}/002", {"file": b"second"})
            self.wait_for_updated_output("updated", "second")

            self.client.set_pipeline_dynamic_spec(
                self.pipeline_path,
                {"file": {"parameters": {"pinned_file_name": "001"}}},
                spec_path="/resources/text/file_sources",
            )
            self.wait_for_updated_output("pinned", "first")

    @pytest.mark.authors(["mikari"])
    def test_yt_file_cache_survives_restart_and_cleans_old_revision(self):
        file_path = f"{self.work_yt_path}/file"
        self.publish_blob_revision(file_path, "001", {"file": b"first"})
        pipeline = self.prepare_pipeline("NYT::NFlow::TYTFileSource", f"<cluster=primary>{file_path}")
        node_config, cache_paths, worker_overrides = self.make_node_config(soft_size_limit=6, hard_size_limit=32)

        with self.start_flow_process_federation(
            node_config=node_config,
            pipeline_binary_args={"--config": pipeline},
            worker_node_config_overrides=worker_overrides,
        ) as federation:
            self.write_input("before")
            self.wait_output("before", "first")

            self.client.remove(file_path)
            federation.controllers[0].restart()
            self.wait_pipeline_state("working")
            federation.workers[0].restart()
            self.write_input("after-restart")
            self.wait_output("after-restart", "first")

            self.publish_blob_revision(file_path, "002", {"file": b"second"})
            self.wait_for_updated_output("updated", "second")
            wait(
                lambda: self.count_cached_objects(cache_paths[0]) == 1,
                timeout=120,
                ignore_exceptions=True,
            )

    @pytest.mark.authors(["mikari"])
    def test_local_file_download_error_is_reported_and_retried(self):
        local_path = os.path.abspath(os.path.join(self.path_to_flow_logs, "missing-local-file"))
        pipeline = self.prepare_pipeline("NYT::NFlow::TLocalFileSource", local_path)
        node_config, _, worker_overrides = self.make_node_config()

        with self.start_flow_process_federation(
            node_config=node_config,
            pipeline_binary_args={"--config": pipeline},
            worker_node_config_overrides=worker_overrides,
        ):
            self.wait_for_pipeline_error("is not a regular file")
            with open(local_path, "w") as output:
                output.write("recovered-download")
            self.wait_for_updated_output("download-recovered", "recovered-download")

    @pytest.mark.authors(["mikari"])
    def test_missing_and_corrupt_yt_file_report_errors_and_recover(self):
        file_path = f"{self.work_yt_path}/missing"
        pipeline = self.prepare_pipeline("NYT::NFlow::TYTFileSource", f"<cluster=primary>{file_path}")
        node_config, _, worker_overrides = self.make_node_config()

        with self.start_flow_process_federation(
            node_config=node_config,
            pipeline_binary_args={"--config": pipeline},
            worker_node_config_overrides=worker_overrides,
        ):
            self.wait_for_pipeline_description_error("File source discovery failed")

            self.publish_blob_revision(file_path, "001", {"file": b"valid"})
            valid = self.wait_for_updated_output("valid", "valid")

            self.publish_blob_revision(file_path, "002", {"file": b"corrupt"})
            self.wait_for_pipeline_error("Test file resource rejected corrupt payload")
            self.write_input("during-corruption")
            still_valid = self.wait_output("during-corruption", "valid")
            assert still_valid["file_snapshot_id"] == valid["file_snapshot_id"]

            self.publish_blob_revision(file_path, "003", {"file": b"recovered"})
            recovered = self.wait_for_updated_output("recovered", "recovered")
            assert recovered["resource_revision"] > valid["resource_revision"]
            assert recovered["file_snapshot_id"] != valid["file_snapshot_id"]

    @pytest.mark.authors(["mikari"])
    def test_capacity_error_keeps_previous_revision(self):
        file_path = f"{self.work_yt_path}/file"
        self.publish_blob_revision(file_path, "001", {"file": b"ok"})
        pipeline = self.prepare_pipeline("NYT::NFlow::TYTFileSource", f"<cluster=primary>{file_path}")
        node_config, _, worker_overrides = self.make_node_config(soft_size_limit=4, hard_size_limit=8)

        with self.start_flow_process_federation(
            node_config=node_config,
            pipeline_binary_args={"--config": pipeline},
            worker_node_config_overrides=worker_overrides,
        ):
            self.write_input("before")
            valid = self.wait_output("before", "ok")

            self.publish_blob_revision(file_path, "002", {"file": b"too-large"})
            self.wait_for_pipeline_error("File storage hard size limit exceeded")
            self.write_input("after")
            preserved = self.wait_output("after", "ok")
            assert preserved["file_snapshot_id"] == valid["file_snapshot_id"]

    @pytest.mark.authors(["mikari"])
    def test_archive_with_two_files(self):
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w") as output:
            for name, value in (("a.txt", b"left"), ("b.txt", b"right")):
                info = tarfile.TarInfo(name)
                info.size = len(value)
                output.addfile(info, io.BytesIO(value))

        file_path = f"{self.work_yt_path}/files.tar"
        self.write_blob_file(file_path, archive.getvalue(), filename="files.tar")
        pipeline = self.prepare_pipeline("NYT::NFlow::TYTFileSource", f"<cluster=primary>{file_path}")
        node_config, _, worker_overrides = self.make_node_config()

        with self.start_flow_process_federation(
            node_config=node_config,
            pipeline_binary_args={"--config": pipeline},
            worker_node_config_overrides=worker_overrides,
        ):
            self.write_input("archive")
            self.wait_output("archive", "left|right")
