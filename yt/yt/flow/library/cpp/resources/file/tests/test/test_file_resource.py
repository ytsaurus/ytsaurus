import io
import os
import tarfile
import time

import requests
import pytest
import yatest.common

from yt.common import wait

from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.queue import batching_write_rows

from .yt_sync import run_yt_sync


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

    def prepare_pipeline(self, source_class_name, source_path):
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
        source_spec = config["spec"]["resources"]["text"]["parameters"]["file_source"]
        source_spec["file_source_class_name"] = source_class_name
        source_spec["parameters"] = {"path": source_path}
        self.patch_config(config)
        return self.dump_config_to_log_dir(config, "pipeline.yson")

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
                "worker": {
                    "file_storage": {
                        "path": base_cache_path,
                        "soft_size_limit": soft_size_limit,
                        "hard_size_limit": hard_size_limit,
                        "cleanup_period": 100,
                    }
                }
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
        rows = self.client.select_rows(f"input, file_text, resource_revision from [{self.output_queue}]")
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

        def probe():
            nonlocal counter, result
            value = f"{prefix}-{counter}"
            counter += 1
            self.write_input(value)
            time.sleep(0.2)
            result = self.find_output(value, file_text)
            return result is not None

        wait(probe, timeout=120, ignore_exceptions=True)
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
    def revision_metric_values(process, revision):
        response = requests.get(
            f"http://localhost:{process.monitoring_port}/solomon_proxy/sensors",
            timeout=10,
        )
        response.raise_for_status()
        sensors = response.json()["sensors"]
        result = {}
        for sensor in sensors:
            labels = sensor.get("labels", {})
            if labels.get("sensor", "").endswith("revision_instance_count") and str(labels.get("revision_id")) == str(
                revision
            ):
                if labels.get("resource") != "text" or labels.get("kind") not in ("applied", "target"):
                    continue
                result[labels.get("kind")] = sensor["value"]
        return result

    def revision_is_fully_applied(self, revision):
        counts = self.resource_view().get("revision_instance_counts", {})
        return counts.get(f"{revision}/applied") == 1 and counts.get(f"{revision}/target") == 1

    @staticmethod
    def count_cached_objects(cache_path):
        return sum(1 for root, _, files in os.walk(cache_path) if "manifest.yson" in files and os.path.basename(root))

    @pytest.mark.authors(["mikari"])
    def test_yt_file_update_and_revision_metrics(self):
        file_path = f"{self.work_yt_path}/file"
        self.client.create("file", file_path)
        self.client.write_file(file_path, b"first")
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
                lambda: self.revision_is_fully_applied(first["resource_revision"]),
                timeout=120,
                ignore_exceptions=True,
            )
            wait(
                lambda: self.revision_metric_values(federation.controllers[0], first["resource_revision"])
                == {"applied": 1, "target": 1},
                timeout=120,
                ignore_exceptions=True,
            )

            self.client.write_file(file_path, b"second")
            second = self.wait_for_updated_output("updated", "second")
            assert second["resource_revision"] > first["resource_revision"]
            wait(
                lambda: self.revision_is_fully_applied(second["resource_revision"]),
                timeout=120,
                ignore_exceptions=True,
            )
            wait(
                lambda: self.revision_metric_values(federation.controllers[0], second["resource_revision"])
                == {"applied": 1, "target": 1},
                timeout=120,
                ignore_exceptions=True,
            )

    @pytest.mark.authors(["mikari"])
    def test_large_yt_file_is_streamed_into_the_cache(self):
        payload_size = 64 * 1024 * 1024
        file_path = f"{self.work_yt_path}/large-file"
        self.client.create("file", file_path)
        self.client.write_file(file_path, b"x" * payload_size)
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
    def test_yt_directory_selects_greatest_direct_file(self):
        directory = f"{self.work_yt_path}/versions"
        self.client.create("map_node", directory)
        self.client.create("file", f"{directory}/001")
        self.client.write_file(f"{directory}/001", b"first")
        self.client.create("map_node", f"{directory}/zzz")
        self.client.create("file", f"{directory}/zzz/nested")
        self.client.write_file(f"{directory}/zzz/nested", b"nested")
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
            self.client.create("file", f"{directory}/002")
            self.client.write_file(f"{directory}/002", b"second")
            self.wait_for_updated_output("updated", "second")

    @pytest.mark.authors(["mikari"])
    def test_yt_file_cache_survives_restart_and_cleans_old_revision(self):
        file_path = f"{self.work_yt_path}/file"
        self.client.create("file", file_path)
        self.client.write_file(file_path, b"first")
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

            self.client.create("file", file_path)
            self.client.write_file(file_path, b"second")
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

            self.client.create("file", file_path)
            self.client.write_file(file_path, b"valid")
            valid = self.wait_for_updated_output("valid", "valid")

            self.client.write_file(file_path, b"corrupt")
            self.wait_for_pipeline_error("Test file resource rejected corrupt payload")
            self.write_input("during-corruption")
            still_valid = self.wait_output("during-corruption", "valid")
            assert still_valid["resource_revision"] == valid["resource_revision"]

            self.client.write_file(file_path, b"recovered")
            recovered = self.wait_for_updated_output("recovered", "recovered")
            assert recovered["resource_revision"] > valid["resource_revision"]

    @pytest.mark.authors(["mikari"])
    def test_capacity_error_keeps_previous_revision(self):
        file_path = f"{self.work_yt_path}/file"
        self.client.create("file", file_path)
        self.client.write_file(file_path, b"ok")
        pipeline = self.prepare_pipeline("NYT::NFlow::TYTFileSource", f"<cluster=primary>{file_path}")
        node_config, _, worker_overrides = self.make_node_config(soft_size_limit=4, hard_size_limit=8)

        with self.start_flow_process_federation(
            node_config=node_config,
            pipeline_binary_args={"--config": pipeline},
            worker_node_config_overrides=worker_overrides,
        ):
            self.write_input("before")
            valid = self.wait_output("before", "ok")

            self.client.write_file(file_path, b"too-large")
            self.wait_for_pipeline_error("File storage hard size limit exceeded")
            self.write_input("after")
            preserved = self.wait_output("after", "ok")
            assert preserved["resource_revision"] == valid["resource_revision"]

    @pytest.mark.authors(["mikari"])
    def test_archive_with_two_files(self):
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w") as output:
            for name, value in (("a.txt", b"left"), ("b.txt", b"right")):
                info = tarfile.TarInfo(name)
                info.size = len(value)
                output.addfile(info, io.BytesIO(value))

        file_path = f"{self.work_yt_path}/files.tar"
        self.client.create("file", file_path)
        self.client.write_file(file_path, archive.getvalue())
        pipeline = self.prepare_pipeline("NYT::NFlow::TYTFileSource", f"<cluster=primary>{file_path}")
        node_config, _, worker_overrides = self.make_node_config()

        with self.start_flow_process_federation(
            node_config=node_config,
            pipeline_binary_args={"--config": pipeline},
            worker_node_config_overrides=worker_overrides,
        ):
            self.write_input("archive")
            self.wait_output("archive", "left|right")
