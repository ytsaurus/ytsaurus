import io
import os
import shutil
import tarfile
import time

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

    def prepare_pipeline(
        self,
        provider_class_name,
        provider_path,
        provider_parameters=None,
        provider_spec=None,
    ):
        return self.prepare_named_pipeline(
            {
                "file": (
                    provider_class_name,
                    provider_path,
                    provider_parameters or {},
                    provider_spec or {},
                )
            }
        )

    def prepare_named_pipeline(self, file_providers):
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
        configured_providers = {}
        for name, provider in file_providers.items():
            provider_class_name, provider_path, *extra = provider
            parameters = {"path": provider_path}
            if extra:
                parameters.update(extra[0])
            configured_provider = {
                "file_provider_class_name": provider_class_name,
                "parameters": parameters,
            }
            if len(extra) > 1:
                configured_provider.update(extra[1])
            configured_providers[name] = configured_provider
        config["spec"]["resources"]["text"]["file_providers"] = configured_providers
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
        with self.client.Transaction():
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
        counts = self.resource_view().get("file_providers", {}).get("file_snapshot_state_counts", {})
        suffix = f"/{state}"
        return {int(key.split("/", 1)[0]): count for key, count in counts.items() if key.endswith(suffix)}

    @staticmethod
    def count_cached_objects(cache_path):
        return sum(1 for root, _, files in os.walk(cache_path) if "manifest.yson" in files and os.path.basename(root))

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize("directory_last,dynamic_pin", [(False, False), (True, False), (True, True)])
    @pytest.mark.parametrize(
        "pause,restart_controller,restart_workers",
        [(False, False, False), (False, True, False), (False, True, True), (True, False, False)],
    )
    def test_spec_change_requires_new_discovery(
        self, directory_last, restart_controller, restart_workers, dynamic_pin, pause
    ):
        old_directory = f"{self.work_yt_path}/old"
        new_directory = old_directory if dynamic_pin else f"{self.work_yt_path}/new"
        new_name = "pinned" if dynamic_pin else "file"
        new_file = f"{new_directory}/{new_name}"
        self.client.create("map_node", old_directory)
        self.write_cypress_file(f"{old_directory}/file", b"first")
        provider_class = "TYTDirectoryLastFileProvider" if directory_last else "TYTFileProvider"
        old_path = old_directory if directory_last else f"{old_directory}/file"
        new_path = new_directory if directory_last else new_file
        pipeline = self.prepare_pipeline(f"NYT::NFlow::{provider_class}", f"<cluster=primary>{old_path}")
        config = get_yson_config(pipeline)
        config["spec"]["resources"]["text"]["always_on"] = True
        pipeline = self.dump_config_to_log_dir(config, "restart-pipeline.yson")
        node_config, cache_paths, worker_overrides = self.make_node_config(workers_count=2)

        def wait_active():
            wait(
                lambda: list(self.snapshot_state_counts("active").values()) == [2],
                timeout=120,
                ignore_exceptions=True,
            )

        def cached_payloads():
            result = {}
            for cache_path in cache_paths:
                for directory, _, files in os.walk(cache_path):
                    if "payload" not in directory.split(os.sep):
                        continue
                    for name in files:
                        path = os.path.join(directory, name)
                        stat = os.stat(path)
                        result[path] = (stat.st_ino, stat.st_mtime_ns, stat.st_size)
            assert result
            return result

        with self.start_flow_process_federation(
            node_config=node_config,
            workers_count=2,
            worker_node_config_overrides=worker_overrides,
            pipeline_binary_args={"--config": pipeline},
        ) as federation:
            self.write_input("before")
            self.wait_output("before", "first")
            wait_active()
            stop = self.client.pause_pipeline if pause else self.client.stop_pipeline
            stop(self.pipeline_path)
            stopped_state = "paused" if pause else "stopped"
            self.wait_pipeline_state(stopped_state)
            if dynamic_pin:
                parameters = {"pinned_file_name": new_name}
                self.client.set_pipeline_dynamic_spec(
                    self.pipeline_path,
                    {"file": {"parameters": parameters}},
                    spec_path="/resources/text/file_providers",
                )
            else:
                current = self.client.get_pipeline_spec(self.pipeline_path)
                current["spec"]["resources"]["text"]["file_providers"]["file"]["parameters"][
                    "path"
                ] = f"<cluster=primary>{new_path}"
                self.client.set_pipeline_spec(
                    self.pipeline_path, current["spec"], expected_version=current["version"], force=pause
                )
            if restart_controller:
                federation.controllers[0].restart()
                self.wait_pipeline_state(stopped_state)
            if restart_workers:
                for worker in federation.workers:
                    worker.stop()
                for directory, _, _ in os.walk(cache_paths[1]):
                    os.chmod(directory, 0o700)
                shutil.rmtree(cache_paths[1])
                for worker in federation.workers:
                    worker.start()
            self.write_input("after-spec-change")
            self.client.start_pipeline(self.pipeline_path)
            self.wait_for_pipeline_description_error("File provider discovery failed")

            def snapshots_reset():
                view = self.resource_view()["file_providers"]
                return (dynamic_pin or view.get("active_file_snapshot_id") is None) and view.get(
                    "preparing_file_snapshot_id"
                ) is None

            # Discovery errors are visible before the corresponding flow view is committed.
            wait(snapshots_reset, timeout=60, ignore_exceptions=True)
            time.sleep(2)
            assert self.client.get_pipeline_state(self.pipeline_path) == "working"
            layout = self.client.get_flow_view(
                self.pipeline_path, view_path="/state/execution_spec/layout", cache=False
            )
            assert layout.get("resource_instances")
            assert not self.client.get_flow_view(
                self.pipeline_path, view_path="/state/execution_spec/resource_target_revisions/value", cache=False
            )
            assert not list(self.client.select_rows(f"* from [{self.output_queue}] where input = 'after-spec-change'"))

            if not self.client.exists(new_directory):
                self.client.create("map_node", new_directory)
            self.write_cypress_file(new_file, b"corrupt")
            self.wait_for_pipeline_description_error("Test file resource rejected corrupt payload")
            assert not list(self.client.select_rows(f"* from [{self.output_queue}] where input = 'after-spec-change'"))
            self.write_cypress_file(new_file, b"second")
            self.wait_output("after-spec-change", "second")
            wait_active()
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped")
            rows = list(self.client.select_rows(f"* from [{self.output_queue}] where input = 'after-spec-change'"))
            assert rows and all(row["file_text"] == "second" for row in rows)
            cached_before = cached_payloads()

            current = self.client.get_pipeline_spec(self.pipeline_path)
            current["spec"]["resources"]["text"]["always_on"] = False
            self.client.set_pipeline_spec(self.pipeline_path, current["spec"], expected_version=current["version"])
            self.client.start_pipeline(self.pipeline_path)
            self.write_input("reused")
            self.wait_output("reused", "second")
            wait(
                lambda: list(self.snapshot_state_counts("active").values()) == [1],
                timeout=120,
                ignore_exceptions=True,
            )
            assert cached_payloads() == cached_before

    @pytest.mark.authors(["mikari"])
    def test_yt_file_update_and_snapshot_metrics(self):
        file_path = f"{self.work_yt_path}/file"
        self.publish_blob_revision(file_path, "001", {"file": b"first"})
        pipeline = self.prepare_pipeline("NYT::NFlow::TYTFileProvider", f"<cluster=primary>{file_path}")
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
        pipeline = self.prepare_pipeline("NYT::NFlow::TYTFileProvider", f"<cluster=primary>{file_path}")
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
    @pytest.mark.parametrize("directory_last", [False, True])
    @pytest.mark.parametrize("object_type", ["file", "table"])
    def test_selected_file_reload_without_job_restart(self, directory_last, object_type):
        directory = f"{self.work_yt_path}/versions"
        self.client.create("map_node", directory)
        file_path = f"{directory}/001"

        def write_payload(payload):
            if object_type == "file":
                self.write_cypress_file(file_path, payload)
            elif self.client.exists(file_path):
                self.client.write_table(file_path, [{"filename": "file", "part_index": 0, "data": payload}])
            else:
                self.write_blob_file(file_path, payload)

        write_payload(b"first")
        provider_class = "TYTDirectoryLastFileProvider" if directory_last else "TYTFileProvider"
        pipeline = self.prepare_pipeline(
            f"NYT::NFlow::{provider_class}",
            f"<cluster=primary>{directory if directory_last else file_path}",
        )
        config = get_yson_config(pipeline)
        config["spec"]["resources"]["text"]["always_on"] = True
        pipeline = self.dump_config_to_log_dir(config, "reload-pipeline.yson")
        node_config, cache_paths, worker_overrides = self.make_node_config(workers_count=2)

        def wait_for_reload(previous_id=None):
            result = None

            def converged():
                nonlocal result
                counts = self.snapshot_state_counts("active")
                if len(counts) != 1:
                    return False
                snapshot_id, count = next(iter(counts.items()))
                if count != 2 or snapshot_id == previous_id:
                    return False
                result = snapshot_id
                return True

            wait(converged, timeout=120, ignore_exceptions=True)
            return result

        def job_ids():
            return set(
                self.client.get_flow_view(
                    self.pipeline_path, view_path="/state/execution_spec/layout/jobs", cache=False
                )
            )

        with self.start_flow_process_federation(
            node_config=node_config,
            workers_count=2,
            pipeline_binary_args={"--config": pipeline},
            worker_node_config_overrides=worker_overrides,
        ):
            self.write_input("before")
            self.wait_output("before", "first")
            first_id = wait_for_reload()
            initial_jobs = job_ids()
            assert initial_jobs
            cached_before = [self.count_cached_objects(path) for path in cache_paths]

            content_revision = self.client.get(f"{file_path}/@content_revision")
            self.client.set(f"{file_path}/@reload_test", 1)
            assert self.client.get(f"{file_path}/@content_revision") == content_revision
            time.sleep(2)
            assert self.snapshot_state_counts("active") == {first_id: 2}
            assert [self.count_cached_objects(path) for path in cache_paths] == cached_before

            write_payload(b"second")
            second_id = wait_for_reload(first_id)
            assert all(self.count_cached_objects(path) > count for path, count in zip(cache_paths, cached_before))
            self.write_input("after-reload")
            row = self.wait_output("after-reload", "second")
            assert row["file_snapshot_id"] == second_id

            old_object_id = self.client.get(f"{file_path}/@id")
            cached_before_replace = [self.count_cached_objects(path) for path in cache_paths]
            with self.client.Transaction():
                self.client.remove(file_path)
                write_payload(b"replacement")
            assert self.client.get(f"{file_path}/@id") != old_object_id
            replacement_id = wait_for_reload(second_id)
            assert all(
                self.count_cached_objects(path) > count for path, count in zip(cache_paths, cached_before_replace)
            )
            self.write_input("after-replace")
            row = self.wait_output("after-replace", "replacement")
            assert row["file_snapshot_id"] == replacement_id

            write_payload(b"corrupt")
            self.wait_for_pipeline_error("Test file resource rejected corrupt payload")
            self.write_input("failed-reload")
            self.wait_output("failed-reload", "replacement")

            write_payload(b"third")
            third_id = wait_for_reload(replacement_id)
            self.write_input("recovered")
            row = self.wait_output("recovered", "third")
            assert row["file_snapshot_id"] == third_id
            assert job_ids() == initial_jobs

    @pytest.mark.authors(["mikari"])
    def test_two_workers_report_independent_cache_and_rollout_state(self):
        file_path = f"{self.work_yt_path}/file"
        self.publish_blob_revision(file_path, "001", {"file": b"ok"})
        self.client.unmount_table(self.input_queue, sync=True)
        self.client.reshard_table(self.input_queue, tablet_count=2, sync=True)
        self.client.mount_table(self.input_queue, sync=True)
        pipeline = self.prepare_pipeline("NYT::NFlow::TYTFileProvider", f"<cluster=primary>{file_path}")
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
                "left": ("NYT::NFlow::TYTFileProvider", f"<cluster=primary>{left_path}"),
                "right": ("NYT::NFlow::TYTFileProvider", f"<cluster=primary>{right_path}"),
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
        pipeline = self.prepare_pipeline("NYT::NFlow::TYTFileProvider", f"<cluster=primary>{file_path}")
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
    def test_yt_file_provider_materializes_all_blob_table_files(self):
        table_path = f"{self.work_yt_path}/files"
        self.write_blob_table(table_path, {"a": b"left", "b": b"right"})
        pipeline = self.prepare_pipeline(
            "NYT::NFlow::TYTFileProvider",
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
            "NYT::NFlow::TYTDirectoryLastFileProvider",
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
            jobs_path = "/state/execution_spec/layout/jobs"
            original_jobs = set(self.client.get_flow_view(self.pipeline_path, view_path=jobs_path, cache=False))
            assert original_jobs
            self.write_blob_table(f"{directory}/002", {"file": b"second"})
            self.wait_for_updated_output("updated", "second")

            self.client.set_pipeline_dynamic_spec(
                self.pipeline_path,
                {"file": {"parameters": {"pinned_file_name": "001"}}},
                spec_path="/resources/text/file_providers",
            )
            self.wait_for_updated_output("pinned", "first")

            self.write_blob_table(f"{directory}/003", {"file": b"corrupt"})
            self.client.set_pipeline_dynamic_spec(
                self.pipeline_path,
                {"file": {"parameters": {"pinned_file_name": "003"}}},
                spec_path="/resources/text/file_providers",
            )
            self.wait_for_pipeline_description_error("Test file resource rejected corrupt payload")
            self.write_input("failed-pin")
            self.wait_output("failed-pin", "first")
            assert set(self.client.get_flow_view(self.pipeline_path, view_path=jobs_path, cache=False)) == original_jobs

            self.client.write_table(f"{directory}/003", [{"filename": "file", "part_index": 0, "data": b"third"}])
            self.wait_for_updated_output("repaired-pin", "third")
            self.client.set_pipeline_dynamic_spec(
                self.pipeline_path,
                {"file": {"parameters": {}}},
                spec_path="/resources/text/file_providers",
            )
            self.write_blob_table(f"{directory}/004", {"file": b"fourth"})
            self.wait_for_updated_output("unpinned", "fourth")
            assert set(self.client.get_flow_view(self.pipeline_path, view_path=jobs_path, cache=False)) == original_jobs

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize("discovered_before_stop", [False, True])
    def test_pin_before_stop_blocks_old_resource_on_restart(self, discovered_before_stop):
        directory = f"{self.work_yt_path}/versions"
        pinned_file = f"{directory}/001"
        self.client.create("map_node", directory)
        self.write_cypress_file(f"{directory}/002", b"old")
        pipeline = self.prepare_pipeline("NYT::NFlow::TYTDirectoryLastFileProvider", f"<cluster=primary>{directory}")
        config = get_yson_config(pipeline)
        config["dynamic_spec"]["controller_connector"].update(
            {"controller_wait_timeout": "10s", "controller_heartbeat_rpc_timeout": "10s"}
        )
        pipeline = self.dump_config_to_log_dir(config, "pipeline.yson")
        node_config, _, worker_overrides = self.make_node_config(workers_count=2)
        with self.start_flow_process_federation(
            node_config=node_config,
            workers_count=2,
            worker_node_config_overrides=worker_overrides,
            pipeline_binary_args={"--config": pipeline},
        ):
            self.write_input("before-pin")
            self.wait_output("before-pin", "old")
            if discovered_before_stop:
                self.write_cypress_file(pinned_file, b"corrupt")
            self.client.set_pipeline_dynamic_spec(
                self.pipeline_path,
                {"file": {"parameters": {"pinned_file_name": "001"}}},
                spec_path="/resources/text/file_providers",
            )
            error = (
                "Test file resource rejected corrupt payload"
                if discovered_before_stop
                else "File provider discovery failed"
            )
            self.wait_for_pipeline_description_error(error)
            self.write_input("while-pinned-unavailable")
            self.wait_output("while-pinned-unavailable", "old")
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped")
            layout = self.client.get_flow_view(
                self.pipeline_path, view_path="/state/execution_spec/layout", cache=False
            )
            assert not layout.get("resource_instances")
            assert not self.client.get_flow_view(
                self.pipeline_path, view_path="/state/execution_spec/resource_target_revisions/value", cache=False
            )

            self.write_input("after-restart")
            self.client.start_pipeline(self.pipeline_path)
            self.wait_for_pipeline_description_error(error)
            self.wait_pipeline_state("working")
            time.sleep(2)
            if not discovered_before_stop:
                layout = self.client.get_flow_view(
                    self.pipeline_path, view_path="/state/execution_spec/layout", cache=False
                )
                assert layout.get("resource_instances")
                assert not self.client.get_flow_view(
                    self.pipeline_path, view_path="/state/execution_spec/resource_target_revisions/value", cache=False
                )

            def rows():
                return list(self.client.select_rows(f"* from [{self.output_queue}] where input = 'after-restart'"))

            assert not rows()
            self.write_cypress_file(pinned_file, b"pinned")
            self.wait_output("after-restart", "pinned")
            assert all(row["file_text"] == "pinned" for row in rows())

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize(
        "preload_required,corrupt_replacement,restart_controller",
        [(False, False, False), (False, True, True), (True, True, False)],
    )
    def test_restart_recovers_from_deleted_active_file(self, preload_required, corrupt_replacement, restart_controller):
        directory = f"{self.work_yt_path}/versions"
        old_file = f"{directory}/001"
        new_file = f"{directory}/002"
        self.client.create("map_node", directory)
        self.write_cypress_file(old_file, b"first")
        old_object_id = self.client.get(f"{old_file}/@id")
        pipeline = self.prepare_pipeline("NYT::NFlow::TYTDirectoryLastFileProvider", f"<cluster=primary>{directory}")
        config = get_yson_config(pipeline)
        config["spec"]["resources"]["text"]["preload_required"] = preload_required
        if preload_required:
            config["dynamic_spec"]["job_manager"] = {"balancer_type": "resource_queue", "async_balancing": False}
        pipeline = self.dump_config_to_log_dir(config, "pipeline.yson")
        node_config, cache_paths, worker_overrides = self.make_node_config()

        def instance_status():
            view = self.client.get_flow_view(self.pipeline_path, cache=False)
            instances = view["state"]["execution_spec"]["layout"]["resource_instances"]
            assert len(instances) == 1
            instance_id, instance = next(iter(instances.items()))
            status = view["feedback"]["worker_statuses"][instance["worker_address"]]["resource_instance_statuses"][
                instance_id
            ]
            return instance_id, status

        with self.start_flow_process_federation(
            node_config=node_config,
            worker_node_config_overrides=worker_overrides,
            pipeline_binary_args={"--config": pipeline},
        ) as federation:
            self.write_input("before-stop")
            before = self.wait_output("before-stop", "first")
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state("stopped")
            federation.workers[0].stop()
            for path, _, _ in os.walk(cache_paths[0]):
                os.chmod(path, 0o700)
            shutil.rmtree(cache_paths[0])
            self.client.remove(old_file)
            wait(lambda: not self.client.exists(f"#{old_object_id}"), timeout=60)
            self.write_cypress_file(new_file, b"corrupt" if corrupt_replacement else b"second")
            if restart_controller:
                federation.controllers[0].restart()
                self.wait_pipeline_state("stopped")
            federation.workers[0].start()
            self.write_input("after-restart")
            self.client.start_pipeline(self.pipeline_path)
            self.wait_pipeline_state("working")

            blocked_instance_id = None
            if corrupt_replacement:
                self.wait_for_pipeline_description_error("Test file resource rejected corrupt payload")
                blocked_instance_id, status = instance_status()
                assert status["load_state"] == "loading"
                assert status.get("active_file_snapshot_id") is None
                assert not status.get("load_error", {}).get("code", 0)
                target = self.client.get_flow_view(
                    self.pipeline_path,
                    view_path="/state/execution_spec/resource_target_revisions/value/text",
                    cache=False,
                )
                assert target["active_file_snapshot"]["id"] == before["file_snapshot_id"]
                assert target["preparing_file_snapshot"]["id"] != before["file_snapshot_id"]
                job_statuses = self.client.get_flow_view(
                    self.pipeline_path, view_path="/feedback/partition_job_statuses", cache=False
                )
                assert all(
                    status.get("current_job_status", {}).get("inited_time") is None for status in job_statuses.values()
                )
                assert not list(self.client.select_rows(f"* from [{self.output_queue}] where input = 'after-restart'"))
                self.write_cypress_file(new_file, b"second")

            after = self.wait_output("after-restart", "second")
            assert after["file_snapshot_id"] != before["file_snapshot_id"]
            wait(
                lambda: instance_status()[1]["load_state"] == "loaded"
                and instance_status()[1].get("active_file_snapshot_id") == after["file_snapshot_id"],
                timeout=60,
                ignore_exceptions=True,
            )
            if blocked_instance_id is not None:
                assert instance_status()[0] == blocked_instance_id
            rows = list(self.client.select_rows(f"* from [{self.output_queue}] where input = 'after-restart'"))
            assert rows and all(row["file_text"] == "second" for row in rows)

    @pytest.mark.authors(["mikari"])
    def test_yt_file_cache_survives_restart_and_cleans_old_revision(self):
        file_path = f"{self.work_yt_path}/file"
        self.publish_blob_revision(file_path, "001", {"file": b"first"})
        pipeline = self.prepare_pipeline("NYT::NFlow::TYTFileProvider", f"<cluster=primary>{file_path}")
        node_config, cache_paths, worker_overrides = self.make_node_config(soft_size_limit=6, hard_size_limit=32)

        with self.start_flow_process_federation(
            node_config=node_config,
            pipeline_binary_args={"--config": pipeline},
            worker_node_config_overrides=worker_overrides,
        ) as federation:
            self.write_input("before")
            self.wait_output("before", "first")

            cached_files = {
                os.path.join(directory, name): os.stat(os.path.join(directory, name))
                for directory, _, files in os.walk(cache_paths[0])
                if "payload" in directory.split(os.sep)
                for name in files
            }
            assert cached_files
            federation.controllers[0].restart()
            self.wait_pipeline_state("working")
            federation.workers[0].restart()
            self.write_input("after-restart")
            self.wait_output("after-restart", "first")
            for path, before in cached_files.items():
                after = os.stat(path)
                assert (after.st_ino, after.st_mtime_ns) == (before.st_ino, before.st_mtime_ns)

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
        pipeline = self.prepare_pipeline("NYT::NFlow::TLocalFileProvider", local_path)
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
        pipeline = self.prepare_pipeline("NYT::NFlow::TYTFileProvider", f"<cluster=primary>{file_path}")
        node_config, _, worker_overrides = self.make_node_config()

        with self.start_flow_process_federation(
            node_config=node_config,
            pipeline_binary_args={"--config": pipeline},
            worker_node_config_overrides=worker_overrides,
        ):
            self.wait_for_pipeline_description_error("File provider discovery failed")

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
        pipeline = self.prepare_pipeline("NYT::NFlow::TYTFileProvider", f"<cluster=primary>{file_path}")
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
    def test_archive_postprocessing_with_two_files(self):
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w") as output:
            for name, value in (("a.txt", b"left"), ("b.txt", b"right")):
                info = tarfile.TarInfo(name)
                info.size = len(value)
                output.addfile(info, io.BytesIO(value))

        file_path = f"{self.work_yt_path}/files.tar"
        self.write_blob_file(file_path, archive.getvalue(), filename="files.tar")
        postprocess_command = '/usr/bin/tar -xf "$YT_FLOW_RESOURCE_PATH/files.tar" -C "$YT_FLOW_POSTPROCESSING_PATH"'
        pipeline = self.prepare_pipeline(
            "NYT::NFlow::TYTFileProvider",
            f"<cluster=primary>{file_path}",
            provider_spec={"postprocess_command": postprocess_command},
        )
        node_config, _, worker_overrides = self.make_node_config()

        with self.start_flow_process_federation(
            node_config=node_config,
            pipeline_binary_args={"--config": pipeline},
            worker_node_config_overrides=worker_overrides,
        ):
            self.write_input("archive")
            self.wait_output("archive", "left|right")
