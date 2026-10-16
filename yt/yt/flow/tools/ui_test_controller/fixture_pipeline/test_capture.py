import getpass
import hashlib
import io
import os
import pathlib
import re
import time
from collections import Counter

import pytest
import yatest.common

from yt.wrapper import yson

from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync
from yt.yt.flow.library.python.queue import batching_write_rows

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")
FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

CAPTURE_SOURCE_FILES = (
    "test_capture.py",
    "lib/computation.cpp",
    "lib/computation.h",
    "pipeline/pipeline.yson",
)

SENSITIVE_FIELD_PARTS = {
    "authorization",
    "cookie",
    "credential",
    "credentials",
    "oauth",
    "password",
    "passwd",
    "secret",
    "token",
}
SENSITIVE_ENVIRONMENT_NAME = re.compile(
    r"(^|_)(ACCESS_KEY|API_KEY|AUTH|AUTHORIZATION|COOKIE|CREDENTIALS?|OAUTH|PASSWORD|PASSWD|PRIVATE_KEY|SECRET|TOKEN)($|_)",
    re.IGNORECASE,
)
TOKEN_PATTERNS = (
    ("Yandex OAuth token", re.compile(r"AQAD-[A-Za-z0-9_-]{20,}")),
    ("OAuth token", re.compile(r"y0_[A-Za-z0-9_-]{20,}")),
    ("authorization header", re.compile(r"(?:Bearer|OAuth)[ \t]+[A-Za-z0-9._~-]{16,}", re.IGNORECASE)),
    ("JWT", re.compile(r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}")),
)

SANITIZED_PIPELINE_ROOT = "//tmp/flow-ui-fixture"
SANITIZED_LOG_ROOT = "/tmp/flow-ui-fixture"
FIRST_WORKER_RPC_PORT = 19021
FIRST_WORKER_MONITORING_PORT = 19121

TABLET_COUNT = 3
ROW_COUNT = 900
SCENARIOS = (
    "stopped_clean",
    "working_healthy",
    "paused_with_backlog",
    "draining_with_backlog",
    "draining_drained",
    "stopped_after_drain",
    "completed",
)

QUEUE_SCHEMA = [
    {"name": "key", "type": "string"},
    {"name": "value", "type": "string"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]


def _capture_source_sha256():
    digest = hashlib.sha256()
    for relative_path in CAPTURE_SOURCE_FILES:
        digest.update(relative_path.encode())
        digest.update(b"\0")
        source_path = pathlib.Path(yatest.common.source_path(f"{yatest.common.context.project_path}/{relative_path}"))
        digest.update(source_path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _address_host(address):
    address = str(address)
    if address.startswith("["):
        return address[1 : address.index("]")]
    return address.rsplit(":", 1)[0]


def _sanitization_replacements(view, work_yt_path, log_path, username):
    workers = view["state"]["workers"]
    replacements = {
        str(work_yt_path): SANITIZED_PIPELINE_ROOT,
        str(log_path): SANITIZED_LOG_ROOT,
    }
    if len(username) >= 4:
        replacements[username] = "ui-test-user"

    for index, old_address in enumerate(sorted(workers, key=str), start=0):
        worker = workers[old_address]
        addresses = (str(old_address), str(worker.get("rpc_address", old_address)), str(worker["monitoring_address"]))
        replacements[addresses[0]] = f"localhost:{FIRST_WORKER_RPC_PORT + index}"
        replacements[addresses[1]] = f"localhost:{FIRST_WORKER_RPC_PORT + index}"
        replacements[addresses[2]] = f"localhost:{FIRST_WORKER_MONITORING_PORT + index}"
        replacements[str(worker["name"])] = "localhost"
        replacements[str(worker["build_version"])] = "ui-test-build"
        for address in addresses:
            replacements[_address_host(address)] = "localhost"
    return replacements


def _dump_sanitized_yson(value, path, replacements):
    buffer = io.BytesIO()
    yson.dump(value, buffer, sort_keys=True, yson_format="pretty")
    payload = buffer.getvalue()
    for old, new in sorted(replacements.items(), key=lambda item: len(item[0]), reverse=True):
        payload = payload.replace(old.encode(), new.encode())
    path.write_bytes(payload)


def _assert_no_sensitive_data(value, environment_secrets, path="$", *, inspect_attributes=True):
    if inspect_attributes and getattr(value, "has_attributes", lambda: False)():
        _assert_no_sensitive_data(
            value.attributes,
            environment_secrets,
            f"{path}.@",
            inspect_attributes=False,
        )
    if isinstance(value, (bytes, str)):
        payload = value.encode() if isinstance(value, str) else bytes(value)
        for name, secret in environment_secrets.items():
            if secret in payload:
                raise AssertionError(
                    f"Value of sensitive environment variable {name} leaked into fixture YSON at {path}"
                )
    elif isinstance(value, dict):
        for key, item in value.items():
            normalized_key = str(key).lower()
            parts = set(re.findall(r"[a-z0-9]+", normalized_key))
            compact_key = re.sub(r"[^a-z0-9]+", "", normalized_key)
            if parts & SENSITIVE_FIELD_PARTS or compact_key in {"accesskey", "apikey", "privatekey", "securevault"}:
                raise AssertionError(f"Sensitive field is forbidden in fixture YSON at {path}.{key}")
            _assert_no_sensitive_data(key, environment_secrets, f"{path}.<key>", inspect_attributes=False)
            _assert_no_sensitive_data(item, environment_secrets, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_sensitive_data(item, environment_secrets, f"{path}[{index}]")


def _audit_fixture_yson(paths, environment=None):
    environment = os.environ if environment is None else environment
    environment_secrets = {
        name: value.encode()
        for name, value in environment.items()
        if SENSITIVE_ENVIRONMENT_NAME.search(name) and len(value) >= 12
    }

    for path in paths:
        payload = path.read_bytes()
        for name, secret in environment_secrets.items():
            if secret in payload:
                raise AssertionError(f"Value of sensitive environment variable {name} leaked into {path.name}")

        text = payload.decode("utf-8", errors="ignore")
        for label, pattern in TOKEN_PATTERNS:
            if pattern.search(text):
                raise AssertionError(f"Possible {label} leaked into {path.name}")

        with path.open("rb") as stream:
            _assert_no_sensitive_data(yson.load(stream), environment_secrets)


@pytest.mark.parametrize(
    ("document", "environment", "error"),
    [
        ({"oauth_token": "redacted"}, {}, "Sensitive field"),
        ({"api_key": "redacted"}, {}, "Sensitive field"),
        (
            {"description": "secret-value-from-environment"},
            {"TEST_TOKEN": "secret-value-from-environment"},
            "TEST_TOKEN",
        ),
        (
            {"description": "escaped-secret-with-\"quote\\and\nnewline"},
            {"TEST_TOKEN": "escaped-secret-with-\"quote\\and\nnewline"},
            "TEST_TOKEN",
        ),
        ({"description": "Bearer abcdefghijklmnopqrstuvwxyz"}, {}, "authorization header"),
    ],
)
@pytest.mark.authors(["pechatnov"])
def test_fixture_secret_audit_rejects_sensitive_data(tmp_path, document, environment, error):
    path = tmp_path / "fixture.yson"
    with path.open("wb") as output:
        yson.dump(document, output)
    with pytest.raises(AssertionError, match=error):
        _audit_fixture_yson([path], environment)


@pytest.mark.authors(["pechatnov"])
def test_capture_sanitization_rewrites_runtime_identity(tmp_path):
    first_address = "[2a02:6b8::1]:30001"
    second_address = "[2a02:6b8::1]:30002"
    view = {
        "state": {
            "workers": {
                first_address: {
                    "address": first_address,
                    "rpc_address": first_address,
                    "monitoring_address": "[2a02:6b8::1]:31001",
                    "name": "developer-host.example.net",
                    "remote_shell_command": "ssh 2a02:6b8::1",
                    "build_version": "local+developer",
                },
                second_address: {
                    "address": second_address,
                    "rpc_address": second_address,
                    "monitoring_address": "[2a02:6b8::1]:31002",
                    "name": "developer-host.example.net",
                    "remote_shell_command": "ssh 2a02:6b8::1",
                    "build_version": "local+developer",
                },
            },
            "job": {
                "worker_address": second_address,
                "pipeline_path": "//tmp/test/pipeline_developer",
                "log_path": "/home/developer/build/log",
            },
        }
    }
    path = tmp_path / "fixture.yson"
    replacements = _sanitization_replacements(
        view,
        "//tmp/test/pipeline_developer",
        "/home/developer/build",
        "developer",
    )
    _dump_sanitized_yson(view, path, replacements)
    with path.open("rb") as stream:
        sanitized = yson.load(stream)

    workers = sanitized["state"]["workers"]
    assert set(workers) == {"localhost:19021", "localhost:19022"}
    assert sanitized["state"]["job"]["worker_address"] == "localhost:19022"
    assert sanitized["state"]["job"]["pipeline_path"] == SANITIZED_PIPELINE_ROOT
    assert sanitized["state"]["job"]["log_path"] == f"{SANITIZED_LOG_ROOT}/log"
    assert {str(worker["name"]) for worker in workers.values()} == {"localhost"}
    assert {str(worker["build_version"]) for worker in workers.values()} == {"ui-test-build"}


class TestFlowUiPipeline(FlowTestBase):
    FLOW_BINARY_PATH = FLOW_BINARY_PATH

    def setup_method(self, method):
        super().setup_method(method)
        self.input_queue = f"{self.work_yt_path}/input_queue"
        self.consumer = f"{self.work_yt_path}/consumer"
        self.output_queue = f"{self.work_yt_path}/output_queue"
        self.audit_queue = f"{self.work_yt_path}/audit_queue"
        self.output_producer = f"{self.work_yt_path}/output_producer"
        self.audit_producer = f"{self.work_yt_path}/audit_producer"
        self.fixture_dir = pathlib.Path(yatest.common.output_path("flow_ui_fixtures"))
        self.fixture_dir.mkdir()
        (self.fixture_dir / "fixtures").mkdir()
        (self.fixture_dir / "job_orchids").mkdir()
        self.captured_scenarios = []
        self.gate_ready = pathlib.Path(self.path_to_flow_logs, "commit_gate.ready")
        self.gate_release = pathlib.Path(self.path_to_flow_logs, "commit_gate.release")

    def _prepare_environment(self):
        run_yt_sync(
            self.primary_cluster_name,
            self.work_yt_path,
            add_input_queue_and_consumer=True,
            input_queue_schema=QUEUE_SCHEMA,
            input_queue_tablet_count=TABLET_COUNT,
            output_queues=[
                {"name": "output_queue", "schema": QUEUE_SCHEMA, "tablet_count": TABLET_COUNT},
                {"name": "audit_queue", "schema": QUEUE_SCHEMA, "tablet_count": TABLET_COUNT},
            ],
            producer_names=["output_producer", "audit_producer"],
        )

    def _prepare_pipeline_config(self):
        config = get_yson_config(PIPELINE_CONFIG_PATH)
        source = config["spec"]["computations"]["source_reader"]["source_streams"]["queue_source"]
        source["parameters"].update(
            {
                "queue_path": f"<cluster={self.primary_cluster_name}>{self.input_queue}",
                "consumer_path": f"<cluster={self.primary_cluster_name}>{self.consumer}",
                "finite": False,
            }
        )

        transform = config["spec"]["computations"]["keyed_transform"]
        transform["parameters"].update(
            {
                "commit_gate_ready_path": str(self.gate_ready),
                "commit_gate_release_path": str(self.gate_release),
            }
        )

        sinks = config["spec"]["computations"]["queue_writer"]["sinks"]
        sinks["output_sink"]["parameters"].update(
            {
                "queue_path": f"<cluster={self.primary_cluster_name}>{self.output_queue}",
                "producer_path": f"<cluster={self.primary_cluster_name}>{self.output_producer}",
            }
        )
        sinks["audit_sink"]["parameters"].update(
            {
                "queue_path": f"<cluster={self.primary_cluster_name}>{self.audit_queue}",
                "producer_path": f"<cluster={self.primary_cluster_name}>{self.audit_producer}",
            }
        )
        self.patch_config(config)
        return config, self.dump_config_to_log_dir(config, "pipeline.yson")

    def _install_specs(self, config):
        description = self.client.flow_execute(
            self.pipeline_path,
            flow_command="describe-pipeline",
            flow_argument={"status_only": True},
        )
        controller_version = None
        for message in description["messages"]:
            markdown = str(message.get("markdown_text", ""))
            if "**Controller build info:**" not in markdown:
                continue
            controller_section = markdown.split("**Controller build info:**", 1)[1]
            for line in controller_section.splitlines():
                prefix = "* Binary version: `"
                if line.startswith(prefix) and line.endswith("`"):
                    controller_version = line[len(prefix) : -1]
                    break
        assert controller_version

        target_update = self.client.flow_execute(
            self.pipeline_path,
            flow_command="set-flow-core-target",
            flow_argument={"flow_core_target": controller_version, "allow_update_on_pause": True},
        )

        deadline = time.monotonic() + 30
        while True:
            target = self.client.get_flow_view(
                self.pipeline_path,
                view_path="/state/execution_spec/flow_core_target",
                cache=False,
            )
            if str(target["value"]) == controller_version and int(target["version"]) == int(target_update["version"]):
                break
            if time.monotonic() >= deadline:
                raise AssertionError("FlowCoreTarget update did not become observable")
            time.sleep(0.05)

        self.client.flow_execute(
            self.pipeline_path,
            flow_command="set-pipeline-specs",
            flow_argument={
                "spec": config["spec"],
                "dynamic_spec": config["dynamic_spec"],
                "allow_spec_update_on_pause": True,
            },
        )

    def _write_input(self, prefix):
        rows = [
            {
                "key": f"key-{index % 17}",
                "value": f"{prefix}-{index}",
                "$tablet_index": index % TABLET_COUNT,
            }
            for index in range(ROW_COUNT)
        ]
        batching_write_rows(rows, lambda batch: self.client.insert_rows(self.input_queue, batch), 100)

    def _reset_gate(self):
        self.gate_ready.unlink(missing_ok=True)
        self.gate_release.unlink(missing_ok=True)

    def _wait_gate_ready(self, timeout=180):
        deadline = time.monotonic() + timeout
        while not self.gate_ready.exists():
            if time.monotonic() >= deadline:
                raise AssertionError("Timed out waiting for transform commit gate")
            time.sleep(0.02)

    def _release_gate(self):
        self.gate_release.touch()

    def _wait_for_view(self, predicate, description, timeout=180):
        deadline = time.monotonic() + timeout
        last_view = None
        while time.monotonic() < deadline:
            try:
                last_view = self.client.get_flow_view(self.pipeline_path, cache=False)
                if predicate(last_view):
                    return last_view
            except Exception:
                pass
            time.sleep(0.02)
        state = self._pipeline_state(last_view) if last_view else "unavailable"
        raise AssertionError(f"Timed out waiting for {description}; last pipeline state is {state}")

    @staticmethod
    def _pipeline_state(view):
        return str(view["state"]["execution_spec"]["pipeline_state"]["value"])

    @staticmethod
    def _streams(view):
        computations = view["state"]["traverse_data"]["computations"]
        return [stream for computation in computations.values() for stream in computation["streams"].values()]

    @classmethod
    def _has_backlog(cls, view):
        return any(int(stream.get("inflight_metrics", {}).get("count", 0)) > 0 for stream in cls._streams(view))

    @classmethod
    def _all_streams_drained(cls, view):
        streams = cls._streams(view)
        return bool(streams) and all(str(stream["state"]) in ("drained", "completed") for stream in streams)

    @classmethod
    def _has_drained_stream(cls, view):
        return any(str(stream["state"]) == "drained" for stream in cls._streams(view))

    @staticmethod
    def _has_runtime_errors(view):
        feedback = view["feedback"]
        for status in feedback["partition_job_statuses"].values():
            job_status = status.get("current_job_status")
            if not job_status:
                continue
            if int(job_status.get("error", {}).get("code", 0)) != 0 or job_status.get("retryable_errors"):
                return True
        for status in feedback["worker_statuses"].values():
            if status.get("errors") or int(status.get("previous_crash_error", {}).get("code", 0)) != 0:
                return True
        return False

    def _capture(self, scenario, view):
        assert scenario == SCENARIOS[len(self.captured_scenarios)]
        job_orchids = {}
        layout = view["state"]["execution_spec"]["layout"]
        for partition_id, partition in layout["partitions"].items():
            job_id = partition.get("current_job_id")
            if not job_id:
                continue
            job = layout["jobs"].get(job_id)
            assert job, f"Current job {job_id} is missing from the layout"
            try:
                response = self.client.flow_execute(
                    self.pipeline_path,
                    flow_command="get-worker-orchid",
                    flow_argument={
                        "worker": job["worker_address"],
                        "path": f"/job_tracker/jobs/{job_id}",
                    },
                )
            except Exception:
                continue
            job_orchids[str(partition_id)] = response["value"]

        replacements = _sanitization_replacements(view, self.work_yt_path, self.path_to_flow_logs, getpass.getuser())
        _dump_sanitized_yson(view, self.fixture_dir / "fixtures" / f"{scenario}.yson", replacements)
        _dump_sanitized_yson(job_orchids, self.fixture_dir / "job_orchids" / f"{scenario}.yson", replacements)

        self.captured_scenarios.append(scenario)

    def _queue_value_counts(self, path):
        return Counter(row["value"] for row in self.client.select_rows(f"value from [{path}]"))

    def _wait_for_output(self, expected_count, timeout=180):
        deadline = time.monotonic() + timeout
        while sum(self._queue_value_counts(self.output_queue).values()) < expected_count:
            if time.monotonic() >= deadline:
                raise AssertionError(f"Timed out waiting for {expected_count} output rows")
            time.sleep(0.1)

    @pytest.mark.authors(["pechatnov"])
    def test_capture_flow_ui_fixtures(self):
        self._prepare_environment()
        config, config_path = self._prepare_pipeline_config()

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": config_path},
            workers_count=2,
            controllers_count=1,
            run_pipeline=False,
            problems=False,
        ):
            self._install_specs(config)
            self.client.stop_pipeline(self.pipeline_path)
            stopped_clean = self._wait_for_view(
                lambda view: self._pipeline_state(view) == "stopped"
                and bool(view.get("current_spec"))
                and not view["state"]["execution_spec"]["layout"]["jobs"],
                "installed stopped pipeline",
            )
            self._capture("stopped_clean", stopped_clean)

            self._release_gate()
            self._write_input("warmup")
            self.client.start_pipeline(self.pipeline_path)
            self.wait_pipeline_state("working")
            self.wait_jobs_initialized()
            self._wait_for_output(ROW_COUNT)

            self._reset_gate()
            self._write_input("initial")
            self._wait_gate_ready()
            working = self._wait_for_view(
                lambda view: self._pipeline_state(view) == "working"
                and self._has_backlog(view)
                and not self._has_runtime_errors(view),
                "working pipeline with backlog",
            )
            self._capture("working_healthy", working)

            self.client.pause_pipeline(self.pipeline_path)
            self._release_gate()
            paused = self._wait_for_view(
                lambda view: self._pipeline_state(view) == "paused" and self._has_backlog(view),
                "paused pipeline with backlog",
            )
            self._capture("paused_with_backlog", paused)

            self._reset_gate()
            self._write_input("after-pause")
            self.client.start_pipeline(self.pipeline_path)
            self.wait_pipeline_state("working")
            self._wait_gate_ready()
            self.client.stop_pipeline(self.pipeline_path)
            draining_with_backlog = self._wait_for_view(
                lambda view: self._pipeline_state(view) == "draining" and self._has_backlog(view),
                "draining pipeline with backlog",
            )
            self._capture("draining_with_backlog", draining_with_backlog)
            self._release_gate()
            draining_drained = self._wait_for_view(
                lambda view: self._pipeline_state(view) == "draining" and self._has_drained_stream(view),
                "draining pipeline with a drained upstream stream",
            )
            self._capture("draining_drained", draining_drained)
            stopped_after_drain = self._wait_for_view(
                lambda view: self._pipeline_state(view) == "stopped"
                and self._all_streams_drained(view)
                and bool(view["state"]["execution_spec"]["layout"]["partitions"])
                and not view["state"]["execution_spec"]["layout"]["jobs"],
                "stopped pipeline after drain",
            )
            self._capture("stopped_after_drain", stopped_after_drain)

            self._release_gate()
            spec = self.client.get_pipeline_spec(self.pipeline_path)["spec"]
            spec["computations"]["source_reader"]["source_streams"]["queue_source"]["parameters"]["finite"] = True
            self.client.set_pipeline_spec(self.pipeline_path, spec)
            self.client.start_pipeline(self.pipeline_path)
            self.wait_pipeline_state("completed")
            completed = self._wait_for_view(
                lambda view: self._pipeline_state(view) == "completed" and self._all_streams_drained(view),
                "completed pipeline",
            )
            self._capture("completed", completed)

        assert tuple(self.captured_scenarios) == SCENARIOS
        expected_values = Counter(
            f"{prefix}-{index}" for prefix in ("warmup", "initial", "after-pause") for index in range(ROW_COUNT)
        )
        assert self._queue_value_counts(self.output_queue) == expected_values
        assert self._queue_value_counts(self.audit_queue) == expected_values

        captured_at = int(time.time()) + 1
        (self.fixture_dir / "scenarios.txt").write_text("".join(f"{scenario}\n" for scenario in SCENARIOS))
        (self.fixture_dir / "now_seconds.txt").write_text(f"{captured_at}\n")
        with (self.fixture_dir / "capture_metadata.yson").open("wb") as output:
            yson.dump(
                {
                    "format_version": 2,
                    "source": "live-flow-integration-test",
                    "pipeline_target": "yt/yt/flow/tools/ui_test_controller/fixture_pipeline",
                    "source_sha256": _capture_source_sha256(),
                    "captured_at_unix_seconds": captured_at,
                    "scenario_count": len(SCENARIOS),
                    "runtime_identity": "sanitized",
                    "safety_check": "environment-and-structure-v1",
                },
                output,
                sort_keys=True,
                yson_format="pretty",
            )

        _audit_fixture_yson(sorted(self.fixture_dir.rglob("*.yson")))
