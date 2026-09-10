import collections
import dataclasses
import http.server
import threading

import pytest
import yatest.common

from yt.common import wait
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase

from .yt_sync import run_yt_sync

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")
FLOW_SERVER_BINARY_PATH = yatest.common.binary_path("yt/yt/flow/bin/flow_server/flow_server")


@dataclasses.dataclass
class ResponseStep:
    status: int
    body: bytes
    release: threading.Event | None = None
    split_body: bool = False


@dataclasses.dataclass
class ReceivedRequest:
    body: bytes
    headers: dict[str, str]
    client_port: int


class HttpReceiver:
    def __init__(self, steps):
        self._steps = collections.deque(steps)
        self._release_events = [step.release for step in steps if step.release is not None]
        self._condition = threading.Condition()
        self._requests = []
        self._split_body_started = threading.Event()
        receiver = self

        class RequestHandler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_POST(self):
                content_length = int(self.headers["Content-Length"])
                body = self.rfile.read(content_length)
                with receiver._condition:
                    receiver._requests.append(ReceivedRequest(body, dict(self.headers.items()), self.client_address[1]))
                    step = receiver._steps.popleft()

                if step.release is not None and not step.split_body:
                    step.release.wait()

                self.send_response(step.status)
                self.send_header("Content-Length", str(len(step.body)))
                self.end_headers()

                if step.split_body:
                    split_at = max(1, len(step.body) // 2)
                    self.wfile.write(step.body[:split_at])
                    self.wfile.flush()
                    receiver._split_body_started.set()
                    if step.release is not None:
                        step.release.wait()
                    self.wfile.write(step.body[split_at:])
                else:
                    self.wfile.write(step.body)
                self.wfile.flush()

            def log_message(self, format, *args):
                pass

        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), RequestHandler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever)
        self._thread.start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self._server.server_port}/messages"

    @property
    def requests(self):
        with self._condition:
            return list(self._requests)

    def wait_request_count(self, count):
        wait(lambda: len(self.requests) >= count, timeout=180)

    def wait_split_body_started(self):
        assert self._split_body_started.wait(timeout=180)

    def close(self):
        for release in self._release_events:
            release.set()
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


@pytest.mark.authors(["blinkov"])
class TestAsyncHttpSink(FlowTestBase):
    FLOW_BINARY_PATH = FLOW_SERVER_BINARY_PATH

    def setup_method(self, method):
        super().setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.consumer = self.work_yt_path + "/consumer"
        run_yt_sync("primary", self.work_yt_path)

    def prepare_pipeline_config(
        self,
        primary_url,
        secondary_url=None,
        max_attempt_count=3,
        finite=True,
    ):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)
        writer = pipeline_config["spec"]["computations"]["writer"]
        writer["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.consumer}",
                "finite": finite,
            }
        )
        writer["sinks"]["http"]["parameters"]["url"] = primary_url
        dynamic_writer = pipeline_config["dynamic_spec"]["computations"]["writer"]
        dynamic_writer["sinks"]["http"]["parameters"]["max_attempt_count"] = max_attempt_count

        if secondary_url is None:
            del writer["sinks"]["http_secondary"]
            del dynamic_writer["sinks"]["http_secondary"]
        else:
            writer["sinks"]["http_secondary"]["parameters"]["url"] = secondary_url

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def test_flow_server_registers_async_http_sink(self):
        config_path = self.prepare_pipeline_config(
            "http://127.0.0.1:1/messages",
            finite=False,
        )
        yatest.common.execute([FLOW_SERVER_BINARY_PATH, "--validate-only", "--config", config_path])

    def write_payloads(self, payloads):
        self.client.insert_rows(self.input_queue, [{"payload": payload} for payload in payloads])

    def source_persisted_offset(self):
        for row in self.client.select_rows(f"* from [{self.pipeline_path}/states]"):
            computation_id = row["computation_id"]
            name = row["name"]
            if isinstance(computation_id, bytes):
                computation_id = computation_id.decode()
            if isinstance(name, bytes):
                name = name.decode()
            if computation_id != "writer" or name != "/$active_source/v0":
                continue
            state = row["state"]
            persisted = state.get("persisted_offset_exclusive_v2", state.get(b"persisted_offset_exclusive_v2"))
            return int(persisted[0]) if persisted else 0
        return None

    def wait_dynamic_spec_applied(self, version):
        wait(
            lambda: self.client.get_flow_view(
                self.pipeline_path,
                view_path="/state/execution_spec/dynamic_pipeline_spec/version",
                cache=False,
            )
            == version,
            timeout=180,
            ignore_exceptions=True,
        )

    def assert_requests(self, receiver, expected_bodies):
        requests = receiver.requests
        assert [request.body for request in requests] == expected_bodies
        for request in requests:
            assert request.headers["Content-Type"] == "application/octet-stream"
            assert request.headers["X-Flow-Test"] == "async-http-sink"

    def test_dynamic_retry_and_persisted_completion(self):
        release = threading.Event()
        with HttpReceiver(
            [
                ResponseStep(201, b"configured", release=release),
                ResponseStep(503, b"retry"),
                ResponseStep(201, b"accepted"),
            ]
        ) as receiver:
            self.write_payloads([b"configuration-barrier"])
            config_path = self.prepare_pipeline_config(receiver.url, max_attempt_count=1, finite=False)

            with self.start_flow_process_federation(
                pipeline_binary_args={"--config": config_path},
                workers_count=1,
                controllers_count=1,
            ):
                receiver.wait_request_count(1)
                dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
                dynamic_spec["spec"]["computations"]["writer"]["sinks"]["http"]["parameters"]["max_attempt_count"] = 2
                self.client.set_pipeline_dynamic_spec(
                    self.pipeline_path,
                    dynamic_spec["spec"],
                    expected_version=dynamic_spec["version"],
                )
                updated_dynamic_spec = self.client.get_pipeline_dynamic_spec(self.pipeline_path)
                assert updated_dynamic_spec["version"] > dynamic_spec["version"]
                self.wait_dynamic_spec_applied(updated_dynamic_spec["version"])
                release.set()
                wait(lambda: self.source_persisted_offset() == 1, timeout=180, ignore_exceptions=True)
                self.write_payloads([b"opaque\0payload"])
                wait(lambda: self.source_persisted_offset() == 2, timeout=180, ignore_exceptions=True)

            self.assert_requests(receiver, [b"configuration-barrier", b"opaque\0payload", b"opaque\0payload"])
            assert len({request.client_port for request in receiver.requests}) == 1

    def test_keep_alive_reuse_and_disable(self):
        with HttpReceiver([ResponseStep(201, b""), ResponseStep(201, b"")]) as reused_receiver, HttpReceiver(
            [ResponseStep(201, b""), ResponseStep(201, b"")]
        ) as disabled_receiver:
            config_path = self.prepare_pipeline_config(reused_receiver.url, disabled_receiver.url, finite=False)

            with self.start_flow_process_federation(
                pipeline_binary_args={"--config": config_path},
                workers_count=1,
                controllers_count=1,
            ):
                self.write_payloads([b"first"])
                wait(lambda: self.source_persisted_offset() == 1, timeout=180, ignore_exceptions=True)
                self.write_payloads([b"second"])
                wait(lambda: self.source_persisted_offset() == 2, timeout=180, ignore_exceptions=True)

            self.assert_requests(reused_receiver, [b"first", b"second"])
            self.assert_requests(disabled_receiver, [b"first", b"second"])
            assert len({request.client_port for request in reused_receiver.requests}) == 1
            assert len({request.client_port for request in disabled_receiver.requests}) == 2

    def test_worker_restart_during_split_response_replays_without_loss(self):
        release = threading.Event()
        with HttpReceiver(
            [
                ResponseStep(200, b"split-response", release=release, split_body=True),
                ResponseStep(200, b"complete-response"),
            ]
        ) as receiver:
            payload = b"restart-payload"
            self.write_payloads([payload])
            config_path = self.prepare_pipeline_config(receiver.url)

            with self.start_flow_process_federation(
                pipeline_binary_args={"--config": config_path},
                workers_count=1,
                controllers_count=1,
                start_watcher_thread=False,
            ) as federation:
                receiver.wait_split_body_started()
                federation.workers[0].stop()
                release.set()
                federation.workers[0].start()
                self.wait_pipeline_state("completed", timeout=180)

            bodies = [request.body for request in receiver.requests]
            assert bodies
            assert set(bodies) == {payload}
