"""Tests for the companion HTTP(S) clients."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from yt.yt.flow.library.python.companion.context import DefaultRuntimeContext, PipelineContext
from yt.yt.flow.library.python.companion.http_client import (
    HttpClient,
    HttpClients,
    create_http_clients,
    get_http_clients,
)
from yt.yt.flow.library.python.companion.job import JobContext
from yt.yt.flow.library.python.companion.service import CompanionRequestProcessor

# ---------- local test server ----------


class _TestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "/final")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path == "/set-cookie":
            self.send_response(200)
            self.send_header("Set-Cookie", "auth=secret; Path=/")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path == "/missing":
            self._send_json(404, {"error": "not found"})
            return
        self._send_json(200, {"method": "GET", "path": self.path, "cookie": self.headers.get("Cookie", "")})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        self._send_json(
            200,
            {
                "method": "POST",
                "path": self.path,
                "body": body.decode("utf-8"),
                "header": self.headers.get("X-Test-Header", ""),
            },
        )

    def _send_json(self, code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


@pytest.fixture(scope="module")
def http_base_url():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _TestHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


# ---------- HttpClient ----------


class TestHttpClient:
    def test_get(self, http_base_url):
        client = HttpClient()
        response = client.get(f"{http_base_url}/resource", params={"q": "1"})
        assert response.status_code == 200
        assert response.ok
        assert response.json() == {"method": "GET", "path": "/resource?q=1", "cookie": ""}
        assert response.headers["Content-Type"] == "application/json"
        assert b"GET" in response.content

    def test_post(self, http_base_url):
        client = HttpClient()
        response = client.post(
            f"{http_base_url}/submit",
            data="payload",
            headers={"X-Test-Header": "value"},
        )
        assert response.status_code == 200
        assert response.json() == {
            "method": "POST",
            "path": "/submit",
            "body": "payload",
            "header": "value",
        }

    def test_error_status_does_not_raise(self, http_base_url):
        client = HttpClient()
        response = client.get(f"{http_base_url}/missing")
        assert response.status_code == 404
        assert not response.ok
        assert response.json() == {"error": "not found"}

    def test_cookies_are_not_persisted(self, http_base_url):
        client = HttpClient()
        client.get(f"{http_base_url}/set-cookie")
        response = client.get(f"{http_base_url}/resource")
        assert response.json()["cookie"] == ""

    def test_default_returns_redirect_response(self, http_base_url):
        client = HttpClient()
        response = client.get(f"{http_base_url}/redirect")
        assert response.status_code == 302
        assert response.headers["Location"] == "/final"

    def test_configured_redirect_count_is_followed(self, http_base_url):
        client = HttpClient({"max_redirect_count": 1})
        response = client.get(f"{http_base_url}/redirect")
        assert response.status_code == 200
        assert response.json() == {"method": "GET", "path": "/final", "cookie": ""}


# ---------- config parsing ----------


class TestClientConfig:
    def test_accepts_bytes_and_str_keys(self):
        for config in (
            {"max_idle_connections": 4, "max_redirect_count": 2},
            {b"max_idle_connections": 4, b"max_redirect_count": 2},
        ):
            client = HttpClient(config)
            assert client._session.max_redirects == 2

    def test_create_http_clients(self):
        clients = create_http_clients(
            {
                "http_client_config": {"max_idle_connections": 2},
                "https_client_config": {},
            }
        )
        assert isinstance(clients, HttpClients)
        assert isinstance(clients.http, HttpClient)
        assert isinstance(clients.https, HttpClient)

    def test_create_http_clients_without_config(self):
        clients = create_http_clients(None)
        assert isinstance(clients.http, HttpClient)
        assert isinstance(clients.https, HttpClient)


# ---------- runtime context exposure ----------


class TestRuntimeContext:
    @staticmethod
    def _make_context(**kwargs):
        return DefaultRuntimeContext(
            internal_state_names=set(),
            stream_specs=None,
            internal_states={},
            external_states={},
            watermarks={},
            min_watermark=0,
            computation_parameters={},
            computation_dynamic_parameters={},
            **kwargs,
        )

    def test_clients_are_required(self):
        ctx = self._make_context()
        with pytest.raises(ValueError):
            ctx.http_client
        with pytest.raises(ValueError):
            ctx.https_client

    def test_clients_are_exposed(self):
        clients = create_http_clients(None)
        ctx = self._make_context(http_client=clients.http, https_client=clients.https)
        assert ctx.http_client is clients.http
        assert ctx.https_client is clients.https


# ---------- end-to-end through the request processor ----------


class TestProcessorProvidesClients:
    @staticmethod
    def _get_proto_modules():
        from yt.yt.flow.library.python.companion._proto_compat import ensure_proto_imports

        ensure_proto_imports()
        from yt.flow.library.cpp.companion.proto import companion_service_pb2 as cs_pb2
        from yt.flow.library.cpp.common.proto import message_pb2 as msg_pb2

        return cs_pb2, msg_pb2

    @staticmethod
    def _make_guid_proto(first=0x12345678, second=0xABCDEF00):
        from yt_proto.yt.core.misc.proto import guid_pb2

        guid = guid_pb2.TGuid()
        guid.first = first
        guid.second = second
        return guid

    @staticmethod
    def _make_job_info(proto_module, streams=None):
        job_info = proto_module.TJobInfo()
        job_info.spec = b"{}"
        job_info.dynamic_spec = b"{}"
        for stream in streams or []:
            job_info.streams.append(stream)
        return job_info

    @staticmethod
    def _make_stream_proto(proto_module, stream_id, spec_id):
        stream = proto_module.TStream()
        stream.stream_id = stream_id
        stream.stream_spec_id = spec_id
        stream.schema = b"[]"
        return stream

    def test_process_batch_sets_clients(self):
        from yt.yt.flow.library.python.companion.context import ResponseContext

        cs_pb2, msg_pb2 = self._get_proto_modules()

        class RecordingComputation:
            computation_id = "mapper"
            request_context = None

            def do_process(self, request_context):
                self.request_context = request_context
                return ResponseContext(
                    job_id=request_context.job_id,
                    request_id=request_context.request_id,
                )

        comp = RecordingComputation()
        pipeline_ctx = PipelineContext()
        pipeline_ctx.register_computation(comp)
        processor = CompanionRequestProcessor(pipeline_ctx, JobContext())

        request = cs_pb2.TReqProcessBatch()
        request.request_id.CopyFrom(self._make_guid_proto(1, 2))
        request.job_id.CopyFrom(self._make_guid_proto(3, 4))
        request.computation_id = "mapper"
        request.job_info.CopyFrom(self._make_job_info(cs_pb2, streams=[self._make_stream_proto(cs_pb2, "input", 0)]))

        class ProtoModule:
            TResponseData = cs_pb2.TResponseData
            TNewTimer = cs_pb2.TNewTimer
            TState = cs_pb2.TState
            TStateItem = cs_pb2.TStateItem
            TMessage = msg_pb2.TMessage

        result = processor.process_batch(request, ProtoModule)
        assert result["status"] == "RS_OK"
        assert comp.request_context.http_client is processor.http_clients.http
        assert comp.request_context.https_client is processor.http_clients.https


class TestProcessWideClients:
    def test_get_http_clients_is_singleton(self):
        first = get_http_clients()
        second = get_http_clients()
        assert first is second
        assert isinstance(first.http, HttpClient)
