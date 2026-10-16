"""E2e test: the Python companion hands the shared HTTP(S) clients to user code.

The Python mirror of the C++ ``tests/companion/http_client`` test: the
"http-get" companion computation fetches the configured URL with
``ctx.http_client`` or ``ctx.https_client`` and emits the response status code.
"""

import http.server
import threading

import pytest
import yatest.common

from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_flow_python_base import (
    FlowTestPythonBase,
)
from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline.yson")

INPUT_QUEUE_SCHEMA = [
    {"name": "key", "type": "string"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

OUTPUT_QUEUE_SCHEMA = [
    {"name": "key", "type": "string"},
    {"name": "status_code", "type": "uint64"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]


class RedirectingHttpServer:
    def __init__(self):
        self._paths = []
        self._lock = threading.Lock()
        receiver = self

        class RequestHandler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                with receiver._lock:
                    receiver._paths.append(self.path)

                if self.path == "/redirect":
                    self.send_response(http.HTTPStatus.FOUND)
                    self.send_header("Location", f"http://127.0.0.1:{self.server.server_port}/ok")
                elif self.path == "/ok":
                    self.send_response(http.HTTPStatus.ACCEPTED)
                else:
                    self.send_response(http.HTTPStatus.NOT_FOUND)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, format, *args):
                pass

        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), RequestHandler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever)
        self._thread.start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self._server.server_port}/redirect"

    @property
    def paths(self):
        with self._lock:
            return list(self._paths)

    def close(self):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class TestCompanionHttpClient(FlowTestPythonBase):
    PYTHON_COMPANION_BINARY = yatest.common.binary_path(
        "yt/yt/flow/tests/companion/http_client/python/http_client_py_companion"
    )

    def setup_method(self, method):
        super().setup_method(method)
        self.input_queue = f"{self.work_yt_path}/input_queue"
        self.input_consumer = f"{self.work_yt_path}/consumer"
        self.output_queue = f"{self.work_yt_path}/output_queue"
        run_yt_sync(
            self.primary_cluster_name,
            self.work_yt_path,
            tablet_cell_bundle=self.tablet_cell_bundle,
            primary_medium=self.primary_medium,
            add_input_queue_and_consumer=True,
            input_queue_schema=INPUT_QUEUE_SCHEMA,
            add_output_queue=True,
            output_queue_schema=OUTPUT_QUEUE_SCHEMA,
        )

    def prepare_pipeline_config(self, url, use_https_client):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)
        reader_parameters = pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"]
        reader_parameters.update(
            {
                "queue_path": f"<cluster={self.primary_cluster_name}>{self.input_queue}",
                "consumer_path": f"<cluster={self.primary_cluster_name}>{self.input_consumer}",
                "finite": True,
            }
        )

        http_get = pipeline_config["spec"]["computations"]["http-get"]
        http_get["parameters"]["url"] = url
        http_get["parameters"]["use_https_client"] = use_https_client
        http_get["sinks"]["queue"]["parameters"][
            "queue_path"
        ] = f"<cluster={self.primary_cluster_name}>{self.output_queue}"

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["sergeypozdeev"])
    @pytest.mark.parametrize("use_https_client", [False, True], ids=["http", "https"])
    def test_http_clients_follow_configured_redirects(self, use_https_client):
        self.client.insert_rows(self.input_queue, [{"key": "probe"}])

        with RedirectingHttpServer() as http_server:
            pipeline_config_path = self.prepare_pipeline_config(http_server.url, use_https_client)
            client_config = {
                "max_redirect_count": 1,
            }
            if use_https_client:
                # Ignored by the Python client; kept for parity with the C++ test.
                client_config["allow_http"] = True

            client_config_key = "https_client_config" if use_https_client else "http_client_config"
            node_config = {
                "companion": {
                    client_config_key: client_config,
                },
            }

            with self.start_flow_process_federation(
                node_config=node_config,
                pipeline_binary_args={"--config": pipeline_config_path},
            ):
                self.wait_pipeline_state("completed", timeout=240)
                rows = list(self.client.select_rows(f"`key`, `status_code` FROM [{self.output_queue}]"))

            assert rows == [{"key": "probe", "status_code": 202}]
            assert "/redirect" in http_server.paths
            assert "/ok" in http_server.paths

    @pytest.mark.authors(["sergeypozdeev"])
    def test_http_client_returns_redirect_response_by_default(self):
        self.client.insert_rows(self.input_queue, [{"key": "probe"}])

        with RedirectingHttpServer() as http_server:
            pipeline_config_path = self.prepare_pipeline_config(http_server.url, False)

            with self.start_flow_process_federation(
                pipeline_binary_args={"--config": pipeline_config_path},
            ):
                self.wait_pipeline_state("completed", timeout=240)
                rows = list(self.client.select_rows(f"`key`, `status_code` FROM [{self.output_queue}]"))

            assert rows == [{"key": "probe", "status_code": 302}]
            assert "/redirect" in http_server.paths
            assert "/ok" not in http_server.paths
