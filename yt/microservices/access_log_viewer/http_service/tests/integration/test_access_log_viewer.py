from base import ClickHouseTestBase, Clique

from yt.wrapper import YtClient
from yt.wrapper.testlib.helpers import wait
from yt_commands import (
    authors,
    create,
    issue_token,
)

from library.python.port_manager import PortManager

import pytest
import requests
import yatest.common

import os
import subprocess
from contextlib import ExitStack
from datetime import datetime, timezone
from typing import Iterator

ACCESS_LOG_VIEWER_BINARY_PATH = yatest.common.binary_path(
    "yt/microservices/access_log_viewer/http_service/access_log_viewer"
)
ACCESS_LOG_SNAPSHOT_ROOT = "//tmp/access_log_viewer_snapshots"
DEBUG_LOGIN = "alice"
CHYT_ALIAS = "access_log_viewer_test"


class TestAccessLogViewer(ClickHouseTestBase):
    NUM_NODES = 3

    DELTA_HTTP_PROXY_CONFIG = {
        "auth": {
            "enable_authentication": True,
        },
    }

    @pytest.fixture(autouse=True)
    def access_log_services(self) -> Iterator[None]:
        """Run a local clique and viewer, stopping both even if setup or the test fails."""
        self._token, _ = issue_token("root")
        self._proxy = self.Env.get_http_proxy_address()
        self._yt_client = YtClient(proxy=self._proxy, token=self._token)
        self._viewer_process = None
        with ExitStack() as resources:
            ports = resources.enter_context(PortManager())
            self._viewer_port = ports.get_port()
            viewer_log = resources.enter_context(
                open(yatest.common.test_output_path("access_log_viewer.log"), "w"),
            )
            resources.enter_context(Clique(1, alias=CHYT_ALIAS))
            resources.callback(self._stop_access_log_viewer)
            self._start_access_log_viewer(viewer_log)
            yield

    def _start_access_log_viewer(self, viewer_log):
        environment = os.environ.copy()
        environment.update({"YT_ACCESS_LOG_VIEWER_TOKEN": self._token, "YT_PROXY": self._proxy})
        command = [
            ACCESS_LOG_VIEWER_BINARY_PATH,
            "--port",
            str(self._viewer_port),
            "--proxy",
            self._proxy,
            "--chyt-cluster-name",
            self._proxy,
            "--chyt-alias",
            CHYT_ALIAS,
            "--include",
            self._proxy,
            "--snapshot-root",
            ACCESS_LOG_SNAPSHOT_ROOT,
            "--logs-timezone",
            "UTC",
            "--debug-login",
            DEBUG_LOGIN,
        ]
        self._viewer_process = subprocess.Popen(
            command,
            env=environment,
            stdout=viewer_log,
            stderr=subprocess.STDOUT,
        )
        self._wait_for_response("GET", "liveness", {"status": "ok"})

    def _stop_access_log_viewer(self):
        if self._viewer_process is None:
            return
        self._viewer_process.terminate()
        try:
            self._viewer_process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self._viewer_process.kill()
            self._viewer_process.wait(timeout=30)
        self._viewer_process = None

    def _request(self, method, endpoint, **kwargs):
        assert self._viewer_process.poll() is None, self._viewer_process.returncode
        return requests.request(
            method,
            f"http://127.0.0.1:{self._viewer_port}/{endpoint}",
            timeout=5,
            **kwargs,
        )

    def _wait_for_response(self, method, endpoint, expected, **kwargs):
        last_response = None

        def matches_response():
            nonlocal last_response
            try:
                response = self._request(method, endpoint, **kwargs)
                last_response = (response.status_code, response.text)
                return response.status_code == 200 and response.json() == expected
            except requests.RequestException as error:
                last_response = str(error)
                return False

        try:
            wait(matches_response, timeout=120, ignore_exceptions=False)
        except Exception as error:
            raise AssertionError(
                f"{endpoint}: expected {expected}, last response {last_response}, "
                f"viewer exit code {self._viewer_process.poll()}"
            ) from error

    def _create_access_log_table(self, granularity, table_name):
        path = f"{ACCESS_LOG_SNAPSHOT_ROOT}/{self._proxy}/{granularity}/{table_name}"
        create(
            "table",
            path,
            recursive=True,
            attributes={"schema": [{"name": "instant", "type": "string"}]},
        )
        instant = table_name.replace("T", " ")
        if granularity == "1d":
            instant += " 00:00:00"
        self._yt_client.write_table(path, [{"instant": instant}])

    @authors("ilyaibraev")
    def test_existing_endpoints_and_visible_range_refresh(self) -> None:
        """Serve existing endpoints and refresh the visible range after a new snapshot arrives."""
        cluster_request = {"cluster": self._proxy}
        self._create_access_log_table("1d", "2024-01-01")
        self._create_access_log_table("30min", "2024-01-02T12:00:00")
        visible_range = {
            "earliest": int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp()),
            "latests": int(datetime(2024, 1, 2, 12, tzinfo=timezone.utc).timestamp()),
        }
        self._wait_for_response("POST", "visible-time-range", visible_range, json=cluster_request)

        for prefix in ["", "v1/"]:
            for endpoint, expected in [
                ("whoami", {"user": DEBUG_LOGIN}),
                ("served-clusters", {"clusters": [self._proxy]}),
                ("served_clusters", {"clusters": [self._proxy]}),
                ("visible-time-range", visible_range),
                ("visible_time_range", visible_range),
            ]:
                response = self._request("POST", prefix + endpoint, json=cluster_request)
                assert response.status_code == 200, response.text
                assert response.json() == expected, response.text
        response = self._request("GET", "readiness")
        assert response.status_code == 200, response.text

        self._create_access_log_table("30min", "2024-01-02T12:30:00")
        updated_range = dict(
            visible_range,
            latests=int(datetime(2024, 1, 2, 12, 30, tzinfo=timezone.utc).timestamp()),
        )
        self._wait_for_response(
            "POST",
            "v1/visible_time_range",
            updated_range,
            json=cluster_request,
        )
