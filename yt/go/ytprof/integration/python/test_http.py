import pytest
import requests
import json
import time

import yatest.common
import yatest.common.network


@pytest.fixture(scope="session")
def running_app():
    with yatest.common.network.PortManager() as pm:
        port = pm.get_port()

        cmd = [
            yatest.common.binary_path("yt/go/ytprof/cmd/app/app"),
            "--config-json",
            json.dumps({"http_endpoint": f"localhost:{port}", "proxy": "test_proxy"}),
            "--log-to-stderr"
        ]

        p = yatest.common.execute(cmd, wait=False, env={"YT_LOG_LEVEL": "DEBUG"})
        time.sleep(1)
        assert p.running

        try:
            yield {"port": port}
        finally:
            p.kill()


def fetch_data(running_app, name):
    rsp = requests.get(f"http://localhost:{running_app['port']}/ytprof/api/{name}")
    if rsp.status_code == 200:
        return rsp.content

    if rsp.status_code == 500:
        raise Exception(rsp.text)

    rsp.raise_for_status()


def test_list(running_app):
    fetch_data(running_app, "list")
