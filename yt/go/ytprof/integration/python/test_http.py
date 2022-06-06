import pytest
import requests
import json
import time
import os

import yatest.common
import yatest.common.network


@pytest.fixture(scope="session")
def running_app():
    with yatest.common.network.PortManager() as pm:
        port = pm.get_port()

        cmd = [
            yatest.common.binary_path("yt/go/ytprof/cmd/app/app"),
            "--config-json",
            json.dumps({
                "http_endpoint": f"localhost:{port}",
                "proxy": os.environ['YT_PROXY'],
                "table_path": "//home/kristevalex/ytprof/testing"}),
            "--log-to-stderr",
        ]

        p = yatest.common.execute(cmd, wait=False, env={"YT_LOG_LEVEL": "DEBUG"})
        time.sleep(1)
        assert p.running
        for _ in range(15):
            try:
                requests.get(f"http://localhost:{port}")
                break
            except:
                time.sleep(1)
        else:
            raise Exception("server not started in 15 sec")

        try:
            yield {"port": port}
        finally:
            p.kill()


def fetch_data(running_app, name, body):
    rsp = requests.post(f"http://localhost:{running_app['port']}/ytprof/api/{name}", json=body)

    if rsp.status_code == 200:
        return rsp.content

    if rsp.status_code == 500:
        raise Exception(rsp.text)

    rsp.raise_for_status()


def test_list(running_app):
    fetch_data(running_app, "list", {
        'Metaquery': {
            'Query': 'true',
            'QueryLimit': 10000,
            'TimePeriod': {
                'PeriodStartTime': '2022-04-24T00:00:00.000000Z',
                'PeriodEndTime': '2022-04-29T00:00:00.000000Z',
            },
        },
    })
