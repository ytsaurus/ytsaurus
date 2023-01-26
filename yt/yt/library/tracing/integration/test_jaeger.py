import pytest
import requests
import time
import sys
import subprocess

import yatest.common

from yatest.common import network

TOTAL_SERVERS = 2


@pytest.fixture(scope="session")
def jaeger_server():
    with network.PortManager() as pm:
        ports = []

        for i in range(TOTAL_SERVERS):
            collector_port = pm.get_port()
            query_port = pm.get_port()

            cmd = [
                yatest.common.binary_path("yt/jaeger/bundle/jaeger-all-in-one"),
                "--collector.grpc-server.host-port", f"127.0.0.1:{collector_port}",
                "--query.http-server.host-port", f"127.0.0.1:{query_port}",
                "--admin.http.host-port", "127.0.0.1:0",
                "--collector.http-server.host-port", "127.0.0.1:0",
                "--collector.zipkin.host-port", "127.0.0.1:0",
                "--http-server.host-port", "127.0.0.1:0",
                "--processor.jaeger-binary.server-host-port", "127.0.0.1:0",
                "--processor.jaeger-compact.server-host-port", "127.0.0.1:0",
                "--processor.zipkin-compact.server-host-port", "127.0.0.1:0",
                "--query.grpc-server.host-port", "127.0.0.1:0",
            ]

            p = yatest.common.execute(cmd, wait=False)
            time.sleep(3)
            assert p.running

            ports.append({"collector_port": collector_port, "query_port": query_port})

        try:
            yield ports
        finally:
            p.kill()


def test_start(jaeger_server):
    cmd = [
        yatest.common.binary_path("yt/yt/library/tracing/example/tracing-example"),
        f"127.0.0.1:{jaeger_server[0]['collector_port']}",
        f"127.0.0.1:{jaeger_server[1]['collector_port']}",
    ]

    traces_id = subprocess.check_output(cmd, stderr=sys.stderr).strip().decode("utf-8").split("\n")

    for i in range(TOTAL_SERVERS):
        trace_id = "".join(part.rjust(8, "0") for part in traces_id[i].split("-"))

        # wait for collector queue
        time.sleep(3)

        rsp = requests.get(f"http://127.0.0.1:{jaeger_server[i]['query_port']}/api/traces/{trace_id}")
        rsp.raise_for_status()

        reply = rsp.json()
        assert len(reply.get("data")) == 1

        trace = reply["data"][0]
        assert len(trace["spans"]) == 2

        trace["spans"] = sorted(trace["spans"], key=lambda x: len(x["references"]))

        root_span = trace["spans"][0]
        child_span = trace["spans"][1]

        assert root_span["operationName"] == "Example"
        assert {'key': 'user', 'type': 'string', 'value': 'prime'} in root_span["tags"]

        assert len(root_span["logs"]) == 2

        assert child_span["operationName"] == "Subrequest"
        assert child_span["references"]
        assert {'key': 'index', 'type': 'string', 'value': '0'} in child_span["tags"]

        process = trace["processes"][root_span["processID"]]
        assert process["serviceName"] == "example"
        assert {'key': 'host', 'type': 'string', 'value': 'prime-dev.qyp.yandex-team.ru'} in process["tags"]
