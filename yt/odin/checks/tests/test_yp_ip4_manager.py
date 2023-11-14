import yp.tests.helpers.conftest  # noqa

from yp.local import DEFAULT_IP4_ADDRESS_POOL_ID
from yp.tests.helpers.conftest import (
    create_nodes,
    create_pod_with_boilerplate,
    wait_pod_is_assigned,
)
from yp.client import BatchingOptions

from yt.environment.helpers import OpenPortIterator
from yt.yson import yson_types
from yt_odin.logserver import FULLY_AVAILABLE_STATE, UNAVAILABLE_STATE
from yt_odin.test_helpers import (make_check_dir, configure_and_run_checks)

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse

import http.server
import json
import threading


yp_ip4_to_ip6 = {}


class RacktablesHttpHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlparse(self.path)

        if url.path == "/racktables_url":
            data = {
                "1.2.3.4": {
                    "net_loc": "TEST_CLUSTER",
                    "fqdn": yp_ip4_to_ip6["1.2.3.4"],
                    "addr6": yp_ip4_to_ip6["1.2.3.4"]
                }
            }
            data = json.dumps(data)
            response = bytes(data, "utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-type", "application/json")
            self.send_header("Content-length", len(response))
            self.end_headers()
            self.wfile.write(response)
            return

        self.send_error(HTTPStatus.BAD_REQUEST)
        return


class TestYPIp4Manager:
    ENABLE_SSL = True
    # Overrides big (comparing to the check timeout) default timeout.
    YP_CLIENT_CONFIG = dict(request_timeout=10 * 1000)

    def create_and_assign_pod(self, yp_client):
        network_project_id = yp_client.create_object(
            "network_project",
            attributes=dict(
                meta=dict(id="somenet"),
                spec=dict(project_id=42),
            ),
        )

        vlan_id = "backbone"
        create_nodes(yp_client, 1, vlan_id=vlan_id)

        pod_spec = dict(
            enable_scheduling=True,
            ip6_address_requests=[
                dict(
                    vlan_id=vlan_id,
                    network_id=network_project_id,
                    enable_internet=True,
                )
            ],
        )
        yp_client.create_object(
            "internet_address",
            attributes={
                "meta": {"ip4_address_pool_id": DEFAULT_IP4_ADDRESS_POOL_ID},
                "spec": {"ip4_address": "1.2.3.4", "network_module_id": "netmodule1"},
            },
        )
        pod_id = create_pod_with_boilerplate(yp_client, spec=pod_spec)

        wait_pod_is_assigned(yp_client, pod_id)

    def _test(self, yp_env, yt_env, tmpdir, res_state, check_options):
        port = next(OpenPortIterator())
        http_endpoint = "http://localhost:{}".format(port)

        certificate_file = tmpdir.join("YandexInternalRootCA.crt")
        certificate_file.write(yp_env.yp_instance.get_certificate())
        check_options.update(dict(
            yp_grpc_address=yp_env.yp_instance.yp_client_secure_grpc_address,
            yp_config=dict(enable_ssl=True, root_certificate={"file_name": str(certificate_file)}),
            racktables_url=http_endpoint + "/racktables_url",
        ))
        checks_path = make_check_dir("yp_ip4_manager", check_options)

        yt_client = yt_env.yt_client
        yt_cluster_url = yt_client.config["proxy"]["url"]

        server = http.server.ThreadingHTTPServer(
            ("localhost", port), RacktablesHttpHandler
        )
        with server:
            server_thread = threading.Thread(target=server.serve_forever)
            server_thread.daemon = True
            server_thread.start()

            storage = configure_and_run_checks(yt_cluster_url, checks_path)
            assert abs(storage.get_service_states("yp_ip4_manager")[0] - res_state) <= 0.001
            server.shutdown()

    def test_available_state(self, yp_env, tmpdir, yt_env):
        yp_client = yp_env.yp_client

        self.create_and_assign_pod(yp_client)

        pods = yp_client.select_objects(
            "pod",
            selectors=["/status/ip6_address_allocations"],
            batching_options=BatchingOptions(),
        )

        for (ip6_address_allocations,) in pods:
            assert not isinstance(ip6_address_allocations, yson_types.YsonEntity)
            for allocation in ip6_address_allocations:
                yp_ip4_to_ip6[allocation["internet_address"]["ip4_address"]] = allocation["address"]

        self._test(yp_env, yt_env, tmpdir, FULLY_AVAILABLE_STATE, {})

    def test_unavailable_state_without_deadline(self, yp_env, tmpdir, yt_env):
        yp_client = yp_env.yp_client
        self.create_and_assign_pod(yp_client)
        yp_ip4_to_ip6["1.2.3.4"] = "1"
        check_options = dict(ip_address_sync_deadline=0)
        self._test(yp_env, yt_env, tmpdir, UNAVAILABLE_STATE, check_options)

    def test_unavailable_state_with_deadline(self, yp_env, tmpdir, yt_env):
        yp_client = yp_env.yp_client
        self.create_and_assign_pod(yp_client)
        yp_ip4_to_ip6["1.2.3.4"] = "1"
        self._test(yp_env, yt_env, tmpdir, FULLY_AVAILABLE_STATE, {})
