import logging
import re
import pytest
import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from yt_sync import run_yt_sync

# Matches an IPv4 address (dotted-decimal) anywhere in a string.
_IPV4_RE = re.compile(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}")

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(
    "yt/yt/flow/tests/flow_execute/pipeline/pipeline.yson"
)
FLOW_BINARY_PATH = yatest.common.binary_path("yt/yt/flow/tests/flow_execute/pipeline/pipeline")

TABLET_COUNT = 1
EVENT_COUNT = 10


def generate_data(event_count, tablet_count):
    result = []
    for i in range(event_count):
        result.append({"data": f"payload_{i}", "$tablet_index": i % tablet_count})
    return result


INPUT_DATA = generate_data(EVENT_COUNT, TABLET_COUNT)

##################################################################


def _assert_is_ipv4_address(address, context=""):
    assert _IPV4_RE.search(address), f"Expected IPv4 address in {context}: {address}"
    assert "::" not in address, f"Unexpected IPv6 notation in {context}: {address}"


class TestIPv4Support(FlowTestBase):
    FLOW_BINARY_PATH = FLOW_BINARY_PATH

    def setup_method(self, method):
        super(TestIPv4Support, self).setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.consumer = self.work_yt_path + "/consumer"
        self.output_queue = self.work_yt_path + "/output_queue"
        self.producer = self.work_yt_path + "/producer"

    def prepare_environment(self):
        run_yt_sync(self.primary_cluster_name, self.work_yt_path, TABLET_COUNT)
        for i in range(0, len(INPUT_DATA), 100):
            batch = INPUT_DATA[i : i + 100]
            self.client.insert_rows(self.input_queue, batch)

    def _get_rpc_proxy_ipv4_addresses(self):
        """Read RPC proxy addresses from Cypress and replace hostnames with 127.0.0.1.

        The test environment's machine hostname only has AAAA (IPv6) DNS records.
        When address_resolver is configured with enable_ipv6=false, the Flow
        process cannot resolve the RPC proxy's FQDN (returned by discover_proxies).

        This method reads the registered RPC proxy entries from
        //sys/rpc_proxies, extracts the port from each "<hostname>:<port>"
        entry, and constructs "127.0.0.1:<port>" addresses.  Since 127.0.0.1
        is a raw IP, TNetworkAddress::TryParse() succeeds immediately and no
        DNS resolution is needed.
        """
        proxies = self.client.list("//sys/rpc_proxies")
        ipv4_addresses = []
        for proxy in proxies:
            # proxy is "<hostname>:<port>"
            port = str(proxy).rsplit(":", 1)[1]
            ipv4_addresses.append(f"127.0.0.1:{port}")
        logging.info("Patched RPC proxy addresses for IPv4: %s", ipv4_addresses)
        return ipv4_addresses

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster={self.primary_cluster_name}>{self.input_queue}",
                "consumer_path": f"<cluster={self.primary_cluster_name}>{self.consumer}",
                "finite": True,
            }
        )

        pipeline_config["spec"]["computations"]["reader"]["sinks"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster={self.primary_cluster_name}>{self.output_queue}",
                "producer_path": f"<cluster={self.primary_cluster_name}>{self.producer}",
            }
        )

        # The borrowed pipeline declares an external state manager; point it at the dedicated
        # table provisioned in yt_sync below.
        pipeline_config["spec"]["computations"]["state_writer"]["external_state_managers"]["/external_state"][
            "parameters"
        ]["path"] = f"<cluster={self.primary_cluster_name}>{self.work_yt_path}/external_state"

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["timoninmaxim"])
    def test_ipv4_only_environment_works(self):
        """
        Verifies that Flow works correctly in an IPv4-only environment (e.g., Kubernetes without IPv6 support).
        """
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config()

        ipv4_only_node_config = {
            "address_resolver": {
                "enable_ipv4": True,
                "enable_ipv6": False,
                # Test environment's DNS may only have IPv6 records for the hostname.
                # Using a raw IPv4 address bypasses DNS resolution entirely.
                "localhost_name_override": "127.0.0.1",
                "resolve_hostname_into_fqdn": False,
            },
            "clients_cache": {
                # Provide explicit RPC proxy addresses as raw IPv4 and disable proxy discovery.
                # This is needed because the YT RPC proxy registers in Cypress using the host's FQDN,
                # and the test environment's FQDN may only has IPv6 DNS records.
                "default_connection": {
                    "proxy_addresses": self._get_rpc_proxy_ipv4_addresses(),
                    "enable_proxy_discovery": False,
                },
            },
        }

        with self.start_flow_process_federation(
            node_config=ipv4_only_node_config,
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
        ):
            self.wait_pipeline_state("completed", timeout=180)

            controller_address = self.client.get(f"{self.pipeline_path}/@leader_controller_address")
            logging.info("Controller leader address: %s", controller_address)
            _assert_is_ipv4_address(controller_address, "leader_controller_address")

            flow_control_rows = list(
                self.client.select_rows(f'* from [{self.pipeline_path}/flow_control] where key = "leader_controller"')
            )
            assert len(flow_control_rows) == 1, (
                f"Expected exactly one row in flow_control for key 'leader_controller', "
                f"got {len(flow_control_rows)}"
            )
            # The value is the leader node info map; the address lives under "rpc_address".
            flow_control_address = flow_control_rows[0]["value"]["rpc_address"]
            logging.info("flow_control table leader address: %s", flow_control_address)
            assert flow_control_address == controller_address, (
                f"flow_control address {flow_control_address!r} != "
                f"@leader_controller_address {controller_address!r}"
            )
            _assert_is_ipv4_address(flow_control_address, "flow_control leader_controller rpc_address")

            workers_info = self.client.flow_execute(
                self.pipeline_path,
                flow_command="describe-workers",
            )
            assert len(workers_info["workers"]) > 0, "Expected at least one worker"
            for worker in workers_info["workers"]:
                worker_addr = worker["address"]
                logging.info("Worker address: %s", worker_addr)
                _assert_is_ipv4_address(worker_addr, "describe-workers")

            # 3. Controller's node_info from orchid must contain IPv4 rpc_address.
            controller_node_info = self.client.flow_execute(
                self.pipeline_path,
                flow_command="get-controller-orchid",
                flow_argument={"path": "/node_info"},
            )
            rpc_address = controller_node_info["value"]["rpc_address"]
            monitoring_address = controller_node_info["value"]["monitoring_address"]
            logging.info("Controller orchid rpc_address: %s", rpc_address)
            logging.info("Controller orchid monitoring_address: %s", monitoring_address)
            _assert_is_ipv4_address(rpc_address, "controller orchid rpc_address")
            _assert_is_ipv4_address(monitoring_address, "controller orchid monitoring_address")
