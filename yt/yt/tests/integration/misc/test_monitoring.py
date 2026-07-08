from yt_env_setup import YTEnvSetup

from yt_commands import authors

import yt.packages.requests as requests
import yt.yson as yson

from yt.environment.tls_helpers import create_ca, create_certificate
from yt.environment.helpers import OpenPortIterator

import os
import pytest

##################################################################


class TestHttpsMonitoringServer(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # Problems with TLS.
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    # Populated during apply_config_patches before any test runs.
    _ca_cert = None
    _port_iterator = None
    _scheduler_https_monitoring_port = None
    _master_https_monitoring_port = None
    _node_https_monitoring_port = None

    @classmethod
    def apply_config_patches(cls, configs, ytserver_version, cluster_index, cluster_path):
        super().apply_config_patches(configs, ytserver_version, cluster_index, cluster_path)

        if cluster_index != 0:
            return

        # Keep OpenPortIterator alive so reserved ports are not released before servers start.
        cls._port_iterator = OpenPortIterator()

        ca_cert = os.path.join(cluster_path, "monitoring_ca.crt")
        ca_cert_key = os.path.join(cluster_path, "monitoring_ca.key")
        monitoring_cert = os.path.join(cluster_path, "monitoring_https.crt")
        monitoring_cert_key = os.path.join(cluster_path, "monitoring_https.key")

        create_ca(ca_cert=ca_cert, ca_cert_key=ca_cert_key)
        create_certificate(
            ca_cert=ca_cert,
            ca_cert_key=ca_cert_key,
            cert=monitoring_cert,
            cert_key=monitoring_cert_key,
            names=["localhost"],
        )

        cls._ca_cert = ca_cert

        credentials = {
            "cert_chain": {"file_name": monitoring_cert},
            "private_key": {"file_name": monitoring_cert_key},
        }

        # Inject HTTPS monitoring port into each scheduler.
        for i, config in enumerate(configs["scheduler"]):
            port = next(cls._port_iterator)
            config["monitoring_https_port"] = port
            config["monitoring_https_credentials"] = credentials
            if i == 0:
                cls._scheduler_https_monitoring_port = port

        # Inject HTTPS monitoring port into primary cell masters.
        primary_cell_tag = configs["master"]["primary_cell_tag"]
        for i, config in enumerate(configs["master"][primary_cell_tag]):
            port = next(cls._port_iterator)
            config["monitoring_https_port"] = port
            config["monitoring_https_credentials"] = credentials
            if i == 0:
                cls._master_https_monitoring_port = port

        # Inject HTTPS monitoring port into each node.
        for i, config in enumerate(configs["node"]):
            port = next(cls._port_iterator)
            config["monitoring_https_port"] = port
            config["monitoring_https_credentials"] = credentials
            if i == 0:
                cls._node_https_monitoring_port = port

    @authors("yuryalekseev")
    def test_scheduler_https_monitoring(self):
        """HTTPS monitoring server on scheduler serves /orchid/ and /solomon/all."""
        port = self.__class__._scheduler_https_monitoring_port
        ca_cert = self.__class__._ca_cert

        rsp = requests.get(f"https://localhost:{port}/orchid/service/name", verify=ca_cert)
        rsp.raise_for_status()
        assert yson.loads(rsp.content) == "scheduler"

        rsp = requests.get(f"https://localhost:{port}/solomon/all", verify=ca_cert)
        rsp.raise_for_status()
        assert "sensors" in rsp.json()

    @authors("yuryalekseev")
    def test_master_https_monitoring(self):
        """HTTPS monitoring server on master serves /orchid/."""
        port = self.__class__._master_https_monitoring_port
        ca_cert = self.__class__._ca_cert

        rsp = requests.get(f"https://localhost:{port}/orchid/service/name", verify=ca_cert)
        rsp.raise_for_status()
        assert yson.loads(rsp.content) == "master"

    @authors("yuryalekseev")
    def test_node_https_monitoring(self):
        """HTTPS monitoring server on node serves /orchid/."""
        port = self.__class__._node_https_monitoring_port
        ca_cert = self.__class__._ca_cert

        rsp = requests.get(f"https://localhost:{port}/orchid/service/name", verify=ca_cert)
        rsp.raise_for_status()
        assert yson.loads(rsp.content) == "node"

    @authors("yuryalekseev")
    def test_http_monitoring_still_works(self):
        """Regular HTTP monitoring is unaffected when HTTPS monitoring is enabled."""
        scheduler_monitoring_port = self.Env.configs["scheduler"][0]["monitoring_port"]
        rsp = requests.get(f"http://localhost:{scheduler_monitoring_port}/orchid/service/name")
        rsp.raise_for_status()
        assert yson.loads(rsp.content) == "scheduler"

    @authors("yuryalekseev")
    def test_https_monitoring_without_ca_cert_fails(self):
        """Accessing HTTPS monitoring without verifying CA certificate raises an SSL error."""
        port = self.__class__._scheduler_https_monitoring_port

        # Verification against the system CA bundle should fail since we use a self-signed CA.
        with pytest.raises(requests.exceptions.SSLError):
            requests.get(f"https://localhost:{port}/orchid/service/name")
