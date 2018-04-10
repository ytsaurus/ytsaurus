import pytest

from yp.client import YpClient

import sys

@pytest.mark.usefixtures("yp_env_configurable")
@pytest.mark.skipif('not hasattr(sys, "extra_modules")')
class TestSsl(object):
    ENABLE_SSL = True
    YP_MASTER_CONFIG = {}

    def test_simple(self, yp_env_configurable):
        # NB: grpc bindings and python should be built with the same version of crypto library.
        # It is not true in standard suite, therefore we disable test in that case.
        address = yp_env_configurable.yp_instance.yp_client_secure_grpc_address
        root_certificate = yp_env_configurable.yp_instance.config["secure_client_grpc_server"]["addresses"][0]["credentials"]["pem_root_certs"]["value"]
        client = YpClient(address, config={"enable_ssl": True, "root_certificate": {"value": root_certificate}})
        assert client.generate_timestamp() > 0

