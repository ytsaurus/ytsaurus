import pytest

from yp.client import YpClient

@pytest.mark.usefixtures("yp_env_configurable")
class TestSsl(object):
    ENABLE_SSL = True
    YP_MASTER_CONFIG = {}

    # TODO(ignat): grpc bindings and python should be built with the same version of crypto library.
    # It is not true in standard suite, but it would be true in arcadia and we enable this test there.
    def DISABLED_test_simple(self, yp_env_configurable):
        address = yp_env_configurable.yp_instance.yp_client_secure_grpc_address
        client = YpClient(address, config={"enable_ssl": True})
        assert client.generate_timestamp() > 0

