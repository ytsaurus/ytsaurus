import pytest


def get_root_certificate(environment):
    return environment.yp_instance.config["secure_client_grpc_server"]["addresses"][0][
        "credentials"
    ]["pem_root_certs"]["value"]


@pytest.mark.usefixtures("yp_env_configurable")
class TestSsl(object):
    ENABLE_SSL = True
    YP_MASTER_CONFIG = {}

    @pytest.mark.parametrize("transport", ["http", "grpc"])
    def test_via_file_name(self, yp_env_configurable, transport, tmpdir):
        root_certificate = get_root_certificate(yp_env_configurable)

        root_certificate_file = tmpdir.join("YandexInternalRootCA.crt")
        root_certificate_file.write(root_certificate)

        yp_client_config = dict(
            enable_ssl=True, root_certificate=dict(file_name=str(root_certificate_file),),
        )
        with yp_env_configurable.yp_instance.create_client(
            config=yp_client_config, transport=transport
        ) as yp_client:
            assert yp_client.generate_timestamp() > 0

    @pytest.mark.parametrize("transport", ["grpc"])
    def test_via_value(self, yp_env_configurable, transport):
        root_certificate = get_root_certificate(yp_env_configurable)

        yp_client_config = dict(enable_ssl=True, root_certificate=dict(value=root_certificate,),)
        with yp_env_configurable.yp_instance.create_client(
            config=yp_client_config, transport=transport
        ) as yp_client:
            assert yp_client.generate_timestamp() > 0
