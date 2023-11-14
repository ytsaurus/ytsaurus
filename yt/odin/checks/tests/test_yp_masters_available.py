import yp.tests.helpers.conftest  # noqa

from yt_odin.logserver import (FULLY_AVAILABLE_STATE, UNAVAILABLE_STATE, TERMINATED_STATE)
from yt_odin.test_helpers import (make_check_dir, configure_and_run_checks)


class TestYPMastersAvailable:
    ENABLE_SSL = True
    # Overrides big (comparing to the check timeout) default timeout.
    YP_CLIENT_CONFIG = dict(request_timeout=10 * 1000)

    def _test(self, yp_env, check_options):
        yt_cluster_url = yp_env.yt_client.config["proxy"]["url"]
        checks_path = make_check_dir("yp_masters_available", check_options)
        return configure_and_run_checks(yt_cluster_url, checks_path)

    def test_available(self, yp_env, tmpdir):
        certificate_file = tmpdir.join("YandexInternalRootCA.crt")
        certificate_file.write(yp_env.yp_instance.get_certificate())
        check_options = dict(
            yp_grpc_address=yp_env.yp_instance.yp_client_secure_grpc_address,
            yp_http_address=yp_env.yp_instance.yp_client_secure_http_address,
            yp_config=dict(enable_ssl=True, root_certificate={"file_name": str(certificate_file)}),
        )
        storage = self._test(yp_env, check_options)
        assert abs(storage.get_service_states("yp_masters_available")[0] - FULLY_AVAILABLE_STATE) <= 0.001

    def test_unavailable(self, yp_env):
        check_options = dict(
            yp_grpc_address=yp_env.yp_instance.yp_client_secure_grpc_address,
            yp_http_address="unused_address",
            yp_config=dict(enable_ssl=True),
        )
        storage = self._test(yp_env, check_options)
        assert abs(storage.get_service_states("yp_masters_available")[0] - UNAVAILABLE_STATE) <= 0.001 or \
            abs(storage.get_service_states("yp_masters_available")[0] - TERMINATED_STATE) <= 0.001
