from yt_odin.logserver import FULLY_AVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_and_run_checks


def test_oauth_health(yt_env):
    proxy_url = yt_env.yt_client.config["proxy"]["url"]
    checks_path = make_check_dir(
        "oauth_health",
        check_options={"is_testing_environment": True},
        check_secrets={"yt_token": ""})
    storage = configure_and_run_checks(proxy_url, checks_path)
    assert abs(storage.get_service_states("oauth_health")[-1] - FULLY_AVAILABLE_STATE) <= 0.001
