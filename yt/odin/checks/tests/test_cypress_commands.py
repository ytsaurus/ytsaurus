from yt_odin.logserver import FULLY_AVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_and_run_checks

TEMP_PATH = "//sys/admin/odin/cypress_commands_test"


def test_cypress_commands(yt_env):
    yt_client = yt_env.yt_client
    proxy_url = yt_env.yt_client.config["proxy"]["url"]
    checks_path = make_check_dir("cypress_commands", {"temp_path": TEMP_PATH})
    storage = configure_and_run_checks(proxy_url, checks_path)

    assert abs(storage.get_service_states("cypress_commands")[-1] - FULLY_AVAILABLE_STATE) <= 0.001
    assert yt_client.exists(TEMP_PATH)
    assert list(yt_client.list(TEMP_PATH)) == []
