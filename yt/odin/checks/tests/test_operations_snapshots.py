from yt_odin.logserver import FULLY_AVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_and_run_checks


CHECK_OPTIONS = {
    "critical_time_without_snapshot_threshold": 3600,
}

CHECK_NAME = "operations_snapshots"


def test_operations_snapshots(yt_env):
    proxy_url = yt_env.yt_client.config["proxy"]["url"]
    checks_path = make_check_dir(CHECK_NAME, CHECK_OPTIONS)
    storage = configure_and_run_checks(proxy_url, checks_path)
    assert abs(storage.get_service_states(CHECK_NAME)[-1] - FULLY_AVAILABLE_STATE) <= 0.001
