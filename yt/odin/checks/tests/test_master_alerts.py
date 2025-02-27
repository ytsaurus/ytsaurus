from yt_odin.logserver import FULLY_AVAILABLE_STATE, UNAVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_odin, run_checks, CheckWatcher, wait


def test_master_alerts(yt_env):
    yt_client = yt_env.yt_client
    proxy_url = yt_client.config["proxy"]["url"]

    alert_path = "//sys/@master_alerts"
    assert yt_client.exists(alert_path)

    checks_path = make_check_dir("master_alerts")

    with configure_odin(proxy_url, checks_path) as odin:
        check_watcher = CheckWatcher(odin.create_db_client(), "master_alerts")

        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE

        yt_client.set("//sys/@config/qooqoo", 42)

        wait(lambda: yt_client.get("//sys/@master_alerts"))

        run_checks(odin)
        assert check_watcher.wait_new_result() == UNAVAILABLE_STATE
