from yt_odin.logserver import FULLY_AVAILABLE_STATE
from yt_odin.test_helpers import (
    make_check_dir,
    configure_odin,
    run_checks,
    CheckWatcher,
)


def test_scheduler_metering_statistics(yt_env):
    yt_client = yt_env.yt_client
    proxy_url = yt_client.config["proxy"]["url"]

    checks_path = make_check_dir("scheduler_metering_statistics", {"max_allowed_lag_in_hours": 1})

    with configure_odin(proxy_url, checks_path) as odin:
        check_watcher = CheckWatcher(odin.create_db_client(), "scheduler_metering_statistics")

        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE
