from yt_odin.logserver import FULLY_AVAILABLE_STATE, PARTIALLY_AVAILABLE_STATE, UNAVAILABLE_STATE
from yt_odin.test_helpers import (
    make_check_dir,
    configure_odin,
    run_checks,
    CheckWatcher,
    wait
)


def test_scheduler_uptime(yt_env):
    yt_client = yt_env.yt_client
    proxy_url = yt_client.config["proxy"]["url"]

    checks_path = make_check_dir("scheduler_uptime")

    with configure_odin(proxy_url, checks_path) as odin:
        check_watcher = CheckWatcher(odin.create_db_client(), "scheduler_uptime")

        run_checks(odin)
        assert check_watcher.wait_new_result() == PARTIALLY_AVAILABLE_STATE

        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE

        connection_time = yt_client.get("//sys/scheduler/@connection_time")

        transaction_aborted = False
        for tx in yt_client.list("//sys/transactions", attributes=["title"]):
            if "Scheduler lock" in tx.attributes.get("title", ""):
                yt_client.abort_transaction(tx)
                transaction_aborted = True
                break

        assert transaction_aborted

        wait(lambda: yt_client.get("//sys/scheduler/@connection_time") != connection_time)

        run_checks(odin)
        assert check_watcher.wait_new_result() == UNAVAILABLE_STATE
