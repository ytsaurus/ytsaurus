from yt_odin.logserver import FULLY_AVAILABLE_STATE, PARTIALLY_AVAILABLE_STATE, UNAVAILABLE_STATE
from yt_odin.test_helpers import (
    make_check_dir,
    configure_odin,
    run_checks,
    CheckWatcher,
    wait
)

from yt.environment.helpers import Restarter, SCHEDULERS_SERVICE

from yt.common import datetime_to_string

import datetime


def _abort_scheduler_transaction(yt_client):
    transaction_aborted = False
    for tx in yt_client.list("//sys/transactions", attributes=["title"]):
        if "Scheduler lock" in tx.attributes.get("title", ""):
            yt_client.abort_transaction(tx)
            transaction_aborted = True
            break

    assert transaction_aborted


def test_regular_restart(yt_env):
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

        with Restarter(yt_env.yt_instance, SCHEDULERS_SERVICE):
            run_checks(odin)
            assert check_watcher.wait_new_result() == PARTIALLY_AVAILABLE_STATE

        wait(lambda: yt_client.get("//sys/scheduler/@connection_time") != connection_time)

        run_checks(odin)
        assert check_watcher.wait_new_result() == UNAVAILABLE_STATE


def test_connection_delay(yt_env):
    yt_client = yt_env.yt_client
    proxy_url = yt_client.config["proxy"]["url"]

    checks_path = make_check_dir("scheduler_uptime")

    with configure_odin(proxy_url, checks_path) as odin:
        check_watcher = CheckWatcher(odin.create_db_client(), "scheduler_uptime")

        run_checks(odin)
        assert check_watcher.wait_new_result() == PARTIALLY_AVAILABLE_STATE

        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE

        yt_client.set("//sys/scheduler/config/testing_options", {"master_connection_delay": {"duration": 100000, "type": "async"}})

        _abort_scheduler_transaction(yt_client)

        wait(lambda: yt_client.exists("//sys/scheduler/orchid/scheduler/service"))
        wait(lambda: not yt_client.get("//sys/scheduler/orchid/scheduler/service/connected"))

        run_checks(odin)
        assert check_watcher.wait_new_result() == PARTIALLY_AVAILABLE_STATE


def test_mute_until(yt_env):
    yt_client = yt_env.yt_client
    proxy_url = yt_client.config["proxy"]["url"]

    checks_path = make_check_dir("scheduler_uptime")

    with configure_odin(proxy_url, checks_path) as odin:
        check_watcher = CheckWatcher(odin.create_db_client(), "scheduler_uptime")

        run_checks(odin)
        assert check_watcher.wait_new_result() == PARTIALLY_AVAILABLE_STATE

        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE

        yt_client.set(
            "//sys/admin/odin/scheduler_uptime/@mute_until",
            datetime_to_string(datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=300)),
        )

        with Restarter(yt_env.yt_instance, SCHEDULERS_SERVICE):
            run_checks(odin)
            assert check_watcher.wait_new_result() == PARTIALLY_AVAILABLE_STATE

        run_checks(odin)
        assert check_watcher.wait_new_result() == PARTIALLY_AVAILABLE_STATE
