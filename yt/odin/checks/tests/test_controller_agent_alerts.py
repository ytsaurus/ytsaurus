from yt_odin.logserver import FULLY_AVAILABLE_STATE, UNAVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_odin, run_checks, CheckWatcher, wait


def test_controller_agent_count(yt_env):
    yt_client = yt_env.yt_client
    proxy_url = yt_client.config["proxy"]["url"]

    agents = yt_client.list("//sys/controller_agents/instances")
    assert len(agents) > 0

    alert_path = "//sys/controller_agents/instances/{}/@alerts/0".format(str(agents[0]))
    assert not yt_client.exists(alert_path)

    checks_path = make_check_dir("controller_agent_alerts")

    yt_client.create("document", "//sys/controller_agents/config", ignore_existing=True, attributes={"value": {}})

    with configure_odin(proxy_url, checks_path) as odin:
        check_watcher = CheckWatcher(odin.create_db_client(), "controller_agent_alerts")

        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE

        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE

        yt_client.set("//sys/controller_agents/config/foo", "bar")

        wait(lambda: yt_client.exists(alert_path))

        run_checks(odin)
        assert check_watcher.wait_new_result() == UNAVAILABLE_STATE

        yt_client.remove("//sys/controller_agents/config/foo")

        wait(lambda: not yt_client.exists(alert_path))

        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE

        # Removing "default" tag should prevent check from turning into unavailable state.
        yt_client.remove("//sys/controller_agents/instances/{}/@tags".format(str(agents[0])))
        yt_client.set("//sys/controller_agents/config/foo", "bar")

        wait(lambda: yt_client.exists(alert_path))

        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE
