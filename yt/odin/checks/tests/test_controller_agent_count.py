from yt_odin.logserver import FULLY_AVAILABLE_STATE, UNAVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir,  configure_odin,  run_checks,  CheckWatcher


def test_controller_agent_count(yt_env):
    yt_client = yt_env.yt_client
    proxy_url = yt_client.config["proxy"]["url"]

    agents = yt_client.get("//sys/controller_agents/instances")
    assert len(agents) > 0

    checks_path = make_check_dir("controller_agent_count")

    with configure_odin(proxy_url, checks_path) as odin:
        check_watcher = CheckWatcher(odin.create_db_client(), "controller_agent_count")

        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE

        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE

        yt_env.yt_instance.kill_controller_agents()

        run_checks(odin)
        assert check_watcher.wait_new_result() == UNAVAILABLE_STATE
