from yt_odin.logserver import FULLY_AVAILABLE_STATE, UNAVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_odin, run_checks, CheckWatcher, wait


def test_controller_agent_uptime(yt_env):
    yt_client = yt_env.yt_client
    proxy_url = yt_client.config["proxy"]["url"]

    def get_connection_time():
        agents = yt_client.list("//sys/controller_agents/instances", attributes=["connection_time"])
        assert len(agents) == 1
        return agents[0].attributes.get("connection_time")

    connection_time = get_connection_time()

    checks_path = make_check_dir("controller_agent_uptime")

    with configure_odin(proxy_url, checks_path) as odin:
        check_watcher = CheckWatcher(odin.create_db_client(), "controller_agent_uptime")

        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE

        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE

        for tx in yt_client.list("//sys/transactions", attributes=["title"]):
            if "Controller agent incarnation for" in tx.attributes.get("title", ""):
                yt_client.abort_transaction(tx)
                break
        wait(lambda: get_connection_time() != connection_time)

        run_checks(odin)
        assert check_watcher.wait_new_result() == UNAVAILABLE_STATE
