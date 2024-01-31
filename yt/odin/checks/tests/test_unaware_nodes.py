from yt_odin.logserver import FULLY_AVAILABLE_STATE, UNAVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_odin, run_checks, CheckWatcher
from yt.test_helpers import wait


def test_allow_unaware_nodes(yt_env):
    yt_client = yt_env.yt_client
    proxy_url = yt_client.config["proxy"]["url"]
    check_options = {"allow_unaware_nodes": True}

    checks_path = make_check_dir("unaware_nodes", check_options)

    with configure_odin(proxy_url, checks_path) as odin:
        check_watcher = CheckWatcher(odin.create_db_client(), "unaware_nodes")
        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE


def test_unaware_nodes(yt_env):
    yt_client = yt_env.yt_client
    proxy_url = yt_client.config["proxy"]["url"]

    checks_path = make_check_dir("unaware_nodes")

    with configure_odin(proxy_url, checks_path) as odin:
        check_watcher = CheckWatcher(odin.create_db_client(), "unaware_nodes")

        run_checks(odin)
        assert check_watcher.wait_new_result() == UNAVAILABLE_STATE

        yt_client.create("rack", attributes={"name": "RACK-1"})
        for node in yt_client.list("//sys/cluster_nodes"):
            yt_client.set("//sys/cluster_nodes/{}/@rack".format(node), "RACK-1")

        wait(lambda: "rack" in yt_client.list("//sys/cluster_nodes", attributes=["rack", "flavors"], read_from="cache")[0].attributes)

        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE
