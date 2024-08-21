from yt_odin.logserver import FULLY_AVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_odin, run_checks, CheckWatcher


CHECK_OPTIONS = {
    "temp_tables_path": "//tmp",
}


def test_remote_copy(yt_env_two_clusters):
    yt_clientA = yt_env_two_clusters.environments[0].yt_client
    proxy_urlA = yt_clientA.config["proxy"]["url"]

    yt_clientB = yt_env_two_clusters.environments[1].yt_client
    proxy_urlB = yt_clientB.config["proxy"]["url"]

    yt_clientA.set("//sys/clusters", {proxy_urlB: yt_clientB.get("//sys/@cluster_connection")})
    yt_clientB.set("//sys/clusters", {proxy_urlA: yt_clientA.get("//sys/@cluster_connection")})

    checks_path = make_check_dir("remote_copy", CHECK_OPTIONS)

    with configure_odin(proxy_urlA, checks_path) as odinA:
        check_watcher = CheckWatcher(odinA.create_db_client(), "remote_copy")
        run_checks(odinA)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE

    with configure_odin(proxy_urlB, checks_path) as odinB:
        check_watcher = CheckWatcher(odinB.create_db_client(), "remote_copy")
        run_checks(odinB)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE
