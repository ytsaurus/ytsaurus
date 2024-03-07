from yt_odin.logserver import FULLY_AVAILABLE_STATE, PARTIALLY_AVAILABLE_STATE, UNAVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_odin, run_checks, CheckWatcher, wait
from yt.wrapper.admin_commands import add_maintenance, remove_maintenance


def test_tablet_cells(yt_env):
    yt_client = yt_env.yt_client
    proxy_url = yt_client.config["proxy"]["url"]

    checks_path = make_check_dir("tablet_cells")

    node = yt_client.list("//sys/cluster_nodes")[0]

    with configure_odin(proxy_url, checks_path) as odin:
        check_watcher = CheckWatcher(odin.create_db_client(), "tablet_cells")

        def _check():
            run_checks(odin)
            return check_watcher.wait_new_result()

        assert _check() == FULLY_AVAILABLE_STATE

        maintenance_id = add_maintenance("cluster_node", node, "disable_tablet_cells", "", client=yt_client)[node]

        wait(lambda: _check() == PARTIALLY_AVAILABLE_STATE)
        assert _check() == UNAVAILABLE_STATE

        yt_client.set("//sys/tablet_cell_bundles/default/@mute_tablet_cells_check", True)
        assert _check() == PARTIALLY_AVAILABLE_STATE
        yt_client.remove("//sys/tablet_cell_bundles/default/@mute_tablet_cells_check")
        assert _check() == UNAVAILABLE_STATE

        remove_maintenance("cluster_node", node, id=maintenance_id, client=yt_client)
        wait(lambda: _check() == FULLY_AVAILABLE_STATE)
