from yt_odin.logserver import FULLY_AVAILABLE_STATE, PARTIALLY_AVAILABLE_STATE, UNAVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_odin, run_checks, CheckWatcher, wait

CHECK_OPTIONS = {
    "max_delta": 4,
    "yellow_delta": 3,
}


def test_tablet_cell_snapshots(yt_env):
    yt_client = yt_env.yt_client
    proxy_url = yt_client.config["proxy"]["url"]

    checks_path = make_check_dir("tablet_cell_snapshots", CHECK_OPTIONS)

    cell_id = yt_client.list("//sys/tablet_cells")[0]

    with configure_odin(proxy_url, checks_path) as odin:
        check_watcher = CheckWatcher(odin.create_db_client(), "tablet_cell_snapshots")

        def _check():
            run_checks(odin)
            return check_watcher.wait_new_result()

        def _restart():
            changelog_id = yt_client.get("//sys/tablet_cells/{}/@max_changelog_id".format(cell_id))
            yt_client.set("//sys/tablet_cell_bundles/default/@options/snapshot_compression_codec", "lz4")
            wait(lambda: yt_client.get("//sys/tablet_cells/{}/@max_changelog_id".format(cell_id)) > changelog_id)
            wait(lambda: yt_client.get("//sys/tablet_cells/{}/@health".format(cell_id)) == "good")

        assert _check() == FULLY_AVAILABLE_STATE

        _restart()
        assert _check() == PARTIALLY_AVAILABLE_STATE

        _restart()
        assert _check() == UNAVAILABLE_STATE

        yt_client.set("//sys/tablet_cell_bundles/default/@mute_tablet_cell_snapshots_check", True)
        assert _check() == PARTIALLY_AVAILABLE_STATE
        yt_client.remove("//sys/tablet_cell_bundles/default/@mute_tablet_cell_snapshots_check")
        assert _check() == UNAVAILABLE_STATE
