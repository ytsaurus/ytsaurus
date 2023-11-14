from yt_odin.logserver import FULLY_AVAILABLE_STATE, UNAVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_odin, run_checks, CheckWatcher


def test_system_quotas(yt_env):
    yt_client = yt_env.yt_client
    proxy_url = yt_client.config["proxy"]["url"]
    checks_path = make_check_dir("system_quotas")

    flat_resource = "//sys/accounts/sys/@{}/node_count"
    subkey_resource = "//sys/accounts/sys/@{}/disk_space_per_medium/default"
    tablet_resource = "//sys/tablet_cell_bundles/sys/@{}/tablet_count"

    # Prepare some tables of significant size
    def create_tables(path, account, bundle):
        yt_client.create("table", path, attributes={"account": account, "compression_codec": "none"})
        yt_client.write_table(path, [{"a": "x" * 100000} for i in range(100)])
        assert yt_client.get("{}/@resource_usage/disk_space_per_medium/default".format(path)) >= 10**7

        yt_client.create("table", path + "-dynamic", attributes={
            "schema": [{"name": "x", "type": "int64"}],
            "tablet_count": 100,
            "dynamic": True,
            "tablet_cell_bundle": bundle})

    def remove_tables(path):
        yt_client.remove(path)
        yt_client.remove(path + "-dynamic")

    create_tables("//tmp/t-sys", "sys", "sys")

    with configure_odin(proxy_url, checks_path) as odin:
        check_watcher = CheckWatcher(odin.create_db_client(), "system_quotas")

        # Ok on first run
        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE

        # Check flat resources
        old_limit = yt_client.get(flat_resource.format("resource_limits"))
        usage = yt_client.get(flat_resource.format("resource_usage"))
        limit = int(usage * 1.1)
        yt_client.set(flat_resource.format("resource_limits"), limit)

        run_checks(odin)
        assert check_watcher.wait_new_result() == UNAVAILABLE_STATE

        # Ok with old limit
        yt_client.set(flat_resource.format("resource_limits"), old_limit)
        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE

        # Check subkey resources
        old_limit = yt_client.get(subkey_resource.format("resource_limits"))
        usage = yt_client.get(subkey_resource.format("resource_usage"))
        limit = int(usage * 1.1)
        yt_client.set(subkey_resource.format("resource_limits"), limit)

        run_checks(odin)
        assert check_watcher.wait_new_result() == UNAVAILABLE_STATE

        # Ok with old limit
        yt_client.set(subkey_resource.format("resource_limits"), old_limit)
        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE

        # Check bundle resources
        old_limit = yt_client.get(tablet_resource.format("resource_limits"))
        usage = yt_client.get(tablet_resource.format("resource_usage"))
        limit = int(usage * 1.1)
        yt_client.set(tablet_resource.format("resource_limits"), limit)

        run_checks(odin)
        assert check_watcher.wait_new_result() == UNAVAILABLE_STATE

        # Ok with old limit
        yt_client.set(tablet_resource.format("resource_limits"), old_limit)
        run_checks(odin)
        assert check_watcher.wait_new_result() == FULLY_AVAILABLE_STATE

    remove_tables("//tmp/t-sys")
