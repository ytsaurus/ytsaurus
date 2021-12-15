from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE
from yt_commands import (
    get, set, raises_yt_error,
    authors, print_debug, build_master_snapshots)

from original_tests.yt.yt.tests.integration.tests.master.test_master_snapshots \
    import MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST

import os
import pytest
import yatest.common

##################################################################


def check_cluster_connection_simple():
    set("//sys/@cluster_connection", {"default_input_row_limit": "abacaba"})

    yield

    assert get("//sys/@cluster_connection") == {"default_input_row_limit": "abacaba"}
    with raises_yt_error("Cannot parse"):
        set("//sys/@cluster_connection", {"default_input_row_limit": "abacaba"})


def check_cluster_name_simple():
    set("//sys/@cluster_name", "a" * 130)

    yield

    assert get("//sys/@cluster_name") == "a" * 130
    with raises_yt_error("too long"):
        set("//sys/@cluster_name", "a" * 130)


class TestMasterSnapshotsCompatibility(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_SECONDARY_MASTER_CELLS = 3
    NUM_NODES = 5
    USE_DYNAMIC_TABLES = True

    ARTIFACT_COMPONENTS = {
        "21_3": ["master"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy", "node", "job-proxy", "exec", "tools"],
    }

    @authors("gritukan")
    def test(self):
        CHECKER_LIST = [
            check_cluster_connection_simple,
            check_cluster_name_simple,
        ] + MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST

        checker_state_list = [iter(c()) for c in CHECKER_LIST]
        for s in checker_state_list:
            next(s)

        build_master_snapshots(set_read_only=True)

        with Restarter(self.Env, MASTERS_SERVICE):
            master_path = os.path.join(self.bin_path, "ytserver-master")
            ytserver_all_trunk_path = yatest.common.binary_path("yt/yt/packages/tests_package/ytserver-all")
            print_debug("Removing {}".format(master_path))
            os.remove(master_path)
            print_debug("Symlinking {} to {}".format(ytserver_all_trunk_path, master_path))
            os.symlink(ytserver_all_trunk_path, master_path)

        for s in checker_state_list:
            with pytest.raises(StopIteration):
                next(s)
