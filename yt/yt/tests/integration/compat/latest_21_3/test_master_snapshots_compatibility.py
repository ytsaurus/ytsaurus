from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE
from yt_commands import (
    get, set, raises_yt_error, create_medium, ls, exists, wait,
    authors, print_debug, build_master_snapshots, update_nodes_dynamic_config)

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


def check_medium_overrides():
    update_nodes_dynamic_config({"data_node": {"medium_updater": {"period": 100}}})
    node = "//sys/data_nodes/" + ls("//sys/data_nodes")[0]
    location_uuid = get(node + "/@statistics/locations/0/location_uuid")

    media = ["my_test_medium" + str(i) for i in range(2)]
    for medium in media:
        if not exists("//sys/media/" + medium):
            create_medium(medium)
    set(node + "/@config", {"medium_overrides": {
        location_uuid: media[0],
        "0-0-0-0": media[0],
        "ffffffff-ffffffff-ffffffff-ffffffff": media[0],
    }})

    wait(lambda: get(node + "/@statistics/locations/0/medium_name") == media[0])

    yield

    assert {location_uuid: media[0]} == get(node + "/@medium_overrides")

    set(node + "/@medium_overrides", {location_uuid: media[1]})
    wait(lambda: get(node + "/@statistics/locations/0/medium_name") == media[1])


class TestMasterSnapshotsCompatibility(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_SECONDARY_MASTER_CELLS = 3
    NUM_NODES = 5
    USE_DYNAMIC_TABLES = True

    DELTA_MASTER_CONFIG = {
        "logging": {
            "abort_on_alert": False,
        },
    }

    DELTA_NODE_CONFIG = {
        "data_node": {
            "incremental_heartbeat_period": 100,
        },
        "cluster_connection": {
            "medium_directory_synchronizer": {
                "sync_period": 1
            }
        }
    }

    ARTIFACT_COMPONENTS = {
        "21_3": ["master"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy", "node", "job-proxy", "exec", "tools"],
    }

    def teardown_method(self, method):
        master_path = os.path.join(self.bin_path, "ytserver-master")
        if os.path.exists(master_path + "__BACKUP"):
            print_debug("Removing symlink {}".format(master_path))
            os.remove(master_path)
            print_debug("Renaming {} to {}".format(master_path + "__BACKUP", master_path))
            os.rename(master_path + "__BACKUP", master_path)
        super(TestMasterSnapshotsCompatibility, self).teardown_method(method)

    @authors("gritukan", "kvk1920")
    def test(self):
        CHECKER_LIST = [
            check_cluster_connection_simple,
            check_cluster_name_simple,
            check_medium_overrides,
        ] + MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST

        checker_state_list = [iter(c()) for c in CHECKER_LIST]
        for s in checker_state_list:
            next(s)

        build_master_snapshots(set_read_only=True)

        with Restarter(self.Env, MASTERS_SERVICE):
            master_path = os.path.join(self.bin_path, "ytserver-master")
            ytserver_all_trunk_path = yatest.common.binary_path("yt/yt/packages/tests_package/ytserver-all")
            print_debug("Renaming {} to {}".format(master_path, master_path + "__BACKUP"))
            os.rename(master_path, master_path + "__BACKUP")
            print_debug("Symlinking {} to {}".format(ytserver_all_trunk_path, master_path))
            os.symlink(ytserver_all_trunk_path, master_path)

        for s in checker_state_list:
            with pytest.raises(StopIteration):
                next(s)
