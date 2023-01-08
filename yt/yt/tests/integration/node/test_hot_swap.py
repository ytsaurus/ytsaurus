from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait,
    ls, get, create,
    write_table, release_locations)

from yt.common import YtError

##################################################################


class TestHotSwap(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "tags": ["config_tag1", "config_tag2"],
        "exec_agent": {
            "slot_manager": {
                "slot_location_statistics_update_period": 100,
            },
        },
        "data_node": {
            "disk_manager_proxy": {
                "is_mock": True,
                "mock_disks": [
                    {
                        "disk_id": "disk1",
                        "device_path": "/dev/disk1",
                        "device_name": "Disk1",
                        "disk_model": "Test Model",
                        "partition_fs_labels": [
                            '/yt/disk1'
                        ],
                        "state": "failed"
                    },
                    {
                        "disk_id": "disk2",
                        "device_path": "/dev/disk2",
                        "device_name": "Disk2",
                        "disk_model": "Test Model",
                        "partition_fs_labels": [
                            '/yt/disk2'
                        ],
                        "state": "failed"
                    },
                    {
                        "disk_id": "disk3",
                        "device_path": "/dev/disk3",
                        "device_name": "Disk3",
                        "disk_model": "Test Model",
                        "partition_fs_labels": [
                            '/yt/disk3'
                        ],
                        "state": "failed"
                    },
                    {
                        "disk_id": "UNKNOWN",
                        "device_path": "/dev/UNKNOWN",
                        "device_name": "UNKNOWN",
                        "disk_model": "Test Model",
                        "partition_fs_labels": [
                            'UNKNOWN'
                        ],
                        "state": "failed"
                    }
                ],
                "mock_yt_paths": ["/yt/disk1", "/yt/disk2", "/yt/disk3", "UNKNOWN"]
            }
        }
    }

    @authors("don-dron")
    def test_release_locations(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 3

        create("table", "//tmp/t")

        def can_write():
            try:
                write_table("//tmp/t", {"a": "b"})
                return True
            except YtError:
                return False

        for node in nodes:
            wait(lambda: get("//sys/cluster_nodes/{0}/@resource_limits/user_slots".format(node)) > 0)
        wait(lambda: can_write())

        for node in ls("//sys/cluster_nodes", attributes=["chunk_locations"]):
            locations = list(node.attributes["chunk_locations"].keys())

            # TODO(don-dron): Add logic for disk failing and recovering.
            wait(lambda: len(release_locations(node, locations)) > 0)

        wait(lambda: can_write())
