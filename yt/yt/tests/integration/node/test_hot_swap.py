import logging

from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE

from yt_commands import (
    authors, wait, create_access_control_object_namespace,
    ls, get, create, set, make_ace, create_access_control_object,
    create_user, exists, get_singular_chunk_id,
    update_nodes_dynamic_config, remove, write_table,
    write_file, disable_chunk_locations,
    resurrect_chunk_locations)

import time

from yt.common import YtError

from os import listdir
from os.path import isfile, join

import io
import zstandard as zstd

##################################################################

log = logging.getLogger(__name__)


class TestHotSwap(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 2
    NUM_SCHEDULERS = 1
    STORE_LOCATION_COUNT = 2

    DELTA_MASTER_CONFIG = {
        "logging": {
            "abort_on_alert": False,
        },
    }

    DELTA_NODE_CONFIG = {
        "tags": ["config_tag1", "config_tag2"],
        "exec_node": {
            "slot_manager": {
                "slot_location_statistics_update_period": 100,
            },
        },
        "data_node": {
            "abort_on_location_disabled": False,
            "incremental_heartbeat_period": 100,
            "incremental_heartbeat_period_splay": 100,
            "publish_disabled_locations": True,
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
    def test_location_disable_and_remove_job_race(self):
        update_nodes_dynamic_config({
            "data_node": {
                "remove_chunk_job" : {
                    "delay_before_start_remove_chunk": 20000
                },
                "abort_on_location_disabled": False,
                "publish_disabled_locations": True
            }
        })
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 2

        create("table", "//tmp/t")

        create("table", "//tmp/f")

        def can_write(key="a"):
            try:
                write_table("//tmp/f", [{"key": key}])
                return True
            except YtError:
                return False

        for node in nodes:
            wait(lambda: exists("//sys/cluster_nodes/{0}/orchid/restart_manager".format(node)))
            wait(lambda: not get("//sys/cluster_nodes/{0}/orchid/restart_manager/need_restart".format(node)))
            wait(lambda: get("//sys/cluster_nodes/{0}/@resource_limits/user_slots".format(node)) > 0)
        wait(lambda: can_write())

        ys = [{"key": "x" * (4 * 1024)} for i in range(1024)]
        write_table("//tmp/t", ys)

        for node in ls("//sys/cluster_nodes", attributes=["chunk_locations"]):
            def chunk_count():
                count = 0
                for location_uuid, _ in get("//sys/cluster_nodes/{}/@chunk_locations".format(node)).items():
                    count = count + get("//sys/chunk_locations/{}/@statistics/chunk_count".format(location_uuid))
                return count
            wait(lambda: chunk_count() != 0)

        remove("//tmp/t")

        for node_id in ls("//sys/cluster_nodes"):
            wait(lambda: get("//sys/cluster_nodes/{}/@resource_usage/removal_slots".format(node_id)) != 0)

        for node in ls("//sys/cluster_nodes", attributes=["chunk_locations"]):
            for location_uuid, _ in get("//sys/cluster_nodes/{}/@chunk_locations".format(node)).items():
                wait(lambda: len(disable_chunk_locations(node, [location_uuid])) > 0)

        wait(lambda: not can_write())

        for node_id in ls("//sys/cluster_nodes"):
            wait(lambda: get("//sys/cluster_nodes/{}/@resource_usage/removal_slots".format(node_id)) == 0)

        for node in ls("//sys/cluster_nodes", attributes=["chunk_locations"]):
            for location_uuid, _ in get("//sys/cluster_nodes/{}/@chunk_locations".format(node)).items():
                wait(lambda: len(resurrect_chunk_locations(node, [location_uuid])) > 0)

        wait(lambda: can_write())

    @authors("don-dron")
    def test_resurrect_chunk_locations(self):
        update_nodes_dynamic_config({"data_node": {"abort_on_location_disabled": False, "publish_disabled_locations": True}})
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 2

        create("file", "//tmp/f")

        def can_write(key="a"):
            try:
                write_file("//tmp/f", str.encode(key))
                return True
            except YtError:
                return False

        for node in nodes:
            wait(lambda: exists("//sys/cluster_nodes/{0}/orchid/restart_manager".format(node)))
            wait(lambda: not get("//sys/cluster_nodes/{0}/orchid/restart_manager/need_restart".format(node)))
            wait(lambda: get("//sys/cluster_nodes/{0}/@resource_limits/user_slots".format(node)) > 0)
        wait(lambda: can_write())

        for node in ls("//sys/cluster_nodes", attributes=["chunk_locations"]):
            def chunk_count():
                count = 0
                for location_uuid, _ in get("//sys/cluster_nodes/{}/@chunk_locations".format(node)).items():
                    count = count + get("//sys/chunk_locations/{}/@statistics/chunk_count".format(location_uuid))
                return count
            wait(lambda: chunk_count() != 0)

        for node in ls("//sys/cluster_nodes", attributes=["chunk_locations"]):
            for location_uuid, _ in get("//sys/cluster_nodes/{}/@chunk_locations".format(node)).items():
                wait(lambda: len(disable_chunk_locations(node, [location_uuid])) > 0)

        # Test second try - must returns empty lists
        for node in ls("//sys/cluster_nodes", attributes=["chunk_locations"]):
            for location_uuid, _ in get("//sys/cluster_nodes/{}/@chunk_locations".format(node)).items():
                wait(lambda: len(disable_chunk_locations(node, [location_uuid])) == 0)

        for node in ls("//sys/cluster_nodes", attributes=["chunk_locations"]):
            for location_uuid, _ in get("//sys/cluster_nodes/{}/@chunk_locations".format(node)).items():
                wait(lambda: not get("//sys/chunk_locations/{}/@statistics/enabled".format(location_uuid)))
                wait(lambda: get("//sys/chunk_locations/{}/@statistics/session_count".format(location_uuid)) == 0)
                wait(lambda: get("//sys/chunk_locations/{}/@statistics/chunk_count".format(location_uuid)) == 0)

        wait(lambda: not can_write())

        for node in ls("//sys/cluster_nodes", attributes=["chunk_locations"]):
            for location_uuid, _ in get("//sys/cluster_nodes/{}/@chunk_locations".format(node)).items():
                wait(lambda: len(resurrect_chunk_locations(node, [location_uuid])) > 0)

        for node in ls("//sys/cluster_nodes", attributes=["chunk_locations"]):
            for location_uuid, _ in get("//sys/cluster_nodes/{}/@chunk_locations".format(node)).items():
                wait(lambda: get("//sys/chunk_locations/{}/@statistics/enabled".format(location_uuid)))

        # Test second try - must returns empty lists
        for node in ls("//sys/cluster_nodes", attributes=["chunk_locations"]):
            for location_uuid, _ in get("//sys/cluster_nodes/{}/@chunk_locations".format(node)).items():
                wait(lambda: len(resurrect_chunk_locations(node, [location_uuid])) == 0)

        wait(lambda: can_write())

        create_user("test_user")

        acl = [make_ace("allow", "test_user", "use")]

        def check():
            node = ls("//sys/cluster_nodes", attributes=["chunk_locations"])[0]
            try:
                disable_chunk_locations(node, [], authenticated_user="test_user")
                return True
            except Exception:
                return False

        wait(lambda: not check())

        try:
            create_access_control_object_namespace(name="admin_commands")
        except Exception:
            pass
        try:
            create_access_control_object(name="disable_chunk_locations", namespace="admin_commands")
        except Exception:
            pass

        wait(lambda: exists("//sys/access_control_object_namespaces/admin_commands/disable_chunk_locations"))
        set("//sys/access_control_object_namespaces/admin_commands/disable_chunk_locations/principal/@acl", acl)

        wait(lambda: check())

        set("//sys/access_control_object_namespaces/admin_commands/disable_chunk_locations/principal/@acl", [])

        wait(lambda: not check())

        for node in ls("//sys/cluster_nodes", attributes=["chunk_locations"]):
            for location_uuid, _ in get("//sys/cluster_nodes/{}/@chunk_locations".format(node)).items():
                wait(lambda: len(disable_chunk_locations(node, [location_uuid])) > 0)

        for node in ls("//sys/cluster_nodes", attributes=["chunk_locations"]):
            for location_uuid, _ in get("//sys/cluster_nodes/{}/@chunk_locations".format(node)).items():
                wait(lambda: not get("//sys/chunk_locations/{}/@statistics/enabled".format(location_uuid)))
                wait(lambda: get("//sys/chunk_locations/{}/@statistics/session_count".format(location_uuid)) == 0)
                wait(lambda: get("//sys/chunk_locations/{}/@statistics/chunk_count".format(location_uuid)) == 0)

        wait(lambda: not can_write())

        for node in ls("//sys/cluster_nodes", attributes=["chunk_locations"]):
            for location_uuid, _ in get("//sys/cluster_nodes/{}/@chunk_locations".format(node)).items():
                wait(lambda: len(resurrect_chunk_locations(node, [location_uuid])) > 0)

        for node in ls("//sys/cluster_nodes", attributes=["chunk_locations"]):
            for location_uuid, _ in get("//sys/cluster_nodes/{}/@chunk_locations".format(node)).items():
                wait(lambda: get("//sys/chunk_locations/{}/@statistics/enabled".format(location_uuid)))

        wait(lambda: can_write())

    @authors("don-dron")
    def test_chunk_duplicate_resurrect(self):
        update_nodes_dynamic_config({"data_node": {"abort_on_location_disabled": False, "publish_disabled_locations": True}})
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 2
        node = nodes[0]

        create("file", "//tmp/f")

        def can_write(key="a"):
            try:
                write_file("//tmp/f", str.encode(key))
                return True
            except YtError:
                return False

        for node in nodes:
            wait(lambda: exists("//sys/cluster_nodes/{0}/orchid/restart_manager".format(node)))
            wait(lambda: not get("//sys/cluster_nodes/{0}/orchid/restart_manager/need_restart".format(node)))
            wait(lambda: get("//sys/cluster_nodes/{0}/@resource_limits/user_slots".format(node)) > 0)
        wait(lambda: can_write())

        chunk_id = get_singular_chunk_id("//tmp/f")

        def find_location_chunks(location):
            chunks = []
            for chunk_id in get("//sys/chunks"):
                replicas = get("//sys/chunks/{}/@stored_replicas".format(chunk_id))

                for replica in replicas:
                    chunk_location = replica.attributes["location_uuid"]

                    if chunk_location == location:
                        chunks.append(chunk_id)

            return chunks

        for node in ls("//sys/cluster_nodes", attributes=["chunk_locations"]):
            def chunk_count():
                count = 0
                for location_uuid, _ in get("//sys/cluster_nodes/{}/@chunk_locations".format(node)).items():
                    count = count + get("//sys/chunk_locations/{}/@statistics/chunk_count".format(location_uuid))
                return count
            wait(lambda: chunk_count() != 0)

        locations = get("//sys/cluster_nodes/{}/@chunk_locations".format(node))
        location_uuids = list(locations.keys())

        assert len(locations) == 2

        location_uuid = location_uuids[0] if len(find_location_chunks(location_uuids[0])) > 0 else location_uuids[1]
        other_location_uuid = location_uuids[1] if location_uuid == location_uuids[0] else location_uuids[0]
        assert location_uuid != other_location_uuid

        chunk_id = find_location_chunks(location_uuid)[0]
        wait(lambda: len(get("//sys/chunks/{}/@stored_replicas".format(chunk_id))) == 2)

        wait(lambda: len(disable_chunk_locations(node, [location_uuid])) > 0)
        wait(lambda: not get("//sys/chunk_locations/{}/@statistics/enabled".format(location_uuid)))

        wait(lambda: len(get("//sys/chunks/{}/@stored_replicas".format(chunk_id))) == 2)
        wait(lambda: get("//sys/chunk_locations/{}/@statistics/session_count".format(location_uuid)) == 0)
        wait(lambda: get("//sys/chunk_locations/{}/@statistics/session_count".format(other_location_uuid)) == 0)
        wait(lambda: len(find_location_chunks(location_uuid)) == 0)
        wait(lambda: chunk_id in find_location_chunks(other_location_uuid))

        wait(lambda: len(resurrect_chunk_locations(node, [location_uuid])) > 0)
        wait(lambda: get("//sys/chunk_locations/{}/@statistics/enabled".format(location_uuid)))

        wait(lambda: chunk_id not in find_location_chunks(location_uuid) and chunk_id in find_location_chunks(other_location_uuid))

    @authors("don-dron")
    def DISABLED_test_lost_location(self):
        update_nodes_dynamic_config({"data_node": {"abort_on_location_disabled": False, "publish_disabled_locations": True}})
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 2

        create("file", "//tmp/f")

        def can_write(key="a"):
            try:
                write_file("//tmp/f", str.encode(key))
                return True
            except YtError:
                return False

        for node in nodes:
            wait(lambda: exists("//sys/cluster_nodes/{0}/orchid/restart_manager".format(node)))
            wait(lambda: not get("//sys/cluster_nodes/{0}/orchid/restart_manager/need_restart".format(node)))
            wait(lambda: get("//sys/cluster_nodes/{0}/@resource_limits/user_slots".format(node)) > 0)
        wait(lambda: can_write())

        for node in ls("//sys/cluster_nodes", attributes=["chunk_locations"]):
            def chunk_count():
                count = 0
                for location_uuid, _ in get("//sys/cluster_nodes/{}/@chunk_locations".format(node)).items():
                    wait(lambda: can_write("//sys/chunk_locations/{}/@statistics/chunk_count".format(location_uuid)))
                    count = count + get("//sys/chunk_locations/{}/@statistics/chunk_count".format(location_uuid))
                return count
            wait(lambda: chunk_count() != 0)

        node = ls("//sys/cluster_nodes", attributes=["chunk_locations"])[0]
        locations = get("//sys/cluster_nodes/{}/@chunk_locations".format(node))
        location_uuids = list(locations.keys())

        for uuid in location_uuids:
            wait(lambda: get("//sys/chunk_locations/{}/@statistics/chunk_count".format(uuid)) > 0)

        location_uuid = location_uuids[0]

        wait(lambda: len(disable_chunk_locations(node, [location_uuid])) > 0)
        wait(lambda: not get("//sys/chunk_locations/{}/@statistics/enabled".format(location_uuid)))
        wait(lambda: get("//sys/chunk_locations/{}/@statistics/session_count".format(location_uuid)) == 0)
        wait(lambda: get("//sys/chunk_locations/{}/@statistics/chunk_count".format(location_uuid)) == 0)

        # Disable disabled location publishing
        update_nodes_dynamic_config({"data_node": {"abort_on_location_disabled": False, "publish_disabled_locations": False}})

        with Restarter(self.Env, NODES_SERVICE):
            time.sleep(2)
            pass

        wait(lambda: not get("//sys/chunk_locations/{}/@statistics/enabled".format(location_uuid)))
        wait(lambda: get("//sys/chunk_locations/{}/@statistics/session_count".format(location_uuid)) == 0)
        wait(lambda: get("//sys/chunk_locations/{}/@statistics/chunk_count".format(location_uuid)) == 0)
        wait(lambda: len(resurrect_chunk_locations(node, [location_uuid])) > 0)
        wait(lambda: get("//sys/chunk_locations/{}/@statistics/enabled".format(location_uuid)))

        wait(lambda: can_write())
        time.sleep(2)

        def contains_alert(logs_path):
            master_files = [join(logs_path, f) for f in listdir(logs_path) if "master" in f and ".log.zst" in f and isfile(join(logs_path, f))]

            for file_path in master_files:
                with open(file_path, "rb") as log_file:
                    decompressor = zstd.ZstdDecompressor()
                    binary_reader = decompressor.stream_reader(log_file, read_size=8192)
                    text_stream = io.TextIOWrapper(binary_reader, encoding='utf-8')
                    for line in text_stream:
                        if "heartbeat with dangling location" in line:
                            return True

            return False

        def check_alert():
            res = contains_alert(self.path_to_run + "/logs")

            if res:
                return res
            else:
                time.sleep(1)
                return res

        wait(lambda: check_alert())
