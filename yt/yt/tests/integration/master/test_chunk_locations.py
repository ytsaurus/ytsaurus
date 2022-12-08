from yt_env_setup import YTEnvSetup
from yt_commands import (
    authors, set, get, ls, remove, exists, wait, get_driver, raises_yt_error,
    create_medium, create, write_table, read_table,
    ban_node, unban_node)

from yt.environment.helpers import Restarter, NODES_SERVICE

##################################################################


class TestChunkLocations(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3

    @authors("babenko")
    def test_locations_created_on_node_registration(self):
        assert get("//sys/cluster_nodes/@count") == get("//sys/chunk_locations/@count")
        for node in ls("//sys/cluster_nodes", attributes=["chunk_locations"]):
            node_address = str(node)
            for location_uuid, _ in node.attributes["chunk_locations"].items():
                assert get("//sys/chunk_locations/{}/@node_address".format(location_uuid)) == node_address
                assert get("//sys/chunk_locations/{}/@uuid".format(location_uuid)) == location_uuid

    @authors("babenko")
    def test_location_offline_state(self):
        node_address = ls("//sys/cluster_nodes")[0]

        def check_locations_state(state):
            for location_uuid, _ in get("//sys/cluster_nodes/{}/@chunk_locations".format(node_address)).items():
                assert get("//sys/chunk_locations/{}/@state".format(location_uuid)) == state

        check_locations_state("online")
        ban_node(node_address, "test location offline state")
        wait(lambda: get("//sys/cluster_nodes/{0}/@state".format(node_address)) == "offline")
        check_locations_state("offline")

    @authors("babenko")
    def test_removing_node_destroys_locations(self):
        node_address = ls("//sys/cluster_nodes")[0]

        location_uuids = list(get("//sys/cluster_nodes/{}/@chunk_locations".format(node_address)).keys())
        assert len(location_uuids) > 0

        ban_node(node_address, "test removing node destroys locations")
        wait(lambda: get("//sys/cluster_nodes/{0}/@state".format(node_address)) == "offline")

        with Restarter(self.Env, NODES_SERVICE):
            remove("//sys/cluster_nodes/{}".format(node_address))

            def check(driver):
                return all(not exists("//sys/chunk_locations/{}".format(uuid), driver=driver) for uuid in location_uuids)

            for cell_index in range(0, self.get_num_secondary_master_cells() + 1):
                wait(lambda: check(get_driver(cell_index)))

    @authors("babenko")
    def test_cannot_remove_location_for_online_node(self):
        node_address = ls("//sys/cluster_nodes")[0]
        location_uuids = list(get("//sys/cluster_nodes/{}/@chunk_locations".format(node_address)).keys())
        assert len(location_uuids) > 0

        for location_uuid in location_uuids:
            with raises_yt_error("Location is online"):
                remove("//sys/chunk_locations/{}".format(location_uuid))

    @authors("babenko")
    def test_can_remove_location_for_offline_node(self):
        node_address = ls("//sys/cluster_nodes")[0]

        location_uuids = list(get("//sys/cluster_nodes/{}/@chunk_locations".format(node_address)).keys())
        assert len(location_uuids) > 0

        ban_node(node_address, "test can remove location for offline node")
        wait(lambda: get("//sys/cluster_nodes/{0}/@state".format(node_address)) == "offline")

        with Restarter(self.Env, NODES_SERVICE):
            for location_uuid in location_uuids:
                remove("//sys/chunk_locations/{}".format(location_uuid))
            unban_node(node_address)

        wait(lambda: all(exists("//sys/chunk_locations/{}".format(location_uuid)) for location_uuid in location_uuids))

    @authors("babenko")
    def test_medium_override(self):
        node_address = ls("//sys/cluster_nodes")[0]

        location_uuids = list(get("//sys/cluster_nodes/{}/@chunk_locations".format(node_address)).keys())
        assert len(location_uuids) > 0
        location_uuid = location_uuids[0]

        medium_override_path = "//sys/chunk_locations/{}/@medium_override".format(location_uuid)

        with raises_yt_error("No such medium"):
            set(medium_override_path, "nonexisting")

        assert not exists(medium_override_path)
        set(medium_override_path, "default")
        assert exists(medium_override_path)
        assert get(medium_override_path) == "default"
        remove(medium_override_path)
        assert not exists(medium_override_path)

    @authors("kvk1920")
    def test_enable_real_chunk_locations(self):
        create("table", "//tmp/t")
        table_content = [{"a": 1, "b": "abc"}, {"a": 2, "b": "bca"}]
        write_table("//tmp/t", table_content)
        nodes = ls("//sys/data_nodes")

        def check(use_imaginary_locations=False):
            assert all(get(f"//sys/data_nodes/{node}/@use_imaginary_chunk_locations") == use_imaginary_locations
                       for node in nodes)
            assert read_table("//tmp/t") == table_content

        check()
        set("//sys/@config/node_tracker/enable_real_chunk_locations", False)
        check()
        set("//sys/@config/node_tracker/enable_real_chunk_locations", True)
        check()

        with Restarter(self.Env, NODES_SERVICE):
            set("//sys/@config/node_tracker/enable_real_chunk_locations", False)

        check(use_imaginary_locations=True)
        set("//sys/@config/node_tracker/enable_real_chunk_locations", True)
        check(use_imaginary_locations=True)
        set("//sys/@config/node_tracker/enable_real_chunk_locations", False)
        check(use_imaginary_locations=True)

        with Restarter(self.Env, NODES_SERVICE):
            set("//sys/@config/node_tracker/enable_real_chunk_locations", True)

        check()
        set("//sys/@config/node_tracker/enable_real_chunk_locations", False)
        check()
        set("//sys/@config/node_tracker/enable_real_chunk_locations", True)
        check()

    @authors("kvk1920")
    def test_sharded_location_map(self):
        LOCATION_SHARD_COUNT = 256
        locations = ls("//sys/chunk_locations")
        shards = ls("//sys/chunk_location_shards")
        assert len(shards) == LOCATION_SHARD_COUNT
        sharded_locations = []
        for shard_name in shards:
            shard = ls(f"//sys/chunk_location_shards/{shard_name}")
            for location in shard:
                shard_index = int(shard_name, base=16)
                uuid_part = int(location.split('-')[-1], base=16) % LOCATION_SHARD_COUNT
                assert shard_index == uuid_part
            sharded_locations += shard
        assert sorted(locations) == sorted(sharded_locations)

    @authors("kvk1920")
    def test_sharded_location_map_attribtues(self):
        locations = {
            str(location): location.attributes
            for location in ls("//sys/chunk_locations", attributes=["state", "node_address"])
        }
        for shard_index in ls("//sys/chunk_location_shards"):
            shard = ls(f"//sys/chunk_location_shards/{shard_index}", attributes=["state", "node_address"])
            for location in shard:
                assert locations[str(location)] == location.attributes
        assert sum(map(lambda shard: shard.attributes["count"], ls("//sys/chunk_location_shards", attributes=["count"]))) == \
            get("//sys/chunk_locations/@count")

##################################################################


class TestChunkLocationsMulticell(TestChunkLocations):
    NUM_SECONDARY_MASTER_CELLS = 2

##################################################################


class TestMediumOverrideSafety(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NON_DEFAULT_MEDIUM = "ssd"

    DELTA_MASTER_CONFIG = {
        "logging": {
            "abort_on_alert": False,
        },
    }

    @classmethod
    def setup_class(cls):
        super(TestMediumOverrideSafety, cls).setup_class()
        create_medium(cls.NON_DEFAULT_MEDIUM)
        set("//sys/@config/cell_master/alert_update_period", 100)

    @authors("kvk1920")
    def test_set_medium_override(self):
        location = ls("//sys/chunk_locations", attributes=["statistics"])[0]

        def try_set_medium_override():
            for medium in (self.NON_DEFAULT_MEDIUM, "default"):
                set(f"//sys/chunk_locations/{location}/@medium_override", medium)

        disk_family = location.attributes["statistics"]["disk_family"]
        invalid_disk_family = "not_" + disk_family
        set(f"//sys/media/{self.NON_DEFAULT_MEDIUM}/@disk_family_whitelist", [disk_family])
        try_set_medium_override()
        set(f"//sys/media/{self.NON_DEFAULT_MEDIUM}/@disk_family_whitelist", [invalid_disk_family])
        with raises_yt_error("Inconsistent medium override"):
            try_set_medium_override()
        remove(f"//sys/media/{self.NON_DEFAULT_MEDIUM}/@disk_family_whitelist")
        try_set_medium_override()

    @authors("kvk1920")
    def test_location_medium(self):
        def has_alert():
            for alert in get("//sys/@master_alerts"):
                if alert["message"] == "Inconsistent medium":
                    return True
            return False

        location = ls("//sys/chunk_locations", attributes=["statistics"])[0]
        disk_family = location.attributes["statistics"]["disk_family"]
        invalid_disk_family = "not_" + disk_family
        set(f"//sys/chunk_locations/{location}/@medium_override", self.NON_DEFAULT_MEDIUM)
        with Restarter(self.Env, NODES_SERVICE):
            set(f"//sys/media/{self.NON_DEFAULT_MEDIUM}/@disk_family_whitelist", [invalid_disk_family])
        wait(has_alert)
        set(f"//sys/media/{self.NON_DEFAULT_MEDIUM}/@disk_family_whitelist", [invalid_disk_family, disk_family])
        wait(lambda: not has_alert())
