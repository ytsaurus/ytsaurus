from yt_env_setup import YTEnvSetup
from yt_commands import (
    authors, set, get, ls, remove, exists, wait, get_driver, raises_yt_error,
    create_domestic_medium,
    set_node_banned, build_snapshot, get_active_primary_master_leader_address)

from yt_helpers import profiler_factory

from yt.environment.helpers import Restarter, NODES_SERVICE, MASTERS_SERVICE

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
        set_node_banned(node_address, True)
        wait(lambda: get("//sys/cluster_nodes/{0}/@state".format(node_address)) == "offline")
        check_locations_state("offline")

    @authors("babenko")
    def test_removing_node_destroys_locations(self):
        node_address = ls("//sys/cluster_nodes")[0]

        location_uuids = list(get("//sys/cluster_nodes/{}/@chunk_locations".format(node_address)).keys())
        assert len(location_uuids) > 0

        set_node_banned(node_address, True)

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

        set_node_banned(node_address, True)

        with Restarter(self.Env, NODES_SERVICE):
            for location_uuid in location_uuids:
                remove("//sys/chunk_locations/{}".format(location_uuid))
            set_node_banned(node_address, False, wait_for_master=False)

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
    def test_sharded_location_map_attributes(self):
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


class TestDanglingChunkLocationsCleaning(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3

    PATCHED_NODE_CONFIGS = []

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        cls.PATCHED_NODE_CONFIGS.append(config)

    @authors("koloshmet")
    def test_clean_dangling_locations(self):
        wait(lambda: len(ls("//sys/chunk_locations")) == self.NUM_NODES)

        with Restarter(self.Env, NODES_SERVICE):
            for node_config in self.PATCHED_NODE_CONFIGS:
                node_config["data_node"]["store_locations"][0]["reset_uuid"] = True
            self.Env.rewrite_node_configs()

        def location_stats():
            locations = ls("//sys/chunk_locations", attributes=["state"])
            return len(locations), sum(1 for location in locations if location.attributes["state"] == "dangling")

        wait(lambda: location_stats() == (self.NUM_NODES * 2, self.NUM_NODES))

        cleaning_limit = 2

        location_cleaner = "//sys/@config/chunk_manager/data_node_tracker/dangling_location_cleaner/{}"
        set(location_cleaner.format("cleanup_period"), 1000)
        set(location_cleaner.format("expiration_timeout"), 2000)
        set(location_cleaner.format("max_locations_to_clean_per_iteration"), cleaning_limit)
        set(location_cleaner.format("enable"), True)

        wait(lambda: location_stats() == (self.NUM_NODES * 2 - cleaning_limit, self.NUM_NODES - cleaning_limit))

        wait(lambda: location_stats() == (self.NUM_NODES, 0))


##################################################################


class TestDanglingChunkLocationsCleaningMulticell(TestDanglingChunkLocationsCleaning):
    NUM_SECONDARY_MASTER_CELLS = 2

##################################################################


class TestPerLocationNodeDisposal(YTEnvSetup):
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_NODES = 6

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "node_tracker": {
            "enable_per_location_node_disposal": True,
            "node_disposal_tick_period": 100,
            "max_concurrent_node_unregistrations": 100,
        }
    }

    DELTA_NODE_FLAVORS = [
        ["data"],
        ["exec"],
        ["tablet"],
        ["data", "exec"],
        ["data", "tablet"],
        ["exec", "tablet"],
    ]

    def _get_nodes_being_disposed_count(self):
        leader_address = get_active_primary_master_leader_address(self)
        profiler = profiler_factory().at_primary_master(leader_address)
        return profiler.gauge("node_tracker/data_nodes_being_disposed").get()

    def _no_nodes_being_disposed(self):
        return self._get_nodes_being_disposed_count() == 0

    def _get_immediatety_dispose_nondata_nodes(self):
        config = get("//sys/@config/node_tracker")
        return config.get("immediatety_dispose_nondata_nodes", False)

    def _check_data_nodes_are_being_disposed(self):
        dispose_nondata_immediately = self._get_immediatety_dispose_nondata_nodes()
        for node in ls("//sys/cluster_nodes"):
            state = get("//sys/cluster_nodes/{}/@state".format(node))
            if "data" in get("//sys/cluster_nodes/{}/@flavors".format(node)) or not dispose_nondata_immediately:
                return state == "being_disposed"
            else:
                return state == "offline"

    @authors("aleksandra-zh")
    def test_nodes_dispose(self):
        with Restarter(self.Env, NODES_SERVICE):
            pass

        wait(self._no_nodes_being_disposed)

    @authors("aleksandra-zh")
    def test_nodes_enter_being_disposed_state(self):
        set("//sys/@config/node_tracker/testing/disable_disposal_finishing", True)

        with Restarter(self.Env, NODES_SERVICE, wait_offline=False):
            wait(self._check_data_nodes_are_being_disposed)

            set("//sys/@config/node_tracker/testing/disable_disposal_finishing", False)

            for node in ls("//sys/cluster_nodes"):
                wait(lambda: get("//sys/cluster_nodes/{}/@state".format(node)) == "offline")

        wait(self._no_nodes_being_disposed)

    @authors("aleksandra-zh")
    def test_nodes_dispose_after_snapshot(self):
        set("//sys/@config/node_tracker/testing/disable_disposal_finishing", True)
        dispose_nondata_immediately = self._get_immediatety_dispose_nondata_nodes()

        node_count = 0
        for node in ls("//sys/cluster_nodes"):
            if "data" in get("//sys/cluster_nodes/{}/@flavors".format(node)) or not dispose_nondata_immediately:
                node_count += 1

        with Restarter(self.Env, NODES_SERVICE, wait_offline=False):
            wait(self._check_data_nodes_are_being_disposed)

            wait(lambda: self._get_nodes_being_disposed_count() == node_count)

            build_snapshot(cell_id=None)

            with Restarter(self.Env, MASTERS_SERVICE):
                pass

            wait(lambda: self._get_nodes_being_disposed_count() == node_count)

            wait(self._check_data_nodes_are_being_disposed)

            set("//sys/@config/node_tracker/testing/disable_disposal_finishing", False)

            for node in ls("//sys/cluster_nodes"):
                wait(lambda: get("//sys/cluster_nodes/{}/@state".format(node)) == "offline")

        wait(self._no_nodes_being_disposed)

    @authors("aleksandra-zh")
    def test_nondata_nodes_dispose_seperately(self):
        set("//sys/@config/node_tracker/immediatety_dispose_nondata_nodes", True)

        set("//sys/@config/node_tracker/max_nodes_being_disposed", 0)
        set("//sys/@config/node_tracker/testing/disable_disposal_finishing", True)

        with Restarter(self.Env, NODES_SERVICE, wait_offline=False):
            wait(self._check_data_nodes_are_being_disposed)

            set("//sys/@config/node_tracker/testing/disable_disposal_finishing", False)
            set("//sys/@config/node_tracker/max_nodes_being_disposed", 10)

            for node in ls("//sys/cluster_nodes"):
                wait(lambda: get("//sys/cluster_nodes/{}/@state".format(node)) == "offline")

        wait(self._no_nodes_being_disposed)


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
        create_domestic_medium(cls.NON_DEFAULT_MEDIUM)

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
