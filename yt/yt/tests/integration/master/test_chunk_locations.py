from yt_env_setup import YTEnvSetup
from yt_commands import (
    authors, set, get, ls, remove, exists, wait, get_driver, raises_yt_error,
    create_domestic_medium, create, write_table, read_table, get_singular_chunk_id,
    add_maintenance, remove_maintenance, get_account_disk_space_limit, set_account_disk_space_limit,
    set_node_banned, build_snapshot, get_active_primary_master_leader_address)

from yt_helpers import profiler_factory

from yt.environment.helpers import Restarter, NODES_SERVICE, MASTERS_SERVICE

from yt.common import YtError

import pytest

##################################################################


class TestChunkLocations(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
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

    @authors("grphil")
    def test_location_disposed_state(self):
        node_address = ls("//sys/cluster_nodes")[0]

        def check_locations_state(state):
            for location_uuid, _ in get("//sys/cluster_nodes/{}/@chunk_locations".format(node_address)).items():
                if get("//sys/chunk_locations/{}/@state".format(location_uuid)) != state:
                    return False
            return True

        wait(lambda: check_locations_state("online"))

        set("//sys/@config/node_tracker/max_locations_being_disposed", 0)
        set_node_banned(node_address, True, wait_for_master=False)

        wait(lambda: check_locations_state("offline"))

        set("//sys/@config/node_tracker/max_locations_being_disposed", 10)

        wait(lambda: get("//sys/cluster_nodes/{0}/@state".format(node_address)) == "offline")
        wait(lambda: check_locations_state("disposed"))

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
            with raises_yt_error('Location is in "online" state'):
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

        with raises_yt_error("No such medium .*"):
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
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_SECONDARY_MASTER_CELLS = 2

##################################################################


class TestChunkLocationMaintenance(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 3
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_NODES = 1

    MEDIUM_CONFIG = {
        "default": {"locations_per_node": 2},
    }

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }

    def teardown_method(self, method):
        for location in ls("//sys/chunk_locations"):
            remove_maintenance("chunk_location", location, all=True)
            remove(f"//sys/chunk_locations/{location}/@medium_override", force=True)
        super().teardown_method(method)

    def _check_requests(self, location, expected):
        expected_types = {request["type"] for request in expected.values()}

        def check_for_cell(driver):
            actual = get(f"//sys/chunk_locations/{location}/@maintenance_requests", driver=driver)
            # Each cell timestamps maintenance when it applies the replicated mutation.
            for request in actual.values():
                assert request.pop("timestamp")
            assert actual == expected

            assert get(f"//sys/chunk_locations/{location}/@decommissioned", driver=driver) == (
                "decommission" in expected_types
            )
            assert get(f"//sys/chunk_locations/{location}/@disable_write_sessions", driver=driver) == (
                "disable_write_sessions" in expected_types
            )
            return True

        for cell_index in range(self.NUM_SECONDARY_MASTER_CELLS + 1):
            driver = get_driver(cell_index)
            wait(lambda: check_for_cell(driver), ignore_exceptions=True)

    def _prepare_tables(self):
        # Separate media let each write target a particular location deterministically.
        if not exists("//sys/media/location_maintenance"):
            create_domestic_medium("location_maintenance")

        set_account_disk_space_limit(
            "tmp", get_account_disk_space_limit("tmp", "default"), "location_maintenance",
        )

        location, sibling = sorted(ls("//sys/chunk_locations"))
        set(f"//sys/chunk_locations/{sibling}/@medium_override", "location_maintenance")
        for uuid, medium in [(location, "default"), (sibling, "location_maintenance")]:
            wait(lambda: get(f"//sys/chunk_locations/{uuid}/@statistics/medium_name") == medium)

        for path, medium in [("//tmp/t", "default"), ("//tmp/sibling", "location_maintenance")]:
            create("table", path, attributes={"replication_factor": 1, "primary_medium": medium})
            write_table(path, [{"value": "before"}])

        create("table", "//tmp/probe", attributes={"replication_factor": 1})

        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get(f"#{chunk_id}/@stored_replicas")[0].attributes["location_uuid"] == location
        return location, sibling, chunk_id

    def _write(self, path):
        write_table(
            path,
            [{"value": "after"}],
            table_writer={
                "allocate_write_targets_retry_count": 1,
                "allocate_write_targets_backoff_time": 0,
            },
        )
        return True

    def _write_is_blocked(self, path):
        try:
            self._write(path)
        except YtError as error:
            if error.contains_text("Not enough target nodes"):
                return True
            raise
        return False

    @authors("evanevannnn")
    @pytest.mark.parametrize("kind", ["decommission", "disable_write_sessions"])
    def test_requests(self, kind):
        """Validate request replication and keep location flags set until the last request is removed."""
        location = ls("//sys/chunk_locations")[0]

        first_id = add_maintenance("chunk_location", location, kind, "first request")[location]
        second_id = add_maintenance("chunk_location", location, kind, "second request")[location]
        expected = {
            first_id: {"type": kind, "user": "root", "comment": "first request"},
            second_id: {"type": kind, "user": "root", "comment": "second request"},
        }
        self._check_requests(location, expected)

        assert remove_maintenance("chunk_location", location, id_=first_id) == {location: {kind: 1}}
        expected = {
            second_id: {"type": kind, "user": "root", "comment": "second request"},
        }
        self._check_requests(location, expected)

        assert remove_maintenance("chunk_location", location, id_=second_id) == {location: {kind: 1}}
        self._check_requests(location, {})

    @authors("evanevannnn")
    def test_validation(self):
        """Validate rejection of unsupported maintenance types, invalid UUIDs, and direct flag writes."""
        location = ls("//sys/chunk_locations")[0]

        for kind in ["ban", "disable_scheduler_jobs", "disable_tablet_cells"]:
            with raises_yt_error("Maintenance type is not supported for component") as error:
                add_maintenance("chunk_location", location, kind, "unsupported")
            maintenance_error = error[0].find_matching_error(
                predicate=lambda inner: inner.message == "Maintenance type is not supported for component"
            )
            assert maintenance_error is not None
            assert maintenance_error.attributes["Type"] == kind
            assert maintenance_error.attributes["Component"] == "chunk_location"
            assert maintenance_error.attributes["SupportedTypes"] == ["decommission", "disable_write_sessions"]

        for flag in ["decommissioned", "disable_write_sessions"]:
            with raises_yt_error("Builtin attribute .* cannot be set"):
                set(f"//sys/chunk_locations/{location}/@{flag}", True)

        for address, message, attribute in [
            ("not-a-uuid", "Invalid chunk location UUID", "Address"),
            ("1-2-3-4", "No such chunk location", "Address"),
        ]:
            with raises_yt_error(message) as error:
                add_maintenance("chunk_location", address, "decommission", "invalid address")
            assert error[0].find_matching_error(
                predicate=lambda inner: inner.message == message and inner.attributes.get(attribute) == address
            ) is not None

            with raises_yt_error(message) as error:
                remove_maintenance("chunk_location", address, all=True)
            assert error[0].find_matching_error(
                predicate=lambda inner: inner.message == message and inner.attributes.get(attribute) == address
            ) is not None

        self._check_requests(location, {})

    @authors("evanevannnn")
    @pytest.mark.parametrize("first_kind, second_kind", [
        ("decommission", "disable_write_sessions"),
        ("disable_write_sessions", "decommission"),
    ])
    def test_write_restrictions(self, first_kind, second_kind):
        """Validate location-specific write blocking and re-enabling after all requests are removed."""
        location, sibling, chunk_id = self._prepare_tables()

        # Both maintenance types prevent new writes to this location; the table must remain readable.
        first_id = add_maintenance("chunk_location", location, first_kind, "first restriction")[location]
        wait(lambda: self._write_is_blocked("//tmp/probe"))
        assert read_table("//tmp/t") == [{"value": "before"}]

        # Removing one request must not lift the other restriction.
        second_id = add_maintenance("chunk_location", location, second_kind, "second restriction")[location]
        remove_maintenance("chunk_location", location, id_=first_id)
        self._check_requests(location, {
            second_id: {"type": second_kind, "user": "root", "comment": "second restriction"},
        })
        assert self._write_is_blocked("//tmp/probe")

        # The sibling location and the node itself remain unrestricted.
        self._write("//tmp/sibling")
        sibling_chunk = get_singular_chunk_id("//tmp/sibling")
        assert get(f"#{sibling_chunk}/@stored_replicas")[0].attributes["location_uuid"] == sibling

        node = ls("//sys/cluster_nodes")[0]
        assert not get(f"//sys/cluster_nodes/{node}/@decommissioned")
        assert not get(f"//sys/cluster_nodes/{node}/@disable_write_sessions")

        # Removing the last request re-enables writes to the original location.
        remove_maintenance("chunk_location", location, id_=second_id)
        self._check_requests(location, {})
        wait(lambda: self._write("//tmp/probe"), ignore_exceptions=True)
        assert read_table("//tmp/probe") == [{"value": "after"}]

    @authors("evanevannnn")
    def test_snapshot_and_node_restart(self):
        """Validate that location maintenance survives master snapshot recovery and node restart."""
        location, _, _ = self._prepare_tables()

        expected = {}
        for kind in ["decommission", "disable_write_sessions"]:
            maintenance_id = add_maintenance("chunk_location", location, kind, "survive restart")[location]
            expected[maintenance_id] = {"type": kind, "user": "root", "comment": "survive restart"}
        self._check_requests(location, expected)
        wait(lambda: self._write_is_blocked("//tmp/probe"))

        for cell_index in range(self.NUM_SECONDARY_MASTER_CELLS + 1):
            driver = get_driver(cell_index)
            build_snapshot(cell_id=get("//sys/@cell_id", driver=driver))
        with Restarter(self.Env, MASTERS_SERVICE):
            pass
        self._check_requests(location, expected)

        with Restarter(self.Env, NODES_SERVICE):
            pass
        wait(lambda: self._write("//tmp/sibling"), ignore_exceptions=True)
        wait(lambda: self._write_is_blocked("//tmp/probe"))
        assert read_table("//tmp/t") == [{"value": "before"}]

        remove_maintenance("chunk_location", location, all=True)
        self._check_requests(location, {})
        wait(lambda: self._write("//tmp/probe"), ignore_exceptions=True)


##################################################################


class TestDanglingChunkLocationsCleaning(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
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
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_SECONDARY_MASTER_CELLS = 2

##################################################################


class TestPerLocationNodeDisposal(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_NODES = 6

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "node_tracker": {
            "node_disposal_tick_period": 100,
            "max_concurrent_node_unregistrations": 100,
            "profiling_period": 100,
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

    def _get_profiler_gauge(self, name):
        leader_address = get_active_primary_master_leader_address(self)
        profiler = profiler_factory().at_primary_master(leader_address)
        return profiler.gauge(name).get()

    def _wait_for_profiler_ready(self):
        wait(lambda: self._get_profiler_gauge("node_tracker/chunk_locations_being_disposed") is not None)

    def _get_locations_being_disposed_count(self):
        return self._get_profiler_gauge("node_tracker/chunk_locations_being_disposed") + self._get_profiler_gauge("node_tracker/chunk_locations_awaiting_disposal")

    def _no_locations_being_disposed(self):
        return self._get_locations_being_disposed_count() == 0

    def _check_data_nodes_are_being_disposed(self):
        for node in ls("//sys/cluster_nodes"):
            state = get("//sys/cluster_nodes/{}/@state".format(node))
            if "data" in get("//sys/cluster_nodes/{}/@flavors".format(node)):
                if state != "being_disposed":
                    return False
            else:
                if state != "offline":
                    return False
        return True

    @authors("aleksandra-zh")
    def test_nodes_dispose(self):
        self._wait_for_profiler_ready()

        with Restarter(self.Env, NODES_SERVICE):
            pass

        wait(self._no_locations_being_disposed)

    @authors("aleksandra-zh")
    def test_nodes_enter_being_disposed_state(self):
        self._wait_for_profiler_ready()

        set("//sys/@config/node_tracker/max_locations_being_disposed", 0)

        with Restarter(self.Env, NODES_SERVICE, wait_offline=False):
            wait(self._check_data_nodes_are_being_disposed)

            set("//sys/@config/node_tracker/max_locations_being_disposed", 10)

            for node in ls("//sys/cluster_nodes"):
                wait(lambda: get("//sys/cluster_nodes/{}/@state".format(node)) == "offline")

        wait(self._no_locations_being_disposed)

    @authors("aleksandra-zh")
    def test_nodes_dispose_after_snapshot(self):
        self._wait_for_profiler_ready()

        set("//sys/@config/node_tracker/max_locations_being_disposed", 0)

        node_count = 0
        for node in ls("//sys/cluster_nodes"):
            if "data" in get("//sys/cluster_nodes/{}/@flavors".format(node)):
                node_count += 1

        with Restarter(self.Env, NODES_SERVICE, wait_offline=False):
            wait(self._check_data_nodes_are_being_disposed)

            wait(lambda: self._get_locations_being_disposed_count() == node_count)

            build_snapshot(cell_id=get("//sys/@cell_id"))

            with Restarter(self.Env, MASTERS_SERVICE):
                pass

            wait(lambda: self._get_locations_being_disposed_count() == node_count)

            wait(self._check_data_nodes_are_being_disposed)

            set("//sys/@config/node_tracker/max_locations_being_disposed", 10)

            for node in ls("//sys/cluster_nodes"):
                wait(lambda: get("//sys/cluster_nodes/{}/@state".format(node)) == "offline")

        wait(self._no_locations_being_disposed)

    @authors("aleksandra-zh")
    def test_nondata_nodes_dispose_seperately(self):
        self._wait_for_profiler_ready()
        set("//sys/@config/node_tracker/immediatety_dispose_nondata_nodes", True)

        set("//sys/@config/node_tracker/max_locations_being_disposed", 0)

        with Restarter(self.Env, NODES_SERVICE, wait_offline=False):
            wait(self._check_data_nodes_are_being_disposed)

            set("//sys/@config/node_tracker/max_locations_being_disposed", 10)

            for node in ls("//sys/cluster_nodes"):
                wait(lambda: get("//sys/cluster_nodes/{}/@state".format(node)) == "offline")

        wait(self._no_locations_being_disposed)


##################################################################


class TestMediumOverrideSafety(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
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
