from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    NODES_SERVICE,
    MASTERS_SERVICE
)

from yt_sequoia_helpers import (
    select_rows_from_ground, lookup_rows_in_ground, mangle_sequoia_path, get_ground_driver
)

from yt.sequoia_tools import DESCRIPTORS

from yt_commands import (
    authors, commit_transaction, create, get, raises_yt_error, remove, get_singular_chunk_id, write_table, read_table, wait,
    exists, create_domestic_medium, ls, set, get_account_disk_space_limit, set_account_disk_space_limit,
    link, build_master_snapshots, start_transaction, abort_transaction, get_active_primary_master_leader_address,
    sync_mount_table, sync_unmount_table, sync_compact_table)

from yt.wrapper import yson

from yt_helpers import profiler_factory

import pytest

##################################################################


def sequoia_tables_empty():
    unapproved_replicas_path = DESCRIPTORS.unapproved_chunk_replicas.get_default_path()
    ground_driver = get_ground_driver()

    sync_unmount_table(unapproved_replicas_path, driver=ground_driver)

    set("{}/@forced_compaction_revision".format(unapproved_replicas_path), 1, driver=ground_driver)
    sync_compact_table(unapproved_replicas_path, driver=ground_driver)

    sync_mount_table(unapproved_replicas_path, driver=ground_driver)

    return all(
        select_rows_from_ground(f"* from [{table.get_default_path()}]") == []
        for table in DESCRIPTORS.get_group("chunk_tables"))


class TestSequoiaReplicas(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    USE_SEQUOIA = True
    NUM_SECONDARY_MASTER_CELLS = 0

    NUM_NODES = 9

    TABLE_MEDIUM_1 = "table_medium_1"
    TABLE_MEDIUM_2 = "table_medium_2"

    DELTA_MASTER_CONFIG = {
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True,
        },
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "sequoia_manager": {
            # Making sure we do not use it for replicas.
            "enable": False
        },
        "chunk_manager": {
            "replica_approve_timeout": 5000,
            "sequoia_chunk_replicas": {
                "replicas_percentage": 100,
                "fetch_replicas_from_sequoia": True
            }
        }
    }

    def _is_purgatory_empty(self):
        total_purgatory_size = 0
        secondary_masters = get("//sys/secondary_masters")
        for cell_tag in secondary_masters:
            for address in secondary_masters[cell_tag]:
                if not get("//sys/secondary_masters/{}/{}/orchid/monitoring/hydra/active_leader".format(cell_tag, address)):
                    continue
                profiler = profiler_factory().at_secondary_master(cell_tag, address)
                total_purgatory_size += profiler.gauge("chunk_server/sequoia_chunk_purgatory_size").get()

        profiler = profiler_factory().at_primary_master(get_active_primary_master_leader_address(self))
        total_purgatory_size += profiler.gauge("chunk_server/sequoia_chunk_purgatory_size").get()
        return total_purgatory_size == 0

    @classmethod
    def setup_class(cls):
        super(TestSequoiaReplicas, cls).setup_class()

        for media in ls("//sys/media"):
            set("//sys/media/{}/@enable_sequoia_replicas".format(media), False)

        create_domestic_medium(cls.TABLE_MEDIUM_1)
        create_domestic_medium(cls.TABLE_MEDIUM_2)
        set("//sys/media/{}/@enable_sequoia_replicas".format(cls.TABLE_MEDIUM_1), True)
        set("//sys/media/{}/@enable_sequoia_replicas".format(cls.TABLE_MEDIUM_2), False)

        mediums = [cls.TABLE_MEDIUM_1, cls.TABLE_MEDIUM_2, "default"]
        iter_mediums = 0
        cls.table_node_indexes = []
        addresses_to_index = {cls.Env.get_node_address(index) : index for index in range(0, cls.NUM_NODES)}

        for node_address in ls("//sys/cluster_nodes"):
            location_uuids = list(get("//sys/cluster_nodes/{}/@chunk_locations".format(node_address)).keys())
            for location_uuid in location_uuids:
                medium_override_path = "//sys/chunk_locations/{}/@medium_override".format(location_uuid)
                set(medium_override_path, mediums[iter_mediums])
                if mediums[iter_mediums] == cls.TABLE_MEDIUM_1:
                    cls.table_node_indexes.append(addresses_to_index[node_address])
                iter_mediums = (iter_mediums + 1) % len(mediums)
        assert len(cls.table_node_indexes) == 3

    def teardown_method(self, method):
        wait(sequoia_tables_empty)
        super(TestSequoiaReplicas, self).teardown_method(method)

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("erasure_codec", ["none", "lrc_12_2_2"])
    def test_chunk_replicas_node_offline1(self, erasure_codec):
        set("//sys/@config/chunk_manager/sequoia_chunk_replicas/enable", True)
        set("//sys/accounts/tmp/@resource_limits/disk_space_per_medium/{}".format(self.TABLE_MEDIUM_1), 10000)

        create("table", "//tmp/t",  attributes={"primary_medium": self.TABLE_MEDIUM_1, "erasure_codec": erasure_codec})

        write_table("//tmp/t", [{"x": 1}])
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")

        desired_replica_count = 3 if erasure_codec == "none" else 16
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")) == 1)
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.location_replicas.get_default_path()}]")) == desired_replica_count)

        rows = select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")
        assert len(rows) == 1
        assert len(rows[0]["stored_replicas"]) == desired_replica_count

        with Restarter(self.Env, NODES_SERVICE, indexes=self.table_node_indexes):
            assert len(select_rows_from_ground(f"* from [{DESCRIPTORS.location_replicas.get_default_path()}]")) == 0
            assert len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}] where yson_length(stored_replicas) > 0")) == 0

        remove("//tmp/t")

        wait(lambda: not exists("#{}".format(chunk_id)))
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")) == 0)
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.location_replicas.get_default_path()}]")) == 0)

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("erasure_codec", ["none", "lrc_12_2_2"])
    def test_chunk_replicas_node_offline2(self, erasure_codec):
        def no_chunk():
            for node in ls("//sys/cluster_nodes"):
                if chunk_id in ls("//sys/cluster_nodes/{0}/orchid/data_node/stored_chunks".format(node)):
                    return False

            return True

        set("//sys/@config/chunk_manager/sequoia_chunk_replicas/enable", True)
        set("//sys/accounts/tmp/@resource_limits/disk_space_per_medium/{}".format(self.TABLE_MEDIUM_1), 10000)
        create("table", "//tmp/t",  attributes={"primary_medium": self.TABLE_MEDIUM_1, "erasure_codec": erasure_codec})

        write_table("//tmp/t", [{"x": 1}])
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")

        desired_replica_count = 3 if erasure_codec == "none" else 16
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")) == 1)
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.location_replicas.get_default_path()}]")) == desired_replica_count)

        with Restarter(self.Env, NODES_SERVICE, indexes=self.table_node_indexes):
            remove("//tmp/t")
            wait(lambda: not exists("#{}".format(chunk_id)))
            wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")) == 0)
            wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.location_replicas.get_default_path()}]")) == 0)
            wait(self._is_purgatory_empty)

        wait(no_chunk)
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")) == 0)
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.location_replicas.get_default_path()}]")) == 0)

    @authors("aleksandra-zh")
    def test_chunk_replicas_purgatory(self):
        set("//sys/@config/chunk_manager/sequoia_chunk_replicas/enable", True)
        set("//sys/accounts/tmp/@resource_limits/disk_space_per_medium/{}".format(self.TABLE_MEDIUM_1), 10000)
        create("table", "//tmp/t",  attributes={"primary_medium": self.TABLE_MEDIUM_1})

        write_table("//tmp/t", [{"x": 1}])
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")

        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")) == 1)
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.location_replicas.get_default_path()}]")) == 3)

        set("//sys/@config/chunk_manager/sequoia_chunk_replicas/enable_chunk_purgatory", False)
        remove("//tmp/t")
        wait(lambda: not exists("#{}".format(chunk_id)))
        wait(lambda: not self._is_purgatory_empty())

        with Restarter(self.Env, NODES_SERVICE, indexes=self.table_node_indexes):
            pass

        def no_destroyed_replicas():
            for node_address in ls("//sys/cluster_nodes"):
                if get("//sys/cluster_nodes/{0}/@destroyed_chunk_replica_count".format(node_address)) > 0:
                    return False
            return True
        wait(no_destroyed_replicas)

        def no_sequoia_chunk_replicas():
            for node_address in ls("//sys/cluster_nodes"):
                chunk_replica_count = get("//sys/cluster_nodes/{0}/@chunk_replica_count".format(node_address))
                if chunk_replica_count.get(self.TABLE_MEDIUM_1, 0) > 0:
                    return False
            return True
        wait(no_sequoia_chunk_replicas)

        set("//sys/@config/chunk_manager/sequoia_chunk_replicas/enable_chunk_purgatory", True)

        wait(self._is_purgatory_empty)
        wait(no_destroyed_replicas)

    @authors("aleksandra-zh")
    def test_replication(self):
        set("//sys/@config/chunk_manager/sequoia_chunk_replicas/enable", True)
        set("//sys/accounts/tmp/@resource_limits/disk_space_per_medium/{}".format(self.TABLE_MEDIUM_1), 10000)
        create("table", "//tmp/t",  attributes={"primary_medium": self.TABLE_MEDIUM_1, "replication_factor": 2})

        write_table("//tmp/t", [{"x": 1}], table_writer={"upload_replication_factor": 2})
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")) == 1)
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.location_replicas.get_default_path()}]")) == 2)

        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")

        set("//tmp/t/@replication_factor", 3)

        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_id))) == 3)
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")) == 1)
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.location_replicas.get_default_path()}]")) == 3)

        remove("//tmp/t")

    @authors("aleksandra-zh")
    def test_last_seen_replicas(self):
        set("//sys/@config/chunk_manager/sequoia_chunk_replicas/enable", True)
        set("//sys/accounts/tmp/@resource_limits/disk_space_per_medium/{}".format(self.TABLE_MEDIUM_1), 10000)

        create("table", "//tmp/t",  attributes={"primary_medium": self.TABLE_MEDIUM_1})

        write_table("//tmp/t", [{"x": 1}])
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")

        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")) == 1)
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.location_replicas.get_default_path()}]")) == 3)

        rows = select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")
        assert len(rows) == 1
        assert len(rows[0]["last_seen_replicas"]) == 3

        assert read_table("//tmp/t") == [{"x": 1}]
        chunk_id = get_singular_chunk_id("//tmp/t")

        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_id))) == 3)
        wait(lambda: len(get("#{}/@last_seen_replicas".format(chunk_id))) == 3)

        remove("//tmp/t")

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("erasure_codec", ["none", "lrc_12_2_2"])
    def test_refresh(self, erasure_codec):
        set("//sys/@config/chunk_manager/sequoia_chunk_replicas/enable", True)
        set("//sys/accounts/tmp/@resource_limits/disk_space_per_medium/{}".format(self.TABLE_MEDIUM_1), 10000)

        create("table", "//tmp/t",  attributes={"primary_medium": self.TABLE_MEDIUM_1, "erasure_codec": erasure_codec})

        write_table("//tmp/t", [{"x": 1}])
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")

        desired_replica_count = 3 if erasure_codec == "none" else 16

        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")) == 1)
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.location_replicas.get_default_path()}]")) == desired_replica_count)

        with Restarter(self.Env, NODES_SERVICE):
            wait(lambda: chunk_id in get("//sys/lost_chunks"))
            set("//sys/@config/chunk_manager/sequoia_chunk_replicas/store_sequoia_replicas_on_master", False)

        wait(lambda: chunk_id not in get("//sys/lost_chunks"))

        set("//sys/@config/chunk_manager/sequoia_chunk_replicas/processed_removed_sequoia_replicas_on_master", False)

        with Restarter(self.Env, NODES_SERVICE):
            wait(lambda: chunk_id in get("//sys/lost_chunks"))

        remove("//tmp/t")

    @authors("aleksandra-zh")
    def test_unapproved(self):
        set("//sys/@config/chunk_manager/sequoia_chunk_replicas/enable", True)
        ground_driver = get_ground_driver()
        unapproved_replicas_path = DESCRIPTORS.unapproved_chunk_replicas.get_default_path()
        sync_unmount_table(unapproved_replicas_path, driver=ground_driver)
        set("{}/@mount_config/min_data_versions".format(unapproved_replicas_path), 1, driver=ground_driver)
        sync_mount_table(unapproved_replicas_path, driver=ground_driver)
        set("//sys/accounts/tmp/@resource_limits/disk_space_per_medium/{}".format(self.TABLE_MEDIUM_1), 10000)

        create("table", "//tmp/t",  attributes={"primary_medium": self.TABLE_MEDIUM_1})

        write_table("//tmp/t", [{"x": 1}])
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")

        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")) == 1)
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.location_replicas.get_default_path()}]")) == 3)

        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.unapproved_chunk_replicas.get_default_path()}]")) == 1)

        set("//sys/@config/chunk_manager/replica_approve_timeout", 100000)
        assert len(get("#{}/@unapproved_sequoia_replicas".format(chunk_id))) > 0
        set("//sys/@config/chunk_manager/replica_approve_timeout", 0)
        assert len(get("#{}/@unapproved_sequoia_replicas".format(chunk_id))) == 0

        set("//sys/@config/chunk_manager/replica_approve_timeout", 5000)

        sync_unmount_table(unapproved_replicas_path, driver=ground_driver)
        set("{}/@mount_config/min_data_versions".format(unapproved_replicas_path), 0, driver=ground_driver)
        sync_mount_table(unapproved_replicas_path, driver=ground_driver)

        remove("//tmp/t")

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("erasure_codec", ["none", "lrc_12_2_2"])
    def test_sequoia_refresh(self, erasure_codec):
        set("//sys/@config/chunk_manager/sequoia_chunk_replicas/enable", True)
        set("//sys/@config/chunk_manager/sequoia_chunk_replicas/enable_sequoia_chunk_refresh", True)
        set("//sys/accounts/tmp/@resource_limits/disk_space_per_medium/{}".format(self.TABLE_MEDIUM_1), 10000)

        set("//sys/@config/chunk_manager/testing/disable_sequoia_chunk_refresh", True)
        create("table", "//tmp/t",  attributes={"primary_medium": self.TABLE_MEDIUM_1, "erasure_codec": erasure_codec})

        write_table("//tmp/t", [{"x": 1}])

        chunk_id = get_singular_chunk_id("//tmp/t")
        parts = chunk_id.split("-")
        cell_tag = parts[2][:-4]

        queue_path = DESCRIPTORS.chunk_refresh_queue.get_default_path() + "_" + str(int(cell_tag, 16))
        wait(lambda: len(select_rows_from_ground(f"* from [{queue_path}] where [$tablet_index] >= -1")) > 0)

        set("//sys/@config/chunk_manager/testing/disable_sequoia_chunk_refresh", False)

        with Restarter(self.Env, NODES_SERVICE):
            wait(lambda: chunk_id in get("//sys/lost_chunks"))

        wait(lambda: chunk_id not in get("//sys/lost_chunks"))
        wait(lambda: len(select_rows_from_ground(f"* from [{queue_path}] where [$tablet_index] >= -1")) == 0)

        remove("//tmp/t")


class TestOnlySequoiaReplicas(TestSequoiaReplicas):
    ENABLE_MULTIDAEMON = False  # There are component restarts.

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "sequoia_manager": {
            # Making sure we do not use it for replicas.
            "enable": False
        },
        "chunk_manager": {
            "replica_approve_timeout": 5000,
            "sequoia_chunk_replicas": {
                "replicas_percentage": 100,
                "fetch_replicas_from_sequoia": True,
                "store_sequoia_replicas_on_master": False,
                "processed_removed_sequoia_replicas_on_master": False
            }
        }
    }

    @classmethod
    def setup_class(cls):
        super(TestOnlySequoiaReplicas, cls).setup_class()

    @authors("aleksandra-zh")
    def test_empty_sequoia_handler(self):
        set("//sys/@config/chunk_manager/sequoia_chunk_replicas/enable", True)

        table = "//tmp/t"
        create("table", table, attributes={"replication_factor": 1})
        write_table("<append=%true>" + table, {"foo": "bar"}, table_writer={"upload_replication_factor": 1})

        chunk_id = get_singular_chunk_id(table)
        wait(lambda: len(get("#{}/@stored_sequoia_replicas".format(chunk_id))) == 0)
        wait(lambda: len(get("#{}/@stored_master_replicas".format(chunk_id))) > 0)
        remove(table)

    @authors("aleksandra-zh")
    def test_master_sequoia_replicas_handler(self):
        set("//sys/@config/chunk_manager/sequoia_chunk_replicas/enable", True)

        disk_space_limit = get_account_disk_space_limit("tmp", "default")
        set_account_disk_space_limit("tmp", disk_space_limit, self.TABLE_MEDIUM_1)
        set_account_disk_space_limit("tmp", disk_space_limit, self.TABLE_MEDIUM_2)

        table = "//tmp/t"
        create("table", table, attributes={
            "media": {
                self.TABLE_MEDIUM_1: {"replication_factor": 1, "data_parts_only": False},
                self.TABLE_MEDIUM_2: {"replication_factor": 1, "data_parts_only": False},
            },
            "primary_medium": self.TABLE_MEDIUM_1
        })
        write_table(table, {"foo": "bar"})

        chunk_id = get(table + "/@chunk_ids")[0]

        def process_yson_medium(data):
            json_data = yson.yson_to_json(data)
            return json_data['$attributes']['medium']

        def process_yson_locations(data):
            json_data = yson.yson_to_json(data)
            return json_data['$attributes']['location_uuid']

        def check_sequoia_replicas(chunk_id):
            stored_sequoia_replicas = get("#{}/@stored_sequoia_replicas".format(chunk_id))
            results = [process_yson_medium(data) == self.TABLE_MEDIUM_1 for data in stored_sequoia_replicas]
            return all(results) and len(results) > 0

        wait(lambda: check_sequoia_replicas(chunk_id))

        def check_master_replicas(chunk_id):
            stored_master_replicas = get("#{}/@stored_master_replicas".format(chunk_id))
            results = [process_yson_medium(data) == self.TABLE_MEDIUM_2 for data in stored_master_replicas]
            return all(results) and len(results) > 0

        wait(lambda: check_master_replicas(chunk_id))

        def check_stored_replicas(chunk_id):
            stored_replicas = get("#{}/@stored_replicas".format(chunk_id))
            stored_sequoia_replicas = get("#{}/@stored_sequoia_replicas".format(chunk_id))
            stored_master_replicas = get("#{}/@stored_master_replicas".format(chunk_id))

            if len(stored_replicas) != 2 or len(stored_sequoia_replicas) != 1 or len(stored_master_replicas) != 1:
                return False

            for replica in stored_replicas:
                if (process_yson_medium(replica) == self.TABLE_MEDIUM_1 and replica not in stored_sequoia_replicas) or \
                        (process_yson_medium(replica) == self.TABLE_MEDIUM_2 and replica not in stored_master_replicas):
                    return False
            return True

        wait(lambda: check_stored_replicas(chunk_id))
        remove(table)

    @authors("aleksandra-zh")
    def test_chunk_attributes(self):
        set("//sys/@config/chunk_manager/sequoia_chunk_replicas/enable", True)

        table = "//tmp/t"
        create("table", table, attributes={"replication_factor": 1})
        write_table("<append=%true>" + table, {"foo": "bar"}, table_writer={"upload_replication_factor": 1})

        chunk_id = get_singular_chunk_id(table)
        attribute_list = ls("#{}/@".format(chunk_id))
        for attr in attribute_list:
            get("#{}/@{}".format(chunk_id, attr))
        remove(table)


class TestSequoiaReplicasMulticell(TestSequoiaReplicas):
    ENABLE_MULTIDAEMON = False  # There are components restarts.
    NUM_SECONDARY_MASTER_CELLS = 3

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        super(TestSequoiaReplicasMulticell, cls).modify_node_config(config, cluster_index)

    @classmethod
    def setup_class(cls):
        super(TestSequoiaReplicasMulticell, cls).setup_class()

    def teardown_method(self, method):
        super(TestSequoiaReplicasMulticell, self).teardown_method(method)


class TestSequoiaQueues(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    USE_SEQUOIA = True
    NUM_CYPRESS_PROXIES = 1

    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["sequoia_node_host"]},
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "sequoia_manager": {
            "enable_ground_update_queues": True
        },
    }

    def _pause_sequoia_queue(self):
        set("//sys/@config/ground_update_queue_manager/queues/sequoia", {"pause_flush": True})

    def _resume_sequoia_queue(self):
        remove("//sys/@config/ground_update_queue_manager/queues/sequoia")

    @authors("aleksandra-zh")
    def test_link(self):
        create("map_node", "//tmp/m1")

        link_count = len(select_rows_from_ground(f"* from [{DESCRIPTORS.path_to_node_id.get_default_path()}]"))

        link("//tmp/m1", "//tmp/m2")
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.path_to_node_id.get_default_path()}]")) > link_count)

        def get_row(path):
            return lookup_rows_in_ground(DESCRIPTORS.path_to_node_id.get_default_path(), [{"path": mangle_sequoia_path(path)}])

        wait(lambda: len(get_row('//tmp/m2')) == 1)

        remove("//tmp/m2")
        wait(lambda: len(get_row('//tmp/m2')) == 0)

    @authors("danilalexeev")
    def test_link_unreachable(self):
        create("table", "//tmp/t1")
        link("//tmp/t1", "//tmp/a/b/c/d/l1", recursive=True)

        def get_row(path):
            return lookup_rows_in_ground(DESCRIPTORS.path_to_node_id.get_default_path(), [{"path": mangle_sequoia_path(path)}])

        try:
            set("//sys/@config/object_manager/enable_gc", False)
            wait(lambda: len(get_row('//tmp/a/b/c/d/l1')) == 1)
            remove("//tmp/a", recursive=True)

            wait(lambda: len(get_row('//tmp/a/b/c/d/l1')) == 0)
        finally:
            set("//sys/@config/object_manager/enable_gc", True)

    @authors("aleksandra-zh")
    def test_restart(self):
        self._pause_sequoia_queue()

        create("map_node", "//tmp/m1")
        link("//tmp/m1", "//tmp/m2")

        build_master_snapshots()

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        self._resume_sequoia_queue()

        def get_row(path):
            return lookup_rows_in_ground(DESCRIPTORS.path_to_node_id.get_default_path(), [{"path": mangle_sequoia_path(path)}])

        wait(lambda: len(get_row('//tmp/m2')) == 1)

        self._pause_sequoia_queue()

        remove("//tmp/m2")

        build_master_snapshots()

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        self._resume_sequoia_queue()

        wait(lambda: len(get_row('//tmp/m2')) == 0)

    @authors("aleksandra-zh")
    def test_branched_link(self):
        create("map_node", "//tmp/m1")

        tx = start_transaction()
        link("//tmp/m1", "//tmp/m2", tx=tx)
        abort_transaction(tx)

        def get_row(path):
            return lookup_rows_in_ground(DESCRIPTORS.path_to_node_id.get_default_path(), [{"path": mangle_sequoia_path(path)}])

        assert len(get_row('//tmp/m2')) == 0


class TestSequoiaPrerequisites(YTEnvSetup):
    USE_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
    NUM_CYPRESS_PROXIES = 1
    NUM_SECONDARY_MASTER_CELLS = 3

    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    DELTA_RPC_PROXY_CONFIG = {
        "cluster_connection": {
            "transaction_manager": {
                "use_cypress_transaction_service": True,
            },
        },
    }

    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["sequoia_node_host"]},
        # Master cell with tag 11 is reserved for portals.
        "12": {"roles": ["sequoia_node_host", "chunk_host", "transaction_coordinator"]},
        "13": {"roles": ["sequoia_node_host", "chunk_host", "transaction_coordinator"]},
    }

    DELTA_CYPRESS_PROXY_DYNAMIC_CONFIG = {
        "object_service": {
            "allow_bypass_master_resolve": True,
        },
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "transaction_manager": {
            "enable_prerequisite_transaction_validation_via_leases": True,
        },
    }

    @authors("cherepashka")
    def test_read_with_prerequisite_transactions_is_forbidden(self):
        tx = start_transaction()

        with raises_yt_error("Read requests with prerequisites are forbidden"):
            get("//tmp/@id", prerequisite_transaction_ids=[tx])
        with raises_yt_error("Read requests with prerequisites are forbidden"):
            exists("//tmp", prerequisite_transaction_ids=[tx])
        with raises_yt_error("Read requests with prerequisites are forbidden"):
            ls("//tmp", prerequisite_transaction_ids=[tx])

        commit_transaction(tx)

    @authors("cherepashka")
    @pytest.mark.parametrize("finish_tx", [commit_transaction, abort_transaction])
    def test_write_with_prerequisites(self, finish_tx):
        tx = start_transaction()
        create("table", "//tmp/t", prerequisite_transaction_ids=[tx])
        tx2 = start_transaction(prerequisite_transaction_ids=[tx])
        commit_transaction(tx2, prerequisite_transaction_ids=[tx])

        finish_tx(tx)
        assert not exists(f"//sys/transactions/{tx}")

    @authors("cherepashka")
    def test_commited_prerequisite_tx(self):
        tx = start_transaction()
        commit_transaction(tx)

        tx2 = start_transaction()

        with raises_yt_error("No such transaction"):
            commit_transaction(tx2, prerequisite_transaction_ids=[tx])

        with raises_yt_error(f"Prerequisite check failed: transaction {tx} is missing in Sequoia"):
            create("table", "//tmp/d", prerequisite_transaction_ids=[tx])

    @authors("cherepashka")
    @pytest.mark.parametrize("finish_tx", [commit_transaction, abort_transaction])
    def test_leases_revokation(self, finish_tx):
        tx = start_transaction()
        create("table", "//tmp/t1", prerequisite_transaction_ids=[tx], attributes={"external_cell_tag": 13})
        create("table", "//tmp/t2", prerequisite_transaction_ids=[tx], attributes={"external_cell_tag": 12})
        finish_tx(tx)
        with raises_yt_error(f"Prerequisite check failed: transaction {tx} is missing in Sequoia"):
            create("table", "//tmp/t3", prerequisite_transaction_ids=[tx], attributes={"external_cell_tag": 13})
        with raises_yt_error(f"Prerequisite check failed: transaction {tx} is missing in Sequoia"):
            create("table", "//tmp/t3", prerequisite_transaction_ids=[tx], attributes={"external_cell_tag": 12})
