from yt_env_setup import YTEnvSetup

from yt_commands import (
<<<<<<< releases/yt/stable/23.2: Fix Sequoia transaction ids
    authors, create, get, remove, get_singular_chunk_id, write_table, wait,
    select_rows, create_domestic_medium, ls, set, get_driver)
=======
    authors, create, get, remove, get_singular_chunk_id, write_table, read_table, wait,
    exists, create_domestic_medium, ls, set, get_account_disk_space_limit, set_account_disk_space_limit,
    link, build_master_snapshots, start_transaction, abort_transaction, get_active_primary_master_leader_address)

from yt.wrapper import yson
>>>>>>> cherry-pick: Fix purgatory corrupting destroyed replicas after node restart

from yt_helpers import profiler_factory

##################################################################


def sequoia_tables_empty():
    ground_driver = get_driver(cluster="primary_ground")
    if len(select_rows("* from [//sys/sequoia/chunk_meta_extensions]", driver=ground_driver)) > 0:
        return False
    if len(select_rows("* from [//sys/sequoia/chunk_replicas]", driver=ground_driver)) > 0:
        return False
    if len(select_rows("* from [//sys/sequoia/location_replicas]", driver=ground_driver)) > 0:
        return False

    return True


class TestSequoiaReplicas(YTEnvSetup):
    USE_SEQUOIA = True
    NUM_NODES = 9

    TABLE_MEDIUM_1 = "table_medium_1"
    TABLE_MEDIUM_2 = "table_medium_2"

    DELTA_MASTER_CONFIG = {
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True,
        },
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "chunk_manager": {
            "sequoia_chunk_replicas_percentage": 100,
            "fetch_replicas_from_sequoia": True
        }
    }

    @classmethod
    def setup_class(cls):
        super(TestSequoiaReplicas, cls).setup_class()
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


class TestOnlySequoiaReplicas(TestSequoiaReplicas):
    DELTA_DYNAMIC_MASTER_CONFIG = {
        "chunk_manager": {
            "sequoia_chunk_replicas_percentage": 100,
            "fetch_replicas_from_sequoia": True,
            "store_sequoia_replicas_on_master": False,
            "processed_removed_sequoia_replicas_on_master": False
        }
    }

    @authors("kivedernikov")
    def test_empty_sequoia_handler(self):
        table = "//tmp/t"
        create("table", table, attributes={"replication_factor": 1})
        write_table("<append=%true>" + table, {"foo": "bar"}, table_writer={"upload_replication_factor": 1})

        chunk_id = get_singular_chunk_id(table)
        wait(lambda: len(get("#{}/@stored_sequoia_replicas".format(chunk_id))) == 0)
        wait(lambda: len(get("#{}/@stored_master_replicas".format(chunk_id))) > 0)
        remove(table)


class TestSequoiaReplicasMulticell(TestSequoiaReplicas):
    NUM_SECONDARY_MASTER_CELLS = 3

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        super(TestSequoiaReplicasMulticell, cls).modify_node_config(config, cluster_index)

    @classmethod
    def setup_class(cls):
        super(TestSequoiaReplicasMulticell, cls).setup_class()

    def teardown_method(self, method):
        super(TestSequoiaReplicasMulticell, self).teardown_method(method)
