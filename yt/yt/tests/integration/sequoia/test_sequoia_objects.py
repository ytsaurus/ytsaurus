from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    NODES_SERVICE
)

from yt_sequoia_helpers import (
    select_rows_from_ground,
)

from yt.sequoia_tools import DESCRIPTORS

from yt_commands import (
    authors, create, get, remove, get_singular_chunk_id, write_table, read_table, wait,
    exists, create_domestic_medium, ls, set, get_account_disk_space_limit, set_account_disk_space_limit)

from yt.wrapper import yson

##################################################################


def sequoia_tables_empty():
    return all(
        select_rows_from_ground(f"* from [{table.get_default_path()}]") == []
        for table in DESCRIPTORS.get_group("chunk_tables"))


class TestSequoiaReplicas(YTEnvSetup):
    USE_SEQUOIA = True
    NUM_SECONDARY_MASTER_CELLS = 0
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["sequoia_node_host"]},
    }
    NUM_NODES = 9

    TABLE_MEDIUM_1 = "table_medium_1"
    TABLE_MEDIUM_2 = "table_medium_2"

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

    @authors("aleksandra-zh")
    def test_chunk_replicas_node_offline1(self):
        set("//sys/accounts/tmp/@resource_limits/disk_space_per_medium/{}".format(self.TABLE_MEDIUM_1), 10000)

        create("table", "//tmp/t",  attributes={"primary_medium": self.TABLE_MEDIUM_1})

        write_table("//tmp/t", [{"x": 1}])
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")

        assert len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")) > 0
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")) == 1)
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.location_replicas.get_default_path()}]")) == 3)

        rows = select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")
        assert len(rows) == 1
        assert len(rows[0]["replicas"]) == 3

        with Restarter(self.Env, NODES_SERVICE, indexes=self.table_node_indexes):
            pass

        remove("//tmp/t")

        wait(lambda: not exists("#{}".format(chunk_id)))
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")) == 0)
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.location_replicas.get_default_path()}]")) == 0)

    @authors("aleksandra-zh")
    def test_chunk_replicas_node_offline2(self):
        set("//sys/accounts/tmp/@resource_limits/disk_space_per_medium/{}".format(self.TABLE_MEDIUM_1), 10000)
        create("table", "//tmp/t",  attributes={"primary_medium": self.TABLE_MEDIUM_1})

        write_table("//tmp/t", [{"x": 1}])
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")

        assert len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")) > 0
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")) == 1)
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.location_replicas.get_default_path()}]")) == 3)

        with Restarter(self.Env, NODES_SERVICE, indexes=self.table_node_indexes):
            remove("//tmp/t")

            wait(lambda: not exists("#{}".format(chunk_id)))
            wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")) == 0)
            wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.location_replicas.get_default_path()}]")) == 0)

        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")) == 0)
        wait(lambda: len(select_rows_from_ground(f"* from [{DESCRIPTORS.location_replicas.get_default_path()}]")) == 0)

    @authors("aleksandra-zh")
    def test_replication(self):
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
        set("//sys/accounts/tmp/@resource_limits/disk_space_per_medium/{}".format(self.TABLE_MEDIUM_1), 10000)

        create("table", "//tmp/t",  attributes={"primary_medium": self.TABLE_MEDIUM_1})

        write_table("//tmp/t", [{"x": 1}])
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")

        assert len(select_rows_from_ground(f"* from [{DESCRIPTORS.chunk_replicas.get_default_path()}]")) > 0
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

    @authors("kivedernikov")
    def test_master_sequoia_replicas_handler(self):
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
