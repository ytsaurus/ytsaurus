from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    NODES_SERVICE
)

from yt_commands import (
    authors, create, get, remove, get_singular_chunk_id, lookup_rows, write_table, read_table, wait,
    exists, concatenate, select_rows, create_domestic_medium, ls, set)

import yt.yson as yson

import yt_proto.yt.client.chunk_client.proto.chunk_meta_pb2 as chunk_meta_pb2


##################################################################

def sequoia_tables_empty():
    if len(select_rows("* from [//sys/sequoia/chunk_meta_extensions]")) > 0:
        return False
    if len(select_rows("* from [//sys/sequoia/chunk_replicas]")) > 0:
        return False
    if len(select_rows("* from [//sys/sequoia/location_replicas]")) > 0:
        return False

    return True


class TestSequoiaObjects(YTEnvSetup):
    USE_SEQUOIA = True

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "chunk_manager": {
            "sequoia_chunk_probability": 100
        }
    }

    def teardown_method(self, method):
        wait(sequoia_tables_empty)
        super(TestSequoiaObjects, self).teardown_method(method)

    def _get_id_hash(self, chunk_id):
        id_part = chunk_id.split('-')[3]
        return int(id_part, 16)

    def _get_key(self, chunk_id):
        return {
            "id_hash": self._get_id_hash(chunk_id),
            "id": chunk_id,
        }

    @authors("gritukan")
    def test_estimated_creation_time(self):
        object_id = "543507cc-00000000-12345678-abcdef01"
        creation_time = {'min': '2012-12-21T08:34:56.000000Z', 'max': '2012-12-21T08:34:57.000000Z'}
        assert get("//sys/estimated_creation_time/{}".format(object_id)) == creation_time

    @authors("gritukan")
    def test_sequoia_chunk(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"x": 1}])
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#{}/@sequoia".format(chunk_id))
        assert get("#{}/@aevum".format(chunk_id)) != "none"

        assert len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(chunk_id)])) == 1

        remove("//tmp/t")

    @authors("aleksandra-zh")
    def test_confirm_sequoia_chunk(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"x": 1}])
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#{}/@sequoia".format(chunk_id))
        assert get("#{}/@aevum".format(chunk_id)) != "none"

        exts = lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(chunk_id)])
        assert len(exts) == 1
        raw_misc_ext = yson.get_bytes(exts[0]["misc_ext"])
        misc_ext = chunk_meta_pb2.TMiscExt()
        misc_ext.ParseFromString(raw_misc_ext)
        assert misc_ext.row_count == 1

        remove("//tmp/t")

    @authors("aleksandra-zh")
    def test_remove_sequoia_chunk(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"x": 1}])
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#{}/@sequoia".format(chunk_id))
        assert get("#{}/@aevum".format(chunk_id)) != "none"

        assert len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(chunk_id)])) == 1

        remove("//tmp/t")

        wait(lambda: not exists("#{}".format(chunk_id)))
        wait(lambda: len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(chunk_id)])) == 0)


class TestSequoiaObjectsMulticell(TestSequoiaObjects):
    NUM_SECONDARY_MASTER_CELLS = 3

    @classmethod
    def modify_node_config(cls, config):
        super(TestSequoiaObjectsMulticell, cls).modify_node_config(config)

    @classmethod
    def setup_class(cls):
        super(TestSequoiaObjectsMulticell, cls).setup_class()

    def teardown_method(self, method):
        super(TestSequoiaObjectsMulticell, self).teardown_method(method)

    @authors("aleksandra-zh")
    def test_remove_foreign_sequoia_chunk(self):
        create("table", "//tmp/t1", attributes={"external_cell_tag": 12})
        write_table("<append=true>//tmp/t1", {"a": "b"})

        create("table", "//tmp/t2", attributes={"external_cell_tag": 13})
        write_table("<append=true>//tmp/t2", {"c": "d"})

        create("table", "//tmp/t", attributes={"external_cell_tag": 13})
        concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/t")

        t1_chunk_id = get_singular_chunk_id("//tmp/t1")
        assert len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(t1_chunk_id)])) == 1

        t2_chunk_id = get_singular_chunk_id("//tmp/t2")
        assert len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(t2_chunk_id)])) == 1

        remove("//tmp/t")
        assert len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(t1_chunk_id)])) == 1
        assert len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(t2_chunk_id)])) == 1

        remove("//tmp/t2")
        wait(lambda: not exists("#{}".format(t2_chunk_id)))
        wait(lambda: len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(t2_chunk_id)])) == 0)
        assert len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(t1_chunk_id)])) == 1

        remove("//tmp/t1")
        wait(lambda: not exists("#{}".format(t1_chunk_id)))
        wait(lambda: len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(t1_chunk_id)])) == 0)
        assert len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(t2_chunk_id)])) == 0


class TestSequoiaReplicas(YTEnvSetup):
    USE_SEQUOIA = True
    NUM_NODES = 9

    TABLE_MEDIUM = "table_medium"

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "chunk_manager": {
            "sequoia_chunk_replicas_percentage": 100
        }
    }

    @classmethod
    def modify_node_config(cls, config):
        node_flavors = [
            ["data", "exec"],
            ["data", "exec"],
            ["data", "exec"],
            ["data", "exec"],
            ["data", "exec"],
            ["data", "exec"],
            ["tablet"],
            ["tablet"],
            ["tablet"],
        ]
        if not hasattr(cls, "node_counter"):
            cls.node_counter = 0
        config["flavors"] = node_flavors[cls.node_counter]
        cls.node_counter = (cls.node_counter + 1) % cls.NUM_NODES

    @classmethod
    def setup_class(cls):
        super(TestSequoiaReplicas, cls).setup_class()
        create_domestic_medium(cls.TABLE_MEDIUM)
        set("//sys/media/{}/@enable_sequoia_replicas".format(cls.TABLE_MEDIUM), True)

        cls.table_node_indexes = []
        addresses_to_index = {cls.Env.get_node_address(index) : index for index in range(0, cls.NUM_NODES)}

        for node_address in ls("//sys/cluster_nodes"):
            flavors = get("//sys/cluster_nodes/{}/@flavors".format(node_address))
            if "data" in flavors:
                location_uuids = list(get("//sys/cluster_nodes/{}/@chunk_locations".format(node_address)).keys())
                assert len(location_uuids) > 0
                for location_uuid in location_uuids:
                    medium_override_path = "//sys/chunk_locations/{}/@medium_override".format(location_uuid)
                    set(medium_override_path, cls.TABLE_MEDIUM)

                cls.table_node_indexes.append(addresses_to_index[node_address])
                if len(cls.table_node_indexes) == 3:
                    break

    def teardown_method(self, method):
        wait(sequoia_tables_empty)
        super(TestSequoiaReplicas, self).teardown_method(method)

    @authors("aleksandra-zh")
    def test_chunk_replicas_node_offline1(self):
        set("//sys/accounts/tmp/@resource_limits/disk_space_per_medium/{}".format(self.TABLE_MEDIUM), 10000)

        create("table", "//tmp/t",  attributes={"primary_medium": self.TABLE_MEDIUM})

        write_table("//tmp/t", [{"x": 1}])
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")

        assert len(select_rows("* from [//sys/sequoia/chunk_replicas]")) > 0
        wait(lambda: len(select_rows("* from [//sys/sequoia/chunk_replicas]")) == 3)
        wait(lambda: len(select_rows("* from [//sys/sequoia/location_replicas]")) == 3)

        with Restarter(self.Env, NODES_SERVICE, indexes=self.table_node_indexes):
            pass

        remove("//tmp/t")

        wait(lambda: not exists("#{}".format(chunk_id)))
        wait(lambda: len(select_rows("* from [//sys/sequoia/chunk_replicas]")) == 0)

    @authors("aleksandra-zh")
    def test_chunk_replicas_node_offline2(self):
        set("//sys/accounts/tmp/@resource_limits/disk_space_per_medium/{}".format(self.TABLE_MEDIUM), 10000)
        create("table", "//tmp/t",  attributes={"primary_medium": self.TABLE_MEDIUM})

        write_table("//tmp/t", [{"x": 1}])
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")

        assert len(select_rows("* from [//sys/sequoia/chunk_replicas]")) > 0
        wait(lambda: len(select_rows("* from [//sys/sequoia/chunk_replicas]")) == 3)
        wait(lambda: len(select_rows("* from [//sys/sequoia/location_replicas]")) == 3)

        with Restarter(self.Env, NODES_SERVICE, indexes=self.table_node_indexes):
            remove("//tmp/t")

            wait(lambda: not exists("#{}".format(chunk_id)))
            wait(lambda: len(select_rows("* from [//sys/sequoia/chunk_replicas]")) == 0)

        wait(lambda: len(select_rows("* from [//sys/sequoia/chunk_replicas]")) == 0)


class TestSequoiaReplicasMulticell(TestSequoiaReplicas):
    NUM_SECONDARY_MASTER_CELLS = 3

    @classmethod
    def modify_node_config(cls, config):
        super(TestSequoiaReplicasMulticell, cls).modify_node_config(config)

    @classmethod
    def setup_class(cls):
        super(TestSequoiaReplicasMulticell, cls).setup_class()

    def teardown_method(self, method):
        super(TestSequoiaReplicasMulticell, self).teardown_method(method)
