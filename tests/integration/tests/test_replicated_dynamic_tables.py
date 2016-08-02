import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestReplicatedDynamicTables(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 0

    def _create_simple_replicated_table(self, path):
        create("replicated_table", path,
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"}]
            })

    def test_replicated_table_must_be_dynamic(self):
        with pytest.raises(YtError): create("replicated_table", "//tmp/t")

    def test_simple(self):
        self.sync_create_cells(1)
        self._create_simple_replicated_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 1, "value": "test"}])
        delete_rows("//tmp/t", [{"key": 2}])

    def test_add_replica_fail1(self):
        with pytest.raises(YtError): create_table_replica("//tmp/t", "slave1", "//tmp/r")

    def test_add_replica_fail2(self):
        create("table", "//tmp/t")
        with pytest.raises(YtError): create_table_replica("//tmp/t", "slave1", "//tmp/r")

    def test_add_remove_replica(self):
        self.sync_create_cells(1)
        self._create_simple_replicated_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        
        replica_id = create_table_replica("//tmp/t", "slave1", "//tmp/r")
        assert exists("//tmp/t/@replicas/{0}".format(replica_id))
        attributes = get("#{0}/@".format(replica_id))
        assert attributes["type"] == "table_replica"
        assert attributes["state"] == "disabled"
        remove_table_replica(replica_id)
        assert not exists("#{0}/@".format(replica_id))

##################################################################

class TestReplicatedDynamicTablesMulticell(TestReplicatedDynamicTables):
    NUM_SECONDARY_MASTER_CELLS = 2
