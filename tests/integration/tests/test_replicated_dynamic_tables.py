import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *
from time import sleep

##################################################################

class TestReplicatedDynamicTables(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 0

    DELTA_NODE_CONFIG = {
        "cluster_directory_synchronizer": {
            "sync_period": 500
        },
        "cluster_connection": {
            # Disable cache
            "table_mount_cache": {
                "expire_after_successful_update_time": 0,
                "expire_after_failed_update_time": 0,
                "expire_after_access_time": 0,
                "refresh_time": 0
            }
        },
        "tablet_node": {
            "tablet_manager": {
                "replicator_soft_backoff_time": 100
            }
        }
    }

    def _get_simple_table_attributes(self):
        return {
            "dynamic": True,
            "schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value1", "type": "string"},
                {"name": "value2", "type": "int64"}
            ]
        }
    
    def _create_simple_replicated_table(self, path):
        attributes = self._get_simple_table_attributes()
        attributes["enable_replication_logging"] = True
        create("replicated_table", path, attributes=attributes)

    def _create_simple_replica_table(self, path):
        create("table", path, attributes=self._get_simple_table_attributes())

    def _init_cluster(self, name):
        set("//sys/clusters/" + name,
            {
                "primary_master": self.Env.configs["master"][0]["primary_master"],
                "secondary_masters": self.Env.configs["master"][0]["secondary_masters"],
                "timestamp_provider": self.Env.configs["master"][0]["timestamp_provider"],
                "cell_tag": 0
            })

    def setup(self):
        self._init_cluster("r1")
        self._init_cluster("r2")

    def test_replicated_table_must_be_dynamic(self):
        with pytest.raises(YtError): create("replicated_table", "//tmp/t")

    def test_simple(self):
        self.sync_create_cells(1)
        self._create_simple_replicated_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 1, "value1": "test"}])
        delete_rows("//tmp/t", [{"key": 2}])

    def test_add_replica_fail1(self):
        with pytest.raises(YtError): create_table_replica("//tmp/t", "r1", "//tmp/r")

    def test_add_replica_fail2(self):
        create("table", "//tmp/t")
        with pytest.raises(YtError): create_table_replica("//tmp/t", "r1", "//tmp/r")

    def test_add_remove_replica(self):
        self.sync_create_cells(1)
        self._create_simple_replicated_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        
        replica_id = create_table_replica("//tmp/t", "r1", "//tmp/r")
        assert exists("//tmp/t/@replicas/{0}".format(replica_id))
        attributes = get("#{0}/@".format(replica_id))
        assert attributes["type"] == "table_replica"
        assert attributes["state"] == "disabled"
        remove_table_replica(replica_id)
        assert not exists("#{0}/@".format(replica_id))

    def test_enable_disable_replica_unmounted(self):
        self._create_simple_replicated_table("//tmp/t")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        replica_id = create_table_replica("//tmp/t", "r1", "//tmp/r")
        attributes = get("#{0}/@".format(replica_id), attributes=["state", "tablets"])
        assert attributes["state"] == "disabled"
        assert len(attributes["tablets"]) == 1
        assert attributes["tablets"][tablet_id]["state"] == "none"

        enable_table_replica(replica_id)
        attributes = get("#{0}/@".format(replica_id), attributes=["state", "tablets"])
        assert attributes["state"] == "enabled"
        assert len(attributes["tablets"]) == 1
        assert attributes["tablets"][tablet_id]["state"] == "none"

        disable_table_replica(replica_id)
        attributes = get("#{0}/@".format(replica_id), attributes=["state", "tablets"])
        assert attributes["state"] == "disabled"
        assert len(attributes["tablets"]) == 1
        assert attributes["tablets"][tablet_id]["state"] == "none"

    def test_enable_disable_replica_mounted(self):
        self.sync_create_cells(1)
        self._create_simple_replicated_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        replica_id = create_table_replica("//tmp/t", "r1", "//tmp/r")
        assert get("#{0}/@state".format(replica_id)) == "disabled"

        enable_table_replica(replica_id)
        assert get("#{0}/@state".format(replica_id)) == "enabled"

        self.sync_disable_table_replica(replica_id)
        assert get("#{0}/@state".format(replica_id)) == "disabled"

    def test_passive_replication(self):
        self.sync_create_cells(1)
        self._create_simple_replicated_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        self._create_simple_replica_table("//tmp/r")
        self.sync_mount_table("//tmp/r")

        replica_id = create_table_replica("//tmp/t", "r1", "//tmp/r")
        enable_table_replica(replica_id)
   
        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}])
        sleep(1.0)
        assert select_rows("* from [//tmp/r]") == [{"key": 1, "value1": "test", "value2": 123}]

        insert_rows("//tmp/t", [{"key": 1, "value1": "new_test"}], update=True)
        sleep(1.0)
        assert select_rows("* from [//tmp/r]") == [{"key": 1, "value1": "new_test", "value2": 123}]

        insert_rows("//tmp/t", [{"key": 1, "value2": 456}], update=True)
        sleep(1.0)
        assert select_rows("* from [//tmp/r]") == [{"key": 1, "value1": "new_test", "value2": 456}]

        delete_rows("//tmp/t", [{"key": 1}])
        sleep(1.0)
        assert select_rows("* from [//tmp/r]") == []

    def test_disable_propagates_replication_row_index(self):
        self.sync_create_cells(1)
        self._create_simple_replicated_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        self._create_simple_replica_table("//tmp/r")
        self.sync_mount_table("//tmp/r")

        replica_id = create_table_replica("//tmp/t", "r1", "//tmp/r")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        enable_table_replica(replica_id)
   
        assert get("#{0}/@tablets/{1}/current_replication_row_index".format(replica_id, tablet_id)) == 0

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}])
        sleep(1.0)
        assert select_rows("* from [//tmp/r]") == [{"key": 1, "value1": "test", "value2": 123}]

        self.sync_disable_table_replica(replica_id)

        assert get("#{0}/@tablets/{1}/current_replication_row_index".format(replica_id, tablet_id)) == 1

    def test_unmount_propagates_replication_row_index(self):
        self.sync_create_cells(1)
        self._create_simple_replicated_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        self._create_simple_replica_table("//tmp/r")
        self.sync_mount_table("//tmp/r")

        replica_id = create_table_replica("//tmp/t", "r1", "//tmp/r")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        enable_table_replica(replica_id)
   
        assert get("#{0}/@tablets/{1}/current_replication_row_index".format(replica_id, tablet_id)) == 0

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}])
        sleep(1.0)
        assert select_rows("* from [//tmp/r]") == [{"key": 1, "value1": "test", "value2": 123}]

        self.sync_unmount_table("//tmp/t")

        assert get("#{0}/@tablets/{1}/current_replication_row_index".format(replica_id, tablet_id)) == 1


##################################################################

class TestReplicatedDynamicTablesMulticell(TestReplicatedDynamicTables):
    NUM_SECONDARY_MASTER_CELLS = 2
