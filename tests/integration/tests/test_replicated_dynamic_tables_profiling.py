import pytest

from test_replicated_dynamic_tables import TestReplicatedDynamicTablesBase, SIMPLE_SCHEMA_SORTED, SIMPLE_SCHEMA_ORDERED, AGGREGATE_SCHEMA

from yt_env_setup import parametrize_external
from yt_commands import *
from time import sleep, time
from yt.environment.helpers import wait

from flaky import flaky

##################################################################

class TestReplicatedDynamicTablesProfiling(TestReplicatedDynamicTablesBase):
    @authors("savrus")
    @pytest.mark.parametrize("schema", [SIMPLE_SCHEMA_SORTED, SIMPLE_SCHEMA_ORDERED])
    def test_replicated_tablet_node_profiling(self, schema):
        self._create_cells()

        replicated_table_path = "//tmp/{}".format(generate_uuid())
        self._create_replicated_table(replicated_table_path, schema, enable_profiling=True, dynamic_store_auto_flush_period=None)

        tablet_profiling = self._get_table_profiling(replicated_table_path)
        def get_all_counters():
            return (
                tablet_profiling.get_counter("write/row_count"),
                tablet_profiling.get_counter("commit/row_count"),
                tablet_profiling.get_counter("write/data_weight"),
                tablet_profiling.get_counter("commit/data_weight"))

        assert get_all_counters() == (0, 0, 0, 0)

        insert_rows(replicated_table_path, [{"key": 1, "value1": "test"}], require_sync_replica=False)
        wait(lambda: get_all_counters() == (1, 1, 13, 13))

    @authors("gridem")
    @flaky(max_runs=5)
    @pytest.mark.parametrize("schema", [SIMPLE_SCHEMA_SORTED, SIMPLE_SCHEMA_ORDERED])
    def test_replica_tablet_node_profiling(self, schema):
        self._create_cells()

        replicated_table_path = "//tmp/{}".format(generate_uuid())
        self._create_replicated_table(replicated_table_path, schema, enable_profiling=True, dynamic_store_auto_flush_period=None)

        replica_table_path = "//tmp/{}".format(generate_uuid())
        replica_id = create_table_replica(replicated_table_path, self.REPLICA_CLUSTER_NAME, replica_table_path)
        self._create_replica_table(replica_table_path, replica_id, schema)

        tablet_profiling = self._get_table_profiling(replicated_table_path)

        def get_lag_row_count():
            return tablet_profiling.get_counter("replica/lag_row_count")

        def get_lag_time():
            return tablet_profiling.get_counter("replica/lag_time") / 1e6 # conversion from us to s

        sync_enable_table_replica(replica_id)
        sleep(2)

        assert get_lag_row_count() == 0
        assert get_lag_time() == 0

        insert_rows(replicated_table_path, [{"key": 0, "value1": "test", "value2": 123}], require_sync_replica=False)
        sleep(2)

        assert get_lag_row_count() == 0
        assert get_lag_time() == 0

        sync_unmount_table(replica_table_path, driver=self.replica_driver)

        insert_rows(replicated_table_path, [{"key": 1, "value1": "test", "value2": 123}], require_sync_replica=False)
        sleep(2)

        assert get_lag_row_count() == 1
        assert 2 <= get_lag_time() <= 8

        insert_rows(replicated_table_path, [{"key": 2, "value1": "test", "value2": 123}], require_sync_replica=False)
        sleep(2)

        assert get_lag_row_count() == 2
        assert 4 <= get_lag_time() <= 10

        sync_mount_table(replica_table_path, driver=self.replica_driver)
        sleep(2)

        assert get_lag_row_count() == 0
        assert get_lag_time() == 0

    @authors("babenko", "gridem")
    @flaky(max_runs=5)
    def test_replication_lag(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.AGGREGATE_SCHEMA)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, schema=self.AGGREGATE_SCHEMA)

        def get_lag_time():
            return get("#{0}/@replication_lag_time".format(replica_id))

        assert get_lag_time() == 0

        insert_rows("//tmp/t", [{"key": 1, "value1": "test1"}], require_sync_replica=False)
        sleep(1.0)
        assert 1000000 < get_lag_time()

        sync_enable_table_replica(replica_id)
        wait(lambda: get_lag_time() == 0)

        sync_disable_table_replica(replica_id)
        wait(lambda: get_lag_time() == 0)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test1"}], require_sync_replica=False)
        sleep(1.0)

        base_lag_time = get_lag_time()
        base_unix_time = time()
        assert base_lag_time > 2000
        for i in xrange(10):
            sleep(1.0)
            cur_unix_time = time()
            cur_lag_time = get_lag_time()
            assert abs((cur_lag_time - base_lag_time) - (cur_unix_time - base_unix_time) * 1000) <= 2000

        sync_enable_table_replica(replica_id)
        wait(lambda: get_lag_time() == 0)

##################################################################

class TestReplicatedDynamicTablesProfilingMulticell(TestReplicatedDynamicTablesProfiling):
    NUM_SECONDARY_MASTER_CELLS = 2
    DELTA_MASTER_CONFIG = {
        "tablet_manager": {
            "tablet_cell_decommissioner": {
                "decommission_check_period": 100,
                "orphans_check_period": 100,
            }
        },
    }

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    @parametrize_external
    def test_external_replicated_table(self, mode, external):
        self._create_cells()
        self._create_replicated_table("//tmp/t", external=external)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": mode})
        self._create_replica_table("//tmp/r", replica_id, external=external)

        if external:
            assert get("//tmp/t/@external") == True
            assert get("//tmp/r/@external", driver=self.replica_driver) == True

        assert get("#" + replica_id + "/@table_path") == "//tmp/t"

        sync_enable_table_replica(replica_id)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test", "value2": 123}])
