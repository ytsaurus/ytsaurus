from .test_replicated_dynamic_tables import (
    TestReplicatedDynamicTablesBase,
    SIMPLE_SCHEMA_SORTED,
    SIMPLE_SCHEMA_ORDERED,
)

from yt_env_setup import parametrize_external
from yt_commands import (
    authors, wait, get, generate_uuid,
    sync_mount_table, sync_unmount_table, create_table_replica, sync_enable_table_replica, sync_disable_table_replica,
    insert_rows, select_rows, remove, generate_timestamp, sync_unfreeze_table)

from flaky import flaky
import pytest

from time import sleep, time

##################################################################


@pytest.mark.enabled_multidaemon
class TestReplicatedDynamicTablesProfiling(TestReplicatedDynamicTablesBase):
    ENABLE_MULTIDAEMON = True

    @authors("savrus")
    @flaky(max_runs=5)
    @pytest.mark.parametrize("schema", [SIMPLE_SCHEMA_SORTED, SIMPLE_SCHEMA_ORDERED])
    def test_replicated_tablet_node_profiling(self, schema):
        self._create_cells()

        replicated_table_path = "//tmp/{}".format(generate_uuid())
        self._create_replicated_table(
            replicated_table_path,
            schema,
            enable_profiling=True,
            dynamic_store_auto_flush_period=None,
        )

        tablet_profiling = self._get_table_profiling(replicated_table_path)

        def get_all_counters():
            return (
                tablet_profiling.get_counter("write/row_count"),
                tablet_profiling.get_counter("commit/row_count"),
                tablet_profiling.get_counter("write/data_weight"),
                tablet_profiling.get_counter("commit/data_weight"),
            )

        assert get_all_counters() == (0, 0, 0, 0)

        insert_rows(
            replicated_table_path,
            [{"key": 1, "value1": "test"}],
            require_sync_replica=False,
        )
        wait(lambda: get_all_counters() == (1, 1, 13, 13))

    @authors("gridem")
    @flaky(max_runs=5)
    @pytest.mark.parametrize("schema", [SIMPLE_SCHEMA_SORTED, SIMPLE_SCHEMA_ORDERED])
    def test_replica_tablet_node_profiling(self, schema):
        self._create_cells()

        replicated_table_path = "//tmp/{}".format(generate_uuid())
        self._create_replicated_table(
            replicated_table_path,
            schema,
            enable_profiling=True,
            dynamic_store_auto_flush_period=None,
        )

        replica_table_path = "//tmp/{}".format(generate_uuid())
        replica_id = create_table_replica(replicated_table_path, self.REPLICA_CLUSTER_NAME, replica_table_path)
        self._create_replica_table(replica_table_path, replica_id, schema)

        tablet_profiling = self._get_table_profiling(replicated_table_path)

        replica_tags = {
            "replica_cluster": self.REPLICA_CLUSTER_NAME,
        }

        def get_lag_row_count():
            return tablet_profiling.get_counter("replica/lag_row_count", tags=replica_tags)

        def get_lag_time():
            return tablet_profiling.get_counter("replica/lag_time", tags=replica_tags)

        sync_enable_table_replica(replica_id)
        sleep(2)

        assert get_lag_row_count() == 0
        assert get_lag_time() == 0

        insert_rows(
            replicated_table_path,
            [{"key": 0, "value1": "test", "value2": 123}],
            require_sync_replica=False,
        )
        sleep(2)

        assert get_lag_row_count() == 0
        assert get_lag_time() == 0

        sync_unmount_table(replica_table_path, driver=self.replica_driver)

        # Sleep before inserting next rows to verify that lag will be computed
        # correctly after delay between consequent writes.
        sleep(10)

        insert_rows(
            replicated_table_path,
            [{"key": 1, "value1": "test", "value2": 123}],
            require_sync_replica=False,
        )
        sleep(2)

        assert get_lag_row_count() == 1
        assert 2 <= get_lag_time() <= 8

        insert_rows(
            replicated_table_path,
            [{"key": 2, "value1": "test", "value2": 123}],
            require_sync_replica=False,
        )
        sleep(2)

        assert get_lag_row_count() == 2
        assert 4 <= get_lag_time() <= 10

        sync_mount_table(replica_table_path, driver=self.replica_driver)
        sleep(10)

        assert get_lag_row_count() == 0
        assert get_lag_time() == 0

    @authors("ifsmirnov")
    @flaky(max_runs=5)
    @pytest.mark.parametrize("with_lag", [True, False])
    @pytest.mark.parametrize("offset_mode", ["row_index", "timestamp"])
    def test_new_replica_lag(self, offset_mode, with_lag):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.SIMPLE_SCHEMA_SORTED)

        tablet_profiling = self._get_table_profiling("//tmp/t")

        replica_tags = {
            "replica_cluster": self.REPLICA_CLUSTER_NAME,
        }

        def _get_lag_time():
            return tablet_profiling.get_counter("replica/lag_time", tags=replica_tags)

        first_replica_id = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r1",
            attributes={"mode": "async"})
        sync_enable_table_replica(first_replica_id)
        self._create_replica_table("//tmp/r1", first_replica_id, schema=self.SIMPLE_SCHEMA_SORTED)

        insert_rows("//tmp/t", [{"key": 1, "value1": "foo"}], require_sync_replica=False)

        # Sleep before inserting next rows to verify that lag will be computed
        # correctly after delay between consequent writes.
        sleep(10)

        assert _get_lag_time() == 0

        attributes = {"mode": "async"}
        if offset_mode == "row_index":
            attributes["start_replication_row_indexes"] = [1]
        else:
            attributes["start_replication_timestamp"] = generate_timestamp()

        if with_lag:
            insert_rows("//tmp/t", [{"key": 1, "value1": "foo"}], require_sync_replica=False)

        second_replica_id = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r2",
            attributes=attributes)
        sync_enable_table_replica(second_replica_id)
        self._create_replica_table("//tmp/r2", second_replica_id, schema=self.SIMPLE_SCHEMA_SORTED, mount=False)
        sync_mount_table("//tmp/r2", freeze=True, driver=self.replica_driver)

        sleep(5)

        if with_lag:
            assert 5.0 - 3 < _get_lag_time() < 5.0 + 3
        else:
            assert _get_lag_time() == 0

        sync_unfreeze_table("//tmp/r2", driver=self.replica_driver)
        wait(lambda: _get_lag_time() == 0)

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
        for i in range(10):
            sleep(1.0)
            cur_unix_time = time()
            cur_lag_time = get_lag_time()
            assert abs((cur_lag_time - base_lag_time) - (cur_unix_time - base_unix_time) * 1000) <= 2000

        sync_enable_table_replica(replica_id)
        wait(lambda: get_lag_time() == 0)

    @authors("prime")
    def test_table_remove(self):
        self._create_cells()

        self._create_replicated_table("//tmp/tt", schema=self.AGGREGATE_SCHEMA)
        replica_id = create_table_replica("//tmp/tt", self.REPLICA_CLUSTER_NAME, "//tmp/rr", attributes={"mode": "async"})
        self._create_replica_table("//tmp/rr", replica_id, schema=self.AGGREGATE_SCHEMA)

        sync_enable_table_replica(replica_id)
        sleep(2)

        insert_rows("//tmp/tt", [{"key": 1, "value1": "test1"}], require_sync_replica=False)
        sleep(1.0)

        tablet_profiling = self._get_table_profiling("//tmp/tt")
        replica_tags = {
            "replica_cluster": self.REPLICA_CLUSTER_NAME,
        }

        assert tablet_profiling.has_projections_with_tags("replica/lag_time", replica_tags)
        remove("//tmp/tt")

        def gauge_removed():
            return not tablet_profiling.has_projections_with_tags("replica/lag_time", replica_tags)
        wait(gauge_removed)


##################################################################


@pytest.mark.enabled_multidaemon
class TestReplicatedDynamicTablesProfilingMulticell(TestReplicatedDynamicTablesProfiling):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    @parametrize_external
    def test_external_replicated_table(self, mode, external):
        self._create_cells()
        self._create_replicated_table("//tmp/t", external=external)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": mode})
        self._create_replica_table("//tmp/r", replica_id, external=external)

        if external:
            assert get("//tmp/t/@external")
            assert get("//tmp/r/@external", driver=self.replica_driver)

        assert get("#" + replica_id + "/@table_path") == "//tmp/t"

        sync_enable_table_replica(replica_id)

        insert_rows(
            "//tmp/t",
            [{"key": 1, "value1": "test", "value2": 123}],
            require_sync_replica=False,
        )
        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)
            == [{"key": 1, "value1": "test", "value2": 123}]
        )
