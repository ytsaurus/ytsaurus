import pytest

from test_dynamic_tables import TestDynamicTablesBase

from yt_env_setup import YTEnvSetup
from yt_commands import *
from time import sleep
from yt.yson import YsonEntity
from yt.environment.helpers import assert_items_equal, wait

from flaky import flaky

##################################################################

class TestReplicatedDynamicTables(TestDynamicTablesBase):
    NUM_REMOTE_CLUSTERS = 1

    DELTA_NODE_CONFIG = {
        "cluster_connection": {
            # Disable cache
            "table_mount_cache": {
                "expire_after_successful_update_time": 0,
                "expire_after_failed_update_time": 0,
                "expire_after_access_time": 0,
                "refresh_time": 0
            },
            "timestamp_provider": {
                "update_period": 500,
            }
        },
        "tablet_node": {
            "replicator_data_weight_throttling_granularity": 1
        }
    }

    SIMPLE_SCHEMA = [
        {"name": "key", "type": "int64", "sort_order": "ascending"},
        {"name": "value1", "type": "string"},
        {"name": "value2", "type": "int64"}
    ]

    SIMPLE_SCHEMA_ORDERED = [
        {"name": "key", "type": "int64"},
        {"name": "value1", "type": "string"},
        {"name": "value2", "type": "int64"}
    ]

    PERTURBED_SCHEMA = [
        {"name": "key", "type": "int64", "sort_order": "ascending"},
        {"name": "value2", "type": "int64"},
        {"name": "value1", "type": "string"}
    ]

    AGGREGATE_SCHEMA = [
        {"name": "key", "type": "int64", "sort_order": "ascending"},
        {"name": "value1", "type": "string"},
        {"name": "value2", "type": "int64", "aggregate": "sum"}
    ]

    EXPRESSION_SCHEMA = [
        {"name": "hash", "type": "int64", "sort_order": "ascending", "expression": "key % 10"},
        {"name": "key", "type": "int64", "sort_order": "ascending"},
        {"name": "value", "type": "int64"},
    ]

    REPLICA_CLUSTER_NAME = "remote_0"


    def setup(self):
        self.replica_driver = get_driver(cluster=self.REPLICA_CLUSTER_NAME)
        self.primary_driver = get_driver(cluster="primary")


    def _get_table_attributes(self, schema):
        return {
            "dynamic": True,
            "schema": schema
        }

    def _create_replicated_table(self, path, schema=SIMPLE_SCHEMA, attributes={}, mount=True):
        attributes.update(self._get_table_attributes(schema))
        attributes["enable_replication_logging"] = True
        create("replicated_table", path, attributes=attributes)
        if mount:
            self.sync_mount_table(path)

    def _create_replica_table(self, path, replica_id, schema=SIMPLE_SCHEMA, mount=True, replica_driver=None):
        if not replica_driver:
            replica_driver = self.replica_driver
        attributes = self._get_table_attributes(schema)
        attributes["upstream_replica_id"] = replica_id
        create("table", path, attributes=attributes, driver=replica_driver)
        if mount:
            self.sync_mount_table(path, driver=replica_driver)

    def _create_cells(self):
        self.sync_create_cells(1)
        self.sync_create_cells(1, driver=self.replica_driver)


    def test_replicated_table_must_be_dynamic(self):
        with pytest.raises(YtError): create("replicated_table", "//tmp/t")

    @pytest.mark.parametrize("schema", [SIMPLE_SCHEMA, SIMPLE_SCHEMA_ORDERED])
    def test_simple(self, schema):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test"}], require_sync_replica=False)
        if (schema is self.SIMPLE_SCHEMA):
            delete_rows("//tmp/t", [{"key": 2}], require_sync_replica=False)

    @pytest.mark.parametrize("schema", [SIMPLE_SCHEMA, SIMPLE_SCHEMA_ORDERED])
    def test_replicated_tablet_node_profiling(self, schema):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema, attributes={"enable_profiling": True})

        tablet_profiling = self._get_table_profiling("//tmp/t")
        def get_all_counters():
            return (
                tablet_profiling.get_counter("write/row_count"),
                tablet_profiling.get_counter("commit/row_count"),
                tablet_profiling.get_counter("write/data_weight"),
                tablet_profiling.get_counter("commit/data_weight"))

        assert get_all_counters() == (0, 0, 0, 0)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test"}], require_sync_replica=False)
        sleep(2)

        assert get_all_counters() == (1, 1, 13, 13)

    @flaky(max_runs=5)
    @pytest.mark.parametrize("schema", [SIMPLE_SCHEMA, SIMPLE_SCHEMA_ORDERED])
    def test_replica_tablet_node_profiling(self, schema):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema, attributes={"enable_profiling": True})
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, schema)

        tablet_profiling = self._get_table_profiling("//tmp/t")

        def get_lag_row_count():
            return tablet_profiling.get_counter("replica/lag_row_count")

        def get_lag_time():
            return tablet_profiling.get_counter("replica/lag_time") / 1e6 # conversion from us to s

        self.sync_enable_table_replica(replica_id)
        sleep(2)

        assert get_lag_row_count() == 0
        assert get_lag_time() == 0

        insert_rows("//tmp/t", [{"key": 0, "value1": "test", "value2": 123}], require_sync_replica=False)
        sleep(2)

        assert get_lag_row_count() == 0
        assert get_lag_time() == 0

        self.sync_unmount_table("//tmp/r", driver=self.replica_driver)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}], require_sync_replica=False)
        sleep(2)

        assert get_lag_row_count() == 1
        assert 2 <= get_lag_time() <= 8

        insert_rows("//tmp/t", [{"key": 2, "value1": "test", "value2": 123}], require_sync_replica=False)
        sleep(2)

        assert get_lag_row_count() == 2
        assert 4 <= get_lag_time() <= 10

        self.sync_mount_table("//tmp/r", driver=self.replica_driver)
        sleep(2)

        assert get_lag_row_count() == 0
        assert get_lag_time() == 0

    @pytest.mark.parametrize("schema", [SIMPLE_SCHEMA, SIMPLE_SCHEMA_ORDERED])
    def test_replication_error(self, schema):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, schema, mount=False)
        self.sync_enable_table_replica(replica_id)

        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1
        tablet_id = tablets[0]["tablet_id"]

        def check_error(message=None):
            errors = get("//tmp/t/@replicas/%s/errors" % replica_id)
            replica_table_tablets = get("#{0}/@tablets".format(replica_id))
            assert len(replica_table_tablets) == 1
            replica_table_tablet = replica_table_tablets[0]
            assert replica_table_tablet["tablet_id"] == tablet_id
            if len(errors) == 0:
                return \
                    message == None and \
                    "replication_error" not in replica_table_tablet
            else:
                return \
                    len(errors) == 1 and \
                    errors[0]["message"] == message and \
                    replica_table_tablet["replication_error"]["message"] == message and \
                    errors[0]["attributes"]["tablet_id"] == tablet_id

        assert check_error()

        insert_rows("//tmp/t", [{"key": 1, "value1": "test1"}], require_sync_replica=False)
        wait(lambda: check_error("Table //tmp/r has no mounted tablets"))

        self.sync_mount_table("//tmp/r", driver=self.replica_driver)
        wait(lambda: check_error())

    def test_replicated_in_memory_fail(self):
        self._create_cells()
        with pytest.raises(YtError):
            self._create_replicated_table("//tmp/t", attributes={"in_memory_mode": "compressed"})
        with pytest.raises(YtError):
            self._create_replicated_table("//tmp/t", attributes={"in_memory_mode": "uncompressed"})

    def test_add_replica_fail1(self):
        with pytest.raises(YtError): create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")

    def test_add_replica_fail2(self):
        create("table", "//tmp/t")
        with pytest.raises(YtError): create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")

    def test_add_replica_fail3(self):
        self._create_replicated_table("//tmp/t", mount=False)
        create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        with pytest.raises(YtError): create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")

    def test_add_remove_replica(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")

        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        assert exists("//tmp/t/@replicas/{0}".format(replica_id))
        attributes = get("#{0}/@".format(replica_id))
        assert attributes["type"] == "table_replica"
        assert attributes["state"] == "disabled"
        remove_table_replica(replica_id)
        assert not exists("#{0}/@".format(replica_id))

    def test_enable_disable_replica_unmounted(self):
        self._create_replicated_table("//tmp/t", mount=False)
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        attributes = get("#{0}/@".format(replica_id), attributes=["state", "tablets"])
        assert attributes["state"] == "disabled"
        assert len(attributes["tablets"]) == 1
        assert attributes["tablets"][0]["state"] == "none"

        alter_table_replica(replica_id, enabled=True)
        attributes = get("#{0}/@".format(replica_id), attributes=["state", "tablets"])
        assert attributes["state"] == "enabled"
        assert len(attributes["tablets"]) == 1
        assert attributes["tablets"][0]["state"] == "none"

        alter_table_replica(replica_id, enabled=False)
        attributes = get("#{0}/@".format(replica_id), attributes=["state", "tablets"])
        assert attributes["state"] == "disabled"
        assert len(attributes["tablets"]) == 1
        assert attributes["tablets"][0]["state"] == "none"

    def test_enable_disable_replica_mounted(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")

        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        assert get("#{0}/@state".format(replica_id)) == "disabled"

        self.sync_enable_table_replica(replica_id)
        assert get("#{0}/@state".format(replica_id)) == "enabled"

        self.sync_disable_table_replica(replica_id)
        assert get("#{0}/@state".format(replica_id)) == "disabled"

    def test_in_sync_replicas_simple(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")

        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        assert get("#{0}/@state".format(replica_id)) == "disabled"

        self._create_replica_table("//tmp/r", replica_id)

        self.sync_enable_table_replica(replica_id)
        assert get("#{0}/@state".format(replica_id)) == "enabled"

        rows = [{"key": 0, "value1": "test", "value2": 42}]
        keys = [{"key": 0}]

        timestamp0 = generate_timestamp()
        assert get_in_sync_replicas("//tmp/t", [], timestamp=timestamp0) == [replica_id]

        wait(lambda: get_in_sync_replicas("//tmp/t", keys, timestamp=timestamp0) == [replica_id])

        insert_rows("//tmp/t", rows, require_sync_replica=False)
        timestamp1 = generate_timestamp()
        assert get_in_sync_replicas("//tmp/t", keys, timestamp=timestamp0) == [replica_id]

        wait(lambda: get_in_sync_replicas("//tmp/t", keys, timestamp=timestamp0) == [replica_id])
        wait(lambda: get_in_sync_replicas("//tmp/t", keys, timestamp=timestamp1) == [replica_id])

        timestamp2 = generate_timestamp()
        assert get_in_sync_replicas("//tmp/t", keys, timestamp=timestamp0) == [replica_id]
        assert get_in_sync_replicas("//tmp/t", keys, timestamp=timestamp1) == [replica_id]

        wait(lambda: get_in_sync_replicas("//tmp/t", keys, timestamp=timestamp2) == [replica_id])
        wait(lambda: get_in_sync_replicas("//tmp/t", [], timestamp=timestamp0) == [replica_id])

    def test_in_sync_relicas_expression(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.EXPRESSION_SCHEMA)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, schema=self.EXPRESSION_SCHEMA)

        self.sync_enable_table_replica(replica_id)

        timestamp0 = generate_timestamp()
        assert get_in_sync_replicas("//tmp/t", [], timestamp=timestamp0) == [replica_id]

        rows = [{"key": 1, "value": 2}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows, require_sync_replica=False)
        timestamp1 = generate_timestamp()
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"hash": 1, "key": 1, "value": 2}])
        wait(lambda: get_in_sync_replicas("//tmp/t", keys, timestamp=timestamp0) == [replica_id])
        wait(lambda: get_in_sync_replicas("//tmp/t", keys, timestamp=timestamp1) == [replica_id])

    def test_in_sync_replicas_disabled(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")

        replica_id1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1")
        replica_id2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2")

        self._create_replica_table("//tmp/r1", replica_id1)
        self._create_replica_table("//tmp/r2", replica_id2)

        self.sync_enable_table_replica(replica_id1)

        assert get("#{0}/@state".format(replica_id1)) == "enabled"
        assert get("#{0}/@state".format(replica_id2)) == "disabled"

        rows = [{"key": 0, "value1": "test", "value2": 42}]
        keys = [{"key": 0}]

        insert_rows("//tmp/t", rows, require_sync_replica=False)
        timestamp = generate_timestamp()

        assert_items_equal(get_in_sync_replicas("//tmp/t", [], timestamp=timestamp), [replica_id1, replica_id2])

        wait(lambda: get_in_sync_replicas("//tmp/t", keys, timestamp=timestamp) == [replica_id1])

        self.sync_enable_table_replica(replica_id2)
        wait(lambda: set(get_in_sync_replicas("//tmp/t", keys, timestamp=timestamp)) == set([replica_id1, replica_id2]))
        wait(lambda: set(get_in_sync_replicas("//tmp/t", [], timestamp=timestamp)) == set([replica_id1, replica_id2]))

    def test_async_replication_sorted(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id)

        self.sync_enable_table_replica(replica_id)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test", "value2": 123}])

        insert_rows("//tmp/t", [{"key": 1, "value1": "new_test"}], update=True, require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "new_test", "value2": 123}])

        insert_rows("//tmp/t", [{"key": 1, "value2": 456}], update=True, require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "new_test", "value2": 456}])

        delete_rows("//tmp/t", [{"key": 1}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [])

    def test_async_replication_ordered(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", self.SIMPLE_SCHEMA_ORDERED)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, self.SIMPLE_SCHEMA_ORDERED)

        self.sync_enable_table_replica(replica_id)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"$tablet_index": 0L, "$row_index": 0L, "key": 1, "value1": "test", "value2": 123}])

        insert_rows("//tmp/t", [{"key": 1, "value1": "new_test"}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)[-1] == {"$tablet_index": 0L, "$row_index": 1L, "key": 1, "value1": "new_test", "value2": YsonEntity()})

        insert_rows("//tmp/t", [{"key": 1, "value2": 456}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)[-1] == {"$tablet_index": 0L, "$row_index": 2L, "key": 1, "value1": YsonEntity(), "value2": 456})

    @flaky(max_runs=5)
    def test_async_replication_bandwidth_limit(self):
        class Inserter:
            def __init__(self, replica_driver):
                self.counter = 0
                self.replica_driver = replica_driver
                self.str100 = "1" * 35  # for total bytes 100

            def insert(self):
                self.counter += 1
                insert_rows(
                    "//tmp/t", [{"key": 1, "value1": self.str100, "value2": self.counter}],
                    require_sync_replica=False)

            def get_inserted_counter(self):
                rows = select_rows("* from [//tmp/r]", driver=self.replica_driver)
                if len(rows) == 0:
                    return 0
                assert len(rows) == 1
                return rows[0]["value2"]

        self._create_cells()
        self._create_replicated_table("//tmp/t", attributes={
            "replication_throttler": {
                "limit": 500
            },
            "max_data_weight_per_replication_commit": 500
        })
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id)

        inserter = Inserter(self.replica_driver)
        for _ in xrange(50):
            inserter.insert()

        self.sync_enable_table_replica(replica_id)

        counter_start = inserter.get_inserted_counter()
        assert counter_start <= 6
        for i in xrange(20):
            sleep(1.0)
            inserted = inserter.get_inserted_counter()
            counter = (inserted - counter_start) / 5
            assert counter - 3 <= i <= counter + 3
            if inserted == inserter.counter:
                break

    def test_sync_replication_sorted(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "sync"})
        replica_id2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r1", replica_id1)
        self._create_replica_table("//tmp/r2", replica_id2)
        self.sync_enable_table_replica(replica_id1)
        self.sync_enable_table_replica(replica_id2)

        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1

        def _do():
            before_index1 = get("#{0}/@tablets/0/current_replication_row_index".format(replica_id1))
            before_index2 = get("#{0}/@tablets/0/current_replication_row_index".format(replica_id2))
            assert before_index1 == before_index2

            before_ts1 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id1))
            before_ts2 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id2))
            assert before_ts1 == before_ts2

            insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}])
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver) == [{"key": 1, "value1": "test", "value2": 123}]
            wait(lambda: select_rows("* from [//tmp/r2]", driver=self.replica_driver) == [{"key": 1, "value1": "test", "value2": 123}])

            insert_rows("//tmp/t", [{"key": 1, "value1": "new_test"}], update=True)
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver) == [{"key": 1, "value1": "new_test", "value2": 123}]
            wait(lambda: select_rows("* from [//tmp/r2]", driver=self.replica_driver) == [{"key": 1, "value1": "new_test", "value2": 123}])

            insert_rows("//tmp/t", [{"key": 1, "value2": 456}], update=True)
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver) == [{"key": 1, "value1": "new_test", "value2": 456}]
            wait(lambda: select_rows("* from [//tmp/r2]", driver=self.replica_driver) == [{"key": 1, "value1": "new_test", "value2": 456}])

            delete_rows("//tmp/t", [{"key": 1}])
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver) == []
            wait(lambda: select_rows("* from [//tmp/r2]", driver=self.replica_driver) == [])

            wait(lambda: get("#{0}/@tablets/0/current_replication_row_index".format(replica_id1)) == before_index1 + 4)
            wait(lambda: get("#{0}/@tablets/0/current_replication_row_index".format(replica_id2)) == before_index1 + 4)

            after_ts1 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id1))
            after_ts2 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id2))
            assert after_ts1 == after_ts2
            assert after_ts1 > before_ts1

        _do()

        alter_table_replica(replica_id1, mode="async")
        alter_table_replica(replica_id1, mode="sync")

        _do()

    def test_sync_replication_ordered(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", self.SIMPLE_SCHEMA_ORDERED)
        replica_id1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "sync"})
        replica_id2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r1", replica_id1, self.SIMPLE_SCHEMA_ORDERED)
        self._create_replica_table("//tmp/r2", replica_id2, self.SIMPLE_SCHEMA_ORDERED)
        self.sync_enable_table_replica(replica_id1)
        self.sync_enable_table_replica(replica_id2)

        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1

        def _last_row(result):
            if (len(result) == 0):
                return {}
            else:
                return result[-1]

        def _do():
            before_index1 = get("#{0}/@tablets/0/current_replication_row_index".format(replica_id1))
            before_index2 = get("#{0}/@tablets/0/current_replication_row_index".format(replica_id2))
            assert before_index1 == before_index2

            before_ts1 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id1))
            before_ts2 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id2))
            assert before_ts1 == before_ts2

            insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}])
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver)[-1] == {'$tablet_index': 0L, '$row_index': before_index1, "key": 1, "value1": "test", "value2": 123}
            wait(lambda: _last_row(select_rows("* from [//tmp/r2]", driver=self.replica_driver)) == {'$tablet_index': 0L, '$row_index': before_index2, "key": 1, "value1": "test", "value2": 123})

            insert_rows("//tmp/t", [{"key": 1, "value1": "new_test"}])
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver)[-1] == {'$tablet_index': 0L, '$row_index': before_index1+1, "key": 1, "value1": "new_test", "value2": YsonEntity()}
            wait(lambda: select_rows("* from [//tmp/r2]", driver=self.replica_driver)[-1] == {'$tablet_index': 0L, '$row_index': before_index2+1, "key": 1, "value1": "new_test", "value2": YsonEntity()})

            insert_rows("//tmp/t", [{"key": 1, "value2": 456}])
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver)[-1] == {'$tablet_index': 0L, '$row_index': before_index1+2, "key": 1, "value1": YsonEntity(), "value2": 456}
            wait(lambda: select_rows("* from [//tmp/r2]", driver=self.replica_driver)[-1] == {'$tablet_index': 0L, '$row_index': before_index2+2, "key": 1, "value1": YsonEntity(), "value2": 456})

            wait(lambda: get("#{0}/@tablets/0/current_replication_row_index".format(replica_id1)) == before_index1 + 3)
            wait(lambda: get("#{0}/@tablets/0/current_replication_row_index".format(replica_id2)) == before_index1 + 3)

            after_ts1 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id1))
            after_ts2 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id2))
            assert after_ts1 == after_ts2
            assert after_ts1 > before_ts1

        _do()

        alter_table_replica(replica_id1, mode="async")
        alter_table_replica(replica_id1, mode="sync")

        _do()

    def test_cannot_sync_write_into_disabled_replica(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r", replica_id)
        with pytest.raises(YtError): insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}])

    def test_upstream_replica_id_check1(self):
        self._create_cells()
        self._create_replica_table("//tmp/r", "1-2-3-4")
        with pytest.raises(YtError): insert_rows("//tmp/r", [{"key": 1, "value2": 456}], driver=self.replica_driver)

    def test_upstream_replica_id_check2(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r", "1-2-3-4")
        self.sync_enable_table_replica(replica_id)
        with pytest.raises(YtError): insert_rows("//tmp/t", [{"key": 1, "value2": 456}])

    @pytest.mark.parametrize("schema", [SIMPLE_SCHEMA, SIMPLE_SCHEMA_ORDERED])
    def test_wait_for_sync(self, schema):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema)
        replica_id1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "sync"})
        replica_id2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r1", replica_id1, schema)
        self._create_replica_table("//tmp/r2", replica_id2, schema)

        self.sync_enable_table_replica(replica_id1)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}])

        def _maybe_add_system_fields(dict, row_index):
            if (schema is self.SIMPLE_SCHEMA_ORDERED):
                dict['$tablet_index'] = 0
                dict['$row_index'] = row_index
                return dict
            else:
                return dict

        assert select_rows("* from [//tmp/r1]", driver=self.replica_driver) == [_maybe_add_system_fields({"key": 1, "value1": "test", "value2": 123}, 0)]
        assert select_rows("* from [//tmp/r2]", driver=self.replica_driver) == []

        alter_table_replica(replica_id1, mode="async")
        alter_table_replica(replica_id2, mode="sync")

        sleep(1.0)

        with pytest.raises(YtError): insert_rows("//tmp/t", [{"key": 1, "value1": "test2", "value2": 456}])

        self.sync_enable_table_replica(replica_id2)

        sleep(1.0)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test2", "value2": 456}])
        assert select_rows("* from [//tmp/r2]", driver=self.replica_driver)[-1] == _maybe_add_system_fields({"key": 1, "value1": "test2", "value2": 456}, 1)

        sleep(1.0)

        assert select_rows("* from [//tmp/r1]", driver=self.replica_driver)[-1] == _maybe_add_system_fields({"key": 1, "value1": "test2", "value2": 456}, 1)

    def test_disable_propagates_replication_row_index(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id)

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        self.sync_enable_table_replica(replica_id)

        assert get("#{0}/@tablets/0/current_replication_row_index".format(replica_id)) == 0

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}], require_sync_replica=False)
        sleep(1.0)
        assert select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test", "value2": 123}]

        self.sync_disable_table_replica(replica_id)

        assert get("#{0}/@tablets/0/current_replication_row_index".format(replica_id)) == 1

    def test_unmount_propagates_replication_row_index(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id)

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        self.sync_enable_table_replica(replica_id)

        assert get("#{0}/@tablets/0/current_replication_row_index".format(replica_id)) == 0

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}], require_sync_replica=False)
        sleep(1.0)
        assert select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test", "value2": 123}]

        self.sync_unmount_table("//tmp/t")

        assert get("#{0}/@tablets/0/current_replication_row_index".format(replica_id)) == 1

    @pytest.mark.parametrize("with_data, schema",
         [(False, SIMPLE_SCHEMA), (True, SIMPLE_SCHEMA), (False, SIMPLE_SCHEMA_ORDERED), (True, SIMPLE_SCHEMA_ORDERED)]
    )
    def test_start_replication_timestamp(self, with_data, schema):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test"}], require_sync_replica=False)

        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={
            "start_replication_timestamp": generate_timestamp()
        })
        self._create_replica_table("//tmp/r", replica_id, schema)

        self.sync_enable_table_replica(replica_id)

        if with_data:
            insert_rows("//tmp/t", [{"key": 2, "value1": "test"}], require_sync_replica=False)

        def _maybe_add_system_fields(dict):
            if (schema is self.SIMPLE_SCHEMA_ORDERED):
                dict['$tablet_index'] = 0
                dict['$row_index'] = 0
                return dict
            else:
                return dict

        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == \
            ([_maybe_add_system_fields({"key": 2, "value1": "test", "value2": YsonEntity()})] if with_data else \
            []))

    @pytest.mark.parametrize("ttl, chunk_count, trimmed_row_count, mode",
        [a + b
            for a in [(0, 1, 1), (60000, 2, 0)]
            for b in [("async",) , ("sync",)]
        ]
    )
    def test_replication_trim(self, ttl, chunk_count, trimmed_row_count, mode):
        self._create_cells()
        self._create_replicated_table("//tmp/t", attributes={"min_replication_log_ttl": ttl})
        self.sync_mount_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r",
            attributes={"mode": mode})
        self._create_replica_table("//tmp/r", replica_id)

        self.sync_enable_table_replica(replica_id)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test1"}], require_sync_replica=False)
        sleep(1.0)
        assert select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test1", "value2": YsonEntity()}]

        self.sync_unmount_table("//tmp/t")
        assert get("//tmp/t/@chunk_count") == 1
        assert get("//tmp/t/@tablets/0/flushed_row_count") == 1
        assert get("//tmp/t/@tablets/0/trimmed_row_count") == 0

        self.sync_mount_table("//tmp/t")
        sleep(1.0)
        insert_rows("//tmp/t", [{"key": 2, "value1": "test2"}], require_sync_replica=False)
        sleep(1.0)
        assert select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test1", "value2": YsonEntity()}, {"key": 2, "value1": "test2", "value2": YsonEntity()}]
        self.sync_unmount_table("//tmp/t")

        assert get("//tmp/t/@chunk_count") == chunk_count
        assert get("//tmp/t/@tablets/0/flushed_row_count") == 2
        assert get("//tmp/t/@tablets/0/trimmed_row_count") == trimmed_row_count

    def test_aggregate_replication(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.AGGREGATE_SCHEMA)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, schema=self.AGGREGATE_SCHEMA)

        self.sync_enable_table_replica(replica_id)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test1"}], require_sync_replica=False)
        sleep(1.0)
        assert select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test1", "value2": YsonEntity()}]

        insert_rows("//tmp/t", [{"key": 1, "value1": "test2", "value2": 100}], require_sync_replica=False)
        sleep(1.0)
        assert select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test2", "value2": 100}]

        insert_rows("//tmp/t", [{"key": 1, "value2": 50}], aggregate=True, update=True, require_sync_replica=False)
        sleep(1.0)
        assert select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test2", "value2": 150}]

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

        self.sync_enable_table_replica(replica_id)
        sleep(1.0)
        assert get_lag_time() == 0

        self.sync_disable_table_replica(replica_id)
        sleep(1.0)
        assert get_lag_time() == 0

        insert_rows("//tmp/t", [{"key": 1, "value1": "test1"}], require_sync_replica=False)
        sleep(1.0)

        shift = get_lag_time()
        assert shift > 2000

        for i in xrange(10):
            sleep(1.0)
            assert shift + i * 1000 <= get_lag_time() <= shift + (i + 4) * 1000

        self.sync_enable_table_replica(replica_id)
        sleep(1.0)
        assert get_lag_time() == 0

    def test_expression_replication(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.EXPRESSION_SCHEMA)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, schema=self.EXPRESSION_SCHEMA)

        self.sync_enable_table_replica(replica_id)

        insert_rows("//tmp/t", [{"key": 1, "value": 2}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"hash": 1, "key": 1, "value": 2}])

        insert_rows("//tmp/t", [{"key": 12, "value": 12}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [
            {"hash": 1, "key": 1, "value": 2},
            {"hash": 2, "key": 12, "value": 12}])

        delete_rows("//tmp/t", [{"key": 1}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"hash": 2, "key": 12, "value": 12}])

    def test_shard_replication(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.SIMPLE_SCHEMA, attributes={"pivot_keys": [[], [10]]})
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, schema=self.SIMPLE_SCHEMA)

        self.sync_enable_table_replica(replica_id)

        insert_rows("//tmp/t", [{"key": 1, "value1": "v", "value2": 2}], require_sync_replica=False)
        sleep(1.0)
        assert select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "v", "value2": 2}]

        # ensuring that we have correctly populated data here
        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 2
        assert tablets[0]["index"] == 0
        assert tablets[0]["pivot_key"] == []
        assert tablets[1]["index"] == 1
        assert tablets[1]["pivot_key"] == [10]

    def test_reshard_replication(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.SIMPLE_SCHEMA, mount=False)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, schema=self.SIMPLE_SCHEMA)

        self.sync_enable_table_replica(replica_id)

        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1

        reshard_table("//tmp/t", [[], [10]])
        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 2

        reshard_table("//tmp/t", [[], [10], [20]])
        tablets = get("//tmp/t/@tablets")

        # ensuring that we have correctly populated data here
        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 3
        assert tablets[0]["index"] == 0
        assert tablets[0]["pivot_key"] == []
        assert tablets[1]["index"] == 1
        assert tablets[1]["pivot_key"] == [10]
        assert tablets[2]["index"] == 2
        assert tablets[2]["pivot_key"] == [20]

        self.sync_mount_table("//tmp/t")
        self.sync_unmount_table("//tmp/t")

    def test_replica_ops_require_exclusive_lock(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", mount=False)

        tx1 = start_transaction()
        lock("//tmp/t", mode="exclusive", tx=tx1)
        with pytest.raises(YtError): create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        abort_transaction(tx1)

        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        tx2 = start_transaction()
        lock("//tmp/t", mode="exclusive", tx=tx2)
        with pytest.raises(YtError): remove_table_replica(replica_id)

    def test_lookup(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.AGGREGATE_SCHEMA)
        replica_id1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "async"})
        replica_id2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2", attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r1", replica_id1, schema=self.AGGREGATE_SCHEMA)
        self._create_replica_table("//tmp/r2", replica_id2, schema=self.AGGREGATE_SCHEMA)

        self.sync_enable_table_replica(replica_id1)
        self.sync_enable_table_replica(replica_id2)

        for i in xrange(10):
            insert_rows("//tmp/t", [{"key": i, "value1": "test" + str(i)}])

        for i in xrange(9):
            assert lookup_rows("//tmp/t", [{"key": i}, {"key": i + 1}], column_names=["key", "value1"]) == \
                               [{"key": i, "value1": "test" + str(i)}, {"key": i + 1, "value1": "test" + str(i + 1)}]

        assert lookup_rows("//tmp/t", [{"key": 100000}]) == []

        self.sync_disable_table_replica(replica_id2)
        alter_table_replica(replica_id2, mode="async")
        clear_metadata_caches()
        sleep(1.0)
        with pytest.raises(YtError): insert_rows("//tmp/t", [{"key": 666, "value1": "hello"}])
        insert_rows("//tmp/t", [{"key": 666, "value1": "hello"}], require_sync_replica=False)

        alter_table_replica(replica_id2, mode="sync")
        sleep(1.0)
        with pytest.raises(YtError): lookup_rows("//tmp/t", [{"key": 666}]) == []

        self.sync_enable_table_replica(replica_id2)
        sleep(1.0)
        assert lookup_rows("//tmp/t", [{"key": 666}], column_names=["key", "value1"]) == [{"key": 666, "value1": "hello"}]

        alter_table_replica(replica_id2, mode="async")
        sleep(1.0)
        with pytest.raises(YtError): lookup_rows("//tmp/t", [{"key": 666}]) == []

    def test_select(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.AGGREGATE_SCHEMA)
        replica_id1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "async"})
        replica_id2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2", attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r1", replica_id1, schema=self.AGGREGATE_SCHEMA)
        self._create_replica_table("//tmp/r2", replica_id2, schema=self.AGGREGATE_SCHEMA)

        self.sync_enable_table_replica(replica_id1)
        self.sync_enable_table_replica(replica_id2)

        rows = [{"key": i, "value1": "test" + str(i)} for i in xrange(10)]
        insert_rows("//tmp/t", rows)
        assert_items_equal(select_rows("key, value1 from [//tmp/t]"), rows)
        assert_items_equal(select_rows("sum(key) from [//tmp/t] group by 0"), [{"sum(key)": 45}])

        create("table", "//tmp/z", attributes={
            "dynamic": True,
            "schema": self.SIMPLE_SCHEMA
        })
        with pytest.raises(YtError): select_rows("* from [//tmp/t] as t1 join [//tmp/z] as t2 on t1.key = t2.key")

        alter_table_replica(replica_id2, mode="async")
        sleep(1.0)
        with pytest.raises(YtError): select_rows("* from [//tmp/t]")

    def test_local_sync_replica_yt_7571(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", "primary", "//tmp/r", attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r", replica_id, replica_driver=self.primary_driver)
        self.sync_enable_table_replica(replica_id)

        rows = [{"key": i, "value1": "test" + str(i)} for i in xrange(10)]
        insert_rows("//tmp/t", rows)

        assert_items_equal(select_rows("key, value1 from [//tmp/t]", driver=self.primary_driver), rows)
        assert_items_equal(select_rows("key, value1 from [//tmp/r]", driver=self.primary_driver), rows)

    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_inverted_schema(self, mode):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": mode})
        self._create_replica_table("//tmp/r", replica_id, schema=self.PERTURBED_SCHEMA)
        self.sync_enable_table_replica(replica_id)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 10}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test", "value2": 10}])

        delete_rows("//tmp/t", [{"key": 1}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [])

##################################################################

class TestReplicatedDynamicTablesMulticell(TestReplicatedDynamicTables):
    NUM_SECONDARY_MASTER_CELLS = 2
