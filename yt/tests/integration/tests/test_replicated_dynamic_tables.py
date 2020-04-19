import pytest

from test_dynamic_tables import DynamicTablesBase

from yt_env_setup import parametrize_external
from yt_commands import *
from time import sleep, time
from yt.yson import YsonEntity
from yt.environment.helpers import assert_items_equal, wait
from yt.wrapper import YtResponseError

from flaky import flaky

##################################################################

SIMPLE_SCHEMA_SORTED = [
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

REQUIRED_SCHEMA = [
    {"name": "key1", "type": "int64", "sort_order": "ascending", "required": True},
    {"name": "key2", "type": "int64", "sort_order": "ascending"},
    {"name": "value1", "type": "int64", "required": True},
    {"name": "value2", "type": "int64"}
]
REQUIREDLESS_SCHEMA = [
    {"name": "key1", "type": "int64", "sort_order": "ascending"},
    {"name": "key2", "type": "int64", "sort_order": "ascending"},
    {"name": "value1", "type": "int64"},
    {"name": "value2", "type": "int64"}
]

EXPRESSION_SCHEMA = [
    {"name": "hash", "type": "int64", "sort_order": "ascending", "expression": "key % 10"},
    {"name": "key", "type": "int64", "sort_order": "ascending"},
    {"name": "value", "type": "int64"},
]
EXPRESSIONLESS_SCHEMA = [
    {"name": "hash", "type": "int64", "sort_order": "ascending"},
    {"name": "key", "type": "int64", "sort_order": "ascending"},
    {"name": "value", "type": "int64"},
]

##################################################################

class TestReplicatedDynamicTablesBase(DynamicTablesBase):
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

    def setup(self):
        self.SIMPLE_SCHEMA_SORTED = SIMPLE_SCHEMA_SORTED
        self.SIMPLE_SCHEMA_ORDERED = SIMPLE_SCHEMA_ORDERED
        self.PERTURBED_SCHEMA = PERTURBED_SCHEMA
        self.AGGREGATE_SCHEMA = AGGREGATE_SCHEMA
        self.REQUIRED_SCHEMA = REQUIRED_SCHEMA
        self.REQUIREDLESS_SCHEMA = REQUIREDLESS_SCHEMA
        self.EXPRESSION_SCHEMA = EXPRESSION_SCHEMA
        self.EXPRESSIONLESS_SCHEMA = EXPRESSIONLESS_SCHEMA
        self.REPLICA_CLUSTER_NAME = "remote_0"

        self.replica_driver = get_driver(cluster=self.REPLICA_CLUSTER_NAME)
        self.primary_driver = get_driver(cluster="primary")

    def teardown(self):
        self.replica_driver = None
        self.primary_driver = None

    def _get_table_attributes(self, schema):
        return {
            "dynamic": True,
            "schema": schema
        }

    def _create_replicated_table(self, path, schema=SIMPLE_SCHEMA_SORTED, mount=True, **attributes):
        attributes.update(self._get_table_attributes(schema))
        attributes["enable_replication_logging"] = True
        create("replicated_table", path, attributes=attributes)
        if mount:
            sync_mount_table(path)

    def _create_replica_table(self, path, replica_id=None, schema=SIMPLE_SCHEMA_SORTED, mount=True, replica_driver=None, **kwargs):
        if not replica_driver:
            replica_driver = self.replica_driver
        attributes = self._get_table_attributes(schema)
        if replica_id:
            attributes["upstream_replica_id"] = replica_id
        attributes.update(kwargs)
        create("table", path, attributes=attributes, driver=replica_driver)
        if mount:
            sync_mount_table(path, driver=replica_driver)

    def _create_cells(self):
        sync_create_cells(1)
        sync_create_cells(1, driver=self.replica_driver)

##################################################################

class TestReplicatedDynamicTables(TestReplicatedDynamicTablesBase):
    @authors("savrus")
    def test_replicated_table_must_be_dynamic(self):
        with pytest.raises(YtError):
            create("replicated_table", "//tmp/t")

    @authors("savrus")
    def test_replicated_table_clone_forbidden(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")

        with pytest.raises(YtError):
            move("//tmp/t", "//tmp/s")

    @authors("babenko")
    def test_replica_cluster_must_exist(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", SIMPLE_SCHEMA_SORTED)

        with pytest.raises(YtError):
            create_table_replica("//tmp/t", "nonexisting", "//tmp/r")

    @authors("babenko", "aozeritsky")
    @pytest.mark.parametrize("schema", [SIMPLE_SCHEMA_SORTED, SIMPLE_SCHEMA_ORDERED])
    def test_simple(self, schema):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test"}], require_sync_replica=False)
        if (schema is self.SIMPLE_SCHEMA_SORTED):
            delete_rows("//tmp/t", [{"key": 2}], require_sync_replica=False)

    @authors("babenko", "gridem")
    @pytest.mark.parametrize("schema", [SIMPLE_SCHEMA_SORTED, SIMPLE_SCHEMA_ORDERED])
    def test_replication_error(self, schema):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, schema, mount=False)
        sync_enable_table_replica(replica_id)

        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1
        tablet_id = tablets[0]["tablet_id"]

        def error_contains_message(error, message):
            if error["message"] == message:
                return True
            for inner_error in error["inner_errors"]:
                if error_contains_message(inner_error, message):
                    return True
            return False

        def check_error(message=None):
            error_count = get("//tmp/t/@replicas/{}/error_count".format(replica_id))
            if error_count != get("#{}/@error_count".format(replica_id)):
                return False
            if error_count != int(get("#{}/@tablets/0/has_error".format(replica_id))):
                return False

            orchid = self._find_tablet_orchid(get_tablet_leader_address(tablet_id), tablet_id)
            errors = orchid["replication_errors"]

            if message is None:
                return error_count == 0 and len(errors) == 0
            else:
                return error_count == 1 and len(errors) == 1 and error_contains_message(errors[replica_id], message)

        assert check_error()

        insert_rows("//tmp/t", [{"key": 1, "value1": "test1"}], require_sync_replica=False)
        wait(lambda: check_error("Table //tmp/r has no mounted tablets"))

        sync_mount_table("//tmp/r", driver=self.replica_driver)
        wait(lambda: check_error())

    @authors("gridem")
    def test_replicated_in_memory_fail(self):
        self._create_cells()
        with pytest.raises(YtError):
            self._create_replicated_table("//tmp/t", in_memory_mode="compressed")
        with pytest.raises(YtError):
            self._create_replicated_table("//tmp/t", in_memory_mode="uncompressed")

    @authors("babenko")
    def test_add_replica_fail1(self):
        with pytest.raises(YtError): create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")

    @authors("babenko")
    def test_add_replica_fail2(self):
        create("table", "//tmp/t")
        with pytest.raises(YtError): create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")

    @authors("babenko")
    def test_add_replica_fail3(self):
        self._create_replicated_table("//tmp/t", mount=False)
        create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        with pytest.raises(YtError): create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")

    @authors("babenko")
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

    @authors("babenko")
    def test_none_state_after_unmount(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", mount=False)

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")

        sync_mount_table("//tmp/t")
        sync_enable_table_replica(replica_id)

        attributes = get("#{0}/@".format(replica_id), attributes=["state", "tablets"])
        assert attributes["state"] == "enabled"
        assert len(attributes["tablets"]) == 1
        assert attributes["tablets"][0]["state"] == "enabled"

        sync_unmount_table("//tmp/t")

        attributes = get("#{0}/@".format(replica_id), attributes=["state", "tablets"])
        assert attributes["state"] == "enabled"
        assert len(attributes["tablets"]) == 1
        assert attributes["tablets"][0]["state"] == "none"

    @authors("babenko")
    def test_enable_disable_replica_unmounted(self):
        self._create_replicated_table("//tmp/t", mount=False)
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

    @authors("babenko")
    def test_enable_disable_replica_mounted(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")

        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        assert get("#{0}/@state".format(replica_id)) == "disabled"

        sync_enable_table_replica(replica_id)
        assert get("#{0}/@state".format(replica_id)) == "enabled"

        sync_disable_table_replica(replica_id)
        assert get("#{0}/@state".format(replica_id)) == "disabled"

    @authors("gridem")
    def test_in_sync_replicas_simple(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")

        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        assert get("#{0}/@state".format(replica_id)) == "disabled"

        self._create_replica_table("//tmp/r", replica_id)

        sync_enable_table_replica(replica_id)
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

    @authors("gridem")
    def test_in_sync_replicas_expression(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.EXPRESSION_SCHEMA)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, schema=self.EXPRESSION_SCHEMA)

        sync_enable_table_replica(replica_id)

        timestamp0 = generate_timestamp()
        assert get_in_sync_replicas("//tmp/t", [], timestamp=timestamp0) == [replica_id]

        rows = [{"key": 1, "value": 2}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows, require_sync_replica=False)
        timestamp1 = generate_timestamp()
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"hash": 1, "key": 1, "value": 2}])
        wait(lambda: get_in_sync_replicas("//tmp/t", keys, timestamp=timestamp0) == [replica_id])
        wait(lambda: get_in_sync_replicas("//tmp/t", keys, timestamp=timestamp1) == [replica_id])

    @authors("gridem")
    def test_in_sync_replicas_disabled(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")

        replica_id1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1")
        replica_id2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2")

        self._create_replica_table("//tmp/r1", replica_id1)
        self._create_replica_table("//tmp/r2", replica_id2)

        sync_enable_table_replica(replica_id1)

        assert get("#{0}/@state".format(replica_id1)) == "enabled"
        assert get("#{0}/@state".format(replica_id2)) == "disabled"

        rows = [{"key": 0, "value1": "test", "value2": 42}]
        keys = [{"key": 0}]

        insert_rows("//tmp/t", rows, require_sync_replica=False)
        timestamp = generate_timestamp()

        assert_items_equal(get_in_sync_replicas("//tmp/t", [], timestamp=timestamp), [replica_id1, replica_id2])

        wait(lambda: get_in_sync_replicas("//tmp/t", keys, timestamp=timestamp) == [replica_id1])

        sync_enable_table_replica(replica_id2)
        wait(lambda: sorted(get_in_sync_replicas("//tmp/t", keys, timestamp=timestamp)) == sorted([replica_id1, replica_id2]))
        wait(lambda: sorted(get_in_sync_replicas("//tmp/t", [], timestamp=timestamp)) == sorted([replica_id1, replica_id2]))

    @authors("babenko")
    def test_async_replication_sorted(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id)

        sync_enable_table_replica(replica_id)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test", "value2": 123}])

        insert_rows("//tmp/t", [{"key": 1, "value1": "new_test"}], update=True, require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "new_test", "value2": 123}])

        insert_rows("//tmp/t", [{"key": 1, "value2": 456}], update=True, require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "new_test", "value2": 456}])

        delete_rows("//tmp/t", [{"key": 1}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [])

    @authors("aozeritsky")
    @pytest.mark.parametrize("preserve_tablet_index", [False, True])
    def test_async_replication_ordered(self, preserve_tablet_index):
        self._create_cells()
        self._create_replicated_table("//tmp/t", self.SIMPLE_SCHEMA_ORDERED, preserve_tablet_index=preserve_tablet_index)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, self.SIMPLE_SCHEMA_ORDERED)

        sync_enable_table_replica(replica_id)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"$tablet_index": 0L, "$row_index": 0L, "key": 1, "value1": "test", "value2": 123}])

        insert_rows("//tmp/t", [{"key": 1, "value1": "new_test"}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)[-1] == {"$tablet_index": 0L, "$row_index": 1L, "key": 1, "value1": "new_test", "value2": YsonEntity()})

        insert_rows("//tmp/t", [{"key": 1, "value2": 456}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)[-1] == {"$tablet_index": 0L, "$row_index": 2L, "key": 1, "value1": YsonEntity(), "value2": 456})

    @authors("aozeritsky")
    @pytest.mark.parametrize("mode", ["async", "sync"])
    def test_replication_preserve_tablet_index(self, mode):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.SIMPLE_SCHEMA_ORDERED, tablet_count=2, preserve_tablet_index=True)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": mode})
        self._create_replica_table("//tmp/r", replica_id, self.SIMPLE_SCHEMA_ORDERED, tablet_count=2)

        sync_enable_table_replica(replica_id)

        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 2

        tablets = get("//tmp/r/@tablets", driver=self.replica_driver)
        assert len(tablets) == 2

        insert_rows("//tmp/t", [{"$tablet_index": 0, "key": 1, "value1": "test1", "value2": 123}], require_sync_replica=False)
        insert_rows("//tmp/t", [{"$tablet_index": 1, "key": 2, "value1": "test2", "value2": 124}], require_sync_replica=False)

        wait(lambda: select_rows("* from [//tmp/r] where [$tablet_index]=0", driver=self.replica_driver) == [{"$tablet_index": 0L, "$row_index": 0L, "key": 1, "value1": "test1", "value2": 123}])
        wait(lambda: select_rows("* from [//tmp/r] where [$tablet_index]=1", driver=self.replica_driver) == [{"$tablet_index": 1L, "$row_index": 0L, "key": 2, "value1": "test2", "value2": 124}])

    @authors("aozeritsky")
    def test_replication_sorted_with_tablet_index(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id)

        sync_enable_table_replica(replica_id)

        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"$tablet_index": 1, "key": 1, "value1": "test", "value2": 123}], require_sync_replica=False)

    @authors("gridem")
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
        self._create_replicated_table("//tmp/t", replication_throttler={"limit": 500}, max_data_weight_per_replication_commit=500)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id)

        inserter = Inserter(self.replica_driver)
        for _ in xrange(50):
            inserter.insert()

        sync_enable_table_replica(replica_id)

        counter_start = inserter.get_inserted_counter()
        assert counter_start <= 7
        for i in xrange(20):
            sleep(1.0)
            inserted = inserter.get_inserted_counter()
            counter = (inserted - counter_start) / 5
            assert counter - 3 <= i <= counter + 3
            if inserted == inserter.counter:
                break

    @authors("babenko")
    def test_sync_replication_sorted(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "sync"})
        replica_id2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r1", replica_id1)
        self._create_replica_table("//tmp/r2", replica_id2)
        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

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

        sync_alter_table_replica_mode(replica_id1, "async")
        sync_alter_table_replica_mode(replica_id1, "sync")

        _do()

    @authors("aozeritsky")
    @pytest.mark.parametrize("commit_ordering", ["weak", "strong"])
    def test_sync_replication_ordered(self, commit_ordering):
        self._create_cells()
        self._create_replicated_table("//tmp/t", self.SIMPLE_SCHEMA_ORDERED)
        replica_id1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "sync"})
        replica_id2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r1", replica_id1, self.SIMPLE_SCHEMA_ORDERED, commit_ordering=commit_ordering)
        self._create_replica_table("//tmp/r2", replica_id2, self.SIMPLE_SCHEMA_ORDERED, commit_ordering=commit_ordering)
        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

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
            # TODO(babenko): fix distributed commit for ordered tables
            wait(lambda: _last_row(select_rows("* from [//tmp/r1]", driver=self.replica_driver)) == {'$tablet_index': 0L, '$row_index': before_index1, "key": 1, "value1": "test", "value2": 123})
            wait(lambda: _last_row(select_rows("* from [//tmp/r2]", driver=self.replica_driver)) == {'$tablet_index': 0L, '$row_index': before_index2, "key": 1, "value1": "test", "value2": 123})

            insert_rows("//tmp/t", [{"key": 1, "value1": "new_test"}])
            wait(lambda: select_rows("* from [//tmp/r1]", driver=self.replica_driver)[-1] == {'$tablet_index': 0L, '$row_index': before_index1+1, "key": 1, "value1": "new_test", "value2": YsonEntity()})
            wait(lambda: select_rows("* from [//tmp/r2]", driver=self.replica_driver)[-1] == {'$tablet_index': 0L, '$row_index': before_index2+1, "key": 1, "value1": "new_test", "value2": YsonEntity()})

            insert_rows("//tmp/t", [{"key": 1, "value2": 456}])
            wait(lambda: select_rows("* from [//tmp/r1]", driver=self.replica_driver)[-1] == {'$tablet_index': 0L, '$row_index': before_index1+2, "key": 1, "value1": YsonEntity(), "value2": 456})
            wait(lambda: select_rows("* from [//tmp/r2]", driver=self.replica_driver)[-1] == {'$tablet_index': 0L, '$row_index': before_index2+2, "key": 1, "value1": YsonEntity(), "value2": 456})

            wait(lambda: get("#{0}/@tablets/0/current_replication_row_index".format(replica_id1)) == before_index1 + 3)
            wait(lambda: get("#{0}/@tablets/0/current_replication_row_index".format(replica_id2)) == before_index1 + 3)

            after_ts1 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id1))
            after_ts2 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id2))
            assert after_ts1 == after_ts2
            assert after_ts1 > before_ts1

        _do()

        sync_alter_table_replica_mode(replica_id1, "async")
        sync_alter_table_replica_mode(replica_id1, "sync")

        _do()

    @authors("aozeritsky")
    def test_replication_unversioned(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "sync"})
        replica_id2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2", attributes={"mode": "async", "preserve_timestamps": "false"})
        self._create_replica_table("//tmp/r1", replica_id1)
        # self._create_replica_table("//tmp/r2", replica_id2)
        self._create_replica_table("//tmp/r2")
        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

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
            wait(lambda: select_rows("* from [//tmp/r2] where key=1", driver=self.replica_driver) == [{"key": 1, "value1": "test", "value2": 123}])

            insert_rows("//tmp/t", [{"key": 1, "value1": "new_test"}], update=True)
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver) == [{"key": 1, "value1": "new_test", "value2": 123}]
            wait(lambda: select_rows("* from [//tmp/r2] where key=1", driver=self.replica_driver) == [{"key": 1, "value1": "new_test", "value2": 123}])

            insert_rows("//tmp/t", [{"key": 1, "value2": 456}], update=True)
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver) == [{"key": 1, "value1": "new_test", "value2": 456}]
            wait(lambda: select_rows("* from [//tmp/r2] where key=1", driver=self.replica_driver) == [{"key": 1, "value1": "new_test", "value2": 456}])

            delete_rows("//tmp/t", [{"key": 1}])
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver) == []
            wait(lambda: select_rows("* from [//tmp/r2] where key=1", driver=self.replica_driver) == [])

            insert_rows("//tmp/r2", [{"key": 2, "value2": 457}], update=True, driver=self.replica_driver)
            wait(lambda: select_rows("* from [//tmp/r2] where key=2", driver=self.replica_driver) == [{"key": 2, "value1": YsonEntity(), "value2": 457}])

            wait(lambda: get("#{0}/@tablets/0/current_replication_row_index".format(replica_id1)) == before_index1 + 4)
            wait(lambda: get("#{0}/@tablets/0/current_replication_row_index".format(replica_id2)) == before_index1 + 4)

            after_ts1 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id1))
            after_ts2 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id2))
            assert after_ts1 == after_ts2
            assert after_ts1 > before_ts1

        _do()

        sync_alter_table_replica_mode(replica_id1, "async")
        sync_alter_table_replica_mode(replica_id1, "sync")

        _do()

    @authors("aozeritsky")
    def test_replication_with_invalid_options(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")

        with pytest.raises(YtError): create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "async", "preserve_timestamps": "false", "atomicity": "none"})
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "async", "preserve_timestamps": "false"})
        with pytest.raises(YtError): alter_table_replica(replica_id, atomicity="none")

    @authors("aozeritsky")
    def test_sync_replication_switch(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", SIMPLE_SCHEMA_SORTED, replicated_table_options={"enable_replicated_table_tracker": False})
        replica_id1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "sync"})
        replica_id2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r1", replica_id1)
        self._create_replica_table("//tmp/r2", replica_id2)
        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        set("//tmp/t/@replicated_table_options", {"enable_replicated_table_tracker": True, "tablet_cell_bundle_name_failure_interval": 1000})

        remove("//tmp/r1", driver=self.replica_driver)

        wait(lambda: get("#{0}/@mode".format(replica_id1)) == "async")
        wait(lambda: get("#{0}/@mode".format(replica_id2)) == "sync")

    @authors("aozeritsky")
    def test_sync_replication_switch_bundle_health(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", SIMPLE_SCHEMA_SORTED, replicated_table_options={"enable_replicated_table_tracker": False})
        replica_id1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "sync"})
        replica_id2 = create_table_replica("//tmp/t", "primary", "//tmp/r2", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r1", replica_id1)
        self._create_replica_table("//tmp/r2", replica_id2, replica_driver = self.primary_driver)
        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        set("//tmp/t/@replicated_table_options", {"enable_replicated_table_tracker": True, "tablet_cell_bundle_name_failure_interval": 1000})

        wait(lambda: get("//sys/tablet_cell_bundles/default/@health", driver=self.replica_driver) == "good")

        nodes = ls("//sys/cluster_nodes", driver=self.replica_driver)
        for node in nodes:
            set("//sys/cluster_nodes/" + node + "/@banned", "true", driver=self.replica_driver)

        wait(lambda: get("//sys/tablet_cell_bundles/default/@health", driver=self.replica_driver) != "good")

        wait(lambda: get("#{0}/@mode".format(replica_id1)) == "async")
        wait(lambda: get("#{0}/@mode".format(replica_id2)) == "sync")

        for node in nodes:
            set("//sys/cluster_nodes/" + node + "/@banned", "false", driver=self.replica_driver)

        wait(lambda: get("//sys/tablet_cell_bundles/default/@health", driver=self.replica_driver) == "good")

    @authors("aozeritsky")
    def test_sync_replication_switch_with_min_sync_replica(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", SIMPLE_SCHEMA_SORTED, replicated_table_options={"enable_replicated_table_tracker": False})
        replica_id1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "async"})
        replica_id2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r1", replica_id1)
        self._create_replica_table("//tmp/r2", replica_id2)
        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        set("//tmp/t/@replicated_table_options", {"enable_replicated_table_tracker": True, "min_sync_replica_count": 2, "tablet_cell_bundle_name_failure_interval": 1000})

        wait(lambda: get("#{0}/@mode".format(replica_id1)) == "sync")
        wait(lambda: get("#{0}/@mode".format(replica_id2)) == "sync")

    def _get_sync_replicas(self, replica_ids):
        result = 0
        for replica_id in replica_ids:
            if get("#{0}/@mode".format(replica_id)) == "sync":
                result = result + 1
        return result

    @authors("aozeritsky")
    def test_sync_replication_switch_with_min_max_sync_replica(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", SIMPLE_SCHEMA_SORTED, replicated_table_options={"enable_replicated_table_tracker": False})
        replica_ids = []
        for i in range(5):
            table_path = "//tmp/r{}".format(i)
            replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, table_path, attributes={"mode": "async"})
            self._create_replica_table(table_path, replica_id)
            sync_enable_table_replica(replica_id)
            replica_ids.append(replica_id)

        set("//tmp/t/@replicated_table_options", {"enable_replicated_table_tracker": True, "min_sync_replica_count": 2, "max_sync_replica_count": 4, "tablet_cell_bundle_name_failure_interval": 1000})

        wait(lambda: self._get_sync_replicas(replica_ids) == 4)

        def brake_sync_replicas(count):
            result = []
            for i in range(5):
                if count <= 0:
                    break
                table_path = "//tmp/r{}".format(i)
                if get("#{0}/@mode".format(replica_ids[i])) == "sync" and exists(table_path, driver=self.replica_driver):
                    remove(table_path, driver=self.replica_driver)
                    count = count - 1
                    result.append(replica_ids[i])

            return result

        broken_replicas = []
        broken_replicas = broken_replicas + brake_sync_replicas(1)
        for replica in broken_replicas:
            wait(lambda: get("#{0}/@mode".format(replica)) == "async")
        wait(lambda: self._get_sync_replicas(replica_ids) == 4)

        broken_replicas = broken_replicas + brake_sync_replicas(2)
        for replica in broken_replicas:
            wait(lambda: get("#{0}/@mode".format(replica)) == "async")
        wait(lambda: self._get_sync_replicas(replica_ids) == 2)

        broken_replicas = brake_sync_replicas(1)
        assert(len(broken_replicas) == 1)
        wait(lambda: self._get_sync_replicas(replica_ids) == 2)

    @authors("ivanashevi")
    def test_sync_replication_switch_with_not_enough_healthy_replicas(self):
        DELTA_MASTER_CONFIG = {
            "replicated_table_tracker": {
                "check_period": 100
            }
        }

        self._create_cells()
        self._create_replicated_table("//tmp/t", SIMPLE_SCHEMA_SORTED, replicated_table_options={"enable_replicated_table_tracker": False})
        replica_ids = []
        for i in range(5):
            table_path = "//tmp/r{}".format(i)
            replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, table_path, attributes={"mode": "async"})
            self._create_replica_table(table_path, replica_id)
            sync_enable_table_replica(replica_id)
            replica_ids.append(replica_id)

        set("//tmp/t/@replicated_table_options", {"enable_replicated_table_tracker": True, "min_sync_replica_count": 3, "max_sync_replica_count": 4, "tablet_cell_bundle_name_failure_interval": 1000})

        wait(lambda: self._get_sync_replicas(replica_ids) == 4)

        for i in range(3):
            remove("//tmp/r{}".format(i), driver=self.replica_driver)
        wait(lambda: self._get_sync_replicas(replica_ids) == 3)

        replica_modes = {replica_id: get("#{0}/@mode".format(replica_id)) for replica_id in replica_ids}
        sleep(1.0)
        for replica_id in replica_ids:
            assert(get("#{0}/@mode".format(replica_id)) == replica_modes[replica_id])

    @authors("aozeritsky")
    def test_replicated_table_tracker_options(self):
        self._create_cells()
        set("//sys/@config/tablet_manager/replicated_table_tracker/bundle_health_cache", {"expire_after_access_time": 1337})
        assert(get("//sys/@config/tablet_manager/replicated_table_tracker/bundle_health_cache/expire_after_access_time") == 1337)

    @authors("babenko")
    def test_cannot_sync_write_into_disabled_replica(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r", replica_id)
        with pytest.raises(YtError): insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}])

    @authors("babenko")
    def test_upstream_replica_id_check1(self):
        self._create_cells()
        self._create_replica_table("//tmp/r", "1-2-3-4")
        with pytest.raises(YtError): insert_rows("//tmp/r", [{"key": 1, "value2": 456}], driver=self.replica_driver)

    @authors("babenko")
    def test_upstream_replica_id_check2(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r", "1-2-3-4")
        sync_enable_table_replica(replica_id)
        with pytest.raises(YtError): insert_rows("//tmp/t", [{"key": 1, "value2": 456}])

    @authors("babenko")
    def test_wait_for_sync_sorted(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", SIMPLE_SCHEMA_SORTED)
        replica_id1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "sync"})
        replica_id2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r1", replica_id1, SIMPLE_SCHEMA_SORTED)
        self._create_replica_table("//tmp/r2", replica_id2, SIMPLE_SCHEMA_SORTED)

        sync_enable_table_replica(replica_id1)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}])

        assert select_rows("* from [//tmp/r1]", driver=self.replica_driver) == [{"key": 1, "value1": "test", "value2": 123}]
        assert select_rows("* from [//tmp/r2]", driver=self.replica_driver) == []

        sync_alter_table_replica_mode(replica_id1, "async")
        sync_alter_table_replica_mode(replica_id2, "sync")

        def check_not_writable():
            try:
                insert_rows("//tmp/t", [{"key": 1, "value1": "test2", "value2": 777}])
                return False
            except YtError:
                return True
        wait(lambda: check_not_writable())

        sync_enable_table_replica(replica_id2)

        def check_writable():
            try:
                insert_rows("//tmp/t", [{"key": 1, "value1": "test2", "value2": 456}])
                return True
            except YtError:
                return False
        wait(lambda: check_writable())

        assert select_rows("* from [//tmp/r2]", driver=self.replica_driver)[-1] == {"key": 1, "value1": "test2", "value2": 456}
        wait(lambda: select_rows("* from [//tmp/r1]", driver=self.replica_driver)[-1] == {"key": 1, "value1": "test2", "value2": 456})

    # XXX(babenko): ordered tables may currently return stale data
    @authors("babenko")
    def test_wait_for_sync_ordered(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", SIMPLE_SCHEMA_ORDERED)
        replica_id1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "sync"})
        replica_id2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r1", replica_id1, SIMPLE_SCHEMA_ORDERED)
        self._create_replica_table("//tmp/r2", replica_id2, SIMPLE_SCHEMA_ORDERED)

        sync_enable_table_replica(replica_id1)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}])

        wait(lambda: select_rows("* from [//tmp/r1]", driver=self.replica_driver) == [{"$tablet_index": 0, "$row_index": 0, "key": 1, "value1": "test", "value2": 123}])
        assert select_rows("* from [//tmp/r2]", driver=self.replica_driver) == []

        sync_alter_table_replica_mode(replica_id1, "async")
        sync_alter_table_replica_mode(replica_id2, "sync")

        def check_not_writable():
            try:
                insert_rows("//tmp/t", [{"key": 1, "value1": "test2", "value2": 777}])
                return False
            except YtError:
                return True
        wait(check_not_writable)

        sync_enable_table_replica(replica_id2)

        def check_writable():
            try:
                insert_rows("//tmp/t", [{"key": 1, "value1": "test2", "value2": 456}])
                return True
            except YtError:
                return False
        wait(check_writable)

        wait(lambda: select_rows("* from [//tmp/r2]", driver=self.replica_driver)[-1] == {"$tablet_index": 0, "$row_index": 1, "key": 1, "value1": "test2", "value2": 456})
        wait(lambda: select_rows("* from [//tmp/r1]", driver=self.replica_driver)[-1] == {"$tablet_index": 0, "$row_index": 1, "key": 1, "value1": "test2", "value2": 456})

    @authors("babenko")
    def test_disable_propagates_replication_row_index(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id)

        sync_enable_table_replica(replica_id)

        assert get("#{0}/@tablets/0/current_replication_row_index".format(replica_id)) == 0

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}], require_sync_replica=False)

        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test", "value2": 123}])

        sync_disable_table_replica(replica_id)

        assert get("#{0}/@tablets/0/current_replication_row_index".format(replica_id)) == 1

    @authors("babenko")
    def test_unmount_propagates_replication_row_index(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id)

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        sync_enable_table_replica(replica_id)

        assert get("#{0}/@tablets/0/current_replication_row_index".format(replica_id)) == 0

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test", "value2": 123}])

        sync_unmount_table("//tmp/t")

        assert get("#{0}/@tablets/0/current_replication_row_index".format(replica_id)) == 1

    @pytest.mark.parametrize("with_data, schema",
         [(False, SIMPLE_SCHEMA_SORTED), (True, SIMPLE_SCHEMA_SORTED), (False, SIMPLE_SCHEMA_ORDERED), (True, SIMPLE_SCHEMA_ORDERED)]
    )
    @authors("babenko", "aozeritsky")
    def test_start_replication_timestamp(self, with_data, schema):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test"}], require_sync_replica=False)

        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={
            "start_replication_timestamp": generate_timestamp()
        })
        self._create_replica_table("//tmp/r", replica_id, schema)

        sync_enable_table_replica(replica_id)

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

    @authors("savrus")
    @pytest.mark.parametrize("schema", [SIMPLE_SCHEMA_SORTED, SIMPLE_SCHEMA_ORDERED])
    def test_start_replication_row_indexes(self, schema):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema)

        rows = [{"key": i, "value1": str(i)} for i in xrange(3)]

        for i in xrange(2):
            insert_rows("//tmp/t", [rows[i]], require_sync_replica=False)
            sync_flush_table("//tmp/t")

        insert_rows("//tmp/t", [rows[2]], require_sync_replica=False)

        for i in xrange(4):
            replica_path = "//tmp/r{0}".format(i)
            replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, replica_path, attributes={
                "start_replication_row_indexes": [i]
            })
            self._create_replica_table(replica_path, replica_id, schema)
            sync_enable_table_replica(replica_id)

        for i in xrange(4):
            wait(lambda: select_rows("key, value1 from [//tmp/r{0}]".format(i), driver=self.replica_driver) == rows[i:])

    @authors("babenko")
    @flaky(max_runs=5)
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_replication_trim(self, mode):
        self._create_cells()
        self._create_replicated_table("//tmp/t", dynamic_store_auto_flush_period=1000, dynamic_store_period_skew=0)

        sync_mount_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r",
            attributes={"mode": mode})
        self._create_replica_table("//tmp/r", replica_id)

        sync_enable_table_replica(replica_id)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test1"}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test1", "value2": YsonEntity()}])
        wait(lambda: get("//tmp/t/@chunk_count") == 1)

        sync_unmount_table("//tmp/t")

        initial_chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(initial_chunk_ids) == 1

        assert get("//tmp/t/@tablets/0/flushed_row_count") == 1
        assert get("//tmp/t/@tablets/0/trimmed_row_count") == 0

        set("//tmp/t/@min_replication_log_ttl", 3000)
        sync_mount_table("//tmp/t")

        sleep(5.0)

        insert_rows("//tmp/t", [{"key": 2, "value1": "test2"}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test1", "value2": YsonEntity()}, {"key": 2, "value1": "test2", "value2": YsonEntity()}])

        def check_chunks():
            chunk_ids = get("//tmp/t/@chunk_ids")
            return len(chunk_ids) == 1 and chunk_ids != initial_chunk_ids
        wait(check_chunks)

        sync_unmount_table("//tmp/t")

        assert get("//tmp/t/@tablets/0/flushed_row_count") == 2
        assert get("//tmp/t/@tablets/0/trimmed_row_count") == 1

    @authors("babenko")
    def test_aggregate_replication(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.AGGREGATE_SCHEMA)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, schema=self.AGGREGATE_SCHEMA)

        sync_enable_table_replica(replica_id)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test1"}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test1", "value2": YsonEntity()}])

        insert_rows("//tmp/t", [{"key": 1, "value1": "test2", "value2": 100}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test2", "value2": 100}])

        insert_rows("//tmp/t", [{"key": 1, "value2": 50}], aggregate=True, update=True, require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test2", "value2": 150}])

    @authors("babenko", "levysotsky", "gridem")
    @pytest.mark.parametrize("only_replica", [True, False])
    @pytest.mark.parametrize("dynamic", [True, False])
    def test_expression_replication(self, dynamic, only_replica):
        self._create_cells()
        replicated_schema = self.EXPRESSIONLESS_SCHEMA if only_replica else self.EXPRESSION_SCHEMA
        self._create_replicated_table("//tmp/t", schema=replicated_schema)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, schema=self.EXPRESSION_SCHEMA)

        if dynamic:
            sync_enable_table_replica(replica_id)
        insert_rows("//tmp/t", [{"key": 1, "value": 2}], require_sync_replica=False)
        if not dynamic:
            sync_unmount_table("//tmp/t")
            sync_mount_table("//tmp/t")
            sync_enable_table_replica(replica_id)

        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"hash": 1, "key": 1, "value": 2}])

        if not dynamic:
            sync_disable_table_replica(replica_id)
        insert_rows("//tmp/t", [{"key": 12, "value": 12}], require_sync_replica=False)
        if not dynamic:
            sync_unmount_table("//tmp/t")
            sync_mount_table("//tmp/t")
            sync_enable_table_replica(replica_id)

        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [
            {"hash": 1, "key": 1, "value": 2},
            {"hash": 2, "key": 12, "value": 12}])

        delete_rows("//tmp/t", [{"key": 1}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"hash": 2, "key": 12, "value": 12}])

    @authors("gridem")
    def test_shard_replication(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.SIMPLE_SCHEMA_SORTED, pivot_keys=[[], [10]])
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, schema=self.SIMPLE_SCHEMA_SORTED)

        sync_enable_table_replica(replica_id)

        insert_rows("//tmp/t", [{"key": 1, "value1": "v", "value2": 2}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "v", "value2": 2}])

        # ensuring that we have correctly populated data here
        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 2
        assert tablets[0]["index"] == 0
        assert tablets[0]["pivot_key"] == []
        assert tablets[1]["index"] == 1
        assert tablets[1]["pivot_key"] == [10]

    @authors("babenko", "gridem")
    def test_reshard_replication(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.SIMPLE_SCHEMA_SORTED, mount=False)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, schema=self.SIMPLE_SCHEMA_SORTED)

        sync_enable_table_replica(replica_id)

        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1

        with pytest.raises(YtError):
            reshard_table("//tmp/t", 3)

        reshard_table("//tmp/t", [[], [10]])

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

        sync_mount_table("//tmp/t")
        sync_unmount_table("//tmp/t")

    @authors("babenko")
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

    @authors("babenko")
    def test_lookup(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.AGGREGATE_SCHEMA)
        replica_id1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "async"})
        replica_id2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2", attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r1", replica_id1, schema=self.AGGREGATE_SCHEMA)
        self._create_replica_table("//tmp/r2", replica_id2, schema=self.AGGREGATE_SCHEMA)

        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        for i in xrange(10):
            insert_rows("//tmp/t", [{"key": i, "value1": "test" + str(i)}])

        for i in xrange(9):
            assert lookup_rows("//tmp/t", [{"key": i}, {"key": i + 1}], column_names=["key", "value1"]) == \
                               [{"key": i, "value1": "test" + str(i)}, {"key": i + 1, "value1": "test" + str(i + 1)}]

        assert lookup_rows("//tmp/t", [{"key": 100000}]) == []

        sync_disable_table_replica(replica_id2)
        sync_alter_table_replica_mode(replica_id2, "async")
        with pytest.raises(YtError): insert_rows("//tmp/t", [{"key": 666, "value1": "hello"}])
        insert_rows("//tmp/t", [{"key": 666, "value1": "hello"}], require_sync_replica=False)

        sync_alter_table_replica_mode(replica_id2, "sync")
        with pytest.raises(YtError): lookup_rows("//tmp/t", [{"key": 666}])

        sync_enable_table_replica(replica_id2)
        def check_catchup():
            try:
                return lookup_rows("//tmp/t", [{"key": 666}], column_names=["key", "value1"]) == [{"key": 666, "value1": "hello"}]
            except:
                return False
        wait(check_catchup)

        sync_alter_table_replica_mode(replica_id2, "async")
        with pytest.raises(YtError): lookup_rows("//tmp/t", [{"key": 666}])

    @authors("babenko")
    def test_select(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.AGGREGATE_SCHEMA)
        replica_id1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "async"})
        replica_id2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2", attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r1", replica_id1, schema=self.AGGREGATE_SCHEMA)
        self._create_replica_table("//tmp/r2", replica_id2, schema=self.AGGREGATE_SCHEMA)

        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        rows = [{"key": i, "value1": "test" + str(i)} for i in xrange(10)]
        insert_rows("//tmp/t", rows)
        assert_items_equal(select_rows("key, value1 from [//tmp/t]"), rows)
        assert_items_equal(select_rows("sum(key) from [//tmp/t] group by 0"), [{"sum(key)": 45}])

        create("table", "//tmp/z", attributes={
            "dynamic": True,
            "schema": self.SIMPLE_SCHEMA_SORTED
        })
        with pytest.raises(YtError): select_rows("* from [//tmp/t] as t1 join [//tmp/z] as t2 on t1.key = t2.key")

        sync_alter_table_replica_mode(replica_id2, "async")
        sleep(1.0)
        with pytest.raises(YtError): select_rows("* from [//tmp/t]")

    @authors("babenko")
    def test_local_sync_replica_yt_7571(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", "primary", "//tmp/r", attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r", replica_id, replica_driver=self.primary_driver)
        sync_enable_table_replica(replica_id)

        rows = [{"key": i, "value1": "test" + str(i)} for i in xrange(10)]
        insert_rows("//tmp/t", rows)

        assert_items_equal(select_rows("key, value1 from [//tmp/t]", driver=self.primary_driver), rows)
        assert_items_equal(select_rows("key, value1 from [//tmp/r]", driver=self.primary_driver), rows)

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_inverted_schema(self, mode):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": mode})
        self._create_replica_table("//tmp/r", replica_id, schema=self.PERTURBED_SCHEMA)
        sync_enable_table_replica(replica_id)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 10}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test", "value2": 10}])

        delete_rows("//tmp/t", [{"key": 1}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [])

    @authors("babenko")
    def test_replica_permissions(self):
        create("map_node", "//tmp/dir")
        self._create_cells()
        self._create_replicated_table("//tmp/dir/t")

        create_user("u")

        set("//tmp/dir/@acl", [make_ace("deny", "u", "write")])
        with pytest.raises(YtError):
            create_table_replica("//tmp/dir/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", authenticated_user="u")

        set("//tmp/dir/@acl", [make_ace("allow", "u", "write")])
        replica_id = create_table_replica("//tmp/dir/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", authenticated_user="u")

        set("//tmp/dir/@acl", [make_ace("deny", "u", "write")])
        with pytest.raises(YtError):
            alter_table_replica(replica_id, enabled=True, authenticated_user="u")

        set("//tmp/dir/@acl", [make_ace("allow", "u", "write")])
        alter_table_replica(replica_id, enabled=True, sauthenticated_user="u")

        set("//tmp/dir/@acl", [make_ace("deny", "u", "write")])
        with pytest.raises(YtError):
            remove("#" + replica_id, authenticated_user="u")

        set("//tmp/dir/@acl", [make_ace("allow", "u", "write")])
        remove("#" + replica_id, authenticated_user="u")

    @authors("ifsmirnov")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_required_columns(self, mode):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.REQUIRED_SCHEMA)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": mode})
        self._create_replica_table("//tmp/r", replica_id, schema=self.REQUIRED_SCHEMA)
        sync_enable_table_replica(replica_id)

        insert_rows("//tmp/t", [{"key1": 1, "key2": 1, "value1": 1, "value2": 1}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == \
            [{"key1": 1, "key2": 1, "value1": 1, "value2": 1}])

        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key2": 1, "value1": 1, "value2": 1}], require_sync_replica=False)
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key1": 1, "key2": 1, "value2": 1}], require_sync_replica=False)

        delete_rows("//tmp/t", [{"key1": 1, "key2": 1}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [])

        with pytest.raises(YtError):
            delete_rows("//tmp/t", [{"key2": 1}], require_sync_replica=False)

    @authors("ifsmirnov")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_required_columns_schema_mismatch_1(self, mode):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.REQUIRED_SCHEMA)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": mode})
        self._create_replica_table("//tmp/r", replica_id, schema=self.REQUIREDLESS_SCHEMA)
        sync_enable_table_replica(replica_id)

        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key2": 1, "value1": 1, "value2": 1}], require_sync_replica=False)
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key1": 2, "key2": 2, "value2": 2}], require_sync_replica=False)
        full_row = {"key1": 3, "key2": 3, "value1": 3, "value2": 3}
        insert_rows("//tmp/t", [full_row], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [full_row])

    @authors("ifsmirnov")
    def test_required_columns_schema_mismatch_2_sync(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.REQUIREDLESS_SCHEMA)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r", replica_id, schema=self.REQUIRED_SCHEMA)
        sync_enable_table_replica(replica_id)

        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key2": 1, "value1": 1, "value2": 1}], require_sync_replica=False)
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key1": 2, "key2": 2, "value2": 2}], require_sync_replica=False)
        full_row = {"key1": 3, "key2": 3, "value1": 3, "value2": 3}
        insert_rows("//tmp/t", [full_row], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [full_row])

    @authors("ifsmirnov")
    @pytest.mark.parametrize("failure_type", ["key", "value"])
    def test_required_columns_schema_mismatch_2_async(self, failure_type):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.REQUIREDLESS_SCHEMA)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r", replica_id, schema=self.REQUIRED_SCHEMA)
        sync_enable_table_replica(replica_id)

        row = {"key1": 2, "key2": 2, "value1": 2, "value2": 2}
        full_row = {"key1": 3, "key2": 3, "value1": 3, "value2": 3}
        if failure_type == "key":
            row.pop("key1")
        else:
            row.pop("value1")

        # Failed to reach the replica due to required.
        insert_rows("//tmp/t", [row], require_sync_replica=False)
        # Should've reached the replica but replication is blocked by previous request.
        insert_rows("//tmp/t", [full_row], require_sync_replica=False)

        assert select_rows("* from [//tmp/r]", driver=self.replica_driver) == []

    @authors("avmatrosov")
    def test_copy(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t_original", schema=self.REQUIREDLESS_SCHEMA)
        replica_id = create_table_replica("//tmp/t_original", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r", schema=self.REQUIRED_SCHEMA)
        sync_enable_table_replica(replica_id)

        sync_freeze_table("//tmp/t_original")
        copy("//tmp/t_original", "//tmp/t_copy")
        sync_unfreeze_table("//tmp/t_original")

        original_replica_info = get("//tmp/t_original/@replicas").values()[0]
        cloned_replica_info = get("//tmp/t_copy/@replicas").values()[0]

        for key in original_replica_info.keys():
            if key != "state":
                assert original_replica_info[key] == cloned_replica_info[key]
        assert cloned_replica_info["state"] == "disabled"

    @authors("avmatrosov")
    def test_replication_via_copied_table(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t_original", schema=self.SIMPLE_SCHEMA_SORTED)
        replica_id = create_table_replica("//tmp/t_original", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": "async", "preserve_timestamps": "false"})
        self._create_replica_table("//tmp/r", schema=self.SIMPLE_SCHEMA_SORTED)

        sync_freeze_table("//tmp/t_original")
        copy("//tmp/t_original", "//tmp/t_copy")
        sync_unfreeze_table("//tmp/t_original")

        sync_mount_table("//tmp/t_copy")

        sync_enable_table_replica(replica_id)
        sync_enable_table_replica(get("//tmp/t_copy/@replicas").keys()[0])

        for table in ["t_original", "t_copy"]:
            rows = [{"key": 1, "value1": table, "value2": 0}]
            insert_rows("//tmp/{}".format(table), rows, update=True, require_sync_replica=False)
            wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == rows)

    @authors("savrus")
    def test_replication_row_indexes(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t_original", schema=self.SIMPLE_SCHEMA_SORTED)
        replica_id = create_table_replica("//tmp/t_original", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": "async", "preserve_timestamps": "false"})
        self._create_replica_table("//tmp/r", schema=self.SIMPLE_SCHEMA_SORTED)
        sync_enable_table_replica(replica_id)

        rows = [{"key": 0, "value1": "0", "value2": 0}]
        insert_rows("//tmp/t_original", rows, require_sync_replica=False)
        wait(lambda: lookup_rows("//tmp/r", [{"key": 0}], driver=self.replica_driver) == rows)

        sync_freeze_table("//tmp/t_original")
        copy("//tmp/t_original", "//tmp/t_copy")
        sync_unfreeze_table("//tmp/t_original")

        copy_id = get("//tmp/t_copy/@replicas").keys()[0]
        original_row_index = get("#{}/@tablets/0/current_replication_row_index".format(replica_id))
        copy_row_index = get("#{}/@tablets/0/current_replication_row_index".format(copy_id))

        sync_enable_table_replica(get("//tmp/t_copy/@replicas").keys()[0])

        delete_rows("//tmp/t_original", [{"key": 0}], require_sync_replica=False)
        wait(lambda: lookup_rows("//tmp/r", [{"key": 0}], driver=self.replica_driver) == [])

        sync_mount_table("//tmp/t_copy")
        rows = [{"key": 1, "value1": "1", "value2": 1}]
        insert_rows("//tmp/t_copy", rows, require_sync_replica=False)
        wait(lambda: lookup_rows("//tmp/r", [{"key": 1}], driver=self.replica_driver) == rows)

        assert select_rows("* from [//tmp/r]", driver=self.replica_driver) == rows

    @authors("akozhikhov")
    def test_get_tablet_infos(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.AGGREGATE_SCHEMA)
        replica_id1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "async"})
        replica_id2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2", attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r1", replica_id1, schema=self.AGGREGATE_SCHEMA)
        self._create_replica_table("//tmp/r2", replica_id2, schema=self.AGGREGATE_SCHEMA)

        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        for i in xrange(3):
            insert_rows("//tmp/t", [{"key": i, "value1": "test" + str(i)}])

        for i in xrange(2):
            assert lookup_rows("//tmp/t", [{"key": i}, {"key": i + 1}], column_names=["key", "value1"]) == \
                   [{"key": i, "value1": "test" + str(i)}, {"key": i + 1, "value1": "test" + str(i + 1)}]

        tablet_info = {}
        def _check():
            tablet_infos = get_tablet_infos("//tmp/t", [0])
            assert tablet_infos.keys() == ["tablets"] and len(tablet_infos["tablets"]) == 1
            tablet_info.update(tablet_infos["tablets"][0])

            return tablet_info["total_row_count"] == 3
        wait(lambda: _check())

        assert tablet_info["trimmed_row_count"] == 0
        barrier_timestamp = tablet_info["barrier_timestamp"]
        assert barrier_timestamp >= tablet_info["last_write_timestamp"]

        sync_replica = None
        async_replica = None
        assert len(tablet_info["replica_infos"]) == 2
        for replica in tablet_info["replica_infos"]:
            assert replica["last_replication_timestamp"] <= barrier_timestamp
            if replica["mode"] == "sync":
                sync_replica = replica
            elif replica["mode"] == "async":
                async_replica = replica

        assert sync_replica["replica_id"] == replica_id2
        assert sync_replica["current_replication_row_index"] == 3

        assert async_replica["replica_id"] == replica_id1
        assert async_replica["current_replication_row_index"] <= 3

##################################################################

class TestReplicatedDynamicTablesSafeMode(TestReplicatedDynamicTablesBase):
    USE_PERMISSION_CACHE = False

    DELTA_NODE_CONFIG = {
        "master_cache_service": {
            "capacity": 0
        }
    }

    @authors("ifsmirnov")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_safe_mode(self, mode):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": mode})
        self._create_replica_table("//tmp/r", replica_id)
        sync_enable_table_replica(replica_id)
        create_user("u")
        create_user("u", driver=self.replica_driver)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 10}], require_sync_replica=False, authenticated_user="u")
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test", "value2": 10}])

        set("//sys/@config/enable_safe_mode", True, driver=self.replica_driver)

        if mode == "sync":
            with pytest.raises(YtError):
                insert_rows("//tmp/t", [{"key": 2, "value1": "test", "value2": 10}], authenticated_user="u")
        else:
            insert_rows("//tmp/t", [{"key": 2, "value1": "test", "value2": 10}], require_sync_replica=False)
            sleep(1)
            assert select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value1": "test", "value2": 10}]
            wait(lambda: get("//tmp/t/@replicas/{}/error_count".format(replica_id)) == 1)

            tablet_id = get("//tmp/t/@tablets/0/tablet_id")
            address = get_tablet_leader_address(tablet_id)
            orchid = self._find_tablet_orchid(address, tablet_id)
            errors = orchid["replication_errors"]
            assert len(errors) == 1
            assert YtResponseError(errors.values()[0]).is_access_denied()

        set("//sys/@config/enable_safe_mode", False, driver=self.replica_driver)

        if mode == "sync":
            insert_rows("//tmp/t", [{"key": 2, "value1": "test", "value2": 10}], authenticated_user="u")

        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [
            {"key": 1, "value1": "test", "value2": 10},
            {"key": 2, "value1": "test", "value2": 10}])

        wait(lambda: get("//tmp/t/@replicas/{}/error_count".format(replica_id)) == 0)

##################################################################

class TestReplicatedDynamicTablesMulticell(TestReplicatedDynamicTables):
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

##################################################################

class TestReplicatedDynamicTablesRpcProxy(TestReplicatedDynamicTables):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

