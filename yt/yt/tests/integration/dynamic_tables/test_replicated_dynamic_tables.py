from yt_dynamic_tables_base import DynamicTablesBase

from yt_env_setup import parametrize_external

from yt_commands import (
    authors, create_table_collocation, wait, WaitFailed, create, ls, get, set, copy, move,
    remove, exists,
    create_user, create_table_replica,
    make_ace, start_transaction, abort_transaction, commit_transaction, lock, insert_rows, select_rows, lookup_rows,
    delete_rows, lock_rows,
    reshard_table, generate_timestamp, get_tablet_infos, get_tablet_leader_address, sync_create_cells,
    sync_mount_table, sync_unmount_table, sync_freeze_table, alter_table, get_tablet_errors,
    sync_unfreeze_table, sync_flush_table, sync_enable_table_replica, sync_disable_table_replica,
    remove_table_replica, alter_table_replica, get_in_sync_replicas, sync_alter_table_replica_mode,
    get_driver, SyncLastCommittedTimestamp, raises_yt_error, get_singular_chunk_id,
    set_node_banned)

from yt.test_helpers import are_items_equal, assert_items_equal
import yt_error_codes
from yt.wrapper import YtError, YtResponseError
import yt.yson as yson

from flaky import flaky
import pytest

from time import sleep
from copy import deepcopy

import builtins
import random

##################################################################

SIMPLE_SCHEMA_SORTED = [
    {"name": "key", "type": "int64", "sort_order": "ascending"},
    {"name": "value1", "type": "string"},
    {"name": "value2", "type": "int64"},
]

SIMPLE_SCHEMA_ORDERED = [
    {"name": "key", "type": "int64"},
    {"name": "value1", "type": "string"},
    {"name": "value2", "type": "int64"},
]

PERTURBED_SCHEMA = [
    {"name": "key", "type": "int64", "sort_order": "ascending"},
    {"name": "value2", "type": "int64"},
    {"name": "value1", "type": "string"},
]

MULTILOCK_SCHEMA_SORTED = [
    {"name": "key", "type": "int64", "sort_order": "ascending"},
    {"name": "value1", "type": "string", "lock": "a"},
    {"name": "value2", "type": "int64", "lock": "b"},
]

AGGREGATE_SCHEMA = [
    {"name": "key", "type": "int64", "sort_order": "ascending"},
    {"name": "value1", "type": "string"},
    {"name": "value2", "type": "int64", "aggregate": "sum"},
]

REQUIRED_SCHEMA = [
    {"name": "key1", "type": "int64", "sort_order": "ascending", "required": True},
    {"name": "key2", "type": "int64", "sort_order": "ascending"},
    {"name": "value1", "type": "int64", "required": True},
    {"name": "value2", "type": "int64"},
]
REQUIREDLESS_SCHEMA = [
    {"name": "key1", "type": "int64", "sort_order": "ascending"},
    {"name": "key2", "type": "int64", "sort_order": "ascending"},
    {"name": "value1", "type": "int64"},
    {"name": "value2", "type": "int64"},
]

EXPRESSION_SCHEMA = [
    {
        "name": "hash",
        "type": "int64",
        "sort_order": "ascending",
        "expression": "key % 10",
    },
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
    NUM_TEST_PARTITIONS = 8
    NUM_REMOTE_CLUSTERS = 1

    ENABLE_STANDALONE_REPLICATED_TABLE_TRACKER = True
    NUM_REPLICATED_TABLE_TRACKERS = 2

    DELTA_NODE_CONFIG = {
        "cluster_connection": {
            # Disable cache
            "table_mount_cache": {
                "expire_after_successful_update_time": 0,
                "expire_after_failed_update_time": 0,
                "expire_after_access_time": 0,
                "refresh_time": 0,
            },
            "timestamp_provider": {
                "update_period": 500,
            },
        },
        "tablet_node": {
            "replicator_data_weight_throttling_granularity": 1
        },
        "tablet_node_hint_manager": {
            "replicator_hint_config_fetcher": {
                "update_period": 100
            }
        }
    }

    def setup_method(self, method):
        super(TestReplicatedDynamicTablesBase, self).setup_method(method)
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

        # COMPAT(kvk1920): Drop this after maintenance request API changed.
        for driver in (self.replica_driver, self.primary_driver):
            set("//sys/@config/node_tracker/forbid_maintenance_attribute_writes", False, driver=driver)
            set("//sys/@config/node_tracker/forbid_maintenance_attribute_writes", False, driver=driver)

    def teardown_method(self, method):
        self.replica_driver = None
        self.primary_driver = None
        super(TestReplicatedDynamicTablesBase, self).teardown_method(method)

    def _get_table_attributes(self, schema):
        return {"dynamic": True, "schema": schema}

    def _create_replicated_table(self, path, schema=SIMPLE_SCHEMA_SORTED, mount=True, **attributes):
        attributes.update(self._get_table_attributes(schema))
        attributes["enable_replication_logging"] = True
        attributes.setdefault("enable_dynamic_store_read", True)
        id = create("replicated_table", path, attributes=attributes)
        if mount:
            sync_mount_table(path)
        return id

    def _create_replica_table(
        self, path, replica_id=None, schema=SIMPLE_SCHEMA_SORTED, mount=True, replica_driver=None, **kwargs
    ):
        if not replica_driver:
            replica_driver = self.replica_driver
        attributes = self._get_table_attributes(schema)
        if replica_id:
            attributes["upstream_replica_id"] = replica_id
        attributes.update(kwargs)
        attributes.setdefault("enable_dynamic_store_read", True)
        create("table", path, attributes=attributes, driver=replica_driver)
        if mount:
            sync_mount_table(path, driver=replica_driver)

    def _create_cells(self, cell_count=1):
        primary_cells = sync_create_cells(cell_count)
        replica_cells = sync_create_cells(cell_count, driver=self.replica_driver)
        return [primary_cells, replica_cells]

    def _check_replication_is_banned(self, rows, dummy, replica_driver):
        rows[0]["value2"] = dummy["counter"]
        dummy["counter"] += 1
        insert_rows("//tmp/t", rows, require_sync_replica=False)
        try:
            wait(
                lambda: lookup_rows("//tmp/r", [{"key": 0}], driver=replica_driver) == rows,
                iter=5,
                sleep_backoff=1)
            return False
        except WaitFailed:
            return True

    def _create_hunk_storage(self, name):
        create("hunk_storage", name, attributes={
            "store_rotation_period": 10000,
            "store_removal_grace_period": 10000,
        })

    def _get_hunk_table_schema(self, schema, max_inline_hunk_size):
        new_schema = deepcopy(schema)
        new_schema[1]["max_inline_hunk_size"] = max_inline_hunk_size
        return new_schema

    def _get_active_store_id(self, hunk_storage, tablet_index=0):
        tablets = get("{}/@tablets".format(hunk_storage))
        tablet_id = tablets[tablet_index]["tablet_id"]
        wait(lambda: exists("//sys/tablets/{}/orchid/active_store_id".format(tablet_id)))
        return get("//sys/tablets/{}/orchid/active_store_id".format(tablet_id))

    def _get_store_chunk_ids(self, path):
        chunk_ids = get(path + "/@chunk_ids")
        return [chunk_id for chunk_id in chunk_ids if get("#{}/@chunk_type".format(chunk_id)) == "table"]

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

    @authors("babenko")
    @pytest.mark.parametrize("schema", [SIMPLE_SCHEMA_SORTED, SIMPLE_SCHEMA_ORDERED])
    def test_simple(self, schema):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test"}], require_sync_replica=False)
        if schema is self.SIMPLE_SCHEMA_SORTED:
            delete_rows("//tmp/t", [{"key": 2}], require_sync_replica=False)

    @authors("babenko", "gridem", "alexelexa")
    @pytest.mark.parametrize("schema", [SIMPLE_SCHEMA_SORTED, SIMPLE_SCHEMA_ORDERED])
    def test_replication_error(self, schema):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema)
        set("//tmp/t/@chunk_writer", {"block_size": 1})

        tablet_count = 3
        singular_row = [{"key": 99, "value1": "test1"}]
        rows = [{"key": i * 40, "value1": str(i)} for i in range(tablet_count)]

        sync_unmount_table("//tmp/t")
        if schema == SIMPLE_SCHEMA_SORTED:
            reshard_table("//tmp/t", [[], [33], [66]])
        else:
            reshard_table("//tmp/t", tablet_count)

            singular_row[0]["$tablet_index"] = 2
            for i in range(tablet_count):
                rows[i]["$tablet_index"] = i

        sync_mount_table("//tmp/t")

        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, schema, mount=False)
        sync_enable_table_replica(replica_id)

        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == tablet_count
        tablet_ids = [tablets[i]["tablet_id"] for i in range(tablet_count)]

        def check_error(tablet_id, expected_error_count=0, expected_error_message=None):
            error_count = get("//tmp/t/@replicas/{}/error_count".format(replica_id))
            if error_count != get("#{}/@error_count".format(replica_id)):
                return False
            if (error_count != sum(int(get("#{}/@tablets/{}/has_error".format(replica_id, tablet_index)))
                                   for tablet_index in range(tablet_count))):
                return False

            errors = get("//sys/tablets/{}/orchid/replication_errors".format(tablet_id))
            if expected_error_message is None:
                return error_count == expected_error_count and len(errors) == 0
            else:
                return error_count == expected_error_count and len(errors) == 1 and YtError.from_dict(errors[replica_id]).contains_text(expected_error_message)

        def check_tablet_errors(tablet_ids, expected_error_message=None, limit=None):
            errors = get_tablet_errors("//tmp/t") if limit is None else get_tablet_errors("//tmp/t", limit=limit)
            if len(errors["replication_errors"]) != int(expected_error_message is not None):
                return False

            error_count = get("//tmp/t/@replicas/{}/error_count".format(replica_id))
            expected_error_count = error_count if limit is None else min(error_count, limit)
            replica_errors = errors["replication_errors"].get(replica_id, [])
            if len(replica_errors) != expected_error_count:
                return False

            actual_tablet_ids = builtins.set(error["attributes"]["tablet_id"] for error in replica_errors)
            tablet_with_error_count = min(limit if limit is not None else len(tablet_ids), error_count)
            if len(actual_tablet_ids) != tablet_with_error_count:
                return False

            limit = limit if limit is not None else 5
            if (limit < len(tablet_ids)) != errors.get("incomplete", False):
                return False

            for error in replica_errors:
                if not YtError.from_dict(error).contains_text(expected_error_message):
                    return False

                if error["attributes"]["tablet_id"] not in tablet_ids:
                    return False
            return True

        assert all(check_error(tablet_id) for tablet_id in tablet_ids)

        insert_rows("//tmp/t", singular_row, require_sync_replica=False)
        wait(lambda: check_error(tablet_ids[2], 1, expected_error_message="Table //tmp/r has no mounted tablets"))

        sync_mount_table("//tmp/r", driver=self.replica_driver)
        wait(lambda: all(check_error(tablet_id) for tablet_id in tablet_ids))

        sync_unmount_table("//tmp/r", driver=self.replica_driver)
        insert_rows("//tmp/t", rows, require_sync_replica=False)
        wait(lambda: all(check_error(tablet_id, tablet_count, expected_error_message="Table //tmp/r has no mounted tablets") for tablet_id in tablet_ids))

        assert check_tablet_errors(tablet_ids, expected_error_message="Table //tmp/r has no mounted tablets")
        assert check_tablet_errors(tablet_ids, expected_error_message="Table //tmp/r has no mounted tablets", limit=2)

        sync_mount_table("//tmp/r", driver=self.replica_driver)
        wait(lambda: all(check_error(tablet_id) for tablet_id in tablet_ids))
        assert check_tablet_errors(tablet_ids)

    @authors("gridem")
    def test_replicated_in_memory_fail(self):
        self._create_cells()
        with pytest.raises(YtError):
            self._create_replicated_table("//tmp/t", in_memory_mode="compressed")
        with pytest.raises(YtError):
            self._create_replicated_table("//tmp/t", in_memory_mode="uncompressed")

    @authors("babenko")
    def test_add_replica_fail1(self):
        with pytest.raises(YtError):
            create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")

    @authors("babenko")
    def test_add_replica_fail2(self):
        create("table", "//tmp/t")
        with pytest.raises(YtError):
            create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")

    @authors("babenko")
    def test_add_replica_fail3(self):
        self._create_replicated_table("//tmp/t", mount=False)
        create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        with pytest.raises(YtError):
            create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")

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

    @authors("ponasenko-rs")
    @pytest.mark.parametrize("replica_ordering", ["sorted", "ordered"])
    def test_incompatible_orderings(self, replica_ordering):
        self._create_cells()
        if replica_ordering == "sorted":
            replicated_table_schema, replica_schema = self.SIMPLE_SCHEMA_ORDERED, self.SIMPLE_SCHEMA_SORTED
        else:
            replicated_table_schema, replica_schema = self.SIMPLE_SCHEMA_SORTED, self.SIMPLE_SCHEMA_ORDERED

        self._create_replicated_table("//tmp/t", schema=replicated_table_schema)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r", replica_id, schema=replica_schema)

        sync_enable_table_replica(replica_id)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}], require_sync_replica=False)

        def _check():
            tablet_id = get("//tmp/t/@tablets/0/tablet_id")
            orchid = self._find_tablet_orchid(get_tablet_leader_address(tablet_id), tablet_id)
            errors = orchid["replication_errors"]

            if len(errors) == 0:
                return False

            message = list(errors.values())[0]["message"]
            return message.startswith("Replicated table and replica table should be either both sorted or both ordered")

        wait(_check)

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

        timestamp1 = generate_timestamp()

        assert_items_equal(get_in_sync_replicas("//tmp/t", [], timestamp=timestamp1),
                           [replica_id1, replica_id2])
        wait(
            lambda: get_in_sync_replicas("//tmp/t", keys, timestamp=timestamp1)
            == [replica_id1])
        assert get_in_sync_replicas("//tmp/t", None, timestamp=timestamp1) == [replica_id1]

        insert_rows("//tmp/t", rows, require_sync_replica=False)

        timestamp2 = generate_timestamp()

        wait(lambda: get_in_sync_replicas("//tmp/t", keys, timestamp=timestamp2) == [replica_id1])
        assert_items_equal(get_in_sync_replicas("//tmp/t", None, timestamp=timestamp2),
                           [replica_id1])
        assert_items_equal(get_in_sync_replicas("//tmp/t", [], timestamp=timestamp2),
                           [replica_id1, replica_id2])

        sync_enable_table_replica(replica_id2)

        wait(
            lambda: are_items_equal(get_in_sync_replicas("//tmp/t", keys, timestamp=timestamp2),
                                    [replica_id1, replica_id2])
        )
        assert_items_equal(get_in_sync_replicas("//tmp/t", None, timestamp=timestamp2),
                           [replica_id1, replica_id2])
        assert_items_equal(get_in_sync_replicas("//tmp/t", [], timestamp=timestamp2),
                           [replica_id1, replica_id2])

    @authors("babenko")
    def test_in_sync_replicas_with_sync_last_committed_timestamp(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")

        replica_id1 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r1",
            attributes={"mode": "sync"},
        )
        replica_id2 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r2",
            attributes={"mode": "async"},
        )

        self._create_replica_table("//tmp/r1", replica_id1)
        self._create_replica_table("//tmp/r2", replica_id2)

        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        timestamp = generate_timestamp()
        wait(
            lambda: are_items_equal(get_in_sync_replicas("//tmp/t", None, timestamp=timestamp),
                                    [replica_id1, replica_id2])
        )

        assert_items_equal(get_in_sync_replicas("//tmp/t", None, timestamp=SyncLastCommittedTimestamp),
                           [replica_id1])

    @authors("babenko")
    @pytest.mark.parametrize("schema", [SIMPLE_SCHEMA_SORTED, MULTILOCK_SCHEMA_SORTED])
    def test_async_replication_sorted(self, schema):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=schema)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, schema=schema)

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

        insert_rows(
            "//tmp/t",
            [{"key": 1, "value1": "new_test"}],
            update=True,
            require_sync_replica=False,
        )
        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)
            == [{"key": 1, "value1": "new_test", "value2": 123}]
        )

        sync_flush_table("//tmp/r", driver=self.replica_driver)

        assert select_rows("* from [//tmp/r]", driver=self.replica_driver) \
            == [{"key": 1, "value1": "new_test", "value2": 123}]

        insert_rows(
            "//tmp/t",
            [{"key": 1, "value2": 456}],
            update=True,
            require_sync_replica=False,
        )
        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)
            == [{"key": 1, "value1": "new_test", "value2": 456}]
        )

        delete_rows("//tmp/t", [{"key": 1}], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [])

    @authors("akozhikhov", "aleksandra-zh")
    @pytest.mark.parametrize("preserve_tablet_index", [False, True])
    @pytest.mark.parametrize("use_hunks", [False, True])
    def test_async_replication_ordered(self, preserve_tablet_index, use_hunks):
        self._create_cells()
        schema = self._get_hunk_table_schema(self.SIMPLE_SCHEMA_ORDERED, 1)
        self._create_replicated_table(
            "//tmp/t",
            schema,
            preserve_tablet_index=preserve_tablet_index,
            min_replication_log_ttl=30000
        )

        if use_hunks:
            self._create_hunk_storage("//tmp/h")
            set("//tmp/t/@hunk_storage_node", "//tmp/h")
            sync_mount_table("//tmp/h")

        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, self.SIMPLE_SCHEMA_ORDERED)

        sync_enable_table_replica(replica_id)

        insert_rows(
            "//tmp/t",
            [{"key": 1, "value1": "test", "value2": 123}],
            require_sync_replica=False,
        )

        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)
            == [
                {
                    "$tablet_index": 0,
                    "$row_index": 0,
                    "key": 1,
                    "value1": "test",
                    "value2": 123,
                }
            ]
        )

        insert_rows("//tmp/t", [{"key": 1, "value1": "new_test"}], require_sync_replica=False)
        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)[-1]
            == {
                "$tablet_index": 0,
                "$row_index": 1,
                "key": 1,
                "value1": "new_test",
                "value2": yson.YsonEntity(),
            }
        )

        insert_rows("//tmp/t", [{"key": 1, "value2": 456}], require_sync_replica=False)
        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)[-1]
            == {
                "$tablet_index": 0,
                "$row_index": 2,
                "key": 1,
                "value1": yson.YsonEntity(),
                "value2": 456,
            }
        )

        # TODO(aleksandra-zh, gritukan): fix multicell
        if use_hunks and not self.is_multicell():
            set("//sys/cluster_nodes/@config", {"%true": {
                "tablet_node": {"hunk_lock_manager": {"hunk_store_extra_lifetime": 123, "unlock_check_period": 127}}
            }})

            sync_unmount_table("//tmp/t")
            sync_mount_table("//tmp/t")

            store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
            assert len(store_chunk_ids) == 1

            hunk_chunk_refs = get("#{}/@hunk_chunk_refs".format(store_chunk_ids[0]))
            assert len(hunk_chunk_refs) == 1
            assert hunk_chunk_refs[0]["hunk_count"] == 2

            sync_unmount_table("//tmp/t")
            sync_unmount_table("//tmp/h")

    @authors("aozeritsky")
    @pytest.mark.parametrize("mode", ["async", "sync"])
    def test_replication_preserve_tablet_index(self, mode):
        self._create_cells()
        self._create_replicated_table(
            "//tmp/t",
            schema=self.SIMPLE_SCHEMA_ORDERED,
            tablet_count=2,
            preserve_tablet_index=True,
        )
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": mode})
        self._create_replica_table("//tmp/r", replica_id, self.SIMPLE_SCHEMA_ORDERED, tablet_count=2)

        sync_enable_table_replica(replica_id)

        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 2

        tablets = get("//tmp/r/@tablets", driver=self.replica_driver)
        assert len(tablets) == 2

        insert_rows(
            "//tmp/t",
            [{"$tablet_index": 0, "key": 1, "value1": "test1", "value2": 123}],
            require_sync_replica=False,
        )
        insert_rows(
            "//tmp/t",
            [{"$tablet_index": 1, "key": 2, "value1": "test2", "value2": 124}],
            require_sync_replica=False,
        )

        wait(
            lambda: select_rows("* from [//tmp/r] where [$tablet_index]=0", driver=self.replica_driver)
            == [
                {
                    "$tablet_index": 0,
                    "$row_index": 0,
                    "key": 1,
                    "value1": "test1",
                    "value2": 123,
                }
            ]
        )
        wait(
            lambda: select_rows("* from [//tmp/r] where [$tablet_index]=1", driver=self.replica_driver)
            == [
                {
                    "$tablet_index": 1,
                    "$row_index": 0,
                    "key": 2,
                    "value1": "test2",
                    "value2": 124,
                }
            ]
        )

    @authors("aozeritsky")
    def test_replication_sorted_with_tablet_index(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id)

        sync_enable_table_replica(replica_id)

        with pytest.raises(YtError):
            insert_rows(
                "//tmp/t",
                [{"$tablet_index": 1, "key": 1, "value1": "test", "value2": 123}],
                require_sync_replica=False,
            )

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
                    "//tmp/t",
                    [{"key": 1, "value1": self.str100, "value2": self.counter}],
                    require_sync_replica=False,
                )

            def get_inserted_counter(self):
                rows = select_rows("* from [//tmp/r]", driver=self.replica_driver)
                if len(rows) == 0:
                    return 0
                assert len(rows) == 1
                return rows[0]["value2"]

        self._create_cells()
        self._create_replicated_table(
            "//tmp/t",
            replication_throttler={"limit": 500},
            max_data_weight_per_replication_commit=500,
        )
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id)

        inserter = Inserter(self.replica_driver)
        for _ in range(50):
            inserter.insert()

        sync_enable_table_replica(replica_id)

        counter_start = inserter.get_inserted_counter()
        assert counter_start <= 7
        for i in range(20):
            sleep(1.0)
            inserted = inserter.get_inserted_counter()
            counter = (inserted - counter_start) // 5
            assert counter - 3 <= i <= counter + 3
            if inserted == inserter.counter:
                break

    @authors("babenko")
    def test_sync_replication_sorted(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id1 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r1",
            attributes={"mode": "sync"},
        )
        replica_id2 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r2",
            attributes={"mode": "async"},
        )
        self._create_replica_table("//tmp/r1", replica_id1)
        self._create_replica_table("//tmp/r2", replica_id2)
        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1

        def _do():
            before_index1 = get("#{0}/@tablets/0/committed_replication_row_index".format(replica_id1))
            before_index2 = get("#{0}/@tablets/0/committed_replication_row_index".format(replica_id2))
            assert before_index1 == before_index2

            before_ts1 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id1))
            before_ts2 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id2))
            assert before_ts1 == before_ts2

            insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}])
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver) == [
                {"key": 1, "value1": "test", "value2": 123}
            ]
            wait(
                lambda: select_rows("* from [//tmp/r2]", driver=self.replica_driver)
                == [{"key": 1, "value1": "test", "value2": 123}]
            )

            insert_rows("//tmp/t", [{"key": 1, "value1": "new_test"}], update=True)
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver) == [
                {"key": 1, "value1": "new_test", "value2": 123}
            ]
            wait(
                lambda: select_rows("* from [//tmp/r2]", driver=self.replica_driver)
                == [{"key": 1, "value1": "new_test", "value2": 123}]
            )

            insert_rows("//tmp/t", [{"key": 1, "value2": 456}], update=True)
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver) == [
                {"key": 1, "value1": "new_test", "value2": 456}
            ]
            wait(
                lambda: select_rows("* from [//tmp/r2]", driver=self.replica_driver)
                == [{"key": 1, "value1": "new_test", "value2": 456}]
            )

            delete_rows("//tmp/t", [{"key": 1}])
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver) == []
            wait(lambda: select_rows("* from [//tmp/r2]", driver=self.replica_driver) == [])

            wait(lambda: get("#{0}/@tablets/0/committed_replication_row_index".format(replica_id1)) == before_index1 + 4)
            wait(lambda: get("#{0}/@tablets/0/committed_replication_row_index".format(replica_id2)) == before_index1 + 4)

            after_ts1 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id1))
            after_ts2 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id2))
            assert after_ts1 == after_ts2
            assert after_ts1 > before_ts1

        _do()

        sync_alter_table_replica_mode(replica_id1, "async")
        sync_alter_table_replica_mode(replica_id1, "sync")

        _do()

    @authors("lukyan")
    def test_transaction_locks(self):
        self._create_cells()

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "a", "type": "int64", "lock": "a"},
            {"name": "b", "type": "int64", "lock": "b"},
            {"name": "c", "type": "int64", "lock": "c"}]

        self._create_replicated_table("//tmp/t", schema=schema)

        replica_id1 = create_table_replica(
            "//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r1", replica_id1, schema=schema)
        sync_enable_table_replica(replica_id1)

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")

        insert_rows("//tmp/t", [{"key": 1, "a": 1}], update=True, tx=tx1)
        lock_rows("//tmp/t", [{"key": 1}], locks=["a", "c"], tx=tx1, lock_type="shared_weak")
        insert_rows("//tmp/t", [{"key": 1, "b": 2}], update=True, tx=tx2)

        commit_transaction(tx1)
        commit_transaction(tx2)

        assert lookup_rows("//tmp/t", [{"key": 1}], column_names=["key", "a", "b"]) == [{"key": 1, "a": 1, "b": 2}]

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")
        tx3 = start_transaction(type="tablet")

        insert_rows("//tmp/t", [{"key": 2, "a": 1}], update=True, tx=tx1)
        lock_rows("//tmp/t", [{"key": 2}], locks=["a", "c"], tx=tx1, lock_type="shared_weak")

        insert_rows("//tmp/t", [{"key": 2, "b": 2}], update=True, tx=tx2)
        lock_rows("//tmp/t", [{"key": 2}], locks=["c"], tx=tx2, lock_type="shared_weak")

        lock_rows("//tmp/t", [{"key": 2}], locks=["a"], tx=tx3, lock_type="shared_weak")

        commit_transaction(tx1)
        commit_transaction(tx2)

        with pytest.raises(YtError):
            commit_transaction(tx3)

        assert lookup_rows("//tmp/t", [{"key": 2}], column_names=["key", "a", "b"]) == [{"key": 2, "a": 1, "b": 2}]

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")

        lock_rows("//tmp/t", [{"key": 3}], locks=["a"], tx=tx1, lock_type="shared_weak")
        insert_rows("//tmp/t", [{"key": 3, "a": 1}], update=True, tx=tx2)

        commit_transaction(tx2)

        with pytest.raises(YtError):
            commit_transaction(tx1)

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")

        lock_rows("//tmp/t", [{"key": 3}], locks=["a"], tx=tx1, lock_type="shared_strong")
        insert_rows("//tmp/t", [{"key": 3, "a": 1}], update=True, tx=tx2)

        commit_transaction(tx1)

        with pytest.raises(YtError):
            commit_transaction(tx2)

    @authors("ponasenko-rs")
    def test_lock_after_delete(self):
        self._create_cells()

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "a", "type": "int64", "lock": "lock_a"}
        ]

        self._create_replicated_table("//tmp/t", schema=schema)

        replica_id1 = create_table_replica(
            "//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "sync"})
        replica_id2 = create_table_replica(
            "//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2", attributes={"mode": "async"})

        self._create_replica_table("//tmp/r1", replica_id1, schema=schema)
        sync_enable_table_replica(replica_id1)
        self._create_replica_table("//tmp/r2", replica_id2, schema=schema)
        sync_enable_table_replica(replica_id2)

        insert_rows("//tmp/t", [{"key": 1, "a": 1}], update=True)

        expected = [{"key": 1, "a": 1}]

        assert lookup_rows("//tmp/t", [{"key": 1}]) == expected
        assert lookup_rows("//tmp/r1", [{"key": 1}], driver=self.replica_driver) == expected
        wait(lambda: lookup_rows("//tmp/r2", [{"key": 1}], driver=self.replica_driver) == expected)

        tx = start_transaction(type="tablet")

        delete_rows("//tmp/t", [{"key": 1}], tx=tx)
        lock_rows("//tmp/t", [{"key": 1}], locks=["lock_a"], tx=tx, lock_type="shared_strong")

        commit_transaction(tx)

        assert lookup_rows("//tmp/t", [{"key": 1}]) == []
        assert lookup_rows("//tmp/r1", [{"key": 1}], driver=self.replica_driver) == []
        wait(lambda: lookup_rows("//tmp/r2", [{"key": 1}], driver=self.replica_driver) == [])

    @authors("akozhikhov", "aleksandra-zh")
    @pytest.mark.parametrize("commit_ordering", ["weak", "strong"])
    @pytest.mark.parametrize("use_hunks", [False, True])
    def test_sync_replication_ordered(self, commit_ordering, use_hunks):
        self._create_cells()
        self._create_replicated_table("//tmp/t", self.SIMPLE_SCHEMA_ORDERED)

        if use_hunks:
            self._create_hunk_storage("//tmp/h")
            set("//tmp/t/@hunk_storage_node", "//tmp/h")
            sync_mount_table("//tmp/h")

        replica_id1 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r1",
            attributes={"mode": "sync"},
        )
        replica_id2 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r2",
            attributes={"mode": "async"},
        )
        self._create_replica_table(
            "//tmp/r1",
            replica_id1,
            self.SIMPLE_SCHEMA_ORDERED,
            commit_ordering=commit_ordering,
        )
        self._create_replica_table(
            "//tmp/r2",
            replica_id2,
            self.SIMPLE_SCHEMA_ORDERED,
            commit_ordering=commit_ordering,
        )
        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1

        def _last_row(result):
            if len(result) == 0:
                return {}
            else:
                return result[-1]

        def _do():
            before_index1 = get("#{0}/@tablets/0/committed_replication_row_index".format(replica_id1))
            before_index2 = get("#{0}/@tablets/0/committed_replication_row_index".format(replica_id2))
            assert before_index1 == before_index2

            before_ts1 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id1))
            before_ts2 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id2))
            assert before_ts1 == before_ts2

            insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}])
            assert _last_row(select_rows("* from [//tmp/r1]", driver=self.replica_driver)) == {
                "$tablet_index": 0,
                "$row_index": before_index1,
                "key": 1,
                "value1": "test",
                "value2": 123,
            }
            wait(
                lambda: _last_row(select_rows("* from [//tmp/r2]", driver=self.replica_driver))
                == {
                    "$tablet_index": 0,
                    "$row_index": before_index2,
                    "key": 1,
                    "value1": "test",
                    "value2": 123,
                }
            )

            insert_rows("//tmp/t", [{"key": 1, "value1": "new_test"}])
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver)[-1] == {
                "$tablet_index": 0,
                "$row_index": before_index1 + 1,
                "key": 1,
                "value1": "new_test",
                "value2": yson.YsonEntity(),
            }
            wait(
                lambda: select_rows("* from [//tmp/r2]", driver=self.replica_driver)[-1]
                == {
                    "$tablet_index": 0,
                    "$row_index": before_index2 + 1,
                    "key": 1,
                    "value1": "new_test",
                    "value2": yson.YsonEntity(),
                }
            )

            insert_rows("//tmp/t", [{"key": 1, "value2": 456}])
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver)[-1] == {
                "$tablet_index": 0,
                "$row_index": before_index1 + 2,
                "key": 1,
                "value1": yson.YsonEntity(),
                "value2": 456,
            }
            wait(
                lambda: select_rows("* from [//tmp/r2]", driver=self.replica_driver)[-1]
                == {
                    "$tablet_index": 0,
                    "$row_index": before_index2 + 2,
                    "key": 1,
                    "value1": yson.YsonEntity(),
                    "value2": 456,
                }
            )

            wait(lambda: get("#{0}/@tablets/0/committed_replication_row_index".format(replica_id1)) == before_index1 + 3)
            wait(lambda: get("#{0}/@tablets/0/committed_replication_row_index".format(replica_id2)) == before_index1 + 3)

            after_ts1 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id1))
            after_ts2 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id2))
            assert after_ts1 == after_ts2
            assert after_ts1 > before_ts1

        _do()

        sync_alter_table_replica_mode(replica_id1, "async")
        sync_alter_table_replica_mode(replica_id1, "sync")

        _do()

        if use_hunks:
            sync_unmount_table("//tmp/t")
            sync_unmount_table("//tmp/h")

    @authors("aozeritsky")
    def test_replication_unversioned(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id1 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r1",
            attributes={"mode": "sync"},
        )
        replica_id2 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r2",
            attributes={"mode": "async", "preserve_timestamps": "false"},
        )
        self._create_replica_table("//tmp/r1", replica_id1)
        self._create_replica_table("//tmp/r2")
        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1

        def _do():
            before_index1 = get("#{0}/@tablets/0/committed_replication_row_index".format(replica_id1))
            before_index2 = get("#{0}/@tablets/0/committed_replication_row_index".format(replica_id2))
            assert before_index1 == before_index2

            before_ts1 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id1))
            before_ts2 = get("#{0}/@tablets/0/current_replication_timestamp".format(replica_id2))
            assert before_ts1 == before_ts2

            insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}])
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver) == [
                {"key": 1, "value1": "test", "value2": 123}
            ]
            wait(
                lambda: select_rows("* from [//tmp/r2] where key=1", driver=self.replica_driver)
                == [{"key": 1, "value1": "test", "value2": 123}]
            )

            insert_rows("//tmp/t", [{"key": 1, "value1": "new_test"}], update=True)
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver) == [
                {"key": 1, "value1": "new_test", "value2": 123}
            ]
            wait(
                lambda: select_rows("* from [//tmp/r2] where key=1", driver=self.replica_driver)
                == [{"key": 1, "value1": "new_test", "value2": 123}]
            )

            insert_rows("//tmp/t", [{"key": 1, "value2": 456}], update=True)
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver) == [
                {"key": 1, "value1": "new_test", "value2": 456}
            ]
            wait(
                lambda: select_rows("* from [//tmp/r2] where key=1", driver=self.replica_driver)
                == [{"key": 1, "value1": "new_test", "value2": 456}]
            )

            delete_rows("//tmp/t", [{"key": 1}])
            assert select_rows("* from [//tmp/r1]", driver=self.replica_driver) == []
            wait(lambda: select_rows("* from [//tmp/r2] where key=1", driver=self.replica_driver) == [])

            insert_rows(
                "//tmp/r2",
                [{"key": 2, "value2": 457}],
                update=True,
                driver=self.replica_driver,
            )
            wait(
                lambda: select_rows("* from [//tmp/r2] where key=2", driver=self.replica_driver)
                == [{"key": 2, "value1": yson.YsonEntity(), "value2": 457}]
            )

            wait(lambda: get("#{0}/@tablets/0/committed_replication_row_index".format(replica_id1)) == before_index1 + 4)
            wait(lambda: get("#{0}/@tablets/0/committed_replication_row_index".format(replica_id2)) == before_index1 + 4)

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

        with pytest.raises(YtError):
            create_table_replica(
                "//tmp/t",
                self.REPLICA_CLUSTER_NAME,
                "//tmp/r1",
                attributes={
                    "mode": "async",
                    "preserve_timestamps": "false",
                    "atomicity": "none",
                },
            )
        replica_id = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r1",
            attributes={"mode": "async", "preserve_timestamps": "false"},
        )
        with pytest.raises(YtError):
            alter_table_replica(replica_id, atomicity="none")

    @authors("akozhikhov")
    def test_sync_replication_switch(self):
        self._create_cells()
        self._create_replicated_table(
            "//tmp/t",
            SIMPLE_SCHEMA_SORTED,
            replicated_table_options={"enable_replicated_table_tracker": False},
        )
        replica_id1 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r1",
            attributes={"mode": "sync"},
        )
        replica_id2 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r2",
            attributes={"mode": "async"},
        )
        self._create_replica_table("//tmp/r1", replica_id1)
        self._create_replica_table("//tmp/r2", replica_id2)
        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        set(
            "//tmp/t/@replicated_table_options",
            {
                "enable_replicated_table_tracker": True,
                "tablet_cell_bundle_name_failure_interval": 1000,
            },
        )

        remove("//tmp/r1", driver=self.replica_driver)

        wait(lambda: get("#{0}/@mode".format(replica_id1)) == "async")
        wait(lambda: get("#{0}/@mode".format(replica_id2)) == "sync")

    @authors("akozhikhov")
    def test_sync_replication_switch_bundle_health(self):
        self._create_cells()
        self._create_replicated_table(
            "//tmp/t",
            SIMPLE_SCHEMA_SORTED,
            replicated_table_options={"enable_replicated_table_tracker": False},
        )
        replica_id1 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r1",
            attributes={"mode": "sync"},
        )
        replica_id2 = create_table_replica("//tmp/t", "primary", "//tmp/r2", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r1", replica_id1)
        self._create_replica_table("//tmp/r2", replica_id2, replica_driver=self.primary_driver)
        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        set(
            "//tmp/t/@replicated_table_options",
            {
                "enable_replicated_table_tracker": True,
                "tablet_cell_bundle_name_failure_interval": 1000,
            },
        )

        wait(lambda: get("//sys/tablet_cell_bundles/default/@health", driver=self.replica_driver) == "good")

        nodes = ls("//sys/cluster_nodes", driver=self.replica_driver)
        for node in nodes:
            set_node_banned(node, True, driver=self.replica_driver)

        wait(lambda: get("//sys/tablet_cell_bundles/default/@health", driver=self.replica_driver) != "good")

        wait(lambda: get("#{0}/@mode".format(replica_id1)) == "async")
        wait(lambda: get("#{0}/@mode".format(replica_id2)) == "sync")

        for node in nodes:
            set_node_banned(node, False, wait_for_master=False, driver=self.replica_driver)

        wait(lambda: get("//sys/tablet_cell_bundles/default/@health", driver=self.replica_driver) == "good")

    @authors("akozhikhov")
    def test_sync_replication_switch_with_min_sync_replica(self):
        self._create_cells()
        self._create_replicated_table(
            "//tmp/t",
            SIMPLE_SCHEMA_SORTED,
            replicated_table_options={"enable_replicated_table_tracker": False},
        )
        replica_id1 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r1",
            attributes={"mode": "async"},
        )
        replica_id2 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r2",
            attributes={"mode": "async"},
        )
        self._create_replica_table("//tmp/r1", replica_id1)
        self._create_replica_table("//tmp/r2", replica_id2)
        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        set(
            "//tmp/t/@replicated_table_options",
            {
                "enable_replicated_table_tracker": True,
                "min_sync_replica_count": 2,
                "tablet_cell_bundle_name_failure_interval": 1000,
            },
        )

        wait(lambda: get("#{0}/@mode".format(replica_id1)) == "sync")
        wait(lambda: get("#{0}/@mode".format(replica_id2)) == "sync")

    def _get_sync_replicas(self, replica_ids):
        result = 0
        for replica_id in replica_ids:
            if get("#{0}/@mode".format(replica_id)) == "sync":
                result = result + 1
        return result

    @authors("akozhikhov")
    def test_sync_replication_switch_with_min_max_sync_replica(self):
        self._create_cells()
        self._create_replicated_table(
            "//tmp/t",
            SIMPLE_SCHEMA_SORTED,
            replicated_table_options={"enable_replicated_table_tracker": False},
        )
        replica_ids = []
        for i in range(5):
            table_path = "//tmp/r{}".format(i)
            replica_id = create_table_replica(
                "//tmp/t",
                self.REPLICA_CLUSTER_NAME,
                table_path,
                attributes={"mode": "async"},
            )
            self._create_replica_table(table_path, replica_id)
            sync_enable_table_replica(replica_id)
            replica_ids.append(replica_id)

        set(
            "//tmp/t/@replicated_table_options",
            {
                "enable_replicated_table_tracker": True,
                "min_sync_replica_count": 2,
                "max_sync_replica_count": 4,
                "tablet_cell_bundle_name_failure_interval": 1000,
            },
        )

        wait(lambda: self._get_sync_replicas(replica_ids) == 4)

        def brake_sync_replicas(count):
            result = []
            for i in range(5):
                if count <= 0:
                    break
                table_path = "//tmp/r{}".format(i)
                if get("#{0}/@mode".format(replica_ids[i])) == "sync" and exists(
                    table_path, driver=self.replica_driver
                ):
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
        assert len(broken_replicas) == 1
        wait(lambda: self._get_sync_replicas(replica_ids) == 2)

    @authors("akozhikhov")
    @flaky(max_runs=3)
    def test_sync_replication_switch_with_not_enough_healthy_replicas(self):
        self._create_cells()
        self._create_replicated_table(
            "//tmp/t",
            SIMPLE_SCHEMA_SORTED,
            replicated_table_options={"enable_replicated_table_tracker": False},
        )
        replica_ids = []
        for i in range(5):
            table_path = "//tmp/r{}".format(i)
            replica_id = create_table_replica(
                "//tmp/t",
                self.REPLICA_CLUSTER_NAME,
                table_path,
                attributes={"mode": "async"},
            )
            self._create_replica_table(table_path, replica_id)
            sync_enable_table_replica(replica_id)
            replica_ids.append(replica_id)

        set(
            "//tmp/t/@replicated_table_options",
            {
                "enable_replicated_table_tracker": True,
                "min_sync_replica_count": 3,
                "max_sync_replica_count": 4,
                "tablet_cell_bundle_name_failure_interval": 1000,
            },
        )

        wait(lambda: self._get_sync_replicas(replica_ids) == 4)

        for i in range(3):
            remove("//tmp/r{}".format(i), driver=self.replica_driver)
        wait(lambda: self._get_sync_replicas(replica_ids[3:]) == 2)
        wait(lambda: self._get_sync_replicas(replica_ids[:3]) == 1)

        replica_modes = {replica_id: get("#{0}/@mode".format(replica_id)) for replica_id in replica_ids}
        sleep(1.0)
        for replica_id in replica_ids:
            assert get("#{0}/@mode".format(replica_id)) == replica_modes[replica_id]

    @authors("akozhikhov")
    def test_replicated_table_tracker_options(self):
        self._create_cells()
        set(
            "//sys/@config/tablet_manager/replicated_table_tracker/bundle_health_cache",
            {"expire_after_access_time": 1337},
        )
        assert (
            get("//sys/@config/tablet_manager/replicated_table_tracker/bundle_health_cache/expire_after_access_time")
            == 1337
        )

    @authors("babenko")
    def test_cannot_sync_write_into_disabled_replica(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r", replica_id)
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}])

    @authors("babenko")
    def test_upstream_replica_id_check1(self):
        self._create_cells()
        self._create_replica_table("//tmp/r", "1-2-3-4")
        with pytest.raises(YtError):
            insert_rows("//tmp/r", [{"key": 1, "value2": 456}], driver=self.replica_driver)

    @authors("babenko")
    def test_upstream_replica_id_check2(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r", "1-2-3-4")
        sync_enable_table_replica(replica_id)
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key": 1, "value2": 456}])

    @authors("babenko")
    def test_wait_for_sync_sorted(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", SIMPLE_SCHEMA_SORTED)
        replica_id1 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r1",
            attributes={"mode": "sync"},
        )
        replica_id2 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r2",
            attributes={"mode": "async"},
        )
        self._create_replica_table("//tmp/r1", replica_id1, SIMPLE_SCHEMA_SORTED)
        self._create_replica_table("//tmp/r2", replica_id2, SIMPLE_SCHEMA_SORTED)

        sync_enable_table_replica(replica_id1)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}])

        assert select_rows("* from [//tmp/r1]", driver=self.replica_driver) == [
            {"key": 1, "value1": "test", "value2": 123}
        ]
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

        assert select_rows("* from [//tmp/r2]", driver=self.replica_driver)[-1] == {
            "key": 1,
            "value1": "test2",
            "value2": 456,
        }
        wait(
            lambda: select_rows("* from [//tmp/r1]", driver=self.replica_driver)[-1]
            == {"key": 1, "value1": "test2", "value2": 456}
        )

    # XXX(babenko): ordered tables may currently return stale data
    @authors("babenko")
    def test_wait_for_sync_ordered(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", SIMPLE_SCHEMA_ORDERED)
        replica_id1 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r1",
            attributes={"mode": "sync"},
        )
        replica_id2 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r2",
            attributes={"mode": "async"},
        )
        self._create_replica_table("//tmp/r1", replica_id1, SIMPLE_SCHEMA_ORDERED)
        self._create_replica_table("//tmp/r2", replica_id2, SIMPLE_SCHEMA_ORDERED)

        sync_enable_table_replica(replica_id1)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test", "value2": 123}])

        wait(
            lambda: select_rows("* from [//tmp/r1]", driver=self.replica_driver)
            == [
                {
                    "$tablet_index": 0,
                    "$row_index": 0,
                    "key": 1,
                    "value1": "test",
                    "value2": 123,
                }
            ]
        )
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

        wait(
            lambda: select_rows("* from [//tmp/r2]", driver=self.replica_driver)[-1]
            == {
                "$tablet_index": 0,
                "$row_index": 1,
                "key": 1,
                "value1": "test2",
                "value2": 456,
            }
        )
        wait(
            lambda: select_rows("* from [//tmp/r1]", driver=self.replica_driver)[-1]
            == {
                "$tablet_index": 0,
                "$row_index": 1,
                "key": 1,
                "value1": "test2",
                "value2": 456,
            }
        )

    @authors("babenko")
    def test_disable_propagates_replication_row_index(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id)

        sync_enable_table_replica(replica_id)

        assert get("#{0}/@tablets/0/committed_replication_row_index".format(replica_id)) == 0

        insert_rows(
            "//tmp/t",
            [{"key": 1, "value1": "test", "value2": 123}],
            require_sync_replica=False,
        )

        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)
            == [{"key": 1, "value1": "test", "value2": 123}]
        )

        sync_disable_table_replica(replica_id)

        assert get("#{0}/@tablets/0/committed_replication_row_index".format(replica_id)) == 1

    @authors("babenko")
    def test_unmount_propagates_replication_row_index(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id)

        sync_enable_table_replica(replica_id)

        assert get("#{0}/@tablets/0/committed_replication_row_index".format(replica_id)) == 0

        insert_rows(
            "//tmp/t",
            [{"key": 1, "value1": "test", "value2": 123}],
            require_sync_replica=False,
        )
        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)
            == [{"key": 1, "value1": "test", "value2": 123}]
        )

        sync_unmount_table("//tmp/t")

        assert get("#{0}/@tablets/0/committed_replication_row_index".format(replica_id)) == 1

    @pytest.mark.parametrize(
        "with_data, schema",
        [
            (False, SIMPLE_SCHEMA_SORTED),
            (True, SIMPLE_SCHEMA_SORTED),
            (False, SIMPLE_SCHEMA_ORDERED),
            (True, SIMPLE_SCHEMA_ORDERED),
        ],
    )
    @authors("babenko")
    def test_start_replication_timestamp(self, with_data, schema):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test"}], require_sync_replica=False)

        replica_id = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r",
            attributes={"start_replication_timestamp": generate_timestamp()},
        )
        self._create_replica_table("//tmp/r", replica_id, schema)

        sync_enable_table_replica(replica_id)

        if with_data:
            insert_rows("//tmp/t", [{"key": 2, "value1": "test"}], require_sync_replica=False)

        def _maybe_add_system_fields(dict):
            if schema is self.SIMPLE_SCHEMA_ORDERED:
                dict["$tablet_index"] = 0
                dict["$row_index"] = 0
                return dict
            else:
                return dict

        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)
            == ([_maybe_add_system_fields({"key": 2, "value1": "test", "value2": yson.YsonEntity()})]
                if with_data else [])
        )

    @authors("savrus")
    @pytest.mark.parametrize("schema", [SIMPLE_SCHEMA_SORTED, SIMPLE_SCHEMA_ORDERED])
    def test_start_replication_row_indexes(self, schema):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema)

        rows = [{"key": i, "value1": str(i)} for i in range(3)]

        for i in range(2):
            insert_rows("//tmp/t", [rows[i]], require_sync_replica=False)
            sync_flush_table("//tmp/t")

        insert_rows("//tmp/t", [rows[2]], require_sync_replica=False)

        for i in range(4):
            replica_path = "//tmp/r{0}".format(i)
            replica_id = create_table_replica(
                "//tmp/t",
                self.REPLICA_CLUSTER_NAME,
                replica_path,
                attributes={"start_replication_row_indexes": [i]},
            )
            self._create_replica_table(replica_path, replica_id, schema)
            sync_enable_table_replica(replica_id)

        for i in range(4):
            wait(
                lambda: select_rows(
                    "key, value1 from [//tmp/r{0}]".format(i),
                    driver=self.replica_driver,
                )
                == rows[i:]
            )

    @authors("babenko")
    @flaky(max_runs=5)
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_replication_trim(self, mode):
        self._create_cells()
        self._create_replicated_table(
            "//tmp/t",
            dynamic_store_auto_flush_period=1000,
            dynamic_store_flush_period_splay=0)

        sync_mount_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": mode})
        self._create_replica_table("//tmp/r", replica_id)

        sync_enable_table_replica(replica_id)

        insert_rows("//tmp/t", [{"key": 1, "value1": "test1"}], require_sync_replica=False)
        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)
            == [{"key": 1, "value1": "test1", "value2": yson.YsonEntity()}]
        )
        # 2 dynamic stores.
        wait(lambda: get("//tmp/t/@chunk_count") == 1 + 2)

        sync_unmount_table("//tmp/t")

        initial_chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(initial_chunk_ids) == 1

        assert get("//tmp/t/@tablets/0/flushed_row_count") == 1
        assert get("//tmp/t/@tablets/0/trimmed_row_count") == 0

        set("//tmp/t/@min_replication_log_ttl", 3000)
        sync_mount_table("//tmp/t")

        sleep(5.0)

        insert_rows("//tmp/t", [{"key": 2, "value1": "test2"}], require_sync_replica=False)
        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)
            == [
                {"key": 1, "value1": "test1", "value2": yson.YsonEntity()},
                {"key": 2, "value1": "test2", "value2": yson.YsonEntity()},
            ]
        )

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
        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)
            == [{"key": 1, "value1": "test1", "value2": yson.YsonEntity()}]
        )

        insert_rows(
            "//tmp/t",
            [{"key": 1, "value1": "test2", "value2": 100}],
            require_sync_replica=False,
        )
        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)
            == [{"key": 1, "value1": "test2", "value2": 100}]
        )

        insert_rows(
            "//tmp/t",
            [{"key": 1, "value2": 50}],
            aggregate=True,
            update=True,
            require_sync_replica=False,
        )
        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)
            == [{"key": 1, "value1": "test2", "value2": 150}]
        )

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

        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)
            == [{"hash": 1, "key": 1, "value": 2}, {"hash": 2, "key": 12, "value": 12}]
        )

        delete_rows("//tmp/t", [{"key": 1}], require_sync_replica=False)
        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [{"hash": 2, "key": 12, "value": 12}]
        )

    @authors("gridem")
    def test_shard_replication(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.SIMPLE_SCHEMA_SORTED, pivot_keys=[[], [10]])
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, schema=self.SIMPLE_SCHEMA_SORTED)

        sync_enable_table_replica(replica_id)

        insert_rows(
            "//tmp/t",
            [{"key": 1, "value1": "v", "value2": 2}],
            require_sync_replica=False,
        )
        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)
            == [{"key": 1, "value1": "v", "value2": 2}]
        )

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
        with pytest.raises(YtError):
            create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        abort_transaction(tx1)

        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        tx2 = start_transaction()
        lock("//tmp/t", mode="exclusive", tx=tx2)
        with pytest.raises(YtError):
            remove_table_replica(replica_id)

    @authors("babenko")
    def test_lookup(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.AGGREGATE_SCHEMA)
        replica_id1 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r1",
            attributes={"mode": "async"},
        )
        replica_id2 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r2",
            attributes={"mode": "sync"},
        )
        self._create_replica_table("//tmp/r1", replica_id1, schema=self.AGGREGATE_SCHEMA)
        self._create_replica_table("//tmp/r2", replica_id2, schema=self.AGGREGATE_SCHEMA)

        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        for i in range(10):
            insert_rows("//tmp/t", [{"key": i, "value1": "test" + str(i)}])

        for i in range(9):
            assert lookup_rows("//tmp/t", [{"key": i}, {"key": i + 1}], column_names=["key", "value1"]) == [
                {"key": i, "value1": "test" + str(i)},
                {"key": i + 1, "value1": "test" + str(i + 1)},
            ]

        assert lookup_rows("//tmp/t", [{"key": 100000}]) == []

        sync_disable_table_replica(replica_id2)
        sync_alter_table_replica_mode(replica_id2, "async")
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key": 666, "value1": "hello"}])
        insert_rows("//tmp/t", [{"key": 666, "value1": "hello"}], require_sync_replica=False)

        sync_alter_table_replica_mode(replica_id2, "sync")
        with pytest.raises(YtError):
            lookup_rows("//tmp/t", [{"key": 666}])

        sync_enable_table_replica(replica_id2)

        def check_catchup():
            try:
                return lookup_rows("//tmp/t", [{"key": 666}], column_names=["key", "value1"]) == [
                    {"key": 666, "value1": "hello"}
                ]
            except YtError:
                return False

        wait(check_catchup)

        sync_alter_table_replica_mode(replica_id2, "async")
        with pytest.raises(YtError):
            lookup_rows("//tmp/t", [{"key": 666}])

    @authors("babenko")
    def test_select(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.AGGREGATE_SCHEMA)
        replica_id1 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r1",
            attributes={"mode": "async"},
        )
        replica_id2 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r2",
            attributes={"mode": "sync"},
        )
        self._create_replica_table("//tmp/r1", replica_id1, schema=self.AGGREGATE_SCHEMA)
        self._create_replica_table("//tmp/r2", replica_id2, schema=self.AGGREGATE_SCHEMA)

        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        rows = [{"key": i, "value1": "test" + str(i)} for i in range(10)]
        insert_rows("//tmp/t", rows)
        assert_items_equal(select_rows("key, value1 from [//tmp/t]"), rows)
        assert_items_equal(select_rows("sum(key) from [//tmp/t] group by 0"), [{"sum(key)": 45}])

        create(
            "table",
            "//tmp/z",
            attributes={"dynamic": True, "schema": self.SIMPLE_SCHEMA_SORTED},
        )
        with pytest.raises(YtError):
            select_rows("* from [//tmp/t] as t1 join [//tmp/z] as t2 on t1.key = t2.key")

        sync_alter_table_replica_mode(replica_id2, "async")
        sleep(1.0)
        with pytest.raises(YtError):
            select_rows("* from [//tmp/t]")

    @authors("babenko")
    def test_local_sync_replica_yt_7571(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", "primary", "//tmp/r", attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r", replica_id, replica_driver=self.primary_driver)
        sync_enable_table_replica(replica_id)

        rows = [{"key": i, "value1": "test" + str(i)} for i in range(10)]
        insert_rows("//tmp/t", rows)

        assert_items_equal(select_rows("key, value1 from [//tmp/t]", driver=self.primary_driver), rows)
        assert_items_equal(select_rows("key, value1 from [//tmp/r]", driver=self.primary_driver), rows)

    @authors("babenko")
    def test_local_async_replica_yt_12906(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", "primary", "//tmp/r", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r", replica_id, replica_driver=self.primary_driver)
        sync_enable_table_replica(replica_id)

        rows = [{"key": i, "value1": "test" + str(i)} for i in range(10)]
        insert_rows("//tmp/t", rows, require_sync_replica=False)

        wait(lambda: select_rows("key, value1 from [//tmp/r]", driver=self.primary_driver) == rows)

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_inverted_schema(self, mode):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": mode})
        self._create_replica_table("//tmp/r", replica_id, schema=self.PERTURBED_SCHEMA)
        sync_enable_table_replica(replica_id)

        insert_rows(
            "//tmp/t",
            [{"key": 1, "value1": "test", "value2": 10}],
            require_sync_replica=False,
        )
        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)
            == [{"key": 1, "value1": "test", "value2": 10}]
        )

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
            create_table_replica(
                "//tmp/dir/t",
                self.REPLICA_CLUSTER_NAME,
                "//tmp/r",
                authenticated_user="u",
            )

        set("//tmp/dir/@acl", [make_ace("allow", "u", "write")])
        replica_id = create_table_replica("//tmp/dir/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", authenticated_user="u")

        set("//tmp/dir/@acl", [make_ace("deny", "u", "write")])
        with pytest.raises(YtError):
            alter_table_replica(replica_id, enabled=True, authenticated_user="u")
        with pytest.raises(YtError):
            set("#" + replica_id + "/@attr", "value", authenticated_user="u")

        set("//tmp/dir/@acl", [make_ace("allow", "u", "write")])
        alter_table_replica(replica_id, enabled=True, sauthenticated_user="u")
        get("//tmp/dir/t/@effective_acl")
        get("#" + replica_id + "/@effective_acl")
        set("#" + replica_id + "/@attr", "value", authenticated_user="u")

        set("//tmp/dir/@acl", [make_ace("deny", "u", "write")])
        with pytest.raises(YtError):
            remove("#" + replica_id, authenticated_user="u")

        set("//tmp/dir/@acl", [make_ace("deny", "u", "read")])
        with pytest.raises(YtError):
            get("#" + replica_id + "/@attr", authenticated_user="u")

        set("//tmp/dir/@acl", [make_ace("allow", "u", "read")])
        assert get("#" + replica_id + "/@attr", authenticated_user="u") == "value"

        set("//tmp/dir/@acl", [make_ace("deny", "u", "read")])
        with pytest.raises(YtError):
            ls("#" + replica_id + "/@", authenticated_user="u")

        set("//tmp/dir/@acl", [make_ace("allow", "u", "read")])
        assert "attr" in ls("#" + replica_id + "/@", authenticated_user="u")

        set("//tmp/dir/@acl", [make_ace("allow", "u", "write")])
        remove("#" + replica_id, authenticated_user="u")

    @authors("akozhikhov")
    def test_replica_write_permission_error(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r", replica_id)
        sync_enable_table_replica(replica_id)

        create_user("u")
        create_user("u", driver=self.replica_driver)
        set("//tmp/r/@acl", [make_ace("deny", "u", "write")], driver=self.replica_driver)

        rows = [{"key": 1, "value1": "1"}]
        with pytest.raises(YtError):
            insert_rows("//tmp/t", rows, authenticated_user="u")

    @authors("ifsmirnov")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_required_columns(self, mode):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.REQUIRED_SCHEMA)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": mode})
        self._create_replica_table("//tmp/r", replica_id, schema=self.REQUIRED_SCHEMA)
        sync_enable_table_replica(replica_id)

        insert_rows(
            "//tmp/t",
            [{"key1": 1, "key2": 1, "value1": 1, "value2": 1}],
            require_sync_replica=False,
        )
        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)
            == [{"key1": 1, "key2": 1, "value1": 1, "value2": 1}]
        )

        with pytest.raises(YtError):
            insert_rows(
                "//tmp/t",
                [{"key2": 1, "value1": 1, "value2": 1}],
                require_sync_replica=False,
            )
        with pytest.raises(YtError):
            insert_rows(
                "//tmp/t",
                [{"key1": 1, "key2": 1, "value2": 1}],
                require_sync_replica=False,
            )

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
            insert_rows(
                "//tmp/t",
                [{"key2": 1, "value1": 1, "value2": 1}],
                require_sync_replica=False,
            )
        with pytest.raises(YtError):
            insert_rows(
                "//tmp/t",
                [{"key1": 2, "key2": 2, "value2": 2}],
                require_sync_replica=False,
            )
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
            insert_rows(
                "//tmp/t",
                [{"key2": 1, "value1": 1, "value2": 1}],
                require_sync_replica=False,
            )
        with pytest.raises(YtError):
            insert_rows(
                "//tmp/t",
                [{"key1": 2, "key2": 2, "value2": 2}],
                require_sync_replica=False,
            )
        full_row = {"key1": 3, "key2": 3, "value1": 3, "value2": 3}
        insert_rows("//tmp/t", [full_row], require_sync_replica=False)
        wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == [full_row])

    @authors("ifsmirnov")
    @pytest.mark.parametrize("failure_type", ["key", "value"])
    def test_required_columns_schema_mismatch_2_async(self, failure_type):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.REQUIREDLESS_SCHEMA)
        replica_id = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r",
            attributes={"mode": "async"},
        )
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
        replica_id = create_table_replica(
            "//tmp/t_original",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r",
            attributes={"mode": "async"},
        )
        self._create_replica_table("//tmp/r", schema=self.REQUIRED_SCHEMA)
        sync_enable_table_replica(replica_id)

        sync_freeze_table("//tmp/t_original")
        copy("//tmp/t_original", "//tmp/t_copy")
        sync_unfreeze_table("//tmp/t_original")

        original_replica_info = list(get("//tmp/t_original/@replicas").values())[0]
        cloned_replica_info = list(get("//tmp/t_copy/@replicas").values())[0]

        for key in list(original_replica_info.keys()):
            if key != "state":
                assert original_replica_info[key] == cloned_replica_info[key]
        assert cloned_replica_info["state"] == "disabled"

    @authors("avmatrosov")
    def test_replication_via_copied_table(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t_original", schema=self.SIMPLE_SCHEMA_SORTED)
        replica_id = create_table_replica(
            "//tmp/t_original",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r",
            attributes={"mode": "async", "preserve_timestamps": "false"},
        )
        self._create_replica_table("//tmp/r", schema=self.SIMPLE_SCHEMA_SORTED)

        sync_freeze_table("//tmp/t_original")
        copy("//tmp/t_original", "//tmp/t_copy")
        sync_unfreeze_table("//tmp/t_original")

        sync_mount_table("//tmp/t_copy")

        sync_enable_table_replica(replica_id)
        sync_enable_table_replica(list(get("//tmp/t_copy/@replicas").keys())[0])

        for table in ["t_original", "t_copy"]:
            rows = [{"key": 1, "value1": table, "value2": 0}]
            insert_rows("//tmp/{}".format(table), rows, update=True, require_sync_replica=False)
            wait(lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver) == rows)

    @authors("savrus")
    def test_replication_row_indexes(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t_original", schema=self.SIMPLE_SCHEMA_SORTED)
        replica_id = create_table_replica(
            "//tmp/t_original",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r",
            attributes={"mode": "async", "preserve_timestamps": "false"},
        )
        self._create_replica_table("//tmp/r", schema=self.SIMPLE_SCHEMA_SORTED)
        sync_enable_table_replica(replica_id)

        rows = [{"key": 0, "value1": "0", "value2": 0}]
        insert_rows("//tmp/t_original", rows, require_sync_replica=False)
        wait(lambda: lookup_rows("//tmp/r", [{"key": 0}], driver=self.replica_driver) == rows)

        sync_freeze_table("//tmp/t_original")
        copy("//tmp/t_original", "//tmp/t_copy")
        sync_unfreeze_table("//tmp/t_original")

        sync_enable_table_replica(list(get("//tmp/t_copy/@replicas").keys())[0])

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
        replica_id1 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r1",
            attributes={"mode": "async"},
        )
        replica_id2 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r2",
            attributes={"mode": "sync"},
        )
        self._create_replica_table("//tmp/r1", replica_id1, schema=self.AGGREGATE_SCHEMA)
        self._create_replica_table("//tmp/r2", replica_id2, schema=self.AGGREGATE_SCHEMA)

        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        for i in range(3):
            insert_rows("//tmp/t", [{"key": i, "value1": "test" + str(i)}])

        for i in range(2):
            assert lookup_rows("//tmp/t", [{"key": i}, {"key": i + 1}], column_names=["key", "value1"]) == [
                {"key": i, "value1": "test" + str(i)},
                {"key": i + 1, "value1": "test" + str(i + 1)},
            ]

        tablet_info = {}

        def _check():
            tablet_infos = get_tablet_infos("//tmp/t", [0])
            assert list(tablet_infos.keys()) == ["tablets"] and len(tablet_infos["tablets"]) == 1
            tablet_info.update(tablet_infos["tablets"][0])

            return tablet_info["total_row_count"] == 3 and \
                tablet_info["barrier_timestamp"] >= tablet_info["last_write_timestamp"]

        wait(lambda: _check())
        assert tablet_info["trimmed_row_count"] == 0

        sync_replica = None
        async_replica = None
        assert len(tablet_info["replica_infos"]) == 2
        for replica in tablet_info["replica_infos"]:
            assert replica["last_replication_timestamp"] <= tablet_info["barrier_timestamp"]
            if replica["mode"] == "sync":
                sync_replica = replica
            elif replica["mode"] == "async":
                async_replica = replica

        assert sync_replica["replica_id"] == replica_id2
        assert sync_replica["current_replication_row_index"] == 3

        assert async_replica["replica_id"] == replica_id1
        assert async_replica["current_replication_row_index"] <= 3

    @authors("ifsmirnov")
    def test_reshard_non_empty_replicated_table(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.SIMPLE_SCHEMA_SORTED, min_replication_log_ttl=0)
        replica_id1 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r1",
            attributes={"mode": "async"},
        )
        replica_id2 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r2",
            attributes={"mode": "sync"},
        )
        self._create_replica_table("//tmp/r1", replica_id1, schema=self.SIMPLE_SCHEMA_SORTED)
        self._create_replica_table("//tmp/r2", replica_id2, schema=self.SIMPLE_SCHEMA_SORTED)

        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        rows = [{"key": 100, "value1": "foo", "value2": 200}]
        insert_rows("//tmp/t", rows)

        wait(
            lambda: all(
                replica["current_replication_row_index"] == 1
                for replica in get_tablet_infos("//tmp/t", [0])["tablets"][0]["replica_infos"]
            )
        )

        sync_flush_table("//tmp/t")

        # Remove one replica to advance replicated trimmed row count.
        remove("#{}".format(replica_id1))
        # 2 dynamic stores.
        wait(lambda: get("//tmp/t/@chunk_count") == 0 + 2)
        assert get_tablet_infos("//tmp/t", [0])["tablets"][0]["trimmed_row_count"] == 1

        sync_unmount_table("//tmp/t")
        with pytest.raises(YtError):
            reshard_table("//tmp/t", [[]])

    @authors("akozhikhov")
    @pytest.mark.parametrize("remove_list", [True, False])
    def test_banned_clusters_in_replicator(self, remove_list):
        self._create_cells()
        self._create_replicated_table("//tmp/t", min_replication_log_ttl=0)
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r", replica_id)
        sync_enable_table_replica(replica_id)

        rows = [{"key": 0, "value1": "0", "value2": 0}]
        insert_rows("//tmp/t", rows, require_sync_replica=False)
        wait(lambda: lookup_rows("//tmp/r", [{"key": 0}], driver=self.replica_driver) == rows)

        set("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters",
            [self.REPLICA_CLUSTER_NAME])

        dummy = {"counter": 1}
        wait(lambda: self._check_replication_is_banned(rows, dummy, self.replica_driver))

        if remove_list:
            remove("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters")
        else:
            set("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters", [])

        rows[0]["value2"] = dummy["counter"]
        insert_rows("//tmp/t", rows, require_sync_replica=False)
        wait(lambda: lookup_rows("//tmp/r", [{"key": 0}], driver=self.replica_driver) == rows)

    @authors("akozhikhov")
    def test_banned_clusters_in_rtt(self):
        set("//sys/@config/tablet_manager/replicated_table_tracker/check_period", 100)
        set("//sys/@config/tablet_manager/replicated_table_tracker/update_period", 100)
        set("//sys/@config/tablet_manager/replicated_table_tracker/bundle_health_cache/refresh_time", 100)

        self._create_cells()
        self._create_replicated_table(
            "//tmp/t",
            schema=self.SIMPLE_SCHEMA_SORTED,
            replicated_table_options={
                "enable_replicated_table_tracker": True,
                "tablet_cell_bundle_name_failure_interval": 100})
        replica_id1 = create_table_replica(
            "//tmp/t",
            "primary",
            "//tmp/r1",
            attributes={"mode": "async"},
        )
        replica_id2 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r2",
            attributes={"mode": "sync"},
        )
        self._create_replica_table(
            "//tmp/r1", replica_id1, schema=self.SIMPLE_SCHEMA_SORTED, replica_driver=self.primary_driver)
        self._create_replica_table(
            "//tmp/r2", replica_id2, schema=self.SIMPLE_SCHEMA_SORTED)

        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        set("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters",
            [self.REPLICA_CLUSTER_NAME])
        wait(lambda: get("#{0}/@mode".format(replica_id1)) == "sync")
        wait(lambda: get("#{0}/@mode".format(replica_id2)) == "async")

        set("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters",
            ["primary"])
        wait(lambda: get("#{0}/@mode".format(replica_id1)) == "async")
        wait(lambda: get("#{0}/@mode".format(replica_id2)) == "sync")

        set("//tmp/t/@replicated_table_options/max_sync_replica_count", 2)
        set("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters", [])
        wait(lambda: get("#{0}/@mode".format(replica_id1)) == "sync")
        wait(lambda: get("#{0}/@mode".format(replica_id2)) == "sync")

    @authors("akozhikhov")
    def test_replicated_table_tracker_enabled_attribute(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1")
        replica_id2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2")
        self._create_replica_table("//tmp/r1", replica_id1)
        self._create_replica_table("//tmp/r2", replica_id2)
        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        replica_infos = get("//tmp/t/@replicas")
        assert replica_infos[replica_id1]["replicated_table_tracker_enabled"]
        assert replica_infos[replica_id2]["replicated_table_tracker_enabled"]

        set("#{}/@enable_replicated_table_tracker".format(replica_id1), True)
        set("#{}/@enable_replicated_table_tracker".format(replica_id2), False)

        replica_infos = get("//tmp/t/@replicas")
        assert replica_infos[replica_id1]["replicated_table_tracker_enabled"]
        assert not replica_infos[replica_id2]["replicated_table_tracker_enabled"]

    @authors("gritukan")
    @pytest.mark.parametrize("preserve_timestamps", [False, True])
    def test_replicate_timestamp_column(self, preserve_timestamps):
        self._create_cells()

        schema = [{"name": "$timestamp", "type": "uint64"}] + SIMPLE_SCHEMA_ORDERED

        self._create_replicated_table("//tmp/t", schema=schema)
        replica_id1 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r1",
            attributes={"mode": "sync"},
        )
        replica_id2 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r2",
            attributes={"mode": "async", "preserve_timestamps": preserve_timestamps},
        )

        self._create_replica_table("//tmp/r1", replica_id1, schema)
        self._create_replica_table("//tmp/r2", replica_id2, schema)
        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        ts_before = generate_timestamp()
        insert_rows("//tmp/t", [{"key": 42, "value1": "a", "value2": 123}])
        ts_after = generate_timestamp()

        def get_rows_from_sync_replica():
            return select_rows("* from [//tmp/r1]", driver=self.replica_driver)
        rows = get_rows_from_sync_replica()
        assert len(rows) == 1
        row1 = rows[0]
        ts = row1["$timestamp"]
        assert ts_before <= ts <= ts_after
        assert row1 == {
            "$timestamp": ts,
            "$tablet_index": 0,
            "$row_index": 0,
            "key": 42,
            "value1": "a",
            "value2": 123,
        }

        def get_rows_from_async_replica():
            return select_rows("* from [//tmp/r2]", driver=self.replica_driver)

        wait(lambda: len(get_rows_from_async_replica()) > 0)
        rows = get_rows_from_async_replica()
        assert len(rows) == 1
        row2 = rows[0]
        if preserve_timestamps:
            assert row1 == row2
        else:
            assert row1["$timestamp"] != row2["$timestamp"]
            del row1["$timestamp"]
            del row2["$timestamp"]
            assert row1 == row2

    @authors("savrus")
    def test_add_zero_lag_replica_to_fully_trimmed_replicated_table(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=self.SIMPLE_SCHEMA_SORTED, min_replication_log_ttl=0)

        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r")
        self._create_replica_table("//tmp/r", replica_id, schema=self.SIMPLE_SCHEMA_SORTED)

        insert_rows("//tmp/t", [{"key": 1, "value2": 2}], require_sync_replica=False)
        sync_freeze_table("//tmp/t")

        sync_enable_table_replica(replica_id)
        wait(lambda: select_rows("key, value2 from [//tmp/r]", driver=self.replica_driver) == [{"key": 1, "value2": 2}])
        wait(lambda: get("//tmp/t/@tablets/0/trimmed_row_count") == 1)

        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1")
        self._create_replica_table("//tmp/r1", replica_id, schema=self.SIMPLE_SCHEMA_SORTED)
        sync_enable_table_replica(replica_id)

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        def _check():
            orchid = self._find_tablet_orchid(get_tablet_leader_address(tablet_id), tablet_id)
            errors = orchid["replication_errors"]
            return replica_id in errors and "Replication reader returned zero rows" in str(errors[replica_id])

        wait(lambda: _check())

    @authors("akozhikhov")
    def test_collocated_replicated_tables(self):
        # TODO(akozhikhov): Fix with new RTT service.
        if self.is_multicell():
            return

        set("//sys/@config/tablet_manager/replicate_table_collocations", False)

        self._create_cells()

        self._create_replicated_table(
            "//tmp/t1",
            schema=self.SIMPLE_SCHEMA_SORTED,
            replicated_table_options={"enable_replicated_table_tracker": True},
            external_cell_tag=11)
        self._create_replicated_table(
            "//tmp/t2",
            schema=self.SIMPLE_SCHEMA_SORTED,
            replicated_table_options={"enable_replicated_table_tracker": True},
            external_cell_tag=11)
        create_table_collocation(table_paths=["//tmp/t1", "//tmp/t2"])

        def _create_replica(replicated_table, replica_table, cluster, mode):
            replica_id = create_table_replica(
                replicated_table,
                cluster,
                replica_table,
                attributes={"mode": mode})
            driver = self.replica_driver if cluster == self.REPLICA_CLUSTER_NAME else self.primary_driver
            self._create_replica_table(
                replica_table,
                replica_id,
                schema=self.SIMPLE_SCHEMA_SORTED,
                replica_driver=driver)
            return replica_id

        replica1 = _create_replica("//tmp/t1", "//tmp/r1", "primary", "sync")
        replica2 = _create_replica("//tmp/t1", "//tmp/r2", self.REPLICA_CLUSTER_NAME, "async")

        replica3 = _create_replica("//tmp/t2", "//tmp/r3", "primary", "async")
        replica4 = _create_replica("//tmp/t2", "//tmp/r4", self.REPLICA_CLUSTER_NAME, "sync")

        sync_enable_table_replica(replica1)
        sync_enable_table_replica(replica2)
        sync_enable_table_replica(replica3)
        sync_enable_table_replica(replica4)

        wait(lambda:
             get("#{}/@mode".format(replica1)) == get("#{}/@mode".format(replica3)) and
             get("#{}/@mode".format(replica2)) == get("#{}/@mode".format(replica4)) and
             get("#{}/@mode".format(replica1)) != get("#{}/@mode".format(replica2)) and
             get("#{}/@mode".format(replica3)) != get("#{}/@mode".format(replica4)))

        if get("#{}/@mode".format(replica1)) == "async":
            expected_sync_cluster = "primary"
            expected_replica1_mode = "sync"
            expected_replica2_mode = "async"
        else:
            expected_sync_cluster = self.REPLICA_CLUSTER_NAME
            expected_replica1_mode = "async"
            expected_replica2_mode = "sync"
        set("//tmp/t1/@replicated_table_options/preferred_sync_replica_clusters", [expected_sync_cluster])
        set("//tmp/t2/@replicated_table_options/preferred_sync_replica_clusters", [expected_sync_cluster])

        wait(lambda:
             get("#{}/@mode".format(replica1)) == expected_replica1_mode and
             get("#{}/@mode".format(replica2)) == expected_replica2_mode and
             get("#{}/@mode".format(replica3)) == expected_replica1_mode and
             get("#{}/@mode".format(replica4)) == expected_replica2_mode)

    @authors("akozhikhov")
    def test_preferred_replica_clusters(self):
        self._create_cells()

        self._create_replicated_table(
            "//tmp/t",
            schema=self.SIMPLE_SCHEMA_SORTED,
            replicated_table_options={"enable_replicated_table_tracker": True},
            external_cell_tag=11)

        replica1 = create_table_replica("//tmp/t", "primary", "//tmp/r1", attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r1", replica1, schema=self.SIMPLE_SCHEMA_SORTED, replica_driver=self.primary_driver)

        replica2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r2", replica2, schema=self.SIMPLE_SCHEMA_SORTED)

        sync_enable_table_replica(replica1)
        sync_enable_table_replica(replica2)

        set("//tmp/t/@replicated_table_options/preferred_sync_replica_clusters", [self.REPLICA_CLUSTER_NAME])
        wait(lambda:
             get("#{}/@mode".format(replica1)) == "async" and
             get("#{}/@mode".format(replica2)) == "sync")

    @authors("akozhikhov")
    def test_forbid_write_if_not_in_sync(self):
        self._create_cells()

        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r", replica_id)
        sync_enable_table_replica(replica_id)

        set("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters",
            [self.REPLICA_CLUSTER_NAME])

        rows = [{"key": 0, "value1": "0", "value2": 0}]
        dummy = {"counter": 1}
        wait(lambda: self._check_replication_is_banned(rows, dummy, self.replica_driver))

        sync_alter_table_replica_mode(replica_id, "sync")

        with raises_yt_error(yt_error_codes.SyncReplicaNotInSync):
            insert_rows("//tmp/t", rows)

    @authors("akozhikhov")
    def test_forbid_weak_commit_ordering(self):
        self._create_cells()
        with raises_yt_error():
            self._create_replicated_table("//tmp/t", self.SIMPLE_SCHEMA_ORDERED, commit_ordering="weak")

    @authors("akozhikhov")
    def test_forbid_none_atomicity(self):
        self._create_cells()
        with raises_yt_error():
            self._create_replicated_table("//tmp/t", atomicity="none")

    @authors("ifsmirnov")
    def test_alter_change_schema(self):
        def _make_schema(key_columns, value_columns):
            columns = []
            for name in key_columns:
                columns.append({"name": name, "type": "string", "sort_order": "ascending"})
            for name in value_columns:
                columns.append({"name": name, "type": "string"})
            return columns

        def _make_row(schema, suffix):
            return {c["name"]: c["name"] + suffix for c in schema}

        key_columns = ["k1", "k2"]
        value_columns = ["v2", "v4"]
        schema = _make_schema(key_columns, value_columns)

        self._create_cells()
        self._create_replicated_table("//tmp/t", schema=schema)

        replica1 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r1", attributes={"mode": "sync"})
        sync_enable_table_replica(replica1)
        replica2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2", attributes={"mode": "async"})
        sync_enable_table_replica(replica2)
        replica3 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r3", attributes={"mode": "async"})
        sync_enable_table_replica(replica3)

        self._create_replica_table("//tmp/r1", replica1, schema=schema)
        self._create_replica_table("//tmp/r2", replica2, schema=schema)
        self._create_replica_table("//tmp/r3", replica3, schema=schema, mount=False)

        rows = []

        def _update_rows_to_schema():
            for row in rows:
                for value in builtins.set(c["name"] for c in schema) - builtins.set(row):
                    row[value] = yson.YsonEntity()

        def _get_rows(replica_path):
            return select_rows("* from [{}]".format(replica_path), driver=self.replica_driver)

        def _alter_table(path, mount=True, driver=None):
            sync_unmount_table(path, driver=driver)
            alter_table(path, schema=schema, driver=driver)
            if mount:
                sync_mount_table(path, driver=driver)

        row = _make_row(schema, "_AAAAA")
        rows.append(row)
        insert_rows("//tmp/t", [row])
        assert_items_equal(_get_rows("//tmp/r1"), rows)
        wait(lambda: are_items_equal(_get_rows("//tmp/r2"), rows))
        # Third replica cannot yet be written.
        wait(lambda: get("//tmp/t/@tablets/0/replication_error_count") == 1)

        key_columns = ["k1", "k2", "k3"]
        value_columns = ["v1", "v2", "v4"]
        schema = _make_schema(key_columns, value_columns)
        _update_rows_to_schema()
        _alter_table("//tmp/t")

        row = _make_row(schema, "_BBBBB")
        rows.append(row)

        # Cannot write since sync replica has fewer key columns.
        with raises_yt_error():
            insert_rows("//tmp/t", [row])

        _alter_table("//tmp/r1", driver=self.replica_driver)
        insert_rows("//tmp/t", [row])
        assert_items_equal(_get_rows("//tmp/r1"), rows)

        # Async replica cannot be written for same reasons.
        wait(lambda: get("//tmp/t/@tablets/0/replication_error_count") == 2)

        _alter_table("//tmp/r2", driver=self.replica_driver)
        wait(lambda: are_items_equal(_get_rows("//tmp/r2"), rows))

        key_columns = ["k1", "k2", "k3"]
        value_columns = ["v1", "v2", "v3", "v4"]
        schema = _make_schema(key_columns, value_columns)
        _update_rows_to_schema()
        _alter_table("//tmp/t")
        _alter_table("//tmp/r1", driver=self.replica_driver)
        _alter_table("//tmp/r2", driver=self.replica_driver)
        _alter_table("//tmp/r3", driver=self.replica_driver, mount=False)

        row = _make_row(schema, "_CCCCC")
        rows.append(row)

        insert_rows("//tmp/t", [row])
        assert_items_equal(_get_rows("//tmp/r1"), rows)
        wait(lambda: are_items_equal(_get_rows("//tmp/r2"), rows))
        sync_mount_table("//tmp/r3", driver=self.replica_driver)
        wait(lambda: are_items_equal(_get_rows("//tmp/r3"), rows))

    @authors("ifsmirnov")
    def test_alter_fails(self):
        self._create_cells()

        schema = self.SIMPLE_SCHEMA_SORTED
        self._create_replicated_table("//tmp/t", schema=schema, mount=False)

        # Even empty table cannot be altered.
        bad_schema = deepcopy(schema)
        bad_schema[0]["type"] = "string"
        with raises_yt_error(yt_error_codes.IncompatibleSchemas):
            alter_table("//tmp/t", schema=bad_schema)

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1234}])
        sync_unmount_table("//tmp/t")

        bad_schema = deepcopy(schema)
        bad_schema[1]["sort_order"] = "ascending"
        with raises_yt_error(yt_error_codes.IncompatibleSchemas):
            alter_table("//tmp/t", schema=bad_schema)

        with raises_yt_error():
            alter_table("//tmp/t", dynamic=False)
        with raises_yt_error():
            alter_table("//tmp/t", schema_modification="unversioned_update")

    @authors("akozhikhov")
    def test_cluster_incoming_replication(self):
        self._create_cells()

        self._create_replicated_table(
            "//tmp/t",
            schema=self.SIMPLE_SCHEMA_SORTED,
            replicated_table_options={"enable_replicated_table_tracker": True},
            external_cell_tag=11)

        replica1 = create_table_replica("//tmp/t", "primary", "//tmp/r1", attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r1", replica1, schema=self.SIMPLE_SCHEMA_SORTED, replica_driver=self.primary_driver)

        replica2 = create_table_replica("//tmp/t", self.REPLICA_CLUSTER_NAME, "//tmp/r2", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r2", replica2, schema=self.SIMPLE_SCHEMA_SORTED)

        sync_enable_table_replica(replica1)
        sync_enable_table_replica(replica2)

        wait(lambda: get("#{0}/@mode".format(replica1)) == "sync")
        wait(lambda: get("#{0}/@mode".format(replica2)) == "async")

        set("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/enable_incoming_replication",
            False,
            driver=self.primary_driver)

        wait(lambda: get("#{0}/@mode".format(replica1)) == "async")
        wait(lambda: get("#{0}/@mode".format(replica2)) == "sync")

    @authors("akozhikhov")
    def test_rtt_for_copied_table(self):
        self._create_cells()
        self._create_replicated_table(
            "//tmp/t_original",
            schema=self.SIMPLE_SCHEMA_SORTED,
            replicated_table_options={
                "enable_replicated_table_tracker": False,
                "min_sync_replica_count": 0,
                "max_sync_replica_count": 2,
            })
        create_table_replica(
            "//tmp/t_original",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r",
            attributes={"mode": "async"},
        )
        create_table_replica(
            "//tmp/t_original",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r2",
            attributes={"mode": "async", "enable_replicated_table_tracker": False},
        )
        self._create_replica_table("//tmp/r", schema=self.SIMPLE_SCHEMA_SORTED)
        self._create_replica_table("//tmp/r2", schema=self.SIMPLE_SCHEMA_SORTED)

        sync_unmount_table("//tmp/t_original")
        copy("//tmp/t_original", "//tmp/t_copy")
        sync_mount_table("//tmp/t_copy")

        replicas = get("//tmp/t_copy/@replicas")
        [replica_id, replica_id2] = list(replicas.keys())
        if replicas[replica_id]["replica_path"] == "//tmp/r2":
            replica_id, replica_id2 = replica_id2, replica_id

        set("//tmp/t_copy/@replicated_table_options/enable_replicated_table_tracker", True)
        assert get("//tmp/t_copy/@replicated_table_options/min_sync_replica_count") == 0
        assert get("#{}/@enable_replicated_table_tracker".format(replica_id))
        assert not get("#{}/@enable_replicated_table_tracker".format(replica_id2))

        sync_enable_table_replica(replica_id)
        sync_enable_table_replica(replica_id2)

        wait(lambda: get("#{0}/@mode".format(replica_id)) == "sync")
        assert get("#{0}/@mode".format(replica_id2)) == "async"

        set("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters",
            [self.REPLICA_CLUSTER_NAME])
        wait(lambda: get("#{0}/@mode".format(replica_id)) == "async")

        set("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters", [])
        wait(lambda: get("#{0}/@mode".format(replica_id)) == "sync")


##################################################################


class TestReplicatedDynamicTablesSafeMode(TestReplicatedDynamicTablesBase):
    USE_PERMISSION_CACHE = False

    DELTA_NODE_CONFIG = {"master_cache_service": {"capacity": 0}}

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

        insert_rows(
            "//tmp/t",
            [{"key": 1, "value1": "test", "value2": 10}],
            require_sync_replica=False,
            authenticated_user="u",
        )
        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)
            == [{"key": 1, "value1": "test", "value2": 10}]
        )

        set("//sys/@config/enable_safe_mode", True, driver=self.replica_driver)

        if mode == "sync":
            with pytest.raises(YtError):
                insert_rows(
                    "//tmp/t",
                    [{"key": 2, "value1": "test", "value2": 10}],
                    authenticated_user="u",
                )
        else:
            insert_rows(
                "//tmp/t",
                [{"key": 2, "value1": "test", "value2": 10}],
                require_sync_replica=False,
            )
            sleep(1)
            assert select_rows("* from [//tmp/r]", driver=self.replica_driver) == [
                {"key": 1, "value1": "test", "value2": 10}
            ]
            wait(lambda: get("//tmp/t/@replicas/{}/error_count".format(replica_id)) == 1)

            tablet_id = get("//tmp/t/@tablets/0/tablet_id")
            address = get_tablet_leader_address(tablet_id)
            orchid = self._find_tablet_orchid(address, tablet_id)
            errors = orchid["replication_errors"]
            assert len(errors) == 1
            assert YtResponseError(list(errors.values())[0]).is_access_denied()

        set("//sys/@config/enable_safe_mode", False, driver=self.replica_driver)

        if mode == "sync":
            insert_rows(
                "//tmp/t",
                [{"key": 2, "value1": "test", "value2": 10}],
                authenticated_user="u",
            )

        wait(
            lambda: select_rows("* from [//tmp/r]", driver=self.replica_driver)
            == [
                {"key": 1, "value1": "test", "value2": 10},
                {"key": 2, "value1": "test", "value2": 10},
            ]
        )

        wait(lambda: get("//tmp/t/@replicas/{}/error_count".format(replica_id)) == 0)

    @authors("akozhikhov")
    def test_replicated_tracker_in_safe_mode(self):
        self._create_cells()
        self._create_replicated_table(
            "//tmp/t",
            replicated_table_options={"enable_replicated_table_tracker": False},
        )
        replica_id1 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r1",
            attributes={"mode": "sync"},
        )
        replica_id2 = create_table_replica("//tmp/t", "primary", "//tmp/r2", attributes={"mode": "async"})
        self._create_replica_table("//tmp/r1", replica_id1)
        self._create_replica_table("//tmp/r2", replica_id2, replica_driver=self.primary_driver)
        sync_enable_table_replica(replica_id1)
        sync_enable_table_replica(replica_id2)

        set(
            "//tmp/t/@replicated_table_options",
            {
                "enable_replicated_table_tracker": True,
                "tablet_cell_bundle_name_failure_interval": 1000,
            },
        )
        set("//sys/@config/enable_safe_mode", True, driver=self.replica_driver)

        wait(lambda: get("#{0}/@mode".format(replica_id1)) == "async")
        wait(lambda: get("#{0}/@mode".format(replica_id2)) == "sync")


##################################################################


class TestReplicatedDynamicTablesMulticell(TestReplicatedDynamicTables):
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


##################################################################


class TestReplicatedDynamicTablesRpcProxy(TestReplicatedDynamicTables):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    @authors("akozhikhov")
    def test_sync_replicas_cache(self):
        self._create_cells()

        self._create_replicated_table("//tmp/t")

        replica_id = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r",
            attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r", replica_id)
        sync_enable_table_replica(replica_id)

        keys = [{"key": 1}]
        rows = [{"key": 1, "value1": "test", "value2": 10}]
        insert_rows("//tmp/t", rows)

        for _ in range(10):
            assert lookup_rows("//tmp/t", keys, cached_sync_replicas_timeout=100) == rows

        replica_id2 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r2",
            attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r2", replica_id2)
        sync_enable_table_replica(replica_id2)
        sync_alter_table_replica_mode(replica_id, "async")

        for _ in range(10):
            assert lookup_rows("//tmp/t", keys, cached_sync_replicas_timeout=5000) == rows
        for _ in range(10):
            assert lookup_rows("//tmp/t", keys, cached_sync_replicas_timeout=100) == rows

    @authors("ngc224")
    @pytest.mark.parametrize("keys_mode", ["nonempty", "empty", "all"])
    def test_get_in_sync_replicas_cache(self, keys_mode):
        self._create_cells()

        self._create_replicated_table("//tmp/t")

        replica_id = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r",
            attributes={"mode": "sync"})

        self._create_replica_table("//tmp/r", replica_id)
        sync_enable_table_replica(replica_id)

        rows = [{"key": 1, "value1": "test", "value2": 10}]
        insert_rows("//tmp/t", rows)

        if keys_mode == "nonempty":
            keys = [{"key": 1}]
        elif keys_mode == "empty":
            keys = []
        elif keys_mode == "all":
            keys = None
        else:
            assert False, "unknown keys mode: {!r}".format(keys_mode)

        def check(timeout, expected_replica_ids):
            for _ in range(10):
                assert_items_equal(
                    get_in_sync_replicas(
                        "//tmp/t", keys,
                        cached_sync_replicas_timeout=timeout,
                        timestamp=SyncLastCommittedTimestamp,
                    ),
                    expected_replica_ids,
                )

        check(timeout=10000, expected_replica_ids=[replica_id])

        replica_id2 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r2",
            attributes={"mode": "sync", "start_replication_row_indexes": [1]})

        self._create_replica_table("//tmp/r2", replica_id2)
        sync_enable_table_replica(replica_id2)

        if keys_mode == "empty":
            check(timeout=10000, expected_replica_ids=[replica_id, replica_id2])
        else:
            check(timeout=10000, expected_replica_ids=[replica_id])

        check(timeout=0, expected_replica_ids=[replica_id, replica_id2])

        sync_alter_table_replica_mode(replica_id2, "async")

        if keys_mode == "empty":
            check(timeout=0, expected_replica_ids=[replica_id, replica_id2])
        else:
            check(timeout=0, expected_replica_ids=[replica_id])

    @authors("akozhikhov")
    @pytest.mark.parametrize("use_replica_cache", [False, True])
    def test_batched_get_in_sync_replicas(self, use_replica_cache):
        [cell_ids, _] = self._create_cells(10)
        node_addresses = [get("#{}/@peers/0/address".format(cell_id)) for cell_id in cell_ids]
        slots_per_node = 4
        assert len(builtins.set(node_addresses)) > (10 // slots_per_node)

        pivot_keys = [[]] + [[i] for i in range(0, 18, 2)]

        self._create_replicated_table("//tmp/t", mount=False)
        reshard_table("//tmp/t", pivot_keys)
        for i in range(10):
            sync_mount_table("//tmp/t", first_tablet_index=i, last_tablet_index=i, cell_id=cell_ids[i])

        replica_id1 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r1",
            attributes={"mode": "sync"})
        self._create_replica_table("//tmp/r1", replica_id1, mount=False)
        reshard_table("//tmp/r1", pivot_keys, driver=self.replica_driver)
        sync_enable_table_replica(replica_id1)
        sync_mount_table("//tmp/r1", driver=self.replica_driver)

        replica_id2 = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r2",
            attributes={"mode": "async"})
        self._create_replica_table("//tmp/r2", replica_id2, mount=False)
        reshard_table("//tmp/r2", pivot_keys, driver=self.replica_driver)
        sync_enable_table_replica(replica_id2)
        sync_mount_table("//tmp/r2", driver=self.replica_driver)
        unmounted_tablet_index = random.randint(0, 9)
        sync_unmount_table("//tmp/r2", driver=self.replica_driver,
                           first_tablet_index=unmounted_tablet_index, last_tablet_index=unmounted_tablet_index)

        for i in range(-1, 18, 2):
            insert_rows("//tmp/t", [{"key": i, "value1": str(i)}])

        def _check(expected_sync_replicas):
            if use_replica_cache:
                actual = get_in_sync_replicas(
                    "//tmp/t",
                    None,
                    timestamp=SyncLastCommittedTimestamp,
                    cached_sync_replicas_timeout=10,
                )
            else:
                actual = get_in_sync_replicas(
                    "//tmp/t",
                    None,
                    timestamp=SyncLastCommittedTimestamp,
                )
            return builtins.set(expected_sync_replicas) == builtins.set(actual)

        assert _check([replica_id1])

        sync_mount_table("//tmp/r2", driver=self.replica_driver)
        sync_alter_table_replica_mode(replica_id2, "sync")
        wait(lambda: _check([replica_id1, replica_id2]))


##################################################################


class TestErasureReplicatedDynamicTables(TestReplicatedDynamicTablesBase):
    @authors("babenko")
    def test_erasure_replicated_table(self):
        self._create_cells()

        erasure_codec = "isa_lrc_12_2_2"
        self._create_replicated_table("//tmp/t", erasure_codec=erasure_codec)

        replica_id = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r",
            attributes={"mode": "async"})
        self._create_replica_table("//tmp/r", replica_id)

        rows = [{"key": i, "value1": "test" + str(i)} for i in range(10)]
        insert_rows("//tmp/t", rows, require_sync_replica=False)

        sync_unmount_table("//tmp/t")

        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#{}/@erasure_codec".format(chunk_id)) == erasure_codec

        sync_mount_table("//tmp/t")

        sync_enable_table_replica(replica_id)

        wait(lambda: select_rows("key, value1 from [//tmp/r] order by key limit 100", driver=self.replica_driver) == rows)
