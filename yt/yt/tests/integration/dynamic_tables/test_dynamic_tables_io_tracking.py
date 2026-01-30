from .test_replicated_dynamic_tables import TestReplicatedDynamicTablesBase

from yt_dynamic_tables_base import DynamicTablesBase
from yt_io_tracking_base import TestIOTrackingBase
from yt_chaos_test_base import ChaosTestBase

from yt_commands import (
    authors, generate_timestamp, get, get_in_sync_replicas, set, exists, get_singular_chunk_id,
    sync_create_cells, sync_mount_table, sync_flush_table,
    create, create_table_replica, sync_enable_table_replica,
    insert_rows, lookup_rows, select_rows, remount_table, sync_unmount_table, update_nodes_dynamic_config)

from yt.common import wait
from yt.test_helpers import are_items_equal

import yt.yson as yson

import pytest

##################################################################


class TestDynamicTableIOTrackingBase(TestIOTrackingBase):
    ENABLE_MULTIDAEMON = False  # Check structured logs.
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "cypress_manager": {
            "default_file_replication_factor": 1,
            "default_table_replication_factor": 1,
            "default_journal_read_quorum": 1,
            "default_journal_write_quorum": 1,
            "default_journal_replication_factor": 1,
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "trace_baggage": {
                "enable_baggage_addition": True
            },
        },
    }

    def _prefer_remote_replicas(self, path):
        if exists("{}/@chunk_reader".format(path)):
            set("{}/@chunk_reader/prefer_local_replicas".format(path), False)
        else:
            set("{}/@chunk_reader".format(path), {"prefer_local_replicas": False})

    def _check_events(self, events, category, allowed_chunk_ids=None):
        for event in events:
            assert event["bytes"] > 0
            assert event["io_requests"] > 0
            assert event["bundle@"] == "default"
            assert event["tablet_category@"] == category
            if allowed_chunk_ids:
                assert event["chunk_id"] in allowed_chunk_ids

##################################################################


class TestTabletNodeIOTracking(TestDynamicTableIOTrackingBase, DynamicTablesBase):
    @authors("tea-mur")
    @pytest.mark.parametrize("sorted", [False, True])
    def test_store_flush(self, sorted):
        # Prepare cluster.
        sync_create_cells(1)
        if sorted:
            self._create_sorted_table("//tmp/table")
        else:
            self._create_ordered_table("//tmp/table")
        sync_mount_table("//tmp/table")

        # Insert + flush.
        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        insert_rows("//tmp/table", [{"key": 0, "value": "test"}])
        sync_flush_table("//tmp/table")

        # Check io log.
        write_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier, check_event_count=True,
                                                filter=lambda event: event.get("data_node_method@") == "FinishChunk")
        self._check_events(write_events, category="store_flush", allowed_chunk_ids=(get_singular_chunk_id("//tmp/table"), ))

    @authors("tea-mur")
    @pytest.mark.parametrize("enable_data_node_lookup", [False, True])
    def test_lookup(self, enable_data_node_lookup):
        # Prepare cluster.
        sync_create_cells(1)
        self._create_sorted_table("//tmp/table")
        self._prefer_remote_replicas("//tmp/table")
        if enable_data_node_lookup:
            self._enable_data_node_lookup("//tmp/table")
            data_node_method = "LookupRows"
        else:
            data_node_method = "GetBlockSet"
        sync_mount_table("//tmp/table")

        # Prepare data.
        data = [{"key": 0, "value": "test"}]
        insert_rows("//tmp/table", data)
        sync_flush_table("//tmp/table")

        # Lookup from data node.
        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        lookup_rows("//tmp/table", [{"key": 0}])

        # Check io log.
        read_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier, check_event_count=True,
                                               filter=lambda event: event.get("data_node_method@") == data_node_method)
        self._check_events(read_events, category="lookup_rows", allowed_chunk_ids=(get_singular_chunk_id("//tmp/table"), ))

    @authors("tea-mur")
    def test_lookup_hunks(self):
        # Prepare cluster.
        sync_create_cells(1)
        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 10},
        ])
        self._create_sorted_table("//tmp/table", schema=schema)
        self._prefer_remote_replicas("//tmp/table")
        sync_mount_table("//tmp/table")

        # Prepare data.
        data = [{"key": 0, "value": "long test string"}]
        insert_rows("//tmp/table", data)
        sync_flush_table("//tmp/table")

        # Lookup from data node.
        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        lookup_rows("//tmp/table", [{"key": 0}])

        # Check io log.
        read_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier, check_event_count=True,
                                               filter=lambda event: event.get("data_node_method@") == "GetChunkFragmentSet")
        self._check_events(read_events, category="lookup_rows")

    @authors("tea-mur")
    @pytest.mark.parametrize("sorted", [False, True])
    @pytest.mark.parametrize(
        "query", (
            "* from [//tmp/table]",
            "* from [//tmp/table] where key = 0",
        ))
    def test_select(self, sorted, query):
        # Prepare cluster.
        sync_create_cells(1)
        if sorted:
            self._create_sorted_table("//tmp/table")
        else:
            self._create_ordered_table("//tmp/table")
        self._prefer_remote_replicas("//tmp/table")
        sync_mount_table("//tmp/table")

        # Prepare data.
        data = [{"key": 0, "value": "test"}]
        insert_rows("//tmp/table", data)
        sync_flush_table("//tmp/table")

        # Select from data node.
        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        select_rows(query)

        # Check io log.
        read_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier, check_event_count=False,
                                               filter=lambda event: event.get("data_node_method@") == "GetBlockSet")
        self._check_events(read_events, category="select_rows", allowed_chunk_ids=(get_singular_chunk_id("//tmp/table"), ))

    @authors("tea-mur")
    def test_compaction(self):
        # Prepare cluster.
        sync_create_cells(1)
        self._create_sorted_table("//tmp/table")
        self._prefer_remote_replicas("//tmp/table")
        sync_mount_table("//tmp/table")

        # Prepare data.
        data = [{"key": 0, "value": "test"}]
        insert_rows("//tmp/table", data)
        sync_flush_table("//tmp/table")

        # Run compaction.
        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        set("//tmp/table/@forced_compaction_revision", 1)
        remount_table("//tmp/table")

        # Extract compaction info from structured lsm log.
        start_compaction_events = self._wait_for_events(count=1, category="Lsm", from_barrier=from_barrier, check_event_count=True,
                                                        filter=lambda event: event.get("event_type") == "start_compaction")
        end_compaction_events = self._wait_for_events(count=1, category="Lsm", from_barrier=from_barrier, check_event_count=True,
                                                      filter=lambda event: event.get("event_type") == "end_compaction")
        old_chunk_ids = start_compaction_events[0]["store_ids"]
        new_chunk_ids = end_compaction_events[0]["store_ids_to_add"]

        # Check io log.
        read_events = self.wait_for_raw_events(count=len(old_chunk_ids), from_barrier=from_barrier, check_event_count=True,
                                               filter=lambda event: event.get("data_node_method@") == "GetBlockSet")
        write_events = self.wait_for_raw_events(count=len(new_chunk_ids), from_barrier=from_barrier, check_event_count=True,
                                                filter=lambda event: event.get("data_node_method@") == "FinishChunk")
        self._check_events(read_events, category="compaction", allowed_chunk_ids=old_chunk_ids)
        self._check_events(write_events, category="compaction", allowed_chunk_ids=new_chunk_ids)

    @authors("tea-mur")
    def test_partitioning(self):
        # Prepare cluster.
        sync_create_cells(1)
        self._create_sorted_table("//tmp/table")
        self._prefer_remote_replicas("//tmp/table")
        set("//tmp/table/@enable_compaction_and_partitioning", False)
        set("//tmp/table/@max_partition_data_size", 640)
        set("//tmp/table/@desired_partition_data_size", 512)
        set("//tmp/table/@min_partition_data_size", 256)
        set("//tmp/table/@min_partitioning_data_size", 256)
        set("//tmp/table/@compression_codec", "none")
        set("//tmp/table/@chunk_writer", {"block_size": 64})
        # Large values for *_compaction_store_count to avoid compaction.
        set("//tmp/table/@min_compaction_store_count", 100)
        set("//tmp/table/@max_compaction_store_count", 101)
        sync_mount_table("//tmp/table")

        # Prepare data.
        data = [{"key": i, "value": str(i)} for i in range(32)]
        insert_rows("//tmp/table", data)
        sync_flush_table("//tmp/table")

        # Run partitioning.
        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        set("//tmp/table/@enable_compaction_and_partitioning", True)
        remount_table("//tmp/table")

        # Extract partitioning info from structured lsm log.
        start_partitioning_events = self._wait_for_events(count=1, category="Lsm", from_barrier=from_barrier, check_event_count=True,
                                                          filter=lambda event: event.get("event_type") == "start_partitioning")
        end_partitioning_events = self._wait_for_events(count=1, category="Lsm", from_barrier=from_barrier, check_event_count=True,
                                                        filter=lambda event: event.get("event_type") == "end_partitioning")
        old_chunk_ids = start_partitioning_events[0]["store_ids"]
        new_chunk_ids = [store["chunk_id"] for store in end_partitioning_events[0]["stores_to_add"]]

        # Check io log.
        read_events = self.wait_for_raw_events(count=len(old_chunk_ids), from_barrier=from_barrier, check_event_count=True,
                                               filter=lambda event: event.get("data_node_method@") == "GetBlockSet")
        write_events = self.wait_for_raw_events(count=len(new_chunk_ids), from_barrier=from_barrier, check_event_count=True,
                                                filter=lambda event: event.get("data_node_method@") == "FinishChunk")
        self._check_events(read_events, category="partitioning", allowed_chunk_ids=old_chunk_ids)
        self._check_events(write_events, category="partitioning", allowed_chunk_ids=new_chunk_ids)

    @authors("tea-mur")
    def test_preload(self):
        # Prepare cluster.
        sync_create_cells(1)
        self._create_sorted_table("//tmp/table")
        self._prefer_remote_replicas("//tmp/table")
        set("//tmp/table/@in_memory_mode", "compressed")
        sync_mount_table("//tmp/table")

        # Prepare data.
        data = [{"key": i, "value": str(i)} for i in range(32)]
        insert_rows("//tmp/table", data)
        sync_unmount_table("//tmp/table")

        # Preload is going to run after mount.
        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        sync_mount_table("//tmp/table")

        # Extract preload info from structured lsm log.
        preload_complete_events = self._wait_for_events(count=1, category="Lsm", from_barrier=from_barrier, check_event_count=True,
                                                        filter=lambda event: event.get("preload_state") == "complete")
        chunk_id = preload_complete_events[0]["store_id"]

        # Check io log.
        read_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier, check_event_count=True,
                                               filter=lambda event: event.get("data_node_method@") == "GetBlockRange")
        self._check_events(read_events, category="preload", allowed_chunk_ids=(chunk_id, ))

    @authors("tea-mur", "akozhikhov")
    def test_dictionary_builder(self):
        def _wait_dictionaries_built(path, previous_hunk_chunk_count):
            # One dictionary hunk chunk for each of two policies.
            wait(lambda: len(self._get_hunk_chunk_ids(path)) == previous_hunk_chunk_count + 2)

        # Prepare cluster.
        sync_create_cells(1)
        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 10},
        ])
        self._create_sorted_table("//tmp/table", schema=schema, chunk_format="table_versioned_simple")
        sync_mount_table("//tmp/table")

        # Prepare data.
        rows = [{"key": i, "value": "value" + str(i) + "x" * 100} for i in range(100)]
        insert_rows("//tmp/table", rows)
        sync_flush_table("//tmp/table")

        # Build dictionary.
        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        set("//tmp/table/@mount_config/value_dictionary_compression", {
            "enable": True,
            "column_dictionary_size": 256,
            "max_processed_chunk_count": 2,
            "backoff_period": 1000,
        })
        remount_table("//tmp/table")
        _wait_dictionaries_built("//tmp/table", 1)

        # Check io log.
        read_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier, check_event_count=False,
                                               filter=lambda event: event.get("data_node_method@") == "GetChunkFragmentSet")
        self._check_events(read_events, category="dictionary_building")

##################################################################


class TestReplicatedTableIOTracking(TestDynamicTableIOTrackingBase, TestReplicatedDynamicTablesBase):
    def setup_method(self, method):
        super(TestReplicatedTableIOTracking, self).setup_method(method)

    @authors("tea-mur")
    def test_replication(self):
        # Prepare cluster.
        self._create_cells()
        self._create_replicated_table("//tmp/table", mount=False)
        self._prefer_remote_replicas("//tmp/table")
        sync_mount_table("//tmp/table")
        replica_id = create_table_replica("//tmp/table", self.REPLICA_CLUSTER_NAME, "//tmp/replica")
        assert get("#{0}/@state".format(replica_id)) == "disabled"
        self._create_replica_table("//tmp/replica", replica_id)

        # Prepare data.
        keys = [{"key": 0}]
        data = [{"key": 0, "value1": "test", "value2": 42}]
        insert_rows("//tmp/table", data, require_sync_replica=False)
        timestamp = generate_timestamp()
        sync_flush_table("//tmp/table")

        # Enable replication.
        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        sync_enable_table_replica(replica_id)
        wait(
            lambda: are_items_equal(get_in_sync_replicas("//tmp/table", keys, timestamp=timestamp),
                                    [replica_id])
        )

        # Check io log.
        read_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier, check_event_count=False,
                                               filter=lambda event: event.get("data_node_method@") == "GetBlockSet")
        self._check_events(read_events, category="replication", allowed_chunk_ids=(get_singular_chunk_id("//tmp/table"), ))

##################################################################


class TestChaosTableIOTracking(TestDynamicTableIOTrackingBase, ChaosTestBase):
    CHAOS_BUNDLE_OPTIONS = {
        "changelog_read_quorum": 1,
        "changelog_write_quorum": 1,
        "changelog_replication_factor": 1,
    }

    def setup_method(self, method):
        super(TestChaosTableIOTracking, self).setup_method(method)

    @authors("tea-mur")
    def test_pull(self):
        # Prepare cluster.
        # Store trimmer reads and caches chunks, so disable it.
        update_nodes_dynamic_config({
            "tablet_node": {
                "store_trimmer": {
                    "enable": False
                },
            },
        })
        cell_id = self._sync_create_chaos_bundle_and_cell(chaos_bundle_options=self.CHAOS_BUNDLE_OPTIONS)
        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/qs"},
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": False, "replica_path": "//tmp/ta"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)
        self._prefer_remote_replicas("//tmp/qs")
        remount_table("//tmp/qs")
        create("chaos_replicated_table", "//tmp/crt", attributes={
            "replication_card_id": card_id,
            "chaos_cell_bundle": "c"
        })

        # Prepare data.
        insert_rows("//tmp/qs", [{"key": 0, "value": "0"}])
        timestamp = generate_timestamp()
        sync_flush_table("//tmp/qs")

        # Enable replication.
        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        self._sync_alter_replica(card_id, replicas, replica_ids, 1, enabled=True)
        wait(lambda: get(f"//tmp/crt/@replicas/{replica_ids[1]}/replication_lag_timestamp") > timestamp)

        # Check io log.
        read_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier, check_event_count=False,
                                               filter=lambda event: event.get("data_node_method@") == "GetBlockSet")
        self._check_events(read_events, category="pull_rows", allowed_chunk_ids=(get_singular_chunk_id("//tmp/qs"), ))

    @authors("tea-mur")
    def test_store_trimmer(self):
        # Prepare cluster.
        cell_id = self._sync_create_chaos_bundle_and_cell(chaos_bundle_options=self.CHAOS_BUNDLE_OPTIONS)
        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/qs"},
        ]
        card_id, _ = self._create_chaos_tables(cell_id, replicas)
        self._prefer_remote_replicas("//tmp/qs")
        remount_table("//tmp/qs")
        create("chaos_replicated_table", "//tmp/crt", attributes={
            "replication_card_id": card_id,
            "chaos_cell_bundle": "c"
        })

        # Prepare data.
        insert_rows("//tmp/qs", [{"key": 0, "value": "0"}])
        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")

        # With store trimmer enabled chunk will be read soon after flush.
        sync_flush_table("//tmp/qs")

        # Check io log.
        read_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier, check_event_count=False,
                                               filter=lambda event: event.get("data_node_method@") == "GetBlockSet")
        self._check_events(read_events, category="replication_log_trim", allowed_chunk_ids=(get_singular_chunk_id("//tmp/qs"), ))
