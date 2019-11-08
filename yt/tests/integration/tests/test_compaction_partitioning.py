import pytest
import __builtin__

from test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_env_setup import wait
from yt_commands import *

import random

################################################################################

class TestCompactionPartitioning(TestSortedDynamicTablesBase):
    @authors("ifsmirnov")
    def test_partition_balancer_chunk_view(self):
        [cell_id] = sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@chunk_writer", {"block_size": 64})
        set("//tmp/t/@compression_codec", "none")
        set("//tmp/t/@tablet_balancer_config/enable_auto_reshard", False)
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        set("//tmp/t/@max_partition_data_size", 640)
        set("//tmp/t/@desired_partition_data_size", 512)
        set("//tmp/t/@min_partition_data_size", 256)
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in xrange(20)])
        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [1], [18]])
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/1/tablet_id")
        address = get_tablet_leader_address(tablet_id)
        orchid = self._find_tablet_orchid(address, tablet_id)
        assert len(orchid["partitions"]) == 1

        build_snapshot(cell_id=cell_id)

        peer = get("//sys/tablet_cells/{}/@peers/0/address".format(cell_id))
        set("//sys/cluster_nodes/{}/@banned".format(peer), True)
        wait(lambda: get("//sys/tablet_cells/{}/@health".format(cell_id)) == "good")

        set("//tmp/t/@enable_compaction_and_partitioning", True)
        remount_table("//tmp/t")
        address = get_tablet_leader_address(tablet_id)
        wait(lambda : len(self._find_tablet_orchid(address, tablet_id)["partitions"]) > 1)

    @authors("savrus")
    def test_partition_balancer(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@max_partition_data_size", 640)
        set("//tmp/t/@desired_partition_data_size", 512)
        set("//tmp/t/@min_partition_data_size", 256)
        set("//tmp/t/@compression_codec", "none")
        set("//tmp/t/@chunk_writer", {"block_size": 64})
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)
        orchid = self._find_tablet_orchid(address, tablet_id)
        assert len(orchid["partitions"]) == 1

        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in xrange(16)])
        sync_flush_table("//tmp/t")
        wait(lambda: len(self._find_tablet_orchid(address, tablet_id)["partitions"]) > 1)

    @authors("ifsmirnov")
    def test_partitioning_with_alter(self):
        # Creating two chunks with @eden=%false:
        # - [1]
        # - [1;1]
        # Two partitions should be created upon mount.
        sync_create_cells(1)
        schema = [
            {"name": "k1", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "int64"},
        ]
        self._create_simple_table("//tmp/t", schema=schema)
        set("//tmp/t/@min_partition_data_size", 1)

        # Create [1] chunk.
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"k1": 1}])
        sync_unmount_table("//tmp/t")
        set("//tmp/t/@forced_compaction_revision", 1)
        chunk_id = get("//tmp/t/@chunk_ids/0")
        sync_mount_table("//tmp/t")
        wait(lambda: get("//tmp/t/@chunk_ids/0") != chunk_id)
        assert not get("#{}/@eden".format(get("//tmp/t/@chunk_ids/0")))

        # Create [1;1] chunk.
        sync_unmount_table("//tmp/t")
        schema = schema[:1] + [{"name": "k2", "type": "int64", "sort_order": "ascending"}] + schema[1:]
        alter_table("//tmp/t", schema=schema)
        sync_reshard_table("//tmp/t", [[], [1,1]])
        sync_mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=1)
        insert_rows("//tmp/t", [{"k1": 1, "k2": 1}])
        sync_unmount_table("//tmp/t")
        set("//tmp/t/@forced_compaction_revision", 1)
        chunk_id = get("//tmp/t/@chunk_ids/1")
        sync_mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=1)
        wait(lambda: get("//tmp/t/@chunk_ids/1") != chunk_id)
        assert not get("#{}/@eden".format(get("//tmp/t/@chunk_ids/1")))

        sync_unmount_table("//tmp/t")
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_reshard_table("//tmp/t", [[]])
        sync_mount_table("//tmp/t")
        wait(lambda: get("//tmp/t/@tablet_statistics/partition_count") > 0)
        assert get("//tmp/t/@tablet_statistics/partition_count") == 2

        expected = [
            {"k1": 1L, "k2": yson.YsonEntity(), "value": yson.YsonEntity()},
            {"k1": 1L, "k2": 1L, "value": yson.YsonEntity()},
        ]

        assert list(lookup_rows("//tmp/t", [{"k1": 1}, {"k1": 1, "k2": 1}])) == expected
        assert list(select_rows("* from [//tmp/t] order by k1,k2 limit 100")) == expected

    @authors("akozhikhov")
    def test_overlapping_store_count(self):
        # This magic combination of parameters makes flushed insertion to be stored in eden stores
        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            max_partition_data_size=640,
            desired_partition_data_size=512,
            min_partition_data_size=256,
            compression_codec="none",
            chunk_writer={"block_size": 1},
        )
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)

        def _check(stores, overlaps):
            orchid = self._find_tablet_orchid(address, tablet_id)
            assert stores == len(orchid["eden"]["stores"])
            wait(lambda: overlaps == get("//tmp/t/@tablet_statistics/overlapping_store_count"))
        _check(stores=1, overlaps=1)

        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in xrange(12)])
        sync_flush_table("//tmp/t")
        _check(stores=2, overlaps=2)

        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in xrange(12, 24)])
        sync_flush_table("//tmp/t")
        _check(stores=3, overlaps=2)

    @authors("babenko")
    def test_store_rotation(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        set("//tmp/t/@max_dynamic_store_row_count", 10)
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)

        rows = [{"key": i, "value": str(i)} for i in xrange(10)]
        insert_rows("//tmp/t", rows)

        def check():
            tablet_data = self._find_tablet_orchid(address, tablet_id)
            return len(tablet_data["eden"]["stores"]) == 1 and \
                   len(tablet_data["partitions"]) == 1 and \
                   len(tablet_data["partitions"][0]["stores"]) == 1
        wait(lambda: check())

    @authors("akozhikhov")
    def test_small_chunks_partition_scenario(self):
        # Create three chunks and check number of partitions.
        sync_create_cells(1)
        schema = make_schema(
            [
                {"name": "k1", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"}
            ],
            unique_keys=True)
        create("table", "//tmp/t", attributes={"schema": schema})

        for key in range(3):
            write_table("<append=%true>//tmp/t", [{"k1": key}])

        alter_table("//tmp/t", dynamic=True)

        def _check(expected_partitions):
            sync_mount_table("//tmp/t")
            wait(lambda: get("//tmp/t/@tablet_statistics/partition_count") > 0)
            assert get("//tmp/t/@tablet_statistics/partition_count") == expected_partitions
            sync_unmount_table("//tmp/t")

        set("//tmp/t/@enable_compaction_and_partitioning", False)
        _check(expected_partitions=1)
        set("//tmp/t/@min_partition_data_size", 1)
        _check(expected_partitions=3)

    @authors("akozhikhov")
    def test_partitioning_with_chunk_views(self):
        # Creating two chunks [{1}, {2}] and [{2}, {3}] and check whether they become partitioned.
        sync_create_cells(1)

        def _wait_not_in_eden(chunk_index):
            chunk_id = get("//tmp/t/@chunk_ids/{0}".format(chunk_index))
            sync_mount_table("//tmp/t", first_tablet_index=chunk_index, last_tablet_index=chunk_index)
            wait(lambda: chunk_id != get("//tmp/t/@chunk_ids/{0}".format(chunk_index)))
            print "\n\n\n", chunk_id, get("//tmp/t/@chunk_ids"), "\n\n"
            assert not get("#{}/@eden".format(get("//tmp/t/@chunk_ids/{0}".format(chunk_index))))
            sync_unmount_table("//tmp/t")

        self._create_simple_table("//tmp/t", )
        set("//tmp/t/@min_partition_data_size", 1)

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1}, {"key": 2}])
        sync_unmount_table("//tmp/t")
        set("//tmp/t/@forced_compaction_revision", 1)

        _wait_not_in_eden(chunk_index=0)

        sync_reshard_table("//tmp/t", [[], [2]])

        sync_mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=1)
        insert_rows("//tmp/t", [{"key": 2}, {"key": 3}])
        sync_unmount_table("//tmp/t")
        set("//tmp/t/@forced_compaction_revision", 1)

        _wait_not_in_eden(chunk_index=1)

        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_reshard_table("//tmp/t", [[]])

        sync_mount_table("//tmp/t")
        wait(lambda: get("//tmp/t/@tablet_statistics/partition_count") > 0)
        assert get("//tmp/t/@tablet_statistics/partition_count") == 2
        sync_unmount_table("//tmp/t")

        assert len(get("//tmp/t/@chunk_ids")) == 2

        expected = [
            {"pivot_key": [], "min_key": [1], "upper_bound_key": [2]},
            {"pivot_key": [2], "min_key": [2], "upper_bound_key": [3, yson.YsonEntity()]},
        ]

        sync_mount_table("//tmp/t", )
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)
        orchid = self._find_tablet_orchid(address, tablet_id)
        for partition_idx in range(len(orchid["partitions"])):
            partition = orchid["partitions"][partition_idx]
            assert partition["pivot_key"] == expected[partition_idx]["pivot_key"]
            assert len(partition["stores"]) == 1
            store_values = dict(partition["stores"].values()[0].iteritems())
            assert store_values["min_key"] == expected[partition_idx]["min_key"]
            assert store_values["upper_bound_key"] == expected[partition_idx]["upper_bound_key"]


################################################################################

class TestCompactionPartitioningMulticell(TestCompactionPartitioning):
    NUM_SECONDARY_MASTER_CELLS = 2
