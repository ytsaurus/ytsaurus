from .test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_commands import (
    authors, wait, create, get, set, insert_rows, delete_rows, select_rows, lookup_rows,
    alter_table, write_table,
    remount_table, get_tablet_leader_address, sync_create_cells, sync_mount_table, sync_unmount_table,
    sync_reshard_table, sync_flush_table, build_snapshot, sorted_dicts, sync_compact_table,
    sync_freeze_table, sync_unfreeze_table, get_singular_chunk_id,
    set_node_banned, disable_write_sessions_on_node, update_nodes_dynamic_config)

from yt.common import YtError

from yt_type_helpers import make_schema

from yt_helpers import profiler_factory

import yt.yson as yson

import pytest

import builtins

from time import sleep, time

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

        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in range(20)])
        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [1], [18]])
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/1/tablet_id")
        address = get_tablet_leader_address(tablet_id)
        orchid = self._find_tablet_orchid(address, tablet_id)
        assert len(orchid["partitions"]) == 1

        build_snapshot(cell_id=cell_id)

        peer = get("//sys/tablet_cells/{}/@peers/0/address".format(cell_id))
        set_node_banned(peer, True)
        wait(lambda: get("//sys/tablet_cells/{}/@health".format(cell_id)) == "good")

        set("//tmp/t/@enable_compaction_and_partitioning", True)
        remount_table("//tmp/t")
        address = get_tablet_leader_address(tablet_id)
        wait(lambda: len(self._find_tablet_orchid(address, tablet_id)["partitions"]) > 1)

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

        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in range(16)])
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
        sync_reshard_table("//tmp/t", [[], [1, 1]])
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
            {"k1": 1, "k2": yson.YsonEntity(), "value": yson.YsonEntity()},
            {"k1": 1, "k2": 1, "value": yson.YsonEntity()},
        ]

        assert list(lookup_rows("//tmp/t", [{"k1": 1}, {"k1": 1, "k2": 1}])) == expected
        assert list(select_rows("* from [//tmp/t] order by k1,k2 limit 100")) == expected

    @authors("akozhikhov")
    def test_overlapping_store_count(self):
        # Create 3 chunks [{2}, {3}], [{4}, {5}] and [{6}, {7}].
        # Then create two chunks in eden [{3}, {4}] and [{5}, {6}], which don't overlap.
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", attributes={"enable_lsm_verbose_logging": True})

        sync_mount_table("//tmp/t")

        def _check(stores, overlaps):
            sync_mount_table("//tmp/t")
            tablet_id = get("//tmp/t/@tablets/0/tablet_id")
            address = get_tablet_leader_address(tablet_id)
            wait(lambda: stores == len(self._find_tablet_orchid(address, tablet_id)["eden"]["stores"]))
            wait(lambda: overlaps == get("//tmp/t/@tablet_statistics/overlapping_store_count"))
            sync_unmount_table("//tmp/t")

        _check(stores=1, overlaps=1)

        self._create_partitions(partition_count=3)

        _check(stores=1, overlaps=2)

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i} for i in range(3, 5)])
        sync_flush_table("//tmp/t")

        _check(stores=2, overlaps=3)

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i} for i in range(5, 7)])
        sync_flush_table("//tmp/t")

        _check(stores=3, overlaps=3)

    @authors("babenko")
    def test_store_rotation(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        set("//tmp/t/@max_dynamic_store_row_count", 10)
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)

        rows = [{"key": i, "value": str(i)} for i in range(10)]
        insert_rows("//tmp/t", rows)

        def check():
            tablet_data = self._find_tablet_orchid(address, tablet_id)
            return (
                len(tablet_data["eden"]["stores"]) == 1
                and len(tablet_data["partitions"]) == 1
                and len(tablet_data["partitions"][0]["stores"]) == 1
            )

        wait(lambda: check())

    @pytest.mark.flaky(max_runs=5)
    @authors("ifsmirnov")
    @pytest.mark.parametrize("sorted", [True, False])
    def test_periodic_rotation(self, sorted):
        sync_create_cells(1)
        if sorted:
            self._create_simple_table("//tmp/t")
        else:
            self._create_simple_table("//tmp/t", schema=[{"name": "key", "type": "int64"}])

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        def _get_last_rotated():
            return get(f"//sys/tablets/{tablet_id}/orchid/last_periodic_rotation_time")

        set("//tmp/t/@dynamic_store_auto_flush_period", 7000)
        set("//tmp/t/@dynamic_store_flush_period_splay", 0)
        set("//tmp/t/@enable_dynamic_store_read", False)
        sync_mount_table("//tmp/t")

        # Rotation attempts are recorded even in case of empty dynamic store.
        last_rotated = _get_last_rotated()
        wait(lambda: _get_last_rotated() != last_rotated)

        rows = [{"key": i} for i in range(10)]
        insert_rows("//tmp/t", rows)

        sleep(2)
        assert get("//tmp/t/@chunk_count") == 0

        wait(lambda: get("//tmp/t/@chunk_count") == 1)

    @authors("akozhikhov")
    def test_small_chunks_partition_scenario(self):
        # Create three chunks and check number of partitions.
        sync_create_cells(1)
        schema = make_schema(
            [
                {"name": "k1", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
            unique_keys=True,
        )
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
        # Creating two chunks [{0}, {1}, {2}] and [{2}, {3}] and check whether they become partitioned.
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", enable_lsm_verbose_logging=True)

        self._create_partitions(partition_count=2, do_overlap=True)

        sync_mount_table("//tmp/t")
        wait(lambda: get("//tmp/t/@tablet_statistics/partition_count") > 0)
        assert get("//tmp/t/@tablet_statistics/partition_count") == 2
        sync_unmount_table("//tmp/t")

        assert len(get("//tmp/t/@chunk_ids")) == 2

        expected = [
            {"pivot_key": [], "min_key": [0], "upper_bound_key": [2]},
            {
                "pivot_key": [2],
                "min_key": [2],
                "upper_bound_key": [3, yson.YsonEntity()],
            },
        ]

        sync_mount_table(
            "//tmp/t",
        )
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)
        orchid = self._find_tablet_orchid(address, tablet_id)
        for partition_idx in range(len(orchid["partitions"])):
            partition = orchid["partitions"][partition_idx]
            assert partition["pivot_key"] == expected[partition_idx]["pivot_key"]
            assert len(partition["stores"]) == 1
            store_values = dict(iter(list(partition["stores"].values())[0].items()))
            assert store_values["min_key"] == expected[partition_idx]["min_key"]
            assert store_values["upper_bound_key"] == expected[partition_idx]["upper_bound_key"]

    @authors("ifsmirnov")
    def test_mount_chunk_view_YT_12532(self):
        sync_create_cells(1)
        schema = make_schema(
            [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
            unique_keys=True,
        )
        self._create_simple_table("//tmp/t", dynamic=False, schema=schema)

        # Chunks:     [0 1 2] [3 4 5] [6 7 8]
        # Tablet #1:     [       |       )

        write_table("<append=%true>//tmp/t", [{"key": i} for i in range(0, 3)])
        write_table("<append=%true>//tmp/t", [{"key": i} for i in range(3, 6)])
        write_table("<append=%true>//tmp/t", [{"key": i} for i in range(6, 9)])

        alter_table("//tmp/t", dynamic=True)
        set("//tmp/t/@min_partition_data_size", 1)
        sync_reshard_table("//tmp/t", [[], [1], [4], [7]])
        sync_reshard_table("//tmp/t", [[1]], first_tablet_index=1, last_tablet_index=2)
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/1/tablet_id")
        address = get_tablet_leader_address(tablet_id)
        orchid = self._find_tablet_orchid(address, tablet_id)
        partitions = orchid["partitions"]
        assert len(partitions) == 3
        assert partitions[0]["pivot_key"] == [1]
        assert partitions[1]["pivot_key"] == [3]
        assert partitions[2]["pivot_key"] == [6]

    @authors("ifsmirnov")
    def test_expired_partition(self):
        cell_id = sync_create_cells(1)[0]

        schema = make_schema(
            [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
            unique_keys=True,
        )
        self._create_simple_table(
            "//tmp/t",
            schema=schema,
            dynamic=False,
            min_data_ttl=15000,
            max_data_ttl=15000,
            min_data_versions=0,
            max_data_versions=1,
            # NB: compressed size of each chunk is 38.
            min_partition_data_size=60,
            desired_partition_data_size=75,
            max_partition_data_size=90,
            min_compaction_store_count=10,
            max_compaction_store_count=10,
        )

        start_time = time()

        def _sleep_until(instant):
            now = time()
            if now - start_time < instant:
                sleep(instant - (now - start_time))

        rows = [{"key": i, "value": str(i)} for i in range(6)]

        # Expire at 15.
        write_table("<append=%true>//tmp/t", [rows[0]])
        write_table("<append=%true>//tmp/t", [rows[1]])
        _sleep_until(5)
        # Expire at 20.
        write_table("<append=%true>//tmp/t", [rows[2]])
        write_table("<append=%true>//tmp/t", [rows[3]])
        _sleep_until(10)
        # Expire at 25.
        write_table("<append=%true>//tmp/t", [rows[4]])
        write_table("<append=%true>//tmp/t", [rows[5]])

        alter_table("//tmp/t", dynamic=True)
        sync_mount_table("//tmp/t")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)

        def _get_partitions():
            return get(
                "//sys/cluster_nodes/{}/orchid/tablet_cells/{}/tablets/{}/partitions".format(
                    address, cell_id, tablet_id
                )
            )

        expected_partitions = [[2, 2, 2], [2, 2], [2], []]
        expected_values = [rows, rows[2:], rows[4:], []]

        def _check(expected, actual):
            while expected and expected[0] != actual:
                expected.pop(0)
            if not expected:
                assert False

        for deadline in (0, 18, 23):
            _sleep_until(deadline)
            # We may encounter an empty partition if its chunks were dropped
            # but the partition itself has not yet been split.
            _check(
                expected_partitions,
                [len(x["stores"]) for x in _get_partitions() if x["stores"]],
            )
            _check(expected_values, sorted_dicts(list(select_rows("* from [//tmp/t]"))))

    @authors("ifsmirnov")
    def test_eden_store_ids_forced_unmount(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@min_partition_data_size", 1)
        set("//tmp/t/@desired_partition_data_size", 2)
        set("//tmp/t/@min_partitioning_store_count", 100)
        set("//tmp/t/@max_partitioning_store_count", 200)
        sync_reshard_table("//tmp/t", [[], [2], [4], [6]])
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i} for i in (1, 2, 4, 6)])
        sync_compact_table("//tmp/t")
        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[]])
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        assert len(get(f"//sys/tablets/{tablet_id}/orchid/partitions")) == 4
        assert len(get(f"//sys/tablets/{tablet_id}/orchid/eden/stores")) == 1

        for i in range(3):
            insert_rows("//tmp/t", [{"key": i} for i in (0, 3, 5, 7)])
            sync_freeze_table("//tmp/t")
            sync_unfreeze_table("//tmp/t")

        # Wait until three stores in Eden compact to one and lose |eden| flag in meta.
        wait(lambda: len(get(f"//sys/tablets/{tablet_id}/orchid/eden/stores")) == 2)
        assert len(get(f"//sys/tablets/{tablet_id}/orchid/partitions")) == 4

        sync_unmount_table("//tmp/t", force=True)
        sync_mount_table("//tmp/t")
        assert len(get(f"//sys/tablets/{tablet_id}/orchid/partitions")) == 4
        assert len(get(f"//sys/tablets/{tablet_id}/orchid/eden/stores")) == 2

    @authors("ifsmirnov")
    def test_compaction_cancelled(self):
        cell_id = sync_create_cells(1)[0]
        cell_node = get(f"#{cell_id}/@peers/0/address")
        disable_write_sessions_on_node(cell_node, "test compaction cancelation")

        self._create_simple_table("//tmp/t", replication_factor=1)
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1}])
        sync_flush_table("//tmp/t")

        profiler = profiler_factory().at_node(cell_node)

        def _get_running_compaction_count():
            return profiler.get("tablet_node/store_compactor/running_compactions")

        chunk_id = get_singular_chunk_id("//tmp/t")
        chunk_node = get(f"#{chunk_id}/@stored_replicas/0")
        set_node_banned(chunk_node, True)

        assert _get_running_compaction_count() == 0.0
        set("//tmp/t/@auto_compaction_period", 1)
        remount_table("//tmp/t")

        wait(lambda: _get_running_compaction_count() == 1.0)
        sleep(1)
        assert get_singular_chunk_id("//tmp/t") == chunk_id

        sync_unmount_table("//tmp/t")
        wait(lambda: _get_running_compaction_count() == 0.0)
        assert get_singular_chunk_id("//tmp/t") == chunk_id

    @authors("zvank")
    def test_delay_between_split_and_merge(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        # Such numbers are chosen that greater chunk is less than max partition data size,
        # but both chunks combined are greater.
        set("//tmp/t/@max_partition_data_size", 74000)
        set("//tmp/t/@desired_partition_data_size", 63000)
        set("//tmp/t/@min_partition_data_size", 6400)
        set("//tmp/t/@chunk_writer", {"block_size": 64})
        set("//tmp/t/@compression_codec", "none")
        set("//tmp/t/@dynamic_store_auto_flush_period", yson.YsonEntity())
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)
        orchid = self._find_tablet_orchid(address, tablet_id)
        assert len(orchid["partitions"]) == 1

        # We want the partition to split right in between these two chunks.
        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in range(24)])
        sync_flush_table("//tmp/t")

        # 73124 bytes.
        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in range(24, 1000)])
        sync_flush_table("//tmp/t")

        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [50]])

        initial_chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(initial_chunk_ids) == 3
        set("//tmp/t/@forced_compaction_revision", 1)
        sync_mount_table("//tmp/t", first_tablet_index=0, last_tablet_index=0)

        def check():
            chunk_ids = get("//tmp/t/@chunk_ids")
            intersection = builtins.set(chunk_ids[:-1]) & builtins.set(initial_chunk_ids[:-1])
            return len(intersection) == 0

        wait(check)

    @authors("ifsmirnov")
    def test_timestamp_digest_too_many_delete_timestamps(self):
        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            mount_config={
                "backing_store_retention_time": 0,
                "dynamic_store_auto_flush_period": 1000,
                "dynamic_store_flush_period_splay": 0,
                "row_digest_compaction": {
                    "period": yson.YsonEntity(),
                },
                "min_data_ttl": 0,
                "compaction_data_size_base": 16 * 2**20,
                "enable_lsm_verbose_logging": True,
            },
            chunk_writer={
                "versioned_row_digest": {
                    "enable": True,
                }
            },
            enable_dynamic_store_read=False,
            compression_codec="none"
        )
        sync_mount_table("//tmp/t")

        update_nodes_dynamic_config({
            "tablet_node": {
                "store_compactor": {
                    "use_row_digests": True,
                }
            }
        })

        # Create a large chunk that will not be compacted will smaller ones and thus will prevent
        # purging delete tombstones by major timestamp.
        insert_rows("//tmp/t", [{"key": 1, "value": "a" * 16 * 2**20}])

        wait(lambda: get("//tmp/t/@chunk_ids"))
        large_chunk_id = get("//tmp/t/@chunk_ids")[0]

        # Insert many delete timestamps and see how chunks are compacted but to no avail.
        start_time = time()
        delete_ts_count = 0
        while time() - start_time < 10:
            delete_rows("//tmp/t", [{"key": 1}])
            delete_ts_count += 1

        lookup_result = lookup_rows("//tmp/t", [{"key": 1}], versioned=True, verbose=False, column_names=[])
        delete_timestamps = lookup_result[0].attributes["delete_timestamps"]

        assert delete_ts_count == len(delete_timestamps)
        assert large_chunk_id in get("//tmp/t/@chunk_ids")
        assert len(get("//tmp/t/@chunk_ids")) > 1

        set("//tmp/t/@mount_config/row_digest_compaction", {
            "max_obsolete_timestamp_ratio": 1,
            "max_timestamps_per_value": delete_ts_count // 4,
        })
        remount_table("//tmp/t")

        wait(lambda: large_chunk_id not in get("//tmp/t/@chunk_ids"))

        lookup_result = lookup_rows("//tmp/t", [{"key": 1}], versioned=True, verbose=False, column_names=[])
        if lookup_result:
            delete_timestamps = lookup_result[0].attributes["delete_timestamps"]
            assert len(delete_timestamps) < delete_ts_count

    @authors("ifsmirnov")
    def test_flush_to_eden(self):
        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            mount_config={
                "always_flush_to_eden": True,
            })
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1}])
        sync_flush_table("//tmp/t")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        chunk_id = get_singular_chunk_id("//tmp/t")

        eden_stores = get(f"//sys/tablets/{tablet_id}/orchid/eden/stores")
        partition_stores = get(f"//sys/tablets/{tablet_id}/orchid/partitions/0/stores")
        assert chunk_id in eden_stores
        assert len(partition_stores) == 0

    @authors("ifsmirnov")
    def test_lsm_statistics(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@max_dynamic_store_row_count", 1)
        set("//tmp/t/@auto_compaction_period", 1)
        set("//tmp/t/@backing_store_retention_time", 0)
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 1, "value": "1"}])
        wait(lambda: len(get("//tmp/t/@chunk_ids")) == 1)

        tablet_id = get("//tmp/t/@tablets")[0]["tablet_id"]
        wait(lambda: get(f"//sys/tablets/{tablet_id}/orchid/lsm_statistics/pending_compaction_store_count/periodic") == 1)

    @authors("alexelexa")
    def test_narrow_chunk_view_compaction(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        set("//tmp/t/@chunk_writer", {"block_size": 1})
        set("//tmp/t/@tablet_balancer_config/enable_auto_reshard", False)
        set("//tmp/t/@enable_compaction_and_partitioning", False)

        sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": 'a' * 66000} for i in range(10)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        assert len(get("//tmp/t/@chunk_ids")) == 1
        size = get("//tmp/t/@compressed_data_size")

        sync_unmount_table("//tmp/t")

        sync_reshard_table(
            "//tmp/t",
            3,
            enable_slicing=True)

        tablet_ids = [tablet["tablet_id"] for tablet in get("//tmp/t/@tablets")]

        sync_mount_table("//tmp/t")

        def is_chunk_view(store_id):
            type = store_id.split("-")[2][-4:].lstrip("0")
            return type == "7b"

        def check():
            def has_compaction_hint(store_orchid):
                return "compaction_hint" in store_orchid.get("compaction_hints")["chunk_view_size"]

            try:
                for tablet_id in tablet_ids:
                    tablet_orchid = self._find_tablet_orchid(get_tablet_leader_address(tablet_id), tablet_id)
                    for partition in tablet_orchid["partitions"]:
                        for store_id, store_orchid in partition["stores"].items():
                            if not is_chunk_view(store_id):
                                continue
                            if not has_compaction_hint(store_orchid):
                                return False

                    for store_id, store_orchid in tablet_orchid["eden"]["stores"].items():
                        if not is_chunk_view(store_id):
                            continue
                        if not has_compaction_hint(store_orchid):
                            return False
                return True
            except YtError:
                return False

        wait(check)

        set("//tmp/t/@enable_compaction_and_partitioning", True)
        remount_table("//tmp/t")

        wait(lambda: all(get(f'#{tablet_id}/@statistics/compressed_data_size') < size for tablet_id in tablet_ids))

    @authors("dave11ar")
    def test_timestamp_digest_ttl_cleanup_expected(self):
        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            mount_config={
                "row_digest_compaction": {
                    "period": yson.YsonEntity(),
                },
                "min_data_ttl": 1000,
                "max_data_ttl": 1000
            },
            chunk_writer={
                "versioned_row_digest": {
                    "enable": True,
                }
            },
            compression_codec="none"
        )
        sync_mount_table("//tmp/t")

        update_nodes_dynamic_config({
            "tablet_node": {
                "store_compactor": {
                    "use_row_digests": True,
                }
            }
        })

        def check(min_data_versions, max_data_versions, max_obsolete_timestamp_ratio):
            for i in range(10):
                insert_rows("//tmp/t", [{"key": 1, "value": "a" * i * 2**20}])

            sync_flush_table("//tmp/t")

            wait(lambda: get("//tmp/t/@chunk_ids"))
            chunk_id = get("//tmp/t/@chunk_ids")[0]

            set("//tmp/t/@min_data_versions", min_data_versions)
            set("//tmp/t/@max_data_versions", max_data_versions)
            set("//tmp/t/@mount_config/row_digest_compaction", {
                "max_obsolete_timestamp_ratio": max_obsolete_timestamp_ratio,
            })
            remount_table("//tmp/t")

            wait(lambda: chunk_id not in get("//tmp/t/@chunk_ids"))

        check(1, 1, 0.89)
        check(0, 1, 0.99)
        check(0, 0, 0.99)

    @authors("dave11ar")
    def test_compaction_hints_after_cell_moved(self):
        cell_id = sync_create_cells(1)[0]

        self._create_simple_table(
            "//tmp/t",
            mount_config={
                "row_digest_compaction": {
                    "max_obsolete_timestamp_ratio": 0,
                },
                "min_data_ttl": 5000,
                "max_data_ttl": 5000,
            },
            chunk_writer={
                "versioned_row_digest": {
                    "enable": True,
                }
            },
            compression_codec="none"
        )
        sync_mount_table("//tmp/t")

        update_nodes_dynamic_config({
            "tablet_node": {
                "store_compactor": {
                    "row_digest_fetch_period": 5000,
                    "row_digest_request_throttler": {
                        "limit": 0.
                    },
                    "use_row_digests": True,
                }
            }
        })
        for i in range(10):
            insert_rows("//tmp/t", [{"key": 1, "value": "a" * i * 1000}])

        sync_flush_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets")[0]["tablet_id"]

        def check_no_hint():
            store_orchid = self._find_tablet_orchid(
                get_tablet_leader_address(tablet_id),
                tablet_id)["partitions"][0]["stores"][chunk_id]

            row_digest = store_orchid.get("compaction_hints")["row_digest"]
            return "compaction_hint" not in row_digest

        wait(lambda: get("//tmp/t/@chunk_ids"))
        chunk_id = get("//tmp/t/@chunk_ids")[0]

        peer = get("//sys/tablet_cells/{}/@peers/0/address".format(cell_id))
        set_node_banned(peer, True)

        wait(lambda: get("//sys/tablet_cells/{}/@health".format(cell_id)) == "good")

        update_nodes_dynamic_config({
            "tablet_node": {
                "store_compactor": {
                    "row_digest_fetch_period": 5000,
                    "row_digest_request_throttler": {
                        "limit": 10000,
                    },
                    "use_row_digests": True,
                }
            }
        })

        wait(lambda: get("//tmp/t/@chunk_ids"))
        assert chunk_id in get("//tmp/t/@chunk_ids")
        assert check_no_hint()

        wait(lambda: chunk_id not in get("//tmp/t/@chunk_ids"))


################################################################################


class TestCompactionPartitioningMulticell(TestCompactionPartitioning):
    NUM_SECONDARY_MASTER_CELLS = 2
