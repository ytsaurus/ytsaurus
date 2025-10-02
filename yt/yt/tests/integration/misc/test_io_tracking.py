from yt_io_tracking_base import TestIOTrackingBase

import yt_commands

from yt_commands import (
    alter_table, authors, create, get, read_journal, wait, read_table, write_file, write_journal, create_account,
    write_table, update_nodes_dynamic_config, get_singular_chunk_id, set_node_banned, sort, get_operation,
    sync_create_cells, create_dynamic_table, sync_mount_table, insert_rows, sync_unmount_table, ls, print_debug,
    reduce, map_reduce, merge, erase, read_file, sorted_dicts, get_driver, remote_copy, create_pool_tree, create_pool)

from yt_helpers import write_log_barrier
from yt_driver_bindings import Driver

import yt.yson as yson

from copy import deepcopy

import os
import random
import time
import pytest

##################################################################


class TestNodeIOTrackingBase(TestIOTrackingBase):
    def read_raw_events(self, *args, **kwargs):
        return self._read_events("IORaw", *args, **kwargs)

    def generate_large_data(self, row_len=10000, row_count=5, column_count=1):
        rnd = random.Random(42)
        # NB. The values are chosen in such a way so they cannot be compressed or deduplicated.
        column_len = row_len // column_count
        large_data = []
        large_data_size = 0
        for row_index in range(row_count):
            row = dict(id=row_index)
            for column_index in range(column_count):
                value = bytes(bytearray(rnd.randint(0, 255) for _ in range(column_len)))
                row[f"data_{column_index}"] = value
                large_data_size += len(value)
            large_data.append(row)
        return large_data, large_data_size

    def generate_large_journal(self, row_len=10000, row_count=5):
        rnd = random.Random(42)
        # NB. The values are chosen in such a way so they cannot be compressed or deduplicated.
        large_journal = [{"payload": bytes(bytearray([rnd.randint(0, 255) for _ in range(row_len)]))} for _ in range(row_count)]
        large_journal_size = row_count * row_len
        return large_journal, large_journal_size

##################################################################


class TestDataNodeIOTracking(TestNodeIOTrackingBase):
    ENABLE_MULTIDAEMON = False  # Check structured logs.
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1,
            "default_journal_read_quorum": 1,
            "default_journal_write_quorum": 1,
            "default_journal_replication_factor": 1,
        }
    }

    @authors("gepardo")
    def test_simple_write(self):
        from_barrier = self.write_log_barrier(self.get_node_address())
        create("table", "//tmp/table")
        write_table("//tmp/table", [{"a": 1, "b": 2, "c": 3}])
        raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier)
        aggregate_events = self.wait_for_aggregate_events(count=1, from_barrier=from_barrier)
        chunk_id = get("//tmp/table/@chunk_ids")[0]

        assert raw_events[0]["data_node_method@"] == "FinishChunk"
        assert raw_events[0]["direction@"] == "write"
        assert raw_events[0]["chunk_id"] == chunk_id
        for counter in ["bytes", "io_requests"]:
            assert raw_events[0][counter] > 0 and raw_events[0][counter] == aggregate_events[0][counter]

    @authors("gepardo")
    def test_path_aggregation(self):
        from_barrier = self.write_log_barrier(self.get_node_address())
        create("table", "//tmp/table1")
        write_table("//tmp/table1", [{"a": 1, "b": 2, "c": 3}])
        assert read_table("//tmp/table1") == [{"a": 1, "b": 2, "c": 3}]
        write_table("//tmp/table1", [{"a": 4, "b": 5, "c": 6}])
        assert read_table("//tmp/table1") == [{"a": 4, "b": 5, "c": 6}]
        create("table", "//tmp/table2")
        write_table("//tmp/table2", [{"a": 1, "b": 2, "c": 3}])
        write_table("<append=%true>//tmp/table2", [{"a": 2, "b": 3, "c": 4}])
        raw_events = self.wait_for_raw_events(count=6, from_barrier=from_barrier)
        path_aggr_events = self.wait_for_path_aggr_events(count=3, from_barrier=from_barrier)

        sum_bytes = 0
        sum_io_requests = 0
        for event in raw_events:
            assert event["bytes"] > 0
            assert event["io_requests"] > 0
            sum_bytes += event["bytes"]
            sum_io_requests += event["io_requests"]

        sum_bytes_aggr = 0
        sum_io_requests_aggr = 0
        for event in path_aggr_events:
            assert event["bytes"] > 0
            assert event["io_requests"] > 0
            sum_bytes_aggr += event["bytes"]
            sum_io_requests_aggr += event["io_requests"]

        assert sum_bytes == sum_bytes_aggr
        assert sum_io_requests == sum_io_requests_aggr

        events = [
            {"object_path": event["object_path"], "tags": event["tags"]}
            for event in path_aggr_events
        ]

        def flatten(src_dict):
            result = []
            for key, value in src_dict.items():
                if isinstance(value, dict):
                    for subkey, subvalue in flatten(value):
                        result.append((key + "/" + subkey, subvalue))
                    continue
                result.append((key, value))
            return result

        def deep_sorted_dicts(list_of_dicts):
            return sorted(list_of_dicts, key=lambda dict: sorted(flatten(dict)))

        expected_events = [
            {"object_path": "//tmp/table1", "tags": {"direction@": "read", "account@": "tmp", "user@": "root"}},
            {"object_path": "//tmp/table1", "tags": {"direction@": "write", "account@": "tmp", "user@": "root"}},
            {"object_path": "//tmp/table2", "tags": {"direction@": "write", "account@": "tmp", "user@": "root"}},
        ]
        assert deep_sorted_dicts(events) == deep_sorted_dicts(expected_events)

    @authors("gepardo")
    def test_two_chunks(self):
        from_barrier = self.write_log_barrier(self.get_node_address())
        create("table", "//tmp/table")
        write_table("//tmp/table", [{"number": 42, "good": True}])
        write_table("<append=%true>//tmp/table", [{"number": 43, "good": False}])
        raw_events = self.wait_for_raw_events(count=2, from_barrier=from_barrier)
        aggregate_events = self.wait_for_aggregate_events(count=1, from_barrier=from_barrier)
        chunk_ids = get("//tmp/table/@chunk_ids")

        assert raw_events[0]["data_node_method@"] == "FinishChunk"
        assert raw_events[0]["direction@"] == "write"
        assert raw_events[0]["chunk_id"] in chunk_ids
        assert raw_events[1]["data_node_method@"] == "FinishChunk"
        assert raw_events[1]["direction@"] == "write"
        assert raw_events[1]["chunk_id"] in chunk_ids
        for counter in ["bytes", "io_requests"]:
            assert raw_events[0][counter] > 0
            assert raw_events[1][counter] > 0
            assert raw_events[0][counter] + raw_events[1][counter] == aggregate_events[0][counter]
        assert raw_events[0]["chunk_id"] != raw_events[1]["chunk_id"]

    @authors("gepardo")
    def test_read_table(self):
        from_barrier = self.write_log_barrier(self.get_node_address())
        create("table", "//tmp/table", attributes={"compression_codec": "zlib_5"})
        write_table("//tmp/table", [{"a": 1, "b": 2, "c": 3}])
        assert read_table("//tmp/table") == [{"a": 1, "b": 2, "c": 3}]
        raw_events = self.wait_for_raw_events(count=2, from_barrier=from_barrier)
        chunk_id = get("//tmp/table/@chunk_ids")[0]

        assert raw_events[0]["data_node_method@"] == "FinishChunk"
        assert raw_events[0]["direction@"] == "write"
        assert raw_events[0]["chunk_id"] == chunk_id
        assert raw_events[0]["location_type@"] == "store"
        assert "location_id" in raw_events[0]
        assert raw_events[0]["compression_codec@"] == "zlib_5"
        assert raw_events[0]["erasure_codec@"] == "none"

        assert raw_events[1]["data_node_method@"] == "GetBlockSet"
        assert raw_events[1]["direction@"] == "read"
        assert raw_events[1]["chunk_id"] == chunk_id
        assert raw_events[1]["location_type@"] == "store"
        assert "location_id" in raw_events[1]
        assert raw_events[1]["compression_codec@"] == "zlib_5"
        assert raw_events[1]["erasure_codec@"] == "none"

        for counter in ["bytes", "io_requests"]:
            assert raw_events[0][counter] > 0
            assert raw_events[1][counter] > 0

    @authors("gepardo")
    def test_large_data(self):
        create("table", "//tmp/table")

        for i in range(10):
            large_data, large_data_size = self.generate_large_data()

            old_disk_space = get("//tmp/table/@resource_usage/disk_space")
            from_barrier = self.write_log_barrier(self.get_node_address())
            write_table("<append=%true>//tmp/table", large_data, verbose=False)
            raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier)
            new_disk_space = get("//tmp/table/@resource_usage/disk_space")

            min_data_bound = 0.95 * large_data_size
            max_data_bound = 1.05 * (new_disk_space - old_disk_space)

            assert raw_events[0]["data_node_method@"] == "FinishChunk"
            assert raw_events[0]["direction@"] == "write"
            assert min_data_bound <= raw_events[0]["bytes"] <= max_data_bound
            assert raw_events[0]["io_requests"] > 0

            from_barrier = self.write_log_barrier(self.get_node_address())
            assert read_table("//tmp/table[#{}:]".format(i * len(large_data)), verbose=False) == large_data
            raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier)

            assert raw_events[0]["data_node_method@"] == "GetBlockSet"
            assert raw_events[0]["direction@"] == "read"
            assert min_data_bound <= raw_events[0]["bytes"] <= max_data_bound
            assert raw_events[0]["io_requests"] > 0

    @authors("gepardo")
    def test_journal(self):
        data = [{"payload":  str(i)} for i in range(20)]

        from_barrier = self.write_log_barrier(self.get_node_address())
        create("journal", "//tmp/journal")
        write_journal("//tmp/journal", data)
        raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier)
        chunk_id = get("//tmp/journal/@chunk_ids")[0]

        assert raw_events[0]["data_node_method@"] == "FlushBlocks"
        assert raw_events[0]["direction@"] == "write"
        assert raw_events[0]["chunk_id"] == chunk_id
        assert raw_events[0]["bytes"] > 0
        assert raw_events[0]["io_requests"] > 0

        from_barrier = self.write_log_barrier(self.get_node_address())
        assert read_journal("//tmp/journal") == data
        raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier)

        assert raw_events[0]["data_node_method@"] == "GetBlockRange"
        assert raw_events[0]["direction@"] == "read"
        assert raw_events[0]["chunk_id"] == chunk_id
        assert raw_events[0]["bytes"] > 0
        assert raw_events[0]["io_requests"] > 0

    @authors("gepardo")
    def test_large_journal(self):
        create("journal", "//tmp/journal")

        for i in range(10):
            large_journal, large_journal_size = self.generate_large_journal(row_len=20000)
            min_data_bound = 0.9 * large_journal_size
            max_data_bound = 1.1 * large_journal_size

            from_barrier = self.write_log_barrier(self.get_node_address())
            write_journal("//tmp/journal", large_journal)
            raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier)

            assert raw_events[0]["data_node_method@"] == "FlushBlocks"
            assert raw_events[0]["direction@"] == "write"
            assert min_data_bound <= raw_events[0]["bytes"] <= max_data_bound
            assert raw_events[0]["io_requests"] > 0

            from_barrier = self.write_log_barrier(self.get_node_address())
            read_result = read_journal("//tmp/journal[#{}:#{}]".format(i * len(large_journal), (i + 1) * len(large_journal)))
            assert read_result == large_journal
            raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier)

            assert raw_events[0]["data_node_method@"] == "GetBlockRange"
            assert raw_events[0]["direction@"] == "read"
            assert min_data_bound <= raw_events[0]["bytes"] <= max_data_bound
            assert raw_events[0]["io_requests"] > 0

##################################################################


class TestMultipleReadIORequests(TestNodeIOTrackingBase):
    ENABLE_MULTIDAEMON = False  # Check structured logs.
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1,
            "default_journal_read_quorum": 1,
            "default_journal_write_quorum": 1,
            "default_journal_replication_factor": 1,
        }
    }

    DELTA_DRIVER_CONFIG = {
        "chunk_client_dispatcher": {
            "chunk_reader_pool_size": 1,
        }
    }

    @authors("ngc224")
    def test_multiple_read_io_requests(self):
        large_data, large_data_size = self.generate_large_data(column_count=100)
        columns = list(large_data[0].keys())

        schema = yson.YsonList([
            {"name": _, "type": "string" if _ != "id" else "int64"}
            for _ in columns
        ])
        schema.attributes["strict"] = True

        create("table", "//tmp/table", attributes={"schema": schema, "optimize_for": "scan"})
        write_table("//tmp/table", large_data)

        from_barrier = self.write_log_barrier(self.get_node_address())

        path = f"//tmp/table{{{columns[0]},{columns[50]},{columns[99]}}}"
        options = {
            "table_reader": {
                "group_size": large_data_size,
                "window_size": large_data_size,
                "group_out_of_order_blocks": True,
            }
        }

        assert len(read_table(path, **options)) == len(large_data)

        raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier)

        assert raw_events[0]["data_node_method@"] == "GetBlockSet"
        assert raw_events[0]["direction@"] == "read"
        assert raw_events[0]["bytes"] > 0
        assert raw_events[0]["io_requests"] == 3

##################################################################


class TestDataNodeErasureIOTracking(TestNodeIOTrackingBase):
    ENABLE_MULTIDAEMON = False  # Check structured logs.
    NUM_MASTERS = 1
    NUM_NODES = 6
    NUM_SCHEDULERS = 1

    def _check_data_read(self, from_barriers, method, chunk_type):
        was_data_read = False
        for node_id in range(self.NUM_NODES):
            raw_events = self.read_raw_events(from_barrier=from_barriers[node_id], node_id=node_id)
            if not raw_events:
                continue
            assert len(raw_events) == 1
            assert raw_events[0]["data_node_method@"] == method
            assert raw_events[0]["bytes"] > 0
            assert raw_events[0]["io_requests"] > 0
            if chunk_type == "blob":
                assert raw_events[0]["compression_codec@"] == "zlib_5"
                assert raw_events[0]["erasure_codec@"] == "reed_solomon_3_3"
            was_data_read = True
        assert was_data_read

    @authors("gepardo")
    def test_erasure_blob_chunks(self):
        data = [{"a": i, "b": 2 * i, "c": 3 * i} for i in range(100)]

        from_barriers = [write_log_barrier(self.get_node_address(node_id)) for node_id in range(self.NUM_NODES)]
        create("table", "//tmp/table", attributes={"erasure_codec": "reed_solomon_3_3", "compression_codec": "zlib_5"})
        write_table("//tmp/table", data)
        chunk_id = get("//tmp/table/@chunk_ids")[0]

        for node_id in range(self.NUM_NODES):
            raw_events = self.wait_for_raw_events(count=1, node_id=node_id, from_barrier=from_barriers[node_id])

            assert raw_events[0]["data_node_method@"] == "FinishChunk"
            assert raw_events[0]["chunk_id"] == chunk_id
            assert raw_events[0]["bytes"] > 0
            assert raw_events[0]["io_requests"] > 0
            assert raw_events[0]["compression_codec@"] == "zlib_5"
            assert raw_events[0]["erasure_codec@"] == "reed_solomon_3_3"

        from_barriers = [write_log_barrier(self.get_node_address(node_id)) for node_id in range(self.NUM_NODES)]
        assert read_table("//tmp/table") == data
        time.sleep(1.0)
        self._check_data_read(from_barriers, "GetBlockSet", "blob")

    @authors("gepardo")
    def test_erasure_journal_chunks(self):
        data = [{"payload": str(i)} for i in range(20)]

        from_barriers = [write_log_barrier(self.get_node_address(node_id)) for node_id in range(self.NUM_NODES)]
        create("journal", "//tmp/journal", attributes={
            "erasure_codec": "reed_solomon_3_3",
            "replication_factor": 1,
            "read_quorum": 6,
            "write_quorum": 6,
        })
        write_journal("//tmp/journal", data)
        chunk_id = get("//tmp/journal/@chunk_ids")[0]

        for node_id in range(self.NUM_NODES):
            raw_events = self.wait_for_raw_events(count=1, node_id=node_id, from_barrier=from_barriers[node_id])

            assert raw_events[0]["data_node_method@"] == "FlushBlocks"
            assert raw_events[0]["chunk_id"] == chunk_id
            assert raw_events[0]["bytes"] > 0
            assert raw_events[0]["io_requests"] > 0

        from_barriers = [write_log_barrier(self.get_node_address(node_id)) for node_id in range(self.NUM_NODES)]
        assert read_journal("//tmp/journal") == data
        time.sleep(1.0)
        self._check_data_read(from_barriers, "GetBlockRange", "journal")

##################################################################


class TestMasterJobIOTracking(TestNodeIOTrackingBase):
    ENABLE_MULTIDAEMON = False  # Check structured logs.
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    def _wait_for_merge(self, table_path, merge_mode, account="tmp"):
        yt_commands.set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        rows = read_table(table_path)
        assert get("{}/@resource_usage/chunk_count".format(table_path)) > 1

        yt_commands.set("{}/@chunk_merger_mode".format(table_path), merge_mode)
        yt_commands.set("//sys/accounts/{}/@merge_job_rate_limit".format(account), 10)
        yt_commands.set("//sys/accounts/{}/@chunk_merger_node_traversal_concurrency".format(account), 1)
        wait(lambda: get("{}/@resource_usage/chunk_count".format(table_path)) == 1)
        assert read_table(table_path) == rows

    @authors("gepardo")
    def test_replicate_chunk_writes(self):
        from_barriers = [write_log_barrier(self.get_node_address(node_id)) for node_id in range(self.NUM_NODES)]
        create("table", "//tmp/table", attributes={"replication_factor": self.NUM_NODES})
        write_table("//tmp/table", [{"a": 1, "b": 2, "c": 3}])
        chunk_id = get("//tmp/table/@chunk_ids")[0]

        def event_filter(event):
            return event.get("data_node_method@") == "FinishChunk" and \
                event["category"] == "IORaw" and event["chunk_id"] == chunk_id

        has_replication_job = False
        read_count = 0
        write_count = 0
        for node_id in range(self.NUM_NODES):
            raw_events = self.wait_for_raw_events(
                count=1, from_barrier=from_barriers[node_id], node_id=node_id, filter=event_filter)
            if "job_type@" not in raw_events[0]:
                continue
            has_replication_job = True
            assert raw_events[0]["job_type@"] == "replicate_chunk"
            assert raw_events[0]["user@"] == "root"
            assert raw_events[0]["location_type@"] == "store"
            assert "job_id" in raw_events[0]
            assert "medium@" in raw_events[0]
            assert "location_id" in raw_events[0]
            assert "disk_family@" in raw_events[0]
            assert raw_events[0]["bytes"] > 0
            assert raw_events[0]["io_requests"] > 0
            direction = raw_events[0]["direction@"]
            assert direction in ["read", "write"]
            if direction == "read":
                read_count += 1
            else:
                write_count += 1

        assert has_replication_job
        assert read_count >= 0
        assert write_count >= 0

    @authors("gepardo")
    def test_large_replicate(self):
        large_data, large_data_size = self.generate_large_data()

        from_barriers = [write_log_barrier(self.get_node_address(node_id)) for node_id in range(self.NUM_NODES)]
        create("table", "//tmp/table", attributes={"replication_factor": self.NUM_NODES})
        write_table("//tmp/table", large_data, verbose=False)
        chunk_id = get("//tmp/table/@chunk_ids")[0]

        def event_filter(event):
            return event.get("data_node_method@") == "FinishChunk" and \
                event["chunk_id"] == chunk_id

        disk_space = get("//tmp/table/@resource_usage/disk_space")
        min_data_bound = 0.95 * large_data_size
        max_data_bound = 1.05 * disk_space // self.NUM_NODES
        assert min_data_bound < max_data_bound

        events = []

        has_replication_write_job = False
        for node_id in range(self.NUM_NODES):
            raw_events = self.wait_for_raw_events(
                count=1, from_barrier=from_barriers[node_id], node_id=node_id, filter=event_filter)
            event = raw_events[0]
            if "job_type@" not in event:
                continue
            has_replication_write_job = True
            events.append(event)
        assert has_replication_write_job

        time.sleep(3.0)

        has_replication_read_job = False
        for node_id in range(self.NUM_NODES):
            raw_events = self.read_raw_events(from_barrier=from_barriers[node_id], node_id=node_id)
            for event in raw_events:
                if "job_type@" in event and "data_node_method@" not in event:
                    has_replication_read_job = True
                    events.append(event)
                    break
        assert has_replication_read_job

        for event in events:
            assert event["job_type@"] == "replicate_chunk"
            assert "job_id" in event
            assert min_data_bound <= event["bytes"] <= max_data_bound

    @authors("gepardo")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_merge_chunks(self, merge_mode):
        from_barrier = self.write_log_barrier(self.get_node_address(node_id=0))

        create("table", "//tmp/table")
        write_table("<append=true>//tmp/table", {"name": "cheetah", "type": "cat"})
        write_table("<append=true>//tmp/table", {"name": "fox", "type": "dog"})
        write_table("<append=true>//tmp/table", {"name": "wolf", "type": "dog"})
        write_table("<append=true>//tmp/table", {"name": "tiger", "type": "cat"})

        self._wait_for_merge("//tmp/table", merge_mode)

        aggregate_events = self.wait_for_aggregate_events(
            count=1, from_barrier=from_barrier, node_id=0,
            filter=lambda event: event.get("job_type@") == "merge_chunks" and event.get("data_node_method@") == "FinishChunk")
        assert aggregate_events[0]["bytes"] > 0
        assert aggregate_events[0]["io_requests"] > 0

    @authors("gepardo")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_large_merge(self, merge_mode):
        large_data, large_data_size = self.generate_large_data()

        from_barrier = self.write_log_barrier(self.get_node_address(node_id=0))

        create("table", "//tmp/table")
        for row in large_data:
            write_table("<append=true>//tmp/table", row, verbose=False)

        disk_space = get("//tmp/table/@resource_usage/disk_space")
        min_data_bound = 0.95 * large_data_size
        max_data_bound = 1.05 * disk_space // self.NUM_NODES
        assert min_data_bound < max_data_bound

        self._wait_for_merge("//tmp/table", merge_mode)

        aggregate_events = self.wait_for_aggregate_events(
            count=1, from_barrier=from_barrier, node_id=0,
            filter=lambda event: event.get("job_type@") == "merge_chunks" and event.get("data_node_method@") == "FinishChunk")
        assert min_data_bound <= aggregate_events[0]["bytes"] <= max_data_bound
        assert aggregate_events[0]["io_requests"] > 0


class TestRepairMasterJobIOTracking(TestNodeIOTrackingBase):
    ENABLE_MULTIDAEMON = False  # Check structured logs.
    NUM_MASTERS = 1
    # We need six nodes to store chunks for reed_solomon_3_3 and one extra node to store repaired chunk.
    NUM_NODES = 7
    NUM_SCHEDULERS = 1

    @authors("gepardo")
    def test_repair_chunk(self):
        from_barriers = [write_log_barrier(self.get_node_address(node_id)) for node_id in range(self.NUM_NODES)]

        create("table", "//tmp/table", attributes={"erasure_codec": "reed_solomon_3_3"})
        write_table("//tmp/table", [{"a": i, "b": 2 * i, "c": 3 * i} for i in range(100)])

        chunk_id = get_singular_chunk_id("//tmp/table")
        replicas = get("#{0}/@stored_replicas".format(chunk_id))
        address_to_ban = str(replicas[3])
        set_node_banned(address_to_ban, True)
        time.sleep(3.0)

        read_result = read_table("//tmp/table",
                                 table_reader={
                                     "unavailable_chunk_strategy": "restore",
                                     "pass_count": 1,
                                     "retry_count": 1,
                                 })
        assert read_result == [{"a": i, "b": 2 * i, "c": 3 * i} for i in range(100)]

        replicas = set(map(str, replicas))
        new_replicas = set(map(str, get("#{0}/@stored_replicas".format(chunk_id))))

        has_repaired_replica = False
        for node_id in range(self.NUM_NODES):
            address = self.get_node_address(node_id)
            if address in new_replicas and address not in replicas:
                has_repaired_replica = True
                raw_events = self.wait_for_raw_events(
                    count=1, from_barrier=from_barriers[node_id], node_id=node_id,
                    filter=lambda event: "job_type@" in event)
                assert len(raw_events) == 1
                assert "job_id" in raw_events[0]
                assert raw_events[0]["bytes"] > 0
                assert raw_events[0]["io_requests"] > 0

        assert has_repaired_replica

        set_node_banned(address_to_ban, False)

##################################################################


class TestClientIOTracking(TestNodeIOTrackingBase):
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

    def _get_proxy_kind(self):
        return "http"

    @authors("gepardo")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_write_static_table(self, optimize_for):
        data1, data_size = self.generate_large_data()
        data2, _ = self.generate_large_data()

        create_account("gepardo")
        create_account("some_other_account")

        create("table", "//tmp/table1", attributes={"optimize_for": optimize_for})
        yt_commands.set("//tmp/table1/@account", "gepardo")

        create("table", "//tmp/table2", attributes={"optimize_for": optimize_for})
        yt_commands.set("//tmp/table2/@account", "some_other_account")

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        write_table("//tmp/table1", data1, verbose=False)
        write_table("//tmp/table2", data2, verbose=False)
        raw_events = self.wait_for_raw_events(count=2, from_barrier=from_barrier,
                                              filter=lambda event: event.get("data_node_method@") == "FinishChunk")

        raw_events.sort(key=lambda event: event["object_path"])
        event1, event2 = raw_events

        disk_space = get("//tmp/table1/@resource_usage/disk_space")
        min_data_bound = 0.95 * data_size
        max_data_bound = 1.05 * disk_space
        assert min_data_bound < max_data_bound

        assert min_data_bound <= event1["bytes"] <= max_data_bound
        assert event1["io_requests"] > 0
        assert event1["account@"] == "gepardo"
        assert event1["object_path"] == "//tmp/table1"
        assert event1["api_method@"] == "write_table"
        assert event1["proxy_kind@"] == self._get_proxy_kind()
        assert "object_id" in event1

        assert min_data_bound <= event2["bytes"] <= max_data_bound
        assert event2["io_requests"] > 0
        assert event2["account@"] == "some_other_account"
        assert event2["object_path"] == "//tmp/table2"
        assert event2["api_method@"] == "write_table"
        assert event2["proxy_kind@"] == self._get_proxy_kind()
        assert "object_id" in event2

    @authors("gepardo")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_read_static_table(self, optimize_for):
        data1, data_size = self.generate_large_data()
        data2, _ = self.generate_large_data()

        create_account("gepardo")
        create_account("some_other_account")

        create("table", "//tmp/table1", attributes={"optimize_for": optimize_for})
        create("table", "//tmp/table2", attributes={"optimize_for": optimize_for})
        write_table("//tmp/table1", data1, verbose=False)
        write_table("//tmp/table2", data2, verbose=False)
        yt_commands.set("//tmp/table1/@account", "gepardo")
        yt_commands.set("//tmp/table2/@account", "some_other_account")

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        assert read_table("//tmp/table2", verbose=False) == data2
        assert read_table("//tmp/table1", verbose=False) == data1
        raw_events = self.wait_for_raw_events(count=2, from_barrier=from_barrier,
                                              filter=lambda event: event.get("data_node_method@") == "GetBlockSet")

        raw_events.sort(key=lambda event: event["object_path"])
        event1, event2 = raw_events

        disk_space = get("//tmp/table1/@resource_usage/disk_space")
        min_data_bound = 0.95 * data_size
        max_data_bound = 1.05 * disk_space
        assert min_data_bound < max_data_bound

        assert min_data_bound <= event1["bytes"] <= max_data_bound
        assert event1["io_requests"] > 0
        assert event1["account@"] == "gepardo"
        assert event1["object_path"] == "//tmp/table1"
        assert event1["api_method@"] == "read_table"
        assert event1["proxy_kind@"] == self._get_proxy_kind()
        assert "object_id" in event1

        assert min_data_bound <= event2["bytes"] <= max_data_bound
        assert event2["io_requests"] > 0
        assert event2["account@"] == "some_other_account"
        assert event2["object_path"] == "//tmp/table2"
        assert event2["api_method@"] == "read_table"
        assert event2["proxy_kind@"] == self._get_proxy_kind()
        assert "object_id" in event2

    @authors("gepardo")
    def test_read_static_table_range(self):
        data = [
            {"row": "first", "cat": "lion"},
            {"row": "second", "cat": "tiger"},
            {"row": "third", "cat": "panthera"},
            {"row": "fourth", "cat": "leopard"},
        ]

        create_account("gepardo")
        create("table", "//tmp/table")
        yt_commands.set("//tmp/table/@account", "gepardo")
        write_table("//tmp/table", data)

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        assert read_table("//tmp/table[#1:#3]") == data[1:3]
        raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier,
                                              filter=lambda event: event.get("data_node_method@") == "GetBlockSet")

        event = raw_events[0]
        assert event["bytes"] > 0
        assert event["io_requests"] > 0
        assert event["account@"] == "gepardo"
        assert event["object_path"] == "//tmp/table"
        assert event["api_method@"] == "read_table"
        assert event["proxy_kind@"] == self._get_proxy_kind()
        assert "object_id" in event

    @authors("gepardo")
    @pytest.mark.parametrize("sorted_table", [False, True])
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_read_dynamic_table(self, sorted_table, optimize_for):
        schema = yson.YsonList([
            {"name": "key", "type": "int64"},
            {"name": "value", "type": "string"},
        ])
        if sorted_table:
            schema[0]["sort_order"] = "ascending"
            schema.attributes["unique_keys"] = True
        data = [
            {"key": 3, "value": "test"},
            {"key": 31, "value": "test read"},
            {"key": 314, "value": "test read dynamic"},
            {"key": 3141, "value": "test read dynamic table"},
        ]

        create_account("gepardo")
        yt_commands.set("//sys/accounts/gepardo/@resource_limits/tablet_count", 10)

        sync_create_cells(1)
        create_dynamic_table("//tmp/my_dyntable", schema=schema, optimize_for=optimize_for, account="gepardo")
        sync_mount_table("//tmp/my_dyntable")
        insert_rows("//tmp/my_dyntable", data)
        sync_unmount_table("//tmp/my_dyntable")

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        assert sorted_dicts(read_table("//tmp/my_dyntable")) == sorted_dicts(data)
        raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier, check_event_count=False,
                                              filter=lambda event: event.get("data_node_method@") == "GetBlockSet")

        for event in raw_events:
            assert event["bytes"] > 0
            assert event["io_requests"] > 0
            assert event["object_path"] == "//tmp/my_dyntable"
            assert event["api_method@"] == "read_table"
            assert event["proxy_kind@"] == self._get_proxy_kind()
            assert event["account@"] == "gepardo"
            assert "object_id" in event

    @authors("gepardo")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_read_dynamic_table_converted_from_static(self, optimize_for):
        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ])
        schema.attributes["strict"] = True
        schema.attributes["unique_keys"] = True
        data = [
            {"key": 3, "value": "test"},
            {"key": 31, "value": "test read"},
            {"key": 314, "value": "test read dynamic"},
            {"key": 3141, "value": "test read dynamic table"},
        ]

        create_account("gepardo")
        yt_commands.set("//sys/accounts/gepardo/@resource_limits/tablet_count", 10)

        sync_create_cells(1)
        create("table", "//tmp/table", attributes={
            "schema": schema,
            "account": "gepardo",
            "optimize_for": optimize_for,
        })
        write_table("//tmp/table", data)
        alter_table("//tmp/table", dynamic=True)

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        assert sorted_dicts(read_table("//tmp/table")) == sorted_dicts(data)
        raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier, check_event_count=False,
                                              filter=lambda event: event.get("data_node_method@") == "GetBlockSet")

        for event in raw_events:
            assert event["bytes"] > 0
            assert event["io_requests"] > 0
            assert event["object_path"] == "//tmp/table"
            assert event["api_method@"] == "read_table"
            assert event["proxy_kind@"] == self._get_proxy_kind()
            assert event["account@"] == "gepardo"
            assert "object_id" in event

    @authors("gepardo")
    def test_files(self):
        create("file", "//tmp/file", attributes={"compression_codec": "zlib_5"})

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        write_file("//tmp/file", b"Soon we will see how this file is being read ;)")
        raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier, check_event_count=False,
                                              filter=lambda event: event.get("data_node_method@") == "FinishChunk")

        write_event = raw_events[0]
        assert write_event["bytes"] > 0
        assert write_event["io_requests"] > 0
        assert write_event["object_path"] == "//tmp/file"
        assert write_event["api_method@"] == "write_file"
        assert write_event["proxy_kind@"] == self._get_proxy_kind()
        assert write_event["account@"] == "tmp"
        assert write_event["compression_codec@"] == "zlib_5"
        assert write_event["erasure_codec@"] == "none"
        assert "object_id" in write_event

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        assert read_file("//tmp/file") == b"Soon we will see how this file is being read ;)"
        raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier, check_event_count=False,
                                              filter=lambda event: event.get("data_node_method@") == "GetBlockSet")

        read_event = raw_events[0]
        assert read_event["bytes"] > 0
        assert read_event["io_requests"] > 0
        assert read_event["object_path"] == "//tmp/file"
        assert read_event["api_method@"] == "read_file"
        assert read_event["proxy_kind@"] == self._get_proxy_kind()
        assert read_event["account@"] == "tmp"
        assert read_event["compression_codec@"] == "zlib_5"
        assert read_event["erasure_codec@"] == "none"
        assert "object_id" in read_event


class TestClientRpcProxyIOTracking(TestClientIOTracking):
    ENABLE_MULTIDAEMON = False  # Check structured logs.
    DRIVER_BACKEND = "rpc"
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True

    def write_log_barrier(self, *args, **kwargs):
        kwargs["driver"] = self.__native_driver
        return super(TestClientRpcProxyIOTracking, self).write_log_barrier(*args, **kwargs)

    def setup_method(self, method):
        super(TestClientRpcProxyIOTracking, self).setup_method(method)
        native_config = deepcopy(self.Env.configs["driver"])
        self.__native_driver = Driver(native_config)

    def _get_proxy_kind(self):
        return "rpc"

##################################################################


class TestJobIOTrackingBase(TestNodeIOTrackingBase):
    ENABLE_MULTIDAEMON = False  # Check structured logs.
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1,
            "default_file_replication_factor": 1,
        }
    }

    DELTA_NODE_CONFIG = {
        "data_node": {
            "block_cache": {
                "compressed_data": {
                    "capacity": 0,
                },
                "uncompressed_data": {
                    "capacity": 0,
                },
            },
        },
    }

    def _validate_basic_tags(self, event, op_id, op_type, users=None, pool_tree="default", pool="/root",
                             allow_zero=False):
        if users is None:
            users = ["root"]
        assert event["pool_tree@"] == pool_tree
        assert event["pool@"] == pool.rsplit("/", 1)[1]
        assert event["pool_path@"] == "/{}{}".format(pool_tree, pool)
        assert event["operation_id"] == op_id
        assert event["operation_type@"] == op_type
        assert "job_id" in event
        if allow_zero:
            assert event["bytes"] >= 0
            assert event["io_requests"] >= 0
            assert event["bytes"] + event["io_requests"] > 0
        else:
            assert event["bytes"] > 0
            assert event["io_requests"] > 0
        assert event["user@"] in users

    def _gather_events(self, raw_events, op, task_to_kind=None, kind_to_job=None,
                       account="tmp", intermediate_account="intermediate", pool_tree="default",
                       pool="/root"):
        op_type = get(op.get_path() + "/@operation_type")

        for event in raw_events:
            self._validate_basic_tags(event, op.id, op_type, pool_tree=pool_tree, pool=pool)

        paths = {}
        for event in raw_events:
            assert event["data_node_method@"] in ["GetBlockSet", "GetBlockRange", "FinishChunk"]
            direction = "read" if event["data_node_method@"] in ["GetBlockSet", "GetBlockRange"] else "write"

            task_name = event["task_name@"]
            job_type = event["job_type@"]
            kind = task_name if task_to_kind is None else task_to_kind[task_name]
            expected_job_type = task_name if kind_to_job is None else kind_to_job[kind]
            assert job_type == expected_job_type
            if kind not in paths:
                paths[kind] = {"read": [], "write": [], "read_chunks": [], "write_chunks": []}

            assert "erasure_codec@" in event
            if job_type == "shallow_merge":
                assert "compression_codec@" not in event
            else:
                assert "compression_codec@" in event

            path = event["object_path"]
            if path.find("intermediate") == -1:
                assert "object_id" in event
                assert event["account@"] == account
            else:
                assert "object_id" not in event
                assert event["account@"] == intermediate_account

            paths[kind][direction].append(path)
            paths[kind][direction + "_chunks"].append(event["chunk_id"])

        return paths


class TestJobIOTracking(TestJobIOTrackingBase):
    ENABLE_MULTIDAEMON = False  # Check structured logs.

    @authors("gepardo")
    def test_pools(self):
        node = ls("//sys/cluster_nodes")[0]
        yt_commands.set("//sys/cluster_nodes/" + node + "/@user_tags/end", "oaken")
        yt_commands.set("//sys/pool_trees/default/@config/nodes_filter", "!oaken")

        create_pool_tree("oak", config={"nodes_filter": "oaken"})
        create_pool("branch", pool_tree="oak")
        create_pool("acorn", pool_tree="oak", parent_name="branch")

        table_data = [
            {"id": 1, "name": "cat"},
            {"id": 1, "name": "python"},
            {"id": 2, "name": "dog"},
            {"id": 2, "name": "rattlesnake"},
        ]

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_in")
        create("table", "//tmp/table_out")
        write_table("//tmp/table_in", table_data)
        raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier)
        assert raw_events[0]["data_node_method@"] == "FinishChunk"

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        op = yt_commands.map(
            in_="//tmp/table_in",
            out="//tmp/table_out",
            command="cat",
            spec={
                "pool": "acorn",
                "pool_trees": ["oak"],
            }
        )
        raw_events = self.wait_for_raw_events(count=2, from_barrier=from_barrier)

        paths = self._gather_events(raw_events, op, pool_tree="oak", pool="/branch/acorn")
        assert paths["map"]["read"] == ["//tmp/table_in"]
        assert paths["map"]["write"] == ["//tmp/table_out"]

        assert read_table("//tmp/table_out") == table_data

    @authors("gepardo")
    def test_ephemeral_pool(self):
        create_pool("first")
        create_pool("second", parent_name="first")
        yt_commands.set("//sys/pool_trees/default/@config/default_parent_pool", "second")

        table_data = [
            {"id": 1, "name": "cat"},
            {"id": 1, "name": "python"},
            {"id": 2, "name": "dog"},
            {"id": 2, "name": "rattlesnake"},
        ]

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_in")
        create("table", "//tmp/table_out")
        write_table("//tmp/table_in", table_data)
        raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier)
        assert raw_events[0]["data_node_method@"] == "FinishChunk"

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        op = yt_commands.map(
            in_="//tmp/table_in",
            out="//tmp/table_out",
            command="cat",
        )
        raw_events = self.wait_for_raw_events(count=2, from_barrier=from_barrier)

        paths = self._gather_events(raw_events, op, pool="/first/second/root")
        assert paths["map"]["read"] == ["//tmp/table_in"]
        assert paths["map"]["write"] == ["//tmp/table_out"]

        assert read_table("//tmp/table_out") == table_data

    @authors("gepardo")
    @pytest.mark.parametrize("op_type", ["map", "ordered_map", "reduce"])
    def test_basic_operations(self, op_type):
        table_data = [
            {"id": 1, "name": "cat"},
            {"id": 1, "name": "python"},
            {"id": 2, "name": "dog"},
            {"id": 2, "name": "rattlesnake"},
        ]

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create(
            "table",
            "//tmp/table_in",
            attributes={"schema": [
                {"name": "id", "type": "int64", "sort_order": "ascending"},
                {"name": "name", "type": "string"},
            ]}
        )
        create("table", "//tmp/table_out")
        write_table("//tmp/table_in", table_data)
        raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier)
        assert raw_events[0]["data_node_method@"] == "FinishChunk"

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        run_op = yt_commands.map if op_type in ["map", "ordered_map"] else reduce
        op = run_op(
            in_="//tmp/table_in",
            out="//tmp/table_out",
            command="cat",
            reduce_by=["id"],  # ignored for maps
            ordered=(op_type == "ordered_map"),  # ignored for reduces
        )
        raw_events = self.wait_for_raw_events(count=2, from_barrier=from_barrier)

        task_name = {
            "map": "map",
            "ordered_map": "ordered_map",
            "reduce": "sorted_reduce",
        }[op_type]

        paths = self._gather_events(raw_events, op)
        assert paths[task_name]["read"] == ["//tmp/table_in"]
        assert paths[task_name]["write"] == ["//tmp/table_out"]

        assert read_table("//tmp/table_out") == table_data

    @authors("gepardo")
    @pytest.mark.parametrize("merge_mode", ["unordered", "ordered", "sorted"])
    def test_merge(self, merge_mode):
        first_data = [
            {"id": 1, "name": "cat"},
            {"id": 2, "name": "dog"},
        ]
        second_data = [
            {"id": 1, "name": "python"},
            {"id": 2, "name": "rattlesnake"},
        ]
        table_attributes = {}
        if merge_mode == "sorted":
            table_attributes = {
                "schema": [
                    {"name": "id", "type": "int64", "sort_order": "ascending"},
                    {"name": "name", "type": "string"},
                ]
            }

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_in1", attributes=table_attributes)
        create("table", "//tmp/table_in2", attributes=table_attributes)
        create("table", "//tmp/table_out")
        write_table("//tmp/table_in1", first_data)
        write_table("//tmp/table_in2", second_data)
        raw_events = self.wait_for_raw_events(count=2, from_barrier=from_barrier)
        assert raw_events[0]["data_node_method@"] == "FinishChunk"
        assert raw_events[1]["data_node_method@"] == "FinishChunk"

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        op = merge(
            mode=merge_mode,
            in_=["//tmp/table_in1", "//tmp/table_in2"],
            out="//tmp/table_out",
            spec={"force_transform": True},
        )
        raw_events = self.wait_for_raw_events(count=3, from_barrier=from_barrier)

        task_name = merge_mode + "_merge"
        paths = self._gather_events(raw_events, op)
        assert sorted(paths[task_name]["read"]) == ["//tmp/table_in1", "//tmp/table_in2"]
        assert sorted(paths[task_name]["write"]) == ["//tmp/table_out"]

        assert sorted_dicts(read_table("//tmp/table_out")) == sorted_dicts(first_data + second_data)

    @authors("gepardo")
    def test_erase(self):
        table_input_data = [
            {"id": 1, "name": "cat"},
            {"id": 1, "name": "python"},
            {"id": 2, "name": "dog"},
            {"id": 2, "name": "rattlesnake"},
        ]
        table_output_data = [
            {"id": 1, "name": "cat"},
            {"id": 2, "name": "rattlesnake"},
        ]

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table")
        write_table("//tmp/table", table_input_data)
        raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier)
        assert raw_events[0]["data_node_method@"] == "FinishChunk"

        input_chunk_ids = get("//tmp/table/@chunk_ids")
        assert len(input_chunk_ids) == 1

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        op = erase("//tmp/table[#1:#3]")
        raw_events = self.wait_for_raw_events(count=3, from_barrier=from_barrier)

        output_chunk_ids = get("//tmp/table/@chunk_ids")
        assert len(output_chunk_ids) == 1

        paths = self._gather_events(raw_events, op)
        assert paths["ordered_merge"]["read"] == ["//tmp/table"] * 2
        assert paths["ordered_merge"]["write"] == ["//tmp/table"]
        assert paths["ordered_merge"]["read_chunks"] == [input_chunk_ids[0]] * 2
        assert paths["ordered_merge"]["write_chunks"] == [output_chunk_ids[0]]

        assert read_table("//tmp/table") == table_output_data

    @authors("gepardo")
    @pytest.mark.parametrize("merge_type", ["shallow", "deep"])
    def test_auto_merge_simple(self, merge_type):
        row_count = 4
        table_data = [{"a": i} for i in range(row_count)]

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_in")
        create("table", "//tmp/table_out")
        write_table("//tmp/table_in", table_data)
        raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier)
        assert raw_events[0]["data_node_method@"] == "FinishChunk"

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        op = yt_commands.map(
            in_="//tmp/table_in",
            out="//tmp/table_out",
            command="cat",
            reduce_by=["a"],
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "max_intermediate_chunk_count": 100,
                    "chunk_count_per_merge_job": 4,
                    "enable_shallow_merge": merge_type == "shallow",
                    "shallow_merge_min_data_weight_per_chunk": 0,
                },
                "data_size_per_job": 1,
            },
        )
        raw_events = self.wait_for_raw_events(count=13, from_barrier=from_barrier)

        merge_job_type = "shallow_merge" if merge_type == "shallow" else "unordered_merge"
        paths = self._gather_events(raw_events, op, kind_to_job={
            "map": "map",
            "auto_merge": merge_job_type
        }, intermediate_account="tmp")

        assert paths["map"]["read"] == ["//tmp/table_in"] * 4
        assert paths["map"]["write"] == ["<intermediate-0>"] * 4
        assert paths["auto_merge"]["read"] == ["<intermediate-0>"] * 4
        assert paths["auto_merge"]["write"] == ["//tmp/table_out"]

        assert sorted_dicts(read_table("//tmp/table_out")) == sorted_dicts(table_data)

    @authors("gepardo")
    @pytest.mark.parametrize("merge_type", ["shallow", "deep"])
    @pytest.mark.parametrize("op_type", ["map", "reduce"])
    def test_auto_merge_two_tables(self, merge_type, op_type):
        create_account("gepardo")

        row_count = 12
        table_data = [{"a": i} for i in range(row_count)]

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_in",
               attributes={"schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}]})
        create("table", "//tmp/table_out1",
               attributes={"schema": [{"name": "a", "type": "int64"}]})
        create("table", "//tmp/table_out2",
               attributes={"schema": [{"name": "a", "type": "int64"}]})
        write_table(
            "//tmp/table_in",
            table_data,
            max_row_buffer_size=1,
            table_writer={"desired_chunk_size": 1})
        raw_events = self.wait_for_raw_events(count=12, from_barrier=from_barrier)
        chunk_ids = get("//tmp/table_in/@chunk_ids")
        assert len(chunk_ids) == 12
        for event in raw_events:
            assert event["data_node_method@"] == "FinishChunk"
            assert event["chunk_id"] in chunk_ids

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        run_op = yt_commands.map if op_type == "map" else reduce
        table_format = b"<columns=[a];enable_string_to_all_conversion=%true;>schemaful_dsv"
        op = run_op(
            in_="//tmp/table_in",
            out=["//tmp/table_out1", "//tmp/table_out2"],
            command="read x; echo $x >&$(($x % 2 * 3 + 1))",
            reduce_by=["a"],  # ignored for maps
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "max_intermediate_chunk_count": 100,
                    "chunk_count_per_merge_job": 3,
                    "use_intermediate_data_account": True,
                    "enable_shallow_merge": merge_type == "shallow",
                    "shallow_merge_min_data_weight_per_chunk": 0,
                },
                "mapper": {"format": yson.loads(table_format)},
                "reducer": {"format": yson.loads(table_format)},
                "data_size_per_job": 1,
                "intermediate_data_account": "gepardo",
            },
        )
        raw_events = self.wait_for_raw_events(count=40, from_barrier=from_barrier)

        merge_job_type = "shallow_merge" if merge_type == "shallow" else "unordered_merge"
        task_name = "map" if op_type == "map" else "sorted_reduce"
        paths = self._gather_events(raw_events, op, kind_to_job={
            task_name: task_name,
            "auto_merge": merge_job_type,
        }, intermediate_account="gepardo")

        assert paths[task_name]["read"] == ["//tmp/table_in"] * 12
        assert sorted(paths[task_name]["write"]) == ["<intermediate-0>"] * 6 + ["<intermediate-1>"] * 6
        assert sorted(paths["auto_merge"]["read"]) == ["<intermediate-0>"] * 6 + ["<intermediate-1>"] * 6
        assert sorted(paths["auto_merge"]["write"]) == ["//tmp/table_out1"] * 2 + ["//tmp/table_out2"] * 2

        output_data = read_table("//tmp/table_out1") + read_table("//tmp/table_out2")
        assert sorted_dicts(list(output_data)) == sorted_dicts(list(table_data))

    def _gather_artifact_events(self, raw_events, op, task_name="map", job_type="map"):
        event_paths = {}
        op_type = get(op.get_path() + "/@operation_type")

        for event in raw_events:
            self._validate_basic_tags(event, op.id, op_type, users=["root"])

            assert event["task_name@"] == task_name
            assert event["job_type@"] == job_type

            assert "object_id" in event
            assert event["account@"] == "tmp"

            if "job_io_kind@" in event:
                workload = event["job_io_kind@"]
                assert workload in ["artifact_copy", "artifact_download", "artifact_bypass_cache"]
            else:
                assert event["data_node_method@"] in ["FinishChunk", "GetBlockSet", "GetBlockRange"]
                workload = "job"

            descriptor = (workload, event["direction@"])

            if descriptor in [
                ("artifact_download", "write"),
                ("artifact_copy", "read"),
            ]:
                assert "medium@" in event
                assert "disk_family@" in event
                assert event["location_type@"] == "cache"
            elif descriptor in [
                ("artifact_download", "read"),
                ("artifact_bypass_cache", "read"),
                ("job", "read"),
                ("job", "write")
            ]:
                assert "medium@" in event
                assert "disk_family@" in event
                assert event["location_type@"] == "store"
            else:
                assert "medium@" not in event
                assert "disk_family@" not in event
                assert event["location_type@"] == "slot"
                "slot_index" in event

            if descriptor not in event_paths:
                event_paths[descriptor] = []
            event_paths[descriptor] += [event["object_path"]]

        for key in event_paths.keys():
            event_paths[key].sort()
        return event_paths

    @authors("gepardo")
    def test_table_artifacts(self):
        table_data = [
            {"id": 1, "name": "cat"},
            {"id": 1, "name": "python"},
            {"id": 2, "name": "dog"},
            {"id": 2, "name": "rattlesnake"},
        ]

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_in")
        create("table", "//tmp/table_out")
        create("table", "//tmp/table_aux")
        write_table("//tmp/table_in", table_data)
        write_table("//tmp/table_aux", [{"x": 1}])
        write_table("<append=%true>//tmp/table_aux", [{"x": 2}])
        raw_events = self.wait_for_raw_events(count=3, from_barrier=from_barrier)
        for event in raw_events:
            assert event["data_node_method@"] == "FinishChunk"

        assert len(get("//tmp/table_aux/@chunk_ids")) == 2

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        op = yt_commands.map(
            in_="//tmp/table_in",
            out="//tmp/table_out",
            command="cat",
            file="<format=json>//tmp/table_aux",
            spec={
                "mapper": {"copy_files": True}
            }
        )
        raw_events = self.wait_for_raw_events(count=7, from_barrier=from_barrier)

        event_paths = self._gather_artifact_events(raw_events, op)
        expected_event_paths = {
            ("artifact_download", "read"): ["//tmp/table_aux", "//tmp/table_aux"],
            ("artifact_download", "write"): ["//tmp/table_aux"],
            ("artifact_copy", "read"): ["//tmp/table_aux"],
            ("artifact_copy", "write"): ["//tmp/table_aux"],
            ("job", "read"): ["//tmp/table_in"],
            ("job", "write"): ["//tmp/table_out"],
        }

        assert sorted(event_paths.items()) == sorted(expected_event_paths.items())

        assert read_table("//tmp/table_out") == table_data

    @authors("gepardo")
    @pytest.mark.parametrize("chunk_count", [1, 2])
    @pytest.mark.parametrize("strategy", ["link", "copy", "bypass"])
    def test_file_artifacts(self, chunk_count, strategy):
        table_data = [
            {"id": 1, "name": "cat"},
            {"id": 1, "name": "python"},
            {"id": 2, "name": "dog"},
            {"id": 2, "name": "rattlesnake"},
        ]

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_in")
        create("table", "//tmp/table_out")
        create("file", "//tmp/script.sh")
        write_table("//tmp/table_in", table_data)
        if chunk_count == 1:
            write_file("//tmp/script.sh", b"#!/bin/sh\nexec cat\n")
        else:
            write_file("//tmp/script.sh", b"#!/bin/sh\n")
            write_file("<append=%true>//tmp/script.sh", b"exec cat\n")
        raw_events = self.wait_for_raw_events(count=1 + chunk_count, from_barrier=from_barrier)
        for event in raw_events:
            assert event["data_node_method@"] == "FinishChunk"

        assert len(get("//tmp/script.sh/@chunk_ids")) == chunk_count

        raw_count = {
            "link": 3 + chunk_count,
            "copy": 5 + chunk_count,
            "bypass": 3 + chunk_count,
        }[strategy]

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        spec = {}
        file = yson.YsonString(b"//tmp/script.sh")
        if strategy == "copy":
            spec.setdefault("mapper", {})["copy_files"] = True
        elif strategy == "bypass":
            file.attributes["bypass_artifact_cache"] = True
        op = yt_commands.map(
            in_="//tmp/table_in",
            out="//tmp/table_out",
            command="sh script.sh",
            file=file,
            spec=spec,
        )
        raw_events = self.wait_for_raw_events(count=raw_count, from_barrier=from_barrier)

        event_paths = self._gather_artifact_events(raw_events, op)
        expected_event_paths = {
            "link": {
                ("artifact_download", "read"): ["//tmp/script.sh"] * chunk_count,
                ("artifact_download", "write"): ["//tmp/script.sh"],
                ("job", "read"): ["//tmp/table_in"],
                ("job", "write"): ["//tmp/table_out"],
            },
            "copy": {
                ("artifact_download", "read"): ["//tmp/script.sh"] * chunk_count,
                ("artifact_download", "write"): ["//tmp/script.sh"],
                ("artifact_copy", "read"): ["//tmp/script.sh"],
                ("artifact_copy", "write"): ["//tmp/script.sh"],
                ("job", "read"): ["//tmp/table_in"],
                ("job", "write"): ["//tmp/table_out"],
            },
            "bypass": {
                ("artifact_bypass_cache", "read"): ["//tmp/script.sh"] * chunk_count,
                ("artifact_bypass_cache", "write"): ["//tmp/script.sh"],
                ("job", "read"): ["//tmp/table_in"],
                ("job", "write"): ["//tmp/table_out"],
            }
        }

        assert sorted(event_paths.items()) == sorted(expected_event_paths[strategy].items())

        assert read_table("//tmp/table_out") == table_data


class TestMapReduceJobIOTracking(TestJobIOTrackingBase):
    ENABLE_MULTIDAEMON = False  # Check structured logs.
    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "map_reduce_operation_options": {
                "min_uncompressed_block_size": 1,
                "spec_template": {
                    "use_new_sorted_pool": False,
                },
            },
        },
    }

    @authors("gepardo")
    def test_map_reduce_simple(self):
        table_data = [
            {"id": 1, "name": "cat"},
            {"id": 1, "name": "python"},
            {"id": 2, "name": "dog"},
            {"id": 2, "name": "rattlesnake"},
        ]

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_in")
        create("table", "//tmp/table_out")
        write_table("//tmp/table_in", table_data)
        raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier)
        assert raw_events[0]["data_node_method@"] == "FinishChunk"

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        op = map_reduce(
            in_="//tmp/table_in",
            out="//tmp/table_out",
            mapper_command="cat",
            reducer_command="cat",
            reduce_by="id",
        )
        raw_events = self.wait_for_raw_events(count=4, from_barrier=from_barrier)

        paths = self._gather_events(raw_events, op, {
            "partition_map(0)": "map",
            "partition_reduce": "reduce",
        }, {
            "map": "partition_map",
            "reduce": "partition_reduce",
        })

        assert paths["map"]["read"] == ["//tmp/table_in"]
        assert paths["map"]["write"] == ["<intermediate-0>"]
        assert paths["reduce"]["read"] == ["<intermediate-0>"]
        assert paths["reduce"]["write"] == ["//tmp/table_out"]

        assert sorted_dicts(read_table("//tmp/table_out")) == sorted_dicts(table_data)

    @authors("gepardo")
    def test_map_reduce_multiple_output(self):
        table_data = [
            {"id": 1, "name": "cat"},
            {"id": 1, "name": "python"},
            {"id": 2, "name": "dog"},
            {"id": 2, "name": "rattlesnake"},
        ]

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_in")
        create("table", "//tmp/table_out_map1")
        create("table", "//tmp/table_out_map2")
        create("table", "//tmp/table_out")
        for row in table_data:
            write_table("<append=%true>//tmp/table_in", [row])
        raw_events = self.wait_for_raw_events(count=4, from_barrier=from_barrier)
        for event in raw_events:
            assert event["data_node_method@"] == "FinishChunk"

        mapper = """
echo "{a=$YT_JOB_INDEX}" 1>&4
[ $YT_JOB_INDEX == 1 ] && echo "{b=$YT_JOB_INDEX}" 1>&7
cat
"""

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        op = map_reduce(
            in_="//tmp/table_in",
            out=["//tmp/table_out_map1", "//tmp/table_out_map2", "//tmp/table_out"],
            mapper_command=mapper,
            reducer_command="cat",
            reduce_by="id",
            spec={
                "mapper_output_table_count": 2,
                "map_job_count": 4,
            }
        )
        raw_events = self.wait_for_raw_events(count=18, from_barrier=from_barrier)

        paths = self._gather_events(raw_events, op, {
            "partition_map(0)": "map",
            "partition_reduce": "reduce",
        }, {
            "map": "partition_map",
            "reduce": "partition_reduce",
        })

        assert paths["map"]["read"] == ["//tmp/table_in"] * 4
        assert sorted(paths["map"]["write"]) == \
            ["//tmp/table_out_map1"] * 4 + ["//tmp/table_out_map2"] + ["<intermediate-0>"] * 4
        assert paths["reduce"]["read"] == ["<intermediate-0>"] * 4
        assert paths["reduce"]["write"] == ["//tmp/table_out"]

        assert sorted_dicts(read_table("//tmp/table_out_map1")) == [{"a": i} for i in range(4)]
        assert sorted_dicts(read_table("//tmp/table_out_map2")) == [{"b": 1}]
        assert sorted_dicts(read_table("//tmp/table_out")) == sorted_dicts(table_data)

    @authors("gepardo")
    def test_map_reduce_no_map(self):
        table_data = [
            {"id": 1, "name": "cat"},
            {"id": 1, "name": "python"},
            {"id": 2, "name": "dog"},
            {"id": 2, "name": "rattlesnake"},
        ]

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_in")
        create("table", "//tmp/table_out")
        write_table("//tmp/table_in", table_data)
        raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier)
        assert raw_events[0]["data_node_method@"] == "FinishChunk"

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        op = map_reduce(
            in_="//tmp/table_in",
            out=["//tmp/table_out"],
            reducer_command="cat",
            reduce_by="id",
        )
        raw_events = self.wait_for_raw_events(count=4, from_barrier=from_barrier)

        paths = self._gather_events(raw_events, op, {
            "partition(0)": "partition",
            "partition_reduce": "reduce",
        }, {
            "partition": "partition",
            "reduce": "partition_reduce",
        })

        assert paths["partition"]["read"] == ["//tmp/table_in"]
        assert paths["partition"]["write"] == ["<intermediate-0>"]
        assert paths["reduce"]["read"] == ["<intermediate-0>"]
        assert paths["reduce"]["write"] == ["//tmp/table_out"]

        assert sorted_dicts(read_table("//tmp/table_out")) == sorted_dicts(table_data)

    @authors("gepardo")
    def test_map_reduce_combiners(self):
        table_data = [
            {"id": 1, "name": "cat"},
            {"id": 1, "name": "python"},
            {"id": 2, "name": "dog"},
            {"id": 2, "name": "rattlesnake"},
        ]

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_in")
        create("table", "//tmp/table_out")
        for row in table_data:
            write_table("<append=%true>//tmp/table_in", [row])
        raw_events = self.wait_for_raw_events(count=4, from_barrier=from_barrier)
        for event in raw_events:
            assert event["data_node_method@"] == "FinishChunk"

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        op = map_reduce(
            in_="//tmp/table_in",
            out="//tmp/table_out",
            mapper_command="cat",
            reducer_command="cat",
            reduce_combiner_command="cat",
            reduce_by="id",
            spec={
                "force_reduce_combiners": True,
                "map_job_count": 4,
            }
        )
        raw_events = self.wait_for_raw_events(count=15, from_barrier=from_barrier)

        paths = self._gather_events(raw_events, op, {
            "partition_map(0)": "map",
            "reduce_combiner": "combine",
            "sorted_reduce": "reduce",
        }, {
            "map": "partition_map",
            "combine": "reduce_combiner",
            "reduce": "sorted_reduce",
        })

        assert paths["map"]["read"] == ["//tmp/table_in"] * 4
        assert paths["map"]["write"] == ["<intermediate-0>"] * 4
        assert paths["combine"]["read"] == ["<intermediate-0>"] * 4
        assert paths["combine"]["write"] == ["<intermediate-0>"]
        assert paths["reduce"]["read"] == ["<intermediate-0>"]
        assert paths["reduce"]["write"] == ["//tmp/table_out"]

        assert sorted_dicts(read_table("//tmp/table_out")) == sorted_dicts(table_data)

    @authors("gepardo")
    def test_map_reduce_partition(self):
        table_data = [
            {"key": x, "value": x**2}
            for x in range(8)
        ]

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_in")
        create("table", "//tmp/table_out")
        for row in table_data:
            write_table("<append=%true>//tmp/table_in", [row])
        raw_events = self.wait_for_raw_events(count=len(table_data), from_barrier=from_barrier)
        for event in raw_events:
            assert event["data_node_method@"] == "FinishChunk"

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        op = map_reduce(
            in_="//tmp/table_in",
            out="//tmp/table_out",
            mapper_command="cat",
            reducer_command="cat",
            reduce_by="key",
            spec={
                "map_job_count": 1,
                "max_partition_factor": 2,
                "partition_count": 8,
                "data_weight_per_reduce_job": 1,
            }
        )
        raw_events = self.wait_for_raw_events(count=31, from_barrier=from_barrier)

        paths = self._gather_events(raw_events, op, {
            "partition_map(0)": "map",
            "partition(1)": "part1",
            "partition(2)": "part2",
            "partition_reduce": "reduce",
        }, {
            "map": "partition_map",
            "part1": "partition",
            "part2": "partition",
            "reduce": "partition_reduce",
        })

        assert paths["map"]["read"] == ["//tmp/table_in"] * 8
        assert paths["map"]["write"] == ["<intermediate-0>"]
        assert paths["part1"]["read"] == ["<intermediate-0>"] * 2
        assert paths["part1"]["write"] == ["<intermediate-0>"] * 2
        assert paths["part2"]["read"] == ["<intermediate-0>"] * 4
        assert paths["part2"]["write"] == ["<intermediate-0>"] * 4
        assert paths["reduce"]["read"] == ["<intermediate-0>"] * 5
        assert paths["reduce"]["write"] == ["//tmp/table_out"] * 5

        assert sorted_dicts(read_table("//tmp/table_out")) == table_data

    @authors("gepardo")
    def test_map_reduce_intermediate(self):
        table_data = [
            {"key": x, "value": x**2}
            for x in range(8)
        ]

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_in")
        create("table", "//tmp/table_out")
        for row in table_data:
            write_table("<append=%true>//tmp/table_in", [row])
        raw_events = self.wait_for_raw_events(count=len(table_data), from_barrier=from_barrier)
        for event in raw_events:
            assert event["data_node_method@"] == "FinishChunk"

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        op = map_reduce(
            in_="//tmp/table_in",
            out="//tmp/table_out",
            mapper_command="cat",
            reducer_command="cat",
            reduce_by="key",
            spec={
                "map_job_count": 4,
                "partition_count": 1,
                "data_size_per_sort_job": 1,
            }
        )
        raw_events = self.wait_for_raw_events(count=28, from_barrier=from_barrier)

        paths = self._gather_events(raw_events, op, {
            "partition_map(0)": "map",
            "intermediate_sort": "intermediate",
            "sorted_reduce": "reduce",
        }, {
            "map": "partition_map",
            "intermediate": "intermediate_sort",
            "reduce": "sorted_reduce",
        })

        assert paths["map"]["read"] == ["//tmp/table_in"] * 8
        assert paths["map"]["write"] == ["<intermediate-0>"] * 4
        assert paths["intermediate"]["read"] == ["<intermediate-0>"] * 4
        assert paths["intermediate"]["write"] == ["<intermediate-0>"] * 4
        assert paths["reduce"]["read"] == ["<intermediate-0>"] * 4
        assert paths["reduce"]["write"] == ["//tmp/table_out"] * 4

        assert sorted_dicts(read_table("//tmp/table_out")) == table_data


class TestSortJobIOTracking(TestJobIOTrackingBase):
    ENABLE_MULTIDAEMON = False  # Check structured logs.
    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "sort_operation_options": {
                "min_uncompressed_block_size": 1,
                "min_partition_size": 1,
                "max_value_count_per_simple_sort_job": 100,
                "max_data_slices_per_job": 100,
                "spec_template": {
                    "use_new_sorted_pool": False,
                }
            },
        }
    }

    def _get_table_data(self):
        return [
            {"type": "quick", "complexity": "nlogn"},
            {"type": "heap", "complexity": "nlogn"},
            {"type": "merge", "complexity": "nlogn"},
            {"type": "bogo", "complexity": "n!n"},
            {"type": "selection", "complexity": "n^2"},
            {"type": "bubble", "complexity": "n^2"},
            {"type": "insertion", "complexity": "n^2"},
            {"type": "tree", "complexity": "nlogn"},
        ]

    def _write_input_table(self, table_data=None, single_chunk=True):
        if table_data is None:
            table_data = self._get_table_data()
        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_in")
        if single_chunk:
            write_table("//tmp/table_in", table_data)
            event_count = 1
        else:
            for row in table_data:
                write_table("<append=%true>//tmp/table_in", [row])
            event_count = len(table_data)
        raw_events = self.wait_for_raw_events(count=event_count, from_barrier=from_barrier)
        for event in raw_events:
            assert event["data_node_method@"] == "FinishChunk"

    def _check_output_table(self, table_data=None, sorted_by="type"):
        if table_data is None:
            table_data = self._get_table_data()
        table_data = sorted(table_data, key=lambda x: x[sorted_by])
        assert read_table("//tmp/table_out") == table_data

    def _get_job_counts_by_task_name(self, op):
        progress = get_operation(op.id)["progress"]
        result = {}
        for task in progress["tasks"]:
            result[task["task_name"]] = task["job_counter"]["total"]
        return result

    @authors("gepardo")
    def test_simple_sort_1_phase(self):
        self._write_input_table()

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_out")
        op = sort(
            in_="//tmp/table_in",
            out="//tmp/table_out",
            sort_by="type",
        )
        raw_events = self.wait_for_raw_events(count=2, from_barrier=from_barrier)

        paths = self._gather_events(raw_events, op)
        assert paths["simple_sort"]["read"] == ["//tmp/table_in"]
        assert paths["simple_sort"]["write"] == ["//tmp/table_out"]

        self._check_output_table()

    @authors("gepardo")
    def test_simple_sort_2_phase(self):
        self._write_input_table()

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_out")
        op = sort(
            in_="//tmp/table_in",
            out="//tmp/table_out",
            sort_by="type",
            spec={"data_weight_per_sort_job": 1},
        )
        job_counts = self._get_job_counts_by_task_name(op)
        sort_jobs = job_counts["simple_sort"]
        merge_jobs = job_counts["sorted_merge"]
        event_count = 3 * sort_jobs + merge_jobs
        raw_events = self.wait_for_raw_events(count=event_count, from_barrier=from_barrier)

        paths = self._gather_events(raw_events, op)
        assert paths["simple_sort"]["read"] == sort_jobs * ["//tmp/table_in"]
        assert paths["simple_sort"]["write"] == sort_jobs * ["<intermediate-0>"]
        assert paths["sorted_merge"]["read"] == sort_jobs * ["<intermediate-0>"]
        assert paths["sorted_merge"]["write"] == merge_jobs * ["//tmp/table_out"]

        self._check_output_table()

    @authors("gepardo")
    def test_sort_2_phase(self):
        self._write_input_table()

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_out")
        op = sort(
            in_="//tmp/table_in",
            out="//tmp/table_out",
            sort_by="type",
            spec={
                "partition_job_count": 2,
                "partition_count": 2,
            },
        )
        raw_events = self.wait_for_raw_events(count=10, from_barrier=from_barrier)

        paths = self._gather_events(raw_events, op, {
            "partition(0)": "partition",
            "final_sort": "final",
        }, {
            "partition": "partition",
            "final": "final_sort",
        })

        assert paths["partition"]["read"] == ["//tmp/table_in"] * 2
        assert paths["partition"]["write"] == ["<intermediate-0>"] * 2
        assert paths["final"]["read"] == ["<intermediate-0>"] * 4
        assert paths["final"]["write"] == ["//tmp/table_out"] * 2

        self._check_output_table()

    @authors("gepardo")
    def test_sort_2_phase_depth_2(self):
        self._write_input_table(single_chunk=False)

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_out")
        op = sort(
            in_="//tmp/table_in",
            out="//tmp/table_out",
            sort_by="type",
            spec={
                "partition_job_count": 2,
                "partition_count": 4,
                "max_partition_factor": 2,
            },
        )
        raw_events = self.wait_for_raw_events(count=24, from_barrier=from_barrier)

        paths = self._gather_events(raw_events, op, {
            "partition(0)": "part0",
            "partition(1)": "part1",
            "final_sort": "final",
        }, {
            "part0": "partition",
            "part1": "partition",
            "final": "final_sort",
        })

        assert paths["part0"]["read"] == ["//tmp/table_in"] * 8
        assert paths["part0"]["write"] == ["<intermediate-0>"] * 2
        assert paths["part1"]["read"] == ["<intermediate-0>"] * 4
        assert paths["part1"]["write"] == ["<intermediate-0>"] * 2
        assert paths["final"]["read"] == ["<intermediate-0>"] * 4
        assert paths["final"]["write"] == ["//tmp/table_out"] * 4

        self._check_output_table()

    @authors("gepardo")
    def test_sort_3_phase(self):
        self._write_input_table(single_chunk=False)

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_out")
        op = sort(
            in_="//tmp/table_in",
            out="//tmp/table_out",
            sort_by="type",
            spec={
                "partition_job_count": 4,
                "partition_count": 2,
                "data_weight_per_sort_job": 1,
                "partition_job_io": {
                    "table_writer": {
                        "desired_chunk_size": 1,
                        "block_size": 1,
                    }
                },
            },
        )
        tasks = {task["task_name"] for task in get_operation(op.id)["progress"]["tasks"]}
        assert tasks == {"partition(0)", "intermediate_sort", "sorted_merge"}
        raw_events = self.wait_for_raw_events(count=48, from_barrier=from_barrier)

        paths = self._gather_events(raw_events, op, {
            "partition(0)": "partition",
            "intermediate_sort": "intermediate",
            "sorted_merge": "merge",
        }, {
            "partition": "partition",
            "intermediate": "intermediate_sort",
            "merge": "sorted_merge",
        })

        assert paths["partition"]["read"] == ["//tmp/table_in"] * 8
        assert paths["partition"]["write"] == ["<intermediate-0>"] * 8
        assert paths["intermediate"]["read"] == ["<intermediate-0>"] * 8
        assert paths["intermediate"]["write"] == ["<intermediate-0>"] * 8
        assert paths["merge"]["read"] == ["<intermediate-0>"] * 8
        assert paths["merge"]["write"] == ["//tmp/table_out"] * 8

        self._check_output_table()

    @authors("gepardo")
    def test_maniac(self):
        table_data = [{"key": 42, "value": i} for i in range(20)]
        self._write_input_table(table_data, single_chunk=False)

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_out")
        op = sort(
            in_="//tmp/table_in",
            out="//tmp/table_out",
            sort_by=["key"],
            spec={
                "partition_job_count": 2,
                "partition_count": 5,
                "data_weight_per_sort_job": 1,
            },
        )
        raw_events = self.wait_for_raw_events(count=26, from_barrier=from_barrier)

        paths = self._gather_events(raw_events, op, {
            "partition(0)": "partition",
            "unordered_merge": "merge",
        }, {
            "partition": "partition",
            "merge": "unordered_merge",
        })

        assert paths["partition"]["read"] == ["//tmp/table_in"] * 20
        assert paths["partition"]["write"] == ["<intermediate-0>"] * 2
        assert paths["merge"]["read"] == ["<intermediate-0>"] * 2
        assert paths["merge"]["write"] == ["//tmp/table_out"] * 2

        assert sorted(read_table("//tmp/table_out"), key=lambda e: e["value"]) == table_data

    @authors("gepardo")
    def test_sort_multitable(self):
        table_data1 = [
            {"id": 1, "name": "cat"},
            {"id": 3, "name": "dog"},
        ]
        table_data2 = [
            {"id": 2, "name": "python"},
            {"id": 4, "name": "rattlesnake"},
        ]

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_in1")
        create("table", "//tmp/table_in2")
        write_table("//tmp/table_in1", table_data1)
        write_table("//tmp/table_in2", table_data2)
        raw_events = self.wait_for_raw_events(count=2, from_barrier=from_barrier)
        for event in raw_events:
            assert event["data_node_method@"] == "FinishChunk"

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_out")
        op = sort(
            in_=["//tmp/table_in1", "//tmp/table_in2"],
            out="//tmp/table_out",
            sort_by=["id"],
        )
        raw_events = self.wait_for_raw_events(count=3, from_barrier=from_barrier)

        paths = self._gather_events(raw_events, op)
        assert sorted(paths["simple_sort"]["read"]) == ["//tmp/table_in1", "//tmp/table_in2"]
        assert paths["simple_sort"]["write"] == ["//tmp/table_out"]

        assert sorted(table_data1 + table_data2, key=lambda e: e["id"]) == \
            read_table("//tmp/table_out")

##################################################################


class TestRemoteCopyIOTrackingBase(TestNodeIOTrackingBase):
    ENABLE_MULTIDAEMON = False  # Check structured logs.
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    NUM_REMOTE_CLUSTERS = 1

    NUM_MASTERS_REMOTE_0 = 1
    NUM_SCHEDULERS_REMOTE_0 = 1
    NUM_NODES_REMOTE_0 = 1

    REMOTE_CLUSTER_NAME = "remote_0"

    def setup_method(self, method):
        super(TestRemoteCopyIOTrackingBase, self).setup_method(method)
        update_nodes_dynamic_config({
            "io_tracker": {
                "enable": True,
                "enable_raw": True,
                "period_quant": 10,
                "aggregation_period": 5000,
            }
        }, driver=self.remote_driver)

    @classmethod
    def setup_class(cls):
        super(TestRemoteCopyIOTrackingBase, cls).setup_class()
        cls.remote_driver = get_driver(cluster=cls.REMOTE_CLUSTER_NAME)

    def _check_remote_copy_tags(self, event, op):
        assert event["pool_tree@"] == "default"
        assert event["operation_id"] == op.id
        assert event["operation_type@"] == "remote_copy"
        assert "job_id" in event
        assert event["job_type@"] == "remote_copy"
        assert event["task_name@"] == "remote_copy"
        assert event["bytes"] > 0
        assert event["io_requests"] > 0
        assert event["user@"] == "root"
        assert "object_id" in event
        assert event["account@"] == "tmp"
        assert "compression_codec@" not in event
        assert "erasure_codec@" in event


class TestRemoteCopyIOTracking(TestRemoteCopyIOTrackingBase):
    ENABLE_MULTIDAEMON = False  # Check structured logs.
    USE_DYNAMIC_TABLES = True
    ENABLE_BULK_INSERT = True

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1,
            "default_journal_read_quorum": 1,
            "default_journal_write_quorum": 1,
            "default_journal_replication_factor": 1,
        }
    }

    DELTA_DYNAMIC_MASTER_CONFIG_0 = {
        "cypress_manager": {
            "default_table_replication_factor": 1,
            "default_journal_read_quorum": 1,
            "default_journal_write_quorum": 1,
            "default_journal_replication_factor": 1,
        }
    }

    @authors("gepardo")
    def test_remote_copy(self):
        table_data = [
            {"id": 1, "name": "cat"},
            {"id": 1, "name": "python"},
            {"id": 2, "name": "dog"},
            {"id": 2, "name": "rattlesnake"},
        ]

        create("table", "//tmp/table_in", driver=self.remote_driver)
        write_table("//tmp/table_in", table_data, driver=self.remote_driver)

        from_barrier = self.write_log_barrier(self.get_node_address())
        from_barrier_remote = self.write_log_barrier(self.get_node_address(cluster_index=1), cluster_index=1)
        create("table", "//tmp/table_out")
        op = remote_copy(
            in_="//tmp/table_in",
            out="//tmp/table_out",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
        )
        raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier)
        raw_events_remote = self.wait_for_raw_events(count=1, from_barrier=from_barrier_remote, cluster_index=1)
        event_local = raw_events[0]
        event_remote = raw_events_remote[0]

        self._check_remote_copy_tags(event_local, op)
        assert event_local["data_node_method@"] == "FinishChunk"
        assert event_local["object_path"] == "//tmp/table_out"

        self._check_remote_copy_tags(event_remote, op)
        assert event_remote["data_node_method@"] in ["GetBlockSet", "GetBlockRange"]
        assert event_remote["object_path"] == "//tmp/table_in"

        assert read_table("//tmp/table_out") == table_data

    @authors("gepardo")
    def test_remote_copy_dynamic_table(self):
        table_data = [
            {"id": 1, "name": "cat"},
            {"id": 2, "name": "python"},
            {"id": 3, "name": "dog"},
            {"id": 4, "name": "rattlesnake"},
        ]
        schema = yson.YsonList([
            {"name": "id", "type": "int64", "sort_order": "ascending"},
            {"name": "name", "type": "string"},
        ])

        sync_create_cells(1)
        sync_create_cells(1, driver=self.remote_driver)

        create_dynamic_table("//tmp/dyntable_in", schema=schema, driver=self.remote_driver)
        sync_mount_table("//tmp/dyntable_in", driver=self.remote_driver)
        insert_rows("//tmp/dyntable_in", table_data, driver=self.remote_driver)
        sync_unmount_table("//tmp/dyntable_in", driver=self.remote_driver)

        create_dynamic_table("//tmp/dyntable_out", schema=schema)

        from_barrier = self.write_log_barrier(self.get_node_address())
        from_barrier_remote = self.write_log_barrier(self.get_node_address(cluster_index=1), cluster_index=1)
        create("table", "//tmp/table_out")
        op = remote_copy(
            in_="//tmp/dyntable_in",
            out="//tmp/dyntable_out",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
        )
        raw_events = self.wait_for_raw_events(
            count=1, from_barrier=from_barrier,
            filter=lambda event: event.get("data_node_method@") == "FinishChunk")
        raw_events_remote = self.wait_for_raw_events(
            count=1, from_barrier=from_barrier_remote, cluster_index=1,
            filter=lambda event: event.get("data_node_method@") in ["GetBlockSet", "GetBlockRange"])
        event_local = raw_events[0]
        event_remote = raw_events_remote[0]

        self._check_remote_copy_tags(event_local, op)
        self._check_remote_copy_tags(event_remote, op)

        assert event_local["object_path"] == "//tmp/dyntable_out"
        assert event_remote["object_path"] == "//tmp/dyntable_in"

        assert read_table("//tmp/dyntable_out") == table_data


class TestRemoteCopyErasureIOTracking(TestRemoteCopyIOTrackingBase):
    ENABLE_MULTIDAEMON = False  # Check structured logs.
    NUM_NODES = 6
    NUM_NODES_REMOTE_0 = 6

    TABLE_DATA = [
        {"id": 1, "name": "cat"},
        {"id": 2, "name": "python"},
        {"id": 3, "name": "dog"},
        {"id": 4, "name": "rattlesnake"},
    ]

    def _write_remote_erasure_table(self, path):
        from_barriers = [
            self.write_log_barrier(self.get_node_address(node_id, cluster_index=1), cluster_index=1)
            for node_id in range(self.NUM_NODES_REMOTE_0)]
        create("table", path, attributes={"erasure_codec": "reed_solomon_3_3"},
               driver=self.remote_driver)
        write_table(path, self.TABLE_DATA, driver=self.remote_driver)
        for node_id in range(self.NUM_NODES):
            raw_events = self.wait_for_raw_events(count=1, node_id=node_id,
                                                  cluster_index=1, from_barrier=from_barriers[node_id])
            assert raw_events[0]["data_node_method@"] == "FinishChunk"

    @authors("gepardo")
    def test_remote_copy_erasure(self):
        self._write_remote_erasure_table("//tmp/table_in")

        from_barriers = [write_log_barrier(self.get_node_address(node_id)) for node_id in range(self.NUM_NODES)]
        from_barriers_remote = [
            self.write_log_barrier(self.get_node_address(node_id, cluster_index=1), cluster_index=1)
            for node_id in range(self.NUM_NODES_REMOTE_0)]

        create("table", "//tmp/table_out", attributes={"erasure_codec": "reed_solomon_3_3"})
        op = remote_copy(
            in_="//tmp/table_in",
            out="//tmp/table_out",
            spec={"cluster_name": self.REMOTE_CLUSTER_NAME},
        )

        time.sleep(3.0)

        for node_id in range(self.NUM_NODES):
            raw_events = self.wait_for_raw_events(count=1, node_id=node_id, from_barrier=from_barriers[node_id])
            self._check_remote_copy_tags(raw_events[0], op)
            assert raw_events[0]["data_node_method@"] == "FinishChunk"
            assert raw_events[0]["object_path"] == "//tmp/table_out"

        was_data_read = True
        for node_id in range(self.NUM_NODES_REMOTE_0):
            raw_events = self.read_raw_events(node_id=node_id, cluster_index=1, from_barrier=from_barriers_remote[node_id])
            if not raw_events:
                continue
            assert len(raw_events) == 1
            was_data_read = True
            self._check_remote_copy_tags(raw_events[0], op)
            assert raw_events[0]["data_node_method@"] in ["GetBlockSet", "GetBlockRange"]
            assert raw_events[0]["object_path"] == "//tmp/table_in"
        assert was_data_read

        assert read_table("//tmp/table_out") == self.TABLE_DATA

    def _enable_direct_maintenance_flag_set(self):
        path = "//sys/@config/node_tracker/forbid_maintenance_attribute_writes"
        local = get(path)
        remote = get(path, driver=self.remote_driver)
        yt_commands.set(path, False)
        yt_commands.set(path, False, driver=self.remote_driver)
        return local, remote

    def _restore_maintenance_flag_config(self, old_value):
        path = "//sys/@config/node_tracker/forbid_maintenance_attribute_writes"
        local, remote = old_value
        yt_commands.set(path, local)
        yt_commands.set(path, remote, driver=self.remote_driver)

    @authors("gepardo")
    def test_remote_copy_erasure_repair(self):
        old_value = self._enable_direct_maintenance_flag_set()
        try:
            self._write_remote_erasure_table("//tmp/table_in")

            yt_commands.set("//sys/@config/chunk_manager/enable_chunk_replicator", False,
                            driver=self.remote_driver)
            yt_commands.set("//sys/@config/chunk_manager/enable_chunk_replicator", False)
            time.sleep(1.0)

            from_barriers = [write_log_barrier(self.get_node_address(node_id)) for node_id in range(self.NUM_NODES)]

            chunk_id = get_singular_chunk_id("//tmp/table_in", driver=self.remote_driver)
            chunk_replicas = get("#{}/@stored_replicas".format(chunk_id), driver=self.remote_driver)
            node_to_ban = str(chunk_replicas[0])

            create("table", "//tmp/table_out", attributes={"erasure_codec": "reed_solomon_3_3"})
            op = remote_copy(
                in_="//tmp/table_in",
                out="//tmp/table_out",
                spec={
                    "cluster_name": self.REMOTE_CLUSTER_NAME,
                    "max_failed_job_count": 1,
                    "delay_in_copy_chunk": 5000,
                    "erasure_chunk_repair_delay": 2000,
                    "repair_erasure_chunks": True,
                    "chunk_availability_policy": "repairable",
                },
                track=False,
            )
            wait(lambda: len(op.get_running_jobs()) == 1)

            # We need to ban node after job start because CA will not start job until
            # all the parts were found.
            set_node_banned(node_to_ban, True,  driver=self.remote_driver)

            op.track()
            time.sleep(3.0)

            write_count = 0
            read_count = 0
            for node_id in range(self.NUM_NODES):
                raw_events = self.wait_for_raw_events(count=1, node_id=node_id, from_barrier=from_barriers[node_id],
                                                      check_event_count=False)
                local_write_count = 0
                for event in raw_events:
                    self._check_remote_copy_tags(event, op)
                    assert event["data_node_method@"] in ["GetBlockSet", "GetBlockRange", "FinishChunk"]
                    assert event["object_path"] == "//tmp/table_out"
                    if event["data_node_method@"] == "FinishChunk":
                        local_write_count += 1
                        write_count += 1
                    else:
                        read_count += 1
                assert local_write_count > 0
            assert write_count == 6
            assert read_count >= 3

            set_node_banned(node_to_ban, False, driver=self.remote_driver)
        finally:
            self._restore_maintenance_flag_config(old_value)


##################################################################


class TestUserJobIOTracking(TestJobIOTrackingBase):
    ENABLE_MULTIDAEMON = False  # Check structured logs.
    USE_PORTO = True

    DELTA_NODE_CONFIG = {
        "data_node": {
            "block_cache": {
                "compressed_data": {
                    "capacity": 0,
                },
                "uncompressed_data": {
                    "capacity": 0,
                },
            },
        },
        "exec_node": {
            "job_proxy": {
                "job_proxy_heartbeat_period": 100,
            },
        },
    }

    @authors("gepardo")
    @pytest.mark.timeout(300)
    def test_simple(self):
        table_data = [{"a": 42}]

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        create("table", "//tmp/table_in")
        create("table", "//tmp/table_out")
        write_table("//tmp/table_in", table_data)
        raw_events = self.wait_for_raw_events(count=1, from_barrier=from_barrier)
        assert raw_events[0]["data_node_method@"] == "FinishChunk"

        os.makedirs(self.default_disk_path)
        os.chmod(self.default_disk_path, 0o777)

        from_barrier = self.write_log_barrier(self.get_node_address(), "Barrier")
        op = yt_commands.map(
            in_="//tmp/table_in",
            out="//tmp/table_out",
            command="""
                dd if=/dev/urandom of={0}/myfile count=400 bs=1024 oflag=direct && \
                dd if={0}/myfile of={0}/myfile2 count=400 bs=1024 iflag=direct oflag=direct && \
                dd if=/dev/urandom of={0}/myfile3 count=400 bs=1024 oflag=direct && \
                sync && \
                cat""".format(self.default_disk_path),
        )

        # If this test failed on your virtual machine, please check your local porto version.
        # You can call a request to porto. For example:
        #
        # portoctl get juggler_client io_read_ops
        #
        # Correct response: 9135202132091
        # Incorrect response: Cannot get io_read_ops: InvalidProperty:(Unknown container property: io_read_ops)
        def check_statistic():
            user_job_statistics = get(op.get_path() + "/@progress/job_statistics")["user_job"]

            if "block_io" not in user_job_statistics:
                print_debug("Cannot get block io statistics for job")
                return False

            block_io_statistic = user_job_statistics["block_io"]
            raw_events = self.wait_for_raw_events(
                count=1,
                from_barrier=from_barrier,
                filter=lambda e: e.get("job_io_kind@") == "user_job",
                check_event_count=False)

            sum_bytes = {"read": 0, "write": 0}
            sum_io_requests = {"read": 0, "write": 0}
            for event in raw_events:
                self._validate_basic_tags(event, op.id, "map", users=["root"], allow_zero=True)
                sum_bytes[event["direction@"]] += event["bytes"]
                sum_io_requests[event["direction@"]] += event["io_requests"]

            return block_io_statistic["bytes_read"]["$"]["completed"]["map"]["sum"] == sum_bytes["read"] and \
                block_io_statistic["bytes_written"]["$"]["completed"]["map"]["sum"] == sum_bytes["write"] and \
                block_io_statistic["io_total"]["$"]["completed"]["map"]["sum"] == sum_io_requests["read"] and \
                block_io_statistic["io_total"]["$"]["completed"]["map"]["sum"] == sum_io_requests["write"]

        wait(check_statistic)
