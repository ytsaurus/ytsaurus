from .test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_commands import (
    authors, wait, ls, get, set, insert_rows, remount_table,
    sync_create_cells, sync_mount_table, sync_reshard_table,
    sync_flush_table, sync_unmount_table, disable_tablet_cells_on_node,
    freeze_table, update_nodes_dynamic_config)

import dateutil
import builtins
import pytest


EMPTY_TASK_DICT = {
    "pending_task_count": 0,
    "running_task_count": 0,
    "failed_task_count": 0,
    "completed_task_count": 0,
    "pending_tasks": [],
    "running_tasks": [],
    "failed_tasks": [],
    "completed_tasks": [],
}


#################################################################


@pytest.mark.enabled_multidaemon
class TestBackgroundActivityOrchid(TestSortedDynamicTablesBase):
    ENABLE_MULTIDAEMON = True
    NUM_NODES = 3
    USE_DYNAMIC_TABLES = True

    @staticmethod
    def _fetch_orchid(node, source, kind, table_paths):
        result = get(f"//sys/cluster_nodes/{node}/orchid/{source}/{kind}_tasks")
        for state in "pending", "running", "failed", "completed":
            tasks_key = f"{state}_tasks"
            count_key = f"{state}_task_count"
            assert len(result[tasks_key]) == result[count_key]
            result[tasks_key] = list(filter(lambda task: task["table_path"] in table_paths, result[tasks_key]))
            result[count_key] = len(result[tasks_key])

        return result

    @staticmethod
    def _drop_unrecognized(dct, known_keys):
        for k in builtins.set(dct.keys()).difference(known_keys):
            del dct[k]

    @staticmethod
    def _check_statistics_zero(statistics):
        for value in statistics.values():
            if value != 0:
                return False

        return True

    @staticmethod
    def _check_statistics_greater_than_zero(statistics):
        for value in statistics.values():
            if value <= 0:
                return False

        return True

    @staticmethod
    def _check_statistics_eq(lhs, rhs):
        lhs_compressed_data_size = lhs["compressed_data_size"]
        rhs_compressed_data_size = rhs["compressed_data_size"]

        lhs.pop("compressed_data_size")
        rhs.pop("compressed_data_size")

        return lhs == rhs and lhs_compressed_data_size == pytest.approx(rhs_compressed_data_size, rel=0.05)

    @staticmethod
    def _sort_and_drop_unrecognized_in_tasks(tasks):
        tasks["completed_tasks"].sort(key=lambda task: task["table_path"])
        for task in tasks["completed_tasks"]:
            task["store_ids"].sort()

            TestBackgroundActivityOrchid._drop_unrecognized(
                task,
                {"tablet_id", "store_ids", "table_path", "reason", "tablet_cell_bundle", "hunk_chunk_count_by_reason"})

    @staticmethod
    def _check_orchid_tasks(tasks):
        for task in tasks["completed_tasks"]:
            if not (
                task["trace_id"] == task["task_id"] and
                task["mount_revision"] == get(f"{task['table_path']}/@tablets/0").get("mount_revision"),
                dateutil.parser.parse(task["start_time"]) <= dateutil.parser.parse(task["finish_time"]) and
                "duration" in task and
                "task_priority" in task and
                "discard_stores" in task["task_priority"] and
                "slack" in task["task_priority"] and
                "effect" in task["task_priority"] and
                "future_effect" in task["task_priority"] and
                "random" in task["task_priority"] and
                task["processed_input_rows_ratio"] == pytest.approx(1, abs=1e-6) and
                TestBackgroundActivityOrchid._check_statistics_eq(task["processed_reader_statistics"], task["total_reader_statistics"]) and
                TestBackgroundActivityOrchid._check_statistics_greater_than_zero(task["processed_reader_statistics"]) and
                TestBackgroundActivityOrchid._check_statistics_greater_than_zero(task["processed_writer_statistics"])
            ):
                return False

        return True

    def _setup_tables(self, num_tables):
        nodes = ls("//sys/cluster_nodes")
        for node in nodes[1:]:
            disable_tablet_cells_on_node(node, "test background activity orchid")
        sync_create_cells(1)

        table_paths = [f"//tmp/t{i}" for i in range(num_tables)]
        for table_path in table_paths:
            self._create_simple_table(table_path)

        return table_paths, nodes[0]

    def _check_compaction_partitioning(self, table_paths, node, kind, store_ids, reason):
        num_tables = len(table_paths)
        tablet_ids = [get(f"{table_path}/@tablets/0/tablet_id") for table_path in table_paths]

        def _tasks_completed():
            tasks = self._fetch_orchid(node, "store_compactor", kind, table_paths)
            if not self._check_orchid_tasks(tasks):
                return False

            self._sort_and_drop_unrecognized_in_tasks(tasks)

            expected_compaction_tasks = {
                "pending_task_count": 0,
                "running_task_count": 0,
                "failed_task_count": 0,
                "completed_task_count": num_tables,
                "pending_tasks": [],
                "running_tasks": [],
                "failed_tasks": [],
                "completed_tasks": [
                    {
                        "tablet_id": tablet_ids[i],
                        "store_ids": store_ids[i],
                        "table_path": table_paths[i],
                        "reason": reason,
                        "tablet_cell_bundle": "default",
                        "hunk_chunk_count_by_reason": {
                            "none": 0,
                            "forced_compaction": 0,
                            "garbage_ratio_too_high": 0,
                            "hunk_chunk_too_small": 0,
                        },
                    }
                    for i in range(num_tables)],
            }

            self._sort_and_drop_unrecognized_in_tasks(expected_compaction_tasks)

            return tasks == expected_compaction_tasks

        wait(_tasks_completed)

    @authors("akozhikhov", "dave11ar")
    def test_compaction_orchid(self):
        NUM_TABLES = 3
        table_paths, node = self._setup_tables(NUM_TABLES)

        assert self._fetch_orchid(node, "store_compactor", "compaction", table_paths) == EMPTY_TASK_DICT
        assert self._fetch_orchid(node, "store_compactor", "partitioning", table_paths) == EMPTY_TASK_DICT

        for table_path in table_paths:
            set(f"{table_path}/@enable_compaction_and_partitioning", False)
            sync_mount_table(table_path)

        store_ids = []

        for table_path in table_paths:
            for i in range(5):
                insert_rows(
                    table_path,
                    [{"key": j, "value": str(j)} for j in range(i * 10, (i + 1) * 10)],
                )
                sync_flush_table(table_path)

            store_ids.append(get(f"{table_path}/@chunk_ids"))
            set(f"{table_path}/@enable_compaction_and_partitioning", True)
            remount_table(table_path)

        self._check_compaction_partitioning(table_paths, node, "compaction", store_ids, "regular")
        assert self._fetch_orchid(node, "store_compactor", "partitioning", table_paths) == EMPTY_TASK_DICT

    @authors("akozhikhov", "dave11ar")
    def test_partitioning_orchid(self):
        NUM_TABLES = 3
        table_paths, node = self._setup_tables(NUM_TABLES)

        for table_path in table_paths:
            self._create_partitions(partition_count=2, table_path=table_path)

        store_ids = []
        for table_path in table_paths:
            # Now add store to eden
            sync_mount_table(table_path)
            insert_rows(table_path, [{"key": i} for i in range(2, 6)])
            sync_flush_table(table_path)
            assert len(get(f"{table_path}/@chunk_ids")) == 3
            eden_chunk_id = get(f"{table_path}/@chunk_ids/2")
            store_ids.append([eden_chunk_id])
            assert get(f"#{eden_chunk_id}/@eden")

            set(f"{table_path}/@enable_compaction_and_partitioning", True)
            set(f"{table_path}/@forced_compaction_revision", 1)
            remount_table(table_path)

        self._check_compaction_partitioning(table_paths, node, "partitioning", store_ids, "forced")

    @authors("ifsmirnov", "dave11ar")
    def test_running_tasks(self):
        cell_id = sync_create_cells(1)[0]
        node = get(f"#{cell_id}/@peers/0/address")
        self._create_simple_table("//tmp/t_running")
        sync_reshard_table("//tmp/t_running", [[], [1]])
        sync_mount_table("//tmp/t_running")
        insert_rows("//tmp/t_running", [{"key": 0}, {"key": 1}])
        sync_unmount_table("//tmp/t_running")
        set("//tmp/t_running/@throttlers", {"tablet_stores_update": {"limit": 0.0000001}})
        set("//tmp/t_running/@forced_compaction_revision", 1)
        sync_mount_table("//tmp/t_running")

        def _check():
            orchid = self._fetch_orchid(node, "store_compactor", "compaction", ["//tmp/t_running"])
            pending = orchid["pending_tasks"]
            running = orchid["running_tasks"]
            failed = orchid["failed_tasks"]
            completed = orchid["completed_tasks"]

            if len(pending) != 0:
                return False

            if len(running) != 1:
                return False
            assert "start_time" in running[0]
            assert "duration" in running[0]
            assert "finish_time" not in running[0]

            # There may be failed tasks due to conflits with partition sampling.
            if len(completed) < 1:
                assert len(failed) > 0
                return False

            for task in completed:
                assert "start_time" in task
                assert "duration" in task
                assert "finish_time" in task

            return True

        wait(_check)

    @authors("dave11ar")
    def test_flush_orchid(self):
        def update_failed_task_count(count):
            update_nodes_dynamic_config({
                "tablet_node": {
                    "store_flusher": {
                        "orchid": {
                            "max_failed_task_count": count,
                        },
                    },
                },
            })

        cell_id_failed, cell_id_completed = sync_create_cells(2)

        def _test(non_empty_state, replication_factor, expected_size, cell_id, task_checker):
            node = get(f"#{cell_id}/@peers/0/address")
            table_path = f"//tmp/{non_empty_state}"

            self._create_simple_table(
                table_path,
                replication_factor=replication_factor,
                chunk_writer={"upload_replication_factor": replication_factor})
            sync_mount_table(table_path, cell_id=cell_id)
            tablet = get(f"{table_path}/@tablets/0")

            insert_rows(table_path, [{"key": 0, "value": "v"}])
            freeze_table(table_path)
            update_failed_task_count(50)

            def _check():
                orchid = self._fetch_orchid(node, "store_flusher", "flush", [table_path])

                for state in "pending", "running", "failed", "completed":
                    if orchid[f"{state}_task_count"] != (expected_size if state == non_empty_state else 0):
                        return False

                for task in orchid[f"{non_empty_state}_tasks"]:
                    if not ("task_id" in task and
                            "store_id" in task and
                            task["task_id"] == task["trace_id"] and
                            task["tablet_id"] == tablet.get("tablet_id") and
                            task["mount_revision"] == tablet.get("mount_revision") and
                            task["table_path"] == table_path and
                            task["tablet_cell_bundle"] == "default" and
                            dateutil.parser.parse(task["start_time"]) <= dateutil.parser.parse(task["finish_time"]) and
                            "processed_writer_statistics" in task and
                            task_checker(task)):
                        return False

                return True

            update_failed_task_count(10)
            wait(_check)

        _test("failed", 10, 10, cell_id_failed, lambda _: True)
        _test("completed", 3, 1, cell_id_completed, lambda task: (
            "processed_writer_statistics" in task and
            self._check_statistics_greater_than_zero(task["processed_writer_statistics"])
        ))
