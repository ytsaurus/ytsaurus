from .test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_commands import (
    authors, wait, ls, get, set, insert_rows, remount_table,
    sync_create_cells, sync_mount_table, sync_reshard_table,
    sync_flush_table, sync_unmount_table, create_dynamic_table,
    disable_tablet_cells_on_node, freeze_table, update_nodes_dynamic_config)

import dateutil
import builtins

#################################################################


class TestBackgroundActivityOrchid(TestSortedDynamicTablesBase):
    NUM_NODES = 3
    USE_DYNAMIC_TABLES = True

    def _fetch_orchid(self, node, source, kind, table_paths):
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

    @authors("akozhikhov")
    def test_compaction_orchid(self):
        NUM_TABLES = 3
        nodes = ls("//sys/cluster_nodes")
        for node in nodes[1:]:
            disable_tablet_cells_on_node(node, "test compact orchid")
        node = nodes[0]
        table_names = [f"//tmp/t{i}" for i in range(NUM_TABLES)]

        sync_create_cells(1)

        for table_idx in range(NUM_TABLES):
            create_dynamic_table(
                "//tmp/t{0}".format(table_idx),
                schema=[
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
            )

        empty_task_dict = {
            "pending_task_count": 0,
            "running_task_count": 0,
            "failed_task_count": 0,
            "completed_task_count": 0,
            "pending_tasks": [],
            "running_tasks": [],
            "failed_tasks": [],
            "completed_tasks": [],
        }
        assert self._fetch_orchid(node, "store_compactor", "compaction", table_names) == empty_task_dict
        assert self._fetch_orchid(node, "store_compactor", "partitioning", table_names) == empty_task_dict

        for table_idx in range(NUM_TABLES):
            set(
                "//tmp/t{0}/@enable_compaction_and_partitioning".format(table_idx),
                False,
            )
            sync_mount_table("//tmp/t{0}".format(table_idx))

        for table_idx in range(NUM_TABLES):
            for i in range(5):
                insert_rows(
                    "//tmp/t{0}".format(table_idx),
                    [{"key": j, "value": str(j)} for j in range(i * 10, (i + 1) * 10)],
                )
                sync_flush_table("//tmp/t{0}".format(table_idx))
            set("//tmp/t{0}/@enable_compaction_and_partitioning".format(table_idx), True)
            remount_table("//tmp/t{0}".format(table_idx))

        tablet_ids = [get(f"//tmp/t{i}/@tablets/0/tablet_id") for i in range(NUM_TABLES)]

        def _compaction_task_completed():
            compaction_tasks = self._fetch_orchid(node, "store_compactor", "compaction", table_names)
            for task in compaction_tasks["completed_tasks"]:
                self._drop_unrecognized(
                    task,
                    {"tablet_id", "store_count", "table_path", "reason", "tablet_cell_bundle"})
            compaction_tasks["completed_tasks"].sort(key=lambda x: x["table_path"])

            expected_compaction_tasks = {
                "pending_task_count": 0,
                "running_task_count": 0,
                "failed_task_count": 0,
                "completed_task_count": NUM_TABLES,
                "pending_tasks": [],
                "running_tasks": [],
                "failed_tasks": [],
                "completed_tasks": [
                    {
                        "tablet_id": tablet_ids[i],
                        "store_count": 5,
                        "table_path": f"//tmp/t{i}",
                        "reason": "regular",
                        "tablet_cell_bundle": "default",
                    }
                    for i in range(NUM_TABLES)],
            }

            return compaction_tasks == expected_compaction_tasks

        wait(_compaction_task_completed)
        assert self._fetch_orchid(node, "store_compactor", "partitioning", table_names) == empty_task_dict

    @authors("akozhikhov")
    def test_partitioning_orchid(self):
        nodes = ls("//sys/cluster_nodes")
        for node in nodes[1:]:
            disable_tablet_cells_on_node(node, "test partitioning orchid")
        node = nodes[0]

        sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        self._create_partitions(partition_count=2)

        # Now add store to eden
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i} for i in range(2, 6)])
        sync_flush_table("//tmp/t")
        assert len(get("//tmp/t/@chunk_ids")) == 3
        assert get("#{}/@eden".format(get("//tmp/t/@chunk_ids/{0}".format(2))))

        set("//tmp/t/@enable_compaction_and_partitioning", True)
        set("//tmp/t/@forced_compaction_revision", 1)
        remount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        def _partitioning_task_completed():
            expected_partitioning_tasks = {
                "pending_task_count": 0,
                "running_task_count": 0,
                "failed_task_count": 0,
                "completed_task_count": 1,
                "pending_tasks": [],
                "running_tasks": [],
                "failed_tasks": [],
                "completed_tasks": [{"tablet_id": tablet_id, "store_count": 1}],
            }
            partitioning_tasks = self._fetch_orchid(node, "store_compactor", "partitioning", ["//tmp/t"])
            if partitioning_tasks["completed_task_count"] != 1:
                return False
            self._drop_unrecognized(
                partitioning_tasks["completed_tasks"][0],
                {"tablet_id", "store_count"})
            return partitioning_tasks == expected_partitioning_tasks

        wait(_partitioning_task_completed)

    @authors("ifsmirnov")
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

        def _test(non_empty_state, replication_factor, expected_size, cell_id):
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
                            dateutil.parser.parse(task["start_time"]) <= dateutil.parser.parse(task["finish_time"])):
                        return False

                return True

            update_failed_task_count(10)
            wait(_check)

        _test("failed", 10, 10, cell_id_failed)
        _test("completed", 3, 1, cell_id_completed)
