from .test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_commands import (
    authors, wait, ls, get, set, insert_rows, remount_table,
    sync_create_cells, sync_mount_table, sync_reshard_table,
    sync_flush_table, sync_unmount_table, create_dynamic_table,
    disable_tablet_cells_on_node)

import builtins

#################################################################


class TestStoreCompactorOrchid(TestSortedDynamicTablesBase):
    NUM_NODES = 3
    USE_DYNAMIC_TABLES = True

    def _fetch_orchid(self, node, kind, table_paths):
        result = get(f"//sys/cluster_nodes/{node}/orchid/store_compactor/{kind}_tasks")
        for state in "pending_tasks", "running_tasks", "finished_tasks":
            assert len(result[state]) == result[state[:-1] + "_count"]
            result[state] = [task for task in result[state] if task["table_path"] in table_paths]
            result[state[:-1] + "_count"] = len(result[state])
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
            "finished_task_count": 0,
            "pending_tasks": [],
            "running_tasks": [],
            "finished_tasks": [],
        }
        assert self._fetch_orchid(node, "compaction", table_names) == empty_task_dict
        assert self._fetch_orchid(node, "partitioning", table_names) == empty_task_dict

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

        def _compaction_task_finished():
            compaction_tasks = self._fetch_orchid(node, "compaction", table_names)
            for task in compaction_tasks["finished_tasks"]:
                self._drop_unrecognized(
                    task,
                    {"tablet_id", "store_count", "table_path", "reason", "tablet_cell_bundle"})
            compaction_tasks["finished_tasks"].sort(key=lambda x: x["table_path"])

            expected_compaction_tasks = {
                "pending_task_count": 0,
                "running_task_count": 0,
                "finished_task_count": NUM_TABLES,
                "pending_tasks": [],
                "running_tasks": [],
                "finished_tasks": [
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

        wait(lambda: _compaction_task_finished())
        assert self._fetch_orchid(node, "partitioning", table_names) == empty_task_dict

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

        def _partitioning_task_finished():
            expected_partitioning_tasks = {
                "pending_task_count": 0,
                "running_task_count": 0,
                "finished_task_count": 1,
                "pending_tasks": [],
                "running_tasks": [],
                "finished_tasks": [{"tablet_id": tablet_id, "store_count": 1}],
            }
            partitioning_tasks = self._fetch_orchid(node, "partitioning", ["//tmp/t"])
            if partitioning_tasks["finished_task_count"] != 1:
                return False
            self._drop_unrecognized(
                partitioning_tasks["finished_tasks"][0],
                {"tablet_id", "store_count"})
            return partitioning_tasks == expected_partitioning_tasks

        wait(lambda: _partitioning_task_finished())

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
            orchid = self._fetch_orchid(node, "compaction", ["//tmp/t_running"])
            pending = orchid["pending_tasks"]
            running = orchid["running_tasks"]
            finished = orchid["finished_tasks"]

            if len(pending) != 0:
                return False

            if len(running) != 1:
                return False
            assert "start_time" in running[0]
            assert "duration" in running[0]
            assert "finish_time" not in running[0]

            # There may be failed tasks due to conflits with partition sampling.
            if len(finished) < 1:
                return False

            for task in finished:
                assert "start_time" in task
                assert "duration" in task
                assert "finish_time" in task

            return True

        wait(_check)
