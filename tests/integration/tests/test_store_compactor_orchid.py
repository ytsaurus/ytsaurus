from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE, SCHEDULERS_SERVICE
from yt_commands import *

from test_sorted_dynamic_tables import TestSortedDynamicTablesBase

import sys
import os.path

#################################################################

class TestStoreCompactorOrchid(TestSortedDynamicTablesBase):
    NUM_NODES = 3
    USE_DYNAMIC_TABLES = True

    @authors("akozhikhov")
    def test_compaction_orchid(self):
        NUM_TABLES = 3L
        nodes = ls("//sys/cluster_nodes")
        for node in nodes[1:]:
            set("//sys/cluster_nodes/{0}/@disable_tablet_cells".format(node), True)
        node = nodes[0]

        sync_create_cells(1)

        for table_idx in range(NUM_TABLES):
            create_dynamic_table("//tmp/t{0}".format(table_idx), schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"}
            ])

        empty_task_dict = {"task_count": 0, "finished_task_count": 0, "pending_tasks": [], "finished_tasks": []}
        assert get("//sys/cluster_nodes/{0}/orchid/store_compactor/compaction_tasks".format(node)) == empty_task_dict
        assert get("//sys/cluster_nodes/{0}/orchid/store_compactor/partitioning_tasks".format(node)) == empty_task_dict

        for table_idx in range(NUM_TABLES):
            set("//tmp/t{0}/@enable_compaction_and_partitioning".format(table_idx), False)
            sync_mount_table("//tmp/t{0}".format(table_idx))

        for table_idx in range(NUM_TABLES):
            for i in range(5):
                insert_rows("//tmp/t{0}".format(table_idx), [{"key": j, "value": str(j)} for j in range(i * 10, (i + 1) * 10)])
                sync_flush_table("//tmp/t{0}".format(table_idx))
            set("//tmp/t{0}/@enable_compaction_and_partitioning".format(table_idx), True)
            remount_table("//tmp/t{0}".format(table_idx))

        tablets = [get("//tmp/t{0}/@tablets/0".format(tablet_idx)) for tablet_idx in range(NUM_TABLES)]
        tablet_ids = [tablets[i]["tablet_id"] for i in range(NUM_TABLES)]
        tablet_ids.sort()

        def _compaction_task_finished():
            compaction_tasks = get("//sys/cluster_nodes/{0}/orchid/store_compactor/compaction_tasks".format(node))
            for task in compaction_tasks["finished_tasks"]:
                # We don't want to predict priorities in integration test
                del task["task_priority"]
                del task["partition_id"]
            compaction_tasks["finished_tasks"].sort(key=lambda x: x["tablet_id"])

            expected_compaction_tasks = {
                "task_count": 0,
                "finished_task_count": NUM_TABLES,
                "pending_tasks": [],
                "finished_tasks": [
                    {"tablet_id": tablet_ids[i], "store_count": 5} for i in range(NUM_TABLES)]
            }

            return compaction_tasks == expected_compaction_tasks

        wait(lambda: _compaction_task_finished())
        assert get("//sys/cluster_nodes/{0}/orchid/store_compactor/partitioning_tasks".format(node)) == empty_task_dict

    @authors("akozhikhov")
    def test_partitioning_orchid(self):
        nodes = ls("//sys/cluster_nodes")
        for node in nodes[1:]:
            set("//sys/cluster_nodes/{0}/@disable_tablet_cells".format(node), True)
        node = nodes[0]

        sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        self._create_partitions(partition_count=2)

        # Now add store to eden
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i} for i in xrange(2, 6)])
        sync_flush_table("//tmp/t")
        assert len(get("//tmp/t/@chunk_ids")) == 3
        assert get("#{}/@eden".format(get("//tmp/t/@chunk_ids/{0}".format(2))))

        set("//tmp/t/@enable_compaction_and_partitioning", True)
        set("//tmp/t/@forced_compaction_revision", 1)
        remount_table("//tmp/t")

        def _partition_task_finished():
            expected_partition_task = {
                "task_count": 0,
                "finished_task_count": 1,
                "pending_tasks": [],
                "finished_tasks": [
                    {"tablet_id": get("//tmp/t/@tablets/0/tablet_id"), "store_count": 1}
                ]}
            partition_task = get("//sys/cluster_nodes/{0}/orchid/store_compactor/partitioning_tasks".format(node))
            if partition_task["finished_task_count"] == 0:
                return False
            del partition_task["finished_tasks"][0]["task_priority"]
            del partition_task["finished_tasks"][0]["partition_id"]
            return partition_task == expected_partition_task
        wait(lambda: _partition_task_finished())
