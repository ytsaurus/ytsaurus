from yt_chaos_test_base import ChaosTestBase

from yt_dynamic_tables_base import SmoothMovementHelper

from yt_commands import (
    authors, wait, sync_create_cells, sync_mount_table, sync_unmount_table,
    insert_rows, select_rows, start_transaction, commit_transaction)

from yt.environment.helpers import are_items_equal

import pytest
import time


##################################################################


class TestChaosSmoothMovement(ChaosTestBase):
    DELTA_NODE_CONFIG = {
        "rpc_server": {
            "services": {
                "TabletService": {
                    "methods": {
                        "Write": {
                            "testing": {
                                "random_delay": 200,
                            },
                        },
                        "RegisterTransactionActions": {
                            "testing": {
                                "random_delay": 200,
                            },
                        },
                    },
                },
            },
        },
    }

    @authors("ifsmirnov")
    @pytest.mark.parametrize("queue_at_the_same_cell", [True, False])
    def test_chaos_smooth_movement_with_nonchaos_table(self, queue_at_the_same_cell):
        chaos_cell_id = self._sync_create_chaos_bundle_and_cell()
        tablet_cell_ids = sync_create_cells(4)

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(chaos_cell_id, replicas)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t", cell_id=tablet_cell_ids[0])

        sync_unmount_table("//tmp/r0")
        sync_mount_table("//tmp/r0", cell_id=tablet_cell_ids[0 if queue_at_the_same_cell else 3])

        self._create_sorted_table("//tmp/nonchaos")
        sync_mount_table("//tmp/nonchaos", cell_id=tablet_cell_ids[0])

        rows = [{"key": i, "value": str(i)} for i in range(30)]

        h1 = SmoothMovementHelper("//tmp/t", cell_id=tablet_cell_ids[1])
        h2 = SmoothMovementHelper("//tmp/nonchaos", cell_id=tablet_cell_ids[1])

        h1.start_forwarding_mutations()
        h2.start_forwarding_mutations()

        def _insert(left, right):
            for i in range(left, right):
                tx = start_transaction(type="tablet")
                insert_rows("//tmp/t", [rows[i]], tx=tx)
                insert_rows("//tmp/nonchaos", [rows[i]], tx=tx)
                commit_transaction(tx)
                time.sleep(0.2)
            wait(lambda: are_items_equal(select_rows("* from [//tmp/t]"), rows[:right]))

        _insert(0, 10)
        self._sync_alter_replica(card_id, replicas, replica_ids, 0, mode="sync")
        _insert(10, 20)
        self._sync_alter_replica(card_id, replicas, replica_ids, 0, mode="async")
        _insert(20, 30)

        h1.finish()
        h2.finish()

    @authors("ifsmirnov")
    @pytest.mark.parametrize("queue_at_the_same_cell", [True, False])
    def test_async_replication_during_target_activation_barrier(self, queue_at_the_same_cell):
        chaos_cell_id = self._sync_create_chaos_bundle_and_cell()
        tablet_cell_ids = sync_create_cells(3)

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(chaos_cell_id, replicas)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t", cell_id=tablet_cell_ids[0])

        sync_unmount_table("//tmp/r0")
        sync_mount_table("//tmp/r0", cell_id=tablet_cell_ids[0 if queue_at_the_same_cell else 2])

        rows = []

        def _insert_row():
            row = {"key": len(rows), "value": str(len(rows))}
            rows.append(row)
            insert_rows("//tmp/t", [row])

        for i in range(10):
            _insert_row()
            h = SmoothMovementHelper("//tmp/t", cell_id=tablet_cell_ids[(i + 1) % 2])
            h.start()
            while h.get_action_state() not in ("completed", "failed"):
                _insert_row()

            # Action has finished, but we want to raise the error if it has failed.
            h.wait_for_action()

        wait(lambda: are_items_equal(select_rows("* from [//tmp/t]"), rows))

    @authors("ifsmirnov")
    def test_move_queue(self):
        chaos_cell_id = self._sync_create_chaos_bundle_and_cell()
        sync_create_cells(3)

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t1"},
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t2"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q1"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/q2"},
        ]
        card_id, replica_ids = self._create_chaos_tables(chaos_cell_id, replicas)

        rows = [{"key": i, "value": str(i)} for i in range(30)]
        next_row_index = 0

        def _insert():
            nonlocal next_row_index

            left = next_row_index
            next_row_index += 1
            right = next_row_index

            for i in range(left, right):
                tx = start_transaction(type="tablet")
                insert_rows("//tmp/t1", [rows[i]], tx=tx)
                commit_transaction(tx)
                time.sleep(0.2)
            wait(lambda: are_items_equal(select_rows("* from [//tmp/t1]"), rows[:right]))
            wait(lambda: are_items_equal(select_rows("* from [//tmp/t2]"), rows[:right]))

        ht1 = SmoothMovementHelper("//tmp/t1")
        ht2 = SmoothMovementHelper("//tmp/t2")
        hq1 = SmoothMovementHelper("//tmp/q1")
        hq2 = SmoothMovementHelper("//tmp/q2")

        hq1.start_forwarding_mutations()
        _insert()
        self._sync_alter_replica(card_id, replicas, replica_ids, 3, mode="sync")  # q2
        self._sync_alter_replica(card_id, replicas, replica_ids, 2, mode="async")  # q1
        _insert()
        hq2.start_forwarding_mutations()
        _insert()
        self._sync_alter_replica(card_id, replicas, replica_ids, 2, mode="sync")  # q1
        self._sync_alter_replica(card_id, replicas, replica_ids, 3, mode="async")  # q2
        _insert()
        ht1.start_forwarding_mutations()
        _insert()
        self._sync_alter_replica(card_id, replicas, replica_ids, 1, mode="sync")  # t2
        self._sync_alter_replica(card_id, replicas, replica_ids, 0, mode="async")  # t1
        _insert()
        ht2.start_forwarding_mutations()
        _insert()
        self._sync_alter_replica(card_id, replicas, replica_ids, 3, mode="sync")  # q2
        self._sync_alter_replica(card_id, replicas, replica_ids, 2, mode="async")  # q1
        _insert()
        ht1.finish()
        hq1.finish()
        hq2.finish()
        ht2.finish()
        _insert()
