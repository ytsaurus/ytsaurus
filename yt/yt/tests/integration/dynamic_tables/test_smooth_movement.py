from yt_dynamic_tables_base import DynamicTablesBase

from yt.environment.helpers import assert_items_equal

from yt_commands import (
    authors, create, wait, get, set,
    sync_create_cells, sync_mount_table, raises_yt_error,
    sync_reshard_table, insert_rows, ls, abort_transaction,
    build_snapshot, select_rows,
)

from yt.common import YtError

import random


##################################################################


class TestSmoothMovement(DynamicTablesBase):
    DELTA_NODE_CONFIG = {
        "tablet_node": {
            "tablet_manager": {
                "sleep_before_post_to_master": 500,
            }
        }
    }

    def _move_tablet(self, tablet_id, cell_id=None):
        if cell_id is None:
            current_cell_id = get(f"#{tablet_id}/@cell_id")
            cell_id = random.choice([c for c in ls("//sys/tablet_cells") if c != current_cell_id])
        return create("tablet_action", "", attributes={
            "kind": "smooth_move",
            "tablet_ids": [tablet_id],
            "cell_ids": [cell_id],
            "expiration_timeout": 60000,
        })

    def _check_action(self, action_id):
        state = get(f"#{action_id}/@state")
        if state == "completed":
            return True
        elif state == "failed":
            raise YtError(get(f"{action_id}/@error"))
        return False

    def _sync_move_tablet(self, tablet_id, cell_id=None):
        action_id = self._move_tablet(tablet_id, cell_id)

        wait(lambda: self._check_action(action_id))

    def _restart_cell(self, cell_id):
        tx_id = get(f"#{cell_id}/@prerequisite_transaction_id")
        abort_transaction(tx_id)

    @authors("ifsmirnov")
    def test_empty_store_rotation_recovery(self):
        sync_create_cells(2)
        self._create_sorted_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [5]])
        sync_mount_table("//tmp/t", first_tablet_index=0, last_tablet_index=0)

        # Forbid writes to the second tablet.
        set("//tmp/t/@mount_config", {
            "max_dynamic_store_pool_size": 1,
            "enable_store_rotation": False,
        })
        sync_mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=1)

        with raises_yt_error("Dynamic store pool size limit reached"):
            insert_rows("//tmp/t", [{"key": 1}, {"key": 10}])

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        old_cell_id = get(f"#{tablet_id}/@cell_id")
        build_snapshot(cell_id=old_cell_id)
        self._sync_move_tablet(tablet_id)
        new_cell_id = get(f"#{tablet_id}/@cell_id")

        self._restart_cell(old_cell_id)
        self._restart_cell(new_cell_id)
        wait(lambda: get(f"#{old_cell_id}/@health") == "good")
        wait(lambda: get(f"#{new_cell_id}/@health") == "good")

    @authors("ifsmirnov")
    def test_basic_write_redirect(self):
        sync_create_cells(2)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        counter = 0

        def _make_row():
            nonlocal counter
            row = {"key": counter, "value": str(counter)}
            counter += 1
            return row

        expected_rows = []
        for i in range(10):
            row = _make_row()
            expected_rows.append(row)
            insert_rows("//tmp/t", [row])

        assert_items_equal(expected_rows, select_rows("* from [//tmp/t]"))

        action_id = self._move_tablet(tablet_id)

        while not self._check_action(action_id):
            row = _make_row()
            try:
                insert_rows("//tmp/t", [row])
                expected_rows.append(row)
            except YtError:
                pass

            try:
                actual = select_rows("* from [//tmp/t]")
                assert_items_equal(expected_rows, actual)
            except YtError:
                pass

        assert_items_equal(expected_rows, select_rows("* from [//tmp/t]"))
