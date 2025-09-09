from yt_dynamic_tables_base import DynamicTablesBase

from yt.environment.helpers import assert_items_equal, are_items_equal

from yt_commands import (
    authors, create, wait, get, set,
    sync_create_cells, sync_mount_table, raises_yt_error,
    sync_reshard_table, insert_rows, ls, abort_transaction,
    build_snapshot, select_rows, update_nodes_dynamic_config,
    create_area, start_transaction, commit_transaction, sync_flush_table, remount_table,
    get_singular_chunk_id,
)

from yt.common import YtError
import yt.yson as yson

import random
import time

import pytest


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

    def _get_movement_stage_from_node(self, tablet_id):
        # Smooth movement orchid does not exist when there is no movement
        # in progress.
        try:
            return get(f"#{tablet_id}/orchid/smooth_movement/stage")
        except YtError:
            return None

    def _restart_cell(self, cell_id, sync=True):
        tx_id = get(f"#{cell_id}/@prerequisite_transaction_id")
        abort_transaction(tx_id)
        if sync:
            wait(lambda: get(f"#{cell_id}/@health") == "good")

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

    def _update_testing_config(self, config):
        update_nodes_dynamic_config({
            "tablet_node": {
                "smooth_movement_tracker": {
                    "testing": config,
                }
            }
        })

    @authors("ifsmirnov")
    @pytest.mark.parametrize("recovery", ["source", "target", None])
    def test_write_redirect_2pc(self, recovery):
        custom_area_id = create_area(
            "custom",
            cell_bundle_id=get("//sys/tablet_cell_bundles/default/@id"))

        update_nodes_dynamic_config({
            "tablet_node": {
                "slots": 1,
            }
        })

        cell_ids = sync_create_cells(3)

        # Extract target cell to a separate area so that it can be disabled.
        set(f"#{cell_ids[2]}/@area", "custom")
        wait(lambda: get(f"#{cell_ids[2]}/@health") == "good")

        self._create_sorted_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [10]])
        tablet_ids = [t["tablet_id"] for t in get("//tmp/t/@tablets")]
        sync_mount_table("//tmp/t", target_cell_ids=[cell_ids[0], cell_ids[1]])

        def _make_rows(*keys):
            return [{"key": x, "value": str(x)} for x in keys]

        # First tablet moves from 0 to 2, second tablet stays at cell 1.

        rows1 = _make_rows(0, 10)
        insert_rows("//tmp/t", rows1)

        self._update_testing_config({
            "delay_after_stage_at_source": {
                "servant_switch_requested": 5000,
            }
        })

        action_id = self._move_tablet(tablet_ids[0], cell_ids[2])
        wait(lambda: self._get_movement_stage_from_node(tablet_ids[0]) == "servant_switch_requested")

        if recovery == "source":
            # Disable target servant so it does not receive forwarded mutations.
            set(f"#{custom_area_id}/@node_tag_filter", "invalid")
            wait(lambda: get(f"#{cell_ids[2]}/@health") == "failed")

        rows2 = _make_rows(1, 11)
        insert_rows("//tmp/t", rows2)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)

        wait(lambda: self._get_movement_stage_from_node(tablet_ids[0]) == "servant_switched")

        if recovery == "source":
            # Ensure target cell has not resurrected.
            assert get(f"#{cell_ids[2]}/@health") == "failed"

            # Restart source cell.
            self._restart_cell(cell_ids[0])

            # Make target cell alive again.
            set(f"#{custom_area_id}/@node_tag_filter", "")
            wait(lambda: get(f"#{cell_ids[2]}/@health") == "good")

        wait(lambda: self._check_action(action_id))

        assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)

        if recovery == "target":
            self._restart_cell(cell_ids[2])
            assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)

    @authors("ifsmirnov")
    @pytest.mark.parametrize("redirect_sweep", [True, False])
    def test_hunks(self, redirect_sweep):
        sync_create_cells(2)
        self._create_sorted_table(
            "//tmp/t",
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string", "max_inline_hunk_size": 10},
            ])
        sync_mount_table("//tmp/t")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        rows = [
            {"key": 1, "value": "q"},
            {"key": 2, "value": "qwlfjpluqwjfpqwfpqwfpljqywujpqwfpqwfpqwfpqwfp"},
        ]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        # Create dangling hunk chunk.
        update_nodes_dynamic_config({"tablet_node": {"hunk_chunk_sweeper": {"enable": False}}})
        set("//tmp/t/@forced_compaction_revision", 1)
        remount_table("//tmp/t")

        def _get_hunk_ref_counts():
            hunk_chunk_ids = ls(f"#{tablet_id}/orchid/hunk_chunks")
            ref_counts = [
                get(f"#{tablet_id}/orchid/hunk_chunks/{c}/store_ref_count")
                for c in hunk_chunk_ids
            ]
            return sorted(ref_counts)

        # chunk, two hunks, two dynamic stores
        wait(lambda: get("//tmp/t/@chunk_count") == 5)
        assert _get_hunk_ref_counts() == [0, 1]

        if redirect_sweep:
            self._update_testing_config({
                "delay_after_stage_at_target": {
                    "target_activated": 5000,
                }
            })

        action_id = self._move_tablet(tablet_id)

        if redirect_sweep:
            wait(lambda: self._get_movement_stage_from_node(tablet_id) == "target_activated")
            update_nodes_dynamic_config({"tablet_node": {"hunk_chunk_sweeper": {"enable": True}}})

        wait(lambda: self._check_action(action_id))

        if redirect_sweep:
            assert _get_hunk_ref_counts() == [1]
        else:
            assert _get_hunk_ref_counts() == [0, 1]

        assert_items_equal(rows, select_rows("* from [//tmp/t]"))

    @authors("ifsmirnov")
    @pytest.mark.parametrize("two_phase", [True, False])
    def test_abort_transactions_before_target_activation(self, two_phase):
        sync_create_cells(2)
        self._create_sorted_table(
            "//tmp/t",
            mount_config={
                "testing": {
                    "write_response_delay": 10000,
                }
            },
            pivot_keys=[[], [10]] if two_phase else [[]],
        )

        self._update_testing_config({
            "delay_after_stage_at_source": {
                "waiting_for_locks_before_activation": 5000,
            }
        })

        sync_mount_table("//tmp/t")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        cell_id = get("//tmp/t/@tablets/0/cell_id")

        tx_id = start_transaction(type="tablet")
        insert_rows("//tmp/t", [{"key": 1}, {"key": 10}], transaction_id=tx_id)
        commit_rsp = commit_transaction(tx_id, return_response=True)

        wait(lambda: tx_id in get(f"#{cell_id}/orchid/transactions"))

        action_id = self._move_tablet(tablet_id)

        # Wait until tx is aborted by smooth movement tracker.
        wait(lambda: tx_id not in get(f"#{cell_id}/orchid/transactions"))
        wait(lambda: self._get_movement_stage_from_node(tablet_id) == "waiting_for_locks_before_activation")

        commit_rsp.wait()
        assert not commit_rsp.is_ok()

        wait(lambda: self._check_action(action_id))

    @authors("ifsmirnov")
    @pytest.mark.parametrize("two_phase", [True, False])
    def test_abort_transactions_before_servant_switch(self, two_phase):
        cell_ids = sync_create_cells(2)

        self._create_sorted_table(
            "//tmp/t",
            mount_config={
                "testing": {
                    "write_response_delay": 10000,
                }
            },
            pivot_keys=[[], [10]] if two_phase else [[]])

        self._update_testing_config({
            "delay_after_stage_at_source": {
                "servant_switch_requested": 5000,
                "waiting_for_locks_before_switch": 5000,
            }
        })

        if two_phase:
            sync_mount_table("//tmp/t", target_cell_ids=cell_ids)
        else:
            sync_mount_table("//tmp/t", cell_id=cell_ids[0])

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        action_id = self._move_tablet(tablet_id)
        wait(lambda: self._get_movement_stage_from_node(tablet_id) == "servant_switch_requested")

        tx_id = start_transaction(type="tablet")
        insert_rows("//tmp/t", [{"key": 1}, {"key": 10}], transaction_id=tx_id)
        commit_rsp = commit_transaction(tx_id, return_response=True)

        wait(lambda: tx_id in get(f"#{cell_ids[0]}/orchid/transactions"))
        token = get(f"#{cell_ids[0]}/orchid/transactions/{tx_id}/externalizer_tablets/0/externalization_token")
        externalized_tx_id = tx_id.replace("-a0002-", "-a000c-") + "@" + token
        wait(lambda: externalized_tx_id in get(f"#{cell_ids[1]}/orchid/transactions"))
        assert self._get_movement_stage_from_node(tablet_id) == "servant_switch_requested"

        wait(lambda: tx_id not in get(f"#{cell_ids[0]}/orchid/transactions"))
        wait(lambda: externalized_tx_id not in get(f"#{cell_ids[1]}/orchid/transactions"))
        assert self._get_movement_stage_from_node(tablet_id) == "waiting_for_locks_before_switch"

        commit_rsp.wait()
        assert not commit_rsp.is_ok()

        wait(lambda: self._check_action(action_id))

    @authors("ifsmirnov")
    def test_wait_preload(self):
        cell_ids = sync_create_cells(2)

        self._create_sorted_table(
            "//tmp/t",
            mount_config={
                "testing": {
                    "simulated_store_preload_delay": 5000,
                }
            },
            in_memory_mode="compressed")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        sync_mount_table("//tmp/t", cell_id=cell_ids[0])
        insert_rows("//tmp/t", [{"key": 0}])
        sync_flush_table("//tmp/t")
        chunk_id = get_singular_chunk_id("//tmp/t")

        def _get_preload_state():
            try:
                return get(
                    f"#{cell_ids[1]}/orchid/tablets/{tablet_id}/partitions/0/stores/{chunk_id}/preload_state")
            except YtError as e:
                if e.is_resolve_error():
                    return None
                raise

        action_id = self._move_tablet(tablet_id)
        wait(lambda: _get_preload_state() == "running")
        time.sleep(2)
        assert _get_preload_state() == "running"

        wait(lambda: self._check_action(action_id))
        assert _get_preload_state() == "complete"

    @authors("ifsmirnov")
    def test_preload_flush_and_compaction(self):
        cell_ids = sync_create_cells(2)

        self._create_sorted_table(
            "//tmp/t",
            mount_config={
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
                "testing": {
                    "simulated_store_preload_delay": 10000,
                }
            },
            in_memory_mode="compressed")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        sync_mount_table("//tmp/t", cell_id=cell_ids[0])
        insert_rows("//tmp/t", [{"key": 0}])
        sync_flush_table("//tmp/t")
        assert get("//tmp/t/@preload_state") == "complete"

        insert_rows("//tmp/t", [{"key": 1}])

        def _get_preload_states():
            prefix = f"#{cell_ids[1]}/orchid/tablets/{tablet_id}/partitions/0/stores"
            try:
                stores = ls(prefix)
                preload_states = []
                for store in stores:
                    preload_states.append(get(f"{prefix}/{store}/preload_state"))
                return preload_states
            except YtError as e:
                if e.is_resolve_error():
                    return []
                raise

        action_id = self._move_tablet(tablet_id)
        # Flushed store is already preloded at the target servant.
        # Replicated store preload is delayed for a long time.
        wait(lambda: are_items_equal(_get_preload_states(), ["running", "complete"]))
        time.sleep(2)
        assert_items_equal(_get_preload_states(), ["running", "complete"])

        set("//tmp/t/@forced_compaction_revision", 1)
        remount_table("//tmp/t")
        # Replicated store is gone, newly compacted store replaced it.
        wait(lambda: are_items_equal(_get_preload_states(), ["complete", "complete"]))

        wait(lambda: self._check_action(action_id))

    @authors("ifsmirnov")
    def test_transaction_serialized_in_other_tablet(self):
        cell_ids = sync_create_cells(2)
        self._create_sorted_table("//tmp/t")
        self._create_ordered_table("//tmp/q", commit_ordering="strong")
        sync_mount_table("//tmp/t", cell_id=cell_ids[0])
        sync_mount_table("//tmp/q", cell_id=cell_ids[0])

        self._update_testing_config({
            "delay_after_stage_at_target": {
                "target_activated": 5000,
            }
        })

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        action_id = self._move_tablet(tablet_id)
        wait(lambda: self._get_movement_stage_from_node(tablet_id) == "target_activated")

        # Transaction is serialized by //tmp/q but not by //tmp/t. Serialization
        # should not be forwarded.
        tx_id = start_transaction(type="tablet")
        insert_rows("//tmp/t", [{"key": 1}], transaction_id=tx_id)
        insert_rows("//tmp/q", [{"key": 1}], transaction_id=tx_id)
        commit_transaction(tx_id)

        wait(lambda: self._check_action(action_id))

##################################################################


class TestSmoothMovementMulticell(TestSmoothMovement):
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }
