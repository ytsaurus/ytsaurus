from yt_dynamic_tables_base import DynamicTablesBase, SmoothMovementHelper

from yt.environment.helpers import assert_items_equal, are_items_equal

from yt_commands import (
    authors, create, wait, get, set, exists,
    sync_create_cells, sync_mount_table, sync_unmount_table, raises_yt_error,
    sync_reshard_table, insert_rows, ls,
    build_snapshot, select_rows, update_nodes_dynamic_config,
    create_area, start_transaction, commit_transaction, sync_flush_table, remount_table,
    get_singular_chunk_id, disable_tablet_cells_on_node, enable_tablet_cells_on_node,
    create_table_replica, alter_table_replica, unmount_table,
    set_node_banned, trim_rows, generate_timestamp
)

from yt.common import YtError
import yt.yson as yson

import random
import time

import pytest


##################################################################


class SmoothMovementBase(DynamicTablesBase):
    _testing_delay = DynamicTablesBase._testing_delay

    DELTA_NODE_CONFIG = {
        "tablet_node": {
            "tablet_manager": {
                "sleep_before_post_to_master": 500,
            },
            "resource_limits": {
                "slots": 1,
            },
        },
        "rpc_server": {
            "services": {
                "TabletService": {
                    "methods": {
                        "Write": _testing_delay(100),
                        "RegisterTransactionActions": _testing_delay(100),
                    },
                },
                "TransactionSupervisorService": {
                    "methods": {
                        "CommitTransaction": _testing_delay(50),
                    },
                },
                "TransactionParticipantService": {
                    "methods": {
                        "CommitTransaction": _testing_delay(50),
                        "PrepareTransaction": _testing_delay(50),
                    },
                },
            },
        },
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

    def _restart_cell(self, cell_id, sync=True, with_snapshot=False):
        def _get_peer_address():
            for peer in get(f"#{cell_id}/@peers"):
                if address := peer.get("address"):
                    return address

        node_address = _get_peer_address()
        if with_snapshot:
            build_snapshot(cell_id)
        disable_tablet_cells_on_node(node_address)
        wait(lambda: _get_peer_address() != node_address)
        enable_tablet_cells_on_node(node_address)
        if sync:
            wait(lambda: get(f"#{cell_id}/@health") == "good")

##################################################################


class TestSmoothMovement(SmoothMovementBase):
    @authors("ifsmirnov")
    @pytest.mark.parametrize("sorted", [True, False])
    def test_empty_store_rotation_recovery(self, sorted):
        sync_create_cells(2)
        self._create_table("//tmp/t", sorted)
        if sorted:
            sync_reshard_table("//tmp/t", [[], [5]])
        else:
            sync_reshard_table("//tmp/t", 2)
        sync_mount_table("//tmp/t", first_tablet_index=0, last_tablet_index=0)

        # Forbid writes to the second tablet.
        set("//tmp/t/@mount_config", {
            "max_dynamic_store_pool_size": 1,
            "enable_store_rotation": False,
        })
        sync_mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=1)

        with raises_yt_error("Dynamic store pool size limit reached"):
            if sorted:
                insert_rows("//tmp/t", [{"key": 1}, {"key": 10}])
            else:
                insert_rows("//tmp/t", [{"$tablet_index": 1, "key": 10}])
                insert_rows("//tmp/t", [
                    {"$tablet_index": 0, "key": 1},
                    {"$tablet_index": 1, "key": 11},
                ])
            select_rows("* from [//tmp/t]")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        old_cell_id = get(f"#{tablet_id}/@cell_id")
        build_snapshot(cell_id=old_cell_id)
        self._sync_move_tablet(tablet_id)
        new_cell_id = get(f"#{tablet_id}/@cell_id")

        self._restart_cell(old_cell_id)
        self._restart_cell(new_cell_id)

    @authors("ifsmirnov")
    @pytest.mark.parametrize("sorted", [True, False])
    def test_basic_write_redirect(self, sorted):
        sync_create_cells(2)
        self._create_table("//tmp/t", sorted)
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

        assert_items_equal(expected_rows, select_rows("key, value from [//tmp/t]"))

        action_id = self._move_tablet(tablet_id)

        while not self._check_action(action_id):
            row = _make_row()
            try:
                insert_rows("//tmp/t", [row])
                expected_rows.append(row)
            except YtError:
                pass

            try:
                actual = select_rows("key, value from [//tmp/t]")
                assert_items_equal(expected_rows, actual)
            except YtError:
                pass

        assert_items_equal(expected_rows, select_rows("key, value from [//tmp/t]"))

    @authors("ponasenko-rs")
    def test_basic_conflict_horizon_timestamp_propagation(self):
        sync_create_cells(2)
        self._create_sorted_table(
            "//tmp/t",
            mount_config={
                "backing_store_retention_time": 5000,
            },
        )
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        ts = generate_timestamp()

        expected_rows = []
        for i in range(10):
            row = {"key": i, "value": str(i)}
            expected_rows.append(row)
            insert_rows("//tmp/t", [row])

        assert_items_equal(expected_rows, select_rows("* from [//tmp/t]"))

        self._sync_move_tablet(tablet_id)

        # Wait until backing store is released.
        time.sleep(5)

        sync_unmount_table("//tmp/t")

        assert get(f"#{tablet_id}/@conflict_horizon_timestamp") > ts

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
    def DISABLED_test_hunks(self, redirect_sweep):
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
    def test_row_cache_rotation_at_target_servant(self):
        sync_create_cells(2)
        self._create_sorted_table(
            "//tmp/t",
            mount_config={
                "lookup_cache_rows_ratio": 1.0,
                "enable_lookup_cache_by_default": True,
                "max_dynamic_store_row_count": 1,
                "enable_compaction_and_partitioning": False,
            })
        sync_mount_table("//tmp/t")

        _next_key = 0

        def _insert():
            nonlocal _next_key

            while True:
                try:
                    insert_rows("//tmp/t", [{"key": _next_key, "value": str(_next_key)}])
                    break
                except YtError:
                    time.sleep(0.1)

            _next_key += 1

        for i in range(5):
            _insert()

        with SmoothMovementHelper("//tmp/t").forwarding_context():
            for i in range(10):
                _insert()
            wait(lambda: get("//tmp/t/@chunk_count") > 10)

        assert_items_equal(
            [{"key": i, "value": str(i)} for i in range(_next_key)],
            select_rows("* from [//tmp/t]"))

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
    def DISABLED_test_transaction_serialized_in_other_tablet(self):
        cell_ids = sync_create_cells(2)
        self._create_sorted_table("//tmp/t")
        self._create_ordered_table("//tmp/q", commit_ordering="strong")
        sync_mount_table("//tmp/t", cell_id=cell_ids[0])
        sync_mount_table("//tmp/q", cell_id=cell_ids[0])

        with SmoothMovementHelper("//tmp/t").forwarding_context():
            # Transaction is serialized by //tmp/q but not by //tmp/t. Serialization
            # should be ignored by target servant of //tmp/t.
            tx_id = start_transaction(type="tablet")
            insert_rows("//tmp/t", [{"key": 1}], transaction_id=tx_id)
            insert_rows("//tmp/q", [{"key": 1}], transaction_id=tx_id)
            commit_transaction(tx_id)

    @authors("ifsmirnov")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_replica_and_replicated_table_at_same_cell(self, mode):
        cell_ids = sync_create_cells(2)
        create(
            "replicated_table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
            })

        r = create_table_replica("//tmp/t", "primary", "//tmp/r")
        alter_table_replica(r, mode=mode, enabled=True)

        self._create_sorted_table("//tmp/r", upstream_replica_id=r)

        sync_mount_table("//tmp/t", cell_id=cell_ids[0])
        sync_mount_table("//tmp/r", cell_id=cell_ids[0])

        # Run twice: move replica tablet away from replicated table tablet
        # and then move it back.
        all_rows = []
        for iter in range(2):
            with SmoothMovementHelper("//tmp/r").forwarding_context():
                batch_size = 15
                rows = [{"key": i, "value": str(i)} for i in range(iter * batch_size, (iter + 1) * batch_size)]
                all_rows += rows
                for row in rows:
                    while True:
                        try:
                            insert_rows("//tmp/t", [row], require_sync_replica=False)
                            break
                        except YtError:
                            time.sleep(0.1)
                    time.sleep(0.5)

            if mode == "sync":
                assert_items_equal(select_rows("* from [//tmp/r]"), all_rows)
            else:
                wait(lambda: are_items_equal(select_rows("* from [//tmp/r]"), all_rows))

    @authors("ifsmirnov")
    @pytest.mark.parametrize("sorted", [True, False])
    def test_fake_dynamic_store_in_snapshot(self, sorted):
        cell_ids = sync_create_cells(2)
        self._create_table(
            "//tmp/t",
            sorted,
            mount_config={"testing": {
                "flush_failure_probability": 1,
                "opaque_stores_in_orchid": False,
            }})
        sync_mount_table("//tmp/t", cell_id=cell_ids[0])

        rows = [
            {"key": 1, "value": "foo"},
            {"key": 2, "value": "bar"},
            {"key": 3, "value": "baz"},
        ]
        insert_rows("//tmp/t", rows[:1])

        h = SmoothMovementHelper("//tmp/t")
        with h.forwarding_context():
            build_snapshot(cell_ids[1])
            insert_rows("//tmp/t", rows[1:2])
            self._restart_cell(cell_ids[1])

            for i in range(10):
                try:
                    insert_rows("//tmp/t", rows[2:3])
                    break
                except YtError:
                    time.sleep(0.5)
            set("//tmp/t/@mount_config/testing/flush_failure_probability", 0)
            remount_table("//tmp/t")

        if sorted:
            expected_rows = rows
        else:
            expected_rows = [
                r | {"$tablet_index": 0, "$row_index": i}
                for i, r in enumerate(rows)
            ]

        assert_items_equal(select_rows("* from [//tmp/t]"), expected_rows)

    @authors("ifsmirnov")
    @pytest.mark.parametrize("sorted", [True, False])
    def test_frozen_tablet(self, sorted):
        sync_create_cells(2)
        self._create_table("//tmp/t", sorted=sorted)
        sync_mount_table("//tmp/t", freeze=True)
        with raises_yt_error("Only mounted tablet can be moved"):
            SmoothMovementHelper("//tmp/t").start()

    @authors("ifsmirnov")
    def test_trim(self):
        sync_create_cells(2)
        self._create_ordered_table(
            "//tmp/t",
            mount_config={
                "testing": {
                    "opaque_stores_in_orchid": False,
                },
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
            })
        sync_mount_table("//tmp/t")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        rows = []
        trimmed_row_count = 0

        def _insert(count):
            nonlocal rows
            offset = len(rows) + trimmed_row_count
            new_rows = [
                {"key": i, "value": str(i)}
                for i in range(offset, offset + count)
            ]
            insert_rows("//tmp/t", new_rows)
            rows += new_rows

        def _trim(count):
            nonlocal trimmed_row_count

            trim_rows("//tmp/t", 0, count)

            if count > trimmed_row_count:
                del rows[:count - trimmed_row_count]
                trimmed_row_count = count

        def _do_check(helper=None):
            assert_items_equal(select_rows("key, value from [//tmp/t]"), rows)
            total_row_count = trimmed_row_count + len(rows)
            assert get("//tmp/t/@tablets/0/trimmed_row_count") == trimmed_row_count
            assert get(f"#{tablet_id}/orchid/trimmed_row_count") == trimmed_row_count
            assert get(f"#{tablet_id}/orchid/total_row_count") == total_row_count
            if helper:
                assert get(f"{helper.target_orchid}/trimmed_row_count") == trimmed_row_count
                assert get(f"{helper.target_orchid}/total_row_count") == total_row_count

        def _check(helper=None):
            try:
                _do_check(helper)
                return True
            except AssertionError:
                return False

        _insert(10)
        _trim(1)

        h = SmoothMovementHelper(tablet_id)
        with h.forwarding_context():
            _check(h)
            _trim(5)
            _check(h)
            _insert(10)

            self._restart_cell(h.target_cell_id, with_snapshot=True)

            wait(lambda: len(get("//tmp/t/@chunk_ids")) == 1)
            _trim(10)
            wait(lambda: len(get("//tmp/t/@chunk_ids")) == 0)

            wait(lambda: _check(h))

        _check()

        _trim(20)
        sync_flush_table("//tmp/t")
        wait(lambda: len(get(f"#{tablet_id}/orchid/stores")) == 1)

        _check()

        h = SmoothMovementHelper(tablet_id)
        with h.forwarding_context():
            _check(h)
            _trim(15)
            with raises_yt_error("Cannot trim tablet"):
                _trim(1000)

        _check()

    @authors("ifsmirnov")
    def test_atomicity_none(self):
        sync_create_cells(2)
        self._create_sorted_table("//tmp/t", atomicity="none")
        sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "foo"}]
        with SmoothMovementHelper("//tmp/t").forwarding_context():
            insert_rows("//tmp/t", rows, atomicity="none")
            assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        assert_items_equal(select_rows("* from [//tmp/t]"), rows)


##################################################################


class TestSmoothMovementLargeCommitDelay(SmoothMovementBase):
    DELTA_NODE_CONFIG = {
        "tablet_node": {
            "transaction_supervisor": {
                "rpc_timeout": 20000,
            },
            "resource_limits": {
                "slots": 1,
            },
        }
    }

    DELTA_MASTER_CONFIG = {
        "transaction_supervisor": {
            "rpc_timeout": 20000,
        },
    }

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        node_to_method = {
            0: "PrepareTransaction",
            1: "CommitTransaction",
        }
        method = node_to_method.get(config["cypress_annotations"]["yt_env_index"])
        if method:
            config["rpc_server"]["services"]["TransactionParticipantService"] \
                ["methods"][method]["testing"]["random_delay"] = 6000  # noqa

    @staticmethod
    def _get_special_node_index(delay_stage):
        if delay_stage == "prepare":
            return 0
        elif delay_stage == "commit":
            return 1
        assert False

    def _prepare(self, delay_stage):
        # Table has tablets on cells 0 and 1. Tablet is moved from cell 0 to 2.
        # 2pc transaction is committed between cells 0 and 1, coordinated by 1.
        # We wait until some transactions are stuck at node 0 in stage "active" or
        # "persistent_commit_prepared" depending on |delay_stage|
        # (and so forwarded to 2 and stuck there in the same state).

        node_to_index = {}
        # Leave only special node and two others, renumber them as 0, 1, 2.
        for n in ls("//sys/tablet_nodes", attributes=["annotations"]):
            index = n.attributes["annotations"]["yt_env_index"]
            special_node_index = self._get_special_node_index(delay_stage)
            if index not in (special_node_index, 2, 3):
                disable_tablet_cells_on_node(n)
            # 0/1 -> 0
            # 2 -> 1
            # 3 -> 2
            node_to_index[str(n)] = max(0, index - 1)

        cell_ids = [""] * 3
        for cell_id in sync_create_cells(3):
            address = get(f"#{cell_id}/@peers/0/address")
            index = node_to_index[address]
            assert index < 3
            cell_ids[index] = cell_id

        return cell_ids

    def _has_transactions_with_state(self, cell_id, state):
        transactions = get(f"#{cell_id}/orchid/transactions")
        return any(tx["state"] == state for tx in transactions.values())

    @authors("ifsmirnov")
    @pytest.mark.parametrize("delay_stage", ["prepare", "commit"])
    def test_unmount_with_stuck_transactions(self, delay_stage):
        cell_ids = self._prepare(delay_stage)

        self._create_sorted_table("//tmp/t", pivot_keys=[[], [100]])
        sync_mount_table("//tmp/t", target_cell_ids=[cell_ids[0], cell_ids[1]])

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        h = SmoothMovementHelper(tablet_id, cell_ids[2])
        h.start_forwarding_mutations()

        # Coordinator is selected at random. Ensure that at least some transactions
        # are coordinated by cell 1.
        for i in range(20):
            insert_rows("//tmp/t", [{"key": i}, {"key": i + 100}], ignore_result=True)

        expected_state = "persistent_commit_prepared" if delay_stage == "commit" else "active"
        wait(lambda: self._has_transactions_with_state(cell_ids[2], expected_state))

        assert exists(f"#{cell_ids[0]}/orchid/tablets/{tablet_id}")
        assert exists(f"#{cell_ids[2]}/orchid/tablets/{tablet_id}")
        unmount_table("//tmp/t", force=True)
        assert h.get_action_state() == "failed"
        wait(lambda: not exists(f"#{cell_ids[0]}/orchid/tablets/{tablet_id}"))
        wait(lambda: not exists(f"#{cell_ids[2]}/orchid/tablets/{tablet_id}"))

    @authors("ifsmirnov")
    @pytest.mark.parametrize("delay_stage", ["prepare", "commit"])
    @pytest.mark.parametrize("with_snapshot", [True, False])
    @pytest.mark.parametrize("servant_to_restart", ["source", "target"])
    def test_recovery_with_stuck_transactions(self, delay_stage, with_snapshot, servant_to_restart):
        cell_ids = self._prepare(delay_stage)

        self._create_sorted_table("//tmp/t", pivot_keys=[[], [100]])
        sync_mount_table("//tmp/t", target_cell_ids=[cell_ids[0], cell_ids[1]])
        self._create_sorted_table("//tmp/correct")
        sync_mount_table("//tmp/correct", cell_id=cell_ids[1])

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        h = SmoothMovementHelper(tablet_id, cell_ids[2])
        h.start_forwarding_mutations()

        # Coordinator is selected at random. Ensure that at least some transactions
        # are coordinated by cell 1.
        for i in range(20):
            tx_id = start_transaction(type="tablet")
            insert_rows("//tmp/t", [{"key": i}, {"key": i + 100}], tx=tx_id)
            insert_rows("//tmp/correct", [{"key": i}, {"key": i + 100}], tx=tx_id)
            commit_transaction(tx_id, ignore_result=True)

        expected_state = "persistent_commit_prepared" if delay_stage == "commit" else "active"
        wait(lambda: self._has_transactions_with_state(cell_ids[2], expected_state))

        target_cell = cell_ids[0] if servant_to_restart == "source" else cell_ids[2]
        if with_snapshot:
            build_snapshot(target_cell)

        node_address = get(f"#{target_cell}/@peers/0/address")
        set_node_banned(node_address, True)
        wait(lambda: get(f"#{target_cell}/@health") == "failed")
        set_node_banned(node_address, False)
        wait(lambda: get(f"#{target_cell}/@health") == "good")

        h.finish()

        actual = select_rows("* from [//tmp/t]")
        expected = select_rows("* from [//tmp/correct]")
        assert_items_equal(actual, expected)

##################################################################


class TestSmoothMovementMulticell(TestSmoothMovement):
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }
