from yt_dynamic_tables_base import DynamicTablesBase

from yt_env_setup import Restarter, NODES_SERVICE

from yt_commands import (
    authors, wait, get, set, mount_table, unmount_table, freeze_table, unfreeze_table,
    wait_for_tablet_state, sync_create_cells, create,
    sync_mount_table, sync_unmount_table, sync_unfreeze_table,
    remount_table, sync_flush_table, select_rows, insert_rows, alter_table,
    sync_enable_table_replica, create_table_replica,
    cancel_tablet_transition, raises_yt_error, create_user, remove,
    multicell_sleep, create_area, ls, write_table, map)

from yt.environment.helpers import assert_items_equal, are_items_equal

from yt.common import YtError
import yt.yson as yson

import random
from time import sleep

import pytest

##################################################################


class TestDynamicTableStateTransitions(DynamicTablesBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_TEST_PARTITIONS = 5
    NUM_SCHEDULERS = 1

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "tablet_manager": {
            "leader_reassignment_timeout": 2000,
            "peer_revocation_timeout": 600000,
        }
    }

    DELTA_NODE_CONFIG = {
        "tablet_node": {
            "tablet_manager": {
                "sleep_before_post_to_master": 400,
            }
        }
    }

    def _get_expected_state(self, initial, first_command, second_command):
        M = "mounted"
        F = "frozen"
        E = "error"
        U = "unmounted"

        expected = {
            "mounted": {
                "mount": {
                    "mount": M,
                    "frozen_mount": E,
                    "unmount": U,
                    "freeze": F,
                    "unfreeze": M,
                },
                # frozen_mount
                "unmount": {
                    "mount": E,
                    "frozen_mount": E,
                    "unmount": U,
                    "freeze": E,
                    "unfreeze": E,
                },
                "freeze": {
                    "mount": E,
                    "frozen_mount": F,
                    "unmount": U,
                    "freeze": F,
                    "unfreeze": E,
                },
                "unfreeze": {
                    "mount": M,
                    "frozen_mount": E,
                    "unmount": U,
                    "freeze": F,
                    "unfreeze": M,
                },
            },
            "frozen": {
                # mount
                "frozen_mount": {
                    "mount": E,
                    "frozen_mount": F,
                    "unmount": U,
                    "freeze": F,
                    "unfreeze": M,
                },
                "unmount": {
                    "mount": E,
                    "frozen_mount": E,
                    "unmount": U,
                    "freeze": E,
                    "unfreeze": E,
                },
                "freeze": {
                    "mount": E,
                    "frozen_mount": F,
                    "unmount": U,
                    "freeze": F,
                    "unfreeze": M,
                },
                "unfreeze": {
                    "mount": M,
                    "frozen_mount": E,
                    "unmount": E,
                    "freeze": E,
                    "unfreeze": M,
                },
            },
            "unmounted": {
                "mount": {
                    "mount": M,
                    "frozen_mount": E,
                    "unmount": E,
                    "freeze": E,
                    "unfreeze": E,
                },
                "frozen_mount": {
                    "mount": E,
                    "frozen_mount": F,
                    "unmount": E,
                    "freeze": F,
                    "unfreeze": E,
                },
                "unmount": {
                    "mount": M,
                    "frozen_mount": F,
                    "unmount": U,
                    "freeze": E,
                    "unfreeze": E,
                },
                # freeze
                # unfreeze
            },
        }
        return expected[initial][first_command][second_command]

    def _create_cell(self):
        self._cell_id = sync_create_cells(1)[0]

    def _get_callback(self, command):
        callbacks = {
            "mount": lambda x: mount_table(x, cell_id=self._cell_id),
            "frozen_mount": lambda x: mount_table(x, cell_id=self._cell_id, freeze=True),
            "unmount": lambda x: unmount_table(x),
            "freeze": lambda x: freeze_table(x),
            "unfreeze": lambda x: unfreeze_table(x),
        }
        return callbacks[command]

    @pytest.mark.parametrize(
        ["initial", "command"],
        [
            ["mounted", "frozen_mount"],
            ["frozen", "mount"],
            ["unmounted", "freeze"],
            ["unmounted", "unfreeze"],
        ],
    )
    @authors("savrus")
    def test_initial_incompatible(self, initial, command):
        self._create_cell()
        self._create_sorted_table("//tmp/t")

        if initial == "mounted":
            sync_mount_table("//tmp/t")
        elif initial == "frozen":
            sync_mount_table("//tmp/t", freeze=True)

        with pytest.raises(YtError):
            self._get_callback(command)("//tmp/t")

    def _do_test_transition(self, initial, first_command, second_command):
        expected = self._get_expected_state(initial, first_command, second_command)
        if expected == "error":
            with Restarter(self.Env, NODES_SERVICE):
                self._get_callback(first_command)("//tmp/t")
                with pytest.raises(YtError):
                    self._get_callback(second_command)("//tmp/t")
        else:
            self._get_callback(first_command)("//tmp/t")
            wait(lambda: get("//tmp/t/@tablet_state") in ["mounted", "unmounted", "frozen"])
            self._get_callback(second_command)("//tmp/t")
            wait(lambda: get("//tmp/t/@tablet_state") == expected)
        wait(lambda: get("//tmp/t/@tablet_state") != "transient")

    @authors("savrus", "levysotsky")
    @pytest.mark.parametrize("second_command", ["mount", "frozen_mount", "unmount", "freeze", "unfreeze"])
    @pytest.mark.parametrize("first_command", ["mount", "unmount", "freeze", "unfreeze"])
    def test_state_transition_conflict_mounted(self, first_command, second_command):
        self._create_cell()
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t", cell_id=self._cell_id)
        self._do_test_transition("mounted", first_command, second_command)

    @authors("savrus", "levysotsky")
    @pytest.mark.parametrize("second_command", ["mount", "frozen_mount", "unmount", "freeze", "unfreeze"])
    @pytest.mark.parametrize("first_command", ["frozen_mount", "unmount", "freeze", "unfreeze"])
    def test_state_transition_conflict_frozen(self, first_command, second_command):
        self._create_cell()
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t", cell_id=self._cell_id, freeze=True)
        self._do_test_transition("frozen", first_command, second_command)

    @authors("savrus")
    @pytest.mark.parametrize("second_command", ["mount", "frozen_mount", "unmount", "freeze", "unfreeze"])
    @pytest.mark.parametrize("first_command", ["mount", "frozen_mount", "unmount"])
    def test_state_transition_conflict_unmounted(self, first_command, second_command):
        self._create_cell()
        self._create_sorted_table("//tmp/t")
        self._do_test_transition("unmounted", first_command, second_command)

    @authors("savrus")
    @pytest.mark.parametrize("inverse", [False, True])
    def test_freeze_expectations(self, inverse):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", pivot_keys=[[], [1]])
        sync_mount_table("//tmp/t", first_tablet_index=0, last_tablet_index=0)

        callbacks = [
            lambda: freeze_table("//tmp/t", first_tablet_index=0, last_tablet_index=0),
            lambda: mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=1, freeze=True),
        ]

        for callback in reversed(callbacks) if inverse else callbacks:
            callback()

        wait_for_tablet_state("//tmp/t", "frozen")
        wait(lambda: get("//tmp/t/@tablet_state") != "transient")
        assert get("//tmp/t/@expected_tablet_state") == "frozen"

    @authors("ifsmirnov")
    @pytest.mark.timeout(5000)
    def test_stress_transitions(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")

        def _force_create_table():
            schema = yson.YsonList([
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ])
            attributes = {"schema": schema, "dynamic": True}
            create("table", "//tmp/t", attributes=attributes, force=True)

        def _create_action():
            tablet_id = get("//tmp/t/@tablets/0/tablet_id")
            create("tablet_action", "", attributes={"kind": "reshard", "tablet_ids": [tablet_id], "pivot_keys": [[]]})

        callbacks = [
            lambda: mount_table("//tmp/t", freeze=False),
            lambda: mount_table("//tmp/t", freeze=True),
            lambda: unmount_table("//tmp/t", force=False),
            lambda: unmount_table("//tmp/t", force=True),
            lambda: freeze_table("//tmp/t"),
            lambda: unfreeze_table("//tmp/t"),
            lambda: cancel_tablet_transition(get("//tmp/t/@tablets/0/tablet_id")),
            _force_create_table,
            _create_action,
        ]

        random.seed(1234)

        for i in range(200):
            sleep(random.choice((0, .100, .200, .500, .1000)))
            try:
                random.choice(callbacks)()
            except YtError:
                pass

    @authors("ifsmirnov")
    @pytest.mark.parametrize("sorted", [True, False])
    @pytest.mark.parametrize("replicated", [True, False])
    @pytest.mark.parametrize("transition_type", ["unmount", "freeze"])
    def test_cancel_transition(self, sorted, replicated, transition_type):
        sync_create_cells(1)

        schema = yson.YsonList([
            {"name": "key", "type": "int64"},
            {"name": "value", "type": "string"},
        ])
        if sorted:
            schema.attributes["unique_keys"] = True
            schema[0]["sort_order"] = "ascending"

        create(
            "replicated_table" if replicated else "table",
            "//tmp/t",
            attributes={"schema": schema, "dynamic": True})

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        if replicated:
            create("table", "//tmp/r1", attributes={"schema": schema, "dynamic": True})
            create("table", "//tmp/r2", attributes={"schema": schema, "dynamic": True})
            r1 = create_table_replica("//tmp/t", "primary", "//tmp/r1", attributes={"mode": "sync"})
            r2 = create_table_replica("//tmp/t", "primary", "//tmp/r2", attributes={"mode": "async"})
            alter_table("//tmp/r1", upstream_replica_id=r1)
            alter_table("//tmp/r2", upstream_replica_id=r2)
            sync_mount_table("//tmp/r1")
            sync_mount_table("//tmp/r2")

        sync_mount_table("//tmp/t")

        if replicated:
            # NB: Should do it after mount because sync_mount_table does not wait for
            # replicas to become enabled and sleep_before_post_to_master may cause
            # race conditions here.
            sync_enable_table_replica(r1)
            sync_enable_table_replica(r2)

        def _set_flush_enabled(enabled):
            set("//tmp/t/@mount_config/testing", {
                "flush_failure_probability": 0.0 if enabled else 1.0,
            })
            remount_table("//tmp/t")

        counter = 0

        def _make_row():
            nonlocal counter
            row = {"key": counter, "value": str(counter)}
            counter += 1
            return row

        rows = []

        _set_flush_enabled(False)

        rows.append(_make_row())
        insert_rows("//tmp/t", [rows[-1]])

        if transition_type == "unmount":
            unmount_table("//tmp/t")
            wait(lambda: get(f"#{tablet_id}/@state") == "unmounting")
        else:
            freeze_table("//tmp/t")
            wait(lambda: get(f"#{tablet_id}/@state") == "freezing")

        cancel_tablet_transition(tablet_id)
        if sorted and not replicated:
            wait(lambda: get(f"#{tablet_id}/@state") == "mounted")
        else:
            # Cancelation during flush is not supported for physically ordered tables.
            # However, transition of the replicate tables may be successfully cancelled
            # if cancelation comes when replication is in progress.
            sleep(3)

            if transition_type == "unmount":
                assert get(f"#{tablet_id}/@state") in ("unmounting", "mounted")
                _set_flush_enabled(True)
                if get(f"#{tablet_id}/@state") != "mounted":
                    wait(lambda: get(f"#{tablet_id}/@state") == "unmounted")
                    sync_mount_table("//tmp/t")
            else:
                assert get(f"#{tablet_id}/@state") in ("freezing", "mounted")
                _set_flush_enabled(True)
                if get(f"#{tablet_id}/@state") != "mounted":
                    wait(lambda: get(f"#{tablet_id}/@state") == "frozen")
                    sync_unfreeze_table("//tmp/t")

        if replicated:
            sync_unmount_table("//tmp/r2")

        rows.append(_make_row())
        insert_rows("//tmp/t", [rows[-1]])

        assert_items_equal(select_rows("key, value from [//tmp/t]"), rows)
        if replicated:
            sync_mount_table("//tmp/r2")
            assert_items_equal(select_rows("key, value from [//tmp/r1]"), rows)
            wait(lambda: len(select_rows("key, value from [//tmp/r2]")) == len(rows))

        _set_flush_enabled(True)
        sync_flush_table("//tmp/t")

        rows.append(_make_row())
        insert_rows("//tmp/t", [rows[-1]])

        assert_items_equal(select_rows("key, value from [//tmp/t]"), rows)
        if replicated:
            assert_items_equal(select_rows("key, value from [//tmp/r1]"), rows)
            wait(lambda: len(select_rows("key, value from [//tmp/r2]")) == len(rows))

    @authors("ifsmirnov")
    def test_cancel_transition_invalid_state(self):
        cell_id = sync_create_cells(1)[0]
        self._create_sorted_table("//tmp/t")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        set("//sys/@config/tablet_manager/peer_revocation_timeout", 0)

        def _disable_tablet_cell():
            set("//sys/tablet_cell_bundles/default/@node_tag_filter", "invalid")
            wait(lambda: get(f"#{cell_id}/@health") == "failed")

        def _enable_tablet_cell():
            set("//sys/tablet_cell_bundles/default/@node_tag_filter", "")
            wait(lambda: get(f"#{cell_id}/@health") == "good")

        def _check_transition_fails():
            with raises_yt_error("Cannot cancel transition"):
                cancel_tablet_transition(tablet_id)

        _disable_tablet_cell()

        # Mounting.
        mount_table("//tmp/t", target_cell_ids=[cell_id])
        wait(lambda: get("//tmp/t/@tablets/0/state") == "mounting")
        _check_transition_fails()

        sync_unmount_table("//tmp/t", force=True)

        # Frozen_mounting.
        mount_table("//tmp/t", target_cell_ids=[cell_id], freeze=True)
        wait(lambda: get("//tmp/t/@tablets/0/state") == "frozen_mounting")
        _check_transition_fails()

        _enable_tablet_cell()
        wait(lambda: get("//tmp/t/@tablets/0/state") == "frozen")
        _disable_tablet_cell()

        # Unfreezing.
        unfreeze_table("//tmp/t")
        wait(lambda: get("//tmp/t/@tablets/0/state") == "unfreezing")
        _check_transition_fails()

        _enable_tablet_cell()
        wait(lambda: get("//tmp/t/@tablets/0/state") == "mounted")

        # Mounted: no-op.
        cancel_tablet_transition(tablet_id)

        # Unmounted: no-op.
        sync_unmount_table("//tmp/t")
        cancel_tablet_transition(tablet_id)

    @authors("ifsmirnov")
    def test_cancel_transition_permissions(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        create_user("u")

        set("//tmp/t/@acl/end", {"subjects": ["u"], "action": "deny", "permissions": ["mount"]})
        multicell_sleep()
        with raises_yt_error():
            cancel_tablet_transition(tablet_id, authenticated_user="u")

        remove("//tmp/t/@acl/0")
        set("//tmp/t/@acl/end", {"subjects": ["u"], "action": "allow", "permissions": ["mount"]})
        multicell_sleep()
        cancel_tablet_transition(tablet_id, authenticated_user="u")

    @authors("ifsmirnov")
    def test_cancel_transition_dynamic_store_read(self):
        sync_create_cells(1)
        self._create_sorted_table(
            "//tmp/t",
            enable_dynamic_store_read=True,
            mount_config={
                "testing": {
                    "flush_failure_probability": 1.0,
                }
            })
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        sync_mount_table("//tmp/t")

        for i in range(5):
            insert_rows("//tmp/t", [{"key": i, "value": str(i)}])
            unmount_table("//tmp/t")
            wait(lambda: get(f"#{tablet_id}/@state") == "unmounting")
            cancel_tablet_transition(tablet_id)
            wait(lambda: get(f"#{tablet_id}/@state") == "mounted")

        set("//tmp/t/@mount_config/testing/flush_failure_probability", 0.0)
        remount_table("//tmp/t")

        sync_unmount_table("//tmp/t")

    @authors("ifsmirnov")
    @pytest.mark.parametrize("transition_type", ["unmount", "freeze"])
    def DISABLED_test_cancel_transition_stuck_replica(self, transition_type):
        cell_ids = sync_create_cells(2)
        custom_area_id = create_area(
            "custom",
            cell_bundle_id=get("//sys/tablet_cell_bundles/default/@id"))
        set(f"#{cell_ids[1]}/@area", "custom")
        wait(lambda: get(f"#{cell_ids[1]}/@health") == "good")

        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ])

        create(
            "replicated_table",
            "//tmp/t",
            attributes={
                "schema": schema,
                "dynamic": True,
                "mount_config": {
                    "max_dynamic_store_row_count": 5,
                },
                "enable_dynamic_store_read": True,
            })
        sync_mount_table("//tmp/t", cell_id=cell_ids[0])
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        create("table", "//tmp/r", attributes={
            "schema": schema,
            "dynamic": True,
            "mount_config": {
                "testing": {
                    "sync_delay_in_write_transaction_commit": 5000,
                }
            }})
        r = create_table_replica("//tmp/t", "primary", "//tmp/r", attributes={"mode": "async"})
        alter_table("//tmp/r", upstream_replica_id=r)
        sync_mount_table("//tmp/r", cell_id=cell_ids[1])
        sync_enable_table_replica(r)

        def _has_stuck_replication_transaction():
            replicas = list(get(f"#{tablet_id}/orchid/replicas").values())
            return replicas[0]["prepared_replication_transaction"] != "0-0-0-0"

        insert_rows("//tmp/t", [{"key": 0, "value": "a"}], require_sync_replica=False)
        wait(_has_stuck_replication_transaction)
        set(f"#{custom_area_id}/@node_tag_filter", "invalid")
        assert _has_stuck_replication_transaction()

        for i in range(1, 5):
            insert_rows("//tmp/t", [{"key": i, "value": "a"}], require_sync_replica=False)

        if transition_type == "unmount":
            unmount_table("//tmp/t")
            wait(lambda: get(f"#{tablet_id}/@state") == "unmounting")
        else:
            freeze_table("//tmp/t")
            wait(lambda: get(f"#{tablet_id}/@state") == "freezing")
        assert _has_stuck_replication_transaction()

        cancel_tablet_transition(tablet_id)
        wait(lambda: get(f"#{tablet_id}/@state") == "mounted")
        assert _has_stuck_replication_transaction()

        for i in range(5, 10):
            insert_rows("//tmp/t", [{"key": i, "value": "a"}], require_sync_replica=False)

        remove("//tmp/r/@mount_config/testing")
        remount_table("//tmp/r")
        set(f"#{custom_area_id}/@node_tag_filter", "")
        wait(lambda: get(f"#{cell_ids[1]}/@health") == "good")

        expected_rows = [{"key": i, "value": "a"} for i in range(10)]
        wait(lambda: are_items_equal(expected_rows, select_rows("* from [//tmp/r]")))

    @authors("ifsmirnov")
    @pytest.mark.parametrize("transition_type", ["unmount", "freeze"])
    def test_cancel_transition_ordered_table_empty_store(self, transition_type):
        sync_create_cells(1)
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "key", "type": "int64"}],
            "dynamic": True,
            "mount_config": {
                "testing": {
                    "flush_failure_probability": 1.0,
                }
            }})
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        sync_mount_table("//tmp/t")

        if transition_type == "unmount":
            unmount_table("//tmp/t")
            wait(lambda: get(f"#{tablet_id}/@state") == "unmounting")
        else:
            freeze_table("//tmp/t")
            wait(lambda: get(f"#{tablet_id}/@state") == "freezing")

        cancel_tablet_transition(tablet_id)
        sleep(2)
        if transition_type == "unmount":
            assert get(f"#{tablet_id}/@state") == "unmounting"
        else:
            assert get(f"#{tablet_id}/@state") == "freezing"

    @authors("ifsmirnov")
    @pytest.mark.parametrize("transition_type", ["unmount", "freeze"])
    def test_cancel_transition_mounted_table_receives_dynamic_stores(self, transition_type):
        sync_create_cells(1)
        self._create_sorted_table(
            "//tmp/t",
            enable_dynamic_store_read=True,
            mount_config={
                "testing": {
                    "flush_failure_probability": 1.0,
                }
            })
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        sync_mount_table("//tmp/t")

        action = unmount_table if transition_type == "unmount" else freeze_table
        transitioning_state = "unmounting" if transition_type == "unmount" else "freezing"

        for i in range(5):
            action("//tmp/t")
            wait(lambda: get(f"#{tablet_id}/@state") == transitioning_state)
            for j in range(3):
                cancel_tablet_transition(tablet_id)
            wait(lambda: get(f"#{tablet_id}/@state") == "mounted")

        root_chunk_list_id = get("//tmp/t/@chunk_list_id")
        tablet_chunk_list_id = get(f"#{root_chunk_list_id}/@child_ids/0")
        master_store_ids = get(f"#{tablet_chunk_list_id}/@child_ids")

        node_store_ids = get(f"#{tablet_id}/orchid/dynamic_store_id_pool")
        node_store_ids += ls(f"#{tablet_id}/orchid/eden/stores")

        assert_items_equal(master_store_ids, node_store_ids)

    @authors("ifsmirnov")
    def test_stress_bulk_insert_in_transient_states(self):
        cell_ids = sync_create_cells(2)

        create("table", "//tmp/t_input")
        rows = [{"key": 1, "value": "1"}]
        write_table("//tmp/t_input", rows)

        self._create_sorted_table(
            "//tmp/t_output",
            enable_dynamic_store_read=True)
        sync_mount_table("//tmp/t_output")
        tablet_id = get("//tmp/t_output/@tablets/0/tablet_id")

        def _run_op():
            return map(
                in_="//tmp/t_input",
                out="//tmp/t_output",
                command="cat",
                spec={
                    "testing": {
                        "delay_inside_operation_commit": random.randint(0, 1000),
                        "delay_inside_operation_commit_stage": "stage5",
                    },
                },
                track=False)

        def _run_tablet_action():
            current_cell = get(f"#{tablet_id}/@cell_id")
            for cell_id in cell_ids:
                if cell_id != current_cell:
                    break
            else:
                assert False
            return create(
                "tablet_action",
                "",
                attributes={
                    "kind": "move",
                    "tablet_ids": [tablet_id],
                    "cell_ids": [cell_id],
                    "expiration_timeout": 60000,
                    "skip_freezing": random.choice([False, True]),
                })

        random.seed(555)

        op = None
        action_id = None

        for i in range(200):
            sleep(random.choice((0, .100, .200, .500, .1000)))

            if random.randint(0, 1) == 0:
                if action_id is None or get(f"#{action_id}/@state") in ("completed", "failed"):
                    action_id = _run_tablet_action()
            else:
                if op is None or op.get_state() in ("completed", "failed"):
                    op = _run_op()


##################################################################


class TestDynamicTableStateTransitionsMulticell(TestDynamicTableStateTransitions):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }


class TestDynamicTableStateTransitionsPortal(TestDynamicTableStateTransitionsMulticell):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    ENABLE_TMP_PORTAL = True

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host", "cypress_node_host"]},
        "12": {"roles": ["chunk_host"]},
    }


class TestDynamicTableStateTransitionsSequoia(TestDynamicTableStateTransitionsMulticell):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True

    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host", "sequoia_node_host"]},
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host", "sequoia_node_host"]},
    }
