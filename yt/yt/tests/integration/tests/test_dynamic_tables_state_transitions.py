from test_dynamic_tables import DynamicTablesBase

from yt_env_setup import wait, Restarter, NODES_SERVICE

from yt_commands import (  # noqa
    authors, print_debug, wait, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists, concatenate,
    create_account, create_network_project, create_tmpdir, create_user, create_group,
    create_pool, create_pool_tree, remove_pool_tree,
    create_data_center, create_rack, create_table,
    create_tablet_cell_bundle, remove_tablet_cell_bundle, create_tablet_cell, create_table_replica,
    make_ace, check_permission, add_member, remove_member, remove_group, remove_user,
    remove_network_project,
    make_batch_request, execute_batch, get_batch_error,
    start_transaction, abort_transaction, commit_transaction, lock,
    insert_rows, select_rows, lookup_rows, delete_rows, trim_rows, alter_table,
    read_file, write_file, read_table, write_table, write_local_file, read_blob_table,
    map, reduce, map_reduce, join_reduce, merge, vanilla, sort, erase, remote_copy,
    run_test_vanilla, run_sleeping_vanilla,
    abort_job, list_jobs, get_job, abandon_job, interrupt_job,
    get_job_fail_context, get_job_input, get_job_stderr, get_job_spec,
    dump_job_context, poll_job_shell,
    abort_op, complete_op, suspend_op, resume_op,
    get_operation, list_operations, clean_operations,
    get_operation_cypress_path, scheduler_orchid_pool_path,
    scheduler_orchid_default_pool_tree_path, scheduler_orchid_operation_path,
    scheduler_orchid_default_pool_tree_config_path, scheduler_orchid_path,
    scheduler_orchid_node_path, scheduler_orchid_pool_tree_config_path, scheduler_orchid_pool_tree_path,
    mount_table, unmount_table, freeze_table, unfreeze_table, reshard_table, remount_table, generate_timestamp,
    reshard_table_automatic, wait_for_tablet_state, wait_for_cells,
    get_tablet_infos, get_table_pivot_keys, get_tablet_leader_address,
    sync_create_cells, sync_mount_table, sync_unmount_table,
    sync_freeze_table, sync_unfreeze_table, sync_reshard_table,
    sync_flush_table, sync_compact_table, sync_remove_tablet_cells,
    sync_reshard_table_automatic, sync_balance_tablet_cells,
    get_first_chunk_id, get_singular_chunk_id, get_chunk_replication_factor, multicell_sleep,
    update_nodes_dynamic_config, update_controller_agent_config,
    update_op_parameters, enable_op_detailed_logs,
    set_node_banned, set_banned_flag, set_account_disk_space_limit, set_node_decommissioned,
    check_all_stderrs,
    create_test_tables, create_dynamic_table, PrepareTables,
    get_statistics, get_recursive_disk_space, get_chunk_owner_disk_space,
    make_random_string, raises_yt_error,
    build_snapshot, is_multicell,
    get_driver, Driver, execute_command,
    AsyncLastCommittedTimestamp)

from yt.common import YtError

import pytest

##################################################################


class TestDynamicTableStateTransitions(DynamicTablesBase):
    NUM_TEST_PARTITIONS = 3

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "tablet_manager": {
            "leader_reassignment_timeout": 2000,
            "peer_revocation_timeout": 600000,
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


##################################################################


class TestDynamicTableStateTransitionsMulticell(TestDynamicTableStateTransitions):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestDynamicTableStateTransitionsPortal(TestDynamicTableStateTransitionsMulticell):
    ENABLE_TMP_PORTAL = True
