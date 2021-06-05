from yt_env_setup import YTEnvSetup

from yt_commands import (  # noqa
    authors, print_debug, wait, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists,
    create_account, create_network_project, create_tmpdir, create_user, create_group,
    create_pool, create_pool_tree, remove_pool_tree,
    create_data_center, create_rack, create_table,
    make_ace, check_permission, add_member,
    make_batch_request, execute_batch, get_batch_error,
    start_transaction, abort_transaction, commit_transaction, lock,
    insert_rows, select_rows, lookup_rows, delete_rows, trim_rows, alter_table,
    read_file, write_file, read_table, write_table, write_local_file,
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
    mount_table, wait_for_tablet_state,
    sync_create_cells, sync_mount_table, sync_unmount_table,
    sync_freeze_table, sync_unfreeze_table, sync_reshard_table,
    sync_flush_table, sync_compact_table,
    get_first_chunk_id, get_singular_chunk_id, get_chunk_replication_factor, multicell_sleep,
    update_nodes_dynamic_config, update_controller_agent_config,
    update_op_parameters, enable_op_detailed_logs,
    set_node_banned, set_banned_flag, set_account_disk_space_limit,
    check_all_stderrs,
    create_test_tables, create_dynamic_table, PrepareTables,
    get_statistics,
    make_random_string, raises_yt_error,
    build_snapshot, build_master_snapshots,
    get_driver, Driver, execute_command)

import yt.yson as yson

import subprocess
import os
import os.path

##################################################################

# The primary purpose of this test is to check that the snapshot validation process runs at all
# and to check anything specific. The usual reason of failure is accidental network usage
# in TBootstrap::Initialize.

##################################################################


class TestSnapshotValidation(YTEnvSetup):
    NUM_MASTERS = 1

    @authors("ifsmirnov")
    def test_master_snapshot_validation(self):
        create("table", "//tmp/t")
        build_master_snapshots()

        snapshot_dir = os.path.join(self.path_to_run, "runtime_data", "master", "0", "snapshots")
        snapshots = [name for name in os.listdir(snapshot_dir) if name.endswith("snapshot")]
        assert len(snapshots) > 0
        snapshot_path = os.path.join(snapshot_dir, snapshots[0])

        config_path = os.path.join(self.path_to_run, "configs", "master-0-0.yson")

        binary = os.path.join(self.bin_path, "ytserver-master")

        # NB: Sleep after initialize is required since the main thread otherwise can halt before
        # some other thread uses network.
        command = [
            binary,
            "--validate-snapshot",
            snapshot_path,
            "--config",
            config_path,
            "--sleep-after-initialize",
        ]

        ret = subprocess.run(command)
        assert ret.returncode == 0

    @authors("akozhikhov")
    def test_tablet_cell_snapshot_validation(self):
        [cell_id] = sync_create_cells(1)

        snapshot_path = "//sys/tablet_cells/{}/snapshots".format(cell_id)
        assert not ls(snapshot_path)
        build_snapshot(cell_id=cell_id)
        snapshots = ls(snapshot_path)
        assert snapshots

        binary = os.path.join(self.bin_path, "ytserver-node")

        config_path = os.path.join(self.path_to_run, "configs", "node-0.yson")

        config = None
        with open(config_path, "r") as fh:
            config = " ".join(fh.read().splitlines())
            config = yson.loads(config)

        config["data_node"]["store_locations"][0]["path"] = "."

        with open(config_path, "w") as fh:
            fh.write(yson.dumps(config, yson_format="pretty"))

        snapshot = read_file("{}/{}".format(snapshot_path, snapshots[0]))
        with open("snapshot_file", "w") as fh:
            fh.write(snapshot)

        command = [
            binary,
            "--validate-snapshot",
            "./snapshot_file",
            "--config",
            config_path,
            "--sleep-after-initialize",
        ]
        ret = subprocess.run(command)
        assert ret.returncode == 0
