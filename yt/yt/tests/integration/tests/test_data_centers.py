from yt_env_setup import YTEnvSetup

from yt_commands import (  # noqa
    authors, print_debug, wait, retry, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists, concatenate,
    create_account, remove_account,
    create_network_project, create_tmpdir, create_user, create_group, create_medium,
    create_pool, create_pool_tree, remove_pool_tree,
    create_data_center, create_rack, create_table, create_proxy_role,
    create_tablet_cell_bundle, remove_tablet_cell_bundle, create_tablet_cell, create_table_replica,
    make_ace, check_permission, check_permission_by_acl, add_member, remove_member, remove_group, remove_user,
    remove_network_project, remove_data_center,
    make_batch_request, execute_batch, get_batch_error,
    start_transaction, abort_transaction, commit_transaction, lock, unlock,
    externalize, internalize,
    insert_rows, select_rows, lookup_rows, delete_rows, trim_rows, alter_table,
    read_file, write_file, read_table, write_table, write_local_file, read_blob_table,
    read_journal, write_journal, truncate_journal, wait_until_sealed,
    map, reduce, map_reduce, join_reduce, merge, vanilla, sort, erase, remote_copy,
    run_test_vanilla, run_sleeping_vanilla,
    abort_job, list_jobs, get_job, abandon_job, interrupt_job,
    get_job_fail_context, get_job_input, get_job_stderr, get_job_spec, get_job_input_paths,
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
    set_node_banned, set_banned_flag,
    set_account_disk_space_limit, set_node_decommissioned,
    get_account_disk_space, get_account_committed_disk_space,
    get_nodes, get_racks, get_data_centers,
    check_all_stderrs,
    create_test_tables, create_dynamic_table, PrepareTables,
    get_statistics, get_recursive_disk_space, get_chunk_owner_disk_space, cluster_resources_equal,
    make_random_string, raises_yt_error,
    build_snapshot, build_master_snapshots,
    gc_collect, is_multicell, clear_metadata_caches,
    get_driver, Driver, execute_command, generate_uuid,
    AsyncLastCommittedTimestamp, MinTimestamp)

from yt.environment.helpers import assert_items_equal

from yt.common import YtError

import pytest

import time
import __builtin__

##################################################################


class TestDataCenters(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 20

    JOURNAL_DATA = [{"data": "payload" + str(i)} for i in xrange(0, 10)]
    FILE_DATA = "payload"

    def _get_replica_nodes(self, chunk_id):
        return list(str(x) for x in get("#" + chunk_id + "/@stored_replicas"))

    def _wait_for_safely_placed(self, chunk_id, replica_count):
        ok = False
        for i in xrange(60):
            if (
                not get("#" + chunk_id + "/@replication_status/unsafely_placed")
                and len(self._get_replica_nodes(chunk_id)) == replica_count
            ):
                ok = True
                break
            time.sleep(1.0)
        assert ok

    def _set_rack(self, node, rack):
        set("//sys/cluster_nodes/" + node + "/@rack", rack)

    def _set_data_center(self, rack, dc):
        set("//sys/racks/" + rack + "/@data_center", dc)

    def _unset_data_center(self, rack):
        remove("//sys/racks/" + rack + "/@data_center")

    def _reset_rack(self, node):
        remove("//sys/cluster_nodes/" + node + "/@rack")

    def _reset_data_center(self, rack):
        remove("//sys/racks/" + rack + "/@data_center")

    def _get_rack(self, node):
        return get("//sys/cluster_nodes/" + node + "/@rack")

    def _get_data_center(self, rack):
        return get("//sys/racks/" + rack + "/@data_center")

    def _has_rack(self, node):
        return "rack" in ls("//sys/cluster_nodes/" + node + "/@")

    def _has_data_center(self, rack):
        return "data_center" in ls("//sys/racks/" + rack + "/@")

    def _init_n_racks(self, n):
        node_to_rack_map = {}
        nodes = get_nodes()
        index = 0
        created_indexes = __builtin__.set()
        for node in nodes:
            rack = "r" + str(index)
            if index not in created_indexes:
                create_rack(rack)
                created_indexes.add(index)
            self._set_rack(node, rack)
            node_to_rack_map[node] = rack
            index = (index + 1) % n
        return node_to_rack_map

    def _init_n_data_centers(self, n):
        rack_to_dc_map = {}
        racks = get_racks()
        index = 0
        created_indexes = __builtin__.set()
        for rack in racks:
            dc = "d" + str(index)
            if index not in created_indexes:
                create_data_center(dc)
                created_indexes.add(index)
            self._set_data_center(rack, dc)
            rack_to_dc_map[rack] = dc
            index = (index + 1) % n
        return rack_to_dc_map

    def _reset_all_racks(self):
        nodes = get_nodes()
        for node in nodes:
            self._reset_rack(node)

    def _reset_all_data_centers(self):
        racks = get_racks()
        for rack in racks:
            self._reset_data_center(rack)

    def _set_rack_map(self, node_to_rack_map):
        racks = frozenset(node_to_rack_map.itervalues())
        for rack in racks:
            create_rack(rack)
        for node, rack in node_to_rack_map.iteritems():
            set("//sys/cluster_nodes/" + node + "/@rack", rack)

    def _set_data_center_map(self, rack_to_dc_map):
        dcs = frozenset(rack_to_dc_map.itervalues())
        for dc in dcs:
            create_data_center(dc)
        for rack, dc in rack_to_dc_map.iteritems():
            set("//sys/racks/" + rack + "/@data_center", dc)

    def _get_max_replicas_per_rack(self, node_to_rack_map, chunk_id):
        replicas = self._get_replica_nodes(chunk_id)
        rack_to_counter = {}
        for replica in replicas:
            rack = node_to_rack_map[replica]
            rack_to_counter.setdefault(rack, 0)
            rack_to_counter[rack] += 1
        return max(rack_to_counter.itervalues())

    def _get_max_replicas_per_data_center(self, node_to_rack_map, rack_to_dc_map, chunk_id):
        replicas = self._get_replica_nodes(chunk_id)
        dc_to_counter = {}
        for replica in replicas:
            rack = node_to_rack_map[replica]
            dc = rack_to_dc_map[rack]
            dc_to_counter.setdefault(dc, 0)
            dc_to_counter[rack] += 1
        return max(dc_to_counter.itervalues())

    @authors("shakurov")
    def test_create(self):
        create_data_center("d")
        assert get_data_centers() == ["d"]
        assert get("//sys/data_centers/d/@name") == "d"

    @authors("shakurov")
    def test_empty_name_fail(self):
        with pytest.raises(YtError):
            create_data_center("")

    @authors("shakurov")
    def test_duplicate_name_fail(self):
        create_data_center("d")
        with pytest.raises(YtError):
            create_data_center("d")

    @authors("shakurov")
    def test_rename_success(self):
        create_data_center("d1")
        create_rack("r1")
        create_rack("r2")

        nodes = get_nodes()
        n1 = nodes[0]
        n2 = nodes[1]
        self._set_rack(n1, "r1")
        self._set_rack(n2, "r2")

        self._set_data_center("r1", "d1")
        # r2 is kept out of d1 deliberately.

        set("//sys/data_centers/d1/@name", "d2")
        assert get("//sys/data_centers/d2/@name") == "d2"
        assert self._get_data_center("r1") == "d2"

    @authors("shakurov")
    def test_rename_fail(self):
        create_data_center("d1")
        create_data_center("d2")
        with pytest.raises(YtError):
            set("//sys/data_centers/d1/@name", "d2")

    @authors("shakurov")
    def test_assign_success1(self):
        create_data_center("d")
        create_rack("r")
        n = get_nodes()[0]
        self._set_rack(n, "r")
        self._set_data_center("r", "d")
        assert self._get_data_center("r") == "d"
        assert self._get_data_center(self._get_rack(n)) == "d"

    @authors("shakurov")
    def test_assign_success2(self):
        self._init_n_racks(10)
        self._init_n_data_centers(1)
        nodes = get_nodes()
        for node in nodes:
            assert self._get_data_center(self._get_rack(node)) == "d0"
        assert_items_equal(get("//sys/data_centers/d0/@racks"), get_racks())

    @authors("shakurov")
    def test_unassign(self):
        create_data_center("d")
        create_rack("r")
        n = get_nodes()[0]
        self._set_rack(n, "r")
        self._set_data_center("r", "d")
        self._unset_data_center("r")
        assert not self._has_data_center("r")

    @authors("shakurov")
    def test_tags(self):
        n = get_nodes()[0]

        tags = get("//sys/cluster_nodes/{0}/@tags".format(n))
        assert "r" not in tags
        assert "d" not in tags

        create_data_center("d")
        create_rack("r")
        self._set_rack(n, "r")

        tags = get("//sys/cluster_nodes/{0}/@tags".format(n))
        assert "r" in tags
        assert "d" not in tags

        self._set_data_center("r", "d")
        tags = get("//sys/cluster_nodes/{0}/@tags".format(n))
        assert "r" in tags
        assert "d" in tags

        self._unset_data_center("r")

        tags = get("//sys/cluster_nodes/{0}/@tags".format(n))
        assert "r" in tags
        assert "d" not in tags

    @authors("shakurov")
    def test_remove(self):
        self._init_n_racks(10)
        self._init_n_data_centers(1)
        remove_data_center("d0")
        racks = get_racks()
        for rack in racks:
            assert not self._has_data_center(rack)

    @authors("shakurov")
    def test_assign_fail(self):
        create_rack("r")
        n = get_nodes()[0]
        self._set_rack(n, "r")
        with pytest.raises(YtError):
            self._set_data_center("r", "d")

    @authors("shakurov")
    def test_write_regular(self):
        self._init_n_racks(10)
        self._init_n_data_centers(3)
        create("file", "//tmp/file")
        write_file("//tmp/file", self.FILE_DATA, file_writer={"upload_replication_factor": 3})

    @authors("shakurov")
    def test_write_erasure(self):
        self._init_n_racks(10)
        self._init_n_data_centers(3)
        create("file", "//tmp/file", attributes={"erasure_codec": "lrc_12_2_2"})
        write_file("//tmp/file", self.FILE_DATA)

    @authors("shakurov")
    def test_write_journal(self):
        self._init_n_racks(10)
        self._init_n_data_centers(3)
        create("journal", "//tmp/j")
        write_journal("//tmp/j", self.JOURNAL_DATA)

    @authors("shakurov")
    def test_journals_with_degraded_data_centers(self):
        self._init_n_racks(10)
        self._init_n_data_centers(2)

        create("journal", "//tmp/j")
        write_journal("//tmp/j", self.JOURNAL_DATA)
        wait_until_sealed("//tmp/j")

        assert read_journal("//tmp/j") == self.JOURNAL_DATA

    @authors("shakurov")
    def test_data_center_count_limit(self):
        for i in xrange(16):
            create_data_center("d" + str(i))
        with pytest.raises(YtError):
            create_data_center("too_many")


##################################################################


class TestDataCentersMulticell(TestDataCenters):
    NUM_SECONDARY_MASTER_CELLS = 2
