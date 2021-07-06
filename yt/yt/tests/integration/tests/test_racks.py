from yt_env_setup import YTEnvSetup, wait

from yt_commands import (  # noqa
    authors, print_debug, wait, retry, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists, concatenate,
    create_account, remove_account,
    create_network_project, create_tmpdir, create_user, create_group, create_medium,
    create_pool, create_pool_tree, remove_pool_tree,
    create_data_center, create_rack, create_table, create_proxy_role,
    create_tablet_cell_bundle, remove_tablet_cell_bundle, create_tablet_cell, create_table_replica,
    make_ace, check_permission, add_member, remove_member, remove_group, remove_user,
    remove_network_project, remove_rack,
    make_batch_request, execute_batch, get_batch_error,
    start_transaction, abort_transaction, commit_transaction, lock,
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
    set_account_disk_space_limit, set_node_decommissioned, get_nodes, get_racks,
    get_account_disk_space, get_account_committed_disk_space,
    check_all_stderrs,
    create_test_tables, create_dynamic_table, PrepareTables,
    get_statistics, get_recursive_disk_space, get_chunk_owner_disk_space, cluster_resources_equal,
    make_random_string, raises_yt_error,
    build_snapshot, build_master_snapshots,
    gc_collect, is_multicell, clear_metadata_caches,
    get_driver, execute_command, generate_uuid,
    AsyncLastCommittedTimestamp, MinTimestamp)

from yt.environment.helpers import assert_items_equal
from yt.common import YtError

import pytest

import __builtin__

##################################################################


class TestRacks(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 20

    JOURNAL_DATA = [{"data": "payload" + str(i)} for i in xrange(0, 10)]
    FILE_DATA = "payload"

    def _get_replica_nodes(self, chunk_id):
        return list(str(x) for x in get("#{0}/@stored_replicas".format(chunk_id)))

    def _wait_for_safely_placed(self, chunk_id):
        def check():
            stat = get("#{0}/@replication_status/default".format(chunk_id))
            return not stat["unsafely_placed"] and not stat["overreplicated"]

        wait(lambda: check())

    def _set_rack(self, node, rack):
        set("//sys/cluster_nodes/" + node + "/@rack", rack)

    def _reset_rack(self, node):
        remove("//sys/cluster_nodes/" + node + "/@rack")

    def _get_rack(self, node):
        return get("//sys/cluster_nodes/" + node + "/@rack")

    def _has_rack(self, node):
        return "rack" in ls("//sys/cluster_nodes/" + node + "/@")

    def _init_n_racks(self, n):
        nodes = get_nodes()
        index = 0
        created_indexes = __builtin__.set()
        for node in nodes:
            rack = "r" + str(index)
            if index not in created_indexes:
                create_rack(rack)
                created_indexes.add(index)
            self._set_rack(node, rack)
            index = (index + 1) % n

    def _reset_all_racks(self):
        nodes = get_nodes()
        for node in nodes:
            self._reset_rack(node)

    def _set_rack_map(self, node_to_rack_map):
        racks = frozenset(node_to_rack_map.itervalues())
        for rack in racks:
            create_rack(rack)
        for node, rack in node_to_rack_map.iteritems():
            set("//sys/cluster_nodes/" + node + "/@rack", rack)

    def _get_max_replicas_per_rack(self, node_to_rack_map, chunk_id):
        replicas = self._get_replica_nodes(chunk_id)
        rack_to_counter = {}
        for replica in replicas:
            rack = node_to_rack_map[replica]
            rack_to_counter.setdefault(rack, 0)
            rack_to_counter[rack] += 1
        return max(rack_to_counter.itervalues())

    @authors("babenko", "ignat")
    def test_create(self):
        create_rack("r")
        assert get_racks() == ["r"]
        assert get("//sys/racks/r/@name") == "r"

    @authors("babenko", "ignat")
    def test_empty_name_fail(self):
        with pytest.raises(YtError):
            create_rack("")

    @authors("babenko", "ignat")
    def test_duplicate_name_fail(self):
        create_rack("r")
        with pytest.raises(YtError):
            create_rack("r")

    @authors("babenko", "ignat")
    def test_rename_success(self):
        create_rack("r1")
        n = get_nodes()[0]
        self._set_rack(n, "r1")

        set("//sys/racks/r1/@name", "r2")
        assert get("//sys/racks/r2/@name") == "r2"
        assert self._get_rack(n) == "r2"

    @authors("babenko", "ignat")
    def test_rename_fail(self):
        create_rack("r1")
        create_rack("r2")
        with pytest.raises(YtError):
            set("//sys/racks/r1/@name", "r2")

    @authors("babenko", "ignat")
    def test_assign_success1(self):
        create_rack("r")
        n = get_nodes()[0]
        self._set_rack(n, "r")
        assert self._get_rack(n) == "r"

    @authors("babenko", "ignat")
    def test_assign_success2(self):
        create_rack("r")
        nodes = get_nodes()
        for node in nodes:
            self._set_rack(node, "r")
        for node in nodes:
            assert self._get_rack(node) == "r"
        assert_items_equal(get("//sys/racks/r/@nodes"), nodes)

    @authors("babenko", "ignat")
    def test_remove(self):
        create_rack("r")
        nodes = get_nodes()
        for node in nodes:
            self._set_rack(node, "r")
        remove_rack("r")
        for node in nodes:
            assert not self._has_rack(node)

    @authors("babenko")
    def test_assign_fail(self):
        n = get_nodes()[0]
        with pytest.raises(YtError):
            self._set_rack(n, "r")

    @authors("babenko", "ignat")
    def test_write_regular(self):
        self._init_n_racks(2)
        create("file", "//tmp/file")
        write_file("//tmp/file", self.FILE_DATA, file_writer={"upload_replication_factor": 3})

    @authors("babenko", "ignat")
    def test_write_erasure(self):
        self._init_n_racks(6)
        create("file", "//tmp/file", attributes={"erasure_codec": "lrc_12_2_2"})
        write_file("//tmp/file", self.FILE_DATA)

    @authors("babenko", "ignat")
    def test_write_journal(self):
        self._init_n_racks(3)
        create("journal", "//tmp/j")
        write_journal("//tmp/j", self.JOURNAL_DATA)

    @authors("babenko")
    def test_unsafely_placed(self):
        create_rack("r1")
        create_rack("r2")

        nodes = ls("//sys/cluster_nodes")
        self._set_rack(nodes[0], "r1")
        for i in xrange(1, len(nodes)):
            self._set_rack(nodes[i], "r2")

        create("file", "//tmp/file")
        write_file("//tmp/file", self.FILE_DATA, file_writer={"upload_replication_factor": 3})

        chunk_id = get_singular_chunk_id("//tmp/file")

        self._set_rack(nodes[0], "r2")
        wait(lambda: get("#" + chunk_id + "/@replication_status/default/unsafely_placed"))

        self._reset_all_racks()
        wait(lambda: not get("#" + chunk_id + "/@replication_status/default/unsafely_placed"))

    @authors("babenko")
    def test_regular_move_to_safe_place(self):
        create("file", "//tmp/file")
        write_file("//tmp/file", self.FILE_DATA, file_writer={"upload_replication_factor": 3})

        chunk_id = get_singular_chunk_id("//tmp/file")
        assert not get("#" + chunk_id + "/@replication_status/default/unsafely_placed")

        replicas = self._get_replica_nodes(chunk_id)
        assert len(replicas) == 3

        mapping = {}
        nodes = get_nodes()
        for node in nodes:
            if node in replicas:
                mapping[node] = "r1"
            else:
                mapping[node] = "r2"
        self._set_rack_map(mapping)

        self._wait_for_safely_placed(chunk_id)

        assert self._get_max_replicas_per_rack(mapping, chunk_id) <= 2

    @authors("babenko", "ignat")
    def test_erasure_move_to_safe_place(self):
        create("file", "//tmp/file", attributes={"erasure_codec": "lrc_12_2_2"})
        write_file("//tmp/file", self.FILE_DATA)

        chunk_id = get_singular_chunk_id("//tmp/file")
        assert not get("#" + chunk_id + "/@replication_status/default/unsafely_placed")

        replicas = self._get_replica_nodes(chunk_id)
        assert len(replicas) == 16

        # put unlisted nodes to the end
        nodes = get_nodes()
        replicas_plus_nodes = replicas
        for node in nodes:
            if node not in replicas:
                replicas_plus_nodes.append(node)

        assert len(replicas_plus_nodes) == TestRacks.NUM_NODES
        mapping = {
            replicas_plus_nodes[0]: "r1",
            replicas_plus_nodes[1]: "r1",
            replicas_plus_nodes[2]: "r1",
            replicas_plus_nodes[3]: "r1",
            replicas_plus_nodes[4]: "r1",
            replicas_plus_nodes[5]: "r2",
            replicas_plus_nodes[6]: "r2",
            replicas_plus_nodes[7]: "r2",
            replicas_plus_nodes[8]: "r3",
            replicas_plus_nodes[9]: "r3",
            replicas_plus_nodes[10]: "r3",
            replicas_plus_nodes[11]: "r4",
            replicas_plus_nodes[12]: "r4",
            replicas_plus_nodes[13]: "r4",
            replicas_plus_nodes[14]: "r5",
            replicas_plus_nodes[15]: "r5",
            replicas_plus_nodes[16]: "r5",
            replicas_plus_nodes[17]: "r6",
            replicas_plus_nodes[18]: "r6",
            replicas_plus_nodes[19]: "r6",
        }
        self._set_rack_map(mapping)

        self._wait_for_safely_placed(chunk_id)

        assert self._get_max_replicas_per_rack(mapping, chunk_id) <= 3

    @authors("shakurov")
    def test_decommission_with_3_racks_yt_9720(self):
        set("//sys/media/default/@config/max_regular_replicas_per_rack", 1)

        create_rack("r0")
        create_rack("r1")
        create_rack("r2")

        nodes = ls("//sys/cluster_nodes")
        for i in xrange(len(nodes)):
            rack = "r" + str(i % 3)
            self._set_rack(nodes[i], rack)

        create("file", "//tmp/file")
        write_file("//tmp/file", self.FILE_DATA, file_writer={"upload_replication_factor": 3})

        chunk_id = get_singular_chunk_id("//tmp/file")

        replicas = get("#" + chunk_id + "/@stored_replicas")

        assert len(replicas) == 3
        node_to_decommission = replicas[0]
        set_node_decommissioned(node_to_decommission, True)

        def decommission_successful():
            replicas = get("#" + chunk_id + "/@stored_replicas")
            return len(replicas) == 3 and (node_to_decommission not in replicas)

        wait(decommission_successful)

        set("//sys/media/default/@config/max_regular_replicas_per_rack", 64)

    @authors("babenko")
    def test_journal_move_to_safe_place(self):
        create("journal", "//tmp/j")
        write_journal("//tmp/j", self.JOURNAL_DATA)
        wait_until_sealed("//tmp/j")

        chunk_id = get_singular_chunk_id("//tmp/j")
        assert not get("#" + chunk_id + "/@replication_status/default/unsafely_placed")

        replicas = self._get_replica_nodes(chunk_id)
        assert len(replicas) == 3

        mapping = {}
        nodes = get_nodes()
        for i in xrange(len(nodes)):
            mapping[nodes[i]] = "r" + str(i % 3)
        for node in replicas:
            mapping[node] = "r0"
        self._set_rack_map(mapping)

        self._wait_for_safely_placed(chunk_id)

        assert self._get_max_replicas_per_rack(mapping, chunk_id) <= 1

    @authors("babenko")
    def test_journals_with_degraded_racks(self):
        mapping = {}
        nodes = get_nodes()
        for i in xrange(len(nodes)):
            mapping[nodes[i]] = "r" + str(i % 2)
        self._set_rack_map(mapping)

        create("journal", "//tmp/j")
        write_journal("//tmp/j", self.JOURNAL_DATA)
        wait_until_sealed("//tmp/j")

        assert read_journal("//tmp/j") == self.JOURNAL_DATA

    @authors("babenko")
    def test_rack_count_limit(self):
        for i in xrange(255):
            create_rack("r" + str(i))
        with pytest.raises(YtError):
            create_rack("too_many")

    @authors("shakurov")
    def test_max_replication_factor(self):
        set("//sys/media/default/@config/max_regular_replicas_per_rack", 1)
        self._init_n_racks(6)

        create("file", "//tmp/file", attributes={"replication_factor": 10})
        write_file("//tmp/file", self.FILE_DATA, file_writer={"upload_replication_factor": 10})

        chunk_id = get_singular_chunk_id("//tmp/file")

        replication_status = get("#{0}/@replication_status/default".format(chunk_id))
        assert replication_status["underreplicated"] or replication_status["unsafely_placed"]

        set("//sys/media/default/@config/max_replication_factor", 6)

        def chunk_is_ok():
            replication_status = get("#{0}/@replication_status/default".format(chunk_id))
            return not replication_status["underreplicated"] and not replication_status["unsafely_placed"]

        wait(lambda: chunk_is_ok)

        set("//sys/media/default/@config/max_regular_replicas_per_rack", 64)


##################################################################


class TestRacksMulticell(TestRacks):
    NUM_SECONDARY_MASTER_CELLS = 2
