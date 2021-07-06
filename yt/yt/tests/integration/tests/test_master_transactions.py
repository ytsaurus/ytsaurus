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
    make_ace, check_permission, add_member, remove_member, remove_group, remove_user,
    remove_network_project,
    make_batch_request, execute_batch, get_batch_error,
    start_transaction, abort_transaction, commit_transaction, ping_transaction, lock,
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
    check_all_stderrs,
    create_test_tables, create_dynamic_table, PrepareTables,
    get_transactions, get_topmost_transactions,
    get_statistics, get_recursive_disk_space, get_chunk_owner_disk_space,
    make_random_string, raises_yt_error,
    build_snapshot, build_master_snapshots,
    gc_collect, is_multicell, clear_metadata_caches,
    get_driver, execute_command,
    AsyncLastCommittedTimestamp, MinTimestamp)

from yt.environment.helpers import assert_items_equal
from yt.common import datetime_to_string, YtError

import pytest
from flaky import flaky

from time import sleep
from datetime import datetime, timedelta
import __builtin__

##################################################################


class TestMasterTransactions(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3

    @authors("babenko")
    def test_simple1(self):
        tx = start_transaction()

        assert exists("//sys/transactions/" + tx)

        commit_transaction(tx)

        assert not exists("//sys/transactions/" + tx)

        # cannot commit committed transaction
        with pytest.raises(YtError):
            commit_transaction(tx)

    @authors("babenko")
    def test_simple2(self):
        tx = start_transaction()

        assert exists("//sys/transactions/" + tx)

        abort_transaction(tx)

        assert not exists("//sys/transactions/" + tx)

        # cannot commit aborted transaction
        with pytest.raises(YtError):
            commit_transaction(tx)

    @authors("panin", "ignat")
    def test_changes_inside_tx(self):
        set("//tmp/value", "42")

        tx = start_transaction()
        set("//tmp/value", "100", tx=tx)

        # check that changes are not seen outside of transaction
        assert get("//tmp/value", tx=tx) == "100"
        assert get("//tmp/value") == "42"

        commit_transaction(tx)
        # changes after commit are applied
        assert get("//tmp/value") == "100"

        tx = start_transaction()
        set("//tmp/value", "100500", tx=tx)
        abort_transaction(tx)

        # changes after abort are not applied
        assert get("//tmp/value") == "100"

        remove("//tmp/value")

    @authors("panin", "ignat")
    def test_nested_tx1(self):
        set("//tmp/t1", 0)

        tx_outer = start_transaction()

        tx1 = start_transaction(tx=tx_outer)
        set("//tmp/t1", 1, tx=tx1)

        start_transaction(tx=tx_outer)

        assert get("//tmp/t1", tx=tx_outer) == 0

        commit_transaction(tx1)
        assert get("//tmp/t1", tx=tx_outer) == 1
        assert get("//tmp/t1") == 0

    @authors("babenko")
    def test_nested_tx2(self):
        set("//tmp/t", 0)
        set("//tmp/t1", 0)
        set("//tmp/t2", 0)

        tx_outer = start_transaction()
        set("//tmp/t", 1, tx=tx_outer)

        tx1 = start_transaction(tx=tx_outer)
        set("//tmp/t1", 1, tx=tx1)

        tx2 = start_transaction(tx=tx_outer)
        set("//tmp/t2", 1, tx=tx2)

        commit_transaction(tx_outer)

        gc_collect()

        assert not exists("//sys/transactions/" + tx_outer)
        assert not exists("//sys/transactions/" + tx1)
        assert not exists("//sys/transactions/" + tx2)

        assert get("//tmp/t") == 1
        assert get("//tmp/t1") == 0
        assert get("//tmp/t2") == 0

    @authors("babenko")
    def test_nested_tx3(self):
        set("//tmp/t", 0)
        set("//tmp/t1", 0)
        set("//tmp/t2", 0)

        tx_outer = start_transaction()
        set("//tmp/t", 1, tx=tx_outer)

        tx1 = start_transaction(tx=tx_outer)
        set("//tmp/t1", 1, tx=tx1)

        tx2 = start_transaction(tx=tx_outer)
        set("//tmp/t2", 1, tx=tx2)

        abort_transaction(tx_outer)

        gc_collect()

        assert not exists("//sys/transactions/" + tx_outer)
        assert not exists("//sys/transactions/" + tx1)
        assert not exists("//sys/transactions/" + tx2)

        assert get("//tmp/t") == 0
        assert get("//tmp/t1") == 0
        assert get("//tmp/t2") == 0

    @authors("panin", "ignat")
    @flaky(max_runs=5)
    def test_timeout(self):
        tx = start_transaction(timeout=2000)

        # check that transaction is still alive after 1 seconds
        sleep(1.0)
        assert exists("//sys/transactions/" + tx)

        # check that transaction is expired after 3 seconds
        sleep(3.0)
        assert not exists("//sys/transactions/" + tx)

    @authors("ignat")
    @flaky(max_runs=5)
    def test_deadline(self):
        tx = start_transaction(
            timeout=10000,
            deadline=datetime_to_string(datetime.utcnow() + timedelta(seconds=2)),
        )

        # check that transaction is still alive after 1 seconds
        sleep(1.0)
        assert exists("//sys/transactions/" + tx)

        # check that transaction is expired after 3 seconds
        sleep(3.0)
        assert not exists("//sys/transactions/" + tx)

    @authors("levysotsky")
    @flaky(max_runs=5)
    def test_set_timeout(self):
        tx = start_transaction(timeout=5 * 1000)
        set("//sys/transactions/{}/@timeout".format(tx), 10 * 1000)
        assert get("//sys/transactions/{}/@timeout".format(tx)) == 10 * 1000

        ping_transaction(tx)

        # check that transaction is still alive after 3 seconds
        sleep(3.0)
        assert exists("//sys/transactions/{}".format(tx))

        # check that transaction is expired after 3 seconds
        sleep(8.0)
        assert not exists("//sys/transactions/{}".format(tx))

    @authors("ignat")
    @flaky(max_runs=5)
    def test_ping(self):
        tx = start_transaction(timeout=3000)

        sleep(1)
        assert exists("//sys/transactions/" + tx)
        ping_transaction(tx)

        sleep(2)
        assert exists("//sys/transactions/" + tx)

    @authors("ignat", "panin")
    @flaky(max_runs=5)
    def test_expire_outer(self):
        tx_outer = start_transaction(timeout=3000)
        tx_inner = start_transaction(tx=tx_outer)

        sleep(1)
        assert exists("//sys/transactions/" + tx_inner)
        assert exists("//sys/transactions/" + tx_outer)
        ping_transaction(tx_inner)

        sleep(2.5)
        # check that outer tx expired (and therefore inner was aborted)
        assert not exists("//sys/transactions/" + tx_inner)
        assert not exists("//sys/transactions/" + tx_outer)

    @authors("ignat", "panin")
    @flaky(max_runs=5)
    def test_ping_ancestors(self):
        tx_outer = start_transaction(timeout=3000)
        tx_inner = start_transaction(tx=tx_outer)

        sleep(1)
        assert exists("//sys/transactions/" + tx_inner)
        assert exists("//sys/transactions/" + tx_outer)
        ping_transaction(tx_inner, ping_ancestor_txs=True)

        sleep(2)
        # check that all tx are still alive
        assert exists("//sys/transactions/" + tx_inner)
        assert exists("//sys/transactions/" + tx_outer)

    @authors("babenko")
    def test_tx_multicell_attrs(self):
        tx = start_transaction(timeout=60000)
        tx_cell_tag = str(get("#" + tx + "/@native_cell_tag"))
        cell_tags = [tx_cell_tag]

        sharded_tx = self.NUM_SECONDARY_MASTER_CELLS > 2

        if sharded_tx:
            create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 3})

            # Populate resolve cache so that passing through the portal doesn't affect tx replication.
            set("//tmp/p/@some_attr", "some_value")

            portal_exit_id = get("//tmp/p/@id")
            table_id = create("table", "//tmp/p/t", tx=tx)  # replicate tx to cell 3
            if "3" not in cell_tags:
                cell_tags.append("3")

        def check(r):
            assert_items_equal(r.keys(), cell_tags)
            for (k, v) in r.iteritems():
                assert v == []

        check(get("#" + tx + "/@staged_object_ids"))
        check(get("#" + tx + "/@imported_object_ids"))
        check(get("#" + tx + "/@exported_objects"))
        assert get("#" + tx + "/@imported_object_count") == 0
        assert get("#" + tx + "/@exported_object_count") == 0

        if sharded_tx:
            branched_node_ids = get("#" + tx + "/@branched_node_ids")
            assert len(branched_node_ids) == 2
            assert_items_equal(branched_node_ids["3"], [table_id, portal_exit_id])
            assert branched_node_ids[tx_cell_tag] == []

            locked_node_ids = get("#" + tx + "/@locked_node_ids")
            assert len(locked_node_ids) == 2
            assert_items_equal(locked_node_ids["3"], [table_id, portal_exit_id])
            assert locked_node_ids[tx_cell_tag] == []

            staged_node_ids = get("#" + tx + "/@staged_node_ids")
            assert len(staged_node_ids) == 2
            assert_items_equal(staged_node_ids["3"], [table_id])
            assert staged_node_ids[tx_cell_tag] == []

            assert len(get("#" + tx + "/@lock_ids/3")) == 2

    @authors("babenko")
    def test_transaction_maps(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        tx3 = start_transaction(tx=tx1)

        txs = get_transactions()
        assert tx1 in txs
        assert tx2 in txs
        assert tx3 in txs
        topmost_txs = get_topmost_transactions()
        assert tx1 in topmost_txs
        assert not (tx2 in topmost_txs)
        assert not (tx3 in topmost_txs)

        abort_transaction(tx2)
        txs = get_transactions()
        assert tx1 in txs
        assert not (tx2 in txs)
        assert tx3 in txs
        topmost_txs = get_topmost_transactions()
        assert tx1 in topmost_txs
        assert not (tx2 in topmost_txs)
        assert not (tx3 in topmost_txs)

        abort_transaction(tx1)
        txs = get_transactions()
        assert not (tx1 in txs)
        assert not (tx2 in txs)
        assert not (tx3 in txs)
        topmost_txs = get_topmost_transactions()
        assert not (tx1 in topmost_txs)
        assert not (tx2 in topmost_txs)
        assert not (tx3 in topmost_txs)

    @authors("babenko", "ignat")
    def test_revision1(self):
        set("//tmp/a", "b")
        r1 = get("//tmp/a/@revision")

        set("//tmp/a2", "b2")
        r2 = get("//tmp/a/@revision")
        assert r2 == r1

    @authors("babenko", "ignat")
    def test_revision2(self):
        r1 = get("//tmp/@revision")

        set("//tmp/a", "b")
        r2 = get("//tmp/@revision")
        assert r2 > r1

    @authors("babenko", "ignat")
    def test_revision3(self):
        set("//tmp/a", "b")
        r1 = get("//tmp/a/@revision")

        tx = start_transaction()

        set("//tmp/a", "c", tx=tx)
        r2 = get("//tmp/a/@revision")
        r3 = get("//tmp/a/@revision", tx=tx)
        assert r2 == r1
        assert r3 > r1

        commit_transaction(tx)
        r4 = get("//tmp/a/@revision")
        assert r4 > r1
        assert r4 > r3

    @authors("babenko")
    def test_revision4(self):
        if self.is_multicell():
            pytest.skip("@current_commit_revision not supported with sharded transactions")
            return

        r1 = get("//sys/@current_commit_revision")
        set("//tmp/t", 1)
        r2 = get("//tmp/t/@revision")
        assert r1 <= r2
        remove("//tmp/t")
        r3 = get("//sys/@current_commit_revision")
        assert r2 < r3

    @authors("babenko", "ignat")
    def test_abort_snapshot_lock(self):
        create("file", "//tmp/file")
        write_file("//tmp/file", "some_data")

        tx = start_transaction()

        lock("//tmp/file", mode="snapshot", tx=tx)
        remove("//tmp/file")
        abort_transaction(tx)

    @authors("babenko", "ignat")
    def test_commit_snapshot_lock(self):
        create("file", "//tmp/file")
        write_file("//tmp/file", "some_data")

        tx = start_transaction()

        lock("//tmp/file", mode="snapshot", tx=tx)
        remove("//tmp/file")
        commit_transaction(tx)

    @authors("babenko")
    def test_title(self):
        tx = start_transaction(attributes={"title": "My title"})
        assert get("#{0}/@title".format(tx)) == "My title"

    @authors("babenko")
    def test_custom_attr(self):
        tx = start_transaction(attributes={"myattr": "myvalue"})
        assert get("#{0}/@myattr".format(tx)) == "myvalue"

    @authors("babenko")
    def test_update_attr(self):
        tx = start_transaction()
        set("#{0}/@myattr".format(tx), "myvalue")
        assert get("#{0}/@myattr".format(tx)) == "myvalue"

    @authors("babenko")
    def test_owner(self):
        create_user("u")
        tx = start_transaction(authenticated_user="u")
        assert get("#{0}/@owner".format(tx)) == "u"

    @authors("ignat")
    def test_prerequisite_transactions(self):
        tx_a = start_transaction()
        tx_b = start_transaction(prerequisite_transaction_ids=[tx_a])

        assert exists("//sys/transactions/" + tx_a)
        assert exists("//sys/transactions/" + tx_b)

        assert get("//sys/transactions/{}/@prerequisite_transaction_ids".format(tx_b)) == [tx_a]
        assert get("//sys/transactions/{}/@dependent_transaction_ids".format(tx_a)) == [tx_b]

        ping_transaction(tx_a)
        ping_transaction(tx_b)

        abort_transaction(tx_a)

        assert not exists("//sys/transactions/" + tx_a)
        assert not exists("//sys/transactions/" + tx_b)

    @authors("shakurov")
    def test_prerequisite_tx_read_requests(self):
        good_tx = start_transaction()
        bad_tx = "a-b-c-d"

        get("//tmp/@id", prerequisite_transaction_ids=[good_tx])
        ls("//tmp/@", prerequisite_transaction_ids=[good_tx])
        exists("//tmp/@id", prerequisite_transaction_ids=[good_tx])

        with pytest.raises(YtError):
            get("//tmp/@id", prerequisite_transaction_ids=[bad_tx])
        with pytest.raises(YtError):
            ls("//tmp/@", prerequisite_transaction_ids=[bad_tx])
        with pytest.raises(YtError):
            exists("//tmp/@id", prerequisite_transaction_ids=[bad_tx])

    @authors("shakurov")
    def test_prerequisite_tx_write_requests(self):
        good_tx = start_transaction()
        bad_tx = "a-b-c-d"

        create("table", "//tmp/t1", prerequisite_transaction_ids=[good_tx])
        set("//tmp/@some_attr", "some_value", prerequisite_transaction_ids=[good_tx])
        remove("//tmp/t1", prerequisite_transaction_ids=[good_tx])

        with pytest.raises(YtError):
            create("table", "//tmp/t2", prerequisite_transaction_ids=[bad_tx])
        with pytest.raises(YtError):
            set("//tmp/@some_attr", "some_value", prerequisite_transaction_ids=[bad_tx])
        create("table", "//tmp/t3")
        with pytest.raises(YtError):
            remove("//tmp/t3", prerequisite_transaction_ids=[bad_tx])

    @authors("shakurov")
    def test_prerequisite_transactions_on_commit(self):
        tx_a = start_transaction()
        tx_b = start_transaction()

        with pytest.raises(YtError):
            commit_transaction(tx_b, prerequisite_transaction_ids=["a-b-c-d"])

        # Failing to commit a transaction with prerequisites provokes its abort.
        wait(lambda: not exists("//sys/transactions/" + tx_b), iter=100)

        tx_c = start_transaction()
        commit_transaction(tx_c, prerequisite_transaction_ids=[tx_a])

    @authors("babenko")
    def test_very_deep_transactions_yt_9961(self):
        tx = None
        for _ in xrange(10):
            if tx is None:
                tx = start_transaction()
            else:
                tx = start_transaction(tx=tx)

        lock("//tmp", tx=tx)

        another_tx = start_transaction()
        with pytest.raises(YtError):
            lock("//tmp", tx=another_tx)

    @authors("babenko")
    def test_transaction_depth(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        tx3 = start_transaction(tx=tx2)
        assert get("#{0}/@depth".format(tx1)) == 0
        assert get("#{0}/@depth".format(tx2)) == 1
        assert get("#{0}/@depth".format(tx3)) == 2

    @authors("babenko")
    def test_transaction_depth_limit(self):
        set("//sys/@config/transaction_manager/max_transaction_depth", 5)
        tx = None
        for _ in xrange(6):
            if tx is None:
                tx = start_transaction()
            else:
                tx = start_transaction(tx=tx)
        with pytest.raises(YtError):
            start_transaction(tx=tx)


class TestMasterTransactionsMulticell(TestMasterTransactions):
    NUM_SECONDARY_MASTER_CELLS = 3
    MASTER_CELL_ROLES = {"2": ["chunk_host"]}

    def _assert_native_content_revision_matches(self, path, tx="0-0-0-0"):
        content_revision = get(path + "/@content_revision", tx=tx)
        native_content_revision = get(path + "/@native_content_revision", tx=tx)
        assert content_revision == native_content_revision

    @authors("shakurov")
    @pytest.mark.parametrize("commit_order", [[1, 3, 2], [3, 1, 2], [3, 2, 1]])
    def test_native_content_revision(self, commit_order):
        create("table", "//tmp/t", attributes={"external_cell_tag": 2})
        self._assert_native_content_revision_matches("//tmp/t")

        write_table("//tmp/t", {"a": "b"})

        self._assert_native_content_revision_matches("//tmp/t")

        tx1 = start_transaction()
        write_table("<append=%true>//tmp/t", {"a": "b"}, tx=tx1)
        self._assert_native_content_revision_matches("//tmp/t")
        self._assert_native_content_revision_matches("//tmp/t", tx1)

        tx2 = start_transaction()
        write_table("<append=%true>//tmp/t", {"a": "b"}, tx=tx2)
        self._assert_native_content_revision_matches("//tmp/t")
        self._assert_native_content_revision_matches("//tmp/t", tx1)
        self._assert_native_content_revision_matches("//tmp/t", tx2)

        tx3 = start_transaction(tx=tx2)
        write_table("<append=%true>//tmp/t", {"a": "b"}, tx=tx3)
        self._assert_native_content_revision_matches("//tmp/t")
        self._assert_native_content_revision_matches("//tmp/t", tx1)
        self._assert_native_content_revision_matches("//tmp/t", tx2)
        self._assert_native_content_revision_matches("//tmp/t", tx3)

        def pick_tx(n):
            return {1: tx1, 2: tx2, 3: tx3}[n]

        committed_txs = __builtin__.set()

        def assert_on_commit():
            for tx in [tx for tx in ["0-0-0-0", tx1, tx2, tx3] if tx not in committed_txs]:
                self._assert_native_content_revision_matches("//tmp/t", tx)

        for tx_num in commit_order:
            tx_to_commit = pick_tx(tx_num)
            commit_transaction(tx_to_commit)
            committed_txs.add(tx_to_commit)
            assert_on_commit()

    @authors("shakurov")
    def test_native_content_revision_copy(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 3})

        create("table", "//tmp/t", attributes={"external_cell_tag": 2})
        self._assert_native_content_revision_matches("//tmp/t")

        copy("//tmp/t", "//tmp/t_copy")
        self._assert_native_content_revision_matches("//tmp/t_copy")
        copy("//tmp/t", "//tmp/p/t_copy")
        self._assert_native_content_revision_matches("//tmp/p/t_copy")

        tx = start_transaction()
        write_table("<append=%true>//tmp/t", {"a": "b"}, tx=tx)

        copy("//tmp/t", "//tmp/t_copy_tx", tx=tx)
        self._assert_native_content_revision_matches("//tmp/t_copy_tx", tx=tx)
        copy("//tmp/t", "//tmp/p/t_copy_tx", tx=tx)
        self._assert_native_content_revision_matches("//tmp/p/t_copy_tx", tx=tx)

        commit_transaction(tx)

        self._assert_native_content_revision_matches("//tmp/t_copy_tx")
        self._assert_native_content_revision_matches("//tmp/p/t_copy_tx")


class TestMasterTransactionsShardedTx(TestMasterTransactionsMulticell):
    NUM_SECONDARY_MASTER_CELLS = 5
    ENABLE_TMP_PORTAL = True
    MASTER_CELL_ROLES = {
        "0": ["cypress_node_host"],
        "1": ["cypress_node_host"],
        "2": ["chunk_host"],
        "3": ["cypress_node_host"],
        "4": ["transaction_coordinator"],
        "5": ["transaction_coordinator"],
    }

    @authors("shakurov")
    def test_prerequisite_transactions_on_commit2(self):
        # Currently there's no way to force particular transaction
        # coordinator cell (which is by design, BTW). So this test
        # will sometimes succeed trivially. But it definitely must
        # NOT be flaky!
        tx_a = start_transaction()
        tx_b = start_transaction()
        commit_transaction(tx_a, prerequisite_transaction_ids=[tx_b])

    def _create_portal_to_cell(self, cell_tag):
        portal_path = "//tmp/p{}".format(cell_tag)
        create("portal_entrance", portal_path, attributes={"exit_cell_tag": cell_tag})
        # Force the newly created portal to go into the resolve caches
        # (on the entrance side). This way, the following requests
        # (that may happen to be transactional) won't provoke tx
        # replication to the entrance cell.
        set(portal_path + "/@some_attr", "some_value")
        return portal_path

    def _replicate_tx_to_cell(self, tx, cell_tag, mode):
        portal_path = self._create_portal_to_cell(cell_tag)
        if mode == "r":
            get(portal_path + "/@id", tx=tx)
        elif mode == "w":
            create("table", portal_path + "/" + tx, tx=tx)
        elif mode == "rs":
            exists(portal_path + "/@qqq", prerequisite_transaction_ids=[tx])
        elif mode == "ws":
            create("table", portal_path + "/" + tx, prerequisite_transaction_ids=[tx])

    @authors("shakurov")
    @pytest.mark.parametrize("replication_mode", ["r", "w", "rs", "ws"])
    def test_lazy_tx_replication(self, replication_mode):
        tx = start_transaction()
        assert get("#" + tx + "/@replicated_to_cell_tags") == []

        self._replicate_tx_to_cell(tx, 3, replication_mode)
        assert get("#" + tx + "/@replicated_to_cell_tags") == [3]

    @authors("shakurov")
    def test_eager_tx_replication(self):
        set(
            "//sys/@config/transaction_manager/enable_lazy_transaction_replication",
            False,
        )
        multicell_sleep()

        tx = start_transaction()
        assert 3 in get("#" + tx + "/@replicated_to_cell_tags")

    @authors("shakurov")
    def test_parent_tx_replication(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        assert get("#" + tx1 + "/@replicated_to_cell_tags") == []

        self._replicate_tx_to_cell(tx2, 3, "r")

        assert get("#" + tx1 + "/@replicated_to_cell_tags") == [3]

    @authors("shakurov")
    @pytest.mark.parametrize("replication_mode", ["r", "w"])
    def test_tx_and_multiple_prerequisite_replication(self, replication_mode):
        tx1 = start_transaction()
        tx2 = start_transaction()
        tx3 = start_transaction()

        assert get("#" + tx1 + "/@replicated_to_cell_tags") == []
        assert get("#" + tx2 + "/@replicated_to_cell_tags") == []
        assert get("#" + tx3 + "/@replicated_to_cell_tags") == []

        portal_path = self._create_portal_to_cell(3)
        if replication_mode == "r":
            get(portal_path + "/@id", tx=tx1, prerequisite_transaction_ids=[tx2, tx3])
        else:
            assert replication_mode == "w"
            create(
                "table",
                portal_path + "/t",
                tx=tx1,
                prerequisite_transaction_ids=[tx2, tx3],
            )

    @authors("shakurov")
    def test_cannot_start_tx_with_conflicting_parent_and_prerequisite(self):
        tx1 = start_transaction()
        tx2 = start_transaction()
        tx1_cell_tag = get("#" + tx1 + "/@native_cell_tag")
        tx2_cell_tag = get("#" + tx2 + "/@native_cell_tag")

        # Sometimes this test will succeed trivially. But it must never flap.
        if tx1_cell_tag != tx2_cell_tag:
            with pytest.raises(YtError):
                start_transaction(tx=tx1, prerequisite_transaction_ids=[tx2])

    @authors("shakurov")
    def test_cannot_start_tx_with_conflicting_prerequisites(self):
        tx1 = start_transaction()
        tx2 = start_transaction()
        tx1_cell_tag = get("#" + tx1 + "/@native_cell_tag")
        tx2_cell_tag = get("#" + tx2 + "/@native_cell_tag")

        # Sometimes this test will succeed trivially. But it must never flap.
        if tx1_cell_tag != tx2_cell_tag:
            with pytest.raises(YtError):
                start_transaction(prerequisite_transaction_ids=[tx1, tx2])

    @authors("shakurov")
    def test_object_prerequisite_transactions(self):
        create_group("g")

        tx = start_transaction(timeout=60000)
        assert get("#" + tx + "/@replicated_to_cell_tags") == []

        add_member("root", "g", prerequisite_transaction_ids=[tx])

        assert "g" in get("//sys/users/root/@member_of")
        for cell_tag in range(self.NUM_SECONDARY_MASTER_CELLS + 1):
            assert "g" in get("//sys/users/root/@member_of", driver=get_driver(cell_tag))

        assert get("#" + tx + "/@replicated_to_cell_tags") == [0]

        remove_member("root", "g", prerequisite_transaction_ids=[tx])

        assert "g" not in get("//sys/users/root/@member_of")
        for cell_tag in range(self.NUM_SECONDARY_MASTER_CELLS + 1):
            assert "g" not in get("//sys/users/root/@member_of", driver=get_driver(cell_tag))

        assert get("#" + tx + "/@replicated_to_cell_tags") == [0]

    @authors("shakurov")
    def test_boomerang_mutation_portal_forwarding(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 3})

        assert not get("//tmp/p/@resolve_cached")

        tx = start_transaction()
        # Must succeed.
        create("map_node", "//tmp/p/d", tx=tx)


class TestMasterTransactionsShardedTxNoBoomerangs(TestMasterTransactionsShardedTx):
    def setup_method(self, method):
        super(TestMasterTransactionsShardedTxNoBoomerangs, self).setup_method(method)
        set("//sys/@config/object_service/enable_mutation_boomerangs", False)
        set("//sys/@config/chunk_service/enable_mutation_boomerangs", False)


class TestMasterTransactionsRpcProxy(TestMasterTransactions):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
