from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait, create, ls, get, set, copy, remove, exists, create_user,
    create_group, add_member, remove_member, start_transaction, abort_transaction,
    commit_transaction, ping_transaction, lock, write_file, write_table,
    get_transactions, get_topmost_transactions, gc_collect, get_driver,
    raises_yt_error, read_table)

from yt_sequoia_helpers import select_cypress_transaction_replicas

from yt.environment.helpers import assert_items_equal
from yt.common import datetime_to_string, YtError

import pytest
from flaky import flaky

import builtins
from datetime import datetime, timedelta
import decorator
from time import sleep


##################################################################


def with_portals_dir(func):
    def wrapper(func, self, *args, **kwargs):
        if not self.ENABLE_TMP_PORTAL:
            create("map_node", "//portals")
        try:
            return func(self, *args, **kwargs)
        finally:
            if not self.ENABLE_TMP_PORTAL:
                remove("//portals", recursive=True)

    return decorator.decorate(func, wrapper)


##################################################################


# NB: CheckInvariants() complexity is at least O(|Cypress nodes|) which is too
# slow in this case.
@pytest.mark.enabled_multidaemon
class TestMasterTransactionsWithoutInvariantChecking(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 3
    NUM_NODES = 0

    DELTA_MASTER_CONFIG = {
        "hydra_manager": {
            "invariants_check_probability": None,
        }
    }

    @authors("kvk1920")
    def test_abort_square(self):
        set("//sys/accounts/tmp/@resource_limits/node_count", 10000000)
        create("map_node", "//tmp/tree1")
        subtree = {str(i): {} for i in range(10000)}
        for i in range(20):
            set(f"//tmp/tree1/{i}", subtree, force=True)
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        with raises_yt_error("its descendants already have"):
            copy("//tmp/tree1", "//tmp/tree2", tx=tx2)
        abort_transaction(tx1)
        gc_collect()
        assert not exists(f"#{tx1}")


@pytest.mark.enabled_multidaemon
class TestMasterTransactions(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 3
    NUM_NODES = 3

    @authors("babenko", "gritukan")
    def test_simple1(self):
        tx = start_transaction()

        assert exists(f"//sys/transactions/{tx}")
        assert get(f"//sys/transactions/{tx}/@cypress_transaction")

        commit_transaction(tx)

        assert not exists(f"//sys/transactions/{tx}")

        # cannot commit committed transaction
        with raises_yt_error("No such transaction"):
            commit_transaction(tx)

    @authors("babenko")
    def test_simple2(self):
        tx = start_transaction()

        assert exists("//sys/transactions/" + tx)

        abort_transaction(tx)

        assert not exists("//sys/transactions/" + tx)

        # cannot commit aborted transaction
        with raises_yt_error("No such transaction"):
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

    @authors("h0pless")
    def test_transaction_method_whitelist(self):
        set("//sys/@config/transaction_manager/alert_transaction_is_not_compatible_with_method", False)
        set("//sys/@config/transaction_manager/transaction_type_to_method_whitelist/transaction", [])
        tx = start_transaction()
        with raises_yt_error("Method \"Set\" is not supported for type"):
            set("//tmp/value", "100", tx=tx)

        remove("//sys/@config/transaction_manager/transaction_type_to_method_whitelist")
        set("//tmp/value", "100", tx=tx)
        commit_transaction(tx)

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

    @authors("kvk1920")
    def test_timeout(self):
        tx1 = start_transaction(timeout=2000)
        tx2 = start_transaction(timeout=2000)

        # Check that transactions are still alive after 1 seconds.
        sleep(1.0)
        assert exists(f"//sys/transactions/{tx1}")
        assert exists(f"//sys/transactions/{tx2}")

        # Check that transactions are expired after 3 seconds
        sleep(3.0)
        assert not exists(f"//sys/transactions/{tx1}")
        assert not exists(f"//sys/transactions/{tx2}")

    @authors("kvk1920")
    def test_deadline(self):
        deadline = datetime_to_string(datetime.utcnow() + timedelta(seconds=2))
        tx1 = start_transaction(timeout=10000, deadline=deadline)
        tx2 = start_transaction(tx=tx1, timeout=10000, deadline=deadline)

        # Check that transactions are still alive after 1 seconds.
        sleep(1.0)
        assert exists(f"//sys/transactions/{tx1}")
        assert exists(f"//sys/transactions/{tx2}")

        # Check that transactions are expired after 3 seconds.
        sleep(3.0)
        assert not exists(f"//sys/transactions/{tx1}")
        assert not exists(f"//sys/transactions/{tx2}")

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

        sleep(3.5)
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
    @with_portals_dir
    def test_tx_multicell_attrs(self):
        tx = start_transaction(timeout=60000)
        tx_cell_tag = str(get("#" + tx + "/@native_cell_tag"))
        cell_tags = [tx_cell_tag]

        sharded_tx = self.NUM_SECONDARY_MASTER_CELLS > 2

        if sharded_tx:
            create("portal_entrance", "//portals/p", attributes={"exit_cell_tag": 13})

            # Populate resolve cache so that passing through the portal doesn't affect tx replication.
            set("//portals/p/@some_attr", "some_value")

            portal_exit_id = get("//portals/p/@id")
            table_id = create("table", "//portals/p/t", tx=tx)  # replicate tx to cell 3
            if "13" not in cell_tags:
                cell_tags.append("13")

        def check(r):
            assert_items_equal(list(r.keys()), cell_tags)
            for (k, v) in r.items():
                assert v == []

        check(get("#" + tx + "/@staged_object_ids"))
        check(get("#" + tx + "/@imported_object_ids"))
        check(get("#" + tx + "/@exported_objects"))
        assert get("#" + tx + "/@imported_object_count") == 0
        assert get("#" + tx + "/@exported_object_count") == 0

        if sharded_tx:
            branched_node_ids = get("#" + tx + "/@branched_node_ids")
            assert len(branched_node_ids) == 2
            assert_items_equal(branched_node_ids["13"], [table_id, portal_exit_id])
            assert branched_node_ids[tx_cell_tag] == []

            locked_node_ids = get("#" + tx + "/@locked_node_ids")
            assert len(locked_node_ids) == 2
            assert_items_equal(locked_node_ids["13"], [table_id, portal_exit_id])
            assert locked_node_ids[tx_cell_tag] == []

            staged_node_ids = get("#" + tx + "/@staged_node_ids")
            assert len(staged_node_ids) == 2
            assert_items_equal(staged_node_ids["13"], [table_id])
            assert staged_node_ids[tx_cell_tag] == []

            assert len(get("#" + tx + "/@lock_ids/13")) == 2

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
        write_file("//tmp/file", b"some_data")

        tx = start_transaction()

        lock("//tmp/file", mode="snapshot", tx=tx)
        remove("//tmp/file")
        abort_transaction(tx)

    @authors("babenko", "ignat")
    def test_commit_snapshot_lock(self):
        create("file", "//tmp/file")
        write_file("//tmp/file", b"some_data")

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

        with raises_yt_error("Unknown transaction cell tag"):
            commit_transaction(tx_b, prerequisite_transaction_ids=["a-b-c-d"])

        # Failing to commit a transaction with prerequisites provokes its abort.
        wait(lambda: not exists("//sys/transactions/" + tx_b), iter=100)

        tx_c = start_transaction()
        commit_transaction(tx_c, prerequisite_transaction_ids=[tx_a])

    @authors("babenko")
    def test_very_deep_transactions_yt_9961(self):
        tx = None
        for _ in range(10):
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
        for _ in range(6):
            if tx is None:
                tx = start_transaction()
            else:
                tx = start_transaction(tx=tx)
        with pytest.raises(YtError):
            start_transaction(tx=tx)

    @authors("shakurov")
    def test_zero_tx_id(self):
        assert not exists("//sys/transactions/0-0-0-0")
        assert not exists("//sys/transactions/0-0-0-0/@")
        with pytest.raises(YtError):
            get("//sys/transactions/0-0-0-0")
        with pytest.raises(YtError):
            get("//sys/transactions/0-0-0-0/@")

        # Zero guid is often treated as a null transaction ID and thus
        # should be handled as a special case.
        assert not exists("#0-0-0-0")
        assert not exists("#0-0-0-0/@")
        with pytest.raises(YtError):
            get("#0-0-0-0")
        with pytest.raises(YtError):
            get("#0-0-0-0/@")

    @authors("shakurov")
    def test_bad_tx_id(self):
        assert not exists("//sys/transactions/a-b-c-d")
        assert not exists("//sys/transactions/a-b-c-d/@")
        with pytest.raises(YtError):
            get("//sys/transactions/a-b-c-d")
            get("//sys/transactions/a-b-c-d/@")

        # Unlike zero guid above, any random guid is not a valid transaction (or,
        # indeed, any object) ID, and it's not correct to check for its existence.
        with pytest.raises(YtError):
            exists("#a-b-c-d")
        with pytest.raises(YtError):
            exists("#a-b-c-d/@")
        with pytest.raises(YtError):
            get("#a-b-c-d")
        with pytest.raises(YtError):
            get("#a-b-c-d/@")


@pytest.mark.enabled_multidaemon
class TestMasterTransactionsMulticell(TestMasterTransactions):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 3
    MASTER_CELL_DESCRIPTORS = {
        "12": {"roles": ["chunk_host"]},
    }

    def _assert_native_content_revision_matches(self, path, tx="0-0-0-0"):
        content_revision = get(path + "/@content_revision", tx=tx)
        native_content_revision = get(path + "/@native_content_revision", tx=tx)
        assert content_revision == native_content_revision

    @authors("shakurov")
    @pytest.mark.parametrize("commit_order", [[1, 3, 2], [3, 1, 2], [3, 2, 1]])
    def test_native_content_revision(self, commit_order):
        create("table", "//tmp/t", attributes={"external_cell_tag": 12})
        self._assert_native_content_revision_matches("//tmp/t")

        write_table("//tmp/t", {"a": "b"})

        self._assert_native_content_revision_matches("//tmp/t")

        tx1 = start_transaction(timeout=120000)
        write_table("<append=%true>//tmp/t", {"a": "b"}, tx=tx1)
        self._assert_native_content_revision_matches("//tmp/t")
        self._assert_native_content_revision_matches("//tmp/t", tx1)

        tx2 = start_transaction(timeout=120000)
        write_table("<append=%true>//tmp/t", {"a": "b"}, tx=tx2)
        self._assert_native_content_revision_matches("//tmp/t")
        self._assert_native_content_revision_matches("//tmp/t", tx1)
        self._assert_native_content_revision_matches("//tmp/t", tx2)

        tx3 = start_transaction(tx=tx2, timeout=120000)
        write_table("<append=%true>//tmp/t", {"a": "b"}, tx=tx3)
        self._assert_native_content_revision_matches("//tmp/t")
        self._assert_native_content_revision_matches("//tmp/t", tx1)
        self._assert_native_content_revision_matches("//tmp/t", tx2)
        self._assert_native_content_revision_matches("//tmp/t", tx3)

        def pick_tx(n):
            return {1: tx1, 2: tx2, 3: tx3}[n]

        committed_txs = builtins.set()

        def assert_on_commit():
            for tx in [tx for tx in ["0-0-0-0", tx1, tx2, tx3] if tx not in committed_txs]:
                self._assert_native_content_revision_matches("//tmp/t", tx)

        for tx_num in commit_order:
            tx_to_commit = pick_tx(tx_num)
            commit_transaction(tx_to_commit)
            committed_txs.add(tx_to_commit)
            assert_on_commit()

    @authors("shakurov")
    @with_portals_dir
    def test_native_content_revision_copy(self):
        create("portal_entrance", "//portals/p", attributes={"exit_cell_tag": 13})

        create("table", "//tmp/t", attributes={"external_cell_tag": 12})
        self._assert_native_content_revision_matches("//tmp/t")

        copy("//tmp/t", "//tmp/t_copy")
        self._assert_native_content_revision_matches("//tmp/t_copy")
        copy("//tmp/t", "//portals/p/t_copy")
        self._assert_native_content_revision_matches("//portals/p/t_copy")

        tx = start_transaction()
        write_table("<append=%true>//tmp/t", {"a": "b"}, tx=tx)

        copy("//tmp/t", "//tmp/t_copy_tx", tx=tx)
        self._assert_native_content_revision_matches("//tmp/t_copy_tx", tx=tx)
        copy("//tmp/t", "//portals/p/t_copy_tx", tx=tx)
        self._assert_native_content_revision_matches("//portals/p/t_copy_tx", tx=tx)

        commit_transaction(tx)

        self._assert_native_content_revision_matches("//tmp/t_copy_tx")
        self._assert_native_content_revision_matches("//portals/p/t_copy_tx")


@pytest.mark.enabled_multidaemon
class TestMasterTransactionsShardedTx(TestMasterTransactionsMulticell):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 5
    NUM_TEST_PARTITIONS = 4
    ENABLE_TMP_PORTAL = True
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["cypress_node_host"]},
        "12": {"roles": ["chunk_host"]},
        "13": {"roles": ["cypress_node_host"]},
        "14": {"roles": ["transaction_coordinator"]},
        "15": {"roles": ["transaction_coordinator"]},
    }

    @authors("shakurov")
    def test_prerequisite_transactions_on_commit2(self):
        # Currently there's no way to force particular transaction
        # coordinator cell (which is by design, BTW). So this test
        # will sometimes succeed trivially. But it definitely must
        # NOT be flaky!
        for _ in range(4):
            tx_a = start_transaction()
            tx_b = start_transaction()
            commit_transaction(tx_a, prerequisite_transaction_ids=[tx_b])

    def _create_portal_to_cell(self, cell_tag):
        portal_path = "//portals/p{}".format(cell_tag)
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

        self._replicate_tx_to_cell(tx, 13, replication_mode)
        assert get("#" + tx + "/@replicated_to_cell_tags") == [13]

    @authors("shakurov")
    def test_parent_tx_replication(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        assert get("#" + tx1 + "/@replicated_to_cell_tags") == []

        self._replicate_tx_to_cell(tx2, 13, "r")

        # TODO(kvk1920): fix mirrored Cypress tx replication.
        wait(lambda: get("#" + tx1 + "/@replicated_to_cell_tags") == [13])

    @authors("shakurov")
    @pytest.mark.parametrize("replication_mode", ["r", "w"])
    def test_tx_and_multiple_prerequisite_replication(self, replication_mode):
        tx1 = start_transaction()
        tx2 = start_transaction()
        tx3 = start_transaction()

        assert get("#" + tx1 + "/@replicated_to_cell_tags") == []
        assert get("#" + tx2 + "/@replicated_to_cell_tags") == []
        assert get("#" + tx3 + "/@replicated_to_cell_tags") == []

        portal_path = self._create_portal_to_cell(13)
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
        get("//sys/@config/multicell_manager/cell_descriptors")
        create_group("g")

        tx = start_transaction(timeout=60000)
        assert get("#" + tx + "/@replicated_to_cell_tags") == []

        add_member("root", "g", prerequisite_transaction_ids=[tx])

        assert "g" in get("//sys/users/root/@member_of")
        for cell_tag in range(self.NUM_SECONDARY_MASTER_CELLS + 1):
            assert "g" in get("//sys/users/root/@member_of", driver=get_driver(cell_tag))

        assert get("#" + tx + "/@replicated_to_cell_tags") == [10]

        remove_member("root", "g", prerequisite_transaction_ids=[tx])

        assert "g" not in get("//sys/users/root/@member_of")
        for cell_tag in range(self.NUM_SECONDARY_MASTER_CELLS + 1):
            assert "g" not in get("//sys/users/root/@member_of", driver=get_driver(cell_tag))

        assert get("#" + tx + "/@replicated_to_cell_tags") == [10]

    @authors("shakurov")
    def test_boomerang_mutation_portal_forwarding(self):
        create("portal_entrance", "//portals/p", attributes={"exit_cell_tag": 13})

        assert not get("//portals/p/@resolve_cached")

        tx = start_transaction()
        # Must succeed.
        create("map_node", "//portals/p/d", tx=tx)

    @authors("shakurov")
    def test_ex_transaction_coordinator_cell_role(self):
        tx = start_transaction()
        tx_cell_tag = get(f"#{tx}/@native_cell_tag")

        set(f"//sys/@config/multicell_manager/cell_descriptors/{tx_cell_tag}/roles",
            ["ex_transaction_coordinator"])

        create("table", "//tmp/qqq", tx=tx)
        assert exists("//tmp/qqq", tx=tx)

        commit_transaction(tx)

        assert exists("//tmp/qqq")

    @authors("h0pless")
    def test_foreign_transaction_map(self):
        with pytest.raises(YtError, match="Error parsing GUID"):
            get("//sys/foreign_transactions/1---4")

        tx = start_transaction()
        tx_cell_tag = get(f"#{tx}/@native_cell_tag")
        assert tx not in ls("//sys/foreign_transactions", driver=get_driver(3))
        assert tx not in ls("//sys/foreign_transactions", driver=get_driver(tx_cell_tag - 10))

        self._replicate_tx_to_cell(tx, 13, "r")
        wait(lambda: tx in ls("//sys/foreign_transactions", driver=get_driver(3)))

        commit_transaction(tx)
        wait(lambda: tx not in ls("//sys/foreign_transactions", driver=get_driver(3)))


@pytest.mark.enabled_multidaemon
class TestMasterTransactionsCTxS(TestMasterTransactionsShardedTx):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    DELTA_RPC_PROXY_CONFIG = {
        "cluster_connection": {
            "transaction_manager": {
                "use_cypress_transaction_service": True,
            }
        }
    }


@pytest.mark.enabled_multidaemon
class TestMasterTransactionsMirroredTx(TestMasterTransactionsCTxS):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = False
    NUM_CYPRESS_PROXIES = 1
    NUM_TEST_PARTITIONS = 6

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "commit_operation_cypress_node_changes_via_system_transaction": True,
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "transaction_manager": {
            "forbid_transaction_actions_for_cypress_transactions": True,
        }
    }


@pytest.mark.enabled_multidaemon
class TestMasterTransactionsRpcProxy(TestMasterTransactions):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


##################################################################

@pytest.mark.enabled_multidaemon
class TestSequoiaCypressTransactionReplication(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    ENABLE_TMP_ROOTSTOCK = False
    ENABLE_TMP_PORTAL = False
    NUM_MASTERS = 3

    NUM_SECONDARY_MASTER_CELLS = 2
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["cypress_node_host", "transaction_coordinator"]},
        "12": {"roles": ["cypress_node_host", "chunk_host"]},
    }

    @authors("kvk1920")
    def test_replication_via_hive(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})

        # Create table on coordinator to force tx replication instead of
        # externalization.
        create("table", "//tmp/p/t", attributes={"external_cell_tag": 12})
        content = [{"key": "a", "value": 1}, {"key": "b", "value": 2}]
        write_table("//tmp/p/t", content)

        tx1 = start_transaction()
        lock("//tmp/p/t", mode="exclusive", tx=tx1)

        tx2 = start_transaction()
        lock_id = lock("//tmp/p/t", mode="exclusive", tx=tx2, waitable=True)["lock_id"]
        assert get(f"#{lock_id}/@state") == "pending"

        # Tx should be externalized instead of replication.
        assert 12 in get(f"#{tx1}/@externalized_to_cell_tags")
        assert not get(f"#{tx1}/@replicated_to_cell_tags")

        # This replcia shouldn't be in Sequoia table since it's materialized via
        # Hive instead of 2PC.
        if self.ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA:
            assert 12 not in select_cypress_transaction_replicas(tx1)

        abort_transaction(tx1)

        assert get(f"#{lock_id}/@state") == "acquired"

        # tx2 should be replicated via Hive.
        assert 12 in get(f"#{tx2}/@externalized_to_cell_tags")
        assert get(f"#{tx2}/@replicated_to_cell_tags") == []
        if self.ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA:
            assert 12 not in select_cypress_transaction_replicas(tx2)

        # Shouldn't crash.
        gc_collect()

    @authors("kvk1920")
    def test_transaction_resource_usage(self):
        create("portal_entrance", "//tmp/p11", attributes={"exit_cell_tag": 11})
        create("portal_entrance", "//tmp/p12", attributes={"exit_cell_tag": 12})

        tx = start_transaction()

        create("map_node", "//tmp/p12/m", tx=tx)

        # //tmp/p12 (branched) + //tmp/p12/m (created)
        assert get(f"#{tx}/@resource_usage")["tmp"]["node_count"] == 2
        assert get(f"#{tx}/@multicell_resource_usage")["12"]["tmp"]["node_count"] == 2

        create("table", "//tmp/p11/t", attributes={"external_cell_tag": 12}, tx=tx)

        # + created table on native cell 11 + created table on external cell 12
        assert get(f"#{tx}/@resource_usage")["tmp"]["node_count"] == 4
        assert get(f"#{tx}/@multicell_resource_usage")["11"]["tmp"]["node_count"] == 1
        assert get(f"#{tx}/@multicell_resource_usage")["12"]["tmp"]["node_count"] == 3

        commit_transaction(tx)

        gc_collect()

    @authors("kvk1920")
    def test_replication_and_externalization_conflict(self):
        create("portal_entrance", "//tmp/p11", attributes={"exit_cell_tag": 11})
        create("portal_entrance", "//tmp/p12", attributes={"exit_cell_tag": 12})

        create("table", "//tmp/p11/t", attributes={"external_cell_tag": 12})
        content = [{"key": "a", "value": 1}, {"key": "b", "value": 2}]
        write_table("//tmp/p11/t", content)

        create("map_node", "//tmp/p12/m")

        tx1 = start_transaction()
        lock("//tmp/p12/m", mode="exclusive", tx=tx1)
        lock("//tmp/p11/t", mode="exclusive", tx=tx1)

        tx2 = start_transaction()

        lock_id_m = lock("//tmp/p12/m", mode="exclusive", tx=tx2, waitable=True)["lock_id"]
        assert get(f"#{lock_id_m}/@state") == "pending"

        assert 12 not in get(f"#{tx2}/@externalized_to_cell_tags")

        lock_id_t = lock("//tmp/p11/t", mode="exclusive", tx=tx2, waitable=True)["lock_id"]
        assert get(f"#{lock_id_t}/@state") == "pending"

        if self.ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA:
            assert 12 in select_cypress_transaction_replicas(tx1)
        assert 12 in get(f"#{tx1}/@replicated_to_cell_tags")
        assert 12 in get(f"#{tx1}/@externalized_to_cell_tags")

        if self.ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA:
            assert 12 in select_cypress_transaction_replicas(tx2)
        assert 12 in get(f"#{tx2}/@replicated_to_cell_tags")
        assert 12 in get(f"#{tx2}/@externalized_to_cell_tags")

        abort_transaction(tx1)
        assert get(f"#{lock_id_t}/@state") == "acquired"
        assert get(f"#{lock_id_m}/@state") == "acquired"

        abort_transaction(tx2)

        # Shouldn't crash.
        gc_collect()

    @authors("kvk1920")
    def test_cross_cell_table_copy(self):
        # The main purpose of this test is to check if
        # "enable_native_tx_externalization" is properly delivered to tx
        # participant via Sequoia transactions.

        create("portal_entrance", "//tmp/portal", attributes={"exit_cell_tag": 11})
        create("table", "//tmp/portal/t")

        assert get("//tmp/portal/t/@external_cell_tag") == 12

        tx = start_transaction()

        content = [{"key": 0, "value": "a"}, {"key": 1, "value": "b"}]

        write_table("//tmp/portal/t", content, tx=tx)

        copy("//tmp/portal/t", "//tmp/t", tx=tx)
        assert exists("//tmp/t", tx=tx)
        assert read_table("//tmp/t", tx=tx) == content

        commit_transaction(tx)
        assert exists("//tmp/t")
        assert read_table("//tmp/t") == content


@pytest.mark.enabled_multidaemon
class TestSequoiaCypressTransactionReplicationMirroredTx(TestSequoiaCypressTransactionReplication):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True

    # COMPAT(kvk1920): Remove when `use_cypress_transaction_service` become `true` by default.
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    DELTA_RPC_PROXY_CONFIG = {
        "cluster_connection": {
            "transaction_manager": {
                "use_cypress_transaction_service": True,
            },
        },
    }
