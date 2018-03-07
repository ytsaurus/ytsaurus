import pytest

from flaky import flaky

from yt_env_setup import YTEnvSetup
from yt_commands import *
from yt.environment.helpers import assert_items_equal
from time import sleep


##################################################################

class TestMasterTransactions(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3

    def test_simple1(self):
        tx = start_transaction()

        assert exists("//sys/transactions/" + tx)

        commit_transaction(tx)

        assert not exists("//sys/transactions/" + tx)

        #cannot commit committed transaction
        with pytest.raises(YtError): commit_transaction(tx)

    def test_simple2(self):
        tx = start_transaction()

        assert exists("//sys/transactions/" + tx)

        abort_transaction(tx)

        assert not exists("//sys/transactions/" + tx)

        #cannot commit aborted transaction
        with pytest.raises(YtError): commit_transaction(tx)

    def test_changes_inside_tx(self):
        set("//tmp/value", "42")

        tx = start_transaction()
        set("//tmp/value", "100", tx = tx)

        # check that changes are not seen outside of transaction
        assert get("//tmp/value", tx = tx) == "100"
        assert get("//tmp/value") == "42"

        commit_transaction(tx)
        # changes after commit are applied
        assert get("//tmp/value") == "100"

        tx = start_transaction()
        set("//tmp/value", "100500", tx = tx)
        abort_transaction(tx)

        #changes after abort are not applied
        assert get("//tmp/value") == "100"

        remove("//tmp/value")

    def test_nested_tx1(self):
        set("//tmp/t1", 0)

        tx_outer = start_transaction()

        tx1 = start_transaction(tx = tx_outer)
        set("//tmp/t1", 1, tx=tx1)

        start_transaction(tx = tx_outer)

        assert get("//tmp/t1", tx=tx_outer) == 0

        commit_transaction(tx1)
        assert get("//tmp/t1", tx=tx_outer) == 1
        assert get("//tmp/t1") == 0

    def test_nested_tx2(self):
        set("//tmp/t", 0)
        set("//tmp/t1", 0)
        set("//tmp/t2", 0)

        tx_outer = start_transaction()
        set("//tmp/t", 1, tx=tx_outer)

        tx1 = start_transaction(tx = tx_outer)
        set("//tmp/t1", 1, tx=tx1)

        tx2 = start_transaction(tx = tx_outer)
        set("//tmp/t2", 1, tx=tx2)

        commit_transaction(tx_outer)

        gc_collect()

        assert not exists("//sys/transactions/" + tx_outer)
        assert not exists("//sys/transactions/" + tx1)
        assert not exists("//sys/transactions/" + tx2)

        assert get("//tmp/t") == 1
        assert get("//tmp/t1") == 0
        assert get("//tmp/t2") == 0

    def test_nested_tx3(self):
        set("//tmp/t", 0)
        set("//tmp/t1", 0)
        set("//tmp/t2", 0)

        tx_outer = start_transaction()
        set("//tmp/t", 1, tx=tx_outer)

        tx1 = start_transaction(tx = tx_outer)
        set("//tmp/t1", 1, tx=tx1)

        tx2 = start_transaction(tx = tx_outer)
        set("//tmp/t2", 1, tx=tx2)

        abort_transaction(tx_outer)

        gc_collect()

        assert not exists("//sys/transactions/" + tx_outer)
        assert not exists("//sys/transactions/" + tx1)
        assert not exists("//sys/transactions/" + tx2)

        assert get("//tmp/t") == 0
        assert get("//tmp/t1") == 0
        assert get("//tmp/t2") == 0

    @flaky(max_runs=5)
    def test_timeout(self):
        tx = start_transaction(timeout=2000)

        # check that transaction is still alive after 1 seconds
        sleep(1.0)
        assert exists("//sys/transactions/" + tx)

        # check that transaction is expired after 3 seconds
        sleep(3.0)
        assert not exists("//sys/transactions/" + tx)

    @flaky(max_runs=5)
    def test_ping(self):
        tx = start_transaction(timeout=2000)

        sleep(1)
        assert exists("//sys/transactions/" + tx)
        ping_transaction(tx)

        sleep(1.5)
        assert exists("//sys/transactions/" + tx)

    @flaky(max_runs=5)
    def test_expire_outer(self):
        tx_outer = start_transaction(timeout=2000)
        tx_inner = start_transaction(tx = tx_outer)

        sleep(1)
        assert exists("//sys/transactions/" + tx_inner)
        assert exists("//sys/transactions/" + tx_outer)
        ping_transaction(tx_inner)

        sleep(1.5)
        # check that outer tx expired (and therefore inner was aborted)
        assert not exists("//sys/transactions/" + tx_inner)
        assert not exists("//sys/transactions/" + tx_outer)

    @flaky(max_runs=5)
    def test_ping_ancestors(self):
        tx_outer = start_transaction(timeout=2000)
        tx_inner = start_transaction(tx = tx_outer)

        sleep(1)
        assert exists("//sys/transactions/" + tx_inner)
        assert exists("//sys/transactions/" + tx_outer)
        ping_transaction(tx_inner, ping_ancestor_txs=True)

        sleep(1)
        # check that all tx are still alive
        assert exists("//sys/transactions/" + tx_inner)
        assert exists("//sys/transactions/" + tx_outer)

    def test_tx_multicell_attrs(self):
        tx = start_transaction()
        cell_tags = [str(x) for x in get("//sys/@registered_master_cell_tags")] + \
                    [str(get("//sys/@cell_tag"))]
        def check(r):
            assert_items_equal(r.keys(), cell_tags)
            for (k, v) in r.iteritems():
                assert v == []
        check(get("#" + tx + "/@staged_object_ids"))
        check(get("#" + tx + "/@imported_object_ids"))
        check(get("#" + tx + "/@exported_objects"))
        assert get("#" + tx + "/@imported_object_count") == 0
        assert get("#" + tx + "/@exported_object_count") == 0

    def test_transaction_maps(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx = tx1)
        tx3 = start_transaction(tx = tx1)

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
        assert not(tx1 in txs)
        assert not (tx2 in txs)
        assert not (tx3 in txs)
        topmost_txs = get_topmost_transactions()
        assert not (tx1 in topmost_txs)
        assert not (tx2 in topmost_txs)
        assert not (tx3 in topmost_txs)

    def test_revision1(self):
        set("//tmp/a", "b")
        r1 = get("//tmp/a/@revision")

        set("//tmp/a2", "b2")
        r2 = get("//tmp/a/@revision")
        assert r2 == r1

    def test_revision2(self):
        r1 = get("//tmp/@revision")

        set("//tmp/a", "b")
        r2 = get("//tmp/@revision")
        assert r2 > r1

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

    def test_revision4(self):
        r1 = get("//sys/@current_commit_revision")
        set("//tmp/t", 1)
        r2 = get("//tmp/t/@revision")
        assert r1 <= r2
        remove("//tmp/t")
        r3 = get("//sys/@current_commit_revision")
        assert r2 < r3

    def test_abort_snapshot_lock(self):
        create("file", "//tmp/file")
        write_file("//tmp/file", "some_data")

        tx = start_transaction()

        lock("//tmp/file", mode="snapshot", tx=tx)
        remove("//tmp/file")
        abort_transaction(tx)

    def test_commit_snapshot_lock(self):
        create("file", "//tmp/file")
        write_file("//tmp/file", "some_data")

        tx = start_transaction()

        lock("//tmp/file", mode="snapshot", tx=tx)
        remove("//tmp/file")
        commit_transaction(tx)

    def test_title(self):
        tx = start_transaction(attributes={"title": "My title"})
        assert get("#{0}/@title".format(tx)) == "My title"

    def test_custom_attr(self):
        tx = start_transaction(attributes={"myattr": "myvalue"})
        assert get("#{0}/@myattr".format(tx)) == "myvalue"

    def test_update_attr(self):
        tx = start_transaction()
        set("#{0}/@myattr".format(tx), "myvalue")
        assert get("#{0}/@myattr".format(tx)) == "myvalue"

    def test_owner(self):
        create_user("u")
        tx = start_transaction(authenticated_user = "u")
        assert get("#{0}/@owner".format(tx)) == "u"

##################################################################

class TestMasterTransactionsMulticell(TestMasterTransactions):
    NUM_SECONDARY_MASTER_CELLS = 2
