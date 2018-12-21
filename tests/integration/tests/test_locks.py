import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import __builtin__

##################################################################

class TestLocks(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3

    def test_invalid_cases(self):
        # outside of transaction
        with pytest.raises(YtError): lock("/")

        # at non-existsing node
        tx = start_transaction()
        with pytest.raises(YtError): lock("//tmp/non_existent", tx=tx)

        # error while parsing mode
        with pytest.raises(YtError): lock("/", mode="invalid", tx=tx)

        # taking None lock is forbidden
        with pytest.raises(YtError): lock("/", mode="None", tx=tx)

        # attributes do not have @lock_mode
        set("//tmp/value", "<attr=some>42", is_raw=True, tx=tx)
        with pytest.raises(YtError): lock("//tmp/value/@attr/@lock_mode", tx=tx)

        abort_transaction(tx)

    def test_lock_mode(self):
        tx = start_transaction()
        set("//tmp/map", "{list=<attr=some>[1;2;3]}", is_raw=True, tx=tx)

        # check that lock is set on nested nodes
        assert get("//tmp/map/@lock_mode", tx = tx) == "exclusive"
        assert get("//tmp/map/list/@lock_mode", tx = tx) == "exclusive"
        assert get("//tmp/map/list/0/@lock_mode", tx = tx) == "exclusive"

        abort_transaction(tx)

    # Uncomment when new response format will be supported in api/v4.
    #def test_lock_full_response(self):
    #    create("map_node", "//tmp/node")

    #    tx = start_transaction()
    #    rsp = lock("//tmp/node", mode="snapshot", tx=tx, full_response=True)
    #    assert rsp.keys() == ["lock_id", "node_id"]
    #    assert rsp["lock_id"] == get("//tmp/node/@locks/0/id")

    #    set("//tmp/node", {})
    #    assert rsp["node_id"] == get("//tmp/node/@id", tx=tx)

    def test_shared_lock_inside_tx(self):
        tx_outer = start_transaction()
        create("table", "//tmp/table", tx=tx_outer)

        tx_inner = start_transaction(tx=tx_outer)
        lock("//tmp/table", mode="shared", tx=tx_inner)

    def test_snapshot_lock(self):
        set("//tmp/node", 42)

        tx = start_transaction()
        lock("//tmp/node", mode = "snapshot", tx = tx)

        set("//tmp/node", 100)
        # check that node under snapshot lock wasn't changed
        assert get("//tmp/node", tx = tx) == 42

        # can't change value under snapshot lock
        with pytest.raises(YtError): set("//tmp/node", 200, tx = tx)

        abort_transaction(tx)

    def _assert_locked(self, path, tx, mode, child_key=None, attribute_key=None):
        self._assert_locked_impl(path, tx, True)

        locks = get(path + "/@locks", tx=tx)
        lock = None
        for l in locks:
            if (l.get("transaction_id") == tx and
                (child_key is None or l.get("child_key") == child_key) and
                (attribute_key is None or l.get("attribute_key") == attribute_key)):
                lock = l
                break

        assert lock
        assert lock["mode"] == mode
        assert lock["state"] == "acquired"
        if child_key is not None:
            assert lock["child_key"] == child_key
        if attribute_key is not None:
            assert lock["attribute_key"] == attribute_key

        assert lock["id"] in get("#{0}/@lock_ids".format(tx))

    def _assert_not_locked(self, path, tx):
        self._assert_locked_impl(path, tx, False)

    def _assert_locked_impl(self, path, tx, assert_true):
        node_id = get(path + "/@id", tx=tx)
        locked_node_ids = get("#{0}/@locked_node_ids".format(tx))
        if assert_true:
            assert node_id in locked_node_ids
        else:
            assert node_id not in locked_node_ids

    @pytest.mark.parametrize("mode", ["snapshot", "exclusive", "shared_child", "shared_attribute"])
    def test_unlock_explicit(self, mode):
        create("map_node", "//tmp/m1")

        if mode == "shared_child":
            mode = "shared"
            kwargs1 = {"child_key": "x1"}
            kwargs2 = {"child_key": "x2"}
        elif mode == "shared_attribute":
            mode = "shared"
            kwargs1 = {"attribute_key": "x1"}
            kwargs2 = {"attribute_key": "x2"}
        else:
            kwargs1 = {}

        tx = start_transaction()
        lock_id = lock("//tmp/m1", mode=mode, tx=tx, **kwargs1)

        self._assert_locked("//tmp/m1", tx, mode, **kwargs1)

        tx2 = start_transaction()
        if mode != "snapshot":
            with pytest.raises(YtError): lock("//tmp/m1", mode=mode, tx=tx2, **kwargs1)
        if mode == "shared":
            lock("//tmp/m1", mode=mode, tx=tx2, **kwargs2)

        unlock("//tmp/m1", tx=tx)

        if mode == "shared":
            self._assert_locked("//tmp/m1", tx2, "shared", **kwargs2)

        self._assert_not_locked("//tmp/m1", tx)
        assert not exists("#{0}".format(lock_id))
        assert len(get("#{0}/@lock_ids".format(tx))) == 0

        lock_id = lock("//tmp/m1", mode=mode, tx=tx2, **kwargs1)
        self._assert_locked("//tmp/m1", tx2, mode, **kwargs1)

        commit_transaction(tx) # mustn't crash

    def test_unlock_implicit_noop(self):
        create("map_node", "//tmp/m1")

        tx = start_transaction()
        set("//tmp/m1/c1", "child_value", tx=tx)
        set("//tmp/m1/@a1", "attribute_value", tx=tx)

        self._assert_locked("//tmp/m1", tx, "shared", child_key="c1")
        self._assert_locked("//tmp/m1", tx, "shared", attribute_key="a1")
        self._assert_locked("//tmp/m1/c1", tx, "exclusive")

        unlock("//tmp/m1", tx=tx)
        unlock("//tmp/m1/c1", tx=tx)

        self._assert_locked("//tmp/m1", tx, "shared", child_key="c1")
        self._assert_locked("//tmp/m1", tx, "shared", attribute_key="a1")
        self._assert_locked("//tmp/m1/c1", tx, "exclusive")

        commit_transaction(tx) # mustn't crash

    @pytest.mark.parametrize("mode", ["exclusive", "shared"])
    def test_unlock_modified(self, mode):
        create("map_node", "//tmp/m1")

        tx = start_transaction()
        lock("//tmp/m1", mode=mode, tx=tx)

        self._assert_locked("//tmp/m1", tx, mode)

        set("//tmp/m1/c1", "child_value", tx=tx)

        with pytest.raises(YtError): unlock("//tmp/m1", tx=tx) # modified and can't be unlocked

    def test_unlock_not_locked(self):
        create("map_node", "//tmp/m1")

        tx = start_transaction()
        self._assert_not_locked("//tmp/m1", tx)
        unlock("//tmp/m1", tx=tx) # should be a quiet noop

    @pytest.mark.parametrize("mode", ["exclusive", "shared"])
    def test_unlock_promoted_lock(self, mode):
        create("map_node", "//tmp/m1")

        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        lock("//tmp/m1", mode=mode, tx=tx2)
        commit_transaction(tx2)

        self._assert_locked("//tmp/m1", tx1, mode)
        unlock("//tmp/m1", tx=tx1)
        self._assert_not_locked("//tmp/m1", tx1)

        commit_transaction(tx1) # mustn't crash

    @pytest.mark.parametrize("mode", ["exclusive", "shared", "snapshot"])
    def test_unlock_and_lock_again(self, mode):
        create("map_node", "//tmp/m1")
        tx = start_transaction()
        lock("//tmp/m1", mode=mode, tx=tx)
        self._assert_locked("//tmp/m1", tx, mode)
        unlock("//tmp/m1", tx=tx)
        self._assert_not_locked("//tmp/m1", tx)
        lock("//tmp/m1", mode=mode, tx=tx)
        self._assert_locked("//tmp/m1", tx, mode)
        commit_transaction(tx) # mustn't crash

    @pytest.mark.parametrize("mode", ["exclusive", "shared", "snapshot"])
    def test_unlock_and_lock_in_parent(self, mode):
        create("map_node", "//tmp/m1")
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        lock("//tmp/m1", mode=mode, tx=tx2)
        self._assert_locked("//tmp/m1", tx2, mode)
        unlock("//tmp/m1", tx=tx2)
        self._assert_not_locked("//tmp/m1", tx2)

        lock("//tmp/m1", mode=mode, tx=tx1)
        self._assert_locked("//tmp/m1", tx1, mode)

        commit_transaction(tx1) # mustn't crash

    @pytest.mark.parametrize("mode", ["exclusive", "shared", "snapshot"])
    def test_unlock_and_lock_in_child(self, mode):
        create("map_node", "//tmp/m1")
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        lock("//tmp/m1", mode=mode, tx=tx1)
        self._assert_locked("//tmp/m1", tx1, mode)
        unlock("//tmp/m1", tx=tx1)
        self._assert_not_locked("//tmp/m1", tx1)

        lock("//tmp/m1", mode=mode, tx=tx2)
        self._assert_locked("//tmp/m1", tx2, mode)

        commit_transaction(tx2) # mustn't crash
        commit_transaction(tx1)

    def test_unlock_pending(self):
        create("map_node", "//tmp/m1")
        tx1 = start_transaction()
        tx2 = start_transaction() # not nested

        acquired_lock_id = lock("//tmp/m1", mode="exclusive", tx=tx1)
        self._assert_locked("//tmp/m1", tx1, "exclusive")
        pending_lock_id = lock("//tmp/m1", tx=tx2, waitable=True)
        assert get("#" + pending_lock_id + "/@state") == "pending"

        unlock("//tmp/m1", tx=tx2)

        self._assert_locked("//tmp/m1", tx1, "exclusive")
        gc_collect()
        assert not exists("#" + pending_lock_id)

        commit_transaction(tx2) # mustn't crash
        commit_transaction(tx1)

    def test_unlock_promotes_pending(self):
        create("map_node", "//tmp/m1")
        tx1 = start_transaction()
        tx2 = start_transaction() # not nested

        acquired_lock_id = lock("//tmp/m1", mode="exclusive", tx=tx1)
        self._assert_locked("//tmp/m1", tx1, "exclusive")
        pending_lock_id = lock("//tmp/m1", tx=tx2, waitable=True)
        assert get("#" + pending_lock_id + "/@state") == "pending"

        assert get("#" + pending_lock_id + "/@transaction_id") == tx2

        unlock("//tmp/m1", tx=tx1)

        assert not exists("#" + acquired_lock_id)

        assert get("#" + pending_lock_id + "/@state") == "acquired"

        commit_transaction(tx2) # mustn't crash
        commit_transaction(tx1)

    def test_unlock_unreachable_node(self):
        create("map_node", "//tmp/m1")
        create("map_node", "//tmp/m1/m2")
        node_id =  create("map_node", "//tmp/m1/m2/m3")

        tx = start_transaction()
        acquired_lock_id = lock("//tmp/m1/m2/m3", mode="snapshot", tx=tx)

        remove("//tmp/m1")

        assert not exists("//tmp/m1/m2/m3")
        assert not exists("//tmp/m1/m2/m3", tx=tx)

        node_path = "#" + node_id
        assert exists(node_path)
        self._assert_locked(node_path, tx, "snapshot")
        unlock(node_path, tx=tx)

        gc_collect()

        assert not exists(node_path)

    @pytest.mark.parametrize("mode", ["shared", "snapshot"])
    def test_unlock_deeply_nested_node1(self, mode):
        create("map_node", "//tmp/m1")

        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        tx3 = start_transaction(tx=tx2)
        tx4 = start_transaction(tx=tx3)
        tx5 = start_transaction(tx=tx4)
        tx6 = start_transaction(tx=tx5)

        if mode == "shared":
            kwargs2 = {"child_key": "c2"}
            kwargs4 = {"child_key": "c4"}
            kwargs6 = {"child_key": "c6"}
        else:
            kwargs2 = {}
            kwargs4 = {}
            kwargs6 = {}

        lock("//tmp/m1", tx=tx2, mode=mode, **kwargs2)
        lock("//tmp/m1", tx=tx4, mode=mode, **kwargs4)
        lock("//tmp/m1", tx=tx6, mode=mode, **kwargs6)

        self._assert_locked("//tmp/m1", tx2, mode, **kwargs2)
        self._assert_locked("//tmp/m1", tx4, mode, **kwargs4)
        self._assert_locked("//tmp/m1", tx6, mode, **kwargs6)

        unlock("//tmp/m1", tx=tx4)
        self._assert_locked("//tmp/m1", tx2, mode, **kwargs2)
        self._assert_not_locked("//tmp/m1", tx4)
        self._assert_locked("//tmp/m1", tx6, mode, **kwargs6)

        unlock("//tmp/m1", tx=tx5) # not lock and does nothing
        self._assert_locked("//tmp/m1", tx2, mode, **kwargs2)
        self._assert_not_locked("//tmp/m1", tx4)
        self._assert_locked("//tmp/m1", tx6, mode, **kwargs6)

        unlock("//tmp/m1", tx=tx6)
        self._assert_locked("//tmp/m1", tx2, mode, **kwargs2)
        self._assert_not_locked("//tmp/m1", tx4)
        self._assert_not_locked("//tmp/m1", tx6)

        if mode != "snapshot":
            with pytest.raises(YtError): lock("//tmp/m1", tx=tx1, mode="exclusive")

        unlock("//tmp/m1", tx=tx2)
        self._assert_not_locked("//tmp/m1", tx2)
        self._assert_not_locked("//tmp/m1", tx4)
        self._assert_not_locked("//tmp/m1", tx6)

        lock("//tmp/m1", tx=tx1, mode="exclusive")
        self._assert_locked("//tmp/m1", tx1, "exclusive")

        for tx in [tx6, tx5, tx4, tx3, tx2, tx1]:
            commit_transaction(tx) # mustn't throw

    @pytest.mark.parametrize("transactions_to_skip", [{1, 3, 5}, {1, 3}, {1, 5}, {3, 5}, {1}, {3}, {5}, __builtin__.set()])
    @pytest.mark.parametrize("modes", [["exclusive", "shared", "snapshot"], ["shared", "exclusive", "shared"], ["exclusive", "shared", "shared"], ["shared", "shared", "exclusive"]])
    def test_unlock_deeply_nested_node2(self, modes, transactions_to_skip):
        create("map_node", "//tmp/m1")

        prev_tx = None
        transactions = [None] * 6
        for i in xrange(0, 6):
            if i+1 in transactions_to_skip:
                transactions[i] = None
            else:
                if prev_tx is None:
                    prev_tx = start_transaction()
                else :
                    prev_tx = start_transaction(tx=prev_tx)
                transactions[i] = prev_tx

        tx1, tx2, tx3, tx4, tx5, tx6 = transactions

        mode2, mode4, mode6 = modes

        if mode2 == "shared":
            kwargs2 = {"child_key": "c2"}
        else:
            kwargs2 = {}

        if mode4 == "shared":
            kwargs4 = {"child_key": "c4"}
        else:
            kwargs4 = {}

        if mode6 == "shared":
            kwargs6 = {"child_key": "c6"}
        else:
            kwargs6 = {}

        lock("//tmp/m1", tx=tx2, mode=mode2, **kwargs2)
        lock("//tmp/m1", tx=tx4, mode=mode4, **kwargs4)
        lock("//tmp/m1", tx=tx6, mode=mode6, **kwargs6)

        self._assert_locked("//tmp/m1", tx2, mode2, **kwargs2)
        self._assert_locked("//tmp/m1", tx4, mode4, **kwargs4)
        self._assert_locked("//tmp/m1", tx6, mode6, **kwargs6)

        unlock("//tmp/m1", tx=tx4)
        self._assert_locked("//tmp/m1", tx2, mode2, **kwargs2)
        self._assert_not_locked("//tmp/m1", tx4)
        self._assert_locked("//tmp/m1", tx6, mode6, **kwargs6)

        if tx5 is not None:
            unlock("//tmp/m1", tx=tx5) # not lock and does nothing
            self._assert_locked("//tmp/m1", tx2, mode2, **kwargs2)
            self._assert_not_locked("//tmp/m1", tx4)
            self._assert_locked("//tmp/m1", tx6, mode6, **kwargs6)

        unlock("//tmp/m1", tx=tx6)
        self._assert_locked("//tmp/m1", tx2, mode2, **kwargs2)
        self._assert_not_locked("//tmp/m1", tx4)
        self._assert_not_locked("//tmp/m1", tx6)

        if tx1 is not None:
            if mode2 == "snapshot":
                with pytest.raises(YtError): lock("//tmp/m1", tx=tx1, mode="shared", child_key="c1")
            else:
                with pytest.raises(YtError): lock("//tmp/m1", tx=tx1, mode="exclusive")

        unlock("//tmp/m1", tx=tx2)
        self._assert_not_locked("//tmp/m1", tx2)
        self._assert_not_locked("//tmp/m1", tx4)
        self._assert_not_locked("//tmp/m1", tx6)

        if tx1 is not None:
            lock("//tmp/m1", tx=tx1, mode="exclusive")
            self._assert_locked("//tmp/m1", tx1, "exclusive")

        for tx in reversed(transactions):
            if tx is not None:
                commit_transaction(tx) # mustn't throw

    def test_unlock_without_transaction(self):
        create("map_node", "//tmp/m1")
        tx = start_transaction()
        lock("//tmp/m1", tx=tx, mode="exclusive")
        assert get("//tmp/m1/@lock_mode", tx=tx) == "exclusive"
        with pytest.raises(YtError): unlock("//tmp/m1")

    def test_remove_map_subtree_lock(self):
        set("//tmp/a", {"b" : 1})
        tx = start_transaction()
        lock("//tmp/a/b", mode = "exclusive", tx = tx);
        with pytest.raises(YtError): remove("//tmp/a")

    def test_remove_list_subtree_lock(self):
        set("//tmp/a", [1])
        tx = start_transaction()
        lock("//tmp/a/0", mode = "exclusive", tx = tx);
        with pytest.raises(YtError): remove("//tmp/a")

    def test_exclusive_vs_snapshot_locks1(self):
        create("table", "//tmp/t")
        tx1 = start_transaction()
        tx2 = start_transaction()
        lock("//tmp/t", mode = "snapshot", tx = tx1)
        lock("//tmp/t", mode = "exclusive", tx = tx2)

    def test_exclusive_vs_snapshot_locks2(self):
        create("table", "//tmp/t")
        tx1 = start_transaction()
        tx2 = start_transaction()
        lock("//tmp/t", mode = "exclusive", tx = tx2)
        lock("//tmp/t", mode = "snapshot", tx = tx1)

    def test_node_locks(self):
        set("//tmp/a", 1)

        tx = start_transaction()
        lock_id = lock("//tmp/a", tx=tx)
        locks = get("//tmp/a/@locks")
        assert len(locks) == 1
        assert locks[0]["state"] == "acquired"
        assert locks[0]["transaction_id"] == tx
        assert locks[0]["mode"] == "exclusive"
        assert get("#" + lock_id +"/@state") == "acquired"

        abort_transaction(tx)
        assert get("//tmp/a/@locks") == []

    def test_lock_propagation_on_commit(self):
        set("//tmp/a", 1)

        tx1 = start_transaction()
        tx2 = start_transaction(tx = tx1)
        lock_id = lock("//tmp/a", tx = tx2)

        locks = get("//tmp/a/@locks")
        assert len(locks) == 1
        assert locks[0]["state"] == "acquired"
        assert locks[0]["transaction_id"] == tx2
        assert locks[0]["mode"] == "exclusive"
        assert get("#" + lock_id +"/@state") == "acquired"

        commit_transaction(tx2)

        locks = get("//tmp/a/@locks")
        assert len(locks) == 1
        assert locks[0]["state"] == "acquired"
        assert locks[0]["transaction_id"] == tx1
        assert locks[0]["mode"] == "exclusive"
        assert get("#" + lock_id +"/@state") == "acquired"

        commit_transaction(tx1)

        assert get('//tmp/a/@locks') == []

    def test_no_lock_propagation_on_abort(self):
        set("//tmp/a", 1)

        tx1 = start_transaction()
        tx2 = start_transaction(tx = tx1)
        lock_id = lock("//tmp/a", tx = tx2)

        locks = get("//tmp/a/@locks")
        assert len(locks) == 1
        assert locks[0]["state"] == "acquired"
        assert locks[0]["transaction_id"] == tx2
        assert locks[0]["mode"] == "exclusive"
        assert get("#" + lock_id +"/@state") == "acquired"

        abort_transaction(tx2)

        assert get("//tmp/a/@locks") == []

    def test_redundant_lock1(self):
        tx = start_transaction()
        set("//tmp/a", "x")

        set("//tmp/a", "b", tx=tx)
        assert len(get("//tmp/a/@locks")) == 1

        set("//tmp/a", "c", tx=tx)
        assert len(get("//tmp/a/@locks")) == 1

    def test_redundant_lock2(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        tx3 = start_transaction(tx=tx1)

        set("//tmp/a", "b", tx=tx2)
        assert len(get("//tmp/@locks")) == 1
        commit_transaction(tx2)
        assert len(get("//tmp/@locks")) == 1

        set("//tmp/a", "c", tx=tx3)
        assert len(get("//tmp/@locks")) == 1
        commit_transaction(tx3)
        assert len(get("//tmp/@locks")) == 1

    def test_redundant_lock3(self):
        create("table", "//tmp/t")
        tx = start_transaction()
        for i in xrange(5):
            write_table("//tmp/t", {"foo": "bar"}, tx = tx)
            assert len(get("//tmp/t/@locks")) == 1

    def test_waitable_lock1(self):
        set("//tmp/a", 1)

        tx1 = start_transaction()
        lock_id1 = lock("//tmp/a", tx=tx1)

        tx2 = start_transaction()
        lock_id2 = lock("//tmp/a", tx=tx2, waitable=True)

        assert get("#" + lock_id1 + "/@state") == "acquired"
        assert get("//tmp/a/@lock_mode", tx=tx1) == "exclusive"
        assert get("#" + lock_id2 + "/@state") == "pending"

        abort_transaction(tx1)

        assert not exists("//sys/locks/" + lock_id1)
        assert get("#" + lock_id2 + "/@state") == "acquired"
        assert get("//tmp/a/@lock_mode", tx=tx2) == "exclusive"

    def test_waitable_lock2(self):
        set("//tmp/a", 1)

        tx1 = start_transaction()
        lock_id1 = lock("//tmp/a", tx=tx1)
        assert get("#" + lock_id1 + "/@state") == "acquired"

        tx2 = start_transaction()
        lock_id2 = lock("//tmp/a", tx=tx2, waitable=True)
        assert get("#" + lock_id2 + "/@state") == "pending"

        tx3 = start_transaction()
        lock_id3 = lock("//tmp/a", tx=tx1, waitable=True)
        assert get("#" + lock_id3 + "/@state") == "acquired"
        assert get("#" + lock_id2 + "/@state") == "pending"

    def test_waitable_lock3(self):
        set("//tmp/a", 1)

        tx1 = start_transaction()
        lock_id1 = lock("//tmp/a", mode="shared", tx=tx1)
        assert get("#" + lock_id1 + "/@state") == "acquired"

        tx2 = start_transaction()
        lock_id2 = lock("//tmp/a", mode="shared", tx=tx2, waitable=True)
        assert get("#" + lock_id2 + "/@state") == "acquired"

    def test_waitable_lock4(self):
        set("//tmp/a", 1)

        tx1 = start_transaction()
        lock_id1 = lock("//tmp/a", tx=tx1, mode="shared")
        assert get("#" + lock_id1 + "/@state") == "acquired"

        tx2 = start_transaction()
        lock_id2 = lock("//tmp/a", tx=tx2, waitable=True)
        assert get("#" + lock_id2 + "/@state") == "pending"

        tx3 = start_transaction()
        lock_id3 = lock("//tmp/a", tx=tx3, mode="shared")
        assert get("#" + lock_id3 + "/@state") == "acquired"

    def test_waitable_lock5(self):
        set("//tmp/a", 1)

        tx1 = start_transaction()
        lock_id1 = lock("//tmp/a", tx=tx1)

        tx2 = start_transaction()
        lock_id2 = lock("//tmp/a", tx=tx2, waitable=True, mode="shared")

        tx3 = start_transaction()
        lock_id3 = lock("//tmp/a", tx=tx3, waitable=True, mode="shared")

        tx4 = start_transaction()
        lock_id4 = lock("//tmp/a", tx=tx4, waitable=True)

        assert get("#" + lock_id1 + "/@state") == "acquired"
        assert get("#" + lock_id2 + "/@state") == "pending"
        assert get("#" + lock_id3 + "/@state") == "pending"
        assert get("#" + lock_id4 + "/@state") == "pending"

        commit_transaction(tx1)
        with pytest.raises(YtError): get("#" + lock_id1 + "/@state")
        assert get("#" + lock_id2 + "/@state") == "acquired"
        assert get("#" + lock_id3 + "/@state") == "acquired"
        assert get("#" + lock_id4 + "/@state") == "pending"

        commit_transaction(tx2)
        assert not exists("//sys/locks/" + lock_id2)
        assert get("#" + lock_id3 + "/@state") == "acquired"
        assert get("#" + lock_id4 + "/@state") == "pending"

        commit_transaction(tx3)
        assert not exists("//sys/locks/" + lock_id3)
        assert get("#" + lock_id4 + "/@state") == "acquired"

        commit_transaction(tx4)
        assert not exists("//sys/locks/" + lock_id4)

    def test_waitable_lock6(self):
        set("//tmp/a", 1)

        tx1 = start_transaction()
        lock_id1 = lock("//tmp/a", tx=tx1)
        assert get("#" + lock_id1 + "/@state") == "acquired"

        remove("//tmp/a", tx=tx1)

        tx2 = start_transaction()
        lock_id2 = lock("//tmp/a", tx=tx2, waitable=True)
        assert get("#" + lock_id2 + "/@state") == "pending"

        commit_transaction(tx1)

        gc_collect() # the lock must become orphaned

        assert not exists("//sys/locks/" + lock_id1)
        assert not exists("//sys/locks/" + lock_id2)

    def test_waitable_lock7(self):
        set("//tmp/a", {"b" : 1 })

        tx1 = start_transaction()
        lock_id1 = lock("//tmp/a/b", tx=tx1)
        assert lock_id1 != "0-0-0-0"

        remove("//tmp/a", tx=tx1)

        tx2 = start_transaction()
        lock_id2 = lock("//tmp/a/b", tx=tx2, waitable=True)
        assert get("#" + lock_id2 + "/@state") == "pending"

        commit_transaction(tx1)

        gc_collect() # the lock must become orphaned

        assert not exists("//sys/locks/" + lock_id1)
        assert not exists("//sys/locks/" + lock_id2)

    def test_waitable_lock8(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx = tx1)
        tx3 = start_transaction()

        create("table", "//tmp/t")
        write_table("//tmp/t", {"foo": "bar"}, tx = tx2)

        lock_id = lock("//tmp/t", tx = tx3, mode = "exclusive", waitable = True)

        assert get("//sys/locks/" + lock_id + "/@state") == "pending"
        assert len(get("//tmp/t/@locks")) == 2

        commit_transaction(tx2)

        assert get("//sys/locks/" + lock_id + "/@state") == "pending"
        assert len(get("//tmp/t/@locks")) == 2

        commit_transaction(tx1)

        assert get("//sys/locks/" + lock_id + "/@state") == "acquired"
        assert len(get("//tmp/t/@locks")) == 1

    def test_waitable_lock9(self):
        tx1 = start_transaction()
        tx2 = start_transaction()
        tx3 = start_transaction()

        create("table", "//tmp/t")

        lock_id1 = lock("//tmp/t", tx = tx1, mode = "exclusive")
        assert get("//sys/locks/" + lock_id1 + "/@state") == "acquired"

        lock_id2 = lock("//tmp/t", tx = tx2, mode = "exclusive", waitable = True)
        assert get("//sys/locks/" + lock_id2 + "/@state") == "pending"

        lock_id3 = lock("//tmp/t", tx = tx3, mode = "snapshot")
        assert get("//sys/locks/" + lock_id3 + "/@state") == "acquired"

    def test_waitable_lock10(self):
        create("table", "//tmp/t")
        tx1 = start_transaction()
        tx2 = start_transaction()
        lock("//tmp/t", tx=tx1, mode="exclusive")
        lock_id2 = lock("//tmp/t", tx=tx2, mode="exclusive", waitable=True)
        assert get("//sys/locks/" + lock_id2 + "/@state") == "pending"
        write_table("<append=true>//tmp/t", {"a": "b"}, tx=tx1)

    def test_waitable_lock11(self):
        create("table", "//tmp/t")
        tx1 = start_transaction()
        tx2 = start_transaction()
        tx3 = start_transaction()
        lock_id1 = lock("//tmp/t", tx=tx1, mode="shared", child_key="a")
        lock_id2 = lock("//tmp/t", tx=tx2, mode="shared", child_key="a", waitable=True)
        lock_id3 = lock("//tmp/t", tx=tx3, mode="shared", child_key="b", waitable=True)
        assert get("#" + lock_id1 + "/@state") == "acquired"
        assert get("#" + lock_id2 + "/@state") == "pending"
        assert get("#" + lock_id3 + "/@state") == "acquired"

    def test_waitable_lock12(self):
        create("table", "//tmp/t")
        tx1 = start_transaction()
        tx2 = start_transaction()
        tx3 = start_transaction()
        tx4 = start_transaction()
        lock_id1 = lock("//tmp/t", tx=tx1, mode="shared", child_key="a")
        lock_id2 = lock("//tmp/t", tx=tx2, mode="shared", child_key="b")
        lock_id3 = lock("//tmp/t", tx=tx3, mode="shared", child_key="a", waitable=True)
        lock_id4 = lock("//tmp/t", tx=tx4, mode="shared", child_key="b", waitable=True)
        assert get("#" + lock_id1 + "/@state") == "acquired"
        assert get("#" + lock_id2 + "/@state") == "acquired"
        assert get("#" + lock_id3 + "/@state") == "pending"
        assert get("#" + lock_id4 + "/@state") == "pending"
        abort_transaction(tx2)
        assert get("#" + lock_id1 + "/@state") == "acquired"
        assert get("#" + lock_id3 + "/@state") == "pending"
        assert get("#" + lock_id4 + "/@state") == "acquired"

    def test_waitable_lock13(self):
        create("table", "//tmp/t")

        tx1 = start_transaction()
        tx2 = start_transaction()
        tx3 = start_transaction()

        lock_id1 = lock("//tmp/t", tx=tx1)
        lock_id2 = lock("//tmp/t", tx=tx2, waitable=True)

        assert get("#" + lock_id1 + "/@state") == "acquired"
        assert get("#" + lock_id2 + "/@state") == "pending"

        write_table("//tmp/t", {"foo": "bar1"}, tx = tx1)
        commit_transaction(tx1)

        assert get("#" + lock_id2 + "/@state") == "acquired"

        lock_id3 = lock("//tmp/t", tx=tx3, waitable=True)
        assert get("#" + lock_id3 + "/@state") == "pending"

        write_table("<append=true>//tmp/t", {"foo": "bar2"}, tx = tx2)
        commit_transaction(tx2)

        assert read_table("//tmp/t") == [{"foo": "bar1"}, {"foo": "bar2"}]

    def test_waitable_lock_14(self):
        create("table", "//tmp/t")

        tx1 = start_transaction()
        tx2 = start_transaction()

        lock_id1 = lock("//tmp/t", tx=tx1)
        lock_id2 = lock("//tmp/t", tx=tx2, waitable=True)

        assert get("#" + lock_id1 + "/@state") == "acquired"
        assert get("#" + lock_id2 + "/@state") == "pending"

        abort_transaction(tx2)
        assert not exists("#" + lock_id2)

    def test_waitable_lock_15(self):
        create("table", "//tmp/t")

        tx1 = start_transaction()
        tx2 = start_transaction()
        tx3 = start_transaction(tx=tx2)

        lock_id1 = lock("//tmp/t", tx=tx1)
        lock_id2 = lock("//tmp/t", tx=tx3, waitable=True)

        assert get("#" + lock_id1 + "/@state") == "acquired"
        assert get("#" + lock_id2 + "/@state") == "pending"

        commit_transaction(tx3)
        assert get("#" + lock_id2 + "/@state") == "pending"
        assert get("#" + lock_id2 + "/@transaction_id") == tx2

    def test_yt_144(self):
        create("table", "//tmp/t")

        tx1 = start_transaction()
        lock("//tmp/t", tx=tx1, mode="exclusive")

        tx2 = start_transaction()
        lock_id = lock("//tmp/t", mode="exclusive", waitable=True, tx=tx2)

        abort_transaction(tx1)
        assert get("//sys/locks/" + lock_id + "/@state") == "acquired"
        remove("//tmp/t", tx=tx2)
        abort_transaction(tx2)

        assert get("//tmp/t/@parent_id") == get("//tmp/@id")

    def test_remove_locks(self):
        set("//tmp/a", {"b" : 1})

        tx1 = start_transaction()
        tx2 = start_transaction()

        set("//tmp/a/b", 2, tx = tx1)
        with pytest.raises(YtError): remove("//tmp/a", tx = tx2)

    def test_map_locks1(self):
        tx = start_transaction()
        set("//tmp/a", 1, tx = tx)
        assert get("//tmp/@lock_mode") == "none"
        assert get("//tmp/@lock_mode", tx = tx) == "shared"

        locks = get("//tmp/@locks", tx = tx)
        assert len(locks) == 1

        lock = locks[0]
        assert lock["mode"] == "shared"
        assert lock["child_key"] == "a"

        commit_transaction(tx)
        assert get("//tmp") == {"a" : 1}

    def test_map_locks2(self):
        tx1 = start_transaction()
        set("//tmp/a", 1, tx = tx1)

        tx2 = start_transaction()
        set("//tmp/b", 2, tx = tx2)

        assert get("//tmp", tx = tx1) == {"a" : 1}
        assert get("//tmp", tx = tx2) == {"b" : 2}
        assert get("//tmp") == {}

        commit_transaction(tx1)
        assert get("//tmp") == {"a" : 1}
        assert get("//tmp", tx = tx2) == {"a" : 1, "b" : 2}

        commit_transaction(tx2)
        assert get("//tmp") == {"a" : 1, "b" : 2}

    def test_map_locks3(self):
        tx1 = start_transaction()
        set("//tmp/a", 1, tx = tx1)

        tx2 = start_transaction()
        with pytest.raises(YtError): set("//tmp/a", 2, tx = tx2)

    def test_map_locks4(self):
        set("//tmp/a", 1)

        tx = start_transaction()
        remove("//tmp/a", tx = tx)

        assert get("//tmp/@lock_mode", tx = tx) == "shared"

        locks = get("//tmp/@locks", tx = tx)
        assert len(locks) == 1

        lock = locks[0]
        assert lock["mode"] == "shared"
        assert lock["child_key"] == "a"

    def test_map_locks5(self):
        set("//tmp/a", 1)

        tx1 = start_transaction()
        remove("//tmp/a", tx = tx1)

        tx2 = start_transaction()
        with pytest.raises(YtError): set("//tmp/a", 2, tx = tx2)

    def test_map_locks6(self):
        tx = start_transaction()
        set("//tmp/a", 1, tx = tx)
        assert get("//tmp/a", tx = tx) == 1
        assert get("//tmp") == {}

        with pytest.raises(YtError): remove("//tmp/a")
        remove("//tmp/a", tx = tx)
        assert get("//tmp", tx = tx) == {}

        commit_transaction(tx)
        assert get("//tmp") == {}

    def test_map_locks7(self):
        set("//tmp/a", 1)

        tx = start_transaction()
        remove("//tmp/a", tx = tx)
        set("//tmp/a", 2, tx = tx)
        remove("//tmp/a", tx = tx)
        commit_transaction(tx)

        assert get("//tmp") == {}

    def test_map_locks8(self):
        tx = start_transaction()
        lock("//tmp", mode="shared", tx=tx, child_key="a")

        with pytest.raises(YtError):
            set("//tmp/a", 1)

        other_tx = start_transaction()
        with pytest.raises(YtError):
            lock("//tmp", mode="shared", tx=other_tx, child_key="a")

    def test_attr_locks1(self):
        tx = start_transaction()
        set("//tmp/@a", 1, tx = tx)
        assert get("//tmp/@lock_mode") == "none"
        assert get("//tmp/@lock_mode", tx = tx) == "shared"

        locks = get("//tmp/@locks", tx = tx)
        assert len(locks) == 1

        lock = locks[0]
        assert lock["mode"] == "shared"
        assert lock["attribute_key"] == "a"

        commit_transaction(tx)
        assert get("//tmp/@a") == 1

    def test_attr_locks2(self):
        tx1 = start_transaction()
        set("//tmp/@a", 1, tx = tx1)

        tx2 = start_transaction()
        set("//tmp/@b", 2, tx = tx2)

        assert get("//tmp/@a", tx = tx1) == 1
        assert get("//tmp/@b", tx = tx2) == 2
        with pytest.raises(YtError): get("//tmp/@a")
        with pytest.raises(YtError): get("//tmp/@b")

        commit_transaction(tx1)
        assert get("//tmp/@a") == 1
        assert get("//tmp/@a", tx = tx2) == 1
        assert get("//tmp/@b", tx = tx2) == 2

        commit_transaction(tx2)
        assert get("//tmp/@a") == 1
        assert get("//tmp/@b") == 2

    def test_attr_locks3(self):
        tx1 = start_transaction()
        set("//tmp/@a", 1, tx = tx1)

        tx2 = start_transaction()
        with pytest.raises(YtError): set("//tmp/@a", 2, tx = tx2)

    def test_attr_locks4(self):
        set("//tmp/@a", 1)

        tx = start_transaction()
        remove("//tmp/@a", tx = tx)

        assert get("//tmp/@lock_mode", tx = tx) == "shared"

        locks = get("//tmp/@locks", tx = tx)
        assert len(locks) == 1

        lock = locks[0]
        assert lock["mode"] == "shared"
        assert lock["attribute_key"] == "a"

    def test_attr_locks5(self):
        set("//tmp/@a", 1)

        tx1 = start_transaction()
        remove("//tmp/@a", tx = tx1)

        tx2 = start_transaction()
        with pytest.raises(YtError): set("//tmp/@a", 2, tx = tx2)

    def test_attr_locks6(self):
        tx = start_transaction()
        set("//tmp/@a", 1, tx = tx)
        assert get("//tmp/@a", tx = tx) == 1
        with pytest.raises(YtError): get("//tmp/@a")

        with pytest.raises(YtError): remove("//tmp/@a")
        remove("//tmp/@a", tx = tx)
        with pytest.raises(YtError): get("//tmp/@a", tx = tx)

        commit_transaction(tx)
        with pytest.raises(YtError): get("//tmp/@a")

    def test_attr_locks7(self):
        tx = start_transaction()
        lock("//tmp", mode="shared", tx=tx, attribute_key="a")

        with pytest.raises(YtError):
            set("//tmp/@a", 1)

        other_tx = start_transaction()
        with pytest.raises(YtError):
            lock("//tmp", mode="shared", tx=other_tx, attribute_key="a")

    def test_lock_mode_for_child_and_attr_locks(self):
        tx = start_transaction()
        with pytest.raises(YtError): lock("//tmp", mode="exclusive", tx=tx, child_key="a")
        with pytest.raises(YtError): lock("//tmp", mode="exclusive", tx=tx, attribute_key="a")

    def test_nested_tx1(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx = tx1)
        lock("//tmp", tx = tx2)
        assert len(get("//tmp/@locks")) == 1
        abort_transaction(tx2)
        assert len(get("//tmp/@locks")) == 0

    def test_nested_tx2(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx = tx1)
        lock_id = lock("//tmp", tx = tx2)
        assert len(get("//tmp/@locks")) == 1
        commit_transaction(tx2)
        assert len(get("//tmp/@locks")) == 1
        assert get("//sys/locks/" + lock_id + "/@transaction_id") == tx1

    def test_nested_tx3(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx = tx1)
        lock_id = lock("//tmp", tx = tx2, mode = "snapshot")
        assert len(get("//tmp/@locks")) == 1
        commit_transaction(tx2)
        assert not exists("//sys/locks/" + lock_id)

    def test_nested_tx4(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx = tx1)
        lock("//tmp", tx = tx1)
        lock("//tmp", tx = tx2)
        with pytest.raises(YtError): lock("//tmp", tx = tx1)

    def test_nested_tx5(self):
        set("//tmp/x", 1)
        tx1 = start_transaction()
        tx2 = start_transaction(tx = tx1)
        set("//tmp/x", 2, tx = tx1)
        set("//tmp/x", 3, tx = tx2)
        with pytest.raises(YtError): set("//tmp/x", 4, tx = tx1)

    def test_manual_lock_not_recursive1(self):
        set("//tmp/x", {"y":{}})
        tx = start_transaction()
        lock("//tmp/x", tx=tx, mode="shared", child_key="a")
        assert len(get("//tmp/x/@locks")) == 1
        assert len(get("//tmp/x/y/@locks")) == 0

    def test_manual_lock_not_recursive2(self):
        set("//tmp/x", {"y":{}})
        tx1 = start_transaction()
        tx2 = start_transaction()
        lock("//tmp/x/y", tx=tx1, mode="shared", child_key="a")
        lock("//tmp/x", tx=tx2, mode="shared", child_key="a")

##################################################################

class TestLocksMulticell(TestLocks):
    NUM_SECONDARY_MASTER_CELLS = 2

