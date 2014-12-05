import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *


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
        # check that node under snapshot lock wasn"t changed
        assert get("//tmp/node", tx = tx) == 42

        # can"t change value under snapshot lock
        with pytest.raises(YtError): set("//tmp/node", 200, tx = tx)
        
        abort_transaction(tx)

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

    def test_lock_propagation1(self):
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

        assert get("//tmp/a/@locks") == []
        
    def test_lock_propagation2(self):
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

        locks = get("//tmp/a/@locks")
        assert len(locks) == 1
        assert locks[0]["state"] == "acquired"
        assert locks[0]["transaction_id"] == tx1
        assert locks[0]["mode"] == "exclusive"
        assert get("#" + lock_id +"/@state") == "acquired"

        commit_transaction(tx1)

        assert get("//tmp/a/@locks") == []
        
    def test_redundant_lock1(self):
        set("//tmp/a", 1)
        tx = start_transaction()
        
        lock_id1 = lock("//tmp/a", tx=tx)
        assert get("#" + lock_id1 + "/@state") == "acquired"

        lock_id2 = lock("//tmp/a", tx=tx)
        assert lock_id2 == "0-0-0-0"

    def test_redundant_lock2(self):
        set("//tmp/a", 1)
        tx = start_transaction()

        lock_id1 = lock("//tmp/a", tx=tx)
        assert get("#" + lock_id1 + "/@state") == "acquired"

        lock_id2 = lock("//tmp/a", tx=tx, mode="shared")
        assert lock_id2 == "0-0-0-0"

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
        assert lock_id3 == "0-0-0-0"

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
        with pytest.raises(YtError): lock("//tmp/a", tx=tx3, mode="shared")

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
        write("//tmp/t", {"foo": "bar"}, tx = tx2)

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

    def test_yt144(self):
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
 
    def test_nested_tx1(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx = tx1)
        lock_id = lock("//tmp", tx = tx2)
        assert len(get("//tmp/@locks")) == 1
        abort_transaction(tx2)
        assert len(get("//tmp/@locks")) == 1
        assert get("//sys/locks/" + lock_id + "/@transaction_id") == tx1
        
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

