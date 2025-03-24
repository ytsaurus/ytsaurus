from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, get, set, remove, exists, start_transaction, abort_transaction,
    commit_transaction, lock, raises_yt_error,
    unlock, read_table, write_table, multicell_sleep, gc_collect, get_driver)

from yt_helpers import get_current_time, parse_yt_time

from yt.common import YtError

import pytest

import builtins
import decorator

##################################################################


def not_implemented_in_sequoia(func):
    def wrapper(func, self, *args, **kwargs):
        if isinstance(self, TestCypressLocksInSequoia):
            pytest.skip("Not implemented in Sequoia")
        return func(self, *args, **kwargs)

    return decorator.decorate(func, wrapper)


def cannot_be_implemented_in_sequoia(reason):
    def wrapper_factory(func):
        def wrapper(func, self, *args, **kwargs):
            if isinstance(self, TestCypressLocksInSequoia):
                pytest.skip(f"Cannot be imlpemented in Sequoia: {reason}")
            return func(self, *args, **kwargs)

        return decorator.decorate(func, wrapper)

    return wrapper_factory


##################################################################


@pytest.mark.enabled_multidaemon
class TestCypressLocks(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_TEST_PARTITIONS = 4

    def _in_sequoia(self):
        return isinstance(self, TestCypressLocksInSequoia)

    @authors("panin", "ignat")
    def test_invalid_cases(self):
        # outside of transaction
        with raises_yt_error("A valid master transaction is required"):
            lock("/")

        # at non-existing node
        tx = start_transaction()
        with raises_yt_error("Node //tmp has no child with key \"non_existent\""):
            lock("//tmp/non_existent", tx=tx)

        # error while parsing mode
        with raises_yt_error("Error parsing ELockMode value \"invalid\""):
            lock("/", mode="invalid", tx=tx)

        # taking None lock is forbidden
        with raises_yt_error("Invalid lock mode \"none\""):
            lock("/", mode="none", tx=tx)

        # attributes do not have @lock_mode
        if self._in_sequoia():
            # TODO(kvk1920): create under tx when branch resolving will be
            # implemented.
            set("//tmp/value", b"42", is_raw=True)

            # TODO(kvk1920): use set with attributes when it will be supported.
            set("//tmp/value/@attr", "some", tx=tx)
        else:
            set("//tmp/value", b"<attr=some>42", is_raw=True, tx=tx)

        with raises_yt_error("\"Lock\" method is not supported"):
            lock("//tmp/value/@attr/@lock_mode", tx=tx)

        abort_transaction(tx)

    @authors("panin", "ignat")
    def test_lock_mode(self):
        tx = start_transaction()

        if self._in_sequoia():
            set("//tmp/map", b"{\"nested_map\"={\"0\"=0; \"1\"=1; \"2\"=2}}", is_raw=True)
            set("//tmp/map/nested_map/0", 42, tx=tx)
            assert get("//tmp/map/nested_map/0/@lock_mode", tx=tx) == "exclusive"
            return

        set("//tmp/map", b"{\"nested_map\"=<attr=some>{\"0\"=0; \"1\"=1; \"2\"=2}}", is_raw=True, tx=tx)

        # check that lock is set on nested nodes
        assert get("//tmp/map/@lock_mode", tx=tx) == "exclusive"
        assert get("//tmp/map/nested_map/@lock_mode", tx=tx) == "exclusive"
        assert get("//tmp/map/nested_map/0/@lock_mode", tx=tx) == "exclusive"

        abort_transaction(tx)

    @authors("ignat")
    def test_lock_full_response(self):
        driver = get_driver(api_version=4)
        create("map_node", "//tmp/node", driver=driver)

        tx = start_transaction(driver=driver)
        rsp = lock("//tmp/node", mode="snapshot", tx=tx, full_response=True, driver=driver)
        assert list(rsp.keys()) == ["lock_id", "node_id", "revision"]
        assert rsp["lock_id"] == get("//tmp/node/@locks/0/id")

        set("//tmp/node", {}, driver=driver, force=True)
        assert rsp["node_id"] == get("//tmp/node/@id", tx=tx, driver=driver)
        assert rsp["revision"] == get("//tmp/node/@revision", tx=tx, driver=driver)

    @authors("babenko")
    def test_lock_nonnull_revision_yt_13962(self):
        driver = get_driver(api_version=4)

        if self._in_sequoia():
            create("file", "//tmp/f")

        tx1 = start_transaction(driver=driver)
        tx2 = start_transaction(tx=tx1, driver=driver)

        if not self._in_sequoia():
            create("file", "//tmp/f", tx=tx1)

        r1 = get("//tmp/f/@revision", tx=tx1)
        set("//tmp/f/@attr", "value", tx=tx1)
        r2 = get("//tmp/f/@revision", tx=tx1)

        assert r2 > r1

        rsp = lock("//tmp/f", mode="snapshot", tx=tx2, full_response=True, driver=driver)
        assert rsp["revision"] == r2

    @authors("babenko")
    def test_lock_null_revision_yt_13962(self):
        driver = get_driver(api_version=4)

        tx1 = start_transaction(driver=driver)
        tx2 = start_transaction(driver=driver)

        create("file", "//tmp/f")

        rsp1 = lock("//tmp/f", mode="exclusive", tx=tx1, full_response=True, driver=driver)
        assert rsp1["revision"] == get("//tmp/f/@revision")

        rsp2 = lock("//tmp/f", mode="exclusive", tx=tx2, waitable=True, full_response=True, driver=driver)
        assert rsp2["revision"] == 0

    @authors("panin")
    def test_shared_lock_inside_tx(self):
        tx_outer = start_transaction()
        create("table", "//tmp/table", tx=tx_outer)

        tx_inner = start_transaction(tx=tx_outer)
        lock("//tmp/table", mode="shared", tx=tx_inner)

    @authors("shakurov")
    def test_snapshot_lock(self):
        set("//tmp/node", 42)

        tx = start_transaction()
        lock("//tmp/node", mode="snapshot", tx=tx)

        set("//tmp/node", 100)
        # check that node under snapshot lock wasn't changed
        assert get("//tmp/node", tx=tx) == 42

        # can't change value under snapshot lock
        with raises_yt_error(f"Cannot take \"exclusive\" lock for node //tmp/node since \"snapshot\" lock is already taken by same transaction {tx}"):
            set("//tmp/node", 200, tx=tx)

        abort_transaction(tx)

    def _get_tx_lock_ids(self, tx):
        return [lock_id for cell_tag, lock_ids in get("#{0}/@lock_ids".format(tx)).items() for lock_id in lock_ids]

    def _get_tx_locked_node_ids(self, tx):
        cell_tag_to_locked_node_ids = get("#{0}/@locked_node_ids".format(tx))
        return [lock_id for cell_tag, lock_ids in cell_tag_to_locked_node_ids.items() for lock_id in lock_ids]

    def _assert_locked(self, path, tx, mode, child_key=None, attribute_key=None):
        self._assert_locked_impl(path, tx, True)

        locks = get(path + "/@locks", tx=tx)
        lock = None
        for some_lock in locks:
            if (
                some_lock.get("transaction_id") == tx
                and (child_key is None or some_lock.get("child_key") == child_key)
                and (attribute_key is None or some_lock.get("attribute_key") == attribute_key)
            ):
                lock = some_lock
                break

        assert lock
        assert lock["mode"] == mode
        assert lock["state"] == "acquired"
        if child_key is not None:
            assert lock["child_key"] == child_key
        if attribute_key is not None:
            assert lock["attribute_key"] == attribute_key

        assert lock["id"] in self._get_tx_lock_ids(tx)

    def _assert_not_locked(self, path, tx):
        self._assert_locked_impl(path, tx, False)

    def _assert_locked_impl(self, path, tx, assert_true):
        node_id = get(path + "/@id", tx=tx)
        locked_node_ids = self._get_tx_locked_node_ids(tx)
        if assert_true:
            assert node_id in locked_node_ids
        else:
            assert node_id not in locked_node_ids

    @authors("shakurov")
    @pytest.mark.parametrize("mode", ["exclusive", "shared"])
    def test_acquired_lock_promotion(self, mode):
        create("map_node", "//tmp/m1")

        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        lock("//tmp/m1", mode=mode, tx=tx2)
        commit_transaction(tx2)

        self._assert_locked("//tmp/m1", tx1, mode)
        locks = get("//tmp/m1/@locks")
        assert len(locks) == 1
        assert locks[0]["state"] == "acquired"
        assert locks[0]["transaction_id"] == tx1
        assert locks[0]["mode"] == mode

        commit_transaction(tx1)  # mustn't crash

    @authors("shakurov")
    def test_pending_lock_promotion(self):
        create("map_node", "//tmp/m1")

        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        other_tx = start_transaction()

        acquired_lock_id = lock("//tmp/m1", mode="exclusive", tx=other_tx)["lock_id"]
        pending_lock_id = lock("//tmp/m1", mode="exclusive", tx=tx2, waitable=True)["lock_id"]
        assert get("#" + acquired_lock_id + "/@state") == "acquired"
        assert get("#" + pending_lock_id + "/@state") == "pending"

        commit_transaction(tx2)

        self._assert_locked("//tmp/m1", other_tx, "exclusive")
        locks = get("//tmp/m1/@locks")
        assert len(locks) == 2
        locks = {
            lock["id"]: {
                "state": lock["state"],
                "transaction_id": lock["transaction_id"],
                "mode": lock["mode"],
            }
            for lock in locks
        }
        assert locks[acquired_lock_id]["state"] == "acquired"
        assert locks[acquired_lock_id]["transaction_id"] == other_tx
        assert locks[acquired_lock_id]["mode"] == "exclusive"
        assert locks[pending_lock_id]["state"] == "pending"
        assert locks[pending_lock_id]["transaction_id"] == tx1
        assert locks[pending_lock_id]["mode"] == "exclusive"

        commit_transaction(other_tx)

        locks = get("//tmp/m1/@locks")
        assert len(locks) == 1
        assert locks[0]["state"] == "acquired"
        assert locks[0]["transaction_id"] == tx1
        assert locks[0]["mode"] == "exclusive"

        commit_transaction(tx1)  # mustn't crash

    @authors("shakurov")
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
        lock_id = lock("//tmp/m1", mode=mode, tx=tx, **kwargs1)["lock_id"]

        self._assert_locked("//tmp/m1", tx, mode, **kwargs1)

        tx2 = start_transaction()
        if mode != "snapshot":
            if mode == "exclusive":
                expected_error = "Cannot take \"exclusive\" lock for node //tmp/m1 since " \
                                 f"\"exclusive\" lock is taken by concurrent transaction {tx}"
            elif mode == "shared_child":
                expected_error = "Cannot take lock for child \"x1\" of node //tmp/m1 since this " \
                                 f"child is locked by concurrent transaction {tx}"
            elif mode == "shared_attribute":
                expected_error = "Cannot take lock for attribute \"x1\" of node //tmp/m1 since " \
                                 f"this attribute is locked by concurrent transaction {tx}"
            else:
                expected_error = None
            with raises_yt_error(expected_error):
                lock("//tmp/m1", mode=mode, tx=tx2, **kwargs1)
        if mode == "shared":
            lock("//tmp/m1", mode=mode, tx=tx2, **kwargs2)

        unlock("//tmp/m1", tx=tx)

        if mode == "shared":
            self._assert_locked("//tmp/m1", tx2, "shared", **kwargs2)

        self._assert_not_locked("//tmp/m1", tx)
        assert not exists("#{0}".format(lock_id))
        assert len(self._get_tx_lock_ids(tx)) == 0

        lock_id = lock("//tmp/m1", mode=mode, tx=tx2, **kwargs1)["lock_id"]
        self._assert_locked("//tmp/m1", tx2, mode, **kwargs1)

        commit_transaction(tx)  # mustn't crash

    @authors("shakurov")
    def test_unlock_implicit_noop(self):
        create("map_node", "//tmp/m1")

        tx = start_transaction()
        if not self._in_sequoia():
            set("//tmp/m1/c1", "child_value", tx=tx)
        set("//tmp/m1/@a1", "attribute_value", tx=tx)

        if not self._in_sequoia():
            self._assert_locked("//tmp/m1", tx, "shared", child_key="c1")
        self._assert_locked("//tmp/m1", tx, "shared", attribute_key="a1")
        if not self._in_sequoia():
            self._assert_locked("//tmp/m1/c1", tx, "exclusive")

        unlock("//tmp/m1", tx=tx)
        if not self._in_sequoia():
            unlock("//tmp/m1/c1", tx=tx)

        if not self._in_sequoia():
            self._assert_locked("//tmp/m1", tx, "shared", child_key="c1")
        self._assert_locked("//tmp/m1", tx, "shared", attribute_key="a1")
        if not self._in_sequoia():
            self._assert_locked("//tmp/m1/c1", tx, "exclusive")

        commit_transaction(tx)  # mustn't crash

    @authors("shakurov")
    @pytest.mark.parametrize("mode", ["exclusive", "shared"])
    def test_unlock_modified(self, mode):
        node_id = create("map_node", "//tmp/m1")

        tx = start_transaction()
        lock("//tmp/m1", mode=mode, tx=tx)

        self._assert_locked("//tmp/m1", tx, mode)

        set("//tmp/m1/c1", "child_value", tx=tx)

        get("//tmp/m1/@locks")

        with raises_yt_error(f"Node #{node_id} is modified by transaction "
                             f"\"{tx}\" and cannot be unlocked"):
            unlock("//tmp/m1", tx=tx)  # modified and can't be unlocked

    @authors("shakurov")
    def test_unlock_not_locked(self):
        create("map_node", "//tmp/m1")

        tx = start_transaction()
        self._assert_not_locked("//tmp/m1", tx)
        unlock("//tmp/m1", tx=tx)  # should be a quiet noop

    @authors("shakurov")
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

        commit_transaction(tx1)  # mustn't crash

    @authors("shakurov")
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
        commit_transaction(tx)  # mustn't crash

    @authors("shakurov")
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

        commit_transaction(tx1)  # mustn't crash

    @authors("shakurov")
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

        commit_transaction(tx2)  # mustn't crash
        commit_transaction(tx1)

    @authors("shakurov")
    def test_unlock_pending(self):
        create("map_node", "//tmp/m1")
        tx1 = start_transaction()
        tx2 = start_transaction()  # not nested

        lock("//tmp/m1", mode="exclusive", tx=tx1)["lock_id"]
        self._assert_locked("//tmp/m1", tx1, "exclusive")
        pending_lock_id = lock("//tmp/m1", tx=tx2, waitable=True)["lock_id"]
        assert get("#" + pending_lock_id + "/@state") == "pending"

        unlock("//tmp/m1", tx=tx2)

        self._assert_locked("//tmp/m1", tx1, "exclusive")
        gc_collect()
        assert not exists("#" + pending_lock_id)

        commit_transaction(tx2)  # mustn't crash
        commit_transaction(tx1)

    @authors("shakurov")
    def test_unlock_promotes_pending(self):
        create("map_node", "//tmp/m1")
        tx1 = start_transaction()
        tx2 = start_transaction()  # not nested

        acquired_lock_id = lock("//tmp/m1", mode="exclusive", tx=tx1)["lock_id"]
        self._assert_locked("//tmp/m1", tx1, "exclusive")
        pending_lock_id = lock("//tmp/m1", tx=tx2, waitable=True)["lock_id"]
        assert get("#" + pending_lock_id + "/@state") == "pending"

        assert get("#" + pending_lock_id + "/@transaction_id") == tx2

        unlock("//tmp/m1", tx=tx1)

        assert not exists("#" + acquired_lock_id)

        assert get("#" + pending_lock_id + "/@state") == "acquired"

        commit_transaction(tx2)  # mustn't crash
        commit_transaction(tx1)

    @authors("shakurov")
    @not_implemented_in_sequoia
    def test_unlock_unreachable_node(self):
        create("map_node", "//tmp/m1")
        create("map_node", "//tmp/m1/m2")
        node_id = create("map_node", "//tmp/m1/m2/m3")

        tx = start_transaction()
        lock("//tmp/m1/m2/m3", mode="snapshot", tx=tx)["lock_id"]

        remove("//tmp/m1")

        assert not exists("//tmp/m1/m2/m3")
        assert not exists("//tmp/m1/m2/m3", tx=tx)

        node_path = "#" + node_id
        assert exists(node_path)
        self._assert_locked(node_path, tx, "snapshot")
        unlock(node_path, tx=tx)

        gc_collect()

        assert not exists(node_path)

    @authors("shakurov")
    @pytest.mark.parametrize("mode", ["shared", "snapshot"])
    def test_unlock_deeply_nested_node1(self, mode):
        create("map_node", "//tmp/m1")

        tx1 = start_transaction(timeout=60000)
        tx2 = start_transaction(tx=tx1, timeout=60000)
        tx3 = start_transaction(tx=tx2, timeout=60000)
        tx4 = start_transaction(tx=tx3, timeout=60000)
        tx5 = start_transaction(tx=tx4, timeout=60000)
        tx6 = start_transaction(tx=tx5, timeout=60000)

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

        unlock("//tmp/m1", tx=tx5)  # not lock and does nothing
        self._assert_locked("//tmp/m1", tx2, mode, **kwargs2)
        self._assert_not_locked("//tmp/m1", tx4)
        self._assert_locked("//tmp/m1", tx6, mode, **kwargs6)

        unlock("//tmp/m1", tx=tx6)
        self._assert_locked("//tmp/m1", tx2, mode, **kwargs2)
        self._assert_not_locked("//tmp/m1", tx4)
        self._assert_not_locked("//tmp/m1", tx6)

        if mode != "snapshot":
            with raises_yt_error("Cannot take \"exclusive\" lock for node //tmp/m1 since \"shared\""
                                 f" lock is taken by concurrent transaction {tx2}"):
                lock("//tmp/m1", tx=tx1, mode="exclusive")

        unlock("//tmp/m1", tx=tx2)
        self._assert_not_locked("//tmp/m1", tx2)
        self._assert_not_locked("//tmp/m1", tx4)
        self._assert_not_locked("//tmp/m1", tx6)

        lock("//tmp/m1", tx=tx1, mode="exclusive")
        self._assert_locked("//tmp/m1", tx1, "exclusive")

        for tx in [tx6, tx5, tx4, tx3, tx2, tx1]:
            commit_transaction(tx)  # mustn't throw

    @authors("shakurov")
    @pytest.mark.parametrize(
        "transactions_to_skip",
        [{1, 3, 5}, {1, 3}, {1, 5}, {3, 5}, {1}, {3}, {5}, builtins.set()],
    )
    @pytest.mark.parametrize(
        "modes",
        [
            ["exclusive", "shared", "snapshot"],
            ["shared", "exclusive", "shared"],
            ["exclusive", "shared", "shared"],
            ["shared", "shared", "exclusive"],
        ],
    )
    def test_unlock_deeply_nested_node2(self, modes, transactions_to_skip):
        create("map_node", "//tmp/m1")

        prev_tx = None
        transactions = [None] * 6
        for i in range(0, 6):
            if i + 1 in transactions_to_skip:
                transactions[i] = None
            else:
                if prev_tx is None:
                    prev_tx = start_transaction(timeout=60000)
                else:
                    prev_tx = start_transaction(tx=prev_tx, timeout=60000)
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
            unlock("//tmp/m1", tx=tx5)  # not lock and does nothing
            self._assert_locked("//tmp/m1", tx2, mode2, **kwargs2)
            self._assert_not_locked("//tmp/m1", tx4)
            self._assert_locked("//tmp/m1", tx6, mode6, **kwargs6)

        unlock("//tmp/m1", tx=tx6)
        self._assert_locked("//tmp/m1", tx2, mode2, **kwargs2)
        self._assert_not_locked("//tmp/m1", tx4)
        self._assert_not_locked("//tmp/m1", tx6)

        if tx1 is not None:
            if mode6 == "snapshot":
                with raises_yt_error("Cannot take lock for child \"c1\" of node //tmp/m1 since "
                                     f"this child is locked by concurrent transaction {tx2}"):
                    lock("//tmp/m1", tx=tx1, mode="shared", child_key="c1")
            else:
                # Topmost transaction and its lock mode.
                expected_lock_mode = None
                lock_owner = None

                for tx, lock_mode in ((tx6, mode6), (tx4, mode4), (tx2, mode2)):
                    if tx is None:
                        continue
                    lock_owner = tx
                    expected_lock_mode = lock_mode

                assert expected_lock_mode
                assert lock_owner

                with raises_yt_error("Cannot take \"exclusive\" lock for node //tmp/m1 since "
                                     f"\"{expected_lock_mode}\" lock is taken by concurrent "
                                     f"transaction {lock_owner}"):
                    lock("//tmp/m1", tx=tx1, mode="exclusive")

        unlock("//tmp/m1", tx=tx2)
        self._assert_not_locked("//tmp/m1", tx2)
        self._assert_not_locked("//tmp/m1", tx4)
        self._assert_not_locked("//tmp/m1", tx6)

        if tx1 is not None:
            lock("//tmp/m1", tx=tx1, mode="exclusive")
            self._assert_locked("//tmp/m1", tx1, "exclusive")

        for tx in reversed(transactions):
            if tx is not None:
                commit_transaction(tx)  # mustn't throw

    @authors("shakurov")
    def test_unlock_without_transaction(self):
        create("map_node", "//tmp/m1")
        tx = start_transaction()
        lock("//tmp/m1", tx=tx, mode="exclusive")
        assert get("//tmp/m1/@lock_mode", tx=tx) == "exclusive"
        with raises_yt_error("A valid master transaction is required"):
            unlock("//tmp/m1")

    @authors("shakurov")
    def test_snapshot_lock_patch_up(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        tx31 = start_transaction(tx=tx2)
        tx32 = start_transaction(tx=tx2)

        create("table", "//tmp/t")
        lock("//tmp/t", mode="exclusive", tx=tx31)
        lock("//tmp/t", mode="snapshot", tx=tx32)
        unlock("//tmp/t", tx=tx31)

        # Must not crash.
        get("//tmp/t/@", tx=tx32)
        get("//tmp/t/@", tx=tx31)
        get("//tmp/t/@", tx=tx2)
        get("//tmp/t/@", tx=tx1)
        assert get("//tmp/t/@lock_mode", tx=tx32) == "snapshot"
        assert get("//tmp/t/@lock_mode", tx=tx31) == "none"
        assert get("//tmp/t/@lock_mode", tx=tx2) == "none"
        assert get("//tmp/t/@lock_mode", tx=tx1) == "none"
        assert get("//tmp/t/@lock_mode") == "none"
        assert get("//tmp/t/@lock_count") == 1

        commit_transaction(tx2)

        assert get("//tmp/t/@lock_mode", tx=tx1) == "none"
        assert get("//tmp/t/@lock_mode") == "none"
        assert get("//tmp/t/@lock_count") == 0

        commit_transaction(tx1)

        assert get("//tmp/t/@lock_mode") == "none"
        assert get("//tmp/t/@lock_count") == 0

    @authors("shakurov")
    def test_unlock_unbranches_nodes_with_other_locks(self):
        tx = start_transaction()

        create("table", "//tmp/t1")

        if not self._in_sequoia():
            create("table", "//tmp/t2", tx=tx)

        lock("//tmp/t1", mode="exclusive", tx=tx)

        unlock("//tmp/t1", tx=tx)
        # Must not be crashed YT_LOG_ALERT.
        abort_transaction(tx)

    @authors("babenko", "ignat")
    def test_remove_map_subtree_lock(self):
        set("//tmp/a", {"b": 1})
        tx = start_transaction()
        lock("//tmp/a/b", mode="exclusive", tx=tx)
        with raises_yt_error("Cannot take \"exclusive\" lock for node //tmp/a/b since \"exclusive\""
                             f" lock is taken by concurrent transaction {tx}"):
            remove("//tmp/a")

    @authors("babenko", "ignat")
    @cannot_be_implemented_in_sequoia("list nodes aren't supported")
    def test_remove_list_subtree_lock(self):
        set("//sys/@config/cypress_manager/forbid_list_node_creation", False)
        set("//tmp/a", [1])
        tx = start_transaction()
        lock("//tmp/a/0", mode="exclusive", tx=tx)
        with raises_yt_error("\"exclusive\" lock is taken by concurrent transaction"):
            remove("//tmp/a")

    @authors("babenko", "ignat")
    def test_exclusive_vs_snapshot_locks1(self):
        create("table", "//tmp/t")
        tx1 = start_transaction()
        tx2 = start_transaction()
        lock("//tmp/t", mode="snapshot", tx=tx1)
        lock("//tmp/t", mode="exclusive", tx=tx2)

    @authors("babenko", "ignat")
    def test_exclusive_vs_snapshot_locks2(self):
        create("table", "//tmp/t")
        tx1 = start_transaction()
        tx2 = start_transaction()
        lock("//tmp/t", mode="exclusive", tx=tx2)
        lock("//tmp/t", mode="snapshot", tx=tx1)

    @authors("babenko", "ignat")
    def test_node_locks(self):
        set("//tmp/a", 1)

        tx = start_transaction()
        lock_id = lock("//tmp/a", tx=tx)["lock_id"]
        locks = get("//tmp/a/@locks")
        assert len(locks) == 1
        assert locks[0]["state"] == "acquired"
        assert locks[0]["transaction_id"] == tx
        assert locks[0]["mode"] == "exclusive"
        assert get("#" + lock_id + "/@state") == "acquired"

        abort_transaction(tx)
        assert get("//tmp/a/@locks") == []

    @authors("babenko", "ignat")
    def test_lock_propagation_on_commit(self):
        set("//tmp/a", 1)

        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        lock_id = lock("//tmp/a", tx=tx2)["lock_id"]

        locks = get("//tmp/a/@locks")
        assert len(locks) == 1
        assert locks[0]["state"] == "acquired"
        assert locks[0]["transaction_id"] == tx2
        assert locks[0]["mode"] == "exclusive"
        assert get("#" + lock_id + "/@state") == "acquired"

        commit_transaction(tx2)

        locks = get("//tmp/a/@locks")
        assert len(locks) == 1
        assert locks[0]["state"] == "acquired"
        assert locks[0]["transaction_id"] == tx1
        assert locks[0]["mode"] == "exclusive"
        assert get("#" + lock_id + "/@state") == "acquired"

        commit_transaction(tx1)

        assert get("//tmp/a/@locks") == []

    @authors("babenko", "ignat")
    def test_no_lock_propagation_on_abort(self):
        set("//tmp/a", 1)

        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        lock_id = lock("//tmp/a", tx=tx2)["lock_id"]

        locks = get("//tmp/a/@locks")
        assert len(locks) == 1
        assert locks[0]["state"] == "acquired"
        assert locks[0]["transaction_id"] == tx2
        assert locks[0]["mode"] == "exclusive"
        assert get("#" + lock_id + "/@state") == "acquired"

        abort_transaction(tx2)

        assert get("//tmp/a/@locks") == []

    @authors("babenko")
    def test_redundant_lock1(self):
        tx = start_transaction()
        set("//tmp/a", "x")

        set("//tmp/a", "b", tx=tx)
        assert len(get("//tmp/a/@locks")) == 1

        set("//tmp/a", "c", tx=tx)
        assert len(get("//tmp/a/@locks")) == 1

    @authors("babenko")
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

    @authors("babenko")
    def test_redundant_lock3(self):
        create("table", "//tmp/t")
        tx = start_transaction()
        for i in range(5):
            write_table("//tmp/t", {"foo": "bar"}, tx=tx)
            assert len(get("//tmp/t/@locks")) == 1

    @authors("babenko", "ignat")
    def test_waitable_lock1(self):
        set("//tmp/a", 1)

        tx1 = start_transaction()
        lock_id1 = lock("//tmp/a", tx=tx1)["lock_id"]

        tx2 = start_transaction()
        lock_id2 = lock("//tmp/a", tx=tx2, waitable=True)["lock_id"]

        assert get("#" + lock_id1 + "/@state") == "acquired"
        assert get("//tmp/a/@lock_mode", tx=tx1) == "exclusive"
        assert get("#" + lock_id2 + "/@state") == "pending"

        abort_transaction(tx1)

        assert not exists("//sys/locks/" + lock_id1)
        assert get("#" + lock_id2 + "/@state") == "acquired"
        assert get("//tmp/a/@lock_mode", tx=tx2) == "exclusive"

    @authors("babenko", "ignat")
    def test_waitable_lock2(self):
        set("//tmp/a", 1)

        tx1 = start_transaction()
        lock_id1 = lock("//tmp/a", tx=tx1)["lock_id"]
        assert get("#" + lock_id1 + "/@state") == "acquired"

        tx2 = start_transaction()
        lock_id2 = lock("//tmp/a", tx=tx2, waitable=True)["lock_id"]
        assert get("#" + lock_id2 + "/@state") == "pending"

        lock_id3 = lock("//tmp/a", tx=tx1, waitable=True)["lock_id"]
        assert get("#" + lock_id3 + "/@state") == "acquired"
        assert get("#" + lock_id2 + "/@state") == "pending"

    @authors("babenko", "ignat")
    def test_waitable_lock3(self):
        set("//tmp/a", 1)

        tx1 = start_transaction()
        lock_id1 = lock("//tmp/a", mode="shared", tx=tx1)["lock_id"]
        assert get("#" + lock_id1 + "/@state") == "acquired"

        tx2 = start_transaction()
        lock_id2 = lock("//tmp/a", mode="shared", tx=tx2, waitable=True)["lock_id"]
        assert get("#" + lock_id2 + "/@state") == "acquired"

    @authors("babenko", "ignat")
    def test_waitable_lock4(self):
        set("//tmp/a", 1)

        tx1 = start_transaction()
        lock_id1 = lock("//tmp/a", tx=tx1, mode="shared")["lock_id"]
        assert get("#" + lock_id1 + "/@state") == "acquired"

        tx2 = start_transaction()
        lock_id2 = lock("//tmp/a", tx=tx2, waitable=True)["lock_id"]
        assert get("#" + lock_id2 + "/@state") == "pending"

        tx3 = start_transaction()
        lock_id3 = lock("//tmp/a", tx=tx3, mode="shared")["lock_id"]
        assert get("#" + lock_id3 + "/@state") == "acquired"

    @authors("babenko", "ignat")
    def test_waitable_lock5(self):
        set("//tmp/a", 1)

        tx1 = start_transaction()
        lock_id1 = lock("//tmp/a", tx=tx1)["lock_id"]

        tx2 = start_transaction()
        lock_id2 = lock("//tmp/a", tx=tx2, waitable=True, mode="shared")["lock_id"]

        tx3 = start_transaction()
        lock_id3 = lock("//tmp/a", tx=tx3, waitable=True, mode="shared")["lock_id"]

        tx4 = start_transaction()
        lock_id4 = lock("//tmp/a", tx=tx4, waitable=True)["lock_id"]

        assert get("#" + lock_id1 + "/@state") == "acquired"
        assert get("#" + lock_id2 + "/@state") == "pending"
        assert get("#" + lock_id3 + "/@state") == "pending"
        assert get("#" + lock_id4 + "/@state") == "pending"

        commit_transaction(tx1)
        with raises_yt_error(f"No such object {lock_id1}"):
            get("#" + lock_id1 + "/@state")
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

    @authors("babenko", "ignat")
    def test_waitable_lock6(self):
        set("//tmp/a", 1)

        tx1 = start_transaction()
        lock_id1 = lock("//tmp/a", tx=tx1)["lock_id"]
        assert get("#" + lock_id1 + "/@state") == "acquired"

        remove("//tmp/a", tx=tx1)

        tx2 = start_transaction()
        lock_id2 = lock("//tmp/a", tx=tx2, waitable=True)["lock_id"]
        assert get("#" + lock_id2 + "/@state") == "pending"

        commit_transaction(tx1)

        # Make sure commit reaches the node host cell.
        multicell_sleep()
        # Transaction object must be destroyed.
        gc_collect()
        # Removed node must be destroyed, the lock must become orphaned.
        gc_collect()

        assert not exists("//sys/locks/" + lock_id1)
        assert not exists("//sys/locks/" + lock_id2)

    @authors("babenko", "ignat")
    def test_waitable_lock7(self):
        set("//tmp/a", {"b": 1})

        tx1 = start_transaction()
        lock_id1 = lock("//tmp/a/b", tx=tx1)["lock_id"]
        assert lock_id1 != "0-0-0-0"

        remove("//tmp/a", tx=tx1)

        tx2 = start_transaction()
        lock_id2 = lock("//tmp/a/b", tx=tx2, waitable=True)["lock_id"]
        assert get("#" + lock_id2 + "/@state") == "pending"

        commit_transaction(tx1)

        # Make sure commit reaches the node host cell.
        multicell_sleep()
        # Transaction object must be destroyed.
        gc_collect()
        # Removed node must be destroyed, the lock must become orphaned.
        gc_collect()

        assert not exists("//sys/locks/" + lock_id1)
        assert not exists("//sys/locks/" + lock_id2)

    @authors("babenko", "ignat")
    def test_waitable_lock8(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        tx3 = start_transaction()

        create("table", "//tmp/t")
        write_table("//tmp/t", {"foo": "bar"}, tx=tx2)

        lock_id = lock("//tmp/t", tx=tx3, mode="exclusive", waitable=True)["lock_id"]

        assert get("//sys/locks/" + lock_id + "/@state") == "pending"
        assert len(get("//tmp/t/@locks")) == 2

        commit_transaction(tx2)

        assert get("//sys/locks/" + lock_id + "/@state") == "pending"
        assert len(get("//tmp/t/@locks")) == 2

        commit_transaction(tx1)

        assert get("//sys/locks/" + lock_id + "/@state") == "acquired"
        assert len(get("//tmp/t/@locks")) == 1

    @authors("babenko", "ignat")
    def test_waitable_lock9(self):
        tx1 = start_transaction()
        tx2 = start_transaction()
        tx3 = start_transaction()

        create("table", "//tmp/t")

        lock_id1 = lock("//tmp/t", tx=tx1, mode="exclusive")["lock_id"]
        assert get("//sys/locks/" + lock_id1 + "/@state") == "acquired"

        lock_id2 = lock("//tmp/t", tx=tx2, mode="exclusive", waitable=True)["lock_id"]
        assert get("//sys/locks/" + lock_id2 + "/@state") == "pending"

        lock_id3 = lock("//tmp/t", tx=tx3, mode="snapshot")["lock_id"]
        assert get("//sys/locks/" + lock_id3 + "/@state") == "acquired"

    @authors("babenko")
    def test_waitable_lock10(self):
        create("table", "//tmp/t")
        tx1 = start_transaction()
        tx2 = start_transaction()
        lock("//tmp/t", tx=tx1, mode="exclusive")
        lock_id2 = lock("//tmp/t", tx=tx2, mode="exclusive", waitable=True)["lock_id"]
        assert get("//sys/locks/" + lock_id2 + "/@state") == "pending"
        write_table("<append=true>//tmp/t", {"a": "b"}, tx=tx1)

    @authors("babenko")
    def test_waitable_lock11(self):
        create("table", "//tmp/t")
        tx1 = start_transaction()
        tx2 = start_transaction()
        tx3 = start_transaction()
        lock_id1 = lock("//tmp/t", tx=tx1, mode="shared", child_key="a")["lock_id"]
        lock_id2 = lock("//tmp/t", tx=tx2, mode="shared", child_key="a", waitable=True)["lock_id"]
        lock_id3 = lock("//tmp/t", tx=tx3, mode="shared", child_key="b", waitable=True)["lock_id"]
        assert get("#" + lock_id1 + "/@state") == "acquired"
        assert get("#" + lock_id2 + "/@state") == "pending"
        assert get("#" + lock_id3 + "/@state") == "acquired"

    @authors("babenko")
    def test_waitable_lock12(self):
        create("table", "//tmp/t")
        tx1 = start_transaction()
        tx2 = start_transaction()
        tx3 = start_transaction()
        tx4 = start_transaction()
        lock_id1 = lock("//tmp/t", tx=tx1, mode="shared", child_key="a")["lock_id"]
        lock_id2 = lock("//tmp/t", tx=tx2, mode="shared", child_key="b")["lock_id"]
        lock_id3 = lock("//tmp/t", tx=tx3, mode="shared", child_key="a", waitable=True)["lock_id"]
        lock_id4 = lock("//tmp/t", tx=tx4, mode="shared", child_key="b", waitable=True)["lock_id"]
        assert get("#" + lock_id1 + "/@state") == "acquired"
        assert get("#" + lock_id2 + "/@state") == "acquired"
        assert get("#" + lock_id3 + "/@state") == "pending"
        assert get("#" + lock_id4 + "/@state") == "pending"
        abort_transaction(tx2)
        assert get("#" + lock_id1 + "/@state") == "acquired"
        assert get("#" + lock_id3 + "/@state") == "pending"
        assert get("#" + lock_id4 + "/@state") == "acquired"

    @authors("babenko")
    def test_waitable_lock13(self):
        create("table", "//tmp/t")

        tx1 = start_transaction()
        tx2 = start_transaction()
        tx3 = start_transaction()

        lock_id1 = lock("//tmp/t", tx=tx1)["lock_id"]
        lock_id2 = lock("//tmp/t", tx=tx2, waitable=True)["lock_id"]

        assert get("#" + lock_id1 + "/@state") == "acquired"
        assert get("#" + lock_id2 + "/@state") == "pending"

        write_table("//tmp/t", {"foo": "bar1"}, tx=tx1)
        commit_transaction(tx1)

        assert get("#" + lock_id2 + "/@state") == "acquired"

        lock_id3 = lock("//tmp/t", tx=tx3, waitable=True)["lock_id"]
        assert get("#" + lock_id3 + "/@state") == "pending"

        write_table("<append=true>//tmp/t", {"foo": "bar2"}, tx=tx2)
        commit_transaction(tx2)

        assert read_table("//tmp/t") == [{"foo": "bar1"}, {"foo": "bar2"}]

    @authors("babenko")
    def test_waitable_lock_14(self):
        create("table", "//tmp/t")

        tx1 = start_transaction()
        tx2 = start_transaction()

        lock_id1 = lock("//tmp/t", tx=tx1)["lock_id"]
        lock_id2 = lock("//tmp/t", tx=tx2, waitable=True)["lock_id"]

        assert get("#" + lock_id1 + "/@state") == "acquired"
        assert get("#" + lock_id2 + "/@state") == "pending"

        abort_transaction(tx2)
        assert not exists("#" + lock_id2)

    @authors("babenko")
    def test_waitable_lock_15(self):
        create("table", "//tmp/t")

        tx1 = start_transaction()
        tx2 = start_transaction()
        tx3 = start_transaction(tx=tx2)

        lock_id1 = lock("//tmp/t", tx=tx1)["lock_id"]
        lock_id2 = lock("//tmp/t", tx=tx3, waitable=True)["lock_id"]

        assert get("#" + lock_id1 + "/@state") == "acquired"
        assert get("#" + lock_id2 + "/@state") == "pending"

        commit_transaction(tx3)
        assert get("#" + lock_id2 + "/@state") == "pending"
        assert get("#" + lock_id2 + "/@transaction_id") == tx2

    @authors("babenko", "ignat")
    def test_yt_144(self):
        create("table", "//tmp/t")

        tx1 = start_transaction()
        lock("//tmp/t", tx=tx1, mode="exclusive")

        tx2 = start_transaction()
        lock_id = lock("//tmp/t", mode="exclusive", waitable=True, tx=tx2)["lock_id"]

        abort_transaction(tx1)
        assert get("//sys/locks/" + lock_id + "/@state") == "acquired"
        remove("//tmp/t", tx=tx2)
        abort_transaction(tx2)

        assert get("//tmp/t/@parent_id") == get("//tmp/@id")

    @authors("babenko", "ignat")
    def test_remove_locks(self):
        set("//tmp/a", {"b": 1})

        tx1 = start_transaction()
        tx2 = start_transaction()

        set("//tmp/a/b", 2, tx=tx1)
        with raises_yt_error("Cannot take \"exclusive\" lock for node //tmp/a/b"
                             " since \"exclusive\" lock is taken by concurrent "
                             f"transaction {tx1}"):
            remove("//tmp/a", tx=tx2)

    @authors("babenko", "ignat")
    def test_map_locks1(self):
        tx = start_transaction()
        set("//tmp/a", 1, tx=tx)
        assert get("//tmp/@lock_mode") == "none"
        assert get("//tmp/@lock_mode", tx=tx) == "shared"

        locks = get("//tmp/@locks", tx=tx)
        assert len(locks) == 1

        lock = locks[0]
        assert lock["mode"] == "shared"
        assert lock["child_key"] == "a"

        commit_transaction(tx)
        assert get("//tmp") == {"a": 1}

    @authors("babenko", "ignat")
    def test_map_locks2(self):
        tx1 = start_transaction()
        set("//tmp/a", 1, tx=tx1)

        tx2 = start_transaction()
        set("//tmp/b", 2, tx=tx2)

        assert get("//tmp", tx=tx1) == {"a": 1}
        assert get("//tmp", tx=tx2) == {"b": 2}
        assert get("//tmp") == {}

        commit_transaction(tx1)
        assert get("//tmp") == {"a": 1}
        assert get("//tmp", tx=tx2) == {"a": 1, "b": 2}

        commit_transaction(tx2)
        assert get("//tmp") == {"a": 1, "b": 2}

    @authors("babenko")
    def test_map_locks3(self):
        tx1 = start_transaction()
        set("//tmp/a", 1, tx=tx1)

        tx2 = start_transaction()
        with raises_yt_error("Cannot take lock for child \"a\" of node //tmp since"
                             f" this child is locked by concurrent transaction {tx1}"):
            set("//tmp/a", 2, tx=tx2)

    @authors("babenko", "ignat")
    def test_map_locks4(self):
        set("//tmp/a", 1)

        tx = start_transaction()
        remove("//tmp/a", tx=tx)

        assert get("//tmp/@lock_mode", tx=tx) == "shared"

        locks = get("//tmp/@locks", tx=tx)
        assert len(locks) == 1

        lock = locks[0]
        assert lock["mode"] == "shared"
        assert lock["child_key"] == "a"

    @authors("babenko", "ignat")
    def test_map_locks5(self):
        set("//tmp/a", 1)

        tx1 = start_transaction()
        remove("//tmp/a", tx=tx1)

        tx2 = start_transaction()
        with raises_yt_error("Cannot take \"exclusive\" lock for node //tmp/a "
                             "since \"exclusive\" lock is taken by concurrent "
                             f"transaction {tx1}"):
            set("//tmp/a", 2, tx=tx2)

    @authors("babenko", "ignat")
    def test_map_locks6(self):
        tx = start_transaction()
        set("//tmp/a", 1, tx=tx)
        assert get("//tmp/a", tx=tx) == 1
        assert get("//tmp") == {}

        with raises_yt_error("Node //tmp has no child with key \"a\""):
            remove("//tmp/a")
        remove("//tmp/a", tx=tx)
        assert get("//tmp", tx=tx) == {}

        commit_transaction(tx)
        assert get("//tmp") == {}

    @authors("babenko", "ignat")
    def test_map_locks7(self):
        set("//tmp/a", 1)

        tx = start_transaction()
        remove("//tmp/a", tx=tx)
        set("//tmp/a", 2, tx=tx)
        remove("//tmp/a", tx=tx)
        commit_transaction(tx)

        assert get("//tmp") == {}

    @authors("ignat")
    def test_map_locks8(self):
        tx = start_transaction()
        lock("//tmp", mode="shared", tx=tx, child_key="a")

        with raises_yt_error("Cannot take lock for child \"a\" of node //tmp since this child is "
                             f"locked by concurrent transaction {tx}"):
            set("//tmp/a", 1)

        other_tx = start_transaction()
        with raises_yt_error("Cannot take lock for child \"a\" of node //tmp since this child is "
                             f"locked by concurrent transaction {tx}"):
            lock("//tmp", mode="shared", tx=other_tx, child_key="a")

    @authors("shakurov")
    @cannot_be_implemented_in_sequoia("map-node snapshots are forbidden")
    def test_map_snapshot_lock(self):
        create("map_node", "//tmp/a")

        create("map_node", "//tmp/a/m")
        create("string_node", "//tmp/a/s")
        doc_id = create("document", "//tmp/a/d")

        tx = start_transaction()
        lock("//tmp/a", mode="snapshot", tx=tx)

        remove("//tmp/a/d")

        assert not exists("//tmp/a/d")
        assert exists("//tmp/a/d", tx=tx)

        create("document", "//tmp/a/d")

        assert get("//tmp/a/d/@id") != get("//tmp/a/d/@id", tx=tx)
        assert get("//tmp/a/d/@id", tx=tx) == doc_id

        set("//sys/@config/cypress_manager/forbid_list_node_creation", False)
        create("list_node", "//tmp/a/l")

        assert exists("//tmp/a/l")
        assert not exists("//tmp/a/l", tx=tx)

    @authors("babenko", "ignat")
    def test_attr_locks1(self):
        tx = start_transaction()
        set("//tmp/@a", 1, tx=tx)
        assert get("//tmp/@lock_mode") == "none"
        assert get("//tmp/@lock_mode", tx=tx) == "shared"

        locks = get("//tmp/@locks", tx=tx)
        assert len(locks) == 1

        lock = locks[0]
        assert lock["mode"] == "shared"
        assert lock["attribute_key"] == "a"

        commit_transaction(tx)
        assert get("//tmp/@a") == 1

    @authors("babenko", "ignat")
    def test_attr_locks2(self):
        tx1 = start_transaction()
        set("//tmp/@a", 1, tx=tx1)

        tx2 = start_transaction()
        set("//tmp/@b", 2, tx=tx2)

        assert get("//tmp/@a", tx=tx1) == 1
        assert get("//tmp/@b", tx=tx2) == 2
        with raises_yt_error("Attribute \"a\" is not found"):
            get("//tmp/@a")
        with raises_yt_error("Attribute \"b\" is not found"):
            get("//tmp/@b")

        commit_transaction(tx1)
        assert get("//tmp/@a") == 1
        assert get("//tmp/@a", tx=tx2) == 1
        assert get("//tmp/@b", tx=tx2) == 2

        commit_transaction(tx2)
        assert get("//tmp/@a") == 1
        assert get("//tmp/@b") == 2

    @authors("babenko")
    def test_attr_locks3(self):
        tx1 = start_transaction()
        set("//tmp/@a", 1, tx=tx1)

        tx2 = start_transaction()
        with raises_yt_error("Cannot take lock for attribute \"a\" of node //tmp since this "
                             f"attribute is locked by concurrent transaction {tx1}"):
            set("//tmp/@a", 2, tx=tx2)

    @authors("babenko", "ignat")
    def test_attr_locks4(self):
        set("//tmp/@a", 1)

        tx = start_transaction()
        remove("//tmp/@a", tx=tx)

        assert get("//tmp/@lock_mode", tx=tx) == "shared"

        locks = get("//tmp/@locks", tx=tx)
        assert len(locks) == 1

        lock = locks[0]
        assert lock["mode"] == "shared"
        assert lock["attribute_key"] == "a"

    @authors("babenko", "ignat")
    def test_attr_locks5(self):
        set("//tmp/@a", 1)

        tx1 = start_transaction()
        remove("//tmp/@a", tx=tx1)

        tx2 = start_transaction()
        with raises_yt_error("Cannot take lock for attribute \"a\" of node //tmp since this "
                             f"attribute is locked by concurrent transaction {tx1}"):
            set("//tmp/@a", 2, tx=tx2)

    @authors("babenko", "ignat")
    def test_attr_locks6(self):
        tx = start_transaction()
        set("//tmp/@a", 1, tx=tx)
        assert get("//tmp/@a", tx=tx) == 1
        with raises_yt_error("Attribute \"a\" is not found"):
            get("//tmp/@a")

        with raises_yt_error("Attribute \"a\" is not found"):
            remove("//tmp/@a")
        remove("//tmp/@a", tx=tx)
        with raises_yt_error("Attribute \"a\" is not found"):
            get("//tmp/@a", tx=tx)

        commit_transaction(tx)
        with raises_yt_error("Attribute \"a\" is not found"):
            get("//tmp/@a")

    @authors("ignat")
    def test_attr_locks7(self):
        tx = start_transaction()
        lock("//tmp", mode="shared", tx=tx, attribute_key="a")

        with raises_yt_error("Cannot take lock for attribute \"a\" of node //tmp since this "
                             f"attribute is locked by concurrent transaction {tx}"):
            set("//tmp/@a", 1)

        other_tx = start_transaction()
        with raises_yt_error("Cannot take lock for attribute \"a\" of node //tmp since this "
                             f"attribute is locked by concurrent transaction {tx}"):
            lock("//tmp", mode="shared", tx=other_tx, attribute_key="a")

    @authors("sandello")
    def test_lock_mode_for_child_and_attr_locks(self):
        tx = start_transaction()
        with raises_yt_error("\"child_key\" can only be specified for shared locks"):
            lock("//tmp", mode="exclusive", tx=tx, child_key="a")
        with raises_yt_error("\"attribute_key\" can only be specified for shared locks"):
            lock("//tmp", mode="exclusive", tx=tx, attribute_key="a")

    @authors("babenko")
    def test_nested_tx1(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        lock("//tmp", tx=tx2)
        assert len(get("//tmp/@locks")) == 1
        abort_transaction(tx2)
        assert len(get("//tmp/@locks")) == 0

    @authors("babenko", "ignat")
    def test_nested_tx2(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        lock_id = lock("//tmp", tx=tx2)["lock_id"]
        assert len(get("//tmp/@locks")) == 1
        commit_transaction(tx2)
        assert len(get("//tmp/@locks")) == 1
        assert get("//sys/locks/" + lock_id + "/@transaction_id") == tx1

    @authors("babenko", "ignat")
    def test_nested_tx3(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        lock_id = lock("//tmp", tx=tx2, mode="snapshot")["lock_id"]
        assert len(get("//tmp/@locks")) == 1
        commit_transaction(tx2)
        assert not exists("//sys/locks/" + lock_id)

    @authors("babenko", "ignat")
    def test_nested_tx4(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        lock("//tmp", tx=tx1)
        lock("//tmp", tx=tx2)
        with raises_yt_error("Cannot take \"exclusive\" lock for node //tmp since \"exclusive\" "
                             f"lock is taken by concurrent transaction {tx2}"):
            lock("//tmp", tx=tx1)

    @authors("babenko", "ignat")
    def test_nested_tx5(self):
        get("//sys/clusters", driver=get_driver(cluster="primary"))
        get("//sys/clusters", driver=get_driver(cluster="primary_ground"))

        set("//tmp/x", 1)
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        set("//tmp/x", 2, tx=tx1)
        set("//tmp/x", 3, tx=tx2)
        with raises_yt_error("Cannot take \"exclusive\" lock for node //tmp/x since \"exclusive\" "
                             f"lock is taken by concurrent transaction {tx2}"):
            set("//tmp/x", 4, tx=tx1)

    @authors("babenko")
    def test_manual_lock_not_recursive1(self):
        set("//tmp/x", {"y": {}})
        tx = start_transaction()
        lock("//tmp/x", tx=tx, mode="shared", child_key="a")
        assert len(get("//tmp/x/@locks")) == 1
        assert len(get("//tmp/x/y/@locks")) == 0

    @authors("babenko")
    def test_manual_lock_not_recursive2(self):
        set("//tmp/x", {"y": {}})
        tx1 = start_transaction()
        tx2 = start_transaction()
        lock("//tmp/x/y", tx=tx1, mode="shared", child_key="a")
        lock("//tmp/x", tx=tx2, mode="shared", child_key="a")

    @authors("shakurov")
    def test_forked_tx_abort1(self):
        create("table", "//tmp/t1")
        tx = start_transaction()
        tx1 = start_transaction(tx=tx)
        tx2 = start_transaction(tx=tx)
        tx3 = start_transaction(tx=tx2)

        lock("//tmp/t1", tx=tx1, mode="shared", child_key="a")
        lock("//tmp/t1", tx=tx2, mode="shared", child_key="b")
        lock("//tmp/t1", tx=tx3, mode="snapshot")

        # Must not crash.
        abort_transaction(tx)

    @authors("shakurov")
    def test_forked_tx_abort2(self):
        create("table", "//tmp/t1")
        tx = start_transaction()
        tx1 = start_transaction(tx=tx)
        tx2 = start_transaction(tx=tx)
        tx3 = start_transaction(tx=tx2)

        lock("//tmp/t1", tx=tx1, mode="shared", child_key="a")
        lock("//tmp/t1", tx=tx2, mode="shared", child_key="b")
        lock("//tmp/t1", tx=tx3, mode="snapshot")

        # Must not crash.
        abort_transaction(tx1)
        abort_transaction(tx)

    @authors("babenko")
    def test_lock_times(self):
        set("//tmp/a", 1)

        tx1 = start_transaction()
        lock_id1 = lock("//tmp/a", tx=tx1)["lock_id"]

        assert parse_yt_time(get("#{}/@creation_time".format(lock_id1))) < get_current_time()
        assert get("#{}/@creation_time".format(lock_id1)) == get("#{}/@acquisition_time".format(lock_id1))

        tx2 = start_transaction()
        lock_id2 = lock("//tmp/a", tx=tx2, waitable=True)["lock_id"]

        assert not exists("#{}/@acquisition_time".format(lock_id2))
        assert get("#" + lock_id1 + "/@state") == "acquired"
        assert get("//tmp/a/@lock_mode", tx=tx1) == "exclusive"
        assert get("#" + lock_id2 + "/@state") == "pending"
        assert parse_yt_time(get("#{}/@creation_time".format(lock_id2))) < get_current_time()
        assert parse_yt_time(get("#{}/@creation_time".format(lock_id1))) < parse_yt_time(
            get("#{}/@creation_time".format(lock_id2))
        )

        abort_transaction(tx1)

        assert not exists("//sys/locks/" + lock_id1)
        assert get("#" + lock_id2 + "/@state") == "acquired"
        assert get("//tmp/a/@lock_mode", tx=tx2) == "exclusive"
        assert exists("#{}/@acquisition_time".format(lock_id2))
        assert parse_yt_time(get("#{}/@acquisition_time".format(lock_id2))) < get_current_time()

    @authors("h0pless")
    def test_lock_count_limit(self):
        set("//sys/@config/cypress_manager/max_locks_per_transaction_subtree", 2)
        create("table", "//tmp/table")
        table_native_cell_tag = get("//tmp/table/@native_cell_tag")

        tx1 = start_transaction()
        lock("//tmp/table", mode="shared", tx=tx1)["lock_id"]
        assert get("#{}/@recursive_lock_count".format(tx1))[str(table_native_cell_tag)] == 1

        tx2 = start_transaction()
        lock("//tmp/table", mode="shared", tx=tx2)["lock_id"]
        assert get("#{}/@recursive_lock_count".format(tx2))[str(table_native_cell_tag)] == 1

        tx11 = start_transaction(tx=tx1)
        lock("//tmp/table", mode="shared", tx=tx11)["lock_id"]
        assert get("#{}/@recursive_lock_count".format(tx11))[str(table_native_cell_tag)] == 1
        assert get("#{}/@recursive_lock_count".format(tx2))[str(table_native_cell_tag)] == 1
        assert get("#{}/@recursive_lock_count".format(tx1))[str(table_native_cell_tag)] == 2

        tx12 = start_transaction(tx=tx1)
        with pytest.raises(YtError,
                           match="Cannot create \"shared\" lock for node .* since "
                           "transaction .* its descendants already have . locks associated with them"):
            lock("//tmp/table", mode="shared", tx=tx12)

        unlock("//tmp/table", tx=tx11)
        assert get("#{}/@recursive_lock_count".format(tx1))[str(table_native_cell_tag)] == 1

        lock("//tmp/table", mode="shared", tx=tx11)
        assert get("#{}/@recursive_lock_count".format(tx11))[str(table_native_cell_tag)] == 1
        assert get("#{}/@recursive_lock_count".format(tx1))[str(table_native_cell_tag)] == 2

        commit_transaction(tx=tx11)
        get("#{}/@recursive_lock_count".format(tx1))
        assert get("#{}/@recursive_lock_count".format(tx1))[str(table_native_cell_tag)] == 2

        with pytest.raises(YtError,
                           match="Cannot create \"shared\" lock for node .* since "
                           "transaction .* its descendants already have . locks associated with them"):
            lock("//tmp/table", mode="shared", tx=tx12)

    @authors("danilalexeev")
    @not_implemented_in_sequoia
    def test_map_node_copy_on_write(self):
        create("map_node", "//tmp/m")
        tx = start_transaction()

        lock("//tmp/m", mode="shared", tx=tx)

        assert get("//tmp/m/@cow_cookie") == get("//tmp/m/@cow_cookie", tx=tx)

        create("table", "//tmp/m/t", tx=tx)

        assert get("//tmp/m/@cow_cookie") != get("//tmp/m/@cow_cookie", tx=tx)


##################################################################


@pytest.mark.enabled_multidaemon
class TestCypressLocksMulticell(TestCypressLocks):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2


@pytest.mark.enabled_multidaemon
class TestCypressLocksRpcProxy(TestCypressLocks):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


@pytest.mark.enabled_multidaemon
class TestCypressLocksMulticellRpcProxy(TestCypressLocksMulticell, TestCypressLocksRpcProxy):
    ENABLE_MULTIDAEMON = True


##################################################################


@pytest.mark.enabled_multidaemon
class TestCypressLocksShardedTx(TestCypressLocksMulticell):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 4
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "13": {"roles": ["transaction_coordinator"]},
        "14": {"roles": ["transaction_coordinator"]},
    }


@pytest.mark.enabled_multidaemon
class TestCypressLocksShardedTxCTxS(TestCypressLocksShardedTx):
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
class TestCypressLocksMirroredTx(TestCypressLocksShardedTxCTxS):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = False
    NUM_CYPRESS_PROXIES = 1
    NUM_TEST_PARTITIONS = 8

    _SUPRESSED_MESSAGES = [
        "Retrieving TVM service ticket",
        "Polling peer",
        "Waiting for bus to become ready",
        "Created bus",
        "Bus has become ready",
        "Request is dropped because channel is terminated",
        "Sleeping before peer polling",
        "Failed to poll peer",
        "Parsing service ticket",
    ]

    DELTA_DRIVER_CONFIG = {
        "logging": {
            "suppressed_messages": _SUPRESSED_MESSAGES,
        },
    }

    DELTA_DRIVER_LOGGING_CONFIG = {
        "suppressed_messages": _SUPRESSED_MESSAGES,
    }

    DELTA_RPC_DRIVER_CONFIG = {
        "logging": {
            "suppressed_messages": _SUPRESSED_MESSAGES,
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "commit_operation_cypress_node_changes_via_system_transaction": True,
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "transaction_manager": {
            "forbid_transaction_actions_for_cypress_transactions": True,
        },
        "cell_master": {
            "logging": {
                "suppressed_messages": _SUPRESSED_MESSAGES,
            },
        },
    }


##################################################################


@pytest.mark.enabled_multidaemon
class TestCypressLocksInSequoia(TestCypressLocksMirroredTx):
    ENABLE_MULTIDAEMON = True
    ENABLE_TMP_ROOTSTOCK = True
    NUM_CYPRESS_PROXIES = 1
    NUM_TEST_PARTITIONS = 12
    NUM_SECONDARY_MASTER_CELLS = 4

    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["sequoia_node_host"]},
        # Master cell with tag 11 is reserved for portals.
        "11": {"roles": ["cypress_node_host"]},
        "12": {"roles": ["transaction_coordinator", "sequoia_node_host"]},
        "13": {"roles": ["transaction_coordinator"]},
        "14": {"roles": ["chunk_host"]},
    }

    # NB: of course, it's better to test the default configuration but it's
    # too slow (+150ms per request).
    DELTA_CYPRESS_PROXY_DYNAMIC_CONFIG = {
        "object_service": {
            "allow_bypass_master_resolve": True,
        },
    }
