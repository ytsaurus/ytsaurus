from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, ls, issue_lease, revoke_lease, reference_lease, unreference_lease, insert_rows, select_rows,
    create, get, set, exists, wait, remove, sync_mount_table, sync_create_cells,
    sync_unmount_table, raises_yt_error, start_transaction, commit_transaction, abort_transaction,
    sync_reshard_table, mount_table, wait_for_tablet_state,
)

from yt.test_helpers import (
    assert_items_equal,
)

from yt.common import YtError

import pytest

import time

##################################################################


class TestDynamicTablesLeases(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    USE_DYNAMIC_TABLES = True

    LEASE_ID = "1-2-3-4"

    def _get_leases(self, cell_id):
        node = get(f"#{cell_id}/@peers/0/address")
        return get(f"//sys/cluster_nodes/{node}/orchid/tablet_cells/{cell_id}/lease_manager/leases")

    def _check_lease(self, cell_id, state, prc, trc, lease_id=LEASE_ID):
        leases = self._get_leases(cell_id)
        assert len(leases) == 1
        assert lease_id in leases
        lease = leases[lease_id]
        assert lease["state"] == state
        assert lease["persistent_ref_counter"] == prc
        assert lease["transient_ref_counter"] == trc

    def _restart_cell(self):
        cell_id = ls("//sys/tablet_cells")[0]
        tx_id = get(f"#{cell_id}/@prerequisite_transaction_id")
        abort_transaction(tx_id)
        wait(lambda: exists(f"#{cell_id}/@prerequisite_transaction_id"))
        wait(lambda: get(f"#{cell_id}/@peers/0/state") == "leading")

    def _create_table(self, atomicity="full"):
        create("table", "//tmp/t", attributes={
            "dynamic": True,
            "atomicity": atomicity,
            "schema": [
                {"name": "k", "type": "string", "sort_order": "ascending"},
                {"name": "v", "type": "string"},
            ]
        })
        sync_mount_table("//tmp/t")

    def _create_lease(self, cell_id, parent_tx="0-0-0-0", prerequisite_txs=[],
                      timeout=10000, atomicity="full"):
        self._create_table(atomicity=atomicity)

        tx = start_transaction(type="master",
                               tx=parent_tx,
                               prerequisite_transaction_ids=prerequisite_txs,
                               timeout=timeout)
        self._write_with_prerequisite(lease_id=tx, atomicity=atomicity)
        self._check_lease(cell_id, "active", 0, 0, lease_id=tx)

        assert get(f"#{tx}/@lease_cell_ids") == [cell_id]
        assert get(f"#{tx}/@successor_transaction_lease_count") == 1
        assert get(f"#{cell_id}/@lease_transaction_ids") == [tx]

        return tx

    def _write_with_prerequisite(self, lease_id=LEASE_ID, key="a", atomicity="full"):
        tx = start_transaction(type="tablet", atomicity=atomicity)
        insert_rows("//tmp/t", [{"k": key, "v": "v"}], tx=tx)
        commit_transaction(tx, prerequisite_transaction_ids=[lease_id])

    @authors("gritukan")
    @pytest.mark.parametrize("mode", ["commit", "abort"])
    @pytest.mark.parametrize("atomicity", ["full", "none"])
    def test_simple(self, mode, atomicity):
        cell_id = sync_create_cells(1)[0]
        tx = self._create_lease(cell_id, atomicity=atomicity)

        self._write_with_prerequisite(lease_id=tx, key="foo", atomicity=atomicity)
        create("map_node", "//tmp/m", tx=tx)
        self._check_lease(cell_id, "active", 0, 0, lease_id=tx)

        if mode == "commit":
            commit_transaction(tx)
            assert exists("//tmp/m")
        else:
            abort_transaction(tx)
            assert not exists("//tmp/m")

        with raises_yt_error("Prerequisite check failed"):
            self._write_with_prerequisite(lease_id=tx, key="bar", atomicity=atomicity)

        assert select_rows("* from [//tmp/t]") == \
            [
                {"k": "a", "v": "v"},
                {"k": "foo", "v": "v"},
            ]

    @authors("gritukan")
    def test_reference_lease(self):
        cell_id = sync_create_cells(1)[0]

        issue_lease(cell_id, self.LEASE_ID)
        self._check_lease(cell_id, "active", 0, 0)

        reference_lease(cell_id, self.LEASE_ID, persistent=True)
        self._check_lease(cell_id, "active", 1, 0)

        reference_lease(cell_id, self.LEASE_ID, persistent=False)
        self._check_lease(cell_id, "active", 1, 1)

        unreference_lease(cell_id, self.LEASE_ID, persistent=True)
        self._check_lease(cell_id, "active", 0, 1)

        unreference_lease(cell_id, self.LEASE_ID, persistent=False)
        self._check_lease(cell_id, "active", 0, 0)

        revoke_lease(cell_id, self.LEASE_ID)
        wait(lambda: len(self._get_leases(cell_id)) == 0)

    @authors("gritukan")
    @pytest.mark.parametrize("mode", ["persistent", "transient"])
    def test_revoking_lease(self, mode):
        cell_id = sync_create_cells(1)[0]

        issue_lease(cell_id, self.LEASE_ID)
        self._check_lease(cell_id, "active", 0, 0)

        persistent = ("mode" == "persistent")
        reference_lease(cell_id, self.LEASE_ID, persistent=persistent)

        if persistent:
            prc, trc = 1, 0
        else:
            prc, trc = 0, 1
        self._check_lease(cell_id, "active", prc, trc)

        revoke_lease(cell_id, self.LEASE_ID)
        self._check_lease(cell_id, "revoking", prc, trc)

        with pytest.raises(YtError):
            reference_lease(cell_id, self.LEASE_ID, persistent=True)
        with pytest.raises(YtError):
            reference_lease(cell_id, self.LEASE_ID, persistent=False)
        self._check_lease(cell_id, "revoking", prc, trc)

        reference_lease(cell_id, self.LEASE_ID, persistent=True, force=True)
        self._check_lease(cell_id, "revoking", prc + 1, trc)

        reference_lease(cell_id, self.LEASE_ID, persistent=False, force=True)
        self._check_lease(cell_id, "revoking", prc + 1, trc + 1)

        unreference_lease(cell_id, self.LEASE_ID, persistent=True)
        unreference_lease(cell_id, self.LEASE_ID, persistent=False)
        unreference_lease(cell_id, self.LEASE_ID, persistent=persistent)
        wait(lambda: len(self._get_leases(cell_id)) == 0)

    @authors("gritukan")
    def test_reset_transient_reference_on_cell_restart(self):
        cell_id = sync_create_cells(1)[0]

        issue_lease(cell_id, self.LEASE_ID)
        self._check_lease(cell_id, "active", 0, 0)

        reference_lease(cell_id, self.LEASE_ID, persistent=False)
        self._check_lease(cell_id, "active", 0, 1)

        self._restart_cell()
        self._check_lease(cell_id, "active", 0, 0)

    @authors("gritukan")
    def test_remove_transiently_referenced_lease_on_cell_restart(self):
        cell_id = sync_create_cells(1)[0]

        issue_lease(cell_id, self.LEASE_ID)
        self._check_lease(cell_id, "active", 0, 0)

        reference_lease(cell_id, self.LEASE_ID, persistent=False)
        self._check_lease(cell_id, "active", 0, 1)

        revoke_lease(cell_id, self.LEASE_ID)
        self._check_lease(cell_id, "revoking", 0, 1)

        self._restart_cell()
        wait(lambda: len(self._get_leases(cell_id)) == 0)

    @authors("gritukan")
    @pytest.mark.parametrize("mode", ["persistent", "transient"])
    def test_force_revoke(self, mode):
        cell_id = sync_create_cells(1)[0]

        issue_lease(cell_id, self.LEASE_ID)
        self._check_lease(cell_id, "active", 0, 0)

        persistent = mode == "persistent"
        reference_lease(cell_id, self.LEASE_ID, persistent=persistent)

        if persistent:
            prc, trc = 1, 0
        else:
            prc, trc = 0, 1
        self._check_lease(cell_id, "active", prc, trc)

        revoke_lease(cell_id, self.LEASE_ID, force=True)
        wait(lambda: len(self._get_leases(cell_id)) == 0)

    @authors("gritukan")
    @pytest.mark.parametrize("mode", ["persistent", "transient"])
    def test_force_revoke_after_revoke(self, mode):
        cell_id = sync_create_cells(1)[0]

        issue_lease(cell_id, self.LEASE_ID)
        self._check_lease(cell_id, "active", 0, 0)

        persistent = mode == "persistent"
        reference_lease(cell_id, self.LEASE_ID, persistent=persistent)

        if persistent:
            prc, trc = 1, 0
        else:
            prc, trc = 0, 1
        self._check_lease(cell_id, "active", prc, trc)

        revoke_lease(cell_id, self.LEASE_ID, force=False)
        self._check_lease(cell_id, "revoking", prc, trc)

        revoke_lease(cell_id, self.LEASE_ID, force=True)
        wait(lambda: len(self._get_leases(cell_id)) == 0)

    @authors("gritukan")
    def test_write_with_prerequisite_lease(self):
        cell_id = sync_create_cells(1)[0]
        self._create_table()
        issue_lease(cell_id, self.LEASE_ID)
        self._check_lease(cell_id, "active", 0, 0)

        self._write_with_prerequisite()

        assert select_rows("* from [//tmp/t]") == [{"k": "a", "v": "v"}]

    @authors("gritukan")
    def test_write_with_prerequisite_lease_failed_1(self):
        sync_create_cells(1)
        self._create_table()

        with pytest.raises(YtError):
            self._write_with_prerequisite()

    @authors("gritukan")
    def test_write_with_prerequisite_lease_failed_2(self):
        cell_id = sync_create_cells(1)[0]
        self._create_table()
        issue_lease(cell_id, self.LEASE_ID)
        self._check_lease(cell_id, "active", 0, 0)

        reference_lease(cell_id, self.LEASE_ID)
        self._check_lease(cell_id, "active", 1, 0)

        revoke_lease(cell_id, self.LEASE_ID)
        self._check_lease(cell_id, "revoking", 1, 0)

        with raises_yt_error("Non-active lease cannot be referenced"):
            self._write_with_prerequisite()

    @authors("gritukan")
    @pytest.mark.parametrize("mode", ["commit", "abort"])
    def test_revoke_transaction_leases(self, mode):
        set("//sys/@config/transaction_manager/throw_on_lease_revocation", True)

        cell_id = sync_create_cells(1)[0]
        tx = self._create_lease(cell_id)

        reference_lease(cell_id, tx)
        self._check_lease(cell_id, "active", 1, 0, lease_id=tx)

        create("map_node", "//tmp/m", tx=tx)

        if mode == "commit":
            with raises_yt_error("Testing error"):
                commit_transaction(tx)
        else:
            with raises_yt_error("Testing error"):
                abort_transaction(tx)

        assert get(f"#{tx}/@state") == "active"
        assert get(f"#{tx}/@leases_state") == "revoking"
        wait(lambda: self._get_leases(cell_id)[tx]["state"] == "revoking")

        self._check_lease(cell_id, "revoking", 1, 0, lease_id=tx)

        unreference_lease(cell_id, tx)

        wait(lambda: get(f"#{tx}/@leases_state") == "revoked")
        assert get(f"#{tx}/@state") == "active"

        assert len(self._get_leases(cell_id)) == 0
        assert len(get(f"#{cell_id}/@lease_transaction_ids")) == 0

        if mode == "commit":
            commit_transaction(tx)
            assert exists("//tmp/m")
        else:
            abort_transaction(tx)
            assert not exists("//tmp/m")

    @authors("gritukan")
    @pytest.mark.parametrize("mode", ["active", "commit", "abort"])
    def test_abort_transaction_with_force(self, mode):
        set("//sys/@config/transaction_manager/throw_on_lease_revocation", True)

        cell_id = sync_create_cells(1)[0]
        tx = self._create_lease(cell_id)

        reference_lease(cell_id, tx)
        self._check_lease(cell_id, "active", 1, 0, lease_id=tx)

        create("map_node", "//tmp/m", tx=tx)

        if mode == "commit":
            with raises_yt_error("Testing error"):
                commit_transaction(tx)
        elif mode == "abort":
            with raises_yt_error("Testing error"):
                abort_transaction(tx)

        assert get(f"#{tx}/@state") == "active"

        if mode == "active":
            assert get(f"#{tx}/@leases_state") == "active"
            self._check_lease(cell_id, "active", 1, 0, lease_id=tx)
        else:
            assert get(f"#{tx}/@leases_state") == "revoking"
            wait(lambda: self._get_leases(cell_id)[tx]["state"] == "revoking")
            self._check_lease(cell_id, "revoking", 1, 0, lease_id=tx)

        abort_transaction(tx, force=True)
        assert not exists(f"#{tx}")
        assert not exists("//tmp/m")
        assert len(get(f"#{cell_id}/@lease_transaction_ids")) == 0

        wait(lambda: len(self._get_leases(cell_id)) == 0)

    @authors("gritukan")
    @pytest.mark.parametrize("mode", ["commit", "abort", "force_abort"])
    @pytest.mark.parametrize("action_tx_name", ["tx", "tx2", "tx3"])
    def test_revoke_lease_successor_transaction(self, mode, action_tx_name):
        set("//sys/@config/transaction_manager/throw_on_lease_revocation", True)

        cell_id = sync_create_cells(1)[0]

        # Legend:
        #  -, | -- ancestry relation
        #  ==   -- prerequisite relation
        #
        #    tx1==→tx2--→tx4
        #     |     |    |
        #     ↓     ↓    ↓
        #    tx3==→ltx==→tx5
        #           |    |
        #           ↓    ↓
        #          tx6==→tx7

        tx1 = start_transaction(type="master")
        tx2 = start_transaction(type="master", prerequisite_txs=[tx1])
        tx3 = start_transaction(type="master", tx=tx1)
        ltx = self._create_lease(cell_id, parent_tx=tx2, prerequisite_txs=[tx3])
        tx4 = start_transaction(type="master", tx=tx2)
        tx5 = start_transaction(type="master", tx=tx4, prerequisite_transaction_ids=[ltx])
        tx6 = start_transaction(type="master", tx=ltx)
        tx7 = start_transaction(type="master", tx=tx5, prerequisite_transaction_ids=[tx6])
        all_tx = [ltx, tx1, tx2, tx3, tx4, tx5, tx6, tx7]

        reference_lease(cell_id, ltx)
        self._check_lease(cell_id, "active", 1, 0, lease_id=ltx)

        assert get(f"#{ltx}/@lease_cell_ids") == [cell_id]
        for tx in [tx1, tx2, tx3, tx4, tx5, tx6, tx7]:
            assert len(get(f"#{tx}/@lease_cell_ids")) == 0
        for tx in [tx1, tx2, tx3, ltx]:
            assert get(f"#{tx}/@successor_transaction_lease_count") == 1
        for tx in [tx4, tx5, tx6, tx7]:
            assert get(f"#{tx}/@successor_transaction_lease_count") == 0

        create("map_node", "//tmp/m", tx=ltx)

        if action_tx_name == "tx":
            action_tx = ltx
            successor_transactions = [ltx, tx5, tx6, tx7]
        elif action_tx_name == "tx2":
            action_tx = tx2
            successor_transactions = [tx2, tx4, ltx, tx5, tx6, tx7]
        elif action_tx_name == "tx3":
            action_tx = tx3
            successor_transactions = [tx3, ltx, tx5, tx6, tx7]

        if mode == "commit":
            with raises_yt_error("Testing error"):
                commit_transaction(action_tx)
        elif mode == "abort":
            with raises_yt_error("Testing error"):
                abort_transaction(action_tx)
        else:
            abort_transaction(action_tx, force=True)

        if mode == "force_abort":
            assert len(get(f"#{cell_id}/@lease_transaction_ids")) == 0
            wait(lambda: len(self._get_leases(cell_id)) == 0)
        else:
            for tx in all_tx:
                assert get(f"#{tx}/@state") == "active"
                if tx in successor_transactions:
                    if tx in [tx4, tx5, tx6, tx7]:
                        assert get(f"#{tx}/@leases_state") == "revoked"
                    else:
                        assert get(f"#{tx}/@leases_state") == "revoking"
                else:
                    assert get(f"#{tx}/@leases_state") == "active"

            # Double revokation start should not break anything.
            with raises_yt_error("Testing error"):
                abort_transaction(ltx)

            wait(lambda: self._get_leases(cell_id)[ltx]["state"] == "revoking")
            self._check_lease(cell_id, "revoking", 1, 0, lease_id=ltx)

            unreference_lease(cell_id, ltx)
            wait(lambda: get(f"#{ltx}/@leases_state") == "revoked")

            for tx in all_tx:
                assert get(f"#{tx}/@state") == "active"
                if tx in successor_transactions:
                    assert get(f"#{tx}/@leases_state") == "revoked"
                else:
                    assert get(f"#{tx}/@leases_state") == "active"

            assert len(self._get_leases(cell_id)) == 0
            assert len(get(f"#{cell_id}/@lease_transaction_ids")) == 0

            if mode == "commit":
                commit_transaction(action_tx)
                if action_tx == ltx:
                    assert exists("//tmp/m", tx=tx2)
            else:
                abort_transaction(action_tx)

        for tx in all_tx:
            if tx in successor_transactions:
                assert not exists(f"#{tx}")
            else:
                assert exists(f"#{tx}")

    @authors("gritukan")
    def test_transaction_with_lease_timeout_1(self):
        cell_id = sync_create_cells(1)[0]
        tx = self._create_lease(cell_id, timeout=2000)
        self._check_lease(cell_id, "active", 0, 0, lease_id=tx)

        wait(lambda: not exists(f"#{tx}"))
        assert len(self._get_leases(cell_id)) == 0
        assert len(get(f"#{cell_id}/@lease_transaction_ids")) == 0

    @authors("gritukan")
    def test_transaction_with_lease_timeout_2(self):
        cell_id = sync_create_cells(1)[0]
        tx = self._create_lease(cell_id, timeout=2000)
        reference_lease(cell_id, tx)
        self._check_lease(cell_id, "active", 1, 0, lease_id=tx)

        time.sleep(4.0)
        assert exists(f"#{tx}")

        self._check_lease(cell_id, "revoking", 1, 0, lease_id=tx)
        unreference_lease(cell_id, tx)

        wait(lambda: not exists(f"#{tx}"))
        assert len(self._get_leases(cell_id)) == 0
        assert len(get(f"#{cell_id}/@lease_transaction_ids")) == 0

    @authors("gritukan")
    def test_remove_tablet_cell(self):
        cell_id = sync_create_cells(1)[0]
        tx = self._create_lease(cell_id)
        reference_lease(cell_id, tx)
        self._check_lease(cell_id, "active", 1, 0, lease_id=tx)

        sync_unmount_table("//tmp/t")

        remove(f"#{cell_id}")
        wait(lambda: self._get_leases(cell_id)[tx]["state"] == "revoking")
        self._check_lease(cell_id, "revoking", 1, 0, lease_id=tx)

        unreference_lease(cell_id, tx)
        wait(lambda: not exists(f"#{cell_id}"))

        assert len(get(f"#{tx}/@lease_cell_ids")) == 0
        assert get(f"#{tx}/@successor_transaction_lease_count") == 0

    @authors("gritukan")
    def test_force_remove_tablet_cell(self):
        cell_id = sync_create_cells(1)[0]
        tx = self._create_lease(cell_id)
        reference_lease(cell_id, tx)
        self._check_lease(cell_id, "active", 1, 0, lease_id=tx)

        sync_unmount_table("//tmp/t")

        remove(f"#{cell_id}", force=True)
        wait(lambda: not exists(f"#{cell_id}"))

        assert len(get(f"#{tx}/@lease_cell_ids")) == 0
        assert get(f"#{tx}/@successor_transaction_lease_count") == 0

    @authors("gritukan")
    def test_distributed_commit(self):
        cell_count = 5
        sync_create_cells(cell_count)
        cell_ids = ls("//sys/tablet_cells")
        create("table", "//tmp/t", attributes={
            "dynamic": True,
            "schema": [
                {"name": "k", "type": "int64", "sort_order": "ascending"},
                {"name": "v", "type": "string"},
            ]
        })
        sync_reshard_table("//tmp/t", [[]] + [[i * 100] for i in range(cell_count - 1)])
        for i in range(len(cell_ids)):
            mount_table(
                "//tmp/t",
                first_tablet_index=i,
                last_tablet_index=i,
                cell_id=cell_ids[i],
            )
        wait_for_tablet_state("//tmp/t", "mounted")
        rows = [{"k": i * 100 - j, "v": "payload" + str(i)} for i in range(cell_count) for j in range(10)]
        m_tx = start_transaction(type="master")
        t_tx = start_transaction(type="tablet")
        insert_rows("//tmp/t", rows, tx=t_tx)
        commit_transaction(t_tx, prerequisite_transaction_ids=[m_tx])
        actual = select_rows("* from [//tmp/t]")
        assert_items_equal(actual, rows)
