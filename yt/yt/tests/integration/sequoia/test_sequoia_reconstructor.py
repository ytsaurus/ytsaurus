from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, create_user, make_ace, remove, write_table, start_transaction, abort_transaction, commit_transaction, lock, link, set)

from sequoia_reconstructor import check_sequoia_tables_reconstruction

import pytest

##################################################################


class TestSequoiaReconstructor(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # We will build snapshots.
    USE_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    NUM_TEST_PARTITIONS = 2

    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["sequoia_node_host"]},
    }

    @authors("grphil")
    def test_simple_reconstruction(self):
        check_sequoia_tables_reconstruction(self)

        create("table", "//tmp/some_dir/table", recursive=True)
        check_sequoia_tables_reconstruction(self)

        write_table("//tmp/some_dir/table", [{"x": "hello"}])
        check_sequoia_tables_reconstruction(self)

        remove("//tmp/some_dir/table")
        check_sequoia_tables_reconstruction(self)

    @authors("grphil")
    def test_simple_transactions(self):
        tx = start_transaction(timeout=100000)
        check_sequoia_tables_reconstruction(self)

        create("map_node", "//tmp/dir", tx=tx)
        create("table", "//tmp/dir/table", tx=tx)
        check_sequoia_tables_reconstruction(self)

        commit_transaction(tx)
        check_sequoia_tables_reconstruction(self)

        tx = start_transaction(timeout=100000)
        check_sequoia_tables_reconstruction(self)
        remove("//tmp/dir/table", tx=tx)
        check_sequoia_tables_reconstruction(self)
        abort_transaction(tx)
        check_sequoia_tables_reconstruction(self)

        tx = start_transaction(timeout=100000)
        check_sequoia_tables_reconstruction(self)
        remove("//tmp/dir/table", tx=tx)
        check_sequoia_tables_reconstruction(self)
        commit_transaction(tx)
        check_sequoia_tables_reconstruction(self)

    @authors("grphil")
    def test_complex_transactions(self):
        tx1 = start_transaction(timeout=100000)
        create("map_node", "//tmp/dir", tx=tx1)

        tx2 = start_transaction(timeout=100000, tx=tx1)
        create("table", "//tmp/dir/table", tx=tx2)
        check_sequoia_tables_reconstruction(self)

        tx3 = start_transaction(timeout=100000, tx=tx2)
        remove("//tmp/dir/table", tx=tx3)
        check_sequoia_tables_reconstruction(self)

        abort_transaction(tx3)
        check_sequoia_tables_reconstruction(self)

        tx3 = start_transaction(timeout=100000, tx=tx2)
        remove("//tmp/dir/table", tx=tx3)
        check_sequoia_tables_reconstruction(self)

        abort_transaction(tx2)
        check_sequoia_tables_reconstruction(self)

        tx2 = start_transaction(timeout=100000, tx=tx1)
        create("table", "//tmp/dir/table", tx=tx2)
        check_sequoia_tables_reconstruction(self)

        commit_transaction(tx2)
        check_sequoia_tables_reconstruction(self)

        tx11 = start_transaction(timeout=100000, tx=tx1)
        create("table", "//tmp/dir/t1", tx=tx11)

        tx12 = start_transaction(timeout=100000, tx=tx1)
        create("table", "//tmp/dir/t2", tx=tx12)

        tx13 = start_transaction(timeout=100000, tx=tx1)
        create("table", "//tmp/dir/t3", tx=tx13)

        tx131 = start_transaction(timeout=100000, tx=tx13)
        create("table", "//tmp/dir/t31", tx=tx131)

        tx1311 = start_transaction(timeout=100000, tx=tx131)
        remove("//tmp/dir/t3", tx=tx1311)

        tx14 = start_transaction(timeout=100000, tx=tx1)
        create("table", "//tmp/dir/t4", tx=tx14)

        check_sequoia_tables_reconstruction(self)

        commit_transaction(tx11)
        check_sequoia_tables_reconstruction(self)

        abort_transaction(tx12)
        check_sequoia_tables_reconstruction(self)

        commit_transaction(tx1311)
        commit_transaction(tx131)
        commit_transaction(tx13)
        commit_transaction(tx14)
        commit_transaction(tx1)
        check_sequoia_tables_reconstruction(self)

    @authors("grphil")
    def test_node_snapshots(self):
        create("map_node", "//tmp/dir")

        tx1 = start_transaction(timeout=100000)
        create("table", "//tmp/dir/t1", tx=tx1)

        tx2 = start_transaction(timeout=100000, tx=tx1)
        create("table", "//tmp/dir/t2", tx=tx2)

        tx3 = start_transaction(timeout=100000, tx=tx2)
        create("table", "//tmp/dir/t3", tx=tx3)

        tx4 = start_transaction(timeout=100000, tx=tx3)
        create("table", "//tmp/dir/t4", tx=tx4)

        check_sequoia_tables_reconstruction(self)

        commit_transaction(tx4)
        commit_transaction(tx3)
        check_sequoia_tables_reconstruction(self)

        tx21 = start_transaction(timeout=100000, tx=tx2)
        tx22 = start_transaction(timeout=100000, tx=tx2)

        lock("//tmp/dir/t3", tx=tx21, mode="snapshot")
        lock("//tmp/dir/t4", tx=tx22, mode="snapshot")
        check_sequoia_tables_reconstruction(self)

        abort_transaction(tx21)
        abort_transaction(tx22)
        check_sequoia_tables_reconstruction(self)

        commit_transaction(tx2)

        tx2 = start_transaction(timeout=100000, tx=tx1)
        tx21 = start_transaction(timeout=100000, tx=tx2)
        tx22 = start_transaction(timeout=100000, tx=tx2)

        lock("//tmp/dir/t2", tx=tx21, mode="snapshot")
        lock("//tmp/dir/t2", tx=tx22, mode="snapshot")
        check_sequoia_tables_reconstruction(self)

        abort_transaction(tx21)
        abort_transaction(tx22)
        check_sequoia_tables_reconstruction(self)

        commit_transaction(tx2)
        commit_transaction(tx1)

        check_sequoia_tables_reconstruction(self)

        tx1 = start_transaction(timeout=100000)
        lock("//tmp/dir", tx=tx1, mode="snapshot")

        check_sequoia_tables_reconstruction(self)

        tx2 = start_transaction(timeout=100000, tx=tx1)
        tx21 = start_transaction(timeout=100000, tx=tx2)
        tx22 = start_transaction(timeout=100000, tx=tx2)

        lock("//tmp/dir", tx=tx22, mode="snapshot")
        lock("//tmp/dir/t1", tx=tx21, mode="snapshot")

        check_sequoia_tables_reconstruction(self)

        abort_transaction(tx21)
        abort_transaction(tx22)

        check_sequoia_tables_reconstruction(self)

        commit_transaction(tx2)
        commit_transaction(tx1)

        check_sequoia_tables_reconstruction(self)

    @authors("grphil")
    def test_dependent_transactions(self):
        tx1 = start_transaction(timeout=100000)
        tx11 = start_transaction(timeout=100000, tx=tx1)

        check_sequoia_tables_reconstruction(self)

        tx111 = start_transaction(timeout=100000, tx=tx11)
        tx112 = start_transaction(timeout=100000, tx=tx11)

        tx2 = start_transaction(timeout=100000)
        tx21 = start_transaction(timeout=100000, tx=tx2, prerequisite_transaction_ids=[tx111])
        tx22 = start_transaction(timeout=100000, tx=tx2, prerequisite_transaction_ids=[tx112])

        check_sequoia_tables_reconstruction(self)

        tx3 = start_transaction(timeout=100000, prequisite_transaction_ids=[tx21, tx22])

        check_sequoia_tables_reconstruction(self)

        tx31 = start_transaction(timeout=100000, tx=tx3, prerequisite_transaction_ids=[tx2])
        start_transaction(timeout=100000, tx=tx31, prerequisite_transaction_ids=[tx11])

        check_sequoia_tables_reconstruction(self)

        tx4 = start_transaction(timeout=100000, prequisite_transaction_ids=[tx21, tx22, tx11, tx3])

        check_sequoia_tables_reconstruction(self)

        abort_transaction(tx4)
        check_sequoia_tables_reconstruction(self)

        abort_transaction(tx111)
        check_sequoia_tables_reconstruction(self)

        abort_transaction(tx112)
        check_sequoia_tables_reconstruction(self)

        commit_transaction(tx2)
        check_sequoia_tables_reconstruction(self)

    @authors("grphil")
    def test_transaction_attributes(self):
        start_transaction(timeout=100000, attributes={"title": "tx1", "non_sequoia_name": "no value"})
        start_transaction(timeout=100000, attributes={"title": "tx2", "operation_title": "title"})
        check_sequoia_tables_reconstruction(self)

    @authors("grphil")
    def test_links(self):
        create("map_node", "//tmp/dir1")
        create("map_node", "//tmp/dir1/dir2")

        tx1 = start_transaction(timeout=100000)
        create("table", "//tmp/dir1/dir2/t1", tx=tx1)
        create("table", "//tmp/dir1/t2", tx=tx1)

        check_sequoia_tables_reconstruction(self)

        tx2 = start_transaction(timeout=100000, tx=tx1)
        link("//tmp/dir1/dir2/t1", "//tmp/dir1/t1", tx=tx2)
        link("//tmp/dir1/t2", "//tmp/dir1/dir2/t2", tx=tx1)

        check_sequoia_tables_reconstruction(self)

    @authors("grphil")
    def test_remove_return(self):
        for remove_depth in range(3):
            for return_depth in range(3):
                table = f"//tmp/t{remove_depth}_{return_depth}"
                tx = start_transaction(timeout=100000)
                create("table", table, tx=tx)
                for _ in range(remove_depth):
                    tx = start_transaction(timeout=100000, tx=tx)
                remove(table, tx=tx)
                for _ in range(return_depth):
                    tx = start_transaction(timeout=100000, tx=tx)
                create("table", table, tx=tx)

        check_sequoia_tables_reconstruction(self)

    @authors("grphil")
    def test_replace_remove(self):
        for replace_depth in range(3):
            for remove_depth in range(3):
                table = f"//tmp/t{replace_depth}_{remove_depth}"
                tx = start_transaction(timeout=100000)
                create("table", table, tx=tx)
                for _ in range(replace_depth):
                    tx = start_transaction(timeout=100000, tx=tx)
                remove(table, tx=tx)
                create("table", table, tx=tx)
                for _ in range(remove_depth):
                    tx = start_transaction(timeout=100000, tx=tx)
                remove(table, tx=tx)

        check_sequoia_tables_reconstruction(self)

    @authors("grphil")
    @pytest.mark.parametrize("start_initial_transaction", [True, False])
    def test_recreate_and_remove(self, start_initial_transaction):
        if start_initial_transaction:
            t1 = start_transaction(timeout=100000)
            create("table", "//tmp/t", tx=t1)
            t2 = start_transaction(timeout=100000, tx=t1)
        else:
            create("table", "//tmp/t")
            t2 = start_transaction(timeout=100000)

        remove("//tmp/t", tx=t2)
        t3 = start_transaction(timeout=100000, tx=t2)
        t4 = start_transaction(timeout=100000, tx=t3)
        create("table", "//tmp/t", tx=t4)
        t5 = start_transaction(timeout=100000, tx=t4)
        remove("//tmp/t", tx=t5)
        check_sequoia_tables_reconstruction(self)
        commit_transaction(t5)
        check_sequoia_tables_reconstruction(self)
        commit_transaction(t4)
        check_sequoia_tables_reconstruction(self)

    @authors("grphil")
    def test_multiple_replacements(self):
        transactions = []
        tx = start_transaction(timeout=100000)
        transactions.append(tx)

        create("table", "//tmp/t", tx=tx)

        for _ in range(5):
            tx = start_transaction(timeout=100000, tx=tx)
            transactions.append(tx)

            remove("//tmp/t", tx=tx)
            create("table", "//tmp/t", tx=tx)

        check_sequoia_tables_reconstruction(self)
        for tx in transactions[::-1]:
            commit_transaction(tx)
            check_sequoia_tables_reconstruction(self)

    @authors("grphil")
    def test_acl(self):
        create_user("u")

        create("table", "//tmp/t", attributes={
            "inherit_acl": False,
            "acl": [make_ace("allow", "u", "write")],
        })
        check_sequoia_tables_reconstruction(self)

        set("//tmp/t/@inherit_acl", True)
        check_sequoia_tables_reconstruction(self)

        set("//tmp/t/@acl/end", make_ace("deny", "everyone", "read"))
        check_sequoia_tables_reconstruction(self)

        remove("//tmp/t")
        check_sequoia_tables_reconstruction(self)
