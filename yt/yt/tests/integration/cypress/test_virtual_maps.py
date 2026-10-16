from yt_env_setup import (
    YTEnvSetup)

from yt_commands import (
    authors, create, ls, get, set, create_account, start_transaction,
    commit_transaction, raises_yt_error,
    gc_collect, get_driver, create_account_resource_usage_lease)

##################################################################


def query_object(id, vmap_path, exists=True, **kwargs):
    ls(vmap_path, **kwargs)

    if exists:
        get(f"#{id}", **kwargs)
        get(f"{vmap_path}/{id}", **kwargs)
    else:
        with raises_yt_error(f"No such object {id}"):
            get(f"#{id}", **kwargs)
        with raises_yt_error(f"Node has no child with key \"{id}\""):
            get(f"{vmap_path}/{id}", **kwargs)


class TestVirtualMaps(YTEnvSetup):
    @authors("ivpiskarev")
    def test_account_resource_usage_leases_vmap(self):
        set("//sys/@config/object_manager/gc_sweep_period", 1000)  # 1 second

        create_account("a")

        for it in range(3):
            tx = start_transaction()
            create("table", f"//tmp/t_{it}", tx=tx)  # To replicate the transaction to the primary cell.
            lease_id = create_account_resource_usage_lease(account="a", transaction_id=tx)

            # Must not crash.
            query_object(lease_id, "//sys/account_resource_usage_leases", exists=True)
            query_object(lease_id, "//sys/account_resource_usage_leases", exists=True, attributes=["account", "transaction_id", "resource_usage"])
            commit_transaction(tx)
            query_object(lease_id, "//sys/account_resource_usage_leases", exists=False)
            query_object(lease_id, "//sys/account_resource_usage_leases", exists=False, attributes=["account", "transaction_id", "resource_usage"])

        # To speed up teardown.
        set("//sys/@config/object_manager/gc_sweep_period", 10)  # 0.01 second

    @authors("ivpiskarev")
    def test_last_ping_info(self):
        n = 4
        txs = [start_transaction() for i in range(n)]

        for attr in ["state", "start_time", "last_ping_time", "last_ping_address"]:
            for i in range(n):
                get(f"#{txs[i]}/@{attr}")
                get(f"//sys/transactions/{txs[i]}/@{attr}")

                get(f"#{txs[i]}", attributes=[attr])
                get(f"//sys/transactions/{txs[i]}", attributes=[attr])

            ls("//sys/transactions")
            get("//sys/transactions")
            ls("//sys/transactions", attributes=[attr])
            get("//sys/transactions", attributes=[attr])

        for tx in txs:
            commit_transaction(tx)
        gc_collect()


class TestVirtualMapsMulticell(TestVirtualMaps):
    NUM_SECONDARY_MASTER_CELLS = 3
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["cypress_node_host"]},
        "12": {"roles": ["transaction_coordinator"]},
        "13": {"roles": ["chunk_host"]},
    }

    @authors("ivpiskarev")
    def test_foreign_transactions_vmap(self):
        set("//sys/@config/object_manager/gc_sweep_period", 5_000)  # 5 seconds

        for _ in range(3):
            tx = start_transaction(replicate_to_master_cell_tags=[10, 11])
            for i in range(2):
                assert tx in ls("//sys/foreign_transactions", driver=get_driver(i))

            # Must not crash.
            for i in range(2):
                query_object(tx, "//sys/foreign_transactions", exists=True, driver=get_driver(i))
                query_object(tx, "//sys/foreign_transactions", exists=True, attributes=["id"], driver=get_driver(i))
            commit_transaction(tx)
            for i in range(2):
                query_object(tx, "//sys/foreign_transactions", exists=False, driver=get_driver(i))
                query_object(tx, "//sys/foreign_transactions", exists=False, attributes=["id"], driver=get_driver(i))

        # To speed up teardown.
        set("//sys/@config/object_manager/gc_sweep_period", 10)  # 0.01 second
