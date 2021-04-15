from time import sleep

from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE
from yt_commands import *
from yt_helpers import Profiler
from yt.environment.helpers import assert_items_equal

import pytest

##################################################################


def check_simple_node():
    set("//tmp/a", 42)

    yield

    assert get("//tmp/a") == 42


def check_schema():
    def get_schema(strict):
        return make_schema(
            [{"name": "value", "type": "string", "required": True}],
            unique_keys=False,
            strict=strict,
        )

    create("table", "//tmp/table1", attributes={"schema": get_schema(True)})
    create("table", "//tmp/table2", attributes={"schema": get_schema(True)})
    create("table", "//tmp/table3", attributes={"schema": get_schema(False)})

    yield

    assert normalize_schema(get("//tmp/table1/@schema")) == get_schema(True)
    assert normalize_schema(get("//tmp/table2/@schema")) == get_schema(True)
    assert normalize_schema(get("//tmp/table3/@schema")) == get_schema(False)


def check_forked_schema():
    schema1 = make_schema(
        [{"name": "foo", "type": "string", "required": True}],
        unique_keys=False,
        strict=True,
    )
    schema2 = make_schema(
        [
            {"name": "foo", "type": "string", "required": True},
            {"name": "bar", "type": "string", "required": True},
        ],
        unique_keys=False,
        strict=True,
    )

    create("table", "//tmp/forked_schema_table", attributes={"schema": schema1})
    tx = start_transaction(timeout=60000)
    lock("//tmp/forked_schema_table", mode="snapshot", tx=tx)

    alter_table("//tmp/forked_schema_table", schema=schema2)

    yield

    assert normalize_schema(get("//tmp/forked_schema_table/@schema")) == schema2
    assert normalize_schema(get("//tmp/forked_schema_table/@schema", tx=tx)) == schema1


def check_removed_account():
    create_account("a1")
    create_account("a2")

    for i in xrange(0, 5):
        table = "//tmp/a1_table{0}".format(i)
        create("table", table, attributes={"account": "a1"})
        write_table(table, {"a": "b"})
        copy(table, "//tmp/a2_table{0}".format(i), attributes={"account": "a2"})

    for i in xrange(0, 5):
        chunk_id = get_singular_chunk_id("//tmp/a2_table{0}".format(i))
        wait(lambda: len(get("#{0}/@requisition".format(chunk_id))) == 2)

    for i in xrange(0, 5):
        remove("//tmp/a1_table" + str(i))

    remove_account("a1", sync_deletion=False)

    yield

    for i in xrange(0, 5):
        chunk_id = get_singular_chunk_id("//tmp/a2_table{0}".format(i))
        wait(lambda: len(get("#{0}/@requisition".format(chunk_id))) == 1)


def check_hierarchical_accounts():
    create_account("b1")
    create_account("b2")
    create_account("b11", "b1")
    create_account("b21", "b2")

    create("table", "//tmp/b11_table", attributes={"account": "b11"})
    write_table("//tmp/b11_table", {"a": "b"})
    create("table", "//tmp/b21_table", attributes={"account": "b21"})
    write_table("//tmp/b21_table", {"a": "b", "c": "d"})
    remove_account("b2", sync_deletion=False)

    # XXX(kiselyovp) this might be flaky
    wait(lambda: get("//sys/accounts/b11/@resource_usage/disk_space_per_medium/default") > 0)
    wait(lambda: get("//sys/accounts/b21/@resource_usage/disk_space_per_medium/default") > 0)
    b11_disk_usage = get("//sys/accounts/b11/@resource_usage/disk_space_per_medium/default")
    b21_disk_usage = get("//sys/accounts/b21/@resource_usage/disk_space_per_medium/default")

    yield

    accounts = ls("//sys/accounts")

    for account in accounts:
        assert not account.startswith("#")

    topmost_accounts = ls("//sys/account_tree")
    for account in [
        "sys",
        "tmp",
        "intermediate",
        "chunk_wise_accounting_migration",
        "b1",
        "b2",
    ]:
        assert account in accounts
        assert account in topmost_accounts
    for account in ["b11", "b21", "root"]:
        assert account in accounts
        assert account not in topmost_accounts

    wait(lambda: get("//sys/account_tree/@ref_counter") == len(topmost_accounts) + 1)

    assert ls("//sys/account_tree/b1") == ["b11"]
    assert ls("//sys/account_tree/b2") == ["b21"]
    assert ls("//sys/account_tree/b1/b11") == []
    assert ls("//sys/account_tree/b2/b21") == []

    assert get("//sys/accounts/b21/@resource_usage/disk_space_per_medium/default") == b21_disk_usage
    assert get("//sys/accounts/b2/@recursive_resource_usage/disk_space_per_medium/default") == b21_disk_usage

    set("//tmp/b21_table/@account", "b11")
    wait(lambda: not exists("//sys/account_tree/b2"), iter=120, sleep_backoff=0.5)
    assert not exists("//sys/accounts/b2")
    assert exists("//sys/accounts/b11")

    assert get("//sys/accounts/b11/@resource_usage/disk_space_per_medium/default") == b11_disk_usage + b21_disk_usage


def check_master_memory():
    resource_limits = {
        "disk_space_per_medium": {"default": 100000},
        "chunk_count": 100,
        "node_count": 100,
        "tablet_count": 100,
        "tablet_static_memory": 100000,
        "master_memory":
        {
            "total": 100000,
            "chunk_host": 100000,
        }
    }
    create_account("a", attributes={"resource_limits": resource_limits})

    create("map_node", "//tmp/dir1", attributes={"account": "a", "sdkjnfkdjs": "lsdkfj"})
    create("table", "//tmp/dir1/t", attributes={"account": "a", "aksdj": "sdkjf"})
    write_table("//tmp/dir1/t", {"adssaa": "kfjhsdkb"})
    copy("//tmp/dir1", "//tmp/dir2", preserve_account=True)

    sync_create_cells(1)
    schema = [
        {"name": "key", "type": "int64", "sort_order": "ascending"},
        {"name": "value", "type": "string"},
    ]
    create_dynamic_table("//tmp/d1", schema=schema, account="a")
    create_dynamic_table("//tmp/d2", schema=schema, account="a")

    sync_reshard_table("//tmp/d1", [[], [1], [2]])
    rows = [{"key": 0, "value": "0"}]
    sync_mount_table("//tmp/d1")
    insert_rows("//tmp/d1", rows)
    sync_freeze_table("//tmp/d1")

    sleep(3)

    master_memory_usage = get("//sys/accounts/a/@resource_usage/master_memory")

    yield

    wait(lambda: get("//sys/accounts/a/@resource_usage/master_memory") == master_memory_usage)


def check_dynamic_tables():
    sync_create_cells(1)
    create_dynamic_table(
        "//tmp/table_dynamic",
        schema=[
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ],
    )
    rows = [{"key": 0, "value": "0"}]
    keys = [{"key": 0}]
    sync_mount_table("//tmp/table_dynamic")
    insert_rows("//tmp/table_dynamic", rows)
    sync_freeze_table("//tmp/table_dynamic")

    yield

    clear_metadata_caches()
    wait_for_cells()
    assert lookup_rows("//tmp/table_dynamic", keys) == rows


def check_security_tags():
    for i in xrange(10):
        create(
            "table",
            "//tmp/table_with_tags" + str(i),
            attributes={"security_tags": ["atag" + str(i), "btag" + str(i)]},
        )

    yield

    for i in xrange(10):
        assert_items_equal(
            get("//tmp/table_with_tags" + str(i) + "/@security_tags"),
            ["atag" + str(i), "btag" + str(i)],
        )


def check_transactions():
    tx1 = start_transaction(timeout=120000)
    tx2 = start_transaction(tx=tx1, timeout=120000)

    create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 2})
    create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 3})
    table_id = create("table", "//tmp/p1/t", tx=tx1)  # replicate tx1 to cell 2
    table_id = create("table", "//tmp/p2/t", tx=tx2)  # replicate tx2 to cell 3
    assert get("#{}/@replicated_to_cell_tags".format(tx1)) == [2, 3]
    assert get("#{}/@replicated_to_cell_tags".format(tx2)) == [3]

    yield

    assert get("#{}/@replicated_to_cell_tags".format(tx1)) == [2, 3]
    assert get("#{}/@replicated_to_cell_tags".format(tx2)) == [3]


def check_duplicate_attributes():
    attrs = []
    for i in xrange(10):
        attrs.append(str(i) * 2000)

    for i in xrange(10):
        create("map_node", "//tmp/m{}".format(i))
        for j in xrange(len(attrs)):
            set("//tmp/m{}/@a{}".format(i, j), attrs[j])

    yield

    for i in xrange(10):
        for j in xrange(len(attrs)):
            assert get("//tmp/m{}/@a{}".format(i, j)) == attrs[j]


def check_proxy_roles():
    create("http_proxy_role_map", "//sys/http_proxy_roles")
    create("rpc_proxy_role_map", "//sys/rpc_proxy_roles")
    create_proxy_role("h", "http")
    create_proxy_role("r", "rpc")

    yield

    assert get("//sys/http_proxy_roles/h/@proxy_kind") == "http"
    assert get("//sys/rpc_proxy_roles/r/@proxy_kind") == "rpc"


def check_attribute_tombstone_yt_14682():
    create("table", "//tmp/table_with_attr")
    set("//tmp/table_with_attr/@attr", "value")

    tx = start_transaction()
    remove("//tmp/table_with_attr/@attr", tx=tx)

    yield

    assert get("//tmp/table_with_attr/@attr") == "value"
    assert not exists("//tmp/table_with_attr/@attr", tx=tx)


class TestMasterSnapshots(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    USE_DYNAMIC_TABLES = True

    @authors("ermolovd")
    def test(self):
        CHECKER_LIST = [
            check_simple_node,
            check_schema,
            check_forked_schema,
            check_dynamic_tables,
            check_security_tags,
            check_master_memory,
            check_hierarchical_accounts,
            check_duplicate_attributes,
            check_proxy_roles,
            check_attribute_tombstone_yt_14682,
            check_removed_account,  # keep this item last as it's sensitive to timings
        ]

        checker_state_list = [iter(c()) for c in CHECKER_LIST]
        for s in checker_state_list:
            next(s)

        build_snapshot(cell_id=None)

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        for s in checker_state_list:
            with pytest.raises(StopIteration):
                next(s)

    @authors("gritukan")
    def test_master_snapshots_free_space_profiling(self):
        sensors = Profiler.at_master(master_index=0).list()
        assert "yt/snapshots/free_space" in sensors
        assert "yt/snapshots/available_space" in sensors
        assert "yt/snapshots/free_space" in sensors
        assert "yt/snapshots/available_space" in sensors


class TestAllMastersSnapshots(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    USE_DYNAMIC_TABLES = True
    NUM_SECONDARY_MASTER_CELLS = 3

    @authors("aleksandra-zh")
    def test(self):
        CHECKER_LIST = [
            check_simple_node,
            check_schema,
            check_forked_schema,
            check_dynamic_tables,
            check_security_tags,
            check_master_memory,
            check_hierarchical_accounts,
            check_transactions,
            check_removed_account,  # keep this item last as it's sensitive to timings
        ]

        checker_state_list = [iter(c()) for c in CHECKER_LIST]
        for s in checker_state_list:
            next(s)

        build_master_snapshots()

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        for s in checker_state_list:
            with pytest.raises(StopIteration):
                next(s)


class TestMastersSnapshotsShardedTx(YTEnvSetup):
    NUM_SECONDARY_MASTER_CELLS = 3
    MASTER_CELL_ROLES = {
        "0": ["cypress_node_host"],
        "1": ["transaction_coordinator"],
        "2": ["chunk_host"],
    }

    @authors("aleksandra-zh")
    def test_reads_in_readonly(self):
        tx = start_transaction(coordinator_master_cell_tag=1)
        create("map_node", "//tmp/m", tx=tx)

        build_snapshot(cell_id=self.Env.configs["master"][0]["primary_master"]["cell_id"], set_read_only=True)
        primary = ls("//sys/primary_masters", suppress_transaction_coordinator_sync=True)[0]
        wait(lambda: get("//sys/primary_masters/{}/orchid/monitoring/hydra/read_only".format(primary), suppress_transaction_coordinator_sync=True))

        abort_transaction(tx)

        for secondary_master in self.Env.configs["master"][0]["secondary_masters"]:
            build_snapshot(cell_id=secondary_master["cell_id"], set_read_only=True)

        secondary_masters = get("//sys/secondary_masters", suppress_transaction_coordinator_sync=True)
        for cell_tag in secondary_masters:
            address = secondary_masters[cell_tag].keys()[0]
            wait(lambda: get("//sys/secondary_masters/{}/{}/orchid/monitoring/hydra/read_only".format(cell_tag, address), suppress_transaction_coordinator_sync=True))

        # Must not hang on this.
        get("//sys/primary_masters/{}/orchid/monitoring/hydra".format(primary), suppress_transaction_coordinator_sync=True)

        with Restarter(self.Env, MASTERS_SERVICE):
            pass
