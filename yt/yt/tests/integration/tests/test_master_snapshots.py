from time import sleep

from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE
from yt_commands import *
from yt.environment.helpers import assert_items_equal

import pytest

##################################################################

def check_simple_node():
    set("//tmp/a", 42)

    yield

    assert get("//tmp/a") == 42

def check_schema():
    def get_schema(strict):
        return make_schema([{"name": "value", "type": "string", "required": True}], unique_keys=False, strict=strict)
    create("table", "//tmp/table1", attributes={"schema": get_schema(True)})
    create("table", "//tmp/table2", attributes={"schema": get_schema(True)})
    create("table", "//tmp/table3", attributes={"schema": get_schema(False)})

    yield

    assert normalize_schema(get("//tmp/table1/@schema")) == get_schema(True)
    assert normalize_schema(get("//tmp/table2/@schema")) == get_schema(True)
    assert normalize_schema(get("//tmp/table3/@schema")) == get_schema(False)

def check_forked_schema():
    schema1 = make_schema(
        [
            {"name": "foo", "type": "string", "required": True}
        ],
        unique_keys=False,
        strict=True,
    )
    schema2 = make_schema(
        [
            {"name": "foo", "type": "string", "required": True},
            {"name": "bar", "type": "string", "required": True}
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
    write_table("//tmp/b21_table", {"a": "b", "c" : "d"})
    remove_account("b2", sync_deletion=False)

    # XXX(kiselyovp) this might be flaky
    b11_disk_usage = get("//sys/accounts/b11/@resource_usage/disk_space_per_medium/default")
    b21_disk_usage = get("//sys/accounts/b21/@resource_usage/disk_space_per_medium/default")
    assert b11_disk_usage > 0
    assert b21_disk_usage > 0

    yield

    accounts = ls("//sys/accounts")

    for account in accounts:
        assert not account.startswith("#")

    topmost_accounts = ls("//sys/account_tree")
    for account in ["sys", "tmp", "intermediate", "chunk_wise_accounting_migration", "b1", "b2"]:
        assert account in accounts
        assert account in topmost_accounts
    for account in ["b11", "b21", "root"]:
        assert account in accounts
        assert account not in topmost_accounts

    assert get("//sys/account_tree/@ref_counter") == len(topmost_accounts) + 1

    assert ls("//sys/account_tree/b1") == ["b11"]
    assert ls("//sys/account_tree/b2") == ["b21"]
    assert ls("//sys/account_tree/b1/b11") == []
    assert ls("//sys/account_tree/b2/b21") == []

    assert get("//sys/accounts/b21/@resource_usage/disk_space_per_medium/default") == b21_disk_usage
    assert get("//sys/accounts/b2/@recursive_resource_usage/disk_space_per_medium/default") == b21_disk_usage

    set("//tmp/b21_table/@account", "b11")
    wait(lambda: not exists("//sys/account_tree/b2"))
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
        "master_memory": 100000
    }
    create_account("a", attributes={"resource_limits": resource_limits})

    create("map_node", "//tmp/dir1", attributes={"account": "a", "sdkjnfkdjs": "lsdkfj"})
    create("table", "//tmp/dir1/t", attributes={"account": "a", "aksdj" : "sdkjf"})
    write_table("//tmp/dir1/t", {"adssaa" : "kfjhsdkb"})
    copy("//tmp/dir1", "//tmp/dir2", preserve_account=True)

    sync_create_cells(1)
    schema=[{"name": "key", "type": "int64", "sort_order": "ascending"},
        {"name": "value", "type": "string"}]
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
    create_dynamic_table("//tmp/t", schema=[
        {"name": "key", "type": "int64", "sort_order": "ascending"},
        {"name": "value", "type": "string"}]
    )
    rows = [{"key": 0, "value": "0"}]
    keys = [{"key": 0}]
    sync_mount_table("//tmp/t")
    insert_rows("//tmp/t", rows)
    sync_freeze_table("//tmp/t")

    yield

    clear_metadata_caches()
    wait_for_cells()
    assert lookup_rows("//tmp/t", keys) == rows

def check_security_tags():
    for i in xrange(10):
        create("table", "//tmp/t" + str(i), attributes={
                "security_tags": ["atag" + str(i), "btag" + str(i)]
            })

    yield

    for i in xrange(10):
        assert_items_equal(get("//tmp/t" + str(i) + "/@security_tags"), ["atag" + str(i), "btag" + str(i)])

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
            check_hierarchical_accounts,
            check_master_memory,
            check_removed_account # keep this item last as it's sensitive to timings
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
        primary_master = ls("//sys/primary_masters")[0]
        profiling = get("//sys/primary_masters/{0}/orchid/profiling".format(primary_master))
        assert "free_space" in profiling["snapshots"]
        assert "available_space" in profiling["snapshots"]
        assert "free_space" in profiling["changelogs"]
        assert "available_space" in profiling["changelogs"]

class TestAllMastersSnapshots(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    USE_DYNAMIC_TABLES = True

    @authors("aleksandra-zh")
    def test(self):
        CHECKER_LIST = [
            check_simple_node,
            check_schema,
            check_forked_schema,
            check_dynamic_tables,
            check_security_tags,
            check_hierarchical_accounts,
            check_master_memory,
            check_removed_account # keep this item last as it's sensitive to timings
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
