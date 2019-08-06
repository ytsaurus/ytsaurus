from yt_env_setup import YTEnvSetup, Restarter, MASTER_CELL_SERVICE
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

    remove_account("a1")

    yield

    for i in xrange(0, 5):
        chunk_id = get_singular_chunk_id("//tmp/a2_table{0}".format(i))
        wait(lambda: len(get("#{0}/@requisition".format(chunk_id))) == 1)

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
            check_removed_account # keep this item last as it's sensitive to timings
        ]

        checker_state_list = [iter(c()) for c in CHECKER_LIST]
        for s in checker_state_list:
            next(s)

        build_snapshot(cell_id=None)

        with Restarter(self.Env, MASTER_CELL_SERVICE):
            pass

        for s in checker_state_list:
            with pytest.raises(StopIteration):
                next(s)
