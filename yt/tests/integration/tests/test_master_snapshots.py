from yt_env_setup import YTEnvSetup
from yt_commands import *


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

    assert get("//tmp/table1/@schema") == get_schema(True)
    assert get("//tmp/table2/@schema") == get_schema(True)
    assert get("//tmp/table3/@schema") == get_schema(False)

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
    tx = start_transaction()
    lock("//tmp/forked_schema_table", mode="snapshot", tx=tx)

    alter_table("//tmp/forked_schema_table", schema=schema2)

    yield

    assert get("//tmp/forked_schema_table/@schema") == schema2
    assert get("//tmp/forked_schema_table/@schema", tx=tx) == schema1


class TestSnapshot(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 0

    def test(self):
        CHECKER_LIST = [
            check_simple_node,
            check_schema,
            check_forked_schema,
        ]

        checker_state_list = [iter(c()) for c in CHECKER_LIST]
        for s in checker_state_list:
            next(s)

        build_snapshot(cell_id=None)

        self.Env.kill_master_cell()
        self.Env.start_master_cell()

        for s in checker_state_list:
            with pytest.raises(StopIteration):
                next(s)
