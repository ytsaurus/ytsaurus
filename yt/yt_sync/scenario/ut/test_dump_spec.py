from io import StringIO

from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.scenario import DumpSpecScenario
from yt.yt_sync.scenario import get_scenario_type


def test_dump_spec_registered():
    assert DumpSpecScenario == get_scenario_type("dump_spec")


def test_dump_spec(default_schema: Types.Schema):
    desired = YtDatabase()
    cluster = desired.add_or_get_cluster(YtCluster.make(YtCluster.Type.MAIN, "primary", False))
    table_path = "//tmp/table"
    cluster.add_table(
        YtTable.make(
            table_path,
            cluster.name,
            YtTable.Type.TABLE,
            table_path,
            True,
            {"dynamic": True, "schema": default_schema, "tablet_count": 2, "my_attr": None},
        )
    )

    scenario = DumpSpecScenario(
        desired, YtDatabase(), Settings(db_type=Settings.REPLICATED_DB), MockYtClientFactory({})
    )
    out = StringIO()
    scenario.setup(out=out)
    assert not scenario.run()

    expected = """BEGIN_TABLE_FEDERATION //tmp/table
[TABLE] primary://tmp/table
{
    "spec": {
        "ordered": false
    },
    "yt_attributes": {
        "dynamic": true,
        "my_attr": null,
        "schema": [
            {
                "expression": "farm_hash(Key) % 10",
                "name": "Hash",
                "sort_order": "ascending",
                "type": "uint64",
                "type_v3": {
                    "item": "uint64",
                    "type_name": "optional"
                }
            },
            {
                "name": "Key",
                "sort_order": "ascending",
                "type": "uint64",
                "type_v3": {
                    "item": "uint64",
                    "type_name": "optional"
                }
            },
            {
                "lock": "api",
                "name": "Value",
                "type": "uint64",
                "type_v3": {
                    "item": "uint64",
                    "type_name": "optional"
                }
            }
        ],
        "tablet_count": 2
    }
}
END_TABLE_FEDERATION //tmp/table

"""

    assert expected == out.getvalue()
