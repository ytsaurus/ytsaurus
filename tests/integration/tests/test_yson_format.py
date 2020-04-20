from yt_commands import *
from yt_env_setup import YTEnvSetup, unix_only


def _test_yson_row(type, optimize_for, canonical_value, format, format_value):
    schema = make_schema([
        {
            "name": "column",
            "type_v3": type,
        }
    ])
    path = "//tmp/table"
    create("table", path, attributes={"schema": schema, "optimize_for": optimize_for}, force=True)
    rows = [{"column": format_value}]
    write_table(path, rows, input_format=format)
    assert read_table(path) == [{"column": canonical_value}]

    read = read_table(path, output_format=format)
    read_rows = list(yson.loads(read, yson_type="list_fragment"))
    assert read_rows == [{"column": format_value}]


@authors("ermolovd")
@pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
class TestYsonPositionalFormat(YTEnvSetup):
    def test_struct(self, optimize_for):
        positional_yson = yson.loads("<complex_type_mode=positional>yson")
        type1 = struct_type([
            ("f1", "int64"),
            ("f2", optional_type("utf8")),
        ])

        _test_yson_row(type1, optimize_for, {"f1": 42, "f2": "forty_two"}, positional_yson, [42, "forty_two"] )
        _test_yson_row(type1, optimize_for, {"f1": 53, "f2": None}, positional_yson, [53, None] )
        _test_yson_row(type1, optimize_for, {"f1": 83, "f2": None}, positional_yson, [83, None] )

        type2 = variant_struct_type([
            ("f1", "int64"),
            ("f2", optional_type("utf8")),
        ])

        _test_yson_row(type2, optimize_for, ["f1", 42], positional_yson, [0, 42])
        _test_yson_row(type2, optimize_for, ["f2", None], positional_yson, [1, None])
        _test_yson_row(type2, optimize_for, ["f2", "foo"], positional_yson, [1, "foo"])
