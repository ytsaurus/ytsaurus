from yt_env_setup import YTEnvSetup

from yt_commands import authors, create, write_table, raises_yt_error

from yt_type_helpers import optional_type


class TestJsonFormat(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    @authors("yurial")
    def test_write_json_nothrow(self):
        create(
            "table",
            "//tmp/t_in",
            attributes={
                "schema": [
                    {"name": "int", "type_v3": "int64"},
                    {"name": "list_string", "type_v3": optional_type("yson")},
                ]
            },
        )
        deep_list = []
        cur = deep_list
        for i in range(10):
            tmp = []
            cur.append(tmp)
            cur = tmp
        cur.append("qwe")
        write_table(
            "//tmp/t_in",
            [
                {
                    "int": 53,
                    "list_string": deep_list
                },
            ],
        )

    @authors("yurial")
    def test_write_json_throw(self):
        create(
            "table",
            "//tmp/t_in",
            attributes={
                "schema": [
                    {"name": "int", "type_v3": "int64"},
                    {"name": "list_string", "type_v3": optional_type("yson")},
                ]
            },
        )
        deep_list = []
        cur = deep_list
        for i in range(1000):
            tmp = []
            cur.append(tmp)
            cur = tmp
        cur.append("qwe")
        with raises_yt_error("Depth limit exceeded while parsing YSON"):
            write_table(
                "//tmp/t_in",
                [
                    {
                        "int": 53,
                        "list_string": deep_list
                    },
                ],
            )
