from yt_env_setup import YTEnvSetup

from yt_commands import authors, create, read_table, write_table, raises_yt_error

import pytest

import yt.yson as yson


@pytest.mark.enabled_multidaemon
class TestBlobFormat(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    ENABLE_MULTIDAEMON = True

    @authors("achains")
    def test_default_read_blob(self):
        create(
            "table",
            "//tmp/t_in",
            attributes={
                "schema": [
                    {"name": "part_index", "type_v3": "int64"},
                    {"name": "data", "type_v3": "string"}
                ]
            },
        )
        write_table(
            "//tmp/t_in",
            [
                {
                    "part_index": 0,
                    "data": "hello"
                },
                {
                    "part_index": 1,
                    "data": "world"
                },
            ],
        )

        format = yson.YsonString(b"blob")
        blob_dump = read_table("//tmp/t_in", output_format=format)
        assert b"helloworld" == blob_dump

    @authors("achains")
    def test_custom_column(self):
        PART_INDEX_COLUMN_NAME = "my_part_index"
        DATA_COLUMN_NAME = "my_data"

        create(
            "table",
            "//tmp/t_in",
            attributes={
                "schema": [
                    {"name": PART_INDEX_COLUMN_NAME, "type_v3": "int64"},
                    {"name": DATA_COLUMN_NAME, "type_v3": "string"}
                ]
            },
        )
        write_table(
            "//tmp/t_in",
            [
                {
                    PART_INDEX_COLUMN_NAME: 0,
                    DATA_COLUMN_NAME: "hello"
                },
                {
                    PART_INDEX_COLUMN_NAME: 1,
                    DATA_COLUMN_NAME: "world"
                },
            ],
        )

        config = {"part_index_column_name": PART_INDEX_COLUMN_NAME, "data_column_name": DATA_COLUMN_NAME}
        blob_dump = read_table("//tmp/t_in", output_format=yson.to_yson_type("blob", attributes=config))
        assert b"helloworld" == blob_dump

    @authors("nadya73")
    def test_disable_part_index_basic(self):
        create(
            "table",
            "//tmp/t_in",
            attributes={
                "schema": [
                    {"name": "key", "type_v3": "string"},
                    {"name": "value", "type_v3": "string"},
                ]
            },
        )
        write_table(
            "//tmp/t_in",
            [
                {"key": "a", "value": "hello"},
                {"key": "b", "value": "world"},
            ],
        )

        config = {"enable_part_index": False, "data_column_name": "value"}
        blob_dump = read_table("//tmp/t_in", output_format=yson.to_yson_type("blob", attributes=config))
        assert b"helloworld" == blob_dump

        blob_dump = read_table("//tmp/t_in[#0:#1]", output_format=yson.to_yson_type("blob", attributes=config))
        assert b"hello" == blob_dump

        blob_dump = read_table("<columns=[value]>//tmp/t_in[#0:#1]", output_format=yson.to_yson_type("blob", attributes=config))
        assert b"hello" == blob_dump

    @authors("nadya73")
    def test_disable_part_index_no_part_index_column(self):
        # Table has no part_index column at all; enable_part_index=false must allow reading it.
        create(
            "table",
            "//tmp/t_in2",
            attributes={
                "schema": [
                    {"name": "text", "type_v3": "string"},
                ]
            },
        )
        write_table(
            "//tmp/t_in2",
            [
                {"text": "foo"},
                {"text": "bar"},
                {"text": "baz"},
            ],
        )

        config = {"enable_part_index": False, "data_column_name": "text"}
        blob_dump = read_table("//tmp/t_in2[#1:#3]", output_format=yson.to_yson_type("blob", attributes=config))
        assert b"barbaz" == blob_dump

    @authors("nadya73")
    def test_disable_part_index_with_non_consecutive_part_index(self):
        create(
            "table",
            "//tmp/t_in3",
            attributes={
                "schema": [
                    {"name": "part_index", "type_v3": "int64"},
                    {"name": "data", "type_v3": "string"},
                ]
            },
        )
        write_table(
            "//tmp/t_in3",
            [
                {"part_index": 0, "data": "first"},
                {"part_index": 5, "data": "second"},
            ],
        )

        blob_dump = read_table("//tmp/t_in3", output_format=yson.to_yson_type("blob", attributes={"enable_part_index": False}))
        assert b"firstsecond" == blob_dump

        with raises_yt_error("must be consecutive"):
            read_table("//tmp/t_in3", output_format=yson.YsonString(b"blob"))

    @authors("nadya73")
    def test_optional_string_data_column(self):
        create(
            "table",
            "//tmp/t_in5",
            attributes={
                "schema": [
                    {"name": "data", "type_v3": {"type_name": "optional", "item": "string"}},
                ]
            },
        )
        write_table(
            "//tmp/t_in5",
            [
                {"data": "hello"},
                {"data": None},
                {"data": "world"},
            ],
        )

        config = {"enable_part_index": False}
        blob_dump = read_table("//tmp/t_in5[#0:#1]", output_format=yson.to_yson_type("blob", attributes=config))
        assert b"hello" == blob_dump

        with raises_yt_error("must be of type"):
            read_table("//tmp/t_in5[#1:#3]", output_format=yson.to_yson_type("blob", attributes=config))
