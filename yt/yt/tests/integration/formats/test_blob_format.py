from yt_env_setup import YTEnvSetup

from yt_commands import authors, create, read_table, write_table

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
