from yt.yson import dumps

from yt_yson_bindings import SkiffSchema, load_skiff, dump_skiff, SkiffTableSwitch

import copy
import pytest
import random
from io import BytesIO

def create_skiff_schema(schemas):
    return SkiffSchema(schemas, {}, "#range_index", "#row_index")

class TestSkiff(object):
    def test_schema_class(self):
        skiff_schema = \
            {
                "wire_type": "tuple",
                "children": [
                    {
                        "wire_type": "int64",
                        "name": "x"
                    },
                    {
                        "wire_type": "variant8",
                        "children": [
                            {
                                "wire_type": "nothing"
                            },
                            {
                                "wire_type": "int64"
                            }
                        ],
                        "name": "y"
                    }
                ]
            }

        schema = create_skiff_schema([skiff_schema])

        assert schema.get_field_names() == ["x", "y"]

        assert not schema.has_other_columns()
        record = schema.create_record()
        assert len(record) == 2

        with pytest.raises(ValueError):
            schema = create_skiff_schema([skiff_schema] * 10)

        schema_copy = copy.copy(schema)
        assert schema is not schema_copy

        schema_copy = copy.deepcopy(schema)
        assert schema is not schema_copy

    def test_record_class(self):
        skiff_schema = \
            {
                "wire_type": "tuple",
                "children": [
                    {
                        "wire_type": "int64",
                        "name": "x"
                    },
                    {
                        "wire_type": "yson32",
                        "name": "y"
                    }
                ]
            }
        schema = create_skiff_schema([skiff_schema])

        assert not schema.has_other_columns()
        record = schema.create_record()

        record["x"] = 5
        record["y"] = [1, 2, 3]

        record_copy = copy.copy(record)
        assert record_copy is not record

        record_copy = copy.deepcopy(record)
        record_copy["y"][1] = record_copy["x"]

        assert record["y"] == [1, 2, 3]
        assert record_copy["y"] == [1, 5, 3]

        assert list(record.items()) == [("x", 5), ("y", [1, 2, 3])]
        assert list(record) == [("x", 5), ("y", [1, 2, 3])]

    def test_load_dump(self):
        skiff_schema = \
            {
                "wire_type": "tuple",
                "children": [
                    {
                        "wire_type": "int64",
                        "name": "x"
                    },
                    {
                        "wire_type": "variant8",
                        "children": [
                            {
                                "wire_type": "nothing"
                            },
                            {
                                "wire_type": "int64"
                            }
                        ],
                        "name": "y"
                    }
                ]
            }
        schema = create_skiff_schema([skiff_schema])

        record = schema.create_record()
        record["x"] = 1
        record["y"] = 2

        stream = BytesIO()
        dump_skiff([record], [stream], [schema])

        stream.seek(0)
        result = list(load_skiff(stream, [schema], "#range_index", "#row_index"))
        assert len(result) == 1

        assert result[0]["x"] == 1
        assert result[0]["y"] == 2

    def test_multi_table_load_dump(self):
        skiff_schemas = [
            {
                "wire_type": "tuple",
                "children": [
                    {
                        "wire_type": "int64",
                        "name": "x"
                    },
                    {
                        "wire_type": "variant8",
                        "children": [
                            {
                                "wire_type": "nothing"
                            },
                            {
                                "wire_type": "int64"
                            }
                        ],
                        "name": "y"
                    }
                ]
            },
            {
                "wire_type": "tuple",
                "children": [
                    {
                        "wire_type": "int64",
                        "name": "z"
                    },
                    {
                        "wire_type": "int64",
                        "name": "y"
                    }
                ]
            }
        ]
        first_schema = create_skiff_schema(skiff_schemas[:1])

        second_schema = create_skiff_schema(skiff_schemas[1:2])

        first_record = first_schema.create_record()
        first_record["x"] = 1
        first_record["y"] = 2

        second_record = second_schema.create_record()
        second_record["z"] = 5
        second_record["y"] = 10

        first_stream = BytesIO()
        second_stream = BytesIO()
        dump_skiff([first_record, SkiffTableSwitch(1), second_record], [first_stream, second_stream], [first_schema, second_schema])

        first_stream.seek(0)
        result = list(load_skiff(first_stream, [first_schema], "#range_index", "#row_index"))
        assert len(result) == 1

        assert result[0]["x"] == 1
        assert result[0]["y"] == 2

        second_stream.seek(0)
        result = list(load_skiff(second_stream, [second_schema], "#range_index", "#row_index"))
        assert len(result) == 1

        assert result[0]["z"] == 5
        assert result[0]["y"] == 10

    def test_yson_field(self):
        skiff_schema = \
            {
                "wire_type": "tuple",
                "children": [
                    {
                        "wire_type": "yson32",
                        "name": "value"
                    }
                ]
            }
        schema = create_skiff_schema([skiff_schema])

        record = schema.create_record()
        record["value"] = {"key1": 1, "key2": "value2", "key3": 0.1}

        stream = BytesIO()
        dump_skiff([record], [stream], [schema])

        stream.seek(0)
        result = list(load_skiff(stream, [schema], "#range_index", "#row_index"))
        assert len(result) == 1

        assert result[0]["value"] == {"key1": 1, "key2": "value2", "key3": 0.1}

    def test_other_columns(self):
        skiff_schema = \
            {
                "wire_type": "tuple",
                "children": [
                    {
                        "wire_type": "yson32",
                        "name": "$other_columns"
                    }
                ]
            }
        schema = create_skiff_schema([skiff_schema])

        record = schema.create_record()
        record["x"] = 1
        record["y"] = "value"
        record["z"] = 0.1

        stream = BytesIO()
        dump_skiff([record], [stream], [schema])

        stream.seek(0)
        result = list(load_skiff(stream, [schema], "#range_index", "#row_index"))
        assert len(result) == 1

        record = result[0]

        assert record["x"] == 1
        assert record["y"] == "value"
        assert record["z"] == 0.1

        assert sorted(list(record)) == [("x", 1), ("y", "value"), ("z", 0.1)]

        del record["z"]
        assert sorted(list(record)) == [("x", 1), ("y", "value")]

    def test_sparse_columns(self):
        skiff_schema = \
            {
                "wire_type": "tuple",
                "children": [
                    {
                        "wire_type": "int64",
                        "name": "t"
                    },
                    {
                        "wire_type": "repeated_variant16",
                        "name": "$sparse_columns",
                        "children": [
                            {
                                "wire_type": "int64",
                                "name": "x"
                            },
                            {
                                "wire_type": "double",
                                "name": "y"
                            },
                            {
                                "wire_type": "string32",
                                "name": "z"
                            },
                        ]
                    }
                ]
            }
        schema = create_skiff_schema([skiff_schema])

        record = schema.create_record()
        record["t"] = 5
        record["y"] = 0.1
        record["z"] = "value"

        stream = BytesIO()
        dump_skiff([record], [stream], [schema])
        stream.seek(0)
        result = list(load_skiff(stream, [schema], "#range_index", "#row_index"))
        assert len(result) == 1

        assert result[0]["t"] == 5
        assert result[0]["x"] is None
        assert result[0]["y"] == 0.1
        assert result[0]["z"] == "value"

        assert sorted(list(result[0])) == [("t", 5), ("y", 0.1), ("z", "value")]

    def test_raw_load(self):
        skiff_schema = \
            {
                "wire_type": "tuple",
                "children": [
                    {
                        "wire_type": "int64",
                        "name": "x"
                    },
                    {
                        "wire_type": "yson32",
                        "name": "$other_columns"
                    }
                ]
            }
        schema = create_skiff_schema([skiff_schema])

        records = []
        for i in xrange(10):
            record = schema.create_record()
            record["x"] = i
            record["y"] = "a" * random.randint(0, 10)
            records.append(record)

        stream = BytesIO()
        dump_skiff(records, [stream], [schema])

        stream.seek(0)
        raw_dumped_data = stream.read()

        stream.seek(0)
        result = list(load_skiff(stream, [schema], "#range_index", "#row_index", raw=True))
        assert len(result) == 10

        assert raw_dumped_data == "".join(result)

        for i, row in enumerate(result):
            iter = load_skiff(BytesIO(row), [schema], "#range_index", "#row_index")
            assert next(iter)["x"] == i
            with pytest.raises(StopIteration):
                next(iter)
