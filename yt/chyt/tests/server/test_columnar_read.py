from base import ClickHouseTestBase, Clique

from yt_commands import (write_table, authors, create, merge)

import yt.yson as yson


class TestColumnarRead(ClickHouseTestBase):
    CONFIG_PATCH = {
        "yt": {
            "settings": {
                "enable_columnar_read": True,
            },
            "table_attribute_cache": {
                "read_from": "follower",
                "expire_after_successful_update_time": 0,
                "expire_after_failed_update_time": 0,
                "refresh_time": 0,
                "expire_after_access_time": 0,
            },
        }
    }

    @staticmethod
    def _check_single_column(clique, type, required, values):
        create(
            "table",
            "//tmp/t",
            attributes={"schema": [{"name": "x", "type": type, "required": required}], "optimize_for": "scan"},
            force=True,
        )
        write_table("//tmp/t", [{"x": value} for value in values])

        data = [row["x"] for row in clique.make_query("select * from `//tmp/t`")]
        assert data == values

    @authors("babenko")
    def test_integer(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            for type in ["int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64"]:
                self._check_single_column(clique, type, True, [i for i in range(10)])
                self._check_single_column(clique, type, True, [77] * 1000)
                self._check_single_column(clique, type, True, [1, 2, 3] * 100)
                self._check_single_column(clique, type, True, [11] * 1000 + [22] * 1000 + [33] * 1000)

                self._check_single_column(clique, type, False, [i if i % 3 == 0 else None for i in range(10)])
                self._check_single_column(clique, type, False, [None] * 1000)
                self._check_single_column(clique, type, False, [1, 2, 3, None] * 100)
                self._check_single_column(clique, type, False, [1] * 1000 + [None] * 1000 + [2] * 1000)

    @authors("babenko")
    def test_signed_integer(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            for type in [
                "int8",
                "int16",
                "int32",
                "int64",
            ]:
                self._check_single_column(clique, type, True, [-i for i in range(10)])

    @authors("babenko")
    def test_boolean(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            self._check_single_column(clique, "boolean", True, [i % 2 == 0 for i in range(10)])
            self._check_single_column(clique, "boolean", True, [False] * 100 + [True] * 200 + [True] * 100)

            self._check_single_column(clique, "boolean", False, [True, None, False, None, True, True, None, False])

    @authors("babenko")
    def test_floating_point(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            for type in ["double", "float"]:
                self._check_single_column(clique, type, True, [1.0, 2.0, 3.14, 2.7])

                self._check_single_column(clique, type, False, [1.0, 2.0, None, 3.14, 2.7, None])

    @authors("babenko")
    def test_string(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            self._check_single_column(clique, "string", True, ["hello", "world"])
            self._check_single_column(clique, "string", True, ["hello", "world"] * 100)
            self._check_single_column(clique, "string", True, ["hello"] * 1000)

            self._check_single_column(clique, "string", True, ["\x00" * 10, "some\x00nulls\x00inside", ""])

            self._check_single_column(clique, "string", False, ["hello", None, "world"])
            self._check_single_column(clique, "string", False, ["hello", "world", None] * 100)
            self._check_single_column(clique, "string", False, ["hello"] * 1000 + [None] * 1000 + ["world"] * 1000)

    @authors("max42")
    def test_null_and_void(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            self._check_single_column(clique, "null", False, [None] * 1000)
            self._check_single_column(clique, "void", False, [None] * 1000)

    @authors("babenko")
    def test_yson(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            create(
                "table",
                "//tmp/t",
                attributes={"schema": [{"name": "x", "type": "any"}], "optimize_for": "scan"},
                force=True,
            )
            row = {"x": {"some": {"yson": True}}}
            write_table("//tmp/t", [row])
            assert yson.loads(clique.make_query("select * from `//tmp/t`")[0]["x"].encode()) == row["x"]

    @authors("babenko")
    def test_any_upcast(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            schema = [
                {"name": "int64", "type": "int64"},
                {"name": "uint64", "type": "uint64"},
                {"name": "boolean", "type": "boolean"},
                {"name": "double", "type": "double"},
                {"name": "float", "type": "float"},
                {"name": "string", "type": "string"},
                {"name": "datetime", "type": "datetime"},
                {"name": "date", "type": "date"},
                {"name": "timestamp", "type": "timestamp"},
                {"name": "interval", "type": "interval"},
                {"name": "any", "type": "any"},
                {"name": "null", "type": "null"},
                {"name": "void", "type": "void"},
            ]
            create("table", "//tmp/s1", attributes={"schema": schema, "optimize_for": "scan"})
            create(
                "table",
                "//tmp/s2",
                attributes={"schema": [{"name": x["name"], "type": "any"} for x in schema], "optimize_for": "scan"},
            )
            row = {
                "int64": -123,
                "uint64": 456,
                "boolean": True,
                "double": 3.14,
                "float": -2.0,
                "string": "text",
                "datetime": 1,
                "date": 2,
                "timestamp": 3,
                "interval": 4,
                "any": {"hello": "world"},
                "null": None,
                "void": None,
            }
            null_row = {key: None for key, value in row.items()}
            write_table("//tmp/s1", [row, null_row])
            merge(in_="//tmp/s1", out="//tmp/s2")
            fields = ""
            for column in schema:
                if len(fields) > 0:
                    fields += ", "
                fields += "ConvertYson({}, 'text') as {}".format(column["name"], column["name"])
            results = clique.make_query("select {} from `//tmp/s2`".format(fields))
            for key, value in results[0].items():
                assert (yson.loads(value.encode()) if value is not None else None) == row[key]
            for key, value in results[1].items():
                assert value is None

    @authors("babenko")
    def test_missing_column_becomes_null(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            create("table", "//tmp/s1", attributes={"schema": [{"name": "a", "type": "int64"}], "optimize_for": "scan"})
            create(
                "table",
                "//tmp/s2",
                attributes={
                    "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}],
                    "optimize_for": "scan",
                },
            )
            write_table("//tmp/s1", [{"a": 123}])
            merge(in_="//tmp/s1", out="//tmp/s2")
            assert clique.make_query("select * from `//tmp/s2`")[0] == {"a": 123, "b": None}

    @authors("babenko")
    def test_date_types(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "datetime", "type": "datetime"},
                    {"name": "date", "type": "date"},
                    {"name": "timestamp", "type": "timestamp"},
                    {"name": "interval_", "type": "interval"},
                ],
                "optimize_for": "scan",
            },
        )
        write_table(
            "//tmp/t1",
            [
                {
                    "datetime": 1,
                    "date": 2,
                    "timestamp": 3,
                    "interval_": 4,
                },
            ],
        )
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            expected_result = [
                {
                    "datetime": "1970-01-01 00:00:01",
                    "date": "1970-01-03",
                    "timestamp": 3,
                    "interval_": 4,
                }
            ]
            assert clique.make_query(
                "select toTimeZone(datetime, 'UTC') as datetime, date, timestamp, interval_ from \"//tmp/t1\""
            ) == expected_result

    @authors("babenko")
    def test_nonuniform_nullability(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "x", "type": "string", "required": True},
                ],
                "optimize_for": "scan",
            },
        )
        write_table("//tmp/t1", [{"x": "hello"}])
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "x", "type": "string", "required": False},
                ],
                "optimize_for": "scan",
            },
        )
        write_table("//tmp/t2", [{"x": None}, {"x": "world"}])
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            assert clique.make_query('select * from concatYtTables("//tmp/t1", "//tmp/t2") order by x') == [
                {"x": "hello"},
                {"x": "world"},
                {"x": None},
            ]

    @authors("babenko")
    def test_integral_upcast(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "x", "type": "int32", "required": True},
                ],
                "optimize_for": "scan",
            },
        )
        write_table("//tmp/t1", [{"x": 1}])
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "x", "type": "int64", "required": True},
                ],
                "optimize_for": "scan",
            },
        )
        write_table("//tmp/t2", [{"x": 2}])
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "x", "type": "int64", "required": True},
                ],
                "optimize_for": "scan",
            },
        )
        merge(in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t")
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            assert clique.make_query('select * from "//tmp/t" order by x') == [{"x": 1}, {"x": 2}]
