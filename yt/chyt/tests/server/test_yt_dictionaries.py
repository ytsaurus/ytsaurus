from base import ClickHouseTestBase, Clique, QueryFailedError

from helpers import get_async_expiring_cache_config

from yt_commands import (authors, write_table, create, remove, raises_yt_error, exists)

import time
from flaky import flaky


class TestYtDictionaries(ClickHouseTestBase):
    @authors("max42")
    def test_int_key_flat(self):
        create(
            "table",
            "//tmp/dict",
            attributes={
                "schema": [
                    {"name": "key", "type": "uint64", "required": True},
                    {"name": "value_str", "type": "string", "required": True},
                    {"name": "value_i64", "type": "int64", "required": True},
                ]
            },
        )
        write_table("//tmp/dict", [{"key": i, "value_str": "str" + str(i), "value_i64": i * i} for i in [1, 3, 5]])

        with Clique(
                1,
                config_patch={
                    "clickhouse": {
                        "dictionaries": [
                            {
                                "name": "dict",
                                "layout": {"flat": {}},
                                "structure": {
                                    "id": {"name": "key"},
                                    "attribute": [
                                        {"name": "value_str", "type": "String", "null_value": "n/a"},
                                        {"name": "value_i64", "type": "Int64", "null_value": 42},
                                    ],
                                },
                                "lifetime": 0,
                                "source": {"yt": {"path": "//tmp/dict"}},
                            }
                        ]
                    }
                },
        ) as clique:
            result = clique.make_query(
                "select number, dictGetString('dict', 'value_str', number) as str, "
                "dictGetInt64('dict', 'value_i64', number) as i64 from numbers(5)"
            )
        assert result == [
            {"number": 0, "str": "n/a", "i64": 42},
            {"number": 1, "str": "str1", "i64": 1},
            {"number": 2, "str": "n/a", "i64": 42},
            {"number": 3, "str": "str3", "i64": 9},
            {"number": 4, "str": "n/a", "i64": 42},
        ]

    @authors("max42")
    def test_composite_key_hashed(self):
        create(
            "table",
            "//tmp/dict",
            attributes={
                "schema": [
                    {"name": "key", "type": "string", "required": True},
                    {"name": "subkey", "type": "int64", "required": True},
                    {"name": "value", "type": "string", "required": True},
                ]
            },
        )
        write_table(
            "//tmp/dict",
            [
                {"key": "a", "subkey": 1, "value": "a1"},
                {"key": "a", "subkey": 2, "value": "a2"},
                {"key": "b", "subkey": 1, "value": "b1"},
            ],
        )

        create(
            "table",
            "//tmp/queries",
            attributes={
                "schema": [
                    {"name": "key", "type": "string", "required": True},
                    {"name": "subkey", "type": "int64", "required": True},
                ]
            },
        )
        write_table(
            "//tmp/queries",
            [
                {"key": "a", "subkey": 1},
                {"key": "a", "subkey": 2},
                {"key": "b", "subkey": 1},
                {"key": "b", "subkey": 2},
            ],
        )

        with Clique(
                1,
                config_patch={
                    "clickhouse": {
                        "dictionaries": [
                            {
                                "name": "dict",
                                "layout": {"complex_key_hashed": {}},
                                "structure": {
                                    "key": {
                                        "attribute": [
                                            {"name": "key", "type": "String"},
                                            {"name": "subkey", "type": "Int64"},
                                        ]
                                    },
                                    "attribute": [{"name": "value", "type": "String", "null_value": "n/a"}],
                                },
                                "lifetime": 0,
                                "source": {"yt": {"path": "//tmp/dict"}},
                            }
                        ]
                    }
                },
        ) as clique:
            result = clique.make_query(
                "select dictGetString('dict', 'value', tuple(key, subkey)) as value from \"//tmp/queries\""
            )
        assert result == [{"value": "a1"}, {"value": "a2"}, {"value": "b1"}, {"value": "n/a"}]

    @authors("max42")
    @flaky(max_runs=3)
    def test_lifetime(self):
        create(
            "table",
            "//tmp/dict",
            attributes={
                "schema": [
                    {"name": "key", "type": "uint64", "required": True},
                    {"name": "value", "type": "string", "required": True},
                ]
            },
        )
        write_table("//tmp/dict", [{"key": 42, "value": "x"}])

        patch = {
            # Disable background update.
            "yt": {
                "table_attribute_cache": get_async_expiring_cache_config(2000, 2000, None, None),
                "permission_cache": get_async_expiring_cache_config(2000, 2000, None, None),
            },
            "clickhouse": {
                "dictionaries": [
                    {
                        "name": "dict",
                        "layout": {"flat": {}},
                        "structure": {
                            "id": {"name": "key"},
                            "attribute": [{"name": "value", "type": "String", "null_value": "n/a"}],
                        },
                        "lifetime": 1,
                        "source": {"yt": {"path": "//tmp/dict"}},
                    }
                ]
            },
        }

        with Clique(1, config_patch=patch) as clique:
            assert (
                clique.make_query("select dictGetString('dict', 'value', CAST(42 as UInt64)) as value")[0]["value"]
                == "x"
            )

            write_table("//tmp/dict", [{"key": 42, "value": "y"}])
            # TODO(max42): make update time customizable in CH and reduce this constant.
            time.sleep(7)
            assert (
                clique.make_query("select dictGetString('dict', 'value', CAST(42 as UInt64)) as value")[0]["value"]
                == "y"
            )

            remove("//tmp/dict")
            time.sleep(7)
            assert (
                clique.make_query("select dictGetString('dict', 'value', CAST(42 as UInt64)) as value")[0]["value"]
                == "y"
            )

            create(
                "table",
                "//tmp/dict",
                attributes={
                    "schema": [
                        {"name": "key", "type": "uint64", "required": True},
                        {"name": "value", "type": "string", "required": True},
                    ]
                },
            )
            write_table("//tmp/dict", [{"key": 42, "value": "z"}])
            time.sleep(7)
            assert (
                clique.make_query("select dictGetString('dict', 'value', CAST(42 as UInt64)) as value")[0]["value"]
                == "z"
            )

    # CHYT-611
    @authors("dakovalkov")
    def test_dict_does_not_exist(self):
        with Clique(1) as clique:
            with raises_yt_error(QueryFailedError):
                clique.make_query("select dictGetString('this_dict_does_not_exist', 'value', 1)")

    @authors("denmogilevec")
    def test_create_dictionary(self):
        schema = [
            {"name": "a", "type": "uint64", "sort_order": "ascending", "required": True},
            {"name": "b", "type": "int64", "required": True},
        ]
        create("table", "//tmp/t", attributes={"schema": schema})
        write_table("//tmp/t", [{"a": i, "b": 2 * i} for i in range(3)])
        with Clique(2, alias="test_alias") as clique:
            clique.make_query("CREATE DICTIONARY t_dict (`a` Int64, `b` Int64) PRIMARY KEY a SOURCE(Yt(Path '//tmp/t')) LAYOUT(FLAT()) LIFETIME(MIN 300 MAX 600);")
            test_query = "Select dictGetInt64('t_dict', 'b', CAST(1 as Int64)) as value"
            assert clique.make_query(test_query) == [{"value": 2}]
            instances = clique.get_active_instances()
            for instance in instances:
                assert clique.make_direct_query(instance, test_query) == [{"value": 2}]

    @authors("denmogilevec")
    def test_dictionary_persistence(self):
        schema = [
            {"name": "a", "type": "uint64", "sort_order": "ascending", "required": True},
            {"name": "b", "type": "int64", "required": True},
        ]
        create("table", "//tmp/t", attributes={"schema": schema})
        write_table("//tmp/t", [{"a": i, "b": 2 * i} for i in range(3)])
        test_alias = "test_alias"
        with Clique(2, alias=test_alias) as clique:
            clique.make_query("CREATE DICTIONARY t_dict (`a` Int64, `b` Int64) PRIMARY KEY a SOURCE(Yt(Path '//tmp/t')) LAYOUT(FLAT()) LIFETIME(MIN 300 MAX 600);")

            test_query = "Select dictGetInt64('t_dict', 'b', CAST(1 as Int64)) as value"
            dictionary_path = f"//sys/strawberry/chyt/{test_alias}/dictionaries/t_dict"
            clique.op.suspend()
            time.sleep(5)
            clique.op.resume()
            assert clique.make_query(test_query) == [{"value": 2}]
            assert exists(dictionary_path)

    @authors("denmogilevec")
    def test_dictionary_underlying_table_invalidation(self):
        schema = [
            {"name": "a", "type": "uint64", "sort_order": "ascending", "required": True},
            {"name": "b", "type": "int64", "required": True},
        ]
        create("table", "//tmp/t", attributes={"schema": schema})
        write_table("//tmp/t", [{"a": i, "b": 2 * i} for i in range(3)])
        with Clique(2, alias="test_alias") as clique:
            instances = clique.get_active_instances()
            clique.make_query("CREATE DICTIONARY t_dict (`a` Int64, `b` Int64) PRIMARY KEY a SOURCE(Yt(Path '//tmp/t')) LAYOUT(FLAT()) LIFETIME(MIN 1 MAX 1);")
            test_query = "Select dictGetInt64('t_dict', 'b', CAST(1 as Int64)) as value"

            assert clique.make_query(test_query) == [{"value": 2}]

            write_table("//tmp/t", [{"a": i, "b": 3 * i} for i in range(3)])
            time.sleep(5)

            assert clique.make_direct_query(instances[0], test_query) == [{"value": 3}]
            assert clique.make_direct_query(instances[1], test_query) == [{"value": 3}]

    @authors("denmogilevec")
    def test_drop_dictionary(self):
        schema = [
            {"name": "a", "type": "uint64", "sort_order": "ascending", "required": True},
            {"name": "b", "type": "int64", "required": True},
        ]
        create("table", "//tmp/t", attributes={"schema": schema})
        write_table("//tmp/t", [{"a": i, "b": 2 * i} for i in range(3)])
        with Clique(2, alias="test_alias") as clique:
            instances = clique.get_active_instances()
            clique.make_query("CREATE DICTIONARY t_dict (`a` Int64, `b` Int64) PRIMARY KEY a SOURCE(Yt(Path '//tmp/t')) LAYOUT(FLAT()) LIFETIME(MIN 300 MAX 600);")
            test_query = "Select dictGetInt64('t_dict', 'b', CAST(1 as Int64)) as value"

            assert clique.make_query(test_query) == [{"value": 2}]

            clique.make_direct_query(instances[0], "DROP DICTIONARY t_dict")

            with raises_yt_error(QueryFailedError):
                clique.make_direct_query(instances[1], test_query)

    @authors("denmogilevec")
    def test_dictionary_ddl_consistency(self):
        schema1 = [
            {"name": "a", "type": "uint64", "sort_order": "ascending", "required": True},
            {"name": "b", "type": "int64", "required": True},
        ]
        schema2 = [
            {"name": "a", "type": "uint64", "sort_order": "ascending", "required": True},
            {"name": "b", "type": "string", "required": True},
        ]
        create("table", "//tmp/t1", attributes={"schema": schema1})
        create("table", "//tmp/t2", attributes={"schema": schema2})
        write_table("//tmp/t1", [{"a": i, "b": 2 * i} for i in range(3)])
        write_table("//tmp/t2", [{"a": i, "b": str(i)} for i in range(3)])
        with Clique(2, alias="test_alias") as clique:
            instances = clique.get_active_instances()
            clique.make_direct_query(instances[0], "CREATE DICTIONARY t_dict (`a` Int64, `b` Int64) PRIMARY KEY a SOURCE(Yt(Path '//tmp/t1')) LAYOUT(FLAT()) LIFETIME(MIN 300 MAX 600);")
            test_query_1 = "Select dictGetInt64('t_dict', 'b', CAST(1 as Int64)) as value"
            assert clique.make_direct_query(instances[1], test_query_1) == [{"value": 2}]

            clique.make_direct_query(instances[1], "DROP DICTIONARY t_dict")
            with raises_yt_error(QueryFailedError):
                clique.make_direct_query(instances[0], test_query_1)

            clique.make_direct_query(instances[1], "CREATE DICTIONARY t_dict (`a` Int64, `b` String) PRIMARY KEY a SOURCE(Yt(Path '//tmp/t2')) LAYOUT(FLAT()) LIFETIME(MIN 300 MAX 600);")
            test_query_2 = "Select dictGetString('t_dict', 'b', CAST(2 as Int64)) as value"
            assert clique.make_direct_query(instances[0], test_query_2) == [{"value": "2"}]

    @authors("denmogilevec")
    def test_drop_not_existing_dictionary(self):
        with Clique(1) as clique:
            with raises_yt_error(QueryFailedError):
                clique.make_query("DROP DICTIONARY this_dict_does_not_exist")
