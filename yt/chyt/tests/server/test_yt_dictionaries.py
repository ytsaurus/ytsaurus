from base import ClickHouseTestBase, Clique, QueryFailedError, enable_sequoia, enable_sequoia_acls

from helpers import get_async_expiring_cache_config, get_breakpoint_node, release_breakpoint, wait_breakpoint

from yt_commands import (authors, write_table, create, remove, raises_yt_error, insert_rows, sync_mount_table,
                         exists, read_table)

import yt.yson as yson

import time
import threading
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
            instances = clique.get_active_instances()
            for instance in instances:
                assert clique.make_direct_query(instance, test_query) == [{"value": 2}]

    @authors("buyval01")
    def test_name_collision(self):
        schema = [
            {"name": "a", "type": "uint64", "sort_order": "ascending", "required": True},
            {"name": "b", "type": "int64", "required": True},
        ]
        create("table", "//tmp/t", attributes={"schema": schema})
        write_table("//tmp/t", [{"a": 0, "b": 1}])

        def run_test(clique, name, table_path):
            clique.make_query(f"CREATE DICTIONARY `{name}` (`a` Int64, `b` Int64) PRIMARY KEY a SOURCE(Yt(Path '//tmp/t')) LAYOUT(FLAT()) LIFETIME(MIN 300 MAX 600);")

            # Create table after dictionary creation to prevent conflict.
            create("table", table_path, attributes={"schema": schema})

            # Test different resolve modes.
            with raises_yt_error(QueryFailedError):
                clique.make_query(f"select * from `{name}`")

            assert clique.make_query(f"select * from `{name}` settings chyt.storage_conflict_resolve_mode='yt'") == []
            assert clique.make_query(f"select * from `{name}` settings chyt.storage_conflict_resolve_mode='clique'") == [{"a": 0, "b": 1}]

            instances = clique.get_active_instances()

            clique.make_query(f"DROP TABLE `{name}`", settings={
                "chyt.storage_conflict_resolve_mode": "yt",
            })
            for inst in instances:
                assert clique.make_direct_query(inst, f"exists dictionary `{name}`") == [{"result": 1}]
            assert not exists(table_path)

            with raises_yt_error(QueryFailedError):
                # drop TABLE on the remaining DICTIONARY should raises.
                clique.make_query(f"DROP TABLE `{name}`")
            for inst in instances:
                assert clique.make_direct_query(inst, f"exists dictionary `{name}`") == [{"result": 1}]

            create("table", table_path, attributes={"schema": schema})

            def concurrent_drop_dictionary():
                wait_breakpoint("drop")
                clique.make_direct_query(instances[0], f"DROP DICTIONARY `{name}`", settings={
                    "chyt.storage_conflict_resolve_mode": "clique",
                })
                # ExternalLoader performs periodic updates every 5 seconds.
                # Let's wait for another instance to notice our deletion.
                time.sleep(10)
                release_breakpoint("drop")

            thread = threading.Thread(target=concurrent_drop_dictionary)
            thread.start()

            clique.make_direct_query(instances[1], f"DROP DICTIONARY `{name}`", settings={
                "chyt.storage_conflict_resolve_mode": "clique",
                "chyt.testing.drop_table_breakpoint": get_breakpoint_node("drop")
            })

            thread.join()

            assert exists(table_path)
            for inst in instances:
                assert clique.make_direct_query(inst, f"exists dictionary `{name}`") == [{"result": 0}]

        with Clique(2) as clique:
            run_test(clique, "//tmp/dict", "//tmp/dict")

        root = "//tmp/root"
        create("map_node", root)

        with Clique(2, config_patch={"yt": {"database_directories": {"my_db": root}}, "clickhouse": {"default_database": "my_db"}}) as clique:
            run_test(clique, "dict", "//tmp/root/dict")

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
            dictionary_path = f"//sys/strawberry/chyt/{test_alias}/storage_artifacts/t_dict"
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

    @authors("denmogilevec")
    def test_direct_dictionary(self):
        create(
            "table",
            "//tmp/dict",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "key", "type": "uint64", "required": True},
                    {"name": "value_str", "type": "string", "required": True},
                    {"name": "value_i64", "type": "int64", "required": True},
                ],
                "enable_dynamic_store_read": True,
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
            },
        )
        sync_mount_table("//tmp/dict")
        insert_rows("//tmp/dict", [{"key": i, "value_str": "str" + str(i), "value_i64": i * i} for i in [1, 3, 5]])

        with Clique(
                3,
                config_patch={
                    "clickhouse": {
                        "dictionaries": [
                            {
                                "name": "dict",
                                "layout": {"direct": {}},
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
                }, export_query_log=True) as clique:
            result = clique.make_query(
                "select number, dictGetString('dict', 'value_str', number) as str, "
                "dictGetInt64('dict', 'value_i64', number) as i64 from numbers(5)",
                full_response=True
            )
            rows = clique.wait_and_get_query_log_rows(result.headers["X-ClickHouse-Query-Id"])
            rows = read_table(clique.query_log_table_path)
            for row in rows:
                print(row)

            def match(row):
                return ("subquery_db.subquery" in row["tables"]) and row["type"] == "QueryFinish"
                # subquery_db.subquery is special clickhouse storage name for tables in queries generated by other queries

            assert len([row for row in rows if match(row)]) == 2
            # One row for initial query on current instance and one for secondary query on it
            assert result.json()["data"] == [
                {"number": 0, "str": "n/a", "i64": 42},
                {"number": 1, "str": "str1", "i64": 1},
                {"number": 2, "str": "n/a", "i64": 42},
                {"number": 3, "str": "str3", "i64": 9},
                {"number": 4, "str": "n/a", "i64": 42},
            ]

    @authors("denmogilevec")
    def test_composite_key_direct_dictionary(self):
        create(
            "table",
            "//tmp/dict",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "key", "type": "string", "required": True},
                    {"name": "value_str", "type": "string", "required": True},
                    {"name": "value_i64", "type": "int64", "required": True},
                ],
                "enable_dynamic_store_read": True,
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
            },
        )
        sync_mount_table("//tmp/dict")
        insert_rows("//tmp/dict", [{"key": i, "value_str": "str" + i, "value_i64": int(i)} for i in ["1", "3", "5"]])

        with Clique(
                1,
                config_patch={
                    "clickhouse": {
                        "dictionaries": [
                            {
                                "name": "dict",
                                "layout": {"complex_key_direct": {}},
                                "structure": {
                                    "key": {
                                        "attribute": [
                                            {"name": "key", "type": "String"},
                                        ],
                                    },
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
                """select dictGetString('dict', 'value_str', CAST(1 as String)) as str"""
            )
            assert result == [
                {"str": "str1"},
            ]


@enable_sequoia
@enable_sequoia_acls
class TestYtDictionariesSequoia(TestYtDictionaries):
    pass
