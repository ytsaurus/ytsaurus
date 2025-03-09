from yt_commands import (create, authors, write_table, insert_rows, get, sync_reshard_table, sync_mount_table,
                         read_table, get_singular_chunk_id, copy, raises_yt_error, alter_table, sync_unmount_table,
                         print_debug, select_rows)

from yt_type_helpers import optional_type

from yt_helpers import skip_if_no_descending

from base import ClickHouseTestBase, Clique, QueryFailedError

from .helpers import get_disabled_cache_config

import yt.yson as yson

from yt.common import wait

from yt.test_helpers import assert_items_equal

import random
import builtins
import datetime

import pytest


class TestInputFetching(ClickHouseTestBase):
    NUM_TEST_PARTITIONS = 6

    @authors("max42", "evgenstf")
    @pytest.mark.parametrize("where_prewhere", ["where", "prewhere"])
    def test_chunk_filter(self, where_prewhere):
        create("table", "//tmp/t", attributes={"schema": [{"name": "i", "type": "int64", "sort_order": "ascending"}]})
        for i in range(10):
            write_table("<append=%true>//tmp/t", [{"i": i}])

        config_patch = {
            "yt": {
                "settings": {
                    "execution": {
                        "enable_min_max_filtering": False,
                    },
                }
            },
            "clickhouse": {
                "settings": {
                    "optimize_move_to_prewhere": False,
                }
            }
        }
        with Clique(1, config_patch=config_patch) as clique:
            clique.make_query_and_validate_read_row_count(
                'select * from "//tmp/t" {} i >= 3'.format(where_prewhere), exact=7
            )
            clique.make_query_and_validate_read_row_count('select * from "//tmp/t" {} i < 2'.format(where_prewhere), exact=2)
            clique.make_query_and_validate_read_row_count(
                'select * from "//tmp/t" {} 5 <= i and i <= 8'.format(where_prewhere), exact=4
            )
            clique.make_query_and_validate_read_row_count(
                'select * from "//tmp/t" {} i in (-1, 2, 8, 8, 15)'.format(where_prewhere), exact=2
            )

    @authors("max42")
    @pytest.mark.parametrize("enable_computed_column_deduction", [False, True])
    def test_computed_column_chunk_filter(self, enable_computed_column_deduction):
        # See also: computed_columns_ut.cpp.

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "c", "type": "int64", "sort_order": "ascending", "expression": "i * 2"},
                    {"name": "i", "type": "int64", "sort_order": "ascending"},
                ]
            },
        )
        for i in range(5):
            write_table("<append=%true>//tmp/t", [{"i": 2 * i}, {"i": 2 * i + 1}])

        with Clique(
                1,
                config_patch={
                    "yt": {"settings": {"enable_computed_column_deduction": enable_computed_column_deduction,
                                        "execution": {"enable_min_max_filtering": False}}},
                    "clickhouse": {"settings": {"optimize_move_to_prewhere": 0}},
                },
        ) as clique:
            def correct_row_count(row_count):
                return row_count if enable_computed_column_deduction else 10

            clique.make_query_and_validate_read_row_count(
                'select * from "//tmp/t" where i == 3', exact=correct_row_count(2)
            )
            clique.make_query_and_validate_read_row_count(
                'select * from "//tmp/t" where i == 6 or i == 7', exact=correct_row_count(2)
            )
            clique.make_query_and_validate_read_row_count(
                'select * from "//tmp/t" where i == 0 or i == 9', exact=correct_row_count(4)
            )
            clique.make_query_and_validate_read_row_count(
                'select * from "//tmp/t" where i in (-1, 2, 8, 8, 15)', exact=correct_row_count(4)
            )
            clique.make_query_and_validate_read_row_count(
                'select * from "//tmp/t" where i in tuple(-1, 2, 8, 8, 15)', exact=correct_row_count(4)
            )
            clique.make_query_and_validate_read_row_count(
                'select * from "//tmp/t" where i in (1)', exact=correct_row_count(2)
            )
            clique.make_query_and_validate_read_row_count(
                'select * from "//tmp/t" where i in tuple(1)', exact=correct_row_count(2)
            )

            # This case should not be optimized.
            clique.make_query_and_validate_read_row_count('select * from "//tmp/t" where 5 <= i and i <= 8', exact=10)

    @authors("max42")
    def test_dynamic_table_farm_hash(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {
                        "name": "computed_key",
                        "type": "uint64",
                        "sort_order": "ascending",
                        "expression": "farm_hash(key)",
                    },
                    {"name": "key", "type": "string", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
                "dynamic": True,
                "enable_dynamic_store_read": True,
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
            },
        )
        tablet_count = 100
        sync_reshard_table(
            "//tmp/t", [[]] + [[yson.YsonUint64(i * 2 ** 64 // tablet_count)] for i in range(tablet_count)]
        )
        sync_mount_table("//tmp/t")
        key_count = 5
        for i in range(key_count):
            insert_rows("//tmp/t", [{"key": "k" + str(i), "value": "v" + str(i)}])

        with Clique(
                1,
                config_patch={
                    "yt": {
                        "settings": {
                            "enable_computed_column_deduction": True,
                            "execution": {
                                "enable_min_max_filtering": False
                            }
                        }
                    },
                    "clickhouse": {
                        "settings": {
                            "optimize_move_to_prewhere": False,
                        }
                    }
                },
        ) as clique:
            clique.make_query_and_validate_read_row_count("select * from `//tmp/t`", exact=5)
            clique.make_query_and_validate_read_row_count("select * from `//tmp/t` where key == 'k1' or key = 'k3'", exact=2)
            clique.make_query_and_validate_read_row_count(
                "select * from (select * from `//tmp/t` where key == 'k4')", exact=1
            )

    @authors("max42")
    def test_dynamic_table_farm_hash_two_components(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {
                        "name": "computed_key",
                        "type": "uint64",
                        "sort_order": "ascending",
                        "expression": "farm_hash(key, subkey)",
                    },
                    {"name": "key", "type": "string", "sort_order": "ascending"},
                    {"name": "subkey", "type": "string", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
                "dynamic": True,
                "enable_dynamic_store_read": True,
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
            },
        )
        tablet_count = 100
        sync_reshard_table(
            "//tmp/t", [[]] + [[yson.YsonUint64(i * 2 ** 64 // tablet_count)] for i in range(tablet_count)]
        )
        sync_mount_table("//tmp/t")
        key_count = 5
        for i in range(key_count):
            insert_rows("//tmp/t", [{"key": "k" + str(i), "subkey": "sk" + str(i), "value": "v" + str(i)}])

        config_patch = {
            "yt": {
                "settings": {
                    "enable_computed_column_deduction": True,
                    "execution": {
                        "enable_min_max_filtering": False
                    }
                }
            },
            "clickhouse": {
                "settings": {
                    "optimize_move_to_prewhere": False,
                }
            }
        }

        with Clique(1, config_patch=config_patch) as clique:
            assert len(clique.make_query_and_validate_read_row_count("select * from `//tmp/t`", exact=5)) == 5
            assert (
                len(
                    clique.make_query_and_validate_read_row_count(
                        "select * from `//tmp/t` where "
                        # TODO (buyval01): CHYT-1254
                        # "(key, subkey) == ('k1', 'sk1') or (key, subkey) = ('k3', 'sk3')",
                        "(key, subkey) in (('k1', 'sk1')) or (key, subkey) in (('k3', 'sk3'))",
                        exact=2,
                    )
                )
                == 2
            )
            assert (
                len(
                    clique.make_query_and_validate_read_row_count(
                        # TODO (buyval01): CHYT-1254
                        # "select * from (select * from `//tmp/t` where (key, subkey) == ('k4', 'sk4'))", exact=1
                        "select * from (select * from `//tmp/t` where (key, subkey) in (('k4', 'sk4')))", exact=1
                    )
                )
                == 1
            )

    @authors("dakovalkov")
    def test_common_schema_sorted(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "string", "sort_order": "ascending"},
                    {"name": "c", "type": "double"},
                ]
            },
        )
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "c", "type": "double"},
                ]
            },
        )
        create(
            "table",
            "//tmp/t3",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "c", "type": "double"},
                ]
            },
        )

        write_table("//tmp/t1", {"a": 42, "b": "x", "c": 3.14})
        write_table("//tmp/t2", {"a": 18, "c": 2.71})
        write_table("//tmp/t3", {"a": 18, "c": 2.71})

        with Clique(1, config_patch={"clickhouse": {"settings": {"optimize_move_to_prewhere": 0}},
                                     "yt": {"settings": {"execution": {"enable_min_max_filtering": False}}}}) as clique:
            # Column 'a' is sorted.
            clique.make_query_and_validate_read_row_count(
                'select * from concatYtTables("//tmp/t1", "//tmp/t2") where a > 18', exact=1
            )
            # Column 'a' isn't sorted.
            clique.make_query_and_validate_read_row_count(
                'select * from concatYtTables("//tmp/t1", "//tmp/t3") where a > 18', exact=2
            )

    @authors("dakovalkov", "buyval01")
    @pytest.mark.parametrize("required", [False, True])
    def test_key_types(self, required):
        int_types = ["uint64", "uint32", "uint16", "uint8", "int64", "int32", "int16", "int8"]
        int_values = [i * 2 for i in range(3)]
        # TODO(dakovalkov): For some reason KeyCondition can not construct from Nullable(int8) now.
        # Delete this test till ClickHouse fix this.
        if not required:
            int_types.pop()

        float_types = ["float", "double"]
        float_values = [i * 2.0 for i in range(3)]

        string_types = ["string"]
        string_values = ["{abc=2}", "{zzz=3}"]

        day_scale = 1
        second_scale = 24 * 60 * 60
        microsecond_scale = second_scale * 10**6
        date_types = ["date", "date32", "datetime", "datetime64", "timestamp", "timestamp64"]
        date_scales = [day_scale, day_scale, second_scale, second_scale, microsecond_scale, microsecond_scale]
        date_values = [i * 10 for i in range(3)]

        # Note: Interval data type in CH is auxiliary and is not supported for data storage,
        # so this behavior is only appropriate for CHYT.
        interval_types = ["interval", "interval64"]
        interval_values = [i for i in range(3)]

        def create_type_table(type, values):
            path = "//tmp/t_{}".format(type)
            create(
                "table",
                path,
                attributes={
                    "schema": [
                        {
                            "name": "key",
                            "type": type,
                            "sort_order": "ascending",
                            "required": required,
                        },
                    ],
                }
            )
            for value in values:
                write_table("<append=%true>" + path, [{"key": value}])

        for type in int_types:
            create_type_table(type, int_values)

        for type in float_types:
            create_type_table(type, float_values)

        for type in string_types:
            create_type_table(type, string_values)

        for type, scale in zip(date_types, date_scales):
            create_type_table(type, [scale * value for value in date_values])

        for type in interval_types:
            create_type_table(type, interval_values)

        config_patch = {
            "yt": {
                "settings": {
                    "execution": {
                        "enable_min_max_filtering": False
                    }
                }
            },
            "clickhouse": {
                "settings": {
                    "optimize_move_to_prewhere": False,
                }
            }
        }
        with Clique(1, config_patch=config_patch) as clique:
            query1 = 'select * from "//tmp/t_{}" where key = 2'
            query2 = 'select * from "//tmp/t_{}" where 1 < key and key < 3'
            for type in (int_types + float_types):
                clique.make_query_and_validate_read_row_count(query1.format(type), exact=1)
                clique.make_query_and_validate_read_row_count(query2.format(type), exact=1)

            query = 'select * from "//tmp/t_{}" where key = \'{{abc=2}}\''
            for type in string_types:
                clique.make_query_and_validate_read_row_count(query.format(type), exact=1)

            query = 'select * from "//tmp/t_{}" where \'1970.01.10\' < key and key < \'1970.01.12\''
            for type in date_types:
                clique.make_query_and_validate_read_row_count(query.format(type), exact=1)

            query = 'select * from "//tmp/t_{}" where INTERVAL 1 Microsecond < key and key < INTERVAL 3 Microsecond'
            for type in interval_types:
                clique.make_query_and_validate_read_row_count(query.format(type), exact=1)

    @authors("max42")
    @pytest.mark.xfail(run="False", reason="Chunk slicing is temporarily not supported")
    def test_chunk_slicing(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "chunk_writer": {"block_size": 1024},
                "compression_codec": "none",
                # TODO(max42): investigate what happens when both columns are sorted.
                "schema": [{"name": "i", "type": "int64", "sort_order": "ascending"}, {"name": "s", "type": "string"}],
            },
        )

        write_table("//tmp/t", [{"i": i, "s": str(i) * (10 * 1024)} for i in range(10)], verbose=False)
        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#" + chunk_id + "/@compressed_data_size") > 100 * 1024
        assert get("#" + chunk_id + "/@max_block_size") < 20 * 1024

        config_patch = {
            "yt": {
                "settings": {
                    "execution": {
                        "enable_min_max_filtering": False
                    }
                }
            },
            "clickhouse": {
                "settings": {
                    "optimize_move_to_prewhere": False
                }
            }
        }
        with Clique(1, config_patch=config_patch) as clique:
            # Due to inclusiveness issues each of the row counts should be correct with some error.
            clique.make_query_and_validate_read_row_count('select i from "//tmp/t" where i >= 3', min=7, max=8)
            clique.make_query_and_validate_read_row_count('select i from "//tmp/t" where i < 2', min=3, max=4)
            clique.make_query_and_validate_read_row_count('select i from "//tmp/t" where 5 <= i and i <= 8', min=4, max=6)
            clique.make_query_and_validate_read_row_count(
                'select i from "//tmp/t" where i in (-1, 2, 8, 8, 15)', min=2, max=4
            )

        # Forcefully disable chunk slicing.
        config_patch = {
            "yt": {
                "settings": {
                    "execution": {
                        "enable_min_max_filtering": False
                    }
                },
                "subquery": {
                    "max_sliced_chunk_count": 0
                }
            },
            "clickhouse": {
                "settings": {
                    "optimize_move_to_prewhere": False
                }
            }
        }
        with Clique(1, config_patch=config_patch) as clique:
            # Due to inclusiveness issues each of the row counts should be correct with some error.
            clique.make_query_and_validate_read_row_count('select i from "//tmp/t" where i >= 3', exact=10)
            clique.make_query_and_validate_read_row_count('select i from "//tmp/t" where i < 2', exact=10)
            clique.make_query_and_validate_read_row_count('select i from "//tmp/t" where 5 <= i and i <= 8', exact=10)
            clique.make_query_and_validate_read_row_count('select i from "//tmp/t" where i in (-1, 2, 8, 8, 15)', exact=10)

    @authors("max42", "gritukan")
    @pytest.mark.parametrize("use_block_sampling", [False, True])
    def test_sampling(self, use_block_sampling):
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}],
                "chunk_writer": {"block_size": 1024},
            },
        )
        write_table("//tmp/t", [{"a": i, "b": "A" * 1500} for i in range(1000)], verbose=False)
        with Clique(1, config_patch={"yt": {"settings": {"execution": {"enable_min_max_filtering": False}}}}) as clique:
            settings = {"chyt.use_block_sampling": int(use_block_sampling)}
            clique.make_query_and_validate_read_row_count('select a from "//tmp/t" sample 0.1', min=60, max=170,
                                                          verbose=False, settings=settings)
            clique.make_query_and_validate_read_row_count('select a from "//tmp/t" sample 100', min=60, max=170,
                                                          verbose=False, settings=settings)
            clique.make_query_and_validate_read_row_count('select a from "//tmp/t" sample 2/20', min=60, max=170,
                                                          verbose=False, settings=settings)
            clique.make_query_and_validate_read_row_count('select a from "//tmp/t" sample 0.1 offset 42', min=60, max=170,
                                                          verbose=False, settings=settings)
            clique.make_query_and_validate_read_row_count('select a from "//tmp/t" sample 10000', exact=1000, verbose=False,
                                                          settings=settings)
            clique.make_query_and_validate_read_row_count('select a from "//tmp/t" sample 10000', exact=1000, verbose=False,
                                                          settings=settings)
            clique.make_query_and_validate_read_row_count('select a from "//tmp/t" sample 0', exact=0, verbose=False,
                                                          settings=settings)
            clique.make_query_and_validate_read_row_count('select a from "//tmp/t" sample 0.000000000001', exact=0,
                                                          verbose=False, settings=settings)
            clique.make_query_and_validate_read_row_count('select a from "//tmp/t" sample 1/100000000000', exact=0,
                                                          verbose=False, settings=settings)

    @authors("max42")
    def test_chyt_143(self):
        # Issues with chunk name table ids, read schema ids and unversioned value row indices.
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}]},
        )
        create(
            "table",
            "//tmp/t2",
            attributes={"schema": [{"name": "b", "type": "string"}, {"name": "a", "type": "int64"}]},
        )
        write_table("//tmp/t1", [{"a": 42, "b": "asd"}])
        write_table("//tmp/t2", [{"b": "qwe", "a": 27}])
        with Clique(1) as clique:
            result = clique.make_query("select * from concatYtTables('//tmp/t1', '//tmp/t2')")
            assert len(result) == 2
            assert len(result[0]) == 2

    @authors("max42")
    def test_duplicating_table_functions(self):
        # CHYT-194.
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        write_table("//tmp/t", [{"a": 42}])
        with Clique(1) as clique:
            result = clique.make_query(
                "select * from concatYtTables('//tmp/t') union all select * from concatYtTables('//tmp/t')"
            )
            assert result == [{"a": 42}, {"a": 42}]

    @authors("max42")
    def disabled_test_min_data_weight_per_thread(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
        write_table("//tmp/t", [{"a": "x" * 100} for _ in range(30)])

        def get_momentary_stats(instances):
            stats = []
            for instance in instances:
                wait(lambda: clique.get_orchid(instance, "/queries/users/root", verbose=False) is not None)
                query_registry = clique.get_orchid(instance, "/queries/users/root")
                stats.append(
                    (
                        (query_registry["historical_initial_query_count"]),
                        query_registry["historical_secondary_query_count"],
                    )
                )
            return stats

        def get_delta_stats(instances, initial_instance, query):
            old_stats = get_momentary_stats(instances)
            clique.make_direct_query(initial_instance, query, verbose=False)
            new_stats = get_momentary_stats(instances)
            return [(rhs[0] - lhs[0], rhs[1] - lhs[1]) for lhs, rhs in zip(old_stats, new_stats)]

        with Clique(3) as clique:
            instances = clique.get_active_instances()
            assert len(instances) == 3
            initial_instance = instances[random.randint(0, 2)]
            delta_stats = get_delta_stats(instances, initial_instance, 'select * from "//tmp/t"')

            for delta_stat, instance in zip(delta_stats, instances):
                assert delta_stat[0] == (1 if instance == initial_instance else 0)
                assert delta_stat[1] == 1

        with Clique(3, config_patch={"yt": {"subquery": {"min_data_weight_per_thread": 5000}}}) as clique:
            instances = clique.get_active_instances()
            assert len(instances) == 3
            initial_instance = instances[random.randint(0, 2)]
            delta_stats = get_delta_stats(instances, initial_instance, 'select * from "//tmp/t"')

            for delta_stat, instance in zip(delta_stats, instances):
                assert delta_stat[0] == (1 if instance == initial_instance else 0)
                assert delta_stat[1] == (1 if instance == initial_instance else 0)

    @authors("max42")
    def test_duplicating_tables(self):
        create("map_node", "//tmp/d")
        create("table", "//tmp/d/t1", attributes={"schema": [{"name": "a", "type": "int64"}]})
        write_table("//tmp/d/t1", [{"a": 1}])
        with Clique(1) as clique:
            assert clique.make_query("select * from concatYtTables(`//tmp/d/t1`, `//tmp/d/t1`)") == [{"a": 1}] * 2
        copy("//tmp/d/t1", "//tmp/d/t2")
        with Clique(1) as clique:
            assert clique.make_query("select * from concatYtTables(`//tmp/d/t1`, `//tmp/d/t1`)") == [{"a": 1}] * 2
            assert clique.make_query("select * from concatYtTables(`//tmp/d/t1`, `//tmp/d/t1`, `//tmp/d/t2`, "
                                     "`//tmp/d/t2`, `//tmp/d/t2`)") == [{"a": 1}] * 5

    @authors("gudqeit")
    def test_reading_multiple_dynamic_tables(self):
        foo_data = [{"key": i, "value": "foo" + str(i)} for i in range(10)]

        for table_name in ("//tmp/dt_1", "//tmp/dt_2"):
            create(
                "table",
                table_name,
                attributes={
                    "dynamic": True,
                    "schema": [
                        {"name": "key", "type": "int64", "sort_order": "ascending"},
                        {"name": "value", "type": "string"},
                    ],
                    "enable_dynamic_store_read": True,
                    "dynamic_store_auto_flush_period": yson.YsonEntity(),
                },
            )
            sync_mount_table(table_name)
            insert_rows(table_name, foo_data)

        bar_data = [{"key": i, "value": "bar" + str(i)} for i in range(10)]
        sync_unmount_table("//tmp/dt_2")
        sync_mount_table("//tmp/dt_2")
        insert_rows("//tmp/dt_2", bar_data)

        with Clique(1) as clique:
            query = "select * from concatYtTables(`//tmp/dt_1`)"
            assert_items_equal(clique.make_query(query), foo_data)

            query = "select * from concatYtTables(`//tmp/dt_1`, `//tmp/dt_2`)"
            assert_items_equal(clique.make_query(query), foo_data + bar_data)

            query = "select * from concatYtTables(`//tmp/dt_1`, `//tmp/dt_1`)"
            assert_items_equal(clique.make_query(query), foo_data * 2)

            query = "select * from `//tmp/dt_1` as a join `//tmp/dt_2` as b using key where key = 0"
            assert_items_equal(clique.make_query(query), [{'key': 0, 'value': 'foo0', 'b.value': 'bar0'}])

    @authors("gudqeit")
    def test_reading_dynamic_and_static_tables(self):
        data = [{"key": 0, "value": "foo"}]

        create(
            "table",
            "//tmp/t_static",
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
            },
        )
        write_table("//tmp/t_static", data)

        create(
            "table",
            "//tmp/t_dynamic",
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
                "dynamic": True,
                "enable_dynamic_store_read": True,
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
            },
        )
        sync_mount_table("//tmp/t_dynamic")
        insert_rows("//tmp/t_dynamic", data)

        with Clique(1) as clique:
            query = "select * from concatYtTables(`//tmp/t_dynamic`, `//tmp/t_static`)"
            assert_items_equal(clique.make_query(query), data * 2)

            query = "select * from `//tmp/t_dynamic` as a join `//tmp/t_static` as b using key"
            assert_items_equal(clique.make_query(query), [{'key': 0, 'value': 'foo', 'b.value': 'foo'}])

            query = "select * from `//tmp/t_static` as a join `//tmp/t_dynamic` as b using key"
            assert_items_equal(clique.make_query(query), [{'key': 0, 'value': 'foo', 'b.value': 'foo'}])

    # CHYT-647
    @authors("dakovalkov")
    def test_long_chunk_key(self):
        create(
            "table",
            "//tmp/t0",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64", "sort_order": "ascending"},
                ],
            },
        )

        write_table("//tmp/t0", [{"a": 1, "b": 1}, {"a": 2, "b": 2}])
        write_table("<append=%true>//tmp/t0", [{"a": 3, "b": 3}])

        alter_table(
            "//tmp/t0",
            schema=[
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64", "sort_order": None},
            ],
        )

        with Clique(1) as clique:
            query = 'select a from "//tmp/t0" order by a'
            assert clique.make_query(query) == [{"a": 1}, {"a": 2}, {"a": 3}]

            query = 'select a from "//tmp/t0" where a = 1 order by a'
            assert clique.make_query(query) == [{"a": 1}]

    @authors("levysotsky")
    def test_renamed_columns_ascending(self):
        sort_order = "ascending"

        schema1 = [
            {"name": "a", "type": "int64", "sort_order": sort_order},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "double"},
        ]
        schema2 = [
            {"name": "a", "type": "int64", "sort_order": sort_order},
            {"name": "c_new", "type": "double", "stable_name": "c"},
            {"name": "b_new", "type": "string", "stable_name": "b"},
        ]
        schema3 = [
            {"name": "a", "type": "int64", "sort_order": sort_order},
            {"name": "c_new", "type": "double", "stable_name": "c"},
            {"name": "d", "type_v3": optional_type("int64")},
            {"name": "b_newer", "type": "string", "stable_name": "b"},
        ]
        schema4 = [
            {"name": "a", "type": "int64", "sort_order": sort_order},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "double"},
            {"name": "d", "type_v3": optional_type("int64")},
        ]

        table_path = "//tmp/t1"
        table_path_with_append = "<append=%true>" + table_path

        create(
            "table",
            table_path,
            attributes={
                "schema": schema1
            },
        )

        write_table(table_path, {"a": 42, "b": "x", "c": 3.14})

        with Clique(1, config_patch=get_disabled_cache_config()) as clique:
            assert clique.make_query('select * from "//tmp/t1" where a > 41') == [
                {"a": 42, "b": "x", "c": 3.14},
            ]
            assert clique.make_query('select b from "//tmp/t1" where a > 41') == [
                {"b": "x"},
            ]

            alter_table(table_path, schema=schema2)

            assert clique.make_query('select * from "//tmp/t1" where a > 41') == [
                {"a": 42, "b_new": "x", "c_new": 3.14},
            ]
            assert clique.make_query('select b_new from "//tmp/t1" where a > 41') == [
                {"b_new": "x"},
            ]

            write_table(table_path_with_append, {"a": 43, "b_new": "y", "c_new": 3.15})

            assert clique.make_query('select * from "//tmp/t1" where a > 42') == [
                {"a": 43, "b_new": "y", "c_new": 3.15}
            ]
            assert clique.make_query('select b_new from "//tmp/t1" where a > 42') == [
                {"b_new": "y"},
            ]

            alter_table(table_path, schema=schema3)

            # Column 'a' is sorted.
            assert clique.make_query('select * from "//tmp/t1" where a > 42') == [
                {"a": 43, "b_newer": "y", "c_new": 3.15, "d": None}
            ]
            assert clique.make_query('select b_newer, d from "//tmp/t1" where a > 42') == [
                {"b_newer": "y", "d": None},
            ]

            write_table(table_path_with_append, {"a": 44, "b_newer": "z", "c_new": 3.16, "d": 12})

            assert clique.make_query('select * from "//tmp/t1" where a > 42 order by a') == [
                {"a": 43, "b_newer": "y", "c_new": 3.15, "d": None},
                {"a": 44, "b_newer": "z", "c_new": 3.16, "d": 12},
            ]
            assert clique.make_query('select b_newer, d from "//tmp/t1" where a > 42 order by b_newer') == [
                {"b_newer": "y", "d": None},
                {"b_newer": "z", "d": 12},
            ]

            alter_table(table_path, schema=schema4)

            assert clique.make_query('select * from "//tmp/t1" where a > 42 order by a') == [
                {"a": 43, "b": "y", "c": 3.15, "d": None},
                {"a": 44, "b": "z", "c": 3.16, "d": 12},
            ]
            assert clique.make_query('select b, d from "//tmp/t1" where a > 42 order by b') == [
                {"b": "y", "d": None},
                {"b": "z", "d": 12},
            ]

    @authors("levysotsky")
    def test_renamed_columns_descending(self):
        skip_if_no_descending(self.Env)
        sort_order = "descending"

        schema1 = [
            {"name": "a", "type": "int64", "sort_order": sort_order},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "double"},
        ]
        schema2 = [
            {"name": "a", "type": "int64", "sort_order": sort_order},
            {"name": "c_new", "type": "double", "stable_name": "c"},
            {"name": "b_new", "type": "string", "stable_name": "b"},
        ]
        schema3 = [
            {"name": "a", "type": "int64", "sort_order": sort_order},
            {"name": "c_new", "type": "double", "stable_name": "c"},
            {"name": "d", "type_v3": optional_type("int64")},
            {"name": "b_newer", "type": "string", "stable_name": "b"},
        ]
        schema4 = [
            {"name": "a", "type": "int64", "sort_order": sort_order},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "double"},
            {"name": "d", "type_v3": optional_type("int64")},
        ]

        table_path = "//tmp/t1"
        table_path_with_append = "<append=%true>" + table_path

        create(
            "table",
            table_path,
            attributes={
                "schema": schema1
            },
        )

        write_table(table_path, {"a": 42, "b": "x", "c": 3.14})

        with Clique(1, config_patch=get_disabled_cache_config()) as clique:
            # Column 'a' is sorted.
            assert clique.make_query('select * from "//tmp/t1" where a > 41') == [
                {"a": 42, "b": "x", "c": 3.14},
            ]
            assert clique.make_query('select b from "//tmp/t1" where a > 41') == [
                {"b": "x"},
            ]

            alter_table(table_path, schema=schema2)

            # Column 'a' is sorted.
            assert clique.make_query('select * from "//tmp/t1" where a > 41') == [
                {"a": 42, "b_new": "x", "c_new": 3.14},
            ]
            assert clique.make_query('select b_new from "//tmp/t1" where a > 41') == [
                {"b_new": "x"},
            ]

            write_table(table_path_with_append, {"a": 41, "b_new": "y", "c_new": 3.15})

            # Column 'a' is sorted.
            assert clique.make_query('select * from "//tmp/t1" where a < 42') == [
                {"a": 41, "b_new": "y", "c_new": 3.15}
            ]
            assert clique.make_query('select b_new from "//tmp/t1" where a < 42') == [
                {"b_new": "y"},
            ]

            alter_table(table_path, schema=schema3)

            # Column 'a' is sorted.
            assert clique.make_query('select * from "//tmp/t1" where a < 42') == [
                {"a": 41, "b_newer": "y", "c_new": 3.15, "d": None}
            ]
            assert clique.make_query('select b_newer, d from "//tmp/t1" where a < 42') == [
                {"b_newer": "y", "d": None},
            ]

            write_table(table_path_with_append, {"a": 40, "b_newer": "z", "c_new": 3.16, "d": 12})

            assert clique.make_query('select * from "//tmp/t1" where a < 42 order by a') == [
                {"a": 40, "b_newer": "z", "c_new": 3.16, "d": 12},
                {"a": 41, "b_newer": "y", "c_new": 3.15, "d": None},
            ]
            assert clique.make_query('select b_newer, d from "//tmp/t1" where a < 42 order by b_newer') == [
                {"b_newer": "y", "d": None},
                {"b_newer": "z", "d": 12},
            ]

            alter_table(table_path, schema=schema4)

            assert clique.make_query('select * from "//tmp/t1" where a < 42 order by a') == [
                {"a": 40, "b": "z", "c": 3.16, "d": 12},
                {"a": 41, "b": "y", "c": 3.15, "d": None},
            ]
            assert clique.make_query('select b, d from "//tmp/t1" where a < 42 order by b') == [
                {"b": "y", "d": None},
                {"b": "z", "d": 12},
            ]

    @authors("levysotsky")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_renamed_columns_several_tables(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": sort_order},
                    {"name": "b", "type": "string", "sort_order": sort_order},
                    {"name": "c_new", "stable_name": "c", "type": "double"},
                ]
            },
        )
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": sort_order},
                    {"name": "c_new", "stable_name": "c", "type": "double"},
                ]
            },
        )

        write_table("//tmp/t1", {"a": 42, "b": "x", "c_new": 3.14})
        write_table("//tmp/t2", {"a": 18, "c_new": 2.71})

        with Clique(1) as clique:
            assert clique.make_query('select * from concatYtTables("//tmp/t1", "//tmp/t2") where a > 18') == [
                {"a": 42, "b": "x", "c_new": 3.14},
            ]

    @authors("levysotsky")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_different_stable_names(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": sort_order},
                    {"name": "b", "type": "string", "sort_order": sort_order},
                    {"name": "c_new", "stable_name": "c", "type": "double"},
                ]
            },
        )
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": sort_order},
                    {"name": "c_new", "stable_name": "c_other", "type": "double"},
                ]
            },
        )

        write_table("//tmp/t1", {"a": 42, "b": "x", "c_new": 3.14})
        write_table("//tmp/t2", {"a": 18, "c_new": 2.71})

        with Clique(1) as clique:
            with raises_yt_error(QueryFailedError):
                clique.make_query('select * from concatYtTables("//tmp/t1", "//tmp/t2") where a > 18')

    @authors("levysotsky")
    def test_different_names_same_stable_names(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "b_1", "stable_name": "b", "type": "int64"},
                ]
            },
        )
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "b_2", "stable_name": "b", "type": "int64"},
                ]
            },
        )

        write_table("//tmp/t1", {"a": 42, "b_1": 12})
        write_table("//tmp/t2", {"a": 18, "b_2": 78})

        with Clique(1) as clique:
            assert clique.make_query('select * from concatYtTables("//tmp/t1", "//tmp/t2") order by a') == [
                {"a": 18, "b_1": None, "b_2": 78},
                {"a": 42, "b_1": 12, "b_2": None},
            ]

    @authors("denvid")
    def test_chyt_banned(self):
        schema = [{"name": "a", "type": "int64"}]
        create(
            "table",
            "//tmp/table_banned",
            attributes={
                "chyt_banned": True,
                "schema": schema,
            },
        )
        create(
            "table",
            "//tmp/table_implicitly_not_banned",
            attributes={
                "schema": schema,
            },
        )
        create(
            "table",
            "//tmp/table_explicitly_not_banned",
            attributes={
                "chyt_banned": False,
                "schema": schema,
            },
        )
        data = [{"a": 1}]
        write_table("//tmp/table_banned", data)
        write_table("//tmp/table_implicitly_not_banned", data)
        write_table("//tmp/table_explicitly_not_banned", data)

        with Clique(1) as clique:
            with raises_yt_error(QueryFailedError):
                clique.make_query('select * from "//tmp/table_banned"')

            assert clique.make_query('select * from "//tmp/table_banned"', settings={'chyt.testing.check_chyt_banned': 0}) == data
            assert clique.make_query('select * from "//tmp/table_implicitly_not_banned"') == data
            assert clique.make_query('select * from "//tmp/table_explicitly_not_banned"') == data

            assert clique.make_query('exists "//tmp/table_surely_nonexisting"') == [{'result': 0}]
            assert clique.make_query('exists "//tmp/table_banned"', settings={'chyt.testing.check_chyt_banned': 0}) == [{'result': 1}]

            with raises_yt_error(QueryFailedError):
                clique.make_query('exists "//tmp/table_banned"', settings={'chyt.testing.check_chyt_banned': 1})

    @authors("denvid")
    def test_min_max_filtering(self):
        schema = [
            {"name": "a", "type": "int64"},
            {"name": "b", "type": "uint64"},
            {"name": "c", "type": "string"},
            {"name": "d", "type": "double"},
            {"name": "e", "type": "any"},
        ]
        table_path = "//tmp/t"
        create(
            "table",
            table_path,
            attributes={
                "schema": schema,
            },
        )

        write_table("<append=%true>" + table_path, [{"a": 5, "b": 8, "c": "polka", "d": 3.14, "e": "a"}])
        write_table("<append=%true>" + table_path, [{"a": -12, "b": 0, "c": "apple", "d": -1.414, "e": None}])
        write_table("<append=%true>" + table_path, [{"a": 65, "b": 74, "c": "ab" * 100, "d": None, "e": 2}])
        write_table("<append=%true>" + table_path, [{"a": 2, "b": 32, "c": "teapot", "d": None, "e": "c"}])
        write_table("<append=%true>" + table_path, [{"a": -33, "b": 91, "c": "ytsaurus", "d": 10.01, "e": []}])

        config_patch = {
            "yt": {
                "settings": {
                    "execution": {
                        "enable_min_max_filtering": True,
                    },
                }
            },
            "clickhouse": {
                "settings": {
                    # NB: prewhere filters rows before returning the block to CH,
                    # so "rows_read" is incorrect in this case.
                    "optimize_move_to_prewhere": False,
                }
            }
        }
        with Clique(1, config_patch=config_patch) as clique:
            assert clique.make_query_and_validate_read_row_count(f'select c from "{table_path}" where a > 2 or b == 0 order by c', exact=3) == \
                [{"c": "ab" * 100}, {"c": "apple"}, {"c": "polka"}]

            clique.make_query_and_validate_read_row_count(f'select d from "{table_path}" where c < \'axe\'', exact=2)

            clique.make_query_and_validate_read_row_count(f'select b from "{table_path}" where d is null', exact=2)
            clique.make_query_and_validate_read_row_count(f'select b from "{table_path}" where d is not null', exact=3)

            clique.make_query_and_validate_read_row_count(f'select b from "{table_path}" prewhere d is null', exact=2)

            # < and > conditions on 'any' columns should not prefilter any values.
            # But it may filter out chunks with only null values sometimes (depends on key condition details).
            clique.make_query_and_validate_read_row_count(f'select b from "{table_path}" where e < \'a\'', min=4, max=5)
            clique.make_query_and_validate_read_row_count(f'select b from "{table_path}" where e > \'a\'', min=4, max=5)
            # But isNull/isNotNull should work fine even with 'any' columns.
            clique.make_query_and_validate_read_row_count(f'select b from "{table_path}" where e is null', exact=1)
            clique.make_query_and_validate_read_row_count(f'select b from "{table_path}" where e is not null', exact=4)

    @authors("buyval01")
    def test_timestamp_key_filtering(self):
        create(
            "table",
            "//tmp/t-ts",
            attributes={
                "schema": [{
                    "name": "ts",
                    "type": "timestamp",
                    "sort_order": "ascending",
                }]
            }
        )

        write_table(
            "<append=%true>//tmp/t-ts",
            [
                {"ts": int(datetime.datetime(2023, 1, 1).timestamp() * 10**6)},
                {"ts": int(datetime.datetime(2023, 5, 5).timestamp() * 10**6)},
                {"ts": int(datetime.datetime(2023, 10, 10).timestamp() * 10**6)},
            ]
        )
        write_table(
            "<append=%true>//tmp/t-ts",
            [
                {"ts": int(datetime.datetime(2023, 12, 12).timestamp() * 10**6)},
                {"ts": int(datetime.datetime(2024, 2, 2).timestamp() * 10**6)},
            ]
        )
        write_table(
            "<append=%true>//tmp/t-ts",
            [
                {"ts": int(datetime.datetime(2024, 5, 5).timestamp() * 10**6)},
                {"ts": int(datetime.datetime(2024, 10, 10).timestamp() * 10**6)}
            ]
        )

        config_patch = {
            "yt": {
                "settings": {
                    "execution": {
                        "enable_min_max_filtering": False,
                    }
                }
            },
        }
        with Clique(1, config_patch=config_patch) as clique:
            query = "select * from '//tmp/t-ts' where ts > toDateTime('2024-01-01T00:00:00')"
            # the first chunk must be filtered and not read
            assert_items_equal(
                clique.make_query_and_validate_read_row_count(query, exact=4),
                [
                    {"ts": "2024-02-02 00:00:00.000000"},
                    {"ts": "2024-05-05 00:00:00.000000"},
                    {"ts": "2024-10-10 00:00:00.000000"},
                ])


class TestReadInOrder(ClickHouseTestBase):
    @staticmethod
    def make_query(clique, query, **kwargs):
        kwargs["full_response"] = True
        result = clique.make_query(query, **kwargs)
        return result.json()["data"], result.headers["X-ClickHouse-Query-Id"]

    @staticmethod
    def get_log_messages(log_table, *, query_id=None, query_ids=tuple(), type="QueryFinish", is_initial_query=1):
        if query_id is not None:
            assert not query_ids
            query_ids = (query_id,)
        assert query_ids

        query_id_filter = ", ".join(f"\"{query_id}\"" for query_id in query_ids)
        return select_rows(
            f"* from [{log_table}] where [initial_query_id] in ({query_id_filter}) and [type] = \"{type}\" and [is_initial_query] = {is_initial_query}",
            verbose=False)

    @staticmethod
    def wait_for_query_in_log(log_table, query_id=None, query_ids=tuple()):
        wait(lambda: len(TestReadInOrder.get_log_messages(log_table, query_id=query_id, query_ids=query_ids)) == (1 if query_id is not None else len(query_ids)))

    @staticmethod
    def get_total_block_rows_read(log_table, query_id):
        secondary_queries = TestReadInOrder.get_log_messages(log_table, query_id=query_id, is_initial_query=0)

        block_rows = 0

        if secondary_queries:
            block_rows = sum([
                secondary_query["chyt_query_statistics"]["secondary_query_source"]["steps"]["0"]["block_rows"]["sum"]
                for secondary_query in secondary_queries
            ])
        else:
            initial_query = TestReadInOrder.get_log_messages(log_table, query_id=query_id, is_initial_query=1)
            block_rows = initial_query["chyt_query_statistics"]["secondary_query_source"]["steps"]["0"]["block_rows"]["sum"]

        print_debug(f"Block rows read by query {query_id}: {block_rows}")
        return block_rows

    @staticmethod
    def get_read_in_order_modes(log_table, query_ids):
        read_in_order_modes = {}
        for log_message in TestReadInOrder.get_log_messages(log_table, query_ids=query_ids):
            assert log_message["is_initial_query"] == 1
            assert log_message["type"] == "QueryFinish"
            read_in_order_modes[log_message["query_id"]] = log_message["chyt_query_runtime_variables"].get("read_in_order_mode", "none")
        return read_in_order_modes

    @authors("achulkov2")
    @pytest.mark.parametrize("instance_count", [1, 2])
    def test_optimize_read_in_order_optimizes(self, instance_count):
        schema = [
            {"name": "ts", "type": "int64", "sort_order": "ascending"},
            {"name": "log_level", "type": "string"},
            {"name": "message", "type": "string"},
        ]

        table_count = 3
        chunk_count = 10
        chunk_row_count = 20

        total_row_count = chunk_row_count * chunk_count * table_count
        small_query_size = 0.1 * total_row_count

        ts_column = []

        for table_index in range(table_count):
            table_name = f"//tmp/table-{table_index}"
            create("table", table_name, attributes={"schema": schema, "chunk_writer": {"block_size": 16}})

            for chunk_index in range(chunk_count):
                write_table(f"<append=%true>{table_name}", [{
                    "ts": chunk_index * 256 + row_index,
                    "log_level": "info" if row_index % 2 else "debug",
                    "message": f"{row_index}, {chunk_index}, {table_index}"}
                    for row_index in range(chunk_row_count)])
                ts_column += [chunk_index * 256 + row_index for row_index in range(chunk_row_count)]

        with Clique(1, export_query_log=True, config_patch={
            "yt": {
                "settings": {
                    "execution": {
                        "enable_optimize_read_in_order": True,
                        "assume_no_null_keys": True,
                    },
                    "testing": {
                        # This is a workaround for simultaneous mounts when exporting query log in cliques with multiple instances.
                        "local_clique_size": instance_count,
                    },
                },
                "subquery": {
                    "min_data_weight_per_thread": 30,
                    "min_slice_data_weight": 1,
                },
            },
        }) as clique:
            log_table = clique.query_log_table_path

            result, query_id = self.make_query(
                clique,
                f'select message from concatYtTables("//tmp/table-0", "//tmp/table-1", "//tmp/table-2") where log_level = \'info\' and ts > {7 * 256 + 2} order by ts limit 6',
                settings={"chyt.execution.input_streams_per_secondary_query": 4})
            assert builtins.set(r["message"] for r in result[:3]) == {f"3, 7, {table_index}" for table_index in range(table_count)}
            assert builtins.set(r["message"] for r in result[3:]) == {f"5, 7, {table_index}" for table_index in range(table_count)}

            self.wait_for_query_in_log(log_table, query_id)
            assert self.get_total_block_rows_read(log_table, query_id) < small_query_size

            result, query_id = self.make_query(
                clique,
                'select message from concatYtTables("//tmp/table-0", "//tmp/table-1", "//tmp/table-2") where log_level = \'info\' order by ts desc limit 6',
                settings={"chyt.execution.input_streams_per_secondary_query": 4})
            assert builtins.set(r["message"] for r in result[:3]) == {f"19, 9, {table_index}" for table_index in range(table_count)}
            assert builtins.set(r["message"] for r in result[3:]) == {f"17, 9, {table_index}" for table_index in range(table_count)}

            self.wait_for_query_in_log(log_table, query_id)
            assert self.get_total_block_rows_read(log_table, query_id) < small_query_size

            result, query_id = self.make_query(
                clique,
                'select message from `//tmp/table-1` where log_level = \'debug\' and ts < 1024 order by ts desc limit 5',
                settings={"chyt.execution.input_streams_per_secondary_query": 64})
            assert result == [{"message": f"{row_index}, 3, 1"} for row_index in range(18, 8, -2)]

            self.wait_for_query_in_log(log_table, query_id)
            assert self.get_total_block_rows_read(log_table, query_id) < small_query_size / 3

            result, query_id = self.make_query(
                clique,
                'select message from concatYtTables("//tmp/table-0", "//tmp/table-1", "//tmp/table-2") where message = \'13, 8, 1\' order by ts limit 1',
                settings={"chyt.execution.input_streams_per_secondary_query": 4})
            assert result == [{"message": "13, 8, 1"}]

            self.wait_for_query_in_log(log_table, query_id)
            # This query should read at least half of the input.
            assert self.get_total_block_rows_read(log_table, query_id) > 0.5 * total_row_count

            result, query_id = self.make_query(
                clique,
                'select ts from concatYtTables("//tmp/table-0", "//tmp/table-1", "//tmp/table-2") order by ts limit 10000',
                settings={"chyt.execution.input_streams_per_secondary_query": 8}, verbose=False)
            assert [r["ts"] for r in result] == sorted(ts_column)

            result, query_id = self.make_query(
                clique,
                'select ts from concatYtTables("//tmp/table-0", "//tmp/table-1", "//tmp/table-2") order by ts desc limit 10000',
                settings={"chyt.execution.input_streams_per_secondary_query": 8}, verbose=False)
            assert [r["ts"] for r in result] == sorted(ts_column, reverse=True)

    def _register_query_check(self, expected_read_in_order_modes, clique, query, expected_read_in_order_mode, expected_result=None, query_settings=None):
        result, query_id = self.make_query(
            clique,
            query,
            settings=query_settings or {})
        if expected_result is not None:
            assert result == expected_result
        expected_read_in_order_modes[query_id] = (query, expected_read_in_order_mode)

    def _check_read_in_order_modes(self, log_table, expected_read_in_order_modes):
        self.wait_for_query_in_log(log_table, query_ids=tuple(expected_read_in_order_modes.keys()))
        read_in_order_modes = self.get_read_in_order_modes(log_table, query_ids=tuple(expected_read_in_order_modes.keys()))
        print_debug(expected_read_in_order_modes)
        print_debug(read_in_order_modes)
        for query_id, query_and_expected_result in expected_read_in_order_modes.items():
            assert read_in_order_modes[query_id] == query_and_expected_result[1], f"Mode differs from expected for query {query_and_expected_result[0]}"

    @authors("achulkov2")
    def test_optimize_read_in_order_nulls_and_nans(self):
        def generate_test_params(key_column_type, required, add_nulls, add_nans):
            assert key_column_type in {"int", "float"}

            table_name_prefix = f"//tmp/table-{key_column_type}-{required}-{add_nulls}-{add_nans}"

            schema = [
                {"name": "key", "type": "int64" if key_column_type == "int" else "float", "sort_order": "ascending", "required": required},
            ]

            table_count = 3

            data = [[5.7, 15.43], [20.07, 42.42]]
            if key_column_type == "int":
                data = [[int(str(k).replace(".", "")) for k in chunk_data] for chunk_data in data]
            if add_nulls:
                data[0] = [None] + data[0]
            if add_nans:
                assert key_column_type == "float"
                data[1] += [float("nan")]

            ordered_keys = sum(([x, x, x] for x in data[0] + data[1]), start=[])

            table_names = []
            for table_index in range(table_count):
                table_name = f"{table_name_prefix}-{table_index}"
                create("table", table_name, attributes={"schema": schema, "chunk_writer": {"block_size": 16}})
                table_names.append(table_name)

                for chunk_data in data:
                    write_table(f"<append=%true>{table_name}", [{"key": k} for k in chunk_data])

            concatenated_table_expression = ", ".join(f'"{name}"' for name in table_names)
            concatenated_table_expression = f"concatYtTables({concatenated_table_expression})"

            return {
                "table": concatenated_table_expression,
                "ordered_table": [{"key": k if str(k) != "nan" else str(k)} for k in ordered_keys],
            }

        with Clique(1, export_query_log=True, config_patch={
            "yt": {
                "settings": {
                    "execution": {
                        "enable_optimize_read_in_order": True,
                    },
                },
                "subquery": {
                    "min_data_weight_per_thread": 30,
                    "min_slice_data_weight": 1,
                },
            },
        }) as clique:
            log_table = clique.query_log_table_path

            expected_read_in_order_modes = {}

            def register_query_check(query, read_in_order_mode, result, settings=None):
                settings = settings or {}
                settings.update({"chyt.execution.input_streams_per_secondary_query": 8})
                self._register_query_check(expected_read_in_order_modes, clique, query, expected_read_in_order_mode=read_in_order_mode, expected_result=result, query_settings=settings)

            # settings={"chyt.execution.assume_no_null_keys": 1}
            test_params = generate_test_params(key_column_type="int", required=True, add_nulls=False, add_nans=False)
            register_query_check(
                f'select * from {test_params["table"]} order by key limit 10000',
                read_in_order_mode="forward", result=test_params["ordered_table"])
            register_query_check(
                f'select * from {test_params["table"]} order by key nulls first limit 10000',
                read_in_order_mode="forward", result=test_params["ordered_table"])
            register_query_check(
                f'select * from {test_params["table"]} order by key desc limit 10000',
                read_in_order_mode="backward", result=test_params["ordered_table"][::-1])
            register_query_check(
                f'select * from {test_params["table"]} order by key desc nulls first limit 10000',
                read_in_order_mode="backward", result=test_params["ordered_table"][::-1])

            test_params = generate_test_params(key_column_type="int", required=False, add_nulls=False, add_nans=False)
            register_query_check(
                f'select * from {test_params["table"]} order by key limit 10000',
                read_in_order_mode="none", result=test_params["ordered_table"])
            register_query_check(
                f'select * from {test_params["table"]} order by key limit 10000',
                read_in_order_mode="forward", result=test_params["ordered_table"], settings={"chyt.execution.assume_no_null_keys": 1})
            # Default for desc is fine.
            register_query_check(
                f'select * from {test_params["table"]} order by key desc limit 10000',
                read_in_order_mode="backward", result=test_params["ordered_table"][::-1])
            register_query_check(
                f'select * from {test_params["table"]} order by key desc limit 10000',
                read_in_order_mode="backward", result=test_params["ordered_table"][::-1], settings={"chyt.execution.assume_no_null_keys": 1})

            test_params = generate_test_params(key_column_type="int", required=False, add_nulls=True, add_nans=False)
            register_query_check(
                f'select * from {test_params["table"]} order by key nulls first limit 10000',
                read_in_order_mode="forward", result=test_params["ordered_table"])
            # Default for desc is fine.
            register_query_check(
                f'select * from {test_params["table"]} order by key desc limit 10000',
                read_in_order_mode="backward", result=test_params["ordered_table"][::-1])

            test_params = generate_test_params(key_column_type="float", required=True, add_nulls=False, add_nans=False)
            # Default direction for ASC is fine.
            register_query_check(
                f'select * from {test_params["table"]} order by key limit 10000',
                read_in_order_mode="forward", result=test_params["ordered_table"])
            # Still fine.
            register_query_check(
                f'select * from {test_params["table"]} order by key limit 10000',
                read_in_order_mode="forward", result=test_params["ordered_table"], settings={"chyt.execution.assume_no_nan_keys": 1})
            # Default direction for DESC is not fine.
            register_query_check(
                f'select * from {test_params["table"]} order by key desc limit 10000',
                read_in_order_mode="none", result=test_params["ordered_table"][::-1])
            register_query_check(
                f'select * from {test_params["table"]} order by key desc limit 10000',
                read_in_order_mode="backward", result=test_params["ordered_table"][::-1], settings={"chyt.execution.assume_no_nan_keys": 1})
            register_query_check(
                f'select * from {test_params["table"]} order by key desc nulls first limit 10000',
                read_in_order_mode="backward", result=test_params["ordered_table"][::-1])

            test_params = generate_test_params(key_column_type="float", required=True, add_nulls=False, add_nans=True)
            # Default direction for ASC is fine.
            register_query_check(
                f'select * from {test_params["table"]} order by key limit 10000',
                read_in_order_mode="forward", result=test_params["ordered_table"])
            register_query_check(
                f'select * from {test_params["table"]} order by key desc nulls first limit 10000',
                read_in_order_mode="backward", result=test_params["ordered_table"][::-1])

            test_params = generate_test_params(key_column_type="float", required=False, add_nulls=False, add_nans=False)
            # No optimization if there could be both NULLs and NANs.
            register_query_check(
                f'select * from {test_params["table"]} order by key nulls last limit 10000',
                read_in_order_mode="none", result=test_params["ordered_table"])
            # No optimization if there could be both NULLs and NANs.
            register_query_check(
                f'select * from {test_params["table"]} order by key nulls first limit 10000',
                read_in_order_mode="none", result=test_params["ordered_table"])
            # No optimization if there could be both NULLs and NANs.
            register_query_check(
                f'select * from {test_params["table"]} order by key desc nulls last limit 10000',
                read_in_order_mode="none", result=test_params["ordered_table"][::-1])
            # No optimization if there could be both NULLs and NANs.
            register_query_check(
                f'select * from {test_params["table"]} order by key desc nulls first limit 10000',
                read_in_order_mode="none", result=test_params["ordered_table"][::-1])
            # Let's just check some of the combinations, I am tired of writing this test.
            register_query_check(
                f'select * from {test_params["table"]} order by key nulls first limit 10000',
                read_in_order_mode="forward", result=test_params["ordered_table"], settings={"chyt.execution.assume_no_nan_keys": 1})
            register_query_check(
                f'select * from {test_params["table"]} order by key desc nulls first limit 10000',
                read_in_order_mode="backward", result=test_params["ordered_table"][::-1], settings={"chyt.execution.assume_no_null_keys": 1})
            register_query_check(
                f'select * from {test_params["table"]} order by key limit 10000',
                read_in_order_mode="forward", result=test_params["ordered_table"], settings={"chyt.execution.assume_no_null_keys": 1, "chyt.execution.assume_no_nan_keys": 1})

            test_params = generate_test_params(key_column_type="float", required=False, add_nulls=True, add_nans=False)
            register_query_check(
                f'select * from {test_params["table"]} order by key nulls first limit 10000',
                read_in_order_mode="forward", result=test_params["ordered_table"], settings={"chyt.execution.assume_no_nan_keys": 1})

            test_params = generate_test_params(key_column_type="float", required=False, add_nulls=False, add_nans=True)
            register_query_check(
                f'select * from {test_params["table"]} order by key desc nulls first limit 10000',
                read_in_order_mode="backward", result=test_params["ordered_table"][::-1], settings={"chyt.execution.assume_no_null_keys": 1})

            test_params = generate_test_params(key_column_type="float", required=False, add_nulls=True, add_nans=True)
            # There is nothing we can actually do to make to optimize these queries.
            register_query_check(
                f'select * from {test_params["table"]} order by key nulls last limit 10000',
                # None values will be at the end instead of the beginning.
                read_in_order_mode="none", result=test_params["ordered_table"][3:] + test_params["ordered_table"][:3])

            self._check_read_in_order_modes(log_table, expected_read_in_order_modes)

    @authors("achulkov2")
    @pytest.mark.parametrize("instance_count", [1, 2])
    def test_optimize_read_in_order_disabled(self, instance_count):
        schema = [
            {"name": "ts", "type": "int64", "sort_order": "ascending"},
            {"name": "ts_2", "type": "int64", "sort_order": "ascending"},
            {"name": "message", "type": "string"},
        ]

        chunk_count = 2
        chunk_row_count = 20

        table_name = "//tmp/table-0"
        create("table", table_name, attributes={"schema": schema, "chunk_writer": {"block_size": 16}})

        for chunk_index in range(chunk_count):
            write_table(f"<append=%true>{table_name}", [{
                "ts": chunk_index * 256 + row_index,
                "ts_2": chunk_index * 256 + row_index + 1543,
                "message": f"{row_index}, {chunk_index}, 0"}
                for row_index in range(chunk_row_count)])

        with Clique(1, export_query_log=True, config_patch={
            "yt": {
                "settings": {
                    "execution": {
                        "enable_optimize_read_in_order": True,
                        "assume_no_null_keys": True,
                    },
                    "testing": {
                        # This is a workaround for simultaneous mounts when exporting query log in cliques with multiple instances.
                        "local_clique_size": instance_count,
                    },
                },
                "subquery": {
                    "min_data_weight_per_thread": 30,
                    "min_slice_data_weight": 1,
                },
            },
        }) as clique:
            log_table = clique.query_log_table_path

            # Queries could have been a test parameter, but CHYT cliques take a while to start,
            # so we save execution time by performing the checks in one test invocation.
            # Also, query log takes a few seconds to write messages, so we run all checks in the end.
            expected_read_in_order_modes = {}

            def register_query_check(query, read_in_order_mode):
                settings = {"chyt.execution.input_streams_per_secondary_query": 8}
                self._register_query_check(expected_read_in_order_modes, clique, query, expected_read_in_order_mode=read_in_order_mode, query_settings=settings)

            register_query_check('select message from "//tmp/table-0" order by ts limit 1', read_in_order_mode="forward")
            register_query_check('select message from "//tmp/table-0" order by ts desc limit 1', read_in_order_mode="backward")
            # No LIMIT.
            register_query_check('select message from "//tmp/table-0" order by ts desc', read_in_order_mode="none")
            # Incompatible read directions.
            register_query_check('select message from "//tmp/table-0" order by ts desc, ts_2 limit 1', read_in_order_mode="none")
            # These are fine though.
            register_query_check('select message from "//tmp/table-0" order by ts desc, ts_2 desc limit 1', read_in_order_mode="backward")
            register_query_check('select message from "//tmp/table-0" order by ts, ts_2 limit 1', read_in_order_mode="forward")
            # No ORDER BY.
            register_query_check('select message from "//tmp/table-0" limit 5', read_in_order_mode="none")
            # Complex expressions in ORDER BY.
            register_query_check('select message from "//tmp/table-0" order by ts * 2 limit 1', read_in_order_mode="none")
            # It seems that CH flattens tuples in ORDER BY somewhere along the way, so this works.
            register_query_check('select message from "//tmp/table-0" order by (ts, ts_2) desc limit 1', read_in_order_mode="backward")
            # Positional arguments in ORDER BY are supported.
            register_query_check('select ts from "//tmp/table-0" order by 1 limit 1', read_in_order_mode="forward")
            # Aliases also work.
            register_query_check('with ts as ts2 select message from "//tmp/table-0" order by ts2 limit 1', read_in_order_mode="forward")
            # Ordering by non-key columns.
            register_query_check('select message from "//tmp/table-0" order by 1 limit 1', read_in_order_mode="none")
            register_query_check('select message from "//tmp/table-0" order by ts, ts_2, message limit 1', read_in_order_mode="none")

            self._check_read_in_order_modes(log_table, expected_read_in_order_modes)


class TestInputFetchingYPath(ClickHouseTestBase):
    def _create_table(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "ki", "type": "int64", "sort_order": "ascending"},
                    {"name": "ks", "type": "string", "sort_order": "ascending"},
                    {"name": "v", "type": "string"},
                ]
            },
        )
        rows = []
        for ki in range(9):
            for ks in ("abc", "def", "ghi"):
                rows.append({"ki": ki, "ks": ks, "v": str(ki) + ks})
            if ki % 3 == 2:
                write_table("<append=%true>//tmp/t", rows)
                rows = []

    @authors("max42", "dakovalkov")
    @pytest.mark.timeout(250)
    def test_ypath_simple(self):
        self._create_table()

        with Clique(1) as clique:
            # Simple form.
            def check_simple(lower_limit, upper_limit):
                if upper_limit is not None:
                    range_spec = "{}:{}".format(lower_limit, upper_limit)
                else:
                    range_spec = lower_limit
                table_path = "//tmp/t[{}]".format(range_spec)
                expected_rows = read_table(table_path)
                actual_rows = clique.make_query("select * from `{}` order by (ki, ks)".format(table_path))
                assert actual_rows == expected_rows

            for lower_limit in ("", "#3", "#12", "()", "(0)", '(0, "def")', "(1)", '(1, "def")'):
                check_simple(lower_limit, None)
                for upper_limit in ("", "#24", "#15", "()", "(2)", '(2, "ghi")', "(1)", '(1, "ghi")'):
                    check_simple(lower_limit, upper_limit)
                    check_simple(upper_limit, lower_limit)

    @authors("max42", "dakovalkov")
    @pytest.mark.timeout(250)
    def test_ypath_complex(self):
        self._create_table()

        yson_max = yson.to_yson_type(None, attributes={"type": "max"})
        yson_min = yson.to_yson_type(None, attributes={"type": "min"})
        yson_null = yson.to_yson_type(None)

        with Clique(1) as clique:
            # Complex form.
            def check_complex(lower_limit, upper_limit):
                if upper_limit is None:
                    range_spec = {"exact": lower_limit}
                else:
                    range_spec = {"lower": lower_limit, "upper": upper_limit}
                table_path = "<ranges=[{}]>//tmp/t".format(yson.dumps(range_spec, yson_format="text").decode())
                expected_rows = read_table(table_path)
                actual_rows = clique.make_query("select * from `{}` order by (ki, ks)".format(table_path))
                assert actual_rows == expected_rows

            for lower_limit in (
                    {},
                    {"row_index": 3},
                    {"row_index": 12},
                    {"key": []},
                    {"key": [0]},
                    {"key": [0, "def"]},
                    {"key": [1]},
                    {"key": [1, "def"]},
                    {"key": [0, yson_max]},
                    {"key": [0, yson_min]},
                    {"key": [0, yson_null]},
                    {"key": [1, yson_max]},
                    {"key": [1, yson_min]},
                    {"key": [1, yson_null]},
            ):
                # Empty exact range is invalid.
                if lower_limit != {}:
                    check_complex(lower_limit, None)
                for upper_limit in (
                        {},
                        {"row_index": 24},
                        {"row_index": 15},
                        {"key": []},
                        {"key": [2]},
                        {"key": [2, "ghi"]},
                        {"key": [1]},
                        {"key": [1, "ghi"]},
                        {"key": [2, yson_max]},
                        {"key": [2, yson_min]},
                        {"key": [2, yson_null]},
                        {"key": [1, yson_max]},
                        {"key": [1, yson_min]},
                        {"key": [1, yson_null]},
                ):
                    check_complex(lower_limit, upper_limit)
                    check_complex(upper_limit, lower_limit)
