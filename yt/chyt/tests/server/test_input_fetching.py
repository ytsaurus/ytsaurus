from yt_commands import (create, authors, write_table, insert_rows, get, sync_reshard_table, sync_mount_table,
                         read_table, get_singular_chunk_id, copy, raises_yt_error, alter_table)

from yt_type_helpers import optional_type

from yt_helpers import skip_if_no_descending

from base import ClickHouseTestBase, Clique, QueryFailedError

from .helpers import get_disabled_cache_config

import yt.yson as yson

from yt.common import wait

import random

import pytest


class TestInputFetching(ClickHouseTestBase):
    NUM_TEST_PARTITIONS = 2

    @authors("max42", "evgenstf")
    @pytest.mark.parametrize("where_prewhere", ["where", "prewhere"])
    def test_chunk_filter(self, where_prewhere):
        create("table", "//tmp/t", attributes={"schema": [{"name": "i", "type": "int64", "sort_order": "ascending"}]})
        for i in range(10):
            write_table("<append=%true>//tmp/t", [{"i": i}])
        with Clique(1) as clique:
            clique.make_query_and_validate_row_count(
                'select * from "//tmp/t" {} i >= 3'.format(where_prewhere), exact=7
            )
            clique.make_query_and_validate_row_count('select * from "//tmp/t" {} i < 2'.format(where_prewhere), exact=2)
            clique.make_query_and_validate_row_count(
                'select * from "//tmp/t" {} 5 <= i and i <= 8'.format(where_prewhere), exact=4
            )
            clique.make_query_and_validate_row_count(
                'select * from "//tmp/t" {} i in (-1, 2, 8, 8, 15)'.format(where_prewhere), exact=2
            )

    @authors("max42")
    def test_computed_column_chunk_filter(self):
        # See also: computed_columns_ut.cpp.

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "c", "type": "int64", "sort_order": "ascending", "expression": "i *  2"},
                    {"name": "i", "type": "int64", "sort_order": "ascending"},
                ]
            },
        )
        for i in range(5):
            write_table("<append=%true>//tmp/t", [{"i": 2 * i}, {"i": 2 * i + 1}])

        for enable_computed_column_deduction in (False, True):
            with Clique(
                    1,
                    config_patch={
                        "yt": {"settings": {"enable_computed_column_deduction": enable_computed_column_deduction}},
                        "clickhouse": {"settings": {"optimize_move_to_prewhere": 0}},
                    },
            ) as clique:
                def correct_row_count(row_count):
                    return row_count if enable_computed_column_deduction else 10

                clique.make_query_and_validate_row_count(
                    'select * from "//tmp/t" where i == 3', exact=correct_row_count(2)
                )
                clique.make_query_and_validate_row_count(
                    'select * from "//tmp/t" where i == 6 or i == 7', exact=correct_row_count(2)
                )
                clique.make_query_and_validate_row_count(
                    'select * from "//tmp/t" where i == 0 or i == 9', exact=correct_row_count(4)
                )
                clique.make_query_and_validate_row_count(
                    'select * from "//tmp/t" where i in (-1, 2, 8, 8, 15)', exact=correct_row_count(4)
                )
                clique.make_query_and_validate_row_count(
                    'select * from "//tmp/t" where i in tuple(-1, 2, 8, 8, 15)', exact=correct_row_count(4)
                )
                clique.make_query_and_validate_row_count(
                    'select * from "//tmp/t" where i in (1)', exact=correct_row_count(2)
                )
                clique.make_query_and_validate_row_count(
                    'select * from "//tmp/t" where i in tuple(1)', exact=correct_row_count(2)
                )

                # This case should not be optimized.
                clique.make_query_and_validate_row_count('select * from "//tmp/t" where 5 <= i and i <= 8', exact=10)

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
                    "yt": {"settings": {"enable_computed_column_deduction": True}}
                },
        ) as clique:
            clique.make_query_and_validate_row_count("select * from `//tmp/t`", exact=5)
            clique.make_query_and_validate_row_count("select * from `//tmp/t` where key == 'k1' or key = 'k3'", exact=2)
            clique.make_query_and_validate_row_count(
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

        with Clique(
                1,
                config_patch={
                    "yt": {"settings": {"enable_computed_column_deduction": True}}
                },
        ) as clique:
            assert len(clique.make_query_and_validate_row_count("select * from `//tmp/t`", exact=5)) == 5
            assert (
                len(
                    clique.make_query_and_validate_row_count(
                        "select * from `//tmp/t` where "
                        "(key, subkey) == ('k1', 'sk1') or (key, subkey) = ('k3', 'sk3')",
                        exact=2,
                    )
                )
                == 2
            )
            assert (
                len(
                    clique.make_query_and_validate_row_count(
                        "select * from (select * from `//tmp/t` where (key, subkey) == ('k4', 'sk4'))", exact=1
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

        with Clique(1, config_patch={"clickhouse": {"settings": {"optimize_move_to_prewhere": 0}}}) as clique:
            # Column 'a' is sorted.
            clique.make_query_and_validate_row_count(
                'select * from concatYtTables("//tmp/t1", "//tmp/t2") where a > 18', exact=1
            )
            # Column 'a' isn't sorted.
            clique.make_query_and_validate_row_count(
                'select * from concatYtTables("//tmp/t1", "//tmp/t3") where a > 18', exact=2
            )

    @authors("dakovalkov")
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

        date_types = ["date", "datetime"]  # , "timestamp"]
        date_scales = [1, 24 * 60 * 60]  # , 24 * 60 * 60 * 10**6]
        date_values = [i * 10 for i in range(3)]

        # TODO(dakovalkov): Delete this when timestamp is represented as DateTime64.
        int_types.append("timestamp")
        # Interval is represented as Int64.
        int_types.append("interval")

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

        with Clique(1) as clique:
            query1 = 'select * from "//tmp/t_{}" where key = 2'
            query2 = 'select * from "//tmp/t_{}" where 1 < key and key < 3'
            for type in (int_types + float_types):
                clique.make_query_and_validate_row_count(query1.format(type), exact=1)
                clique.make_query_and_validate_row_count(query2.format(type), exact=1)

            query = 'select * from "//tmp/t_{}" where key = \'{{abc=2}}\''
            for type in string_types:
                clique.make_query_and_validate_row_count(query.format(type), exact=1)

            query = 'select * from "//tmp/t_{}" where \'1970.01.10\' < key and key < \'1970.01.12\''
            for type in date_types:
                clique.make_query_and_validate_row_count(query.format(type), exact=1)

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

        with Clique(1) as clique:
            # Due to inclusiveness issues each of the row counts should be correct with some error.
            clique.make_query_and_validate_row_count('select i from "//tmp/t" where i >= 3', min=7, max=8)
            clique.make_query_and_validate_row_count('select i from "//tmp/t" where i < 2', min=3, max=4)
            clique.make_query_and_validate_row_count('select i from "//tmp/t" where 5 <= i and i <= 8', min=4, max=6)
            clique.make_query_and_validate_row_count(
                'select i from "//tmp/t" where i in (-1, 2, 8, 8, 15)', min=2, max=4
            )

        # Forcefully disable chunk slicing.
        with Clique(1, config_patch={"yt": {"subquery": {"max_sliced_chunk_count": 0}}}) as clique:
            # Due to inclusiveness issues each of the row counts should be correct with some error.
            clique.make_query_and_validate_row_count('select i from "//tmp/t" where i >= 3', exact=10)
            clique.make_query_and_validate_row_count('select i from "//tmp/t" where i < 2', exact=10)
            clique.make_query_and_validate_row_count('select i from "//tmp/t" where 5 <= i and i <= 8', exact=10)
            clique.make_query_and_validate_row_count('select i from "//tmp/t" where i in (-1, 2, 8, 8, 15)', exact=10)

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
        with Clique(1) as clique:
            settings = {"chyt.use_block_sampling": int(use_block_sampling)}
            clique.make_query_and_validate_row_count('select a from "//tmp/t" sample 0.1', min=60, max=170,
                                                     verbose=False, settings=settings)
            clique.make_query_and_validate_row_count('select a from "//tmp/t" sample 100', min=60, max=170,
                                                     verbose=False, settings=settings)
            clique.make_query_and_validate_row_count('select a from "//tmp/t" sample 2/20', min=60, max=170,
                                                     verbose=False, settings=settings)
            clique.make_query_and_validate_row_count('select a from "//tmp/t" sample 0.1 offset 42', min=60, max=170,
                                                     verbose=False, settings=settings)
            clique.make_query_and_validate_row_count('select a from "//tmp/t" sample 10000', exact=1000, verbose=False,
                                                     settings=settings)
            clique.make_query_and_validate_row_count('select a from "//tmp/t" sample 10000', exact=1000, verbose=False,
                                                     settings=settings)
            clique.make_query_and_validate_row_count('select a from "//tmp/t" sample 0', exact=0, verbose=False,
                                                     settings=settings)
            clique.make_query_and_validate_row_count('select a from "//tmp/t" sample 0.000000000001', exact=0,
                                                     verbose=False, settings=settings)
            clique.make_query_and_validate_row_count('select a from "//tmp/t" sample 1/100000000000', exact=0,
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

    @authors("dakovalkov")
    def test_chyt_526(self):

        create(
            "table",
            "//tmp/t_static",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                ],
            },
        )
        create(
            "table",
            "//tmp/t_dynamic",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64"},
                ],
                "dynamic": True,
                "enable_dynamic_store_read": True,
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
            },
        )

        with Clique(1) as clique:
            with raises_yt_error(QueryFailedError):
                clique.make_query("select * from concatYtTables(`//tmp/t_dynamic`, `//tmp/t_dynamic`)")
            with raises_yt_error(QueryFailedError):
                clique.make_query("select * from concatYtTables(`//tmp/t_dynamic`, `//tmp/t_static`)")

            assert clique.make_query("select * from concatYtTables(`//tmp/t_dynamic`)") == []
            assert clique.make_query("select * from concatYtTables(`//tmp/t_static`)") == []
            assert clique.make_query("select * from concatYtTables(`//tmp/t_static`, `//tmp/t_static`)") == []
            assert clique.make_query("select * from `//tmp/t_dynamic` as a join `//tmp/t_dynamic` as b using a") == []

            with raises_yt_error(QueryFailedError):
                clique.make_query("select * from `//tmp/t_dynamic` as a join `//tmp/t_static` as b using a") == []
            with raises_yt_error(QueryFailedError):
                clique.make_query("select * from `//tmp/t_static` as a join `//tmp/t_dynamic` as b using a") == []

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
