from yt_env_setup import find_ut_file, skip_if_rpc_driver_backend

from yt_dynamic_tables_base import DynamicTablesBase

from yt_helpers import profiler_factory

from yt_commands import (
    authors, create_dynamic_table, wait, create, ls, get, move, create_user, make_ace,
    insert_rows, raises_yt_error, select_rows, delete_rows, sorted_dicts, generate_uuid,
    write_local_file, reshard_table, sync_create_cells, sync_mount_table, sync_unmount_table, sync_flush_table,
    WaitFailed)

from yt_type_helpers import (
    decimal_type,
    make_column,
    make_sorted_column,
    optional_type,
    struct_type,
)

from yt.environment.helpers import assert_items_equal
from yt.common import YtError
import yt.yson as yson

from yt_driver_bindings import Driver

from flaky import flaky
import pytest

from copy import deepcopy
from random import randint, shuffle
from math import isnan
import os
import time
import builtins
import functools

##################################################################


class TestQuery(DynamicTablesBase):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_DRIVER_CONFIG = {
        "client_cache": {"capacity": 10, "shard_count": 1},
        "function_registry_cache": {
            "expire_after_successful_update_time": 3000,
            "expire_after_failed_update_time": 3000,
            "refresh_time": 2000
        },
    }

    DELTA_RPC_DRIVER_CONFIG = DELTA_DRIVER_CONFIG

    def _sample_data(self, path="//tmp/t", chunks=3, stripe=3):
        create(
            "table",
            path,
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64"},
                ],
            },
        )

        sync_mount_table(path)

        for i in range(chunks):
            data = [{"a": (i * stripe + j), "b": (i * stripe + j) * 10} for j in range(1, 1 + stripe)]
            insert_rows(path, data)

    def _create_table(self, path, schema, data, optimize_for="lookup"):
        create(
            "table",
            path,
            attributes={
                "dynamic": True,
                "optimize_for": optimize_for,
                "schema": schema,
            },
        )

        sync_mount_table(path)
        insert_rows(path, data)

    @authors("sandello")
    def test_simple(self):
        sync_create_cells(1)
        for i in range(0, 50, 10):
            path = "//tmp/t{0}".format(i)

            self._sample_data(path=path, chunks=i, stripe=10)
            result = select_rows("a, b from [{}]".format(path), verbose=False)

            assert len(result) == 10 * i

    @authors("lukyan")
    def test_response_parameters(self):
        sync_create_cells(1)
        self._sample_data(path="//tmp/t")
        response_parameters = {}
        select_rows("* from [//tmp/t]", response_parameters=response_parameters, enable_statistics=True)
        assert "read_time" in response_parameters

    @authors("lukyan")
    def test_full_scan(self):
        sync_create_cells(1)
        self._sample_data(path="//tmp/t")
        with pytest.raises(YtError):
            select_rows("* from [//tmp/t]", allow_full_scan=False)

    @authors("lukyan")
    def test_execution_pool(self):
        create_user("u")
        sync_create_cells(1)
        self._sample_data(path="//tmp/t")

        create("map_node", "//sys/ql_pools/configured_pool", attributes={
            "weight": 10.0,
            "acl": [make_ace("allow", "u", "use")]
        })
        select_rows(
            "* from [//tmp/t]",
            allow_full_scan=True,
            execution_pool="configured_pool",
            authenticated_user="u",
        )

        select_rows(
            "* from [//tmp/t]",
            allow_full_scan=True,
            execution_pool="unconfigured_pool",
            authenticated_user="u",
        )

        create("map_node", "//sys/ql_pools/secured_pool")
        with pytest.raises(YtError):
            select_rows(
                "* from [//tmp/t]",
                allow_full_scan=True,
                execution_pool="secured_pool",
                authenticated_user="u",
            )

    @authors("sandello")
    def test_project1(self):
        sync_create_cells(1)
        self._sample_data(path="//tmp/t")
        expected = [{"s": 2 * i + 10 * i - 1} for i in range(1, 10)]

        actual = select_rows("2 * a + b - 1 as s from [//tmp/t]")
        assert_items_equal(actual, expected)

        actual = select_rows("2 * a + b - 1 as s from [//tmp/t] limit 1000")
        assert expected == actual

    @authors("sandello")
    def test_group_by1(self):
        sync_create_cells(1)
        self._sample_data(path="//tmp/t")
        expected = [{"s": 450}]

        actual = select_rows("sum(b) as s from [//tmp/t] group by 1 as k")
        assert_items_equal(actual, expected)

    @authors("sandello", "lukyan", "asaitgalin")
    def test_group_by2(self):
        sync_create_cells(1)
        self._sample_data(path="//tmp/t")
        expected = [{"k": 0, "s": 200}, {"k": 1, "s": 250}]

        actual = select_rows("k, sum(b) as s from [//tmp/t] group by a % 2 as k")
        assert_items_equal(actual, expected)

    @authors("lukyan")
    def test_group_by_primary_prefix(self):
        sync_create_cells(1)

        tt = "//tmp/t"

        create(
            "table",
            tt,
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64", "sort_order": "ascending"},
                    {"name": "v", "type": "int64"},
                ],
            },
        )

        reshard_table(tt, [[], [3, 3], [6, 6]])
        sync_mount_table(tt)

        data = [{"a": i // 10, "b": i % 10, "v": i} for i in range(100)]
        insert_rows(tt, data)

        grouped = {}
        for item in data:
            key = (item["a"], item["v"] % 2)
            if key not in grouped:
                grouped[key] = 0
            grouped[key] += item["b"]

        expected = [{"k": k, "x": x, "s": s} for (k, x), s in list(grouped.items())]
        actual = select_rows("k, x, sum(b) as s from [//tmp/t] group by a as k, v % 2 as x")
        assert_items_equal(actual, expected)

    @authors("lukyan")
    def test_group_by_disjoint(self):
        sync_create_cells(1)

        tt = "//tmp/t"
        tj = "//tmp/j"

        create(
            "table",
            tt,
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "dummy", "type": "int64"},
                ],
            },
        )

        reshard_table(tt, [[], [3], [6]])
        sync_mount_table(tt)

        insert_rows(tt, [{"a": i} for i in range(10)])

        create(
            "table",
            tj,
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64", "sort_order": "ascending"},
                    {"name": "v", "type": "int64"},
                ],
            },
        )

        reshard_table(tj, [[], [3, 6], [6, 6]])
        sync_mount_table(tj)

        data = [{"a": i // 10, "b": i % 10, "v": i} for i in range(100)]
        insert_rows(tj, data)

        grouped = {}
        for item in data:
            key = (item["a"], item["v"] % 2)
            if key not in grouped:
                grouped[key] = 0
            grouped[key] += item["b"]

        expected = [{"k": k, "x": x, "s": s} for (k, x), s in list(grouped.items())]
        actual = select_rows("k, x, sum(b) as s from [//tmp/t] join [//tmp/j] using a group by a as k, v % 2 as x")
        assert_items_equal(actual, expected)

    @authors("lukyan")
    def test_having(self):
        sync_create_cells(3)

        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64"},
                ],
            },
        )

        sync_mount_table("//tmp/t")

        data = [{"a": i, "b": i * 10} for i in range(0, 100)]
        insert_rows("//tmp/t", data)

        expected = [{"k": 0, "aa": 49.0, "mb": 0, "ab": 490.0}]
        actual = select_rows(
            """
            k, avg(a) as aa, min(b) as mb, avg(b) as ab
            from [//tmp/t]
            group by a % 2 as k
            having mb < 5"""
        )
        assert expected == actual

    @authors("lbrown")
    def test_merging_group_by(self):
        sync_create_cells(1)

        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64"},
                ],
            },
        )

        pivots = [[i * 5] for i in range(0, 20)]
        pivots.insert(0, [])
        reshard_table("//tmp/t", pivots)

        sync_mount_table("//tmp/t")

        data = [{"a": i, "b": i * 10} for i in range(0, 100)]
        insert_rows("//tmp/t", data)

        expected = [
            {"k": 0, "aa": 49.0, "mb": 0, "ab": 490.0},
            {"k": 1, "aa": 50.0, "mb": 10, "ab": 500.0},
        ]
        actual = select_rows(
            """
            k, avg(a) as aa, min(b) as mb, avg(b) as ab
            from [//tmp/t]
            group by a % 2 as k
            order by k limit 2"""
        )
        assert expected == actual

    @authors("lbrown")
    def test_merging_group_by2(self):
        sync_create_cells(1)

        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "string"},
                ],
            },
        )

        pivots = [[i * 5] for i in range(0, 20)]
        pivots.insert(0, [])
        reshard_table("//tmp/t", pivots)

        sync_mount_table("//tmp/t")

        data = [{"a": i, "b": str(i)} for i in range(0, 100)]
        insert_rows("//tmp/t", data)

        expected = [{"k": 0, "m": "98"}, {"k": 1, "m": "99"}]
        actual = select_rows("k, max(b) as m from [//tmp/t] group by a % 2 as k order by k limit 2")
        assert expected == actual

    @authors("lukyan")
    def test_limit(self):
        sync_create_cells(1)

        self._sample_data(path="//tmp/t")

        expected = [{"a": 1, "b": 10}]
        actual = select_rows("* from [//tmp/t] limit 1")
        assert expected == actual

    @authors("lukyan")
    def test_order_by(self):
        sync_create_cells(1)

        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "k", "type": "int64", "sort_order": "ascending"},
                    {"name": "u", "type": "int64"},
                    {"name": "v", "type": "int64"},
                ],
            },
        )

        sync_mount_table("//tmp/t")

        values = [i for i in range(0, 300)]
        shuffle(values)

        data = [{"k": i, "v": values[i], "u": randint(0, 1000)} for i in range(0, 100)]
        insert_rows("//tmp/t", data)

        def pick_items(row, items):
            return dict((col, v) for col, v in row.items() if col in items)

        filtered = [pick_items(row, ["k", "v"]) for row in data if row["u"] > 500]
        expected = sorted(filtered, key=functools.cmp_to_key(lambda x, y: x["v"] - y["v"]))[0:10]

        actual = select_rows("k, v from [//tmp/t] where u > 500 order by v limit 10")
        assert expected == actual

        expected = sorted(filtered, key=functools.cmp_to_key(lambda x, y: x["v"] - y["v"]))[20:30]

        actual = select_rows("k, v from [//tmp/t] where u > 500 order by v offset 20 limit 10")
        assert expected == actual

    @authors("lukyan")
    def test_keys_coordination(self):
        sync_create_cells(1)

        # Test coordination of keys via join.
        # 1. Full keys
        # 2. Prefix

        # Pivot is prefix
        # Pivot is full key

        tt = "//tmp/t"
        tj = "//tmp/j"

        create(
            "table",
            tt,
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "dummy", "type": "int64"},
                ],
            },
        )

        reshard_table(tt, [[], [3], [6]])
        sync_mount_table(tt)

        insert_rows(tt, [{"a": i} for i in range(10)])

        create(
            "table",
            tj,
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "b", "type": "int64", "sort_order": "ascending"},
                    {"name": "c", "type": "int64", "sort_order": "ascending"},
                    {"name": "v", "type": "int64"},
                ],
            },
        )

        reshard_table(tj, [[], [3, 6], [6, 6]])
        sync_mount_table(tj)

        data = [{"b": i // 10, "c": i % 10, "v": i} for i in range(100)]
        insert_rows(tj, data)

        expected = [dict(list(row.items()) + [("a", row["b"])]) for row in data]

        actual = select_rows("a, b, c, v from [//tmp/t] join [//tmp/j] on a = b")
        assert_items_equal(actual, expected)

        actual = select_rows("a, b, c, v from [//tmp/t] join [//tmp/j] on (a + 0) = b")
        assert_items_equal(actual, expected)

        insert_rows(tt, [{"a": i} for i in range(100)])

        expected = [dict(list(row.items()) + [("a", row["b"] * 10 + row["c"])]) for row in data]

        actual = select_rows("a, b, c, v from [//tmp/t] join [//tmp/j] on (a / 10, a % 10) = (b, c)")
        assert_items_equal(actual, expected)

        actual = select_rows("a, b, c, v from [//tmp/t] join [//tmp/j] on (a / 10, a % 10) = (b, c) where a = 36")
        assert_items_equal(actual, [expected[36]])

    @authors("lukyan")
    def test_inefficient_join(self):
        sync_create_cells(1)
        self._create_table(
            "//tmp/jl",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64"},
            ],
            [],
            "scan",
        )

        self._create_table(
            "//tmp/jr",
            [
                {"name": "c", "type": "int64", "sort_order": "ascending"},
                {"name": "d", "type": "int64"},
            ],
            [],
            "scan",
        )

        with pytest.raises(YtError):
            select_rows(
                "* from [//tmp/jl] join [//tmp/jr] on b = d",
                allow_join_without_index=False,
            )

    @authors("lukyan")
    def test_join_via_in(self):
        sync_create_cells(1)
        self._create_table(
            "//tmp/jl",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64"},
            ],
            [
                {"a": 1, "b": 1},
                {"a": 2, "b": 3},
                {"a": 3, "b": 6},
                {"a": 4, "b": 1},
                {"a": 5, "b": 3},
                {"a": 6, "b": 6}
            ],
            "scan",
        )

        self._create_table(
            "//tmp/jr",
            [
                {"name": "c", "type": "int64", "sort_order": "ascending"},
                {"name": "d", "type": "int64", "sort_order": "ascending"},
                {"name": "e", "type": "string"}
            ],
            [
                {"c": 1, "d": 2, "e": "a"},
                {"c": 2, "d": 1, "e": "b"},
                {"c": 2, "d": 2, "e": "c"},
                {"c": 2, "d": 3, "e": "d"},
                {"c": 2, "d": 4, "e": "e"},
                {"c": 2, "d": 5, "e": "f"},
                {"c": 2, "d": 6, "e": "g"},
                {"c": 3, "d": 1, "e": "h"},
            ],
            "scan",
        )

        expected = [
            {"a": 1, "b": 1, "c": 2, "d": 1, "e": "b"},
            {"a": 2, "b": 3, "c": 2, "d": 3, "e": "d"},
            {"a": 3, "b": 6, "c": 2, "d": 6, "e": "g"},
            {"a": 4, "b": 1, "c": 2, "d": 1, "e": "b"},
            {"a": 5, "b": 3, "c": 2, "d": 3, "e": "d"},
            {"a": 6, "b": 6, "c": 2, "d": 6, "e": "g"},
        ]

        actual = select_rows(
            "* from [//tmp/jl] join [//tmp/jr] on b = d and c = 2",
            allow_join_without_index=True,
            max_subqueries=1,
        )

        assert sorted_dicts(expected) == sorted_dicts(actual)

        read_count_path = "//tmp/jr/@tablets/0/performance_counters/dynamic_row_lookup_count"
        wait(lambda: get(read_count_path) > 0)
        assert get(read_count_path) == 3

    @authors("lukyan")
    def test_join(self):
        sync_create_cells(1)

        self._create_table(
            "//tmp/jl",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64", "sort_order": "ascending"},
                {"name": "c", "type": "int64"},
            ],
            [
                {"a": 1, "b": 2, "c": 80},
                {"a": 1, "b": 3, "c": 71},
                {"a": 1, "b": 4, "c": 62},
                {"a": 2, "b": 1, "c": 53},
                {"a": 2, "b": 2, "c": 44},
                {"a": 2, "b": 3, "c": 35},
                {"a": 2, "b": 4, "c": 26},
                {"a": 3, "b": 1, "c": 17},
            ],
            "scan",
        )

        self._create_table(
            "//tmp/jr",
            [
                {"name": "c", "type": "int64", "sort_order": "ascending"},
                {"name": "d", "type": "int64"},
                {"name": "e", "type": "int64"},
            ],
            [
                {"d": 1, "e": 2, "c": 80},
                {"d": 1, "e": 3, "c": 71},
                {"d": 1, "e": 4, "c": 62},
                {"d": 2, "e": 1, "c": 53},
                {"d": 2, "e": 2, "c": 44},
                {"d": 2, "e": 3, "c": 35},
                {"d": 2, "e": 4, "c": 26},
                {"d": 3, "e": 1, "c": 17},
            ],
            "scan",
        )

        expected = [
            {"a": 1, "b": 2, "c": 80, "d": 1, "e": 2},
            {"a": 1, "b": 3, "c": 71, "d": 1, "e": 3},
            {"a": 1, "b": 4, "c": 62, "d": 1, "e": 4},
            {"a": 2, "b": 1, "c": 53, "d": 2, "e": 1},
            {"a": 2, "b": 2, "c": 44, "d": 2, "e": 2},
            {"a": 2, "b": 3, "c": 35, "d": 2, "e": 3},
            {"a": 2, "b": 4, "c": 26, "d": 2, "e": 4},
            {"a": 3, "b": 1, "c": 17, "d": 3, "e": 1},
        ]

        actual = select_rows("* from [//tmp/jl] join [//tmp/jr] using c where a < 4")
        assert sorted_dicts(expected) == sorted_dicts(actual)

        expected = [{"a": 2, "b": 1, "c": 53, "d": 2, "e": 1}]

        actual = select_rows("* from [//tmp/jl] join [//tmp/jr] using c where (a, b) IN ((2, 1))")
        assert expected == actual

        expected = [{"l.a": 2, "l.b": 1, "l.c": 53, "r.c": 53, "r.d": 2, "r.e": 1}]

        actual = select_rows(
            """
            * from [//tmp/jl] as l
            join [//tmp/jr] as r on l.c + 1 = r.c + 1
             where (l.a, l.b) in ((2, 1))""",
            allow_join_without_index=True,
        )
        assert expected == actual

    @authors("lukyan")
    def test_join_common_prefix(self):
        sync_create_cells(1)

        self._create_table(
            "//tmp/jl",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64", "sort_order": "ascending"},
                {"name": "c", "type": "int64"},
            ],
            [
                {"a": 1, "b": 2, "c": 80},
                {"a": 1, "b": 3, "c": 71},
                {"a": 1, "b": 4, "c": 62},
                {"a": 2, "b": 1, "c": 53},
                {"a": 2, "b": 2, "c": 44},
                {"a": 2, "b": 3, "c": 35},
                {"a": 2, "b": 4, "c": 26},
                {"a": 3, "b": 1, "c": 17},
            ],
            "scan",
        )

        self._create_table(
            "//tmp/jr",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64", "sort_order": "ascending"},
                {"name": "d", "type": "int64"},
            ],
            [
                {"a": 1, "b": 2, "d": 80},
                {"a": 1, "b": 4, "d": 62},
                {"a": 2, "b": 1, "d": 53},
                {"a": 2, "b": 3, "d": 35},
                {"a": 3, "b": 1, "d": 17},
            ],
            "scan",
        )

        expected = [
            {"a": 1, "b": 2, "c": 80, "d": 80},
            {"a": 1, "b": 3, "c": 71, "d": None},
            {"a": 1, "b": 4, "c": 62, "d": 62},
            {"a": 2, "b": 1, "c": 53, "d": 53},
            {"a": 2, "b": 2, "c": 44, "d": None},
            {"a": 2, "b": 3, "c": 35, "d": 35},
            {"a": 2, "b": 4, "c": 26, "d": None},
            {"a": 3, "b": 1, "c": 17, "d": 17},
        ]

        actual = select_rows("* from [//tmp/jl] left join [//tmp/jr] using a, b")
        assert sorted_dicts(expected) == sorted_dicts(actual)

    @authors("lukyan")
    def test_join_common_prefix2(self):
        sync_create_cells(1)

        self._create_table(
            "//tmp/jl",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "c", "type": "int64"},
            ],
            [{"a": 1, "c": 3}],
            "scan",
        )

        self._create_table(
            "//tmp/jr",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64", "sort_order": "ascending"},
                {"name": "d", "type": "int64"},
            ],
            [{"a": 1, "b": 2, "d": 4}],
            "scan",
        )

        expected = [{"l.a": 1, "r.a": 1, "r.b": 2, "l.c": 3, "r.d": 4}]

        actual = select_rows("* from [//tmp/jl] l left join [//tmp/jr] r on (l.a, 2) = (r.a, r.b) where l.a = 1")
        assert sorted_dicts(expected) == sorted_dicts(actual)

    @authors("lukyan")
    def test_join_common_prefix_limit(self):
        sync_create_cells(1)

        self._create_table(
            "//tmp/jl",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64"},
            ],
            [
                {"a": 1, "b": 2},
                {"a": 2, "b": 3},
                {"a": 3, "b": 4},
                {"a": 4, "b": 1},
                {"a": 5, "b": 2},
                {"a": 6, "b": 3},
                {"a": 7, "b": 4},
                {"a": 8, "b": 1},
            ],
            "scan",
        )

        self._create_table(
            "//tmp/jr",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "c", "type": "int64"},
            ],
            [
                {"a": 1, "c": 80},
                {"a": 3, "c": 62},
                {"a": 4, "c": 53},
                {"a": 6, "c": 17},
            ],
            "scan",
        )

        expected = [
            {"a": 2, "b": 3, "c": None},
            {"a": 5, "b": 2, "c": None},
        ]

        actual = select_rows("* from [//tmp/jl] left join [//tmp/jr] using a where c = null and a between 2 and 7 limit 2")
        assert sorted_dicts(expected) == sorted_dicts(actual)

    @authors("lukyan")
    def test_join_many(self):
        sync_create_cells(1)

        self._create_table(
            "//tmp/a",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "c", "type": "string"},
            ],
            [
                {"a": 1, "c": "a"},
                {"a": 2, "c": "b"},
                {"a": 3, "c": "c"},
                {"a": 4, "c": "a"},
                {"a": 5, "c": "b"},
                {"a": 6, "c": "c"},
            ],
        )

        self._create_table(
            "//tmp/b",
            [
                {"name": "b", "type": "int64", "sort_order": "ascending"},
                {"name": "c", "type": "string"},
                {"name": "d", "type": "string"},
            ],
            [
                {"b": 100, "c": "a", "d": "X"},
                {"b": 200, "c": "b", "d": "Y"},
                {"b": 300, "c": "c", "d": "X"},
                {"b": 400, "c": "a", "d": "Y"},
                {"b": 500, "c": "b", "d": "X"},
                {"b": 600, "c": "c", "d": "Y"},
            ],
        )

        self._create_table(
            "//tmp/c",
            [
                {"name": "d", "type": "string", "sort_order": "ascending"},
                {"name": "e", "type": "int64"},
            ],
            [{"d": "X", "e": 1234}, {"d": "Y", "e": 5678}],
        )

        expected = [
            {"a": 2, "c": "b", "b": 200, "d": "Y", "e": 5678},
            {"a": 2, "c": "b", "b": 500, "d": "X", "e": 1234},
            {"a": 3, "c": "c", "b": 300, "d": "X", "e": 1234},
            {"a": 3, "c": "c", "b": 600, "d": "Y", "e": 5678},
            {"a": 4, "c": "a", "b": 100, "d": "X", "e": 1234},
            {"a": 4, "c": "a", "b": 400, "d": "Y", "e": 5678},
        ]

        actual = select_rows(
            "* from [//tmp/a] join [//tmp/b] using c join [//tmp/c] using d where a in (2,3,4)",
            allow_join_without_index=True,
        )
        assert sorted_dicts(expected) == sorted_dicts(actual)

    @authors("dtorilov")
    def test_yt_22385(self):
        sync_create_cells(1)

        self._create_table(
            "//tmp/t",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64"},
            ],
            [
                {"a": 0, "b": 1},
                {"a": 1, "b": 2},
            ],
        )

        expected = [
            {"t1.a": 0, "t1.b": 1, "t2.a": 0, "t2.b": 1, "t3.a": 0, "t3.b": 1},
            {"t1.a": 1, "t1.b": 2, "t2.a": 1, "t2.b": 2, "t3.a": 1, "t3.b": 2},
        ]

        actual = select_rows(
            "* from [//tmp/t] t1 join [//tmp/t] t2 on (t1.a + 0) = (t2.a) join [//tmp/t] t3 on (t1.a + 0) = (t3.a)",
            allow_join_without_index=True,
        )
        assert sorted_dicts(expected) == sorted_dicts(actual)

    @authors("lukyan")
    def test_types(self):
        sync_create_cells(1)

        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "boolean"},
                    {"name": "c", "type": "string"},
                    {"name": "d", "type": "uint64"},
                ],
            },
        )
        sync_mount_table("//tmp/t")

        format = yson.loads(b"<format=text>yson")
        insert_rows(
            "//tmp/t",
            b'{a=10;b=%false;c="hello";d=32u};{a=20;b=%true;c="world";d=64u};',
            input_format=format,
            is_raw=True,
        )

        assert (
            select_rows('a, b, c, d from [//tmp/t] where c="hello"', output_format=format)
            == b'{"a"=10;"b"=%false;"c"="hello";"d"=32u;};\n'
        )

    @authors("lukyan")
    def test_tablets(self):
        sync_create_cells(1)

        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "int64"},
                ],
            },
        )

        sync_mount_table("//tmp/t")

        stripe = 10

        for i in range(0, 10):
            data = [{"key": (i * stripe + j), "value": (i * stripe + j) * 10} for j in range(1, 1 + stripe)]
            insert_rows("//tmp/t", data)

        sync_unmount_table("//tmp/t")
        reshard_table("//tmp/t", [[], [10], [30], [50], [70], [90]])
        sync_mount_table("//tmp/t", first_tablet_index=0, last_tablet_index=2)

        select_rows("* from [//tmp/t] where key < 50")

        with pytest.raises(YtError):
            select_rows("* from [//tmp/t] where key < 51")

    @authors("babenko", "savrus", "lukyan")
    def test_computed_column_simple(self):
        sync_create_cells(1)

        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "schema": [
                    {
                        "name": "hash",
                        "type": "int64",
                        "expression": "key * 33",
                        "sort_order": "ascending",
                    },
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "int64"},
                ],
            },
        )
        reshard_table("//tmp/t", [[]] + [[i] for i in range(1, 100 * 33, 1000)])
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": i, "value": i * 2} for i in range(0, 100)])

        expected = [{"hash": 42 * 33, "key": 42, "value": 42 * 2}]
        actual = select_rows("* from [//tmp/t] where key = 42")
        assert_items_equal(actual, expected)

        expected = [{"hash": i * 33, "key": i, "value": i * 2} for i in range(10, 80)]
        actual = sorted_dicts(select_rows("* from [//tmp/t] where key >= 10 and key < 80"))
        assert_items_equal(actual, expected)

        expected = [{"hash": i * 33, "key": i, "value": i * 2} for i in [10, 20, 30]]
        actual = sorted_dicts(select_rows("* from [//tmp/t] where key in (10, 20, 30)"))
        assert_items_equal(actual, expected)

    @authors("savrus")
    def test_computed_column_far_divide(self):
        sync_create_cells(1)

        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {
                        "name": "hash",
                        "type": "int64",
                        "expression": "key2 / 2",
                        "sort_order": "ascending",
                    },
                    {"name": "key1", "type": "int64", "sort_order": "ascending"},
                    {"name": "key2", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "int64"},
                ],
            },
        )

        reshard_table("//tmp/t", [[]] + [[i] for i in range(1, 500, 10)])
        sync_mount_table("//tmp/t")

        def expected(key_range):
            return [{"hash": i // 2, "key1": i, "key2": i, "value": i * 2} for i in key_range]

        insert_rows("//tmp/t", [{"key1": i, "key2": i, "value": i * 2} for i in range(0, 1000)])

        actual = select_rows("* from [//tmp/t] where key2 = 42")
        assert_items_equal(actual, expected([42]))

        actual = sorted_dicts(select_rows("* from [//tmp/t] where key2 >= 10 and key2 < 80"))
        assert_items_equal(actual, expected(range(10, 80)))

        actual = sorted_dicts(select_rows("* from [//tmp/t] where key2 in (10, 20, 30)"))
        assert_items_equal(actual, expected([10, 20, 30]))

        actual = sorted_dicts(select_rows("* from [//tmp/t] where key2 in (10, 20, 30) and key1 in (30, 40)"))
        assert_items_equal(actual, expected([30]))

    @authors("savrus")
    def test_computed_column_modulo(self):
        sync_create_cells(1)

        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {
                        "name": "hash",
                        "type": "int64",
                        "expression": "key2 % 2",
                        "sort_order": "ascending",
                    },
                    {"name": "key1", "type": "int64", "sort_order": "ascending"},
                    {"name": "key2", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "int64"},
                ],
            },
        )

        reshard_table("//tmp/t", [[]] + [[i] for i in range(1, 500, 10)])
        sync_mount_table("//tmp/t")

        def expected(key_range):
            return [{"hash": i % 2, "key1": i, "key2": i, "value": i * 2} for i in key_range]

        insert_rows("//tmp/t", [{"key1": i, "key2": i, "value": i * 2} for i in range(0, 1000)])

        actual = select_rows("* from [//tmp/t] where key2 = 42")
        assert_items_equal(actual, expected([42]))

        actual = sorted_dicts(select_rows("* from [//tmp/t] where key1 >= 10 and key1 < 80"))
        assert_items_equal(actual, expected(range(10, 80)))

        actual = sorted_dicts(select_rows("* from [//tmp/t] where key1 in (10, 20, 30)"))
        assert_items_equal(actual, expected([10, 20, 30]))

        actual = sorted_dicts(select_rows("* from [//tmp/t] where key1 in (10, 20, 30) and key2 in (30, 40)"))
        assert_items_equal(actual, expected([30]))

    @authors("lbrown")
    def test_udf(self):
        registry_path = "//tmp/udfs"
        create("map_node", registry_path)

        abs_path = os.path.join(registry_path, "abs_udf")
        create(
            "file",
            abs_path,
            attributes={
                "function_descriptor": {
                    "name": "abs_udf",
                    "argument_types": [{"tag": "concrete_type", "value": "int64"}],
                    "result_type": {"tag": "concrete_type", "value": "int64"},
                    "calling_convention": "simple",
                }
            },
        )

        local_implementation_path = find_ut_file("test_udfs.bc")
        write_local_file(abs_path, local_implementation_path)

        sync_create_cells(1)
        self._sample_data(path="//tmp/u")
        expected = [{"s": 2 * i} for i in range(1, 10)]
        actual = select_rows("abs_udf(-2 * a) as s from [//tmp/u]")
        assert_items_equal(actual, expected)

    @authors("prime")
    def test_empty_udf(self):
        registry_path = "//tmp/udfs"
        create("map_node", registry_path)

        abs_path = os.path.join(registry_path, "empty_udf")
        create(
            "file",
            abs_path,
            attributes={
                "function_descriptor": {
                    "name": "empty_udf",
                    "argument_types": [{"tag": "concrete_type", "value": "int64"}],
                    "result_type": {"tag": "concrete_type", "value": "int64"},
                    "calling_convention": "simple",
                }
            },
        )

        sync_create_cells(1)
        self._sample_data(path="//tmp/u")

        with pytest.raises(YtError):
            select_rows("empty_udf(-2 * a) as s from [//tmp/u]")

    @authors("lukyan")
    def test_udf_custom_path(self):
        registry_path = "//home/udfs"
        create("map_node", "//home")
        create("map_node", registry_path)

        abs_path = os.path.join(registry_path, "abs_udf")
        create(
            "file",
            abs_path,
            attributes={
                "function_descriptor": {
                    "name": "abs_udf",
                    "argument_types": [{"tag": "concrete_type", "value": "int64"}],
                    "result_type": {"tag": "concrete_type", "value": "int64"},
                    "calling_convention": "simple",
                }
            },
        )

        local_implementation_path = find_ut_file("test_udfs.bc")
        write_local_file(abs_path, local_implementation_path)

        sync_create_cells(1)
        self._sample_data(path="//tmp/u")
        expected = [{"s": 2 * i} for i in range(1, 10)]
        actual = select_rows("abs_udf(-2 * a) as s from [//tmp/u]", udf_registry_path=registry_path)
        assert_items_equal(actual, expected)

    @authors("lukyan")
    def test_udf_fc(self):
        registry_path = "//tmp/udfs"
        create("map_node", registry_path)

        udf_fc_path = os.path.join(registry_path, "udf_fc")
        create(
            "file",
            udf_fc_path,
            attributes={
                "function_descriptor": {
                    "name": "udf_with_function_context",
                    "argument_types": [{"tag": "concrete_type", "value": "int64"}],
                    "result_type": {"tag": "concrete_type", "value": "int64"},
                    "calling_convention": "unversioned_value",
                    "use_function_context": True,
                }
            },
        )

        local_implementation_path = find_ut_file("test_udfs_fc.bc")
        write_local_file(udf_fc_path, local_implementation_path)

        sync_create_cells(1)
        self._sample_data(path="//tmp/u")
        expected = [{"s": 2 * i} for i in range(1, 10)]
        actual = select_rows("udf_fc(2 * a) as s from [//tmp/u]")
        assert_items_equal(actual, expected)

    @authors("lbrown")
    def test_udaf(self):
        registry_path = "//tmp/udfs"
        create("map_node", registry_path)

        avg_path = os.path.join(registry_path, "avg_udaf")
        create(
            "file",
            avg_path,
            attributes={
                "aggregate_descriptor": {
                    "name": "avg_udaf",
                    "argument_type": {"tag": "concrete_type", "value": "int64"},
                    "state_type": {"tag": "concrete_type", "value": "string"},
                    "result_type": {"tag": "concrete_type", "value": "double"},
                    "calling_convention": "unversioned_value",
                }
            },
        )

        local_implementation_path = find_ut_file("test_udfs.bc")
        write_local_file(avg_path, local_implementation_path)

        sync_create_cells(1)
        self._sample_data(path="//tmp/ua")
        expected = [{"x": 5.0}]
        actual = select_rows("avg_udaf(a) as x from [//tmp/ua] group by 1")
        assert_items_equal(actual, expected)

    @authors("lukyan")
    @skip_if_rpc_driver_backend
    @flaky(max_runs=5)
    def test_udf_cache(self):
        sync_create_cells(1)
        self._sample_data(path="//tmp/u")
        query = "a, xxx_udf(a, 2) as s from [//tmp/u]"

        registry_path = "//tmp/udfs"

        xxx_path = os.path.join(registry_path, "xxx_udf")
        create("map_node", registry_path)

        udfs_impl_path = find_ut_file("test_udfs.bc")

        create(
            "file",
            xxx_path,
            attributes={
                "function_descriptor": {
                    "name": "exp_udf",
                    "argument_types": [
                        {"tag": "concrete_type", "value": "int64"},
                        {"tag": "concrete_type", "value": "int64"},
                    ],
                    "result_type": {"tag": "concrete_type", "value": "int64"},
                    "calling_convention": "simple",
                }
            },
        )
        write_local_file(xxx_path, udfs_impl_path)

        expected_exp = [{"a": i, "s": i * i} for i in range(1, 10)]
        actual = select_rows(query)
        assert_items_equal(actual, expected_exp)

        move(xxx_path, xxx_path + ".bak")
        create(
            "file",
            xxx_path,
            attributes={
                "function_descriptor": {
                    "name": "sum_udf",
                    "argument_types": [{"tag": "concrete_type", "value": "int64"}],
                    "repeated_argument_type": {
                        "tag": "concrete_type",
                        "value": "int64",
                    },
                    "result_type": {"tag": "concrete_type", "value": "int64"},
                    "calling_convention": "unversioned_value",
                }
            },
        )
        write_local_file(xxx_path, udfs_impl_path)

        # Still use cache
        actual = select_rows(query)
        assert_items_equal(actual, expected_exp)

        time.sleep(5)

        expected_sum = [{"a": i, "s": i + 2} for i in range(1, 10)]
        actual = select_rows(query)
        assert_items_equal(actual, expected_sum)

    @authors("savrus", "lbrown")
    def test_aggregate_string_capture(self):
        sync_create_cells(1)

        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "a", "type": "string", "sort_order": "ascending"},
                    {"name": "b", "type": "int64"},
                ],
            },
        )

        sync_mount_table("//tmp/t")

        # Need at least 1024 items to ensure a second batch in the scan operator
        data = [{"a": "A" + str(j) + "BCD"} for j in range(1, 2048)]
        insert_rows("//tmp/t", data)

        expected = [{"m": "a1000bcd"}]
        actual = select_rows("min(lower(a)) as m from [//tmp/t] group by 1")
        assert_items_equal(actual, expected)

    @authors("lbrown")
    def test_cardinality(self):
        sync_create_cells(1)

        create(
            "table",
            "//tmp/card",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64"},
                ],
            },
        )

        pivots = [[i * 1000] for i in range(0, 20)]
        pivots.insert(0, [])
        reshard_table("//tmp/card", pivots)

        sync_mount_table("//tmp/card")

        data = [{"a": i} for i in range(0, 20000)]
        insert_rows("//tmp/card", data)
        insert_rows("//tmp/card", data)
        insert_rows("//tmp/card", data)
        insert_rows("//tmp/card", data)

        actual = select_rows("cardinality(a) as b from [//tmp/card] group by a % 2 as k with totals")
        assert actual[0]["b"] > 0.95 * 10000
        assert actual[0]["b"] < 1.05 * 10000
        assert actual[1]["b"] > 0.95 * 10000
        assert actual[1]["b"] < 1.05 * 10000
        assert actual[2]["b"] > 1.95 * 10000
        assert actual[2]["b"] < 2.05 * 10000

    @authors("babenko")
    def test_yt_2375(self):
        sync_create_cells(1)
        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "int64"},
                ],
            },
        )
        reshard_table("//tmp/t", [[]] + [[i] for i in range(1, 1000, 10)])
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": i, "value": 10 * i} for i in range(0, 1000)])
        # should not raise
        select_rows(
            "sleep(value) from [//tmp/t]",
            output_row_limit=1,
            fail_on_incomplete_result=False,
        )

    @authors("savrus")
    def test_null(self):
        sync_create_cells(1)

        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64"},
                ],
            },
        )

        sync_mount_table("//tmp/t")

        data = [{"a": None, "b": 0}, {"a": 1, "b": 1}]
        insert_rows("//tmp/t", data)

        expected = data[0:1]
        actual = select_rows("* from [//tmp/t] where a = null")
        assert actual == expected

    @authors("ifsmirnov")
    def test_nan(self):
        sync_create_cells(1)

        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "double", "sort_order": "ascending"},
                    {"name": "b", "type": "double"},
                ],
            },
        )

        sync_mount_table("//tmp/t")

        nan = float("nan")
        str_nan = "(1.0 / 0 - 1.0 / 0)"

        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"a": nan, "b": 1.0}])
        data = [{"a": 1.0, "b": nan}, {"a": 2.0, "b": 2.0}, {"a": 3.0}]
        insert_rows("//tmp/t", data)

        def _isnan(x):
            return isinstance(x, float) and isnan(x)

        # Comparison that respects NaN == NaN and YsonEntity == nothing.
        def _compare(lhs, rhs):
            if isinstance(lhs, list) or isinstance(lhs, tuple):
                return len(lhs) == len(rhs) and all(_compare(x, y) for x, y in zip(lhs, rhs))
            elif isinstance(lhs, dict):
                for key in builtins.set(list(lhs.keys())).union(list(rhs.keys())):
                    lhs_value = lhs.get(key)
                    if isinstance(lhs_value, yson.YsonEntity):
                        lhs_value = None
                    rhs_value = rhs.get(key)
                    if isinstance(rhs_value, yson.YsonEntity):
                        rhs_value = None
                    if not _compare(lhs_value, rhs_value):
                        return False
                return True
            else:
                if _isnan(lhs):
                    return _isnan(rhs)
                return lhs == rhs

        assert _compare(select_rows("* from [//tmp/t]"), data)
        assert _compare(select_rows("* from [//tmp/t] where is_nan(b)"), data[:1])
        assert _compare(select_rows("* from [//tmp/t] where is_null(b)"), data[2:])
        with pytest.raises(YtError):
            select_rows("* from [//tmp/t] where b > 0")
        assert _compare(select_rows("* from [//tmp/t] where if(is_nan(b), false, b > 0)"), data[1:2])

        assert all(_isnan(list(x.values())[0]) for x in select_rows("if(true, {}, 1) from [//tmp/t]".format(str_nan)))
        with pytest.raises(YtError):
            select_rows("* from [//tmp/t] where b = {}".format(str_nan))
        with pytest.raises(YtError):
            select_rows("* from [//tmp/t] where b = if(true, {}, 0)".format(str_nan))
        with pytest.raises(YtError):
            select_rows("{} > 1 from [//tmp/t]".format(str_nan))
        with pytest.raises(YtError):
            select_rows("if({}, 0, 1) from [//tmp/t]".format(str_nan))
        with pytest.raises(YtError):
            select_rows("if(true, {}, 0) > 1 from [//tmp/t]".format(str_nan))

        assert list(select_rows("is_nan({}) from [//tmp/t]".format(str_nan))[0].values())[0]
        assert not list(select_rows("is_nan({}) from [//tmp/t]".format("123"))[0].values())[0]
        assert not list(select_rows("is_nan({}) from [//tmp/t]".format("#"))[0].values())[0]

    @authors("lukyan")
    def test_bad_limits(self):
        sync_create_cells(1)

        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64", "sort_order": "ascending"},
                    {"name": "c", "type": "int64", "sort_order": "ascending"},
                    {"name": "x", "type": "string"},
                ],
            },
        )

        pivots = [[i * 5] for i in range(0, 20)]
        pivots.insert(0, [])
        reshard_table("//tmp/t", pivots)

        sync_mount_table("//tmp/t")

        data = [{"a": i, "b": i, "c": i, "x": str(i)} for i in range(0, 100)]
        insert_rows("//tmp/t", data)

        select_rows("x from [//tmp/t] where (a = 18 and b = 10 and c >= 70) or (a = 18 and b >= 10) or (a >= 18)")

    @authors("lukyan")
    def test_multi_between(self):
        sync_create_cells(1)

        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64", "sort_order": "ascending"},
                    {"name": "c", "type": "int64"},
                ],
            },
        )

        sync_mount_table("//tmp/t")

        data = [{"a": i // 10, "b": i % 10, "c": i} for i in range(0, 100)]
        insert_rows("//tmp/t", data)

        expected = data[10:13] + data[23:25] + data[35:40] + data[40:60]

        actual = select_rows(
            """
        * from [//tmp/t] where
            (a, b) between (
                (1) and (1, 2),
                (2, 3) and (2, 4),
                (3, 5) and (3),
                4 and 5
            )
        """
        )
        assert actual == expected

    @authors("lukyan")
    def test_offset(self):
        sync_create_cells(1)

        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64"},
                ],
            },
        )

        sync_mount_table("//tmp/t")

        data = [{"a": i, "b": i} for i in range(0, 11)]
        insert_rows("//tmp/t", data)

        expected = data[8:9]

        actual = select_rows("""* from [//tmp/t] offset 8 limit 1""")
        assert actual == expected

    @authors("levysotsky")
    # TODO(levysotsky): Test more builtin functions?
    def test_any_to_yson_string(self):
        sync_create_cells(1)
        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "any"},
                ],
            },
        )
        sync_mount_table("//tmp/t")

        data = [{"a": i, "b": {"x": i}} for i in range(0, 11)]
        insert_rows("//tmp/t", data)

        expected = [{"a": 7, "b_str": '{"x"=7;}'}]
        actual = select_rows(r"a, any_to_yson_string(b) as b_str from [//tmp/t] where a = 7")
        assert expected == actual

        length = 100000
        long_binary_string = b"\xFF" * length
        escaped_string = r"\xFF" * length
        long_yson_rows = [{"a": 13, "b": {"x": long_binary_string}}]
        expected = [{"a": 13, "b_str": '{"x"="' + escaped_string + '";}'}]
        insert_rows("//tmp/t", long_yson_rows)
        actual = select_rows(r"a, any_to_yson_string(b) as b_str from [//tmp/t] where a = 13")
        assert expected == actual

    @authors("ermolovd")
    def test_join_different_types(self):
        sync_create_cells(1)

        tt = "//tmp/t"
        tj = "//tmp/j"

        create_dynamic_table(tt, schema=[
            make_sorted_column("key", optional_type("int16")),
            make_column("value", "string")
        ])

        create_dynamic_table(tj, schema=[
            make_sorted_column("key", "int32"),
            make_column("value_value", "string"),
        ])

        sync_mount_table(tt)
        sync_mount_table(tj)

        insert_rows(tt, [{"key": i, "value": str(i)} for i in range(5)])
        insert_rows(tj, [{"key": i, "value_value": "{0}_{0}".format(str(i))} for i in range(10)])

        expected = [{"key": i, "value": str(i), "value_value": "{0}_{0}".format(str(i))} for i in range(5)]

        actual = select_rows("* from [//tmp/t] join [//tmp/j] using key")
        assert_items_equal(actual, expected)

    @authors("ermolovd")
    def test_join_nonv1_types(self):
        sync_create_cells(1)

        tt = "//tmp/t"
        tj = "//tmp/j"

        create_dynamic_table(tt, schema=[
            make_sorted_column("a", "string"),
            make_column("b", decimal_type(3, 2))
        ])

        create_dynamic_table(tj, schema=[
            make_sorted_column("b", "string"),
            make_column("c", "string"),
        ])

        sync_mount_table(tt)
        sync_mount_table(tj)

        insert_rows(tt, [{"a": "a", "b": b"\x80\x00\x00\x00"}])
        insert_rows(tj, [{"b": b"\x80\x00\x00\x00", "c": "c"}])

        with raises_yt_error("nonsimple type"):
            select_rows("* from [//tmp/t] join [//tmp/j] using b")

    @authors("dtorilov")
    def test_select_with_placeholders(self):
        sync_create_cells(1)
        self._create_table(
            "//tmp/t",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64"},
                {"name": "c", "type": "int64"},
                {"name": "d", "type": "string"},
            ],
            [
                {"a": 1, "b": 0, "c": 1, "d": "a"},
                {"a": 2, "b": 0, "c": 5, "d": "f"},
                {"a": 3, "b": 1, "c": 3, "d": "a"},
                {"a": 4, "b": 1, "c": 1, "d": "d"},
                {"a": 5, "b": 1, "c": 3, "d": "d"},
                {"a": 6, "b": 0, "c": 1, "d": "a"},
                {"a": 7, "b": 0, "c": 1, "d": "a"},
                {"a": 8, "b": 1, "c": 5, "d": "f"},
            ],
            "scan",
        )

        expected = [
            {"a": 3, "b": 1, "c": 3, "d": "a"},
            {"a": 5, "b": 1, "c": 3, "d": "d"},
            {"a": 8, "b": 1, "c": 5, "d": "f"},
        ]

        requests = [
            (
                r"a, b, c, d from [//tmp/t] where b = {first} and (c, d) > {second} order by a limit 3",
                {"first": 1, "second": [2, "b"]},
            ),
            (
                r"a, b, c, d from [//tmp/t] where b = {first} and (c, d) > ({second}, {third}) order by a limit 3",
                {"first": 1, "second": 2, "third": "b"},
            ),
        ]

        for query, placeholders in requests:
            actual = select_rows(query, placeholder_values=placeholders)
            assert expected == actual

    @authors("akozhikhov", "dave11ar")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_filter_ranges(self, optimize_for):
        sync_create_cells(1)

        table_path = f"//tmp/t{generate_uuid()}"

        create(
            "table",
            table_path,
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64", "sort_order": "ascending"},
                    {"name": "c", "type": "int64", "sort_order": "ascending"},
                    {"name": "d", "type": "int64"},
                ],
                "chunk_writer": {
                    "key_prefix_filter" : {
                        "enable": True,
                        "prefix_lengths": [1, 3],
                    },
                },
                "mount_config": {
                    "enable_key_filter_for_lookup": True,
                },
                "optimize_for": optimize_for,
            },
        )

        sync_mount_table(table_path)

        rows = [
            {"a": 1, "b": 1, "c": 1, "d": 1},
            {"a": 3, "b": 3, "c": 3, "d": 3},
            {"a": 5, "b": 5, "c": 5, "d": 5},
        ]
        insert_rows(table_path, rows)

        sync_flush_table(table_path)

        profiling = self._get_key_filter_profiling_wrapper("select", table_path)

        def _check_query(expected, predicate, min_input):
            def _check_counters():
                input, filtered_out, false_positive = profiling.get_deltas()
                return input >= min_input and 0 <= filtered_out + false_positive <= input

            assert_items_equal(select_rows(f"* from [{table_path}] where {predicate}"), expected)
            wait(_check_counters)
            profiling.commit()

        _check_query(rows[0:1], "(a) in ((1), (2))", 1)
        _check_query(rows[0:1], "(a, b) in ((1, 1), (1, 2), (2, 1))", 3)
        _check_query(rows[0:2], "(a, b, c) in ((1, 1, 1), (2, 2, 2), (3, 3, 3))", 3)

        _check_query(rows[0:1], "(a) between (1) and (2)", 0)
        _check_query(rows[0:1], "(a, b) between ((1) and (1, 2))", 1)

        # No common prefix in ranges inferred in following query.
        _check_query([], "(a, b) between (1, 2) and (2, 1)", 0)
        _check_query([], "(a, b) between ((2) and (2, 1))", 1)
        _check_query([rows[1], rows[2]], """(a, b, c) between (
                     (3, 3, 2) and (3, 3, 4),
                     (5, 3) and (5, 4),
                     (5, 5, 5) and (5, 6))""", 1)

    @authors("dtorilov")
    def test_select_with_case_operator(self):
        sync_create_cells(1)
        self._create_table(
            "//tmp/t",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64"},
            ],
            [
                {"a": 0, "b": 0},
                {"a": 3, "b": 2},
                {"a": 1, "b": 2},
            ],
            "scan",
        )

        requests = [
            (
                """
                select case
                    when a = 0 then 'aaa'
                    when 15/a = 5 then 'bbb'
                    else 'ccc'
                end as m
                from [//tmp/t]
                order by m
                limit 3
                """,
                [
                    {"m": "aaa"},
                    {"m": "bbb"},
                    {"m": "ccc"},
                ]
            ),
            (
                """
                select case a
                    when 0 then b
                    when 1 then b + b * b
                    else 2
                end as m
                from [//tmp/t]
                order by m
                limit 3
                """,
                [
                    {"m": 0},
                    {"m": 2},
                    {"m": 6},
                ]
            ),
        ]

        for query, expected in requests:
            actual = select_rows(query)
            assert expected == actual

    @authors("sabdenovch")
    def test_select_with_canonical_null_relations(self):
        sync_create_cells(1)
        self._create_table(
            "//tmp/t",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64"},
            ],
            [
                {"a": 0, "b": 0},
                {"a": 1, "b": 2},
                {"a": 3},
            ])

        assert_items_equal(
            select_rows("a from [//tmp/t] where b != 0 limit 3"),
            [{"a": 1}, {"a": 3}])
        assert_items_equal(
            select_rows("a from [//tmp/t] where b != 0 limit 3", use_canonical_null_relations=True),
            [{"a": 1}])

    @authors("sabdenovch")
    def test_read_without_merge_sorted(self):
        sync_create_cells(1)
        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "key1", "type": "int64", "sort_order": "ascending"},
                    {"name": "key2", "type": "string", "sort_order": "ascending"},
                    {"name": "value1", "type": "int64"},
                    {"name": "value2", "type": "string"},
                    {"name": "aggr", "type": "int64", "aggregate": "sum"},
                ],
                "single_column_group_by_default": False,
            })

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [
            {"key1": 1, "key2": "2", "value1": 0, "value2": "value", "aggr": 0},
            {"key1": 2, "value1": 2},
        ], update=True, aggregate=True)

        ts2 = "$timestamp:value2"

        def find(rows, key1, value2):
            return list(filter(lambda x: x["key1"] == key1 and x["value2"] == value2, rows))[0]

        assert_items_equal(
            select_rows("key1, value2 from [//tmp/t]", merge_versioned_rows=False),
            [{"key1": 1, "value2": "value"}, {"key1": 2, "value2": None}],
        )

        timestamped_rows = select_rows(
            f"key1, value2, [{ts2}] from [//tmp/t]",
            merge_versioned_rows=False,
            with_timestamps=True,
        )
        ts_value1 = find(timestamped_rows, 1, "value")[ts2]
        ts_value2 = find(timestamped_rows, 2, None)[ts2]
        assert not isinstance(ts_value1, yson.YsonEntity)
        assert isinstance(ts_value2, yson.YsonEntity)

        sync_flush_table("//tmp/t")
        insert_rows(
            "//tmp/t",
            [
                {"key1": 1, "key2": "2", "value1": 2, "value2": "new_value", "aggr": 1},
                {"key1": 1, "key2": "2", "value1": 2, "value2": "new_value", "aggr": 2},
            ],
            aggregate=True,
        )

        assert_items_equal(
            select_rows("key1, value2, aggr from [//tmp/t]", merge_versioned_rows=False),
            [
                {"key1": 1, "value2": "value", "aggr": 0},
                {"key1": 1, "value2": "new_value", "aggr": 3},
                {"key1": 2, "value2": None, "aggr": None}
            ],
        )

        timestamped_rows = select_rows(
            f"key1, value2, aggr, [{ts2}] from [//tmp/t]",
            merge_versioned_rows=False,
            with_timestamps=True,
        )
        ts_value1 = find(timestamped_rows, 1, "value")[ts2]
        ts_value2 = find(timestamped_rows, 1, "new_value")[ts2]
        ts_value3 = find(timestamped_rows, 2, None)[ts2]
        assert ts_value1 < ts_value2
        assert isinstance(ts_value3, yson.YsonEntity)

    @authors("sabdenovch")
    def test_array_join(self):
        sync_create_cells(1)
        self._create_table(
            "//tmp/t",
            [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "nestedA", "type_v3": {"type_name": "optional", "item": {"type_name": "list", "item": "int64"}}},
                {"name": "nestedB", "type_v3": {"type_name": "list", "item": "string"}},
            ],
            [
                {"key": 1, "nestedA": [1, 2, 3], "nestedB": ["1", "2", "3"]},
                {"key": 2, "nestedA": [5, 6], "nestedB": ["5"]},
                {"key": 3, "nestedA": [7], "nestedB": ["7", "8"]},
                {"key": 4, "nestedA": None, "nestedB": []},
            ],
        )

        actual = select_rows("key, flattenedA, flattenedB from [//tmp/t] array join nestedA as flattenedA, nestedB as flattenedB limit 100")
        expected = [
            {"key": 1, "flattenedA": 1, "flattenedB": "1"},
            {"key": 1, "flattenedA": 2, "flattenedB": "2"},
            {"key": 1, "flattenedA": 3, "flattenedB": "3"},

            {"key": 2, "flattenedA": 5, "flattenedB": "5"},
            {"key": 2, "flattenedA": 6, "flattenedB": None},

            {"key": 3, "flattenedA": 7, "flattenedB": "7"},
            {"key": 3, "flattenedA": None, "flattenedB": "8"},
        ]
        assert expected == actual

        expected.append({"key": 4, "flattenedA": None, "flattenedB": None})
        actual = select_rows("key, flattenedA, flattenedB from [//tmp/t] left array join nestedA as flattenedA, nestedB as flattenedB limit 100")
        assert expected == actual

    @authors("sabdenovch")
    def test_array_join_with_table_join(self):
        sync_create_cells(1)
        self._create_table(
            "//tmp/a",
            [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "nestedA", "type_v3": {"type_name": "list", "item": {"type_name": "optional", "item": "int64"}}},
            ],
            [
                {"key": 1, "nestedA": [1, None, 3]},
                {"key": 2, "nestedA": [5, 6]},
                {"key": 3, "nestedA": [7]},
                {"key": 4, "nestedA": []},
            ],
        )

        self._create_table(
            "//tmp/b",
            [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "nestedB", "type_v3": {"type_name": "list", "item": "string"}},
            ],
            [
                {"key": 1, "nestedB": ["1", "2", "3"]},
                {"key": 2, "nestedB": ["5"]},
                {"key": 3, "nestedB": ["7", "8"]},
                {"key": 4, "nestedB": []},
            ],
        )

        actual = select_rows(
            "key, flattenedA, flattenedB from [//tmp/a] "
            "array join nestedA as flattenedA "
            "join [//tmp/b] using key "
            "array join nestedB as flattenedB "
            "limit 100")
        expected = [
            {"key": 1, "flattenedA": 1, "flattenedB": "1"},
            {"key": 1, "flattenedA": 1, "flattenedB": "2"},
            {"key": 1, "flattenedA": 1, "flattenedB": "3"},
            {"key": 1, "flattenedA": None, "flattenedB": "1"},
            {"key": 1, "flattenedA": None, "flattenedB": "2"},
            {"key": 1, "flattenedA": None, "flattenedB": "3"},
            {"key": 1, "flattenedA": 3, "flattenedB": "1"},
            {"key": 1, "flattenedA": 3, "flattenedB": "2"},
            {"key": 1, "flattenedA": 3, "flattenedB": "3"},

            {"key": 2, "flattenedA": 5, "flattenedB": "5"},
            {"key": 2, "flattenedA": 6, "flattenedB": "5"},

            {"key": 3, "flattenedA": 7, "flattenedB": "7"},
            {"key": 3, "flattenedA": 7, "flattenedB": "8"},
        ]
        assert expected == actual

        actual = select_rows(
            "A.key, flattenedA, flattenedB from [//tmp/b] AS B "
            "array join B.nestedB as flattenedB "
            "join [//tmp/a] AS A on B.key = A.key "
            "array join A.nestedA as flattenedA "
            "limit 100")
        expected = [
            {"A.key": 1, "flattenedB": "1", "flattenedA": 1},
            {"A.key": 1, "flattenedB": "1", "flattenedA": None},
            {"A.key": 1, "flattenedB": "1", "flattenedA": 3},
            {"A.key": 1, "flattenedB": "2", "flattenedA": 1},
            {"A.key": 1, "flattenedB": "2", "flattenedA": None},
            {"A.key": 1, "flattenedB": "2", "flattenedA": 3},
            {"A.key": 1, "flattenedB": "3", "flattenedA": 1},
            {"A.key": 1, "flattenedB": "3", "flattenedA": None},
            {"A.key": 1, "flattenedB": "3", "flattenedA": 3},

            {"A.key": 2, "flattenedB": "5", "flattenedA": 5},
            {"A.key": 2, "flattenedB": "5", "flattenedA": 6},

            {"A.key": 3, "flattenedB": "7", "flattenedA": 7},
            {"A.key": 3, "flattenedB": "8", "flattenedA": 7},
        ]
        assert expected == actual

    @authors("sabdenovch")
    def test_array_join_descartes(self):
        sync_create_cells(1)
        self._create_table(
            "//tmp/t",
            [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "nestedA", "type_v3": {"type_name": "optional", "item": {"type_name": "list", "item": "int64"}}},
                {"name": "nestedB", "type_v3": {"type_name": "list", "item": "string"}},
            ],
            [
                {"key": 1, "nestedA": [1, 2, 3], "nestedB": ["1", "2", "3"]},
                {"key": 2, "nestedA": [5, 6], "nestedB": ["5"]},
                {"key": 3, "nestedA": [7], "nestedB": ["7", "8"]},
                {"key": 4, "nestedA": None, "nestedB": []},
            ],
        )

        actual = select_rows("key, flattenedA, flattenedB from [//tmp/t] array join nestedA as flattenedA array join nestedB as flattenedB limit 100")
        expected = [
            {"key": 1, "flattenedA": 1, "flattenedB": "1"},
            {"key": 1, "flattenedA": 1, "flattenedB": "2"},
            {"key": 1, "flattenedA": 1, "flattenedB": "3"},
            {"key": 1, "flattenedA": 2, "flattenedB": "1"},
            {"key": 1, "flattenedA": 2, "flattenedB": "2"},
            {"key": 1, "flattenedA": 2, "flattenedB": "3"},
            {"key": 1, "flattenedA": 3, "flattenedB": "1"},
            {"key": 1, "flattenedA": 3, "flattenedB": "2"},
            {"key": 1, "flattenedA": 3, "flattenedB": "3"},

            {"key": 2, "flattenedA": 5, "flattenedB": "5"},
            {"key": 2, "flattenedA": 6, "flattenedB": "5"},

            {"key": 3, "flattenedA": 7, "flattenedB": "7"},
            {"key": 3, "flattenedA": 7, "flattenedB": "8"},
        ]
        assert expected == actual

    @authors("dtorilov")
    def test_composite_types(self):
        sync_create_cells(1)

        path = "//tmp/t"

        create_dynamic_table(path, schema=[
            make_sorted_column("key", "int64"),
            make_column("value", struct_type([("a", "int16"), ("b", "int16"), ])),
        ])

        sync_mount_table(path)

        insert_rows(path, [{"key": i, "value": {"a": i, "b": i*2}} for i in range(100)])

        expected = [{"k": i, "v": i} for i in range(100)]

        actual = select_rows(f"t.key as k, t.value.a as v from `{path}` as t limit 100", syntax_version=2)
        assert expected == actual

    @authors("dave11ar")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_versioned_select(self, optimize_for):
        path = "//tmp/t"

        sync_create_cells(1)

        self._create_table(
            path=path,
            optimize_for=optimize_for,
            schema=[
                {"name": "k", "type": "int64", "sort_order": "ascending"},
                {"name": "v1", "type": "string"},
                {"name": "v2", "type": "string"},
                {"name": "v3", "type": "string"},
                {"name": "v4", "type": "int64", "aggregate": "sum"},
                {"name": "v5", "type": "string"},
            ],
            data=[{"k": 1, "v1": "a", "v2": "a", "v3": "a", "v4": 1, "v5": "a"}],
        )

        insert_rows(
            path=path,
            data=[{"k": 1, "v1": "b", "v3": "b"}],
            update=True,
        )
        insert_rows(
            path=path,
            data=[{"k": 1, "v2": "c", "v5": "c"}],
            update=True,
        )

        def check_vs(row, v1=None, v2=None, v3=None, v4=None, v5=None):
            def check(name, value):
                if value:
                    assert row[name] == value
                else:
                    assert name not in row

            assert row["k"] == 1
            check("v1", v1)
            check("v2", v2)
            check("v3", v3)
            check("v4", v4)
            check("v5", v5)

        def check_timestamps(row, order):
            previous_ts = 0
            for columns in order:
                current_ts = row[f"$timestamp:{columns[0]}"]
                assert previous_ts < current_ts

                for i in range(1, len(columns)):
                    assert row[f"$timestamp:{columns[i]}"] == current_ts

                previous_ts = current_ts

        row = select_rows("* from [//tmp/t]", with_timestamps=True)[0]
        check_vs(row, "b", "c", "b", 1, "c")
        check_timestamps(row, [["v4"], ["v1", "v3"], ["v2", "v5"]])

        first_timestamp = row["$timestamp:v4"]
        row = select_rows("* from [//tmp/t]", timestamp=first_timestamp, with_timestamps=True)[0]
        check_vs(row, "a", "a", "a", 1, "a")
        check_timestamps(row, [["v1", "v2", "v3", "v4", "v5"]])
        assert row["$timestamp:v1"] == first_timestamp

        row = select_rows(f"k, v1, [$timestamp:v1], v2, v3, [$timestamp:v5], v4, v5, [$timestamp:v4], [$timestamp:v3] from [{path}]", with_timestamps=True)[0]
        check_vs(row, "b", "c", "b", 1, "c")
        check_timestamps(row, [["v4"], ["v1", "v3"], ["v5"]])

        row = select_rows(f"k, [$timestamp:v2], [$timestamp:v1], [$timestamp:v5], [$timestamp:v3], [$timestamp:v4] from [{path}]", with_timestamps=True)[0]
        check_vs(row)
        check_timestamps(row, [["v4"], ["v1", "v3"], ["v2", "v5"]])

        row = select_rows(f"k, [$timestamp:v2], v1, v4, [$timestamp:v3], [$timestamp:v4] from [{path}]", with_timestamps=True)[0]
        check_vs(row, v1="b", v4=1)
        check_timestamps(row, [["v4"], ["v3"], ["v2"]])

        with raises_yt_error('Undefined reference "$timestamp:v3"'):
            select_rows(f"k, v1, v2, [$timestamp:v3], v4 from [{path}]")

        with raises_yt_error('Undefined reference "$timestamp:v42"'):
            select_rows(f"k, v1, v2, [$timestamp:v42], v4 from [{path}]", with_timestamps=True)

        renamed = select_rows(
            f"[$timestamp:v4] as tv4_2, [$timestamp:v4] as tv4_1, k as key, v4 as vv4, [$timestamp:v2] as tv2 from [{path}]",
            with_timestamps=True,
        )[0]
        assert len(renamed) == 5
        assert renamed["key"] == 1
        assert renamed["vv4"] == 1
        assert renamed["tv2"] > renamed["tv4_1"]
        assert renamed["tv4_1"] == renamed["tv4_2"]

        old_timestamp = renamed["tv2"]

        lookup = select_rows(f"[$timestamp:v2] from [{path}] where k = 1", with_timestamps=True)
        assert len(lookup) == 1
        assert lookup[0]["$timestamp:v2"] == old_timestamp

        insert_rows(
            path=path,
            data=[{"k": 2, "v1": "i", "v2": "i", "v3": "i", "v4": 10, "v5": "i"}])

        selected_with_where = select_rows(f"* from [{path}] where [$timestamp:v1] > {old_timestamp}", with_timestamps=True)
        assert len(selected_with_where) == 1
        assert selected_with_where[0]["k"] == 2

        delete_rows(path, [{"k": 2}])

        def check_aggregate(aggregate, v1, v4, expected_v4, last_value):
            insert_rows(
                path=path,
                data=[{"k": 1, "v1": v1, "v4": v4}],
                update=True,
                aggregate=aggregate,
            )

            row = select_rows(
                f"k, [$timestamp:v2], v1, v4, v3, v2, v5, [$timestamp:v3], [$timestamp:v4], [$timestamp:v1], [$timestamp:v5] from [{path}]",
                with_timestamps=True,
            )[0]
            check_vs(row, v1, "c", "b", expected_v4, "c")
            check_timestamps(row, [["v3"], ["v2", "v5"], ["v1", "v4"]])

            new_last_value = row["$timestamp:v4"]
            assert new_last_value > last_value
            return new_last_value

        check_aggregate(
            aggregate=True,
            v1="e",
            v4=4,
            expected_v4=7,
            last_value=check_aggregate(
                aggregate=False,
                v1="d",
                v4=3,
                expected_v4=3,
                last_value=row["$timestamp:v4"],
            ),
        )

        delete_rows(path, [{"k": 1}])
        assert select_rows(
            f"k, [$timestamp:v2], v1, v4, v3, v2, v5, [$timestamp:v3], [$timestamp:v4], [$timestamp:v1], [$timestamp:v5] from [{path}]",
            with_timestamps=True,
        ) == []

    @authors("sabdenovch")
    def test_select_from_ordered_table(self):
        sync_create_cells(1)

        path = "//tmp/t"

        create_dynamic_table(path, schema=[
            make_column("value", "int64"),
        ])

        sync_mount_table(path)

        insert_rows(path, [{"value": i} for i in range(10)])

        # full scan
        expected = [{"$tablet_index": 0, "$row_index": i, "value": i} for i in range(10)]
        actual = select_rows(f"* from [{path}] limit 10")
        assert expected == actual

        # prefix scan
        actual = select_rows(f"* from [{path}] where [$tablet_index] in (0) limit 10")
        assert expected == actual

        # full key scan
        expected = [{"$tablet_index": 0, "$row_index": i, "value": i} for i in range(1)]
        actual = select_rows(f"* from [{path}] where ([$tablet_index], [$row_index]) in ((0,0))")
        assert expected == actual

        # join on common key
        expected = [{"$tablet_index": 0, "$row_index": i, "value": i} for i in range(10)]
        actual = select_rows(f"* from [{path}] join [{path}] using [$tablet_index], [$row_index], value limit 10")
        assert expected == actual

        # join on common key prefix
        expected = [{"A.value": i} for i in range(10)]
        actual = select_rows(f"A.value from [{path}] A join [{path}] B on "
                             "(A.[$tablet_index], A.value) = (B.[$tablet_index], B.[$row_index]) limit 10")
        assert expected == actual

        # join on foreign key prefix
        expected = [{"A.value": i} for i in range(10)]
        actual = select_rows(f"A.value from [{path}] A join [{path}] B on "
                             "(0, A.value) = (B.[$tablet_index], B.[$row_index]) limit 10")
        assert expected == actual


class TestQueryRpcProxy(TestQuery):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    @authors("akozhikhov", "alexelexa")
    def test_detailed_select_profiling(self):
        sync_create_cells(1)
        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
                "enable_detailed_profiling": True,
            },
        )
        sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "one"}]
        insert_rows("//tmp/t", rows)

        node_select_duration_histogram = profiler_factory().at_tablet_node("//tmp/t").histogram(
            name="select/duration")

        rpc_proxy = ls("//sys/rpc_proxies")[0]

        rpc_driver_config = deepcopy(self.Env.configs["rpc_driver"])
        rpc_driver_config["proxy_addresses"] = [rpc_proxy]
        rpc_driver_config["api_version"] = 3
        rpc_driver = Driver(config=rpc_driver_config)

        proxy_select_duration_histogram = profiler_factory().at_rpc_proxy(rpc_proxy).histogram(
            name="rpc_proxy/detailed_table_statistics/select_duration",
            fixed_tags={"table_path": "//tmp/t"})

        def check():
            def _check(select_duration_histogram):
                try:
                    bins = select_duration_histogram.get_bins(verbose=True)
                    bin_counters = [bin["count"] for bin in bins]
                    if sum(bin_counters) != 1:
                        return False
                    if len(bin_counters) < 20:
                        return False
                    return True
                except YtError as e:
                    # TODO(eshcherbin): get rid of this.
                    if "No sensors have been collected so far" not in str(e):
                        raise e

            assert select_rows("""* from [//tmp/t]""", driver=rpc_driver) == rows

            try:
                wait(lambda: _check(node_select_duration_histogram), iter=5, sleep_backoff=0.5)
                wait(lambda: _check(proxy_select_duration_histogram), iter=5, sleep_backoff=0.5)
                return True
            except WaitFailed:
                return False

        wait(lambda: check())
        assert profiler_factory().at_rpc_proxy(rpc_proxy).get(
            name="rpc_proxy/detailed_table_statistics/select_mount_cache_wait_time",
            tags={"table_path": "//tmp/t"},
            postprocessor=lambda data: data.get('all_time_max'),
            summary_as_max_for_all_time=True,
            export_summary_as_max=True,
            verbose=False,
            default=0) > 0
