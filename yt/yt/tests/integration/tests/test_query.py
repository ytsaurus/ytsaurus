import pytest
import __builtin__

import os
import os.path

from yt_env_setup import YTEnvSetup, find_ut_file, skip_if_rpc_driver_backend
from yt_commands import *

from yt.environment.helpers import assert_items_equal
from yt.yson import YsonList

from flaky import flaky

from random import randint, shuffle

from math import isnan

##################################################################

class TestQuery(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_DRIVER_CONFIG = {
        "client_cache": {
            "capacity": 10,
            "shard_count": 1
        },
        "function_registry_cache": {
            "success_expiration_time": 3000,
            "success_probation_time": 2000,
            "failure_expiration_time": 3000
        }
    }

    def _sample_data(self, path="//tmp/t", chunks=3, stripe=3):
        create("table", path, attributes={
                   "dynamic": True,
                   "optimize_for" : "scan",
                   "schema": [
                        {"name": "a", "type": "int64", "sort_order": "ascending"},
                        {"name": "b", "type": "int64"}
                    ]
               })

        sync_mount_table(path)

        for i in xrange(chunks):
            data = [
                {"a": (i * stripe + j), "b": (i * stripe + j) * 10}
                for j in xrange(1, 1 + stripe)]
            insert_rows(path, data)

    def _create_table(self, path, schema, data, optimize_for = "lookup"):
        create("table", path,
               attributes={
                   "dynamic": True,
                   "optimize_for" : optimize_for,
                   "schema": schema
               })

        sync_mount_table(path)
        insert_rows(path, data)

    @authors("sandello")
    def test_simple(self):
        sync_create_cells(1)
        for i in xrange(0, 50, 10):
            path = "//tmp/t{0}".format(i)

            self._sample_data(path=path, chunks=i, stripe=10)
            result = select_rows("a, b from [{}]".format(path), verbose=False)

            assert len(result) == 10 * i

    @authors("lukyan")
    def test_full_scan(self):
        sync_create_cells(1)
        self._sample_data(path="//tmp/t")
        with pytest.raises(YtError): select_rows("* from [//tmp/t]", allow_full_scan=False)

    @authors("sandello")
    def test_project1(self):
        sync_create_cells(1)
        self._sample_data(path="//tmp/t")
        expected = [{"s": 2 * i + 10 * i - 1} for i in xrange(1, 10)]
        actual = select_rows("2 * a + b - 1 as s from [//tmp/t]")
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

        create("table", tt, attributes={
               "dynamic": True,
               "optimize_for" : "scan",
               "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64", "sort_order": "ascending"},
                    {"name": "v", "type": "int64"}
                ]
           })

        reshard_table(tt, [[], [3, 3], [6, 6]])
        sync_mount_table(tt)

        data = [{"a": i / 10, "b": i % 10, "v": i} for i in xrange(100)]
        insert_rows(tt, data)

        grouped = {}
        for item in data:
            key = (item['a'], item['v'] % 2)
            if key not in grouped:
                grouped[key] = 0
            grouped[key] +=  item['b']

        expected = [{'k': k, 'x': x, 's': s} for (k, x), s in grouped.items()]

        actual = select_rows("k, x, sum(b) as s from [//tmp/t] group by a as k, v % 2 as x")
        assert_items_equal(actual, expected)

    @authors("lukyan")
    def test_group_by_disjoint(self):
        sync_create_cells(1)

        tt = "//tmp/t"
        tj = "//tmp/j"

        create("table", tt, attributes={
               "dynamic": True,
               "optimize_for" : "scan",
               "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "dummy", "type": "int64"}
                ]
           })

        reshard_table(tt, [[], [3], [6]])
        sync_mount_table(tt)

        insert_rows(tt, [{"a": i} for i in xrange(10)])

        create("table", tj, attributes={
               "dynamic": True,
               "optimize_for" : "scan",
               "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64", "sort_order": "ascending"},
                    {"name": "v", "type": "int64"}
                ]
           })

        reshard_table(tj, [[], [3, 6], [6, 6]])
        sync_mount_table(tj)

        data = [{"a": i / 10, "b": i % 10, "v": i} for i in xrange(100)]
        insert_rows(tj, data)

        grouped = {}
        for item in data:
            key = (item['a'], item['v'] % 2)
            if key not in grouped:
                grouped[key] = 0
            grouped[key] +=  item['b']

        expected = [{'k': k, 'x': x, 's': s} for (k, x), s in grouped.items()]

        actual = select_rows("k, x, sum(b) as s from [//tmp/t] join [//tmp/j] using a group by a as k, v % 2 as x")
        assert_items_equal(actual, expected)

    @authors("lukyan")
    def test_having(self):
        sync_create_cells(3)

        create("table", "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64"}]
            })

        sync_mount_table("//tmp/t")

        data = [{"a" : i, "b" : i * 10} for i in xrange(0,100)]
        insert_rows("//tmp/t", data)

        expected = [{"k": 0, "aa": 49.0, "mb": 0, "ab": 490.0}]
        actual = select_rows("""
            k, avg(a) as aa, min(b) as mb, avg(b) as ab
            from [//tmp/t]
            group by a % 2 as k
            having mb < 5""")
        assert expected == actual

    @authors("lbrown")
    def test_merging_group_by(self):
        sync_create_cells(1)

        create("table", "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64"}]
            })

        pivots = [[i*5] for i in xrange(0,20)]
        pivots.insert(0, [])
        reshard_table("//tmp/t", pivots)

        sync_mount_table("//tmp/t")

        data = [{"a" : i, "b" : i * 10} for i in xrange(0,100)]
        insert_rows("//tmp/t", data)

        expected = [
            {"k": 0, "aa": 49.0, "mb": 0, "ab": 490.0},
            {"k": 1, "aa": 50.0, "mb": 10, "ab": 500.0}]
        actual = select_rows("""
            k, avg(a) as aa, min(b) as mb, avg(b) as ab
            from [//tmp/t]
            group by a % 2 as k
            order by k limit 2""")
        assert expected == actual

    @authors("lbrown")
    def test_merging_group_by2(self):
        sync_create_cells(1)

        create("table", "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for" : "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "string"}]
            })

        pivots = [[i*5] for i in xrange(0,20)]
        pivots.insert(0, [])
        reshard_table("//tmp/t", pivots)

        sync_mount_table("//tmp/t")

        data = [{"a" : i, "b" : str(i)} for i in xrange(0,100)]
        insert_rows("//tmp/t", data)

        expected = [
            {"k": 0, "m": "98"},
            {"k": 1, "m": "99"}]
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

        create("table", "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for" : "scan",
                "schema": [
                    {"name": "k", "type": "int64", "sort_order": "ascending"},
                    {"name": "u", "type": "int64"},
                    {"name": "v", "type": "int64"}]
            })

        sync_mount_table("//tmp/t")

        values = [i for i in xrange(0, 300)]
        shuffle(values)

        data = [
            {"k": i, "v": values[i], "u": randint(0, 1000)}
            for i in xrange(0, 100)]
        insert_rows("//tmp/t", data)

        def pick_items(row, items):
            return dict((col, v) for col, v in row.iteritems() if col in items)

        filtered = [pick_items(row, ['k', 'v']) for row in data if row['u'] > 500]
        expected = sorted(filtered, cmp=lambda x, y: x['v'] - y['v'])[0:10];

        actual = select_rows("k, v from [//tmp/t] where u > 500 order by v limit 10")
        assert expected == actual

        expected = sorted(filtered, cmp=lambda x, y: x['v'] - y['v'])[20:30];

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

        create("table", tt, attributes={
               "dynamic": True,
               "optimize_for" : "scan",
               "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "dummy", "type": "int64"}
                ]
           })

        reshard_table(tt, [[], [3], [6]])
        sync_mount_table(tt)

        insert_rows(tt, [{"a": i} for i in xrange(10)])

        create("table", tj, attributes={
               "dynamic": True,
               "optimize_for" : "scan",
               "schema": [
                    {"name": "b", "type": "int64", "sort_order": "ascending"},
                    {"name": "c", "type": "int64", "sort_order": "ascending"},
                    {"name": "v", "type": "int64"}
                ]
           })

        reshard_table(tj, [[], [3, 6], [6, 6]])
        sync_mount_table(tj)

        data = [{"b": i / 10, "c": i % 10, "v": i} for i in xrange(100)]
        insert_rows(tj, data)

        expected = [dict(row.items() + [('a', row['b'])]) for row in data]

        actual = select_rows("a, b, c, v from [//tmp/t] join [//tmp/j] on a = b")
        assert_items_equal(actual, expected)

        actual = select_rows("a, b, c, v from [//tmp/t] join [//tmp/j] on (a + 0) = b")
        assert_items_equal(actual, expected)


        insert_rows(tt, [{"a": i} for i in xrange(100)])

        expected = [dict(row.items() + [('a', row['b'] * 10 + row['c'])]) for row in data]

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
                {"name": "b", "type": "int64"}],
            [],
            "scan");

        self._create_table(
            "//tmp/jr",
            [
                {"name": "c", "type": "int64", "sort_order": "ascending"},
                {"name": "d", "type": "int64"}],
            [],
            "scan");

        with pytest.raises(YtError): select_rows("* from [//tmp/jl] join [//tmp/jr] on b = d", allow_join_without_index=False)

    @authors("lukyan")
    def test_join(self):
        sync_create_cells(1)

        self._create_table(
            "//tmp/jl",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64", "sort_order": "ascending"},
                {"name": "c", "type": "int64"}],
            [
                {"a": 1, "b": 2, "c": 80 },
                {"a": 1, "b": 3, "c": 71 },
                {"a": 1, "b": 4, "c": 62 },
                {"a": 2, "b": 1, "c": 53 },
                {"a": 2, "b": 2, "c": 44 },
                {"a": 2, "b": 3, "c": 35 },
                {"a": 2, "b": 4, "c": 26 },
                {"a": 3, "b": 1, "c": 17 }
            ],
            "scan");


        self._create_table(
            "//tmp/jr",
            [
                {"name": "c", "type": "int64", "sort_order": "ascending"},
                {"name": "d", "type": "int64"},
                {"name": "e", "type": "int64"}],
            [
                {"d": 1, "e": 2, "c": 80 },
                {"d": 1, "e": 3, "c": 71 },
                {"d": 1, "e": 4, "c": 62 },
                {"d": 2, "e": 1, "c": 53 },
                {"d": 2, "e": 2, "c": 44 },
                {"d": 2, "e": 3, "c": 35 },
                {"d": 2, "e": 4, "c": 26 },
                {"d": 3, "e": 1, "c": 17 }
            ],
            "scan");

        expected = [
            {"a": 1, "b": 2, "c": 80, "d": 1, "e": 2},
            {"a": 1, "b": 3, "c": 71, "d": 1, "e": 3},
            {"a": 1, "b": 4, "c": 62, "d": 1, "e": 4},
            {"a": 2, "b": 1, "c": 53, "d": 2, "e": 1},
            {"a": 2, "b": 2, "c": 44, "d": 2, "e": 2},
            {"a": 2, "b": 3, "c": 35, "d": 2, "e": 3},
            {"a": 2, "b": 4, "c": 26, "d": 2, "e": 4},
            {"a": 3, "b": 1, "c": 17, "d": 3, "e": 1}]

        actual = select_rows("* from [//tmp/jl] join [//tmp/jr] using c where a < 4")
        assert sorted(expected) == sorted(actual)

        expected = [
            {"a": 2, "b": 1, "c": 53, "d": 2, "e": 1}]

        actual = select_rows("* from [//tmp/jl] join [//tmp/jr] using c where (a, b) IN ((2, 1))")
        assert expected == actual

        expected = [
            {"l.a": 2, "l.b": 1, "l.c": 53, "r.c": 53, "r.d": 2, "r.e": 1}]

        actual = select_rows("""
            * from [//tmp/jl] as l
            join [//tmp/jr] as r on l.c + 1 = r.c + 1
             where (l.a, l.b) in ((2, 1))""", allow_join_without_index=True)
        assert expected == actual

    @authors("lukyan")
    def test_join_common_prefix(self):
        sync_create_cells(1)

        self._create_table(
            "//tmp/jl",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64", "sort_order": "ascending"},
                {"name": "c", "type": "int64"}],
            [
                {"a": 1, "b": 2, "c": 80 },
                {"a": 1, "b": 3, "c": 71 },
                {"a": 1, "b": 4, "c": 62 },
                {"a": 2, "b": 1, "c": 53 },
                {"a": 2, "b": 2, "c": 44 },
                {"a": 2, "b": 3, "c": 35 },
                {"a": 2, "b": 4, "c": 26 },
                {"a": 3, "b": 1, "c": 17 }
            ],
            "scan");


        self._create_table(
            "//tmp/jr",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64", "sort_order": "ascending"},
                {"name": "d", "type": "int64"}],
            [
                {"a": 1, "b": 2, "d": 80 },
                {"a": 1, "b": 4, "d": 62 },
                {"a": 2, "b": 1, "d": 53 },
                {"a": 2, "b": 3, "d": 35 },
                {"a": 3, "b": 1, "d": 17 }
            ],
            "scan");

        expected = [
            {"a": 1, "b": 2, "c": 80, "d": 80},
            {"a": 1, "b": 3, "c": 71, "d": None},
            {"a": 1, "b": 4, "c": 62, "d": 62},
            {"a": 2, "b": 1, "c": 53, "d": 53},
            {"a": 2, "b": 2, "c": 44, "d": None},
            {"a": 2, "b": 3, "c": 35, "d": 35},
            {"a": 2, "b": 4, "c": 26, "d": None},
            {"a": 3, "b": 1, "c": 17, "d": 17}]

        actual = select_rows("* from [//tmp/jl] left join [//tmp/jr] using a, b")
        assert sorted(expected) == sorted(actual)

    @authors("lukyan")
    def test_join_common_prefix2(self):
        sync_create_cells(1)

        self._create_table(
            "//tmp/jl",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "c", "type": "int64"}],
            [
                {"a": 1, "c": 3 }
            ],
            "scan");


        self._create_table(
            "//tmp/jr",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64", "sort_order": "ascending"},
                {"name": "d", "type": "int64"}],
            [
                {"a": 1, "b": 2, "d": 4 }
            ],
            "scan");

        expected = [
            {"l.a": 1, "r.a": 1, "r.b": 2, "l.c": 3, "r.d": 4}]

        actual = select_rows("* from [//tmp/jl] l left join [//tmp/jr] r on (l.a, 2) = (r.a, r.b) where l.a = 1")
        assert sorted(expected) == sorted(actual)

    @authors("lukyan")
    def test_join_many(self):
        sync_create_cells(1)

        self._create_table(
            "//tmp/a",
            [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "c", "type": "string"}],
            [
                {"a": 1, "c": "a"},
                {"a": 2, "c": "b"},
                {"a": 3, "c": "c"},
                {"a": 4, "c": "a"},
                {"a": 5, "c": "b"},
                {"a": 6, "c": "c"}
            ]);

        self._create_table(
            "//tmp/b",
            [
                {"name": "b", "type": "int64", "sort_order": "ascending"},
                {"name": "c", "type": "string"},
                {"name": "d", "type": "string"}],
            [
                {"b": 100, "c": "a", "d": "X"},
                {"b": 200, "c": "b", "d": "Y"},
                {"b": 300, "c": "c", "d": "X"},
                {"b": 400, "c": "a", "d": "Y"},
                {"b": 500, "c": "b", "d": "X"},
                {"b": 600, "c": "c", "d": "Y"}
            ]);

        self._create_table(
            "//tmp/c",
            [
                {"name": "d", "type": "string", "sort_order": "ascending"},
                {"name": "e", "type": "int64"}],
            [
                {"d": "X", "e": 1234},
                {"d": "Y", "e": 5678}
            ]);

        expected = [
            {"a": 2, "c": "b", "b": 200, "d": "Y", "e": 5678},
            {"a": 2, "c": "b", "b": 500, "d": "X", "e": 1234},

            {"a": 3, "c": "c", "b": 300, "d": "X", "e": 1234},
            {"a": 3, "c": "c", "b": 600, "d": "Y", "e": 5678},

            {"a": 4, "c": "a", "b": 100, "d": "X", "e": 1234},
            {"a": 4, "c": "a", "b": 400, "d": "Y", "e": 5678}]

        actual = select_rows(
            "* from [//tmp/a] join [//tmp/b] using c join [//tmp/c] using d where a in (2,3,4)",
            allow_join_without_index=True)
        assert sorted(expected) == sorted(actual)

    @authors("lukyan")
    def test_types(self):
        sync_create_cells(1)

        create("table", "//tmp/t",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "boolean"},
                    {"name": "c", "type": "string"},
                    {"name": "d", "type": "uint64"}
                ]
            })
        sync_mount_table("//tmp/t")

        format = yson.loads("<format=text>yson")
        insert_rows(
            "//tmp/t",
            '{a=10;b=%false;c="hello";d=32u};{a=20;b=%true;c="world";d=64u};',
            input_format=format,
            is_raw=True)

        assert select_rows('a, b, c, d from [//tmp/t] where c="hello"', output_format=format) == \
                '{"a"=10;"b"=%false;"c"="hello";"d"=32u;};\n'

    @authors("lukyan")
    def test_tablets(self):
        sync_create_cells(1)

        create("table", "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for" : "scan",
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "int64"}]
            })

        sync_mount_table("//tmp/t")

        stripe = 10

        for i in xrange(0, 10):
            data = [
                {"key": (i * stripe + j), "value": (i * stripe + j) * 10}
                for j in xrange(1, 1 + stripe)]
            insert_rows("//tmp/t", data)

        sync_unmount_table("//tmp/t")
        reshard_table("//tmp/t", [[], [10], [30], [50], [70], [90]])
        sync_mount_table("//tmp/t", first_tablet_index=0, last_tablet_index=2)

        select_rows("* from [//tmp/t] where key < 50")

        with pytest.raises(YtError): select_rows("* from [//tmp/t] where key < 51")

    @authors("babenko", "savrus", "lukyan")
    def test_computed_column_simple(self):
        sync_create_cells(1)

        create("table", "//tmp/t",
                attributes={
                    "dynamic": True,
                    "schema": [
                        {"name": "hash", "type": "int64", "expression": "key * 33", "sort_order": "ascending"},
                        {"name": "key", "type": "int64", "sort_order": "ascending"},
                        {"name": "value", "type": "int64"}
                    ]
                })
        reshard_table("//tmp/t", [[]] + [[i] for i in xrange(1, 100 * 33, 1000)])
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": i, "value": i * 2} for i in xrange(0,100)])

        expected = [{"hash": 42 * 33, "key": 42, "value": 42 * 2}]
        actual = select_rows("* from [//tmp/t] where key = 42")
        assert_items_equal(actual, expected)

        expected = [{"hash": i * 33, "key": i, "value": i * 2} for i in xrange(10,80)]
        actual = sorted(select_rows("* from [//tmp/t] where key >= 10 and key < 80"))
        assert_items_equal(actual, expected)

        expected = [{"hash": i * 33, "key": i, "value": i * 2} for i in [10, 20, 30]]
        actual = sorted(select_rows("* from [//tmp/t] where key in (10, 20, 30)"))
        assert_items_equal(actual, expected)

    @authors("savrus")
    def test_computed_column_far_divide(self):
        sync_create_cells(1)

        create("table", "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for" : "scan",
                "schema": [
                    {"name": "hash", "type": "int64", "expression": "key2 / 2", "sort_order": "ascending"},
                    {"name": "key1", "type": "int64", "sort_order": "ascending"},
                    {"name": "key2", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "int64"}]
            })

        reshard_table("//tmp/t", [[]] + [[i] for i in xrange(1, 500, 10)])
        sync_mount_table("//tmp/t")

        def expected(key_range):
            return [{"hash": i / 2, "key1": i, "key2": i, "value": i * 2} for i in key_range]

        insert_rows("//tmp/t", [{"key1": i, "key2": i, "value": i * 2} for i in xrange(0,1000)])

        actual = select_rows("* from [//tmp/t] where key2 = 42")
        assert_items_equal(actual, expected([42]))

        actual = sorted(select_rows("* from [//tmp/t] where key2 >= 10 and key2 < 80"))
        assert_items_equal(actual, expected(xrange(10,80)))

        actual = sorted(select_rows("* from [//tmp/t] where key2 in (10, 20, 30)"))
        assert_items_equal(actual, expected([10, 20, 30]))

        actual = sorted(select_rows("* from [//tmp/t] where key2 in (10, 20, 30) and key1 in (30, 40)"))
        assert_items_equal(actual, expected([30]))

    @authors("savrus")
    def test_computed_column_modulo(self):
        sync_create_cells(1)

        create("table", "//tmp/t",
             attributes={
                 "dynamic": True,
                 "optimize_for" : "scan",
                 "schema": [
                    {"name": "hash", "type": "int64", "expression": "key2 % 2", "sort_order": "ascending"},
                    {"name": "key1", "type": "int64", "sort_order": "ascending"},
                    {"name": "key2", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "int64"}]
             })

        reshard_table("//tmp/t", [[]] + [[i] for i in xrange(1, 500, 10)])
        sync_mount_table("//tmp/t")

        def expected(key_range):
            return [{"hash": i % 2, "key1": i, "key2": i, "value": i * 2} for i in key_range]

        insert_rows("//tmp/t", [{"key1": i, "key2": i, "value": i * 2} for i in xrange(0,1000)])

        actual = select_rows("* from [//tmp/t] where key2 = 42")
        assert_items_equal(actual, expected([42]))

        actual = sorted(select_rows("* from [//tmp/t] where key1 >= 10 and key1 < 80"))
        assert_items_equal(actual, expected(xrange(10,80)))

        actual = sorted(select_rows("* from [//tmp/t] where key1 in (10, 20, 30)"))
        assert_items_equal(actual, expected([10, 20, 30]))

        actual = sorted(select_rows("* from [//tmp/t] where key1 in (10, 20, 30) and key2 in (30, 40)"))
        assert_items_equal(actual, expected([30]))

    @authors("lbrown")
    def test_udf(self):
        registry_path =  "//tmp/udfs"
        create("map_node", registry_path)

        abs_path = os.path.join(registry_path, "abs_udf")
        create("file", abs_path,
            attributes = { "function_descriptor": {
                "name": "abs_udf",
                "argument_types": [{
                    "tag": "concrete_type",
                    "value": "int64"}],
                "result_type": {
                    "tag": "concrete_type",
                    "value": "int64"},
                "calling_convention": "simple"}})

        local_implementation_path = find_ut_file("test_udfs.bc")
        write_local_file(abs_path, local_implementation_path)

        sync_create_cells(1)
        self._sample_data(path="//tmp/u")
        expected = [{"s": 2 * i} for i in xrange(1, 10)]
        actual = select_rows("abs_udf(-2 * a) as s from [//tmp/u]")
        assert_items_equal(actual, expected)

    @authors("lukyan")
    def test_udf_custom_path(self):
        registry_path =  "//home/udfs"
        create("map_node", "//home")
        create("map_node", registry_path)

        abs_path = os.path.join(registry_path, "abs_udf")
        create("file", abs_path,
            attributes = { "function_descriptor": {
                "name": "abs_udf",
                "argument_types": [{
                    "tag": "concrete_type",
                    "value": "int64"}],
                "result_type": {
                    "tag": "concrete_type",
                    "value": "int64"},
                "calling_convention": "simple"}})

        local_implementation_path = find_ut_file("test_udfs.bc")
        write_local_file(abs_path, local_implementation_path)

        sync_create_cells(1)
        self._sample_data(path="//tmp/u")
        expected = [{"s": 2 * i} for i in xrange(1, 10)]
        actual = select_rows("abs_udf(-2 * a) as s from [//tmp/u]", udf_registry_path=registry_path)
        assert_items_equal(actual, expected)

    @authors("lukyan")
    def test_udf_fc(self):
        registry_path =  "//tmp/udfs"
        create("map_node", registry_path)

        udf_fc_path = os.path.join(registry_path, "udf_fc")
        create("file", udf_fc_path,
            attributes = { "function_descriptor": {
                "name": "udf_with_function_context",
                "argument_types": [{
                    "tag": "concrete_type",
                    "value": "int64"}],
                "result_type": {
                    "tag": "concrete_type",
                    "value": "int64"},
                "calling_convention": "unversioned_value",
                "use_function_context": True}})

        local_implementation_path = find_ut_file("test_udfs_fc.bc")
        write_local_file(udf_fc_path, local_implementation_path)

        sync_create_cells(1)
        self._sample_data(path="//tmp/u")
        expected = [{"s": 2 * i} for i in xrange(1, 10)]
        actual = select_rows("udf_fc(2 * a) as s from [//tmp/u]")
        assert_items_equal(actual, expected)

    @authors("lbrown")
    def test_udaf(self):
        registry_path = "//tmp/udfs"
        create("map_node", registry_path)

        avg_path = os.path.join(registry_path, "avg_udaf")
        create("file", avg_path,
            attributes = { "aggregate_descriptor": {
                "name": "avg_udaf",
                "argument_type": {
                    "tag": "concrete_type",
                    "value": "int64"},
                "state_type": {
                    "tag": "concrete_type",
                    "value": "string"},
                "result_type": {
                    "tag": "concrete_type",
                    "value": "double"},
                "calling_convention": "unversioned_value"}})

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

        registry_path =  "//tmp/udfs"

        xxx_path = os.path.join(registry_path, "xxx_udf")
        create("map_node", registry_path)

        udfs_impl_path = find_ut_file("test_udfs.bc")

        create("file", xxx_path,
            attributes = { "function_descriptor": {
                "name": "exp_udf",
                "argument_types": [{
                    "tag": "concrete_type",
                    "value": "int64"},
                   {"tag": "concrete_type",
                    "value": "int64"}],
                "result_type": {
                    "tag": "concrete_type",
                    "value": "int64"},
                "calling_convention": "simple"}})
        write_local_file(xxx_path, udfs_impl_path)

        expected_exp = [{"a": i, "s": i * i} for i in xrange(1, 10)]
        actual = select_rows(query)
        assert_items_equal(actual, expected_exp)

        move(xxx_path, xxx_path + ".bak")
        create("file", xxx_path,
            attributes = { "function_descriptor": {
                "name": "sum_udf",
                "argument_types": [{
                    "tag": "concrete_type",
                    "value": "int64"}],
                "repeated_argument_type": {
                    "tag": "concrete_type",
                    "value": "int64"},
                "result_type": {
                    "tag": "concrete_type",
                    "value": "int64"},
                "calling_convention": "unversioned_value"}})
        write_local_file(xxx_path, udfs_impl_path)

        # Still use cache
        actual = select_rows(query)
        assert_items_equal(actual, expected_exp)

        time.sleep(5)

        expected_sum = [{"a": i, "s": i + 2} for i in xrange(1, 10)]
        actual = select_rows(query)
        assert_items_equal(actual, expected_sum)

    @authors("savrus", "lbrown")
    def test_aggregate_string_capture(self):
        sync_create_cells(1)

        create("table", "//tmp/t",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "a", "type": "string", "sort_order": "ascending"},
                    {"name": "b", "type": "int64"}
                ]
            })

        sync_mount_table("//tmp/t")

        # Need at least 1024 items to ensure a second batch in the scan operator
        data = [
            {"a": "A" + str(j) + "BCD"}
            for j in xrange(1, 2048)]
        insert_rows("//tmp/t", data)

        expected = [{"m": "a1000bcd"}]
        actual = select_rows("min(lower(a)) as m from [//tmp/t] group by 1")
        assert_items_equal(actual, expected)

    @authors("lbrown")
    def test_cardinality(self):
        sync_create_cells(1)

        create("table", "//tmp/card",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64"}
                ]
            })

        pivots = [[i*1000] for i in xrange(0,20)]
        pivots.insert(0, [])
        reshard_table("//tmp/card", pivots)

        sync_mount_table("//tmp/card")

        data = [{"a" : i} for i in xrange(0,20000)]
        insert_rows("//tmp/card", data)
        insert_rows("//tmp/card", data)
        insert_rows("//tmp/card", data)
        insert_rows("//tmp/card", data)

        actual = select_rows("cardinality(a) as b from [//tmp/card] group by a % 2 as k with totals")
        assert actual[0]["b"] > .95 * 10000
        assert actual[0]["b"] < 1.05 * 10000
        assert actual[1]["b"] > .95 * 10000
        assert actual[1]["b"] < 1.05 * 10000
        assert actual[2]["b"] > 1.95 * 10000
        assert actual[2]["b"] < 2.05 * 10000

    @authors("babenko")
    def test_yt_2375(self):
        sync_create_cells(1)
        create("table", "//tmp/t",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "int64"}
                ]
            })
        reshard_table("//tmp/t", [[]] + [[i] for i in xrange(1, 1000, 10)])
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": i, "value": 10 * i} for i in xrange(0, 1000)])
        # should not raise
        select_rows("sleep(value) from [//tmp/t]", output_row_limit=1, fail_on_incomplete_result=False)

    @authors("savrus")
    def test_null(self):
        sync_create_cells(1)

        create("table", "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64"}]
            })

        sync_mount_table("//tmp/t")

        data = [{"a" : None, "b" : 0}, {"a" : 1, "b" : 1}]
        insert_rows("//tmp/t", data)

        expected = data[0:1]
        actual = select_rows("* from [//tmp/t] where a = null")
        assert actual == expected

    @authors("ifsmirnov")
    def test_nan(self):
        sync_create_cells(1)

        create("table", "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "double", "sort_order": "ascending"},
                    {"name": "b", "type": "double"}]
            })

        sync_mount_table("//tmp/t")

        nan = float("nan")
        str_nan = "(1.0 / 0 - 1.0 / 0)"

        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"a": nan, "b": 1.0}])
        data = [{"a" : 1.0, "b" : nan}, {"a" : 2.0, "b" : 2.0}, {"a": 3.0}]
        insert_rows("//tmp/t", data)

        def _isnan(x):
            return isinstance(x, float) and isnan(x)

        # Comparison that respects NaN == NaN and YsonEntity == nothing.
        def _compare(lhs, rhs):
            if (isinstance(lhs, list) or isinstance(lhs, tuple)):
                return len(lhs) == len(rhs) and all(_compare(x, y) for x, y in zip(lhs, rhs))
            elif isinstance(lhs, dict):
                for key in __builtin__.set(lhs.keys()).union(rhs.keys()):
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

        assert all(
            _isnan(x.values()[0])
            for x
            in select_rows("if(true, {}, 1) from [//tmp/t]".format(str_nan)))
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

        assert select_rows("is_nan({}) from [//tmp/t]".format(str_nan))[0].values()[0] == True
        assert select_rows("is_nan({}) from [//tmp/t]".format("123"))[0].values()[0] == False
        assert select_rows("is_nan({}) from [//tmp/t]".format("#"))[0].values()[0] == False

    @authors("lukyan")
    def test_bad_limits(self):
        sync_create_cells(1)

        create("table", "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for" : "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64", "sort_order": "ascending"},
                    {"name" : "c", "type" : "int64", "sort_order" : "ascending"},
                    {"name" : "x", "type" : "string"}]
            })

        pivots = [[i*5] for i in xrange(0,20)]
        pivots.insert(0, [])
        reshard_table("//tmp/t", pivots)

        sync_mount_table("//tmp/t")

        data = [{"a" : i, "b" : i, "c" : i, "x" : str(i)} for i in xrange(0,100)]
        insert_rows("//tmp/t", data)

        select_rows("x from [//tmp/t] where (a = 18 and b = 10 and c >= 70) or (a = 18 and b >= 10) or (a >= 18)")

    @authors("lukyan")
    def test_multi_between(self):
        sync_create_cells(1)

        create("table", "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64", "sort_order": "ascending"},
                    {"name": "c", "type": "int64"}]
            })

        sync_mount_table("//tmp/t")

        data = [{"a" : i / 10, "b" : i % 10, "c" : i} for i in xrange(0,100)]
        insert_rows("//tmp/t", data)

        expected = data[10:13] + data[23:25] + data[35:40] + data[40:60]

        actual = select_rows('''
        * from [//tmp/t] where
            (a, b) between (
                (1) and (1, 2),
                (2, 3) and (2, 4),
                (3, 5) and (3),
                4 and 5
            )
        ''')
        assert actual == expected

    @authors("lukyan")
    def test_offset(self):
        sync_create_cells(1)

        create("table", "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "int64"}]
            })

        sync_mount_table("//tmp/t")

        data = [{"a" : i, "b" : i} for i in xrange(0,11)]
        insert_rows("//tmp/t", data)

        expected = data[8:9]

        actual = select_rows('''* from [//tmp/t] offset 8 limit 1''')
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
                    {"name": "b", "type": "any"}]
            })
        sync_mount_table("//tmp/t")

        data = [{"a": i, "b": {"x": i}} for i in xrange(0, 11)]
        insert_rows("//tmp/t", data)

        expected = [{"a": 7, "b_str": "{\"x\"=7;}"}]
        actual = select_rows(r'a, any_to_yson_string(b) as b_str from [//tmp/t] where a = 7')
        assert expected == actual

        length = 100000
        long_binary_string = "\xFF" * length
        escaped_string = r"\xFF" * length
        long_yson_rows = [{"a": 13, "b": {"x": long_binary_string}}]
        expected = [{"a": 13, "b_str": "{\"x\"=\"" + escaped_string + "\";}"}]
        insert_rows("//tmp/t", long_yson_rows)
        actual = select_rows(r'a, any_to_yson_string(b) as b_str from [//tmp/t] where a = 13')
        assert expected == actual


class TestQueryRpcProxy(TestQuery):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
