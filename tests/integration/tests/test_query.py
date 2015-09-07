import pytest

import os

from yt_env_setup import YTEnvSetup
from yt_commands import *

from time import sleep
from random import randint
from random import shuffle
from distutils.spawn import find_executable

##################################################################

@pytest.mark.skipif('os.environ.get("BUILD_ENABLE_LLVM", None) == "NO"')
class TestQuery(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    def _wait(self, predicate):
        while not predicate():
            sleep(1)

    def _sync_create_cells(self, size, count):
        ids = []
        for _ in xrange(count):
            ids.append(create_tablet_cell(size))

        print "Waiting for tablet cells to become healthy..."
        self._wait(lambda: all(get("//sys/tablet_cells/" + id + "/@health") == "good" for id in ids))

    def _wait_for_tablet_state(self, path, states):
        print "Waiting for tablets to become %s..." % ", ".join(str(state) for state in states)
        self._wait(lambda: all(any(x["state"] == state for state in states) for x in get(path + "/@tablets")))

    def _sample_data(self, path="//tmp/t", chunks=3, stripe=3):
        create("table", path,
            attributes = {
                "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "int64"}]
            })

        for i in xrange(chunks):
            data = [
                {"a": (i * stripe + j), "b": (i * stripe + j) * 10}
                for j in xrange(1, 1 + stripe)]
            write("<append=true>" + path, data)

        sort(in_=path, out=path, sort_by=["a", "b"])

    def _create_table(self, path, schema, key_columns, data):
        create("table", path,
            attributes = {
                "schema": schema,
                "key_columns": key_columns
            })
        mount_table(path)
        self._wait_for_tablet_state(path, ["mounted"])
        insert_rows(path, data)

    # TODO(sandello): TableMountCache is not invalidated at the moment,
    # so table names must be unique.

    def test_simple(self):
        for i in xrange(0, 50, 10):
            path = "//tmp/t{0}".format(i)

            self._sample_data(path=path, chunks=i, stripe=10)
            result = select_rows("a, b from [{0}]".format(path), verbose=False)

            assert len(result) == 10 * i

    def test_invalid_data(self):
        path = "//tmp/t"
        create("table", path,
            attributes = {
                "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "int64"}]
            })
        data = [{"a" : 1, "b" : 2}, 
                {"a" : 1, "b" : 2.2}, 
                {"a" : 1, "b" : "smth"}]

        write("<sorted_by=[a; b]>" + path, data)
        with pytest.raises(YtError):
            result = select_rows("a, b from [{0}] where b=b".format(path), verbose=False)
            print result

    def test_project1(self):
        self._sample_data(path="//tmp/p1")
        expected = [{"s": 2 * i + 10 * i - 1} for i in xrange(1, 10)]
        actual = select_rows("2 * a + b - 1 as s from [//tmp/p1]")
        assert expected == actual

    def test_group_by1(self):
        self._sample_data(path="//tmp/g1")
        expected = [{"s": 450}]
        actual = select_rows("sum(b) as s from [//tmp/g1] group by 1 as k")
        self.assertItemsEqual(actual, expected)

    def test_group_by2(self):
        self._sample_data(path="//tmp/g2")
        expected = [{"k": 0, "s": 200}, {"k": 1, "s": 250}]
        actual = select_rows("k, sum(b) as s from [//tmp/g2] group by a % 2 as k")
        self.assertItemsEqual(actual, expected)

    def test_limit(self):
        self._sample_data(path="//tmp/l1")
        expected = [{"a": 1, "b": 10}]
        actual = select_rows("* from [//tmp/l1] limit 1")
        assert expected == actual

    def test_order_by(self):
        self._sync_create_cells(3, 1)

        create("table", "//tmp/o1",
            attributes = {
                "schema": [
                    {"name": "k", "type": "int64"},
                    {"name": "u", "type": "int64"},
                    {"name": "v", "type": "int64"}],
                "key_columns": ["k"]
            })

        mount_table("//tmp/o1")
        self._wait_for_tablet_state("//tmp/o1", ["mounted"])

        values = [i for i in xrange(0, 300)]
        shuffle(values)

        data = [
            {"k": i, "v": values[i], "u": randint(0, 1000)}
            for i in xrange(0, 100)]
        insert_rows("//tmp/o1", data)

        expected = [dict([(col, v) for col, v in row.iteritems() if col in ['k', 'v']]) for row in data if row['u'] > 500]
        expected = sorted(expected, cmp=lambda x, y: x['v'] - y['v'])[0:10]

        actual = select_rows("k, v from [//tmp/o1] where u > 500 order by v limit 10")
        assert expected == actual

    def test_join(self):
        self._sync_create_cells(3, 1)

        self._create_table(
            "//tmp/jl",
            [
                {"name": "a", "type": "int64"},
                {"name": "b", "type": "int64"},
                {"name": "c", "type": "int64"}],
            ["a", "b"],
            [
                {"a": 1, "b": 2, "c": 80 },
                {"a": 1, "b": 3, "c": 71 },
                {"a": 1, "b": 4, "c": 62 },
                {"a": 2, "b": 1, "c": 53 },
                {"a": 2, "b": 2, "c": 44 },
                {"a": 2, "b": 3, "c": 35 },
                {"a": 2, "b": 4, "c": 26 },
                {"a": 3, "b": 1, "c": 17 }
            ]);

        self._create_table(
            "//tmp/jr",
            [
                {"name": "c", "type": "int64"},
                {"name": "d", "type": "int64"},
                {"name": "e", "type": "int64"}],
            ["c"],
            [
                {"d": 1, "e": 2, "c": 80 },
                {"d": 1, "e": 3, "c": 71 },
                {"d": 1, "e": 4, "c": 62 },
                {"d": 2, "e": 1, "c": 53 },
                {"d": 2, "e": 2, "c": 44 },
                {"d": 2, "e": 3, "c": 35 },
                {"d": 2, "e": 4, "c": 26 },
                {"d": 3, "e": 1, "c": 17 }
            ]);

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
        assert expected == actual

        expected = [
            {"a": 2, "b": 1, "c": 53, "d": 2, "e": 1}]

        actual = select_rows("* from [//tmp/jl] join [//tmp/jr] using c where (a, b) IN ((2, 1))")
        assert expected == actual

    def test_types(self):
        create("table", "//tmp/t")

        format = yson.loads("<boolean_as_string=false;format=text>yson")
        write(
            "//tmp/t",
            '{a=10;b=%false;c="hello";d=32u};{a=20;b=%true;c="world";d=64u};',
            input_format=format,
            is_raw=True)

        sort(in_="//tmp/t", out="//tmp/t", sort_by=["a", "b", "c", "d"])
        set("//tmp/t/@schema", [
            {"name": "a", "type": "int64"},
            {"name": "b", "type": "boolean"},
            {"name": "c", "type": "string"},
            {"name": "d", "type": "uint64"},
        ])

        assert select_rows('a, b, c, d from [//tmp/t] where c="hello"', output_format=format) == \
                '{"a"=10;"b"=%false;"c"="hello";"d"=32u};\n'

    def test_tablets(self):
        self._sync_create_cells(3, 1)

        create("table", "//tmp/tt",
            attributes = {
                "schema": [{"name": "key", "type": "int64"}, {"name": "value", "type": "int64"}],
                "key_columns": ["key"]
            })

        mount_table("//tmp/tt")
        self._wait_for_tablet_state("//tmp/tt", ["mounted"])

        stripe = 10

        for i in xrange(0, 10):
            data = [
                {"key": (i * stripe + j), "value": (i * stripe + j) * 10}
                for j in xrange(1, 1 + stripe)]
            insert_rows("//tmp/tt", data)

        unmount_table("//tmp/tt")
        self._wait_for_tablet_state("//tmp/tt", ["unmounted"])
        reshard_table("//tmp/tt", [[], [10], [30], [50], [70], [90]])
        mount_table("//tmp/tt", first_tablet_index=0, last_tablet_index=2)
        self._wait_for_tablet_state("//tmp/tt", ["unmounted", "mounted"])

        select_rows("* from [//tmp/tt] where key < 50")

        with pytest.raises(YtError): select_rows("* from [//tmp/tt] where key < 51")

    def test_computed_column_simple(self):
        self._sync_create_cells(3, 1)

        create("table", "//tmp/tc",
            attributes = {
                "schema": [
                    {"name": "hash", "type": "int64", "expression": "key * 33"},
                    {"name": "key", "type": "int64"},
                    {"name": "value", "type": "int64"}],
                "key_columns": ["hash", "key"]
            })
        reshard_table("//tmp/tc", [[]] + [[i] for i in xrange(1, 100 * 33, 1000)])
        mount_table("//tmp/tc")
        self._wait_for_tablet_state("//tmp/tc", ["mounted"])

        insert_rows("//tmp/tc", [{"key": i, "value": i * 2} for i in xrange(0,100)])

        expected = [{"hash": 42 * 33, "key": 42, "value": 42 * 2}]
        actual = select_rows("* from [//tmp/tc] where key = 42")
        self.assertItemsEqual(actual, expected)

        expected = [{"hash": i * 33, "key": i, "value": i * 2} for i in xrange(10,80)]
        actual = sorted(select_rows("* from [//tmp/tc] where key >= 10 and key < 80"))
        self.assertItemsEqual(actual, expected)

        expected = [{"hash": i * 33, "key": i, "value": i * 2} for i in [10, 20, 30]]
        actual = sorted(select_rows("* from [//tmp/tc] where key in (10, 20, 30)"))
        self.assertItemsEqual(actual, expected)

    def test_computed_column_far_divide(self):
        self._sync_create_cells(3, 1)

        create("table", "//tmp/tc",
            attributes = {
                "schema": [
                    {"name": "hash", "type": "int64", "expression": "key2 / 2"},
                    {"name": "key1", "type": "int64"},
                    {"name": "key2", "type": "int64"},
                    {"name": "value", "type": "int64"}],
                "key_columns": ["hash", "key1", "key2"]
            })
        reshard_table("//tmp/tc", [[]] + [[i] for i in xrange(1, 500, 10)])
        mount_table("//tmp/tc")
        self._wait_for_tablet_state("//tmp/tc", ["mounted"])

        def expected(key_range):
            return [{"hash": i / 2, "key1": i, "key2": i, "value": i * 2} for i in key_range]

        insert_rows("//tmp/tc", [{"key1": i, "key2": i, "value": i * 2} for i in xrange(0,1000)])

        actual = select_rows("* from [//tmp/tc] where key2 = 42")
        self.assertItemsEqual(actual, expected([42]))

        actual = sorted(select_rows("* from [//tmp/tc] where key2 >= 10 and key2 < 80"))
        self.assertItemsEqual(actual, expected(xrange(10,80)))

        actual = sorted(select_rows("* from [//tmp/tc] where key2 in (10, 20, 30)"))
        self.assertItemsEqual(actual, expected([10, 20, 30]))

        actual = sorted(select_rows("* from [//tmp/tc] where key2 in (10, 20, 30) and key1 in (30, 40)"))
        self.assertItemsEqual(actual, expected([30]))

    def test_computed_column_modulo(self):
        self._sync_create_cells(3, 1)

        create("table", "//tmp/tc",
            attributes = {
                "schema": [
                    {"name": "hash", "type": "int64", "expression": "key2 % 2"},
                    {"name": "key1", "type": "int64"},
                    {"name": "key2", "type": "int64"},
                    {"name": "value", "type": "int64"}],
                "key_columns": ["hash", "key1", "key2"]
            })
        reshard_table("//tmp/tc", [[]] + [[i] for i in xrange(1, 500, 10)])
        mount_table("//tmp/tc")
        self._wait_for_tablet_state("//tmp/tc", ["mounted"])

        def expected(key_range):
            return [{"hash": i % 2, "key1": i, "key2": i, "value": i * 2} for i in key_range]

        insert_rows("//tmp/tc", [{"key1": i, "key2": i, "value": i * 2} for i in xrange(0,1000)])

        actual = select_rows("* from [//tmp/tc] where key2 = 42")
        self.assertItemsEqual(actual, expected([42]))

        actual = sorted(select_rows("* from [//tmp/tc] where key1 >= 10 and key1 < 80"))
        self.assertItemsEqual(actual, expected(xrange(10,80)))

        actual = sorted(select_rows("* from [//tmp/tc] where key1 in (10, 20, 30)"))
        self.assertItemsEqual(actual, expected([10, 20, 30]))

        actual = sorted(select_rows("* from [//tmp/tc] where key1 in (10, 20, 30) and key2 in (30, 40)"))
        self.assertItemsEqual(actual, expected([30]))

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

        sum_path = os.path.join(registry_path, "sum_udf")
        create("file", sum_path,
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

        local_implementation_path = find_executable("test_udfs.bc")
        upload_file(abs_path, local_implementation_path)
        upload_file(sum_path, local_implementation_path)

        self._sample_data(path="//tmp/u")
        expected = [{"s": 2 * i} for i in xrange(1, 10)]
        actual = select_rows("abs_udf(-2 * a) as s from [//tmp/u] where sum_udf(b, 1, 2) = sum_udf(3, b)")
        self.assertItemsEqual(actual, expected)

    def test_YT_2375(self):
        self._sync_create_cells(3, 3)
        create(
            "table", "//tmp/t",
            attributes={
                "schema": [{"name": "key", "type": "int64"}, {"name": "value", "type": "int64"}],
                "key_columns": ["key"],
            })
        reshard_table("//tmp/t", [[]] + [[i] for i in xrange(1, 1000, 10)])
        mount_table("//tmp/t")
        self._wait_for_tablet_state("//tmp/t", ["mounted"])

        insert_rows("//tmp/t", [{"key": i, "value": 10 * i} for i in xrange(0, 1000)])
        # should not raise
        select_rows("sleep(value) from [//tmp/t]", output_row_limit=1, fail_on_incomplete_result=False)

