import pytest

import os

from yt_env_setup import YTEnvSetup
from yt_commands import *

from time import sleep
from random import randint

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

    # TODO(sandello): TableMountCache is not invalidated at the moment,
    # so table names must be unique.

    def test_simple(self):
        for i in xrange(0, 50, 10):
            path = "//tmp/t{0}".format(i)

            self._sample_data(path=path, chunks=i, stripe=10)
            result = select_rows("a, b from [{0}]".format(path), verbose=False)

            assert len(result) == 10 * i

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
                "schema": [{"name": "key", "type": "int64"}, {"name": "value", "type": "int64"}],
                "key_columns": ["key"]
            })

        mount_table("//tmp/o1")
        self._wait_for_tablet_state("//tmp/o1", ["mounted"])

        data = [
            {"key": i, "value": randint(0, 1000)}
            for i in xrange(0, 100)]
        insert_rows("//tmp/o1", data)

        expected = data[:]
        expected = sorted(expected, cmp=lambda x, y: x['value'] - y['value'])[0:10]

        actual = select_rows("key, value from [//tmp/o1] order by value limit 10")
        assert expected == actual

    def test_join(self):

        self._sync_create_cells(3, 1)

        create("table", "//tmp/jl",
            attributes = {
                "schema": [
                    {"name": "LogID", "type": "int64"},
                    {"name": "OrderID", "type": "int64"},
                    {"name": "UpdateTime", "type": "int64"}],
                "key_columns": ["LogID", "OrderID"]
            })

        mount_table("//tmp/jl")
        self._wait_for_tablet_state("//tmp/jl", ["mounted"])

        create("table", "//tmp/jr",
            attributes = {
                "schema": [
                    {"name": "UpdateTime", "type": "int64"},
                    {"name": "LogID1", "type": "int64"},
                    {"name": "OrderID1", "type": "int64"}],
                "key_columns": ["UpdateTime"]
            })

        mount_table("//tmp/jr")
        self._wait_for_tablet_state("//tmp/jr", ["mounted"])

        data = [
            {"LogID": 1, "OrderID": 2, "UpdateTime": 0 },
            {"LogID": 1, "OrderID": 3, "UpdateTime": 1 },
            {"LogID": 1, "OrderID": 4, "UpdateTime": 2 },
            {"LogID": 2, "OrderID": 1, "UpdateTime": 3 },
            {"LogID": 2, "OrderID": 2, "UpdateTime": 4 },
            {"LogID": 2, "OrderID": 3, "UpdateTime": 5 },
            {"LogID": 2, "OrderID": 4, "UpdateTime": 6 },
            {"LogID": 3, "OrderID": 1, "UpdateTime": 7 }]

        insert_rows("//tmp/jl", data)

        data = [
            {"LogID1": 1, "OrderID1": 2, "UpdateTime": 0 },
            {"LogID1": 1, "OrderID1": 3, "UpdateTime": 1 },
            {"LogID1": 1, "OrderID1": 4, "UpdateTime": 2 },
            {"LogID1": 2, "OrderID1": 1, "UpdateTime": 3 },
            {"LogID1": 2, "OrderID1": 2, "UpdateTime": 4 },
            {"LogID1": 2, "OrderID1": 3, "UpdateTime": 5 },
            {"LogID1": 2, "OrderID1": 4, "UpdateTime": 6 },
            {"LogID1": 3, "OrderID1": 1, "UpdateTime": 7 }]

        insert_rows("//tmp/jr", data)

        expected = [
            {"LogID": 1, "OrderID": 2, "UpdateTime": 0, "LogID1": 1, "OrderID1": 2},
            {"LogID": 1, "OrderID": 3, "UpdateTime": 1, "LogID1": 1, "OrderID1": 3},
            {"LogID": 1, "OrderID": 4, "UpdateTime": 2, "LogID1": 1, "OrderID1": 4},
            {"LogID": 2, "OrderID": 1, "UpdateTime": 3, "LogID1": 2, "OrderID1": 1},
            {"LogID": 2, "OrderID": 2, "UpdateTime": 4, "LogID1": 2, "OrderID1": 2},
            {"LogID": 2, "OrderID": 3, "UpdateTime": 5, "LogID1": 2, "OrderID1": 3},
            {"LogID": 2, "OrderID": 4, "UpdateTime": 6, "LogID1": 2, "OrderID1": 4},
            {"LogID": 3, "OrderID": 1, "UpdateTime": 7, "LogID1": 3, "OrderID1": 1}]

        actual = select_rows("* from [//tmp/jl] join [//tmp/jr] using UpdateTime where LogID < 4")
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

    def test_computed_column(self):
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
        
    def test_udf(self):
        registry_path =  "//tmp/udfs"
        create("document", registry_path)

        implementation_path = "//tmp/abs_udf.bc"
        data = { "abs_udf": {
            "name": "abs_udf",
            "argument_types": [
                "int64"],
            "result_type": "int64",
            "implementation_path": implementation_path,
            "calling_convention": "simple"
        }}
        set(registry_path, data)

        local_implementation_path = os.path.join(os.path.dirname(__file__), "../../../yt/unittests/udf/test_udfs.bc")
        create("file", implementation_path)
        upload_file(implementation_path, local_implementation_path)

        self._sample_data(path="//tmp/u")
        expected = [{"s": 2 * i} for i in xrange(1, 10)]
        actual = select_rows("abs_udf(-2 * a) as s from [//tmp/u]")
        assert expected == actual
