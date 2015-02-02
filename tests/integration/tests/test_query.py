import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

from time import sleep

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
            result = select("a, b from [{0}]".format(path), verbose=False)

            assert len(result) == 10 * i

    def test_project1(self):
        self._sample_data(path="//tmp/p1")
        expected = [{"s": 2 * i + 10 * i - 1} for i in xrange(1, 10)]
        actual = select("2 * a + b - 1 as s from [//tmp/p1]")
        assert expected == actual

    def test_group_by1(self):
        self._sample_data(path="//tmp/g1")
        expected = [{"s": 450}]
        actual = select("sum(b) as s from [//tmp/g1] group by 1 as k")
        self.assertItemsEqual(expected, actual)

    def test_group_by2(self):
        self._sample_data(path="//tmp/g2")
        expected = [{"k": 0, "s": 200}, {"k": 1, "s": 250}]
        actual = select("k, sum(b) as s from [//tmp/g2] group by a % 2 as k")
        self.assertItemsEqual(expected, actual)

    def test_limit(self):
        self._sample_data(path="//tmp/l1")
        expected = [{"a": 1, "b": 10}]
        actual = select("* from [//tmp/l1] limit 1")
        assert expected == actual

    def test_types(self):
        create("table", "//tmp/t")

        format = yson.loads("<boolean_as_string=false;format=text>yson")
        write("//tmp/t", '{a=10;b=%false;c="hello";d=32u};{a=20;b=%true;c="world";d=64u};', input_format=format, is_raw=True)

        sort(in_="//tmp/t", out="//tmp/t", sort_by=["a", "b", "c", "d"])
        set("//tmp/t/@schema", [
            {"name": "a", "type": "int64"},
            {"name": "b", "type": "boolean"},
            {"name": "c", "type": "string"},
            {"name": "d", "type": "uint64"},
        ])

        assert select('a, b, c, d from [//tmp/t] where c="hello"', output_format=format) == \
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
            insert("//tmp/tt", data)

        unmount_table("//tmp/tt")
        self._wait_for_tablet_state("//tmp/tt", ["unmounted"])
        reshard_table("//tmp/tt", [[], [10], [30], [50], [70], [90]])
        mount_table("//tmp/tt", first_tablet_index=0, last_tablet_index=2)
        self._wait_for_tablet_state("//tmp/tt", ["unmounted", "mounted"])

        select("* from [//tmp/tt] where key < 50")

        with pytest.raises(YtError): select("* from [//tmp/tt] where key < 51")
