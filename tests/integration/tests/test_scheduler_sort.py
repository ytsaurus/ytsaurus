import pytest

from random import shuffle
from yt_env_setup import YTEnvSetup
from yt_commands import *


##################################################################

class TestSchedulerSortCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    def test_simple(self):
        v1 = {"key" : "aaa"}
        v2 = {"key" : "bb"}
        v3 = {"key" : "bbxx"}
        v4 = {"key" : "zfoo"}
        v5 = {"key" : "zzz"}

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [v3, v5, v1, v2, v4]) # some random order

        create("table", "//tmp/t_out")

        sort(in_="//tmp/t_in",
             out="//tmp/t_out",
             sort_by="key")

        assert read_table("//tmp/t_out") == [v1, v2, v3, v4, v5]
        assert get("//tmp/t_out/@sorted") ==  True
        assert get("//tmp/t_out/@sorted_by") ==  ["key"]

    def test_large_values(self):
        a = "".join(["a"] * 10 * 1024)
        b = "".join(["b"] * 100 * 1024)
        v1 = {"key" : b, "subkey" : b}
        v2 = {"key" : a, "subkey" : a}

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", v1)
        write_table("<append=true>//tmp/t_in", v2)

        create("table", "//tmp/t_out")

        sort(in_="//tmp/t_in",
             out="//tmp/t_out",
             sort_by=["key", "subkey"])

        assert read_table("//tmp/t_out") == [v2, v1]
        assert get("//tmp/t_out/@sorted") ==  True
        assert get("//tmp/t_out/@sorted_by") ==  ["key", "subkey"]

    # the same as test_simple but within transaction
    def test_simple_transacted(self):
        tx = start_transaction()

        v1 = {"key" : "aaa"}
        v2 = {"key" : "bb"}
        v3 = {"key" : "bbxx"}
        v4 = {"key" : "zfoo"}
        v5 = {"key" : "zzz"}

        create("table", "//tmp/t_in", tx=tx)
        write_table("//tmp/t_in", [v3, v5, v1, v2, v4], tx=tx) # some random order

        create("table", "//tmp/t_out", tx=tx)

        sort(in_="//tmp/t_in",
             out="//tmp/t_out",
             sort_by="key",
             tx=tx)

        commit_transaction(tx)

        assert read_table("//tmp/t_out") == [v1, v2, v3, v4, v5]
        assert get("//tmp/t_out/@sorted") ==  True
        assert get("//tmp/t_out/@sorted_by") ==  ["key"]

    def test_empty_columns(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", {"foo": "bar"})

        with pytest.raises(YtError):
            sort(in_="//tmp/t_in",
                 out="//tmp/t_out",
                 sort_by=[])

    def test_empty_in(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        sort(in_="//tmp/t_in",
             out="//tmp/t_out",
             sort_by="key")

        assert read_table("//tmp/t_out") == []
        assert get("//tmp/t_out/@sorted")

    def test_non_empty_out(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", {"foo": "bar"})
        write_table("//tmp/t_out", {"hello": "world"})

        with pytest.raises(YtError):
            sort(in_="//tmp/t_in",
             out="<append=true>//tmp/t_out",
             sort_by="foo")

    def test_maniac(self):
        v1 = {"key" : "aaa"}
        v2 = {"key" : "bb"}
        v3 = {"key" : "bbxx"}
        v4 = {"key" : "zfoo"}
        v5 = {"key" : "zzz"}

        create("table", "//tmp/t_in")
        for i in xrange(0, 10):
            write_table("<append=true>//tmp/t_in", [v3, v5, v1, v2, v4]) # some random order

        create("table", "//tmp/t_out")

        sort(in_="//tmp/t_in",
             out="//tmp/t_out",
             sort_by="missing_key",
             spec={"partition_count": 5,
                   "partition_job_count": 2,
                   "data_size_per_sort_job": 1})

        assert len(read_table("//tmp/t_out")) == 50

    def test_many_merge(self):
        v1 = {"key" : "aaa"}
        v2 = {"key" : "bb"}
        v3 = {"key" : "bbxx"}
        v4 = {"key" : "zfoo"}
        v5 = {"key" : "zzz"}

        create("table", "//tmp/t_in")
        for i in xrange(0, 10):
            row = [v1, v2, v3, v4, v5]
            shuffle(row)
            write_table("<append=true>//tmp/t_in", row) # some random order

        create("table", "//tmp/t_out")

        sort(in_="//tmp/t_in",
             out="//tmp/t_out",
             sort_by="key",
             spec={"partition_count": 5,
                   "partition_job_count": 2,
                   "data_size_per_sort_job": 1})

        assert len(read_table("//tmp/t_out")) == 50

    def test_with_intermediate_account(self):
        v1 = {"key" : "aaa"}
        v2 = {"key" : "bb"}
        v3 = {"key" : "bbxx"}
        v4 = {"key" : "zfoo"}
        v5 = {"key" : "zzz"}

        create("table", "//tmp/t_in")
        for i in xrange(0, 10):
            write_table("<append=true>//tmp/t_in", [v3, v5, v1, v2, v4]) # some random order

        create("table", "//tmp/t_out")

        with pytest.raises(YtError):
            sort(in_="//tmp/t_in",
                 out="//tmp/t_out",
                 sort_by="missing_key",
                 spec={"partition_count": 5,
                       "partition_job_count": 2,
                       "data_size_per_sort_job": 1,
                       "intermediate_data_account": "non_existing"})

    def test_composite_key(self):
        v1 = {"key": -7, "subkey": "bar", "value": "v1"}
        v2 = {"key": -7, "subkey": "foo", "value": "v2"}
        v3 = {"key": 12, "subkey": "a", "value": "v3"}
        v4 = {"key": 12, "subkey": "z", "value": "v4"}
        v5 = {"key": 500, "subkey": "foo", "value": "v5"}

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [v2, v5, v1, v4, v3]) # some random order

        create("table", "//tmp/t_out")

        sort(in_="//tmp/t_in",
             out="//tmp/t_out",
             sort_by=["key", "subkey"])

        assert read_table("//tmp/t_out") == [v1, v2, v3, v4, v5]

        create("table", "//tmp/t_another_out")
        sort(in_="//tmp/t_out",
             out="//tmp/t_another_out",
             sort_by=["subkey", "key"])

        assert read_table("//tmp/t_another_out") == [v3, v1, v2, v5, v4]

    def test_many_inputs(self):
        v1 = {"key": -7, "value": "v1"}
        v2 = {"key": -3, "value": "v2"}
        v3 = {"key": 0, "value": "v3"}
        v4 = {"key": 12, "value": "v4"}
        v5 = {"key": 500, "value": "v5"}
        v6 = {"key": 100500, "value": "v6"}

        create("table", "//tmp/in1")
        create("table", "//tmp/in2")

        write_table("//tmp/in1", [v5, v1, v4]) # some random order
        write_table("//tmp/in2", [v3, v6, v2]) # some random order

        create("table", "//tmp/t_out")
        sort(in_=["//tmp/in1", "//tmp/in2"],
             out="//tmp/t_out",
             sort_by="key")

        assert read_table("//tmp/t_out") == [v1, v2, v3, v4, v5, v6]

    def sort_with_options(self, **kwargs):
        input = "//tmp/in"
        output = "//tmp/out"
        create("table", input)
        create("table", output)
        for i in xrange(20, 0, -1):
            write_table("<append=true>" + input, [{"key": i, "value" : [1, 2]}])

        args = {"in_": [input], "out" : output, "sort_by" : "key"}
        args.update(kwargs)

        sort(**args)
        assert get("//tmp/out/@sorted")
        assert read_table(output + '{key}') == [{"key": i} for i in xrange(1, 21)]

    def test_one_partition_no_merge(self):
        self.sort_with_options()

    def test_one_partition_with_merge(self):
        self.sort_with_options(spec={"data_size_per_sort_job": 1})

    def test_two_partitions_no_merge(self):
        self.sort_with_options(spec={"partition_count": 2})

    def test_ten_partitions_no_merge(self):
        self.sort_with_options(spec={"partition_count": 10})

    def test_two_partitions_with_merge(self):
        self.sort_with_options(spec={"partition_count": 2, "partition_data_size": 1, "data_size_per_sort_job": 1})

    def test_inplace_sort(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"key" : "b"}, {"key" : "a"}])

        sort(in_="//tmp/t",
             out="//tmp/t",
             sort_by="key")

        assert read_table("//tmp/t") == [{"key" : "a"}, {"key" : "b"}]

##################################################################

class TestSchedulerSortCommandsMulticell(TestSchedulerSortCommands):
    NUM_SECONDARY_MASTER_CELLS = 2
