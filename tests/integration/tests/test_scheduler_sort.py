import pytest

from random import shuffle
from yt_env_setup import YTEnvSetup, make_schema
from yt_commands import *


##################################################################

class TestSchedulerSortCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler" : {
            "sort_operation_options" : {
                "min_uncompressed_block_size" : 1,
                "min_partition_size" : 1
            }
        }
    }

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
                   "data_size_per_sort_job": 1,
                   "partition_job_io" : {"table_writer" :
                        {"desired_chunk_size" : 1, "block_size" : 1024}}})

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

        create_user("test_user")
        create_account("test_account")

        sort(in_="//tmp/t_in",
             out="//tmp/t_out",
             sort_by="key",
             user="test_user")

        with pytest.raises(YtError):
            sort(in_="//tmp/t_in",
                 out="//tmp/t_out",
                 sort_by="key",
                 spec={"partition_count": 5,
                       "partition_job_count": 2,
                       "data_size_per_sort_job": 1,
                       "intermediate_data_account": "non_existing"})

        with pytest.raises(YtError):
            sort(in_="//tmp/t_in",
                 out="//tmp/t_out",
                 sort_by="missing_key",
                 spec={"intermediate_data_account": "test_account"},
                 user="test_user")

        set("//sys/accounts/test_account/@acl", [{"action": "allow", "permissions": ["use"], "subjects": ["test_user"]}])

        sort(in_="//tmp/t_in",
             out="//tmp/t_out",
             sort_by="key",
             user="test_user")

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

    def sort_with_options(self, optimize_for, **kwargs):
        input = "//tmp/in"
        output = "//tmp/out"
        create("table", input, attributes={"optimize_for" : optimize_for})
        create("table", output)
        for i in xrange(20, 0, -1):
            write_table("<append=true>" + input, [{"key": i, "value" : [1, 2]}])

        args = {"in_": [input], "out" : output, "sort_by" : "key"}
        args.update(kwargs)

        sort(**args)
        assert get("//tmp/out/@sorted")
        assert read_table(output + '{key}') == [{"key": i} for i in xrange(1, 21)]

    def test_one_partition_no_merge(self):
        self.sort_with_options('lookup')

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_one_partition_with_merge(self, optimize_for):
        self.sort_with_options(optimize_for, spec={"data_size_per_sort_job": 1})

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_two_partitions_no_merge(self, optimize_for):
        self.sort_with_options(optimize_for, spec={"partition_count": 2})

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_ten_partitions_no_merge(self, optimize_for):
        self.sort_with_options(optimize_for, spec={"partition_count": 10})

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_two_partitions_with_merge(self, optimize_for):
        self.sort_with_options(optimize_for, spec={"partition_count": 2, "partition_data_size": 1, "data_size_per_sort_job": 1})

    def test_inplace_sort(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"key" : "b"}, {"key" : "a"}])

        sort(in_="//tmp/t",
             out="//tmp/t",
             sort_by="key")

        assert get("//tmp/t/@sorted")
        assert read_table("//tmp/t") == [{"key" : "a"}, {"key" : "b"}]

    def test_inplace_sort_with_schema(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "key", "type": "string"}]})
        write_table("//tmp/t", [{"key" : "b"}, {"key" : "a"}])

        sort(in_="//tmp/t",
             out="//tmp/t",
             sort_by="key")

        assert get("//tmp/t/@sorted")
        assert get("//tmp/t/@schema") == make_schema([{"name": "key", "type": "string", "sort_order": "ascending"}], strict = True, unique_keys = False)
        assert read_table("//tmp/t") == [{"key" : "a"}, {"key" : "b"}]

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_auto_schema_inference(self, optimize_for):
        loose_schema = make_schema([{"name" : "key", "type" : "int64"}], strict=False)
        strict_schema = make_schema([{"name" : "key", "type" : "int64"}])

        create("table", "//tmp/input_loose", attributes={"schema" : loose_schema})
        create("table", "//tmp/input_weak")
        create("table", "//tmp/output_weak", attributes={"optimize_for" : optimize_for})
        create("table", "//tmp/output_loose",
            attributes={"optimize_for" : optimize_for, "schema" : loose_schema})
        create("table", "//tmp/output_strict",
            attributes={"optimize_for" : optimize_for, "schema" : strict_schema})

        write_table("<append=true>//tmp/input_loose", {"key": 1, "value": "foo"})
        write_table("<append=true>//tmp/input_weak", {"key": 1, "value": "foo"})

        # input weak
        sort(in_="//tmp/input_weak",
            out="//tmp/output_loose",
            sort_by="key")

        assert get("//tmp/output_loose/@schema_mode") == "strong"
        assert get("//tmp/output_loose/@sorted")

        sort(in_="//tmp/input_weak",
            out="//tmp/output_weak",
            sort_by="key")

        assert get("//tmp/output_weak/@schema_mode") == "weak"
        assert get("//tmp/output_weak/@sorted")

        with pytest.raises(YtError):
            sort(in_="//tmp/input_weak",
                out="//tmp/output_strict",
                sort_by="key")

        # input loose
        sort(in_="//tmp/input_loose",
            out="//tmp/output_loose",
            sort_by="key")

        assert get("//tmp/output_loose/@schema_mode") == "strong"
        assert get("//tmp/output_loose/@sorted")

        sort(in_="//tmp/input_loose",
            out="//tmp/output_weak",
            sort_by="key")

        assert get("//tmp/output_weak/@schema_mode") == "strong"
        assert get("//tmp/output_weak/@sorted")

        with pytest.raises(YtError):
            sort(in_="//tmp/input_loose",
                out="//tmp/output_strict",
                sort_by="key")

    def test_unique_keys_inference(self):
        schema_in = make_schema([
                {"name": "key1", "type": "string", "sort_order": "ascending"},
                {"name": "key2", "type": "string", "sort_order": "ascending"}],
            strict = True,
            unique_keys = True)

        schema_out = make_schema([
                {"name": "key2", "type": "string", "sort_order": "ascending"},
                {"name": "key1", "type": "string", "sort_order": "ascending"}],
            strict = True,
            unique_keys = True)

        create("table", "//tmp/t_in", attributes={"schema": schema_in})
        create("table", "//tmp/t_out")
        write_table("//tmp/t_in", [{"key1" : "a", "key2": "b"}, {"key1" : "b", "key2": "a"}])

        for out_table in ["//tmp/t_out", "//tmp/t_in"]:
            sort(in_="//tmp/t_in",
                 out=out_table,
                 sort_by=["key2", "key1"])

            assert get(out_table + "/@sorted")
            assert get(out_table + "/@schema") == schema_out
            assert read_table(out_table) == [{"key1" : "b", "key2": "a"}, {"key1" : "a", "key2": "b"}]

    def test_schema_validation(self):
        create("table", "//tmp/input")
        create("table", "//tmp/output", attributes={"schema":
            make_schema([
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}])
            })

        for i in xrange(10, 0, -2):
            write_table("<append=true>//tmp/input", [{"key": i, "value": "foo"}, {"key": i-1, "value": "foo"}])

        sort(in_="//tmp/input",
            out="//tmp/output",
            sort_by="key",
            spec={"schema_inference_mode" : "from_output"})

        assert get("//tmp/output/@schema_mode") == "strong"
        assert get("//tmp/output/@schema/@strict")
        assert read_table("//tmp/output") == [{"key": i, "value": "foo"} for i in xrange(1,11)]

        write_table("<sorted_by=[key]>//tmp/input", {"key": "1", "value": "foo"})
        assert get("//tmp/input/@sorted_by") == ["key"]

        with pytest.raises(YtError):
            sort(in_="//tmp/input",
                out="//tmp/output",
                sort_by="key")

    def test_unique_keys_validation(self):
        create("table", "//tmp/input")
        create("table", "//tmp/output", attributes={"schema":
            make_schema([
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"}],
                unique_keys=True)
            })

        for i in xrange(10, 0, -2):
            write_table("<append=true>//tmp/input", [{"key": i, "value": "foo"}, {"key": i-1, "value": "foo"}])

        sort(in_="//tmp/input",
            out="//tmp/output",
            sort_by="key",
            spec={"schema_inference_mode" : "from_output"})

        assert get("//tmp/output/@schema/@strict")
        assert get("//tmp/output/@schema/@unique_keys")
        assert read_table("//tmp/output") == [{"key": i, "value": "foo"} for i in xrange(1,11)]

        write_table("<sorted_by=[key]>//tmp/input", [{"key": 1, "value": "foo"} for i in range(2)])

        with pytest.raises(YtError):
            sort(in_="//tmp/input",
                out="//tmp/output",
                sort_by="key",
                spec={"schema_inference_mode" : "from_output"})

        erase("//tmp/input")

        for i in xrange(2):
            write_table("<append=%true; sorted_by=[key]>//tmp/input", {"key": 1, "value": "foo"})

        with pytest.raises(YtError):
            sort(in_="//tmp/input",
                out="//tmp/output",
                sort_by="key",
                spec={"schema_inference_mode" : "from_output"})

##################################################################

class TestSchedulerSortCommandsMulticell(TestSchedulerSortCommands):
    NUM_SECONDARY_MASTER_CELLS = 2
