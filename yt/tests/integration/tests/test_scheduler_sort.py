import pytest

from copy import deepcopy
from random import shuffle
from yt_env_setup import YTEnvSetup
from yt.environment.helpers import assert_items_equal
from yt_commands import *


##################################################################

class TestSchedulerSortCommands(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "sort_operation_options" : {
                "min_uncompressed_block_size" : 1,
                "min_partition_size" : 1,
            }
        }
    }

    @authors("ignat")
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

    @authors("psushin")
    def test_megalomaniac_protection(self):
        v1 = {"key" : "aaa"}
        v2 = {"key" : "bb"}
        v3 = {"key" : "bbxx"}
        v4 = {"key" : "zfoo"}
        v5 = {"key" : "zzz"}

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [v3, v5, v1, v2, v4]) # some random order

        create("table", "//tmp/t_out")

        with pytest.raises(YtError):
            sort(in_="//tmp/t_in",
                 out="//tmp/t_out",
                 sort_by="key",
                 spec={"max_input_data_weight" : 5})

        with pytest.raises(YtError):
            sort(in_="//tmp/t_in",
                 out="//tmp/t_out",
                 sort_by="key",
                 spec={"max_shuffle_data_slice_count" : 1, 
                       "partition_job_count" : 2,
                       "partition_count" : 2})

        with pytest.raises(YtError):
            sort(in_="//tmp/t_in",
                 out="//tmp/t_out",
                 sort_by="key",
                 spec={"max_shuffle_job_count" : 1, 
                       "partition_job_count" : 2,
                       "partition_count" : 2})

        with pytest.raises(YtError):
            sort(in_="//tmp/t_in",
                 out="//tmp/t_out",
                 sort_by="key",
                 spec={"max_merge_data_slice_count" : 1, 
                       "partition_job_count" : 2,
                       "partition_count" : 2,
                       "data_size_per_sort_job" : 3})

    @authors("max42")
    def test_sort_with_sampling(self):
        create("table", "//tmp/t_in")

        n = 1003
        write_table("//tmp/t_in", [{"a": (42 * x) % n} for x in range(n)])

        create("table", "//tmp/t_out")

        op = sort(in_="//tmp/t_in",
                  out="//tmp/t_out",
                  sort_by="a",
                  spec={
                      "partition_job_io": {"table_reader": {"sampling_rate": 0.5}},
                      "partition_count": 10,
                  })

        result = read_table("//tmp/t_out")

        assert n * 0.5 - 100 <= len(result) <= n * 0.5 + 100

    @authors("psushin")
    def test_simple_read_limits(self):
        v1 = {"key" : "aaa", "value" : "2"}
        v2 = {"key" : "bb", "value" : "5"}
        v3 = {"key" : "bbxx", "value" : "1"}
        v4 = {"key" : "zfoo", "value" : "4"}
        v5 = {"key" : "zzz", "value" : "3"}

        create("table", "//tmp/t_in")
        write_table(
            "<schema=[{name=key; type=string; sort_order=ascending}; {name=value;type=string}]>//tmp/t_in",
            [v1, v2, v3, v4, v5])

        create("table", "//tmp/t_out")

        sort(in_="<lower_limit={key=[b]}; upper_limit={key=[z]}>//tmp/t_in",
             out="//tmp/t_out",
             sort_by="value")

        assert read_table("//tmp/t_out") == [v3, v2]
        assert get("//tmp/t_out/@sorted") ==  True
        assert get("//tmp/t_out/@sorted_by") ==  ["value"]

    @authors("psushin")
    def test_key_weight_limit(self):
        v1 = {"key" : "aaa"}
        v2 = {"key" : "bb"}

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [v2, v1])

        create("table", "//tmp/t_out")

        with pytest.raises(YtError):
            sort(in_="//tmp/t_in",
                 out="//tmp/t_out",
                 sort_by="key",
                 spec={"merge_job_io" : {"table_writer" : {"max_key_weight" : 2}}})

    @authors("psushin")
    def test_foreign(self):
        v1 = {"key" : "aaa"}

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [v1])

        create("table", "//tmp/t_out")

        with pytest.raises(YtError):
            sort(in_="<foreign=true>//tmp/t_in",
                 out="//tmp/t_out",
                 sort_by="key")

    @authors("psushin")
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
             sort_by=["key", "subkey"],
             spec={"merge_job_io" : {"table_writer" : {"max_key_weight" : 250 * 1024}}})

        assert read_table("//tmp/t_out") == [v2, v1]
        assert get("//tmp/t_out/@sorted") ==  True
        assert get("//tmp/t_out/@sorted_by") ==  ["key", "subkey"]

    # the same as test_simple but within transaction
    @authors("babenko", "ignat")
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

    @authors("ignat")
    def test_empty_columns(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", {"foo": "bar"})

        with pytest.raises(YtError):
            sort(in_="//tmp/t_in",
                 out="//tmp/t_out",
                 sort_by=[])

    @authors("ignat")
    def test_empty_in(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        sort(in_="//tmp/t_in",
             out="//tmp/t_out",
             sort_by="key")

        assert read_table("//tmp/t_out") == []
        assert get("//tmp/t_out/@sorted")

    @authors("panin", "ignat")
    def test_non_empty_out(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", {"foo": "bar"})
        write_table("//tmp/t_out", {"hello": "world"})

        with pytest.raises(YtError):
            sort(in_="//tmp/t_in",
             out="<append=true>//tmp/t_out",
             sort_by="foo")

    @authors("ermolovd")
    def test_validate_schema(self):
        create("table", "//tmp/t_in", attributes={
            "schema": make_schema(
                [
                    {"name": "field", "type": "int64", "required": False},
                ]
            )
        })
        create("table", "//tmp/t_out", attributes={
            "schema": make_schema(
                [
                    {"name": "field", "type": "int64", "required": True},
                ]
            )
        })
        write_table("//tmp/t_in", [{"field": 1}])
        sort(in_="//tmp/t_in",
             out="//tmp/t_out",
             sort_by="field")

        write_table("<append=%true>//tmp/t_in", [{"field": None}])

        with pytest.raises(YtError):
            sort(in_="//tmp/t_in",
                 out="//tmp/t_out",
                 sort_by="field")

    @authors("dakovalkov")
    def test_append_simple(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out", attributes={
            "schema": make_schema([
                {"name": "key", "type": "int64", "sort_order": "ascending"}],
                unique_keys=False)
            })

        write_table("//tmp/t_in", {"key": 2})
        write_table("//tmp/t_out", {"key": 1})

        sort(in_="//tmp/t_in",
             out="<append=true>//tmp/t_out",
             sort_by="key")

        assert read_table("//tmp/t_out") == [{"key": 1}, {"key": 2}]
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") ==  ["key"]

    @authors("dakovalkov")
    def test_append_different_key_columns(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out", attributes={
            "schema": make_schema([
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "subkey", "type": "int64"}],
                unique_keys=False)
            })

        write_table("//tmp/t_in", {"key": 2, "subkey": 2})
        write_table("//tmp/t_out", {"key": 1, "subkey": 1})

        with pytest.raises(YtError):
            sort(in_="//tmp/t_in",
                 out="<append=true>//tmp/t_out",
                 sort_by=["key", "subkey"])

    @authors("dakovalkov")
    def test_append_different_key_columns_2(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out", attributes={
            "schema": make_schema([
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "subkey", "type": "int64", "sort_order": "ascending"}],
                unique_keys=False)
            })

        write_table("//tmp/t_in", {"key": 2, "subkey": 2})
        write_table("//tmp/t_out", {"key": 1, "subkey": 1})

        with pytest.raises(YtError):
            sort(in_="//tmp/t_in",
                 out="<append=true>//tmp/t_out",
                 sort_by="key")

    @authors("ignat")
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

    @authors("psushin", "ignat")
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

    @authors("max42")
    def test_several_merge_jobs_per_partition(self):
        create("table", "//tmp/t_in")
        rows = [{"key": "k%03d" % (i), "value": "v%03d" % (i)} for i in xrange(500)]
        shuffled_rows = rows[::]
        shuffle(shuffled_rows)
        write_table("//tmp/t_in", shuffled_rows)

        create("table", "//tmp/t_out")

        sort(in_="//tmp/t_in",
             out="//tmp/t_out",
             sort_by="key",
             spec={"partition_count": 2,
                   "partition_job_count": 10,
                   "data_size_per_sort_job": 1,
                   "partition_job_io" : {"table_writer" :
                        {"desired_chunk_size" : 1, "block_size" : 1024}}})

        assert read_table("//tmp/t_out") == rows
        assert get("//tmp/t_out/@chunk_count") >= 10

    @authors("dakovalkov")
    def test_sort_with_row_count_limit(self):
        create("table", "//tmp/t_in")
        rows = [{"key": "k%03d" % (i), "value": "v%03d" % (i)} for i in xrange(500)]
        shuffled_rows = rows[::]
        shuffle(shuffled_rows)

        for i in range(10):
            write_table("<append=%true>//tmp/t_in", shuffled_rows[50*i:50*(i+1)])

        create("table", "//tmp/t_out")

        sort(in_="//tmp/t_in",
             out="<row_count_limit=10>//tmp/t_out",
             sort_by="key",
             spec={"partition_count": 50,
                   "partition_job_count": 100,
                   "data_size_per_sort_job": 1})

        output_rows = read_table("//tmp/t_out")
        assert sorted(output_rows) == output_rows
        assert len(output_rows) < 250
        assert len(output_rows) >= 10

    @authors("ignat", "klyachin")
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
             authenticated_user="test_user")

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
                 authenticated_user="test_user")

        set("//sys/accounts/test_account/@acl", [make_ace("allow", "test_user", "use")])

        sort(in_="//tmp/t_in",
             out="//tmp/t_out",
             sort_by="key",
             authenticated_user="test_user")

    @authors("panin", "ignat")
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

    @authors("ignat")
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

    @authors("ignat", "babenko", "psushin")
    def test_one_partition_no_merge(self):
        self.sort_with_options('lookup')

    @authors("psushin")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_one_partition_with_merge(self, optimize_for):
        self.sort_with_options(optimize_for, spec={"data_size_per_sort_job": 1})

    @authors("psushin")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_two_partitions_no_merge(self, optimize_for):
        self.sort_with_options(optimize_for, spec={"partition_count": 2})

    @authors("psushin")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_ten_partitions_no_merge(self, optimize_for):
        self.sort_with_options(optimize_for, spec={"partition_count": 10})

    @authors("psushin")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_two_partitions_with_merge(self, optimize_for):
        self.sort_with_options(optimize_for, spec={"partition_count": 2, "partition_data_size": 1, "data_size_per_sort_job": 1})

    @authors("ignat")
    def test_inplace_sort(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"key" : "b"}, {"key" : "a"}])

        sort(in_="//tmp/t",
             out="//tmp/t",
             sort_by="key")

        assert get("//tmp/t/@sorted")
        assert read_table("//tmp/t") == [{"key" : "a"}, {"key" : "b"}]

    @authors("ignat")
    def test_inplace_sort_with_schema(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "key", "type": "string"}]})
        write_table("//tmp/t", [{"key" : "b"}, {"key" : "a"}])

        sort(in_="//tmp/t",
             out="//tmp/t",
             sort_by="key")

        assert get("//tmp/t/@sorted")
        assert normalize_schema(get("//tmp/t/@schema")) == make_schema(
            [{"name": "key", "type": "string", "required": False, "sort_order": "ascending"}],
            strict = True, unique_keys = False)
        assert read_table("//tmp/t") == [{"key" : "a"}, {"key" : "b"}]

    @authors("psushin")
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

    @authors("savrus")
    def test_unique_keys_inference(self):
        schema_in = make_schema([
                {"name": "key1", "type": "string", "sort_order": "ascending"},
                {"name": "key2", "type": "string", "sort_order": "ascending"},
                {"name": "key3", "type": "string"}],
            strict = True,
            unique_keys = True)

        create("table", "//tmp/t_in", attributes={"schema": schema_in})
        create("table", "//tmp/t_out")

        row1 = {"key1" : "a", "key2": "b", "key3": "c"}
        row2 = {"key1" : "b", "key2": "a", "key3": "d"}
        write_table("//tmp/t_in", [row1, row2])

        def _do(out_table, sort_by, unique_keys, result):
            sort(in_="//tmp/t_in",
                 out=out_table,
                 sort_by=sort_by,
                 spec={"schema_inference_mode": "from_input"})

            assert get(out_table + "/@sorted_by") == sort_by
            assert get(out_table + "/@schema/@strict")
            assert get(out_table + "/@schema/@unique_keys") == unique_keys
            assert read_table(out_table) == result

        _do("//tmp/t_out", ["key2", "key1"], True, [row2, row1])
        _do("//tmp/t_out", ["key2"], False, [row2, row1])
        _do("//tmp/t_out", ["key3", "key2", "key1"], True, [row1, row2])
        _do("//tmp/t_out", ["key3", "key1"], False, [row1, row2])
        _do("//tmp/t_in", ["key2", "key1"], True, [row2, row1])

    @authors("savrus")
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

    @authors("ermolovd")
    def test_complex_types_schema_validation(self):
        input_schema = make_schema([
            {"name": "index", "type_v3": "int64"},
            {"name": "value", "type_v3": optional_type(optional_type("string"))},
        ], unique_keys=False, strict=True)
        output_schema = make_schema([
            {"name": "index", "type_v3": "int64", "sort_order": "ascending"},
            {"name": "value", "type_v3": list_type(optional_type("string"))},
        ], unique_keys=False, strict=True)

        create("table", "//tmp/input", attributes={"schema": input_schema})
        create("table", "//tmp/output", attributes={"schema": output_schema})
        write_table("//tmp/input", [
            {"index": 1, "value": [None]},
            {"index": 2, "value": ["foo"]},
        ])

        # We check that yson representation of types are compatible with each other
        write_table("//tmp/output", read_table("//tmp/input"))

        with pytest.raises(YtError):
            sort(
                in_="//tmp/input",
                out="//tmp/output",
                sort_by="index",
                spec={"schema_inference_mode": "auto"},
            )
        sort(
            in_="//tmp/input",
            out="//tmp/output",
            sort_by="index",
            spec={"schema_inference_mode": "from_output"},
        )
        assert normalize_schema_v3(output_schema) == normalize_schema_v3(get("//tmp/output/@schema"))
        sort(
            in_="//tmp/input",
            out="//tmp/output",
            sort_by="index",
            spec={"schema_inference_mode": "from_input"},
        )
        input_sorted_schema = deepcopy(input_schema)
        input_sorted_schema[0]["sort_order"] = "ascending"
        assert normalize_schema_v3(input_sorted_schema) == normalize_schema_v3(get("//tmp/output/@schema"))

    @authors("savrus")
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

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("sort_order", [None, "ascending"])
    def test_sort_on_dynamic_table(self, sort_order, optimize_for):
        schema= [
            {"name": "key1", "type": "int64", "sort_order": sort_order},
            {"name": "key2", "type": "int64", "sort_order": sort_order},
            {"name": "value", "type": "string"}
        ]

        sync_create_cells(1)
        create_dynamic_table("//tmp/t", schema=schema, optimize_for=optimize_for)

        create("table", "//tmp/t_out")

        rows = [{"key1": None, "key2": i, "value": str(i)} for i in range(2)]
        rows += [{"key1": i, "key2": i, "value": str(i)} for i in range(6)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")

        sort(
            in_="//tmp/t",
            out="//tmp/t_out",
            sort_by=["key1", "key2"])
        assert read_table("//tmp/t_out") == rows

        rows1 = [{"key1": i, "key2": i, "value": str(i+1)} for i in range(3, 10)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows1)
        sync_unmount_table("//tmp/t")

        def update(new):
            def update_row(row):
                if sort_order == "ascending":
                    for r in rows:
                        if all(r[k] == row[k] for k in ("key1", "key2")):
                            r["value"] = row["value"]
                            return
                rows.append(row)
            for row in new:
                update_row(row)

        update(rows1)

        def verify_sort(sort_by):
            sort(
                in_="//tmp/t",
                out="//tmp/t_out",
                sort_by=sort_by)
            actual = read_table("//tmp/t_out")

            # Oh Yson
            for row in actual:
                for k in row.iterkeys():
                    if row[k] == None:
                        row[k] = None

            key = lambda r: [r[k] for k in sort_by]
            for i in xrange(1, len(actual)):
                assert key(actual[i-1]) <= key(actual[i])

            wide_by = sort_by + [c["name"] for c in schema if c["name"] not in sort_by]
            key = lambda r: [r[k] for k in wide_by]
            assert sorted(actual, key=key) == sorted(rows, key=key)

        verify_sort(["key1"])
        verify_sort(["key2"])
        verify_sort(["key2", "key1"])
        verify_sort(["key1", "key2", "value"])
        verify_sort(["value", "key2", "key1"])

    @authors("savrus", "psushin")
    def test_computed_columns(self):
        create("table", "//tmp/t",
            attributes={
                "schema": [
                    {"name": "k1", "type": "int64", "expression": "k2 * 2" },
                    {"name": "k2", "type": "int64"}]
            })

        write_table("//tmp/t", [{"k2": i} for i in xrange(2)])
        assert read_table("//tmp/t") == [{"k1": i * 2, "k2": i} for i in xrange(2)]

        sort(
            in_="//tmp/t",
            out="//tmp/t",
            sort_by="k1")

        assert normalize_schema(get("//tmp/t/@schema")) == make_schema([
            {"name": "k1", "type": "int64", "expression": "k2 * 2", "sort_order": "ascending", "required": False},
            {"name": "k2", "type": "int64", "required": False},
        ], unique_keys=False, strict=True)

        assert read_table("//tmp/t") == [{"k1": i * 2, "k2": i} for i in xrange(2)]

        create("table", "//tmp/t2")
        for i in xrange(5):
            write_table("//tmp/t2", {"k2" : i})

        with pytest.raises(YtError):
            # sort table with weak schema into table with computed column
            sort(
                in_="//tmp/t2",
                out="//tmp/t",
                sort_by="k1")

    @authors("savrus")
    def test_writer_config(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out",
            attributes={
                "chunk_writer": {"block_size": 1024},
                "compression_codec": "none"
            })

        write_table("//tmp/t_in", [{"value": "A"*1024} for i in xrange(10)])

        sort(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by="value",
            spec={"job_count": 1})

        chunk_id = get_singular_chunk_id("//tmp/t_out")
        assert get("#" + chunk_id + "/@compressed_data_size") > 1024 * 10
        assert get("#" + chunk_id + "/@max_block_size") < 1024 * 2

    @authors("savrus")
    def test_column_selectors_schema_inference(self):
        create("table", "//tmp/t", attributes={
            "schema": make_schema([
                {"name": "k1", "type": "int64", "sort_order": "ascending"},
                {"name": "k2", "type": "int64", "sort_order": "ascending"},
                {"name": "v1", "type": "int64"},
                {"name": "v2", "type": "int64"}],
                unique_keys=True)
        })
        create("table", "//tmp/t_out")
        rows = [{"k1": i, "k2": i + 1, "v1": i + 2, "v2": i + 3} for i in xrange(2)]
        write_table("//tmp/t", rows)

        sort(in_="//tmp/t{k1,v1}",
             out="//tmp/t_out",
             sort_by="k1")

        assert_items_equal(read_table("//tmp/t_out"), [{k: r[k] for k in ("k1", "v1")} for r in rows])

        schema = make_schema(
            [{"name": "k1", "type": "int64", "required": False, "sort_order": "ascending"},
             {"name": "v1", "type": "int64", "required": False}],
            unique_keys=False, strict=True)

        assert normalize_schema(get("//tmp/t_out/@schema")) == schema

        remove("//tmp/t_out")
        create("table", "//tmp/t_out")

        sort(in_="//tmp/t{k1,k2,v2}",
             out="//tmp/t_out",
             sort_by=["k1","k2"])

        assert_items_equal(read_table("//tmp/t_out"), [{k: r[k] for k in ("k1", "k2", "v2")} for r in rows])

        schema = make_schema([
            {"name": "k1", "type": "int64", "required": False, "sort_order": "ascending"},
            {"name": "k2", "type": "int64", "required": False, "sort_order": "ascending"},
            {"name": "v2", "type": "int64", "required": False},
        ], unique_keys=True, strict=True)
        assert normalize_schema(get("//tmp/t_out/@schema")) == schema

    @authors("savrus")
    def test_column_selectors_output_schema_validation(self):
        create("table", "//tmp/t", attributes={
            "schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"}]
        })
        create("table", "//tmp/t_out", attributes={
            "schema": [{"name": "key", "type": "int64", "sort_order": "ascending"}]
        })
        rows = [{"key": i, "value": str(i)} for i in xrange(2)]
        write_table("//tmp/t", rows)

        sort(in_="//tmp/t{key}",
             out="//tmp/t_out",
             sort_by="key")

        assert_items_equal(read_table("//tmp/t_out"), [{"key": r["key"]} for r in rows])

    @authors("savrus")
    def test_query_filtering(self):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": i} for i in xrange(2)])

        with pytest.raises(YtError):
            sort(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={"input_query": "a where a > 0"})

    @authors("gritukan")
    def test_pivot_keys(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        rows = [{"key": "%02d" % key} for key in range(50)]
        shuffle(rows)
        write_table("//tmp/t1", rows)

        sort(in_="//tmp/t1",
             out="//tmp/t2",
             sort_by="key",
             spec={"pivot_keys": [["01"], ["43"]]})
        assert_items_equal(read_table("//tmp/t2"), sorted(rows))
        chunk_ids = get("//tmp/t2/@chunk_ids")
        assert sorted([get("#" + chunk_id + "/@row_count") for chunk_id in chunk_ids]) == [1, 7, 42]

    @authors("gritukan")
    def test_pivot_keys_incorrect_options(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        rows = [{"key": "%02d" % i, "value": i} for i in range(50)]
        write_table("//tmp/t1", rows)

        with pytest.raises(YtError):
            sort(in_="//tmp/t1",
                 out="//tmp/t2",
                 sort_by="key",
                 spec={"pivot_keys": [["73"], ["37"]]})

    @authors("gritukan")
    def test_non_existent_sort_by_column(self):
        create("table", "//tmp/in", attributes={
            "schema": [{"name": "x", "type": "int64"}]
        })
        create("table", "//tmp/out")
        write_table("//tmp/in", [{"x": 1}])

        with pytest.raises(YtError):
            sort(in_="//tmp/in", out="//tmp/out", sort_by="foo")

    @authors("gritukan")
    def test_non_strict_schema(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        write_table("<schema=<strict=false>[{name=value;type=string}]>//tmp/in", [{"key": 2, "value": "a"}, {"key": 1, "value": "b"}])

        sort(in_="//tmp/in", out="//tmp/out1", sort_by="key")
        assert read_table("//tmp/out1") == [{"key": 1, "value": "b"}, {"key": 2, "value": "a"}]
        schema = make_schema([
            {"name": "key", "type": "any", "required": False, "sort_order": "ascending"},
            {"name": "value", "type": "string", "required": False},
        ], unique_keys=False, strict=False)
        assert normalize_schema(get("//tmp/out1/@schema")) == schema

        sort(in_="//tmp/in", out="//tmp/out2", sort_by="foo")
        assert sorted(read_table("//tmp/out1")) == [{"key": 1, "value": "b"}, {"key": 2, "value": "a"}]
        schema = make_schema([
            {"name": "foo", "type": "any", "required": False, "sort_order": "ascending"},
            {"name": "value", "type": "string", "required": False},
        ], unique_keys=False, strict=False)
        assert normalize_schema(get("//tmp/out2/@schema")) == schema

##################################################################

class TestSchedulerSortCommandsMulticell(TestSchedulerSortCommands):
    NUM_SECONDARY_MASTER_CELLS = 2
