import pytest

from yt_env_setup import YTEnvSetup, unix_only
from yt_commands import *
from yt.yson import YsonEntity


##################################################################

class TestSchedulerJoinReduceCommandsOneCell(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period" : 10,
            "running_jobs_update_period" : 10,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 10,
            "join_reduce_operation_options": {
                "job_splitter": {
                    "min_job_time": 5000,
                    "min_total_data_size": 1024,
                    "update_period": 100,
                    "median_excess_duration": 3000,
                    "candidate_percentile": 0.8,
                    "max_jobs_per_split": 3,
                }
            }
        }
    }

    @unix_only
    def test_join_reduce_tricky_chunk_boundaries(self):
        create("table", "//tmp/in1")
        write_table(
            "//tmp/in1",
            [
                {"key": "0", "value": 1},
                {"key": "2", "value": 2}
            ],
            sorted_by = ["key", "value"])

        create("table", "//tmp/in2")
        write_table(
            "//tmp/in2",
            [
                {"key": "2", "value": 6},
                {"key": "5", "value": 8}
            ],
            sorted_by = ["key", "value"])

        create("table", "//tmp/out")

        join_reduce(
            in_ = ["//tmp/in1{key}", "<foreign=true>//tmp/in2{key}"],
            out = ["<sorted_by=[key]>//tmp/out"],
            command = "cat",
            join_by = "key",
            spec = {
                "reducer": {
                    "format": yson.loads("<line_prefix=tskv;enable_table_index=true>dsv")},
                "data_size_per_job": 1})

        rows = read_table("//tmp/out")
        assert len(rows) == 3
        assert rows == \
            [
                {"key": "0", "@table_index": "0"},
                {"key": "2", "@table_index": "0"},
                {"key": "2", "@table_index": "1"}
            ]

        assert get("//tmp/out/@sorted")

    @unix_only
    def test_join_reduce_cat_simple(self):
        create("table", "//tmp/in1")
        write_table(
            "//tmp/in1",
            [
                {"key": 0, "value": 1},
                {"key": 1, "value": 2},
                {"key": 3, "value": 3},
                {"key": 7, "value": 4}
            ],
            sorted_by = "key")

        create("table", "//tmp/in2")
        write_table(
            "//tmp/in2",
            [
                {"key": -1, "value": 5},
                {"key": 1, "value": 6},
                {"key": 3, "value": 7},
                {"key": 5, "value": 8}
            ],
            sorted_by = "key")

        create("table", "//tmp/out")

        join_reduce(
            in_ = ["<foreign=true>//tmp/in1", "//tmp/in2"],
            out = "<sorted_by=[key]>//tmp/out",
            join_by="key",
            command = "cat",
            spec = {
                "reducer": {
                    "format": "dsv"}})

        assert read_table("//tmp/out") == \
            [
                {"key": "-1", "value": "5"},
                {"key": "1", "value": "2"},
                {"key": "1", "value": "6"},
                {"key": "3", "value": "3"},
                {"key": "3", "value": "7"},
                {"key": "5", "value": "8"},
            ]

        assert get("//tmp/out/@sorted")

    @unix_only
    def test_join_reduce_split_further(self):
        create("table", "//tmp/primary")
        write_table(
            "//tmp/primary",
            [
                {"key": 0, "value": 0},
                {"key": 9, "value": 0}
            ],
            sorted_by = "key")

        create("table", "//tmp/foreign")
        write_table(
            "//tmp/foreign",
            [
                {"key": 0, "value": 1},
                {"key": 4, "value": 1}
            ],
            sorted_by = "key")

        write_table(
            "<append=true; sorted_by=[key]>//tmp/foreign",
            [
                {"key": 5, "value": 1},
                {"key": 9, "value": 1}
            ])

        create("table", "//tmp/out")

        join_reduce(
            in_ = ["<foreign=true>//tmp/foreign", "//tmp/primary"],
            out = "<sorted_by=[key]>//tmp/out",
            join_by="key",
            command = "cat",
            spec = {
                "data_size_per_job" : 1,
                "reducer": {
                    "format": "dsv"}})

        # Must be split into two jobs, despite that only one primary slice is available.
        assert get("//tmp/out/@chunk_count") == 2
        assert get("//tmp/out/@sorted")

    @unix_only
    def test_join_reduce_primary_attribute_compatibility(self):
        create("table", "//tmp/in1")
        write_table("//tmp/in1", [{"key": i, "value": i+1} for i in range(8)], sorted_by = "key")

        create("table", "//tmp/in2")
        write_table("//tmp/in2", [{"key": 2*i-1, "value": i+10} for i in range(4)], sorted_by = "key")

        create("table", "//tmp/out")

        join_reduce(
            in_ = ["//tmp/in1", "<primary=true>//tmp/in2"],
            out = "<sorted_by=[key]>//tmp/out",
            join_by="key",
            command = "cat",
            spec = {
                "reducer": {
                    "format": "dsv"}})

        assert read_table("//tmp/out") == \
            [
                {"key": "-1", "value": "10"},
                {"key": "1", "value": "2"},
                {"key": "1", "value": "11"},
                {"key": "3", "value": "4"},
                {"key": "3", "value": "12"},
                {"key": "5", "value": "6"},
                {"key": "5", "value": "13"},
            ]

        assert get("//tmp/out/@sorted")

    @unix_only
    def test_join_reduce_control_attributes_yson(self):
        create("table", "//tmp/in1")
        write_table(
            "//tmp/in1",
            [
                {"key": 2, "value": 7},
                {"key": 4, "value": 3},
            ],
            sorted_by = "key")

        create("table", "//tmp/in2")
        write_table(
            "//tmp/in2",
            [
                {"key": 0, "value": 4},
                {"key": 2, "value": 6},
                {"key": 4, "value": 8},
                {"key": 6, "value": 10},
            ],
            sorted_by = "key")

        create("table", "//tmp/out")

        op = join_reduce(
            in_ = ["//tmp/in1", "<foreign=true>//tmp/in2"],
            out = "<sorted_by=[key]>//tmp/out",
            join_by = "key",
            command = "cat 1>&2",
            spec = {
                "reducer": {
                    "format": yson.loads("<format=text>yson")},
                "job_io": {
                    "control_attributes": {
                        "enable_table_index": "true",
                        "enable_row_index": "true"}},
                "job_count": 1})

        job_ids = ls("//sys/operations/{0}/jobs".format(op.id))
        assert len(job_ids) == 1
        assert read_file("//sys/operations/{0}/jobs/{1}/stderr".format(op.id, job_ids[0])) == \
"""<"table_index"=0;>#;
<"row_index"=0;>#;
{"key"=2;"value"=7;};
<"table_index"=1;>#;
<"row_index"=1;>#;
{"key"=2;"value"=6;};
<"table_index"=0;>#;
<"row_index"=1;>#;
{"key"=4;"value"=3;};
<"table_index"=1;>#;
<"row_index"=2;>#;
{"key"=4;"value"=8;};
"""

    @unix_only
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_join_reduce_cat_two_output(self, optimize_for):
        schema = [
            {"name" : "key", "type" : "int64", "sort_order" : "ascending"},
            {"name" : "value", "type" : "int64", "sort_order" : "ascending"}
        ]

        create("table", "//tmp/in1", attributes={"schema" : schema, "optimize_for" : optimize_for})
        write_table(
            "//tmp/in1",
            [
                {"key": 0, "value": 1},
                {"key": 2, "value": 2},
                {"key": 4, "value": 3},
                {"key": 8, "value": 4}
            ])

        create("table", "//tmp/in2", attributes={"schema" : schema, "optimize_for" : optimize_for})
        write_table(
            "//tmp/in2",
            [
                {"key": 2, "value": 5},
                {"key": 3, "value": 6},
            ])

        create("table", "//tmp/in3", attributes={"schema" : schema, "optimize_for" : optimize_for})
        write_table(
            "//tmp/in3",
            [ {"key": 2, "value": 1}, ])

        create("table", "//tmp/in4", attributes={"schema" : schema, "optimize_for" : optimize_for})
        write_table(
            "//tmp/in4",
            [ {"key": 3, "value": 7}, ])

        create("table", "//tmp/out1")
        create("table", "//tmp/out2")

        join_reduce(
            in_ = ["//tmp/in1", "<foreign=true>//tmp/in2", "<foreign=true>//tmp/in3", "<foreign=true>//tmp/in4"],
            out = ["<sorted_by=[key]>//tmp/out1", "<sorted_by=[key]>//tmp/out2"],
            command = "cat | tee /dev/fd/4 | grep @table_index=0",
            join_by = "key",
            spec = {
                "reducer": {
                    "format": yson.loads("<enable_table_index=true>dsv")}})

        assert read_table("//tmp/out1") == \
            [
                {"key": "0", "value": "1", "@table_index": "0"},
                {"key": "2", "value": "2", "@table_index": "0"},
                {"key": "4", "value": "3", "@table_index": "0"},
                {"key": "8", "value": "4", "@table_index": "0"},
            ]

        assert read_table("//tmp/out2") == \
            [
                {"key": "0", "value": "1", "@table_index": "0"},
                {"key": "2", "value": "2", "@table_index": "0"},
                {"key": "2", "value": "5", "@table_index": "1"},
                {"key": "2", "value": "1", "@table_index": "2"},
                {"key": "4", "value": "3", "@table_index": "0"},
                {"key": "8", "value": "4", "@table_index": "0"},
            ]

        assert get("//tmp/out1/@sorted")
        assert get("//tmp/out2/@sorted")


    @unix_only
    def test_join_reduce_empty_in(self):
        create("table", "//tmp/in1", attributes={"schema": [{"name": "key", "type": "any", "sort_order": "ascending"}]})
        create("table", "//tmp/in2", attributes={"schema": [{"name": "key", "type": "any", "sort_order": "ascending"}]})
        create("table", "//tmp/out")

        join_reduce(
            in_ = ["//tmp/in1", "<foreign=true>//tmp/in2"],
            out = "//tmp/out",
            join_by = "key",
            command = "cat")

        assert read_table("//tmp/out") == []

    @unix_only
    def test_join_reduce_duplicate_key_columns(self):
        create("table", "//tmp/in1", attributes={
                "schema": [
                    {"name": "a", "type": "any", "sort_order": "ascending"},
                    {"name": "b", "type": "any", "sort_order": "ascending"}
                ]})
        create("table", "//tmp/in2", attributes={
                "schema": [
                    {"name": "a", "type": "any", "sort_order": "ascending"},
                    {"name": "b", "type": "any", "sort_order": "ascending"}
                ]})
        create("table", "//tmp/out")

        # expected error: Duplicate key column name "a"
        with pytest.raises(YtError):
            join_reduce(
                in_ = "//tmp/in",
                out = "//tmp/out",
                command = "cat",
                join_by=["a", "b", "a"])

    @unix_only
    def test_join_reduce_unsorted_input(self):
        create("table", "//tmp/in1")
        write_table("//tmp/in1", {"foo": "bar"})
        create("table", "//tmp/in2", attributes={"schema": [{"name": "foo", "type": "any", "sort_order": "ascending"}]})
        create("table", "//tmp/out")

        # expected error: Input table //tmp/in1 is not sorted
        with pytest.raises(YtError):
            join_reduce(
                in_ = ["//tmp/in1", "<foreign=true>//tmp/in2"],
                out = "//tmp/out",
                join_by = "key",
                command = "cat")

    @unix_only
    def test_join_reduce_different_key_column(self):
        create("table", "//tmp/in1")
        write_table("//tmp/in1", {"foo": "bar"}, sorted_by=["foo"])
        create("table", "//tmp/in2", attributes={"schema": [{"name": "baz", "type": "any", "sort_order": "ascending"}]})
        create("table", "//tmp/out")

        # expected error: Key columns do not match
        with pytest.raises(YtError):
            join_reduce(
                in_ = ["//tmp/in1", "<foreign=true>//tmp/in2"],
                out = "//tmp/out",
                join_by = "key",
                command = "cat")

    @unix_only
    def test_join_reduce_non_prefix(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", {"key": "1", "subkey": "2"}, sorted_by=["key", "subkey"])

        # expected error: Input table is sorted by columns that are not compatible with the requested columns"
        with pytest.raises(YtError):
            join_reduce(
                in_ = ["//tmp/in", "<foreign=true>//tmp/in"],
                out = "//tmp/out",
                command = "cat",
                join_by = "subkey")

    @unix_only
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_join_reduce_short_limits(self, optimize_for):
        schema = [
            {"name" : "key", "type" : "string", "sort_order" : "ascending"},
            {"name" : "subkey", "type" : "string", "sort_order" : "ascending"}
        ]
        create("table", "//tmp/in1", attributes={"schema" : schema, "optimize_for" : optimize_for})
        create("table", "//tmp/in2", attributes={"schema" : schema, "optimize_for" : optimize_for})
        create("table", "//tmp/out")
        write_table("//tmp/in1", [{"key": "1", "subkey": "2"}, {"key": "3"}, {"key": "5"}])
        write_table("//tmp/in2", [{"key": "1", "subkey": "3"}, {"key": "3", "subkey": "3"}, {"key": "4"}])

        join_reduce(
            in_ = ['//tmp/in1["1":"4"]', "<foreign=true>//tmp/in2"],
            out = "<sorted_by=[key; subkey]>//tmp/out",
            command = "cat",
            join_by = ["key", "subkey"],
            spec = {
                "reducer": {
                    "format": yson.loads("<line_prefix=tskv>dsv")},
                "data_size_per_job": 1})

        assert read_table("//tmp/out") == \
            [
                {"key": "1", "subkey": "2"},
                {"key": "3", "subkey": YsonEntity()}
            ]

    @unix_only
    def test_join_reduce_many_output_tables(self):
        output_tables = ["//tmp/t%d" % i for i in range(3)]

        create("table", "//tmp/t_in")
        for table_path in output_tables:
            create("table", table_path)

        write_table("//tmp/t_in", [{"k": 10}], sorted_by="k")

        reducer = \
"""
cat  > /dev/null
echo {v = 0} >&1
echo {v = 1} >&4
echo {v = 2} >&7
"""
        create("file", "//tmp/reducer.sh")
        write_file("//tmp/reducer.sh", reducer)

        join_reduce(
            in_ = ["//tmp/t_in", "<foreign=true>//tmp/t_in"],
            out = output_tables,
            join_by = "k",
            command = "bash reducer.sh",
            file = "//tmp/reducer.sh")

        assert read_table(output_tables[0]) == [{"v": 0}]
        assert read_table(output_tables[1]) == [{"v": 1}]
        assert read_table(output_tables[2]) == [{"v": 2}]

    @unix_only
    def test_join_reduce_job_count(self):
        create("table", "//tmp/in1", attributes={"compression_codec": "none"})
        create("table", "//tmp/in2", attributes={"schema": [{"name": "key", "type": "string", "sort_order": "ascending"}]})
        create("table", "//tmp/out")

        count = 1000

        # Job count works only if we have enough splits in input chunks.
        # Its default rate 0.0001, so we should have enough rows in input table
        write_table(
            "//tmp/in1",
            [{"key": "%.010d" % num} for num in xrange(count)],
            sorted_by = ["key"],
            table_writer = {"block_size": 1024})
        # write secondary table as one row per chunk
        write_table(
            "//tmp/in2",
            [{"key": "%.010d" % num} for num in xrange(0, count, 20)],
            sorted_by=["key"],
            max_row_buffer_size=1,
            table_writer={"desired_chunk_size": 1})

        assert get("//tmp/in2/@chunk_count") == count // 20

        join_reduce(
            in_ = ["//tmp/in1", "<foreign=true>//tmp/in2"],
            out = "//tmp/out",
            command = 'echo "key=`wc -l`"',
            join_by = ["key"],
            spec = {
                "reducer": {
                    "format": yson.loads("<enable_table_index=true>dsv")
                },
                "data_size_per_job": 250
            })

        read_table("//tmp/out");
        get("//tmp/out/@row_count")
        # Check that operation has more than 1 job
        assert get("//tmp/out/@row_count") >= 2

    @unix_only
    def test_join_reduce_key_switch_yamr(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out")

        write_table(
            "//tmp/in",
            [
                {"key": "a", "value": ""},
                {"key": "b", "value": ""},
                {"key": "b", "value": ""}
            ],
            sorted_by = ["key"])

        op = join_reduce(
            in_ = ["//tmp/in", "<foreign=true>//tmp/in"],
            out = "//tmp/out",
            command = "cat 1>&2",
            join_by = ["key"],
            spec = {
                "job_io": {
                    "control_attributes": {
                        "enable_key_switch": "true"
                    }
                },
                "reducer": {
                    "format": yson.loads("<lenval=true>yamr"),
                    "enable_input_table_index": False
                },
                "job_count": 1
            })

        jobs_path = "//sys/operations/{0}/jobs".format(op.id)
        job_ids = ls(jobs_path)
        assert len(job_ids) == 1
        stderr_bytes = read_file("{0}/{1}/stderr".format(jobs_path, job_ids[0]))

        assert stderr_bytes.encode("hex") == \
            "010000006100000000" \
            "010000006100000000" \
            "feffffff" \
            "010000006200000000" \
            "010000006200000000" \
            "010000006200000000" \
            "010000006200000000"

    @unix_only
    def test_join_reduce_with_small_block_size(self):
        create("table", "//tmp/in1", attributes={"compression_codec": "none"})
        create("table", "//tmp/in2")
        create("table", "//tmp/out")

        count = 300

        write_table(
            "<append=true>//tmp/in1",
            [ {"key": "%05d" % (10000 + num / 2), "val1": num} for num in xrange(count) ],
            sorted_by = ["key"],
            table_writer = {"block_size": 1024})
        write_table(
            "<append=true>//tmp/in1",
            [ {"key": "%05d" % (10000 + num / 2), "val1": num} for num in xrange(count, 2 * count) ],
            sorted_by = ["key"],
            table_writer = {"block_size": 1024})

        write_table(
            "<append=true>//tmp/in2",
            [ {"key": "%05d" % (10000 + num / 2), "val2": num} for num in xrange(count) ],
            sorted_by = ["key"],
            table_writer = {"block_size": 1024})
        write_table(
            "<append=true>//tmp/in2",
            [ {"key": "%05d" % (10000 + num / 2), "val2": num} for num in xrange(count, 2 * count) ],
            sorted_by = ["key"],
            table_writer = {"block_size": 1024})

        join_reduce(
            in_ = [
                '<ranges=[{lower_limit={row_index=100;key=["10010"]};upper_limit={row_index=540;key=["10280"]}}];primary=true>//tmp/in1',
                "<foreign=true>//tmp/in2"
            ],
            out = "//tmp/out",
            command = """awk '{print $0"\tji="ENVIRON["YT_JOB_INDEX"]"\tsi="ENVIRON["YT_START_ROW_INDEX"]}' """,
            join_by = ["key"],
            spec = {
                "reducer": {
                    "format": yson.loads("<enable_table_index=true;table_index_column=ti>dsv")
                },
                "data_size_per_job": 500
            })

        assert get("//tmp/out/@row_count") > 800

    @unix_only
    def test_join_reduce_skewed_key_distribution(self):
        create("table", "//tmp/in1")
        create("table", "//tmp/in2")
        create("table", "//tmp/out")

        data1 = [{"key": "a"}] * 8000 + [{"key": "b"}] * 2000
        write_table(
            "//tmp/in1",
            data1,
            sorted_by=["key"],
            table_writer={"block_size": 1024})

        data2 = [
            {"key": "a"},
            {"key": "b"}
        ]
        write_table(
            "//tmp/in2",
            data2,
            sorted_by=["key"])

        op = join_reduce(
            in_=["//tmp/in1", "<foreign=true>//tmp/in2"],
            out=["//tmp/out"],
            command="uniq",
            join_by="key",
            spec={
                "reducer": {"format": yson.loads("<enable_table_index=true>dsv")},
                "job_count": 2
            })

        assert get("//tmp/out/@chunk_count") == 2

        assert sorted(read_table("//tmp/out")) == \
            sorted([
                {"key": "a", "@table_index": "0"},
                {"key": "a", "@table_index": "1"},
                # ------partition boundary-------
                {"key": "a", "@table_index": "0"},
                {"key": "a", "@table_index": "1"},
                {"key": "b", "@table_index": "0"},
                {"key": "b", "@table_index": "1"},
            ])

        histogram = get("//sys/operations/{0}/@progress/input_data_size_histogram".format(op.id))
        assert sum(histogram["count"]) == 2

    # Check compatibility with deprecated <primary=true> attribute
    @unix_only
    def test_join_reduce_compatibility(self):
        create("table", "//tmp/in1")
        write_table(
            "//tmp/in1",
            [
                {"key": 1, "value": 1},
                {"key": 2, "value": 2},
            ],
            sorted_by = "key")

        create("table", "//tmp/in2")
        write_table(
            "//tmp/in2",
            [
                {"key": -1, "value": 5},
                {"key": 1, "value": 6},
            ],
            sorted_by = "key")

        create("table", "//tmp/out")

        join_reduce(
            in_ = ["//tmp/in1", "//tmp/in1", "<primary=true>//tmp/in2"],
            out = "<sorted_by=[key]>//tmp/out",
            command = "cat",
            join_by = "key",
            spec = {
                "reducer": {
                    "format": "dsv"}})

        assert read_table("//tmp/out") == \
            [
                {"key": "-1", "value": "5"},
                {"key": "1", "value": "1"},
                {"key": "1", "value": "1"},
                {"key": "1", "value": "6"},
            ]

        assert get("//tmp/out/@sorted")

    @unix_only
    def test_join_reduce_row_count_limit(self):
        create("table", "//tmp/in1")

        write_table(
            "<append=true>//tmp/in1",
            [{"key": "%05d" % i, "value": "foo"} for i in range(5)],
            sorted_by=["key"],
            max_row_buffer_size=1,
            table_writer={"desired_chunk_size": 1})

        create("table", "//tmp/in2")
        write_table(
            "<append=true>//tmp/in2",
            [{"key": "%05d" % i, "value": "bar"} for i in range(5)],
            sorted_by = ["key"],
            max_row_buffer_size=1,
            table_writer={"desired_chunk_size": 1})

        assert get("//tmp/in1/@chunk_count") == 5
        assert get("//tmp/in2/@chunk_count") == 5

        create("table", "//tmp/out")
        op = join_reduce(
            in_=["<foreign=true>//tmp/in2", "//tmp/in1"],
            out="<row_count_limit=5>//tmp/out",
            command="cat",
            join_by=["key"],
            spec={
                "reducer": {
                    "format": "dsv"
                },
                "data_size_per_job": 1,
                "max_failed_job_count": 1
            })

        assert len(read_table("//tmp/out")) == 6

    @unix_only
    def test_join_reduce_short_range(self):
        count = 300

        create("table", "//tmp/in1")
        write_table(
            "<append=true>//tmp/in1",
            [ {"key": "%05d" % num, "subkey" : "", "value": num} for num in xrange(count) ],
            sorted_by = ["key", "subkey"],
            table_writer = {"block_size": 1024})

        create("table", "//tmp/in2")
        write_table(
            "<append=true>//tmp/in2",
            [ {"key": "%05d" % num, "subkey" : "", "value": num} for num in xrange(count) ],
            sorted_by = ["key", "subkey"],
            table_writer = {"block_size": 1024})

        create("table", "//tmp/out")
        join_reduce(
            in_=['//tmp/in1["00100":"00200"]', "<foreign=true>//tmp/in2"],
            out="//tmp/out",
            command="cat",
            join_by=["key", "subkey"],
            spec={
                "reducer": {
                    "format": "dsv"
                },
                "data_size_per_job": 512,
                "max_failed_job_count": 1
            })

        assert get("//tmp/out/@row_count") == 200

    @unix_only
    def test_join_reduce_cartesian_product(self):
        create("table", "//tmp/in")
        for i in range(20):
            write_table(
                "<append=true>//tmp/in",
                [{"fake_key": ""} for num in xrange(i * 100, (i + 1) * 100)],
                sorted_by = ["fake_key"],
                table_writer = {"block_size": 1024})

        create("table", "//tmp/out")
        join_reduce(
            in_=['//tmp/in', "<foreign=true>//tmp/in"],
            out="//tmp/out",
            command="echo a=$JOB_INDEX",
            join_by=["fake_key"],
            spec={
                "reducer": {
                    "format": "dsv"
                },
                "max_failed_job_count": 1,
                "job_count": 10,
                "nightly_options" : {
                    "use_new_endpoint_keys": True
                },
                "consider_only_primary_size": True
            })

        job_count = get("//tmp/out/@row_count")
        assert 9 <= job_count <= 11

    @unix_only
    def test_join_reduce_input_paths_attr(self):
        create("table", "//tmp/in1")
        for i in xrange(0, 5, 2):
            write_table(
                "<append=true>//tmp/in1",
                [{"key": "%05d" % (i+j), "value": "foo"} for j in xrange(2)],
                sorted_by = ["key"])

        create("table", "//tmp/in2")
        for i in xrange(3, 16, 2):
            write_table(
                "<append=true>//tmp/in2",
                [{"key": "%05d" % ((i+j)/4), "value": "foo"} for j in xrange(2)],
                sorted_by = ["key"])

        create("table", "//tmp/out")
        op = join_reduce(
            dont_track=True,
            in_=["<foreign=true>//tmp/in1", '//tmp/in2["00001":"00004"]'],
            out="//tmp/out",
            command="exit 1",
            join_by=["key"],
            spec={
                "reducer": {
                    "format": "dsv"
                },
                "job_count": 1,
                "max_failed_job_count": 1
            })
        with pytest.raises(YtError):
            op.track();

    def test_join_reduce_on_dynamic_table(self):
        sync_create_cells(1)
        create("table", "//tmp/t1",
           attributes = {
               "schema": [
                   {"name": "key", "type": "string", "sort_order": "ascending"},
                   {"name": "value", "type": "string"}],
               "dynamic": True
           })

        create("table", "//tmp/t2")
        create("table", "//tmp/t_out")

        rows = [{"key": str(i), "value": str(i)} for i in range(1)]
        sync_mount_table("//tmp/t1")
        insert_rows("//tmp/t1", rows)
        sync_unmount_table("//tmp/t1")

        joined_rows = [{"key": "0", "value": "joined"}]
        write_table("//tmp/t2", joined_rows, sorted_by=["key"])

        join_reduce(
            in_=["//tmp/t1", "<foreign=true>//tmp/t2"],
            out="//tmp/t_out",
            join_by="key",
            command="cat",
            spec={
                "reducer": {
                    "format": "dsv"
                }})

        assert read_table("//tmp/t_out") == rows + joined_rows

        rows = [{"key": str(i), "value": str(i+1)} for i in range(1)]
        sync_mount_table("//tmp/t1")
        insert_rows("//tmp/t1", rows)
        sync_unmount_table("//tmp/t1")

        join_reduce(
            in_=["//tmp/t1", "<foreign=true>//tmp/t2"],
            out="//tmp/t_out",
            join_by="key",
            command="cat",
            spec={
                "reducer": {
                    "format": "dsv"
                }})

        assert read_table("//tmp/t_out") == rows + joined_rows

    def test_join_reduce_with_dynamic_foreign(self):
        create("table", "//tmp/t1",
            attributes = {
                "schema": [
                    {"name": "key1", "type": "string", "sort_order": "ascending"},
                    {"name": "primary_value", "type": "int64"}
                ]
            })

        sync_create_cells(1)
        create("table", "//tmp/t2",
            attributes = {
                "schema": [
                    {"name": "key1", "type": "string", "sort_order": "ascending"},
                    {"name": "key2", "type": "string", "sort_order": "ascending"},
                    {"name": "foreign_value", "type": "int64"}],
                "dynamic": True
            })

        create("table", "//tmp/t_out")

        write_table("//tmp/t1", [{"key1": "7", "primary_value": 42}])

        rows = [{"key1": str(i), "key2": str(i * i), "foreign_value": i} for i in range(10)]
        sync_mount_table("//tmp/t2")
        insert_rows("//tmp/t2", rows)
        sync_unmount_table("//tmp/t2")

        join_reduce(
            in_=["//tmp/t1", "<foreign=%true>//tmp/t2"],
            out="//tmp/t_out",
            join_by="key1",
            command="cat",
            spec={
                "reducer": {
                    "format": "dsv"
                }
            }
        )

        assert len(read_table("//tmp/t_out")) == 2

    def test_join_reduce_interrupt_job(self):
        create("table", "//tmp/input1")
        write_table(
            "//tmp/input1",
            [{"key": "(%08d)" % (i * 2 + 1), "value": "(t_1)", "data": "a" * (2 * 1024 * 1024)} for i in range(3)],
            sorted_by = ["key", "value"])

        create("table", "//tmp/input2")
        write_table(
            "//tmp/input2",
            [{"key": "(%08d)" % (i / 2), "value": "(t_2)"} for i in range(14)],
            sorted_by = ["key"])

        create("table", "//tmp/output")

        op = join_reduce(
            dont_track=True,
            label="interrupt_job",
            in_=["<foreign=true>//tmp/input2", "//tmp/input1"],
            out="<sorted_by=[key]>//tmp/output",
            command=with_breakpoint("""read; echo "${REPLY/(???)/(job)}"; echo "$REPLY"; BREAKPOINT ; cat """),
            join_by=["key"],
            spec={
                "reducer": {
                    "format": "dsv"
                },
                "max_failed_job_count": 1,
                "job_io" : {
                    "buffer_row_count" : 1,
                },
                "enable_job_splitting": False,
            })

        jobs = wait_breakpoint()
        interrupt_job(jobs[0])
        release_breakpoint()
        op.track()

        result = read_table("//tmp/output", verbose=False)
        for row in result:
            print "key:", row["key"], "value:", row["value"]
        assert len(result) == 11
        row_index = 0
        job_indexes = []
        row_table_count = {}
        for row in result:
            if row["value"] == "(job)":
                job_indexes.append(row_index)
            row_table_count[row["value"]] = row_table_count.get(row["value"], 0) + 1
            row_index += 1
        assert row_table_count["(job)"] == 2
        assert row_table_count["(t_1)"] == 3
        assert row_table_count["(t_2)"] == 6
        assert job_indexes[1] == 4
        assert get("//sys/operations/{0}/@progress/job_statistics/data/input/row_count/$/completed/join_reduce/sum".format(op.id)) == len(result) - 2

    @pytest.mark.xfail(run = True, reason = "max42 should support TChunkStripeList->TotalRowCount in TSortedChunkPool")
    def test_join_reduce_job_splitter(self):
        create("table", "//tmp/in_1")
        for j in range(20):
            write_table(
                "<append=true>//tmp/in_1",
                [{"key": "%08d" % (j * 4 + i), "value": "(t_1)", "data": "a" * (1024 * 1024)} for i in range(4)],
                sorted_by = ["key"],
                table_writer = {
                    "block_size": 1024,
                })

        create("table", "//tmp/in_2")
        for j in range(20):
            write_table(
                "//tmp/in_2",
                [{"key": "(%08d)" % ((j * 10 + i) / 2), "value": "(t_2)"} for i in range(10)],
                sorted_by = ["key"],
                table_writer = {
                    "block_size": 1024,
                })

        input_ = ["<foreign=true>//tmp/in_2"] + ["//tmp/in_1"]
        output = "//tmp/output"
        create("table", output)

        command="""
while read ROW; do
    if [ "$YT_JOB_INDEX" == 0 ]; then
        sleep 2
    else
        sleep 0.2
    fi
    echo "$ROW"
done
"""

        op = join_reduce(
            dont_track=True,
            label="split_job",
            in_=input_,
            out=output,
            command=command,
            join_by="key",
            spec={
                "reducer": {
                    "format": "dsv",
                },
                "data_size_per_job": 17 * 1024 * 1024,
                "max_failed_job_count": 1,
                "job_io": {
                    "buffer_row_count" : 1,
                },
            })

        op.track()

        completed = get("//sys/operations/{0}/@progress/jobs/completed".format(op.id))
        interrupted = completed["interrupted"]
        assert completed["total"] >= 6
        assert interrupted["job_split"] >= 1


class TestSchedulerNewJoinReduceCommandsOneCell(TestSchedulerJoinReduceCommandsOneCell):
    @classmethod
    def modify_controller_agent_config(cls, config):
        TestSchedulerJoinReduceCommandsOneCell.modify_controller_agent_config(config)
        config["controller_agent"]["join_reduce_operation_options"]["spec_template"] = {"use_new_controller": True}

    @unix_only
    def test_join_reduce_two_primaries(self):
        create("table", "//tmp/in1")
        write_table("//tmp/in1", [{"key": 0}], sorted_by="key")

        create("table", "//tmp/in2")
        write_table("//tmp/in2", [{"key": 0}], sorted_by="key")

        create("table", "//tmp/in3")
        write_table("//tmp/in3", [{"key": 0, "value": 1}], sorted_by="key")

        create("table", "//tmp/out")

        join_reduce(
            in_=["//tmp/in1", "//tmp/in2", "<foreign=true>//tmp/in3"],
            out="//tmp/out",
            join_by="key",
            command="cat",
            spec={"reducer": {"format": "dsv"}})

        expected = [
            {"key": "0"},
            {"key": "0"},
            {"key": "0", "value": "1"}
        ]
        assert read_table("//tmp/out") == expected


class TestSchedulerJoinReduceCommandsMulticell(TestSchedulerJoinReduceCommandsOneCell):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestSchedulerNewJoinReduceCommandsMulticell(TestSchedulerNewJoinReduceCommandsOneCell):
    NUM_SECONDARY_MASTER_CELLS = 2
