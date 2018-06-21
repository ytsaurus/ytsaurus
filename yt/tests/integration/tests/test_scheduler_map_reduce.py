import pytest

from yt_env_setup import YTEnvSetup, unix_only, wait
from yt.environment.helpers import assert_items_equal
from yt_commands import *

from collections import defaultdict
import datetime


##################################################################

class TestSchedulerMapReduceCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent" : {
            "sort_operation_options" : {
                "min_uncompressed_block_size" : 1
            },
            "map_reduce_operation_options" : {
                "min_uncompressed_block_size" : 1,
            },
            "enable_partition_map_job_size_adjustment" : True
        }
    }

    @pytest.mark.parametrize("method", ["map_sort_reduce", "map_reduce", "map_reduce_1p", "reduce_combiner_dev_null",
                                        "force_reduce_combiners", "ordered_map_reduce"])
    def test_simple(self, method):
        text = \
"""
So, so you think you can tell Heaven from Hell,
blue skies from pain.
Can you tell a green field from a cold steel rail?
A smile from a veil?
Do you think you can tell?
And did they get you to trade your heroes for ghosts?
Hot ashes for trees?
Hot air for a cool breeze?
Cold comfort for change?
And did you exchange a walk on part in the war for a lead role in a cage?
How I wish, how I wish you were here.
We're just two lost souls swimming in a fish bowl, year after year,
Running over the same old ground.
What have you found? The same old fears.
Wish you were here.
"""

        # remove punctuation from text
        stop_symbols = ",.?"
        for s in stop_symbols:
            text = text.replace(s, " ")

        mapper = """
import sys

for line in sys.stdin:
    for word in line.lstrip("line=").split():
        print "word=%s\\tcount=1" % word
"""
        reducer = """
import sys

from itertools import groupby

def read_table():
    for line in sys.stdin:
        row = {}
        fields = line.strip().split("\t")
        for field in fields:
            key, value = field.split("=", 1)
            row[key] = value
        yield row

for key, rows in groupby(read_table(), lambda row: row["word"]):
    count = sum(int(row["count"]) for row in rows)
    print "word=%s\\tcount=%s" % (key, count)
"""

        tx = start_transaction(timeout=30000)

        create("table", "//tmp/t_in", tx=tx)
        create("table", "//tmp/t_map_out", tx=tx)
        create("table", "//tmp/t_reduce_in", tx=tx)
        create("table", "//tmp/t_out", tx=tx)

        for line in text.split("\n"):
            write_table("<append=true>//tmp/t_in", {"line": line}, tx=tx)

        create("file", "//tmp/yt_streaming.py")
        create("file", "//tmp/mapper.py")
        create("file", "//tmp/reducer.py")

        write_file("//tmp/mapper.py", mapper, tx=tx)
        write_file("//tmp/reducer.py", reducer, tx=tx)

        if method == "map_sort_reduce":
            map(in_="//tmp/t_in",
                out="//tmp/t_map_out",
                command="python mapper.py",
                file=["//tmp/mapper.py", "//tmp/yt_streaming.py"],
                spec={"mapper": {"format": "dsv"}},
                tx=tx)

            sort(in_="//tmp/t_map_out",
                 out="//tmp/t_reduce_in",
                 sort_by="word",
                 tx=tx)

            reduce(in_="//tmp/t_reduce_in",
                   out="//tmp/t_out",
                   reduce_by = "word",
                   command="python reducer.py",
                   file=["//tmp/reducer.py", "//tmp/yt_streaming.py"],
                   spec={"reducer": {"format": "dsv"}},
                   tx=tx)
        elif method == "map_reduce":
            map_reduce(in_="//tmp/t_in",
                       out="//tmp/t_out",
                       sort_by="word",
                       mapper_command="python mapper.py",
                       mapper_file=["//tmp/mapper.py", "//tmp/yt_streaming.py"],
                       reduce_combiner_command="python reducer.py",
                       reduce_combiner_file=["//tmp/reducer.py", "//tmp/yt_streaming.py"],
                       reducer_command="python reducer.py",
                       reducer_file=["//tmp/reducer.py", "//tmp/yt_streaming.py"],
                       spec={"partition_count": 2,
                             "map_job_count": 2,
                             "mapper": {"format": "dsv"},
                             "reduce_combiner": {"format": "dsv"},
                             "reducer": {"format": "dsv"},
                             "data_size_per_sort_job": 10},
                       tx=tx)
        elif method == "map_reduce_1p":
            map_reduce(in_="//tmp/t_in",
                       out="//tmp/t_out",
                       sort_by="word",
                       mapper_command="python mapper.py",
                       mapper_file=["//tmp/mapper.py", "//tmp/yt_streaming.py"],
                       reducer_command="python reducer.py",
                       reducer_file=["//tmp/reducer.py", "//tmp/yt_streaming.py"],
                       spec={"partition_count": 1, "mapper": {"format": "dsv"}, "reducer": {"format": "dsv"}},
                       tx=tx)
        elif method == "reduce_combiner_dev_null":
            map_reduce(in_="//tmp/t_in",
                       out="//tmp/t_out",
                       sort_by="word",
                       mapper_command="python mapper.py",
                       mapper_file=["//tmp/mapper.py", "//tmp/yt_streaming.py"],
                       reduce_combiner_command="cat >/dev/null",
                       reducer_command="python reducer.py",
                       reducer_file=["//tmp/reducer.py", "//tmp/yt_streaming.py"],
                       spec={"partition_count": 2,
                             "map_job_count": 2,
                             "mapper": {"format": "dsv"},
                             "reduce_combiner": {"format": "dsv"},
                             "reducer": {"format": "dsv"},
                             "data_size_per_sort_job": 10},
                       tx=tx)
        elif method == "force_reduce_combiners":
            map_reduce(in_="//tmp/t_in",
                       out="//tmp/t_out",
                       sort_by="word",
                       mapper_command="python mapper.py",
                       mapper_file=["//tmp/mapper.py", "//tmp/yt_streaming.py"],
                       reduce_combiner_command="python reducer.py",
                       reduce_combiner_file=["//tmp/reducer.py", "//tmp/yt_streaming.py"],
                       reducer_command="cat",
                       spec={"partition_count": 2,
                             "map_job_count": 2,
                             "mapper": {"format": "dsv"},
                             "reduce_combiner": {"format": "dsv"},
                             "reducer": {"format": "dsv"},
                             "force_reduce_combiners": True},
                       tx=tx)
        elif method == "ordered_map_reduce":
            map_reduce(in_="//tmp/t_in",
                       out="//tmp/t_out",
                       sort_by="word",
                       mapper_command="python mapper.py",
                       mapper_file=["//tmp/mapper.py", "//tmp/yt_streaming.py"],
                       reduce_combiner_command="python reducer.py",
                       reduce_combiner_file=["//tmp/reducer.py", "//tmp/yt_streaming.py"],
                       reducer_command="python reducer.py",
                       reducer_file=["//tmp/reducer.py", "//tmp/yt_streaming.py"],
                       spec={"partition_count": 2,
                             "map_job_count": 2,
                             "mapper": {"format": "dsv"},
                             "reduce_combiner": {"format": "dsv"},
                             "reducer": {"format": "dsv"},
                             "data_size_per_sort_job": 10,
                             "ordered": True},
                       tx=tx)
        else:
            assert False

        commit_transaction(tx)

        # count the desired output
        expected = defaultdict(int)
        for word in text.split():
            expected[word] += 1

        output = []
        if method != "reduce_combiner_dev_null":
            for word, count in expected.items():
                output.append( {"word": word, "count": str(count)} )
            assert_items_equal(read_table("//tmp/t_out"), output)
        else:
            assert_items_equal(read_table("//tmp/t_out"), output)

    @unix_only
    @pytest.mark.parametrize("ordered", [False, True])
    def test_many_output_tables(self, ordered):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")
        write_table("//tmp/t_in", {"line": "some_data"})
        map_reduce(in_="//tmp/t_in",
                   out=["//tmp/t_out1", "//tmp/t_out2"],
                   sort_by="line",
                   reducer_command="cat",
                   spec={"reducer": {"format": "dsv"},
                         "ordered": ordered})

    @unix_only
    def test_reduce_with_sort(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [ {"x": 1, "y" : 2},
                              {"x": 1, "y" : 1},
                              {"x": 1, "y" : 3} ])

        write_table("<append=true>//tmp/t_in",
                            [ {"x": 2, "y" : 3},
                              {"x": 2, "y" : 2},
                              {"x": 2, "y" : 4} ])


        reducer = """
import sys
y = 0
for l in sys.stdin:
  l = l.strip('\\n')
  pairs = l.split('\\t')
  pairs = [a.split("=") for a in pairs]
  d = dict([(a[0], int(a[1])) for a in pairs])
  x = d['x']
  y += d['y']
  print l
print "x={0}\ty={1}".format(x, y)
"""

        create("file", "//tmp/reducer.py")
        write_file("//tmp/reducer.py", reducer)

        map_reduce(in_="//tmp/t_in",
                   out="<sorted_by=[x; y]>//tmp/t_out",
                   reduce_by="x",
                   sort_by=["x", "y"],
                   reducer_file=["//tmp/reducer.py"],
                   reducer_command="python reducer.py",
                   spec={
                     "partition_count": 2,
                     "reducer": {"format": "dsv"}})

        assert read_table("//tmp/t_out") == [{"x": "1", "y" : "1"},
                                       {"x": "1", "y" : "2"},
                                       {"x": "1", "y" : "3"},
                                       {"x": "1", "y" : "6"},
                                       {"x": "2", "y" : "2"},
                                       {"x": "2", "y" : "3"},
                                       {"x": "2", "y" : "4"},
                                       {"x": "2", "y" : "9"}]

        assert get('//tmp/t_out/@sorted')

    @unix_only
    def test_row_count_limit(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [ {"x": 1, "y" : 2}])
        write_table("<append=true>//tmp/t_in",  [ {"x": 2, "y" : 3} ])

        map_reduce(in_="//tmp/t_in",
                   out="<row_count_limit=1>//tmp/t_out",
                   reduce_by="x",
                   sort_by="x",
                   reducer_command="cat",
                   spec={
                     "partition_count": 2,
                     "reducer": {"format": "dsv"},
                     "resource_limits" : { "user_slots" : 1}})

        assert len(read_table("//tmp/t_out")) == 1

    def test_intermediate_live_preview(self):
        create_user("u")
        acl = [make_ace("allow", "u", "write")]

        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"foo": "bar"})
        create("table", "//tmp/t2")

        op = map_reduce(dont_track=True, mapper_command="cat", reducer_command="cat; sleep 3",
                        in_="//tmp/t1", out="//tmp/t2",
                        sort_by=["foo"], spec={"intermediate_data_acl": acl})

        time.sleep(2)

        operation_path = get_operation_cypress_path(op.id)
        scheduler_transaction_id = get(operation_path + "/@async_scheduler_transaction_id")
        assert exists(operation_path + "/intermediate", tx=scheduler_transaction_id)

        intermediate_acl = get(operation_path + "/intermediate/@acl", tx=scheduler_transaction_id)
        assert [make_ace("allow", "root", "read")] + acl == intermediate_acl

        op.track()
        assert read_table("//tmp/t2") == [{"foo": "bar"}]

    def test_intermediate_new_live_preview(self):
        create_user("u")
        acl = [make_ace("allow", "u", "write")]

        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"foo": "bar"})
        create("table", "//tmp/t2")

        op = map_reduce(dont_track=True, mapper_command="cat", reducer_command=with_breakpoint("cat; BREAKPOINT"),
                        in_="//tmp/t1", out="//tmp/t2",
                        sort_by=["foo"], spec={"intermediate_data_acl": acl})

        wait(lambda: op.get_job_count("completed") == 1)

        operation_path = op.get_path()
        get(operation_path + "/controller_orchid/data_flow_graph/vertices")
        intermediate_live_data = read_table(operation_path + "/controller_orchid/data_flow_graph/vertices/partition_map/live_previews/0")

        intermediate_acl = get(operation_path + "/intermediate_data_access/@acl")
        assert acl == intermediate_acl

        release_breakpoint()

        op.track()
        assert intermediate_live_data == [{"foo": "bar"}]
        assert read_table("//tmp/t2") == [{"foo": "bar"}]


    def test_incorrect_intermediate_data_acl(self):
        create_user("u")
        acl = [make_ace("u", "blabla", "allow")]

        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"foo": "bar"})
        create("table", "//tmp/t2")

        with pytest.raises(YtError):
            map_reduce(mapper_command="cat", reducer_command="cat",
                       in_="//tmp/t1", out="//tmp/t2",
                       sort_by=["foo"], spec={"intermediate_data_acl": acl})

    def test_intermediate_compression_codec(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"foo": "bar"})
        create("table", "//tmp/t2")

        op = map_reduce(dont_track=True,
                        mapper_command="cat", reducer_command="sleep 5; cat",
                        in_="//tmp/t1", out="//tmp/t2",
                        sort_by=["foo"], spec={"intermediate_compression_codec": "brotli_3"})
        time.sleep(1)
        operation_path = get_operation_cypress_path(op.id)
        async_transaction_id = get(operation_path + "/@async_scheduler_transaction_id")
        assert "brotli_3" == get(operation_path + "/intermediate/@compression_codec", tx=async_transaction_id)
        op.abort()

    @unix_only
    def test_query_simple(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        map_reduce(in_="//tmp/t1", out="//tmp/t2", mapper_command="cat", reducer_command="cat", sort_by=["a"],
            spec={"input_query": "a", "input_schema": [{"name": "a", "type": "string"}]})

        assert read_table("//tmp/t2") == [{"a": "b"}]

    def _ban_nodes_with_intermediate_chunks(self):
        # Figure out the intermediate chunk
        chunks = ls("//sys/chunks", attributes=["staging_transaction_id"])
        intermediate_chunk_ids = []
        for c in chunks:
            if "staging_transaction_id" in c.attributes:
              tx_id = c.attributes["staging_transaction_id"]
              if "Scheduler \"output\" transaction" in get("#{}/@title".format(tx_id)):
                  intermediate_chunk_ids.append(str(c))

        assert len(intermediate_chunk_ids) == 1
        intermediate_chunk_id = intermediate_chunk_ids[0]

        replicas = get("#{}/@stored_replicas".format(intermediate_chunk_id))
        assert len(replicas) == 1
        node_id = replicas[0]

        set("//sys/nodes/{}/@banned".format(node_id), True)
        return [node_id]

    @unix_only
    @pytest.mark.parametrize("ordered", [False, True])
    def test_lost_jobs(self, ordered):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"x": 1, "y" : 2}, {"x": 2, "y" : 3}] * 5)

        reducer_cmd = " ; ".join([
            "cat",
            events_on_fs().notify_event_cmd("reducer_started"),
            events_on_fs().wait_event_cmd("continue_reducer")])

        op = map_reduce(in_="//tmp/t_in",
             out="//tmp/t_out",
             reduce_by="x",
             sort_by="x",
             reducer_command=reducer_cmd,
             spec={"partition_count": 2,
                   "sort_locality_timeout" : 0,
                   "sort_assignment_timeout" : 0,
                   "enable_partitioned_data_balancing" : False,
                   "sort_job_io" : {"table_reader" : {"retry_count" : 1, "pass_count" : 1}},
                   "resource_limits" : { "user_slots" : 1},
                   "ordered": ordered},
             dont_track=True)

        # We wait for the first reducer to start (second is pending due to resource_limits).
        events_on_fs().wait_event("reducer_started", timeout=datetime.timedelta(1000))

        self._ban_nodes_with_intermediate_chunks()

        # First reducer will probably compelete successfully, but the second one
        # must fail due to unavailable intermediate chunk.
        # This will lead to a lost map job.
        events_on_fs().notify_event("continue_reducer")
        op.track()

        assert get("//sys/operations/{0}/@progress/partition_jobs/lost".format(op.id)) == 1

    @unix_only
    @pytest.mark.parametrize("ordered", [False, True])
    def test_unavailable_intermediate_chunks(self, ordered):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"x": 1, "y" : 2}, {"x": 2, "y" : 3}] * 5)

        reducer_cmd = " ; ".join([
            "cat",
            events_on_fs().notify_event_cmd("reducer_started"),
            events_on_fs().wait_event_cmd("continue_reducer")])

        op = map_reduce(in_="//tmp/t_in",
             out="//tmp/t_out",
             reduce_by="x",
             sort_by="x",
             reducer_command=reducer_cmd,
             spec={"enable_intermediate_output_recalculation" : False,
                   "sort_assignment_timeout" : 0,
                   "sort_locality_timeout" : 0,
                   "enable_partitioned_data_balancing" : False,
                   "sort_job_io" : {"table_reader" : {"retry_count" : 1, "pass_count" : 1}},
                   "partition_count": 2,
                   "resource_limits" : { "user_slots" : 1},
                   "ordered": ordered},
             dont_track=True)

        # We wait for the first reducer to start (the second one is pending due to resource_limits).
        events_on_fs().wait_event("reducer_started", timeout=datetime.timedelta(1000))

        banned_nodes = self._ban_nodes_with_intermediate_chunks()

        # The first reducer will probably complete successfully, but the second one
        # must fail due to unavailable intermediate chunk.
        # This will lead to a lost map job.
        events_on_fs().notify_event("continue_reducer")

        def get_unavailable_chunk_count():
            return get("//sys/operations/{0}/@progress/estimated_input_statistics/unavailable_chunk_count".format(op.id))

        # Wait till scheduler discovers that chunk is unavailable.
        wait(lambda: get_unavailable_chunk_count() > 0)

        # Make chunk available again.
        for n in banned_nodes:
            set("//sys/nodes/{0}/@banned".format(n), False)

        wait(lambda: get_unavailable_chunk_count() == 0)

        op.track()

        assert get("//sys/operations/{0}/@progress/partition_reduce_jobs/aborted".format(op.id)) > 0
        assert get("//sys/operations/{0}/@progress/partition_jobs/lost".format(op.id)) == 0

    @pytest.mark.parametrize("ordered", [False, True])
    def test_progress_counter(self, ordered):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"x": 1, "y" : 2}])

        reducer_cmd = " ; ".join([
            "cat",
            events_on_fs().notify_event_cmd("reducer_started"),
            events_on_fs().wait_event_cmd("continue_reducer")])

        op = map_reduce(in_="//tmp/t_in",
                        out="//tmp/t_out",
                        reduce_by="x",
                        sort_by="x",
                        reducer_command=reducer_cmd,
                        spec={"partition_count": 1, "ordered": ordered},
                        dont_track=True)

        events_on_fs().wait_event("reducer_started", timeout=datetime.timedelta(1000))

        job_ids = list(op.get_running_jobs())
        assert len(job_ids) == 1
        job_id = job_ids[0]

        abort_job(job_id)

        events_on_fs().notify_event("continue_reducer")

        op.track()

        partition_reduce_counter = get("//sys/operations/{0}/@progress/data_flow_graph/vertices/partition_reduce/job_counter".format(op.id))
        total_counter = get("//sys/operations/{0}/@progress/jobs".format(op.id))

        assert partition_reduce_counter["aborted"]["total"] == 1
        assert partition_reduce_counter["pending"] == 0

    @unix_only
    def test_query_reader_projection(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b", "c": "d"})

        map_reduce(in_="//tmp/t1", out="//tmp/t2", mapper_command="cat", reducer_command="cat", sort_by=["a"],
            spec={"input_query": "a", "input_schema": [{"name": "a", "type": "string"}]})

        assert read_table("//tmp/t2") == [{"a": "b"}]

    @unix_only
    def test_query_with_condition(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": i} for i in xrange(2)])

        map_reduce(in_="//tmp/t1", out="//tmp/t2", mapper_command="cat", reducer_command="cat", sort_by=["a"],
            spec={"input_query": "a where a > 0", "input_schema": [{"name": "a", "type": "int64"}]})

        assert read_table("//tmp/t2") == [{"a": 1}]

    @unix_only
    def test_query_asterisk(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        rows = [
            {"a": 1, "b": 2, "c": 3},
            {"b": 5, "c": 6},
            {"a": 7, "c": 8}]
        write_table("//tmp/t1", rows)

        schema = [{"name": "z", "type": "int64"},
            {"name": "a", "type": "int64"},
            {"name": "y", "type": "int64"},
            {"name": "b", "type": "int64"},
            {"name": "x", "type": "int64"},
            {"name": "c", "type": "int64"},
            {"name": "u", "type": "int64"}]

        for row in rows:
            for column in schema:
                if column["name"] not in row.keys():
                    row[column["name"]] = None

        map_reduce(in_="//tmp/t1", out="//tmp/t2", mapper_command="cat", reducer_command="cat", sort_by=["a"],
            spec={
                "input_query": "* where a > 0 or b > 0",
                "input_schema": schema})

        assert_items_equal(read_table("//tmp/t2"), rows)

    def test_bad_control_attributes(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"foo": "bar"})
        create("table", "//tmp/t2")

        with pytest.raises(YtError):
            map_reduce(mapper_command="cat", reducer_command="cat",
                       in_="//tmp/t1", out="//tmp/t2",
                       sort_by=["foo"],
                       spec={"reduce_job_io": {"control_attributes" : {"enable_table_index" : "true"}}})

    def test_schema_validation(self):
        create("table", "//tmp/input")
        create("table", "//tmp/output", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}]
            })

        for i in xrange(10):
            write_table("<append=true; sorted_by=[key]>//tmp/input", {"key": i, "value": "foo"})

        map_reduce(
            in_="//tmp/input",
            out="//tmp/output",
            sort_by="key",
            mapper_command="cat",
            reducer_command="cat")

        assert get("//tmp/output/@schema_mode") == "strong"
        assert get("//tmp/output/@schema/@strict")
        assert_items_equal(read_table("//tmp/output"), [{"key": i, "value": "foo"} for i in xrange(10)])

        write_table("<sorted_by=[key]>//tmp/input", {"key": "1", "value": "foo"})

        with pytest.raises(YtError):
            map_reduce(
                in_="//tmp/input",
                out="//tmp/output",
                sort_by="key",
                mapper_command="cat",
                reducer_command="cat")

    def test_computed_columns(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "k1", "type": "int64", "expression": "k2 * 2" },
                    {"name": "k2", "type": "int64"}]
            })

        write_table("//tmp/t1", [{"k2": i} for i in xrange(2)])

        map_reduce(
            in_="//tmp/t1",
            out="//tmp/t2",
            sort_by="k2",
            mapper_command="cat",
            reducer_command="cat")

        assert get("//tmp/t2/@schema_mode") == "strong"
        assert read_table("//tmp/t2") == [{"k1": i * 2, "k2": i} for i in xrange(2)]

    @unix_only
    def test_map_reduce_input_paths_attr(self):
        create("table", "//tmp/input")
        create("table", "//tmp/output")

        for i in xrange(3):
            write_table("<append=true>//tmp/input", {"key": i, "value": "foo"})

        op = map_reduce(
            dont_track=True,
            in_="//tmp/input",
            out="//tmp/output",
            mapper_command="cat; [ $YT_JOB_INDEX == 0 ] && exit 1 || true",
            reducer_command="cat; exit 2",
            sort_by=["key"],
            spec={
                "max_failed_job_count": 2
            })
        with pytest.raises(YtError):
            op.track();

        jobs_path = "//sys/operations/{0}/jobs".format(op.id)
        job_ids = ls(jobs_path)
        assert len(job_ids) == 2
        for job_id in job_ids:
            job = get("{0}/{1}/@".format(jobs_path, job_id))
            if job["job_type"] == "partition_map":
                expected = yson.loads('[<ranges=[{lower_limit={row_index=0};upper_limit={row_index=3}}]>"//tmp/input"]')
                assert  job["input_paths"] == expected
            else:
                assert job["job_type"] == "partition_reduce"
                assert "input_paths" not in job

    @pytest.mark.skipif("True", reason="YT-8228")
    def test_map_reduce_job_size_adjuster_boost(self):
        create("table", "//tmp/t_input")
        # original_data should have at least 1Mb of data
        original_data = [{"index": "%05d" % i, "foo": "a"*35000} for i in xrange(31)]
        for row in original_data:
            write_table("<append=true>//tmp/t_input", row, verbose=False)

        create("table", "//tmp/t_output")

        op = map_reduce(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            sort_by="lines",
            mapper_command="echo lines=`wc -l`",
            reducer_command="cat",
            spec={
                "mapper": {"format": "dsv"},
                "map_job_io": {"table_writer": {"block_size": 1024}},
                "resource_limits": {"user_slots": 1}
            })

        expected = [{"lines": str(2**i)} for i in xrange(5)]
        actual = read_table("//tmp/t_output")
        assert_items_equal(actual, expected)

    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("sort_order", [None, "ascending"])
    def test_map_reduce_on_dynamic_table(self, sort_order, optimize_for):
        def _create_dynamic_table(path):
            create("table", path,
                attributes = {
                    "schema": [
                        {"name": "key", "type": "int64", "sort_order": sort_order},
                        {"name": "value", "type": "string"}
                    ],
                    "dynamic": True,
                    "optimize_for": optimize_for
                })

        self.sync_create_cells(1)
        _create_dynamic_table("//tmp/t")

        create("table", "//tmp/t_out")

        rows = [{"key": i, "value": str(i)} for i in range(6)]
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)
        self.sync_unmount_table("//tmp/t")

        map_reduce(
            in_="//tmp/t",
            out="//tmp/t_out",
            sort_by="key",
            mapper_command="cat",
            reducer_command="cat")

        assert_items_equal(read_table("//tmp/t_out"), rows)

        rows1 = [{"key": i, "value": str(i+1)} for i in range(3, 10)]
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows1)
        self.sync_unmount_table("//tmp/t")

        map_reduce(
            in_="//tmp/t",
            out="//tmp/t_out",
            sort_by="key",
            mapper_command="cat",
            reducer_command="cat")

        def update(new):
            def update_row(row):
                if sort_order == "ascending":
                    for r in rows:
                        if r["key"] == row["key"]:
                            r["value"] = row["value"]
                            return
                rows.append(row)
            for row in new:
                update_row(row)

        update(rows1)

        assert_items_equal(read_table("//tmp/t_out"), rows)

    @pytest.mark.parametrize("sorted", [False, True])
    @pytest.mark.parametrize("ordered", [False, True])
    def test_map_output_table(self, sorted, ordered):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        create("table", "//tmp/t_out_map", attributes={
            "schema": [
                {"name": "bypass_key", "type": "int64", "sort_order": "ascending" if sorted else None}
            ]
        })
        for i in range(10):
            write_table("<append=%true>//tmp/t_in", [{"a": i}])

        map_reduce(
            in_="//tmp/t_in",
            out=["//tmp/t_out_map", "//tmp/t_out"],
            mapper_command="echo \"{bypass_key=$YT_JOB_INDEX}\" 1>&4; echo '{shuffle_key=23}'",
            reducer_command="cat",
            reduce_by=["shuffle_key"],
            sort_by=["shuffle_key"],
            spec={"mapper_output_table_count" : 1, "max_failed_job_count": 1, "data_size_per_map_job": 1, "ordered": ordered})
        assert read_table("//tmp/t_out") == [{"shuffle_key": 23}] * 10
        assert len(read_table("//tmp/t_out_map")) == 10

##################################################################

class TestSchedulerMapReduceCommandsMulticell(TestSchedulerMapReduceCommands):
    NUM_SECONDARY_MASTER_CELLS = 2
