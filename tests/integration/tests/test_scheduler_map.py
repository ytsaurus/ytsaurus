import pytest
import time
import __builtin__
import os
import tempfile

from yt.wrapper import format

from yt_env_setup import YTEnvSetup, mark_multicell, linux_only
from yt_commands import *
from distutils.spawn import find_executable

##################################################################

def get_statistics(statistics, complex_key):
    result = statistics
    for part in complex_key.split("."):
        if part:
            result = result[part]
    return result

##################################################################

class TestCGroups(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "exec_agent" : {
            "enable_cgroups" : True,
            "supported_cgroups" : [ "cpuacct", "blkio", "memory"],
            "slot_manager" : {
                "enforce_job_control" : True,
            }
        }
    }

    def test_failed_jobs_twice(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"} for i in xrange(200)])
        op_id = map(
            dont_track=True,
            in_="//tmp/t1",
            out="//tmp/t2",
            command='trap "" HUP; bash -c "sleep 60" &; sleep $[( $RANDOM % 5 )]s; exit 42;',
            spec={"max_failed_job_count": 1, "job_count": 200})

        with pytest.raises(YtError):
            track_op(op_id)

        for job_desc in ls("//sys/operations/{0}/jobs".format(op_id), attr=["error"]):
            print job_desc.attributes
            print job_desc.attributes["error"]["inner_errors"][0]["message"]
            assert "Process exited with code " in job_desc.attributes["error"]["inner_errors"][0]["message"]


class TestEventLog(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler" : {
            "event_log" : {
                "flush_period" : 5000
            }
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent" : {
            "enable_cgroups" : True,
            "supported_cgroups" : [ "cpuacct", "blkio", "memory"],
            "slot_manager" : {
                "enforce_job_control" : True
            }
        }
    }

    def test_scheduler_event_log(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"}])
        op_id = map(in_="//tmp/t1", out="//tmp/t2", command='cat; bash -c "for (( I=0 ; I<=100*1000 ; I++ )) ; do echo $(( I+I*I )); done; sleep 2" >/dev/null')

        statistics = get("//sys/operations/{0}/@progress/job_statistics".format(op_id))
        assert get_statistics(statistics, "user_job.cpu.user.$.completed.map.sum") > 0
        assert get_statistics(statistics, "user_job.block_io.bytes_read.$.completed.map.sum") is not None
        assert get_statistics(statistics, "user_job.current_memory.rss.$.completed.map.count") > 0
        assert get_statistics(statistics, "user_job.max_memory.$.completed.map.count") > 0
        assert get_statistics(statistics, "user_job.cumulative_memory_mb_sec.$.completed.map.count") > 0
        assert get_statistics(statistics, "job_proxy.cpu.user.$.completed.map.count") == 1
        assert get_statistics(statistics, "job_proxy.cpu.user.$.completed.map.count") == 1

        # wait for scheduler to dump the event log
        time.sleep(6)
        res = read_table("//sys/scheduler/event_log")
        event_types = __builtin__.set()
        for item in res:
            event_types.add(item["event_type"])
            if item["event_type"] == "job_completed":
                stats = item["statistics"]
                user_time = get_statistics(stats, "user_job.cpu.user")
                # our job should burn enough cpu
                assert user_time > 0
        assert "operation_started" in event_types



class TestJobProber(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "exec_agent" : {
            'enable_cgroups' : True,
            "supported_cgroups" : [ "cpuacct", "blkio", "memory"]
        }
    }

    def test_strace_job(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        tmpdir = tempfile.mkdtemp(prefix="strace_job")

        command = "touch {0}/started || exit 1; cat; until rmdir {0} 2>/dev/null; do sleep 1; done".format(tmpdir)

        op_id = map(
            dont_track=True,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=command,
            spec={
                "mapper": {
                    "format": "json"
                }
            })

        try:
            pin_filename = os.path.join(tmpdir, "started")
            while not os.access(pin_filename, os.F_OK):
                time.sleep(0.2)

            jobs_path = "//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op_id)
            jobs = ls(jobs_path)
            assert jobs

            result = strace_job(jobs[0])
        finally:
            try:
                os.unlink(pin_filename)
            except OSError:
                pass
            try:
                os.unlink(tmpdir)
            except OSError:
                pass

        for pid, trace in result['traces'].iteritems():
            if trace['trace'] != "attach: ptrace(PTRACE_ATTACH, ...): No such process\n":
                assert trace['trace'].startswith("Process {0} attached".format(pid))
        track_op(op_id)

##################################################################

class TestSchedulerMapCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    def test_empty_table(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        map(in_="//tmp/t1", out="//tmp/t2", command="cat")

        assert read_table("//tmp/t2") == []

    @linux_only
    def test_one_chunk(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})
        op_id = map(dont_track=True,
            in_="//tmp/t1", out="//tmp/t2", command=r'cat; echo "{v1=\"$V1\"};{v2=\"$V2\"}"',
            spec={"mapper": {"environment": {"V1": "Some data", "V2": "$(SandboxPath)/mytmp"}},
                  "title": "MyTitle"})

        get("//sys/operations/%s/@spec" % op_id)
        track_op(op_id)

        res = read_table("//tmp/t2")
        assert len(res) == 3
        assert res[0] == {"a" : "b"}
        assert res[1] == {"v1" : "Some data"}
        assert res[2].has_key("v2")
        assert res[2]["v2"].endswith("/mytmp")
        assert res[2]["v2"].startswith("/")

    def test_big_input(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        count = 1000 * 1000
        original_data = [{"index": i} for i in xrange(count)]
        write_table("//tmp/t1", original_data)

        command = "cat"
        map(in_="//tmp/t1", out="//tmp/t2", command=command)

        new_data = read_table("//tmp/t2", verbose=False)
        assert sorted(row.items() for row in new_data) == [[("index", i)] for i in xrange(count)]

    def test_two_inputs_at_the_same_time(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output1")
        create("table", "//tmp/t_output2")

        count = 1000
        original_data = [{"index": i} for i in xrange(count)]
        write_table("//tmp/t_input", original_data)

        file = "//tmp/some_file.txt"
        create("file", file)
        write_file(file, "{value=42};\n")

        command = 'bash -c "cat <&0 & sleep 0.1; cat some_file.txt >&4; wait;"'
        map(in_="//tmp/t_input",
            out=["//tmp/t_output1", "//tmp/t_output2"],
            command=command,
            file=[file],
            verbose=True)

        assert read_table("//tmp/t_output2") == [{"value": 42}]
        assert sorted([row.items() for row in read_table("//tmp/t_output1")]) == [[("index", i)] for i in xrange(count)]

    def test_first_after_second(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output1")
        create("table", "//tmp/t_output2")

        count = 10000
        original_data = [{"index": i} for i in xrange(count)]
        write_table("//tmp/t_input", original_data)

        file1 = "//tmp/some_file.txt"
        create("file", file1)
        write_file(file1, "}}}}};\n")

        command = 'cat some_file.txt >&4; cat >&4; echo "{value=42}"'
        op_id = map(dont_track=True,
                    in_="//tmp/t_input",
                    out=["//tmp/t_output1", "//tmp/t_output2"],
                    command=command,
                    file=[file1],
                    verbose=True)
        with pytest.raises(YtError):
            track_op(op_id)

    @linux_only
    def test_in_equal_to_out(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"foo": "bar"})

        map(in_="//tmp/t1", out="<append=true>//tmp/t1", command="cat")

        assert read_table("//tmp/t1") == [{"foo": "bar"}, {"foo": "bar"}]

    #TODO(panin): refactor
    def _check_all_stderrs(self, op_id, expected_content, expected_count):
        jobs_path = "//sys/operations/" + op_id + "/jobs"
        assert get(jobs_path + "/@count") == expected_count
        for job_id in ls(jobs_path):
            assert read_file(jobs_path + "/" + job_id + "/stderr") == expected_content

    # check that stderr is captured for successfull job
    @linux_only
    def test_stderr_ok(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        command = """cat > /dev/null; echo stderr 1>&2; echo {operation='"'$YT_OPERATION_ID'"'}';'; echo {job_index=$YT_JOB_INDEX};"""

        op_id = map(dont_track=True, in_="//tmp/t1", out="//tmp/t2", command=command)
        track_op(op_id)

        assert read_table("//tmp/t2") == [{"operation" : op_id}, {"job_index" : 0}]
        self._check_all_stderrs(op_id, "stderr\n", 1)

    # check that stderr is captured for failed jobs
    @linux_only
    def test_stderr_failed(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        command = """echo "{x=y}{v=};{a=b}"; while echo xxx 2>/dev/null; do false; done; echo stderr 1>&2; cat > /dev/null;"""

        op_id = map(dont_track=True, in_="//tmp/t1", out="//tmp/t2", command=command)
        # if all jobs failed then operation is also failed
        with pytest.raises(YtError):
            track_op(op_id)

        self._check_all_stderrs(op_id, "stderr\n", 10)

    # check max_stderr_count
    @linux_only
    def test_stderr_limit(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        command = "cat > /dev/null; echo stderr 1>&2; exit 125"

        op_id = map(dont_track=True, in_="//tmp/t1", out="//tmp/t2", command=command, spec={"max_failed_job_count": 5})
        # if all jobs failed then operation is also failed
        with pytest.raises(YtError):
            track_op(op_id)

        self._check_all_stderrs(op_id, "stderr\n", 5)

    @pytest.mark.skipif("not sys.platform.startswith(\"linux\")")
    def test_stderr_of_failed_jobs(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"} for i in xrange(110)])

        tmpdir = tempfile.mkdtemp(prefix="stderr_of_failed_jobs_semaphore")
        try:
            os.chmod(tmpdir, 0777)
            for i in xrange(109):
                with open(os.path.join(tmpdir, str(i)), "w") as f:
                    f.close()

            command = """cat > /dev/null;
                SEMAPHORE_DIR={0}
                echo stderr 1>&2;
                if [ "$YT_START_ROW_INDEX" = "109" ]; then
                    until rmdir $SEMAPHORE_DIR 2>/dev/null; do sleep 1; done
                    exit 125;
                else
                    rm $SEMAPHORE_DIR/$YT_START_ROW_INDEX
                    exit 0;
                fi;""".format(tmpdir)

            op_id = map(dont_track=True, in_="//tmp/t1", out="//tmp/t2", command=command,
                        spec={"max_failed_job_count": 1, "job_count": 110})
            with pytest.raises(YtError):
                track_op(op_id)

            # The default number of stderr is 100. We check that we have 101-st stderr of failed job,
            # that is last one.
            self._check_all_stderrs(op_id, "stderr\n", 101)
        finally:
            try:
                os.rmdir(tmpdir)
            except:
                pass

    def test_job_progress(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"} for i in xrange(10)])

        tmpdir = tempfile.mkdtemp(prefix="job_progress")
        keeper_filename = os.path.join(tmpdir, "keep")

        try:
            with open(keeper_filename, "w") as f:
                f.close()

            op_id = map(dont_track=True, in_="//tmp/t1", out="//tmp/t2", command="""
                DIR={0}
                until rmdir $DIR 2>/dev/null; do sleep 1; done;
                cat
                """.format(tmpdir))

            while True:
                try:
                    job_id, _1 = get("//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op_id)).popitem()
                    time.sleep(0.1)
                except KeyError:
                    pass
                else:
                    break

            progress = get("//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs/{1}/progress".format(op_id, job_id))
            assert progress >= 0
            os.unlink(keeper_filename)

            track_op(op_id)
        finally:
            try:
                os.unlink(keeper_filename)
            except OSError:
                pass
            try:
                os.unlink(tmpdir)
            except OSError:
                pass

    def test_estimated_statistics(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"key" : i} for i in xrange(5)])

        sort(in_="//tmp/t1", out="//tmp/t1", sort_by="key")
        op_id = map(command="cat", in_="//tmp/t1[:1]", out="//tmp/t2")

        statistics = get("//sys/operations/{0}/@progress/estimated_input_statistics".format(op_id))
        for key in ["chunk_count", "uncompressed_data_size", "compressed_data_size", "row_count", "unavailable_chunk_count"]:
            assert key in statistics
        assert statistics["chunk_count"] == 1

    def test_input_row_count(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"key" : i} for i in xrange(5)])

        sort(in_="//tmp/t1", out="//tmp/t1", sort_by="key")
        op_id = map(command="cat", in_="//tmp/t1[:1]", out="//tmp/t2")

        assert get("//tmp/t2/@row_count") == 1

        row_count = get("//sys/operations/{0}/@progress/job_statistics/data/input/row_count/$/completed/map/sum".format(op_id))
        assert row_count == 1

    def test_multiple_output_row_count(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")
        write_table("//tmp/t1", [{"key" : i} for i in xrange(5)])

        op_id = map(command="cat; echo {hello=world} >&4", in_="//tmp/t1", out=["//tmp/t2", "//tmp/t3"])
        assert get("//tmp/t2/@row_count") == 5
        row_count = get("//sys/operations/{0}/@progress/job_statistics/data/output/0/row_count/$/completed/map/sum".format(op_id))
        assert row_count == 5
        row_count = get("//sys/operations/{0}/@progress/job_statistics/data/output/1/row_count/$/completed/map/sum".format(op_id))
        assert row_count == 1


    def test_invalid_output_record(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"key": "foo", "value": "ninja"})

        command = """awk '($1=="foo"){print "bar"}'"""

        with pytest.raises(YtError):
            map(command=command,
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={"mapper": {"format": "yamr"}})

    @linux_only
    def test_fail_context(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        command = 'python -c "import os; os.read(0, 1);"'

        op_id = map(dont_track=True, in_="//tmp/t1", out="//tmp/t2", command=command,
                spec={ "mapper": { "input_format" : "dsv", "check_input_fully_consumed": True}})
        # if all jobs failed then operation is also failed
        with pytest.raises(YtError): track_op(op_id)

        jobs_path = "//sys/operations/" + op_id + "/jobs"
        for job_id in ls(jobs_path):
            assert len(read_file(jobs_path + "/" + job_id + "/fail_context")) > 0

    def test_dump_job_context(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        tmpdir = tempfile.mkdtemp(prefix="dump_job_context_semaphore")

        command="touch {0}/started; cat; until rmdir {0} 2>/dev/null; do sleep 1; done".format(tmpdir)

        op_id = map(
            dont_track=True,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=command,
            spec={
                "mapper": {
                    "input_format": "json",
                    "output_format": "json"
                }
            })

        pin_filename = os.path.join(tmpdir, "started")
        while not os.access(pin_filename, os.F_OK):
            time.sleep(0.2)

        try:
            jobs_path = "//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs".format(op_id)
            jobs = ls(jobs_path)
            assert jobs
            for job_id in jobs:
                dump_job_context(job_id, "//tmp/input_context")

        finally:
            os.unlink(pin_filename)

        track_op(op_id)

        context = read_file("//tmp/input_context")
        assert get("//tmp/input_context/@description/type") == "input_context"
        assert format.JsonFormat(process_table_index=True).loads_row(context)["foo"] == "bar"

    @linux_only
    def test_sorted_output(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in xrange(2):
            write_table("<append=true>//tmp/t1", {"key": "foo", "value": "ninja"})

        command = """cat >/dev/null; k1="$YT_JOB_INDEX"0; k2="$YT_JOB_INDEX"1; echo "{key=$k1; value=one}; {key=$k2; value=two}" """

        map(in_="//tmp/t1",
            out="<sorted_by=[key];append=true>//tmp/t2",
            command=command,
            spec={"job_count": 2})

        assert get("//tmp/t2/@sorted")
        assert get("//tmp/t2/@sorted_by") == ["key"]
        assert read_table("//tmp/t2") == [{"key":0 , "value":"one"}, {"key":1, "value":"two"}, {"key":10, "value":"one"}, {"key":11, "value":"two"}]

    def test_sorted_output_overlap(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in xrange(2):
            write_table("<append=true>//tmp/t1", {"key": "foo", "value": "ninja"})

        command = 'cat >/dev/null; echo "{key=1; value=one}; {key=2; value=two}"'

        with pytest.raises(YtError):
            map(
                in_="//tmp/t1",
                out="<sorted_by=[key]>//tmp/t2",
                command=command,
                spec={"job_count": 2})

    def test_sorted_output_job_failure(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in xrange(2):
            write_table("<append=true>//tmp/t1", {"key": "foo", "value": "ninja"})

        command = "cat >/dev/null; echo {key=2; value=one}; {key=1; value=two}"

        with pytest.raises(YtError):
            map(
                in_="//tmp/t1",
                out="<sorted_by=[key]>//tmp/t2",
                command=command,
                spec={"job_count": 2})

    @linux_only
    def test_job_count(self):
        create("table", "//tmp/t1")
        for i in xrange(5):
            write_table("<append=true>//tmp/t1", {"foo": "bar"})

        command = "cat > /dev/null; echo {hello=world}"

        def check(table_name, job_count, expected_num_records):
            create("table", table_name)
            map(in_="//tmp/t1",
                out=table_name,
                command=command,
                spec={"job_count": job_count})
            assert read_table(table_name) == [{"hello": "world"} for i in xrange(expected_num_records)]

        check("//tmp/t2", 3, 3)
        check("//tmp/t3", 10, 5) # number of jobs can"t be more that number of chunks

    @linux_only
    def test_with_user_files(self):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        file1 = "//tmp/some_file.txt"
        file2 = "//tmp/renamed_file.txt"

        create("file", file1)
        create("file", file2)

        write_file(file1, "{value=42};\n")
        write_file(file2, "{a=b};\n")

        create("table", "//tmp/table_file")
        write_table("//tmp/table_file", {"text": "info"})

        command= "cat > /dev/null; cat some_file.txt; cat my_file.txt; cat table_file;"

        map(in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=[file1, "<file_name=my_file.txt>" + file2, "<format=yson>//tmp/table_file"])

        assert read_table("//tmp/output") == [{"value": 42}, {"a": "b"}, {"text": "info"}]

    @linux_only
    def test_empty_user_files(self):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        file1 = "//tmp/empty_file.txt"
        create("file", file1)

        table_file = "//tmp/table_file"
        create("table", table_file)

        command= "cat > /dev/null; cat empty_file.txt; cat table_file"

        map(in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=[file1, "<format=yamr>" + table_file])

        assert read_table("//tmp/output") == []

    @linux_only
    def test_multi_chunk_user_files(self):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        file1 = "//tmp/regular_file"
        create("file", file1)
        write_file(file1, "{value=42};\n")
        set(file1 + "/@compression_codec", "lz4")
        write_file("<append=true>" + file1, "{a=b};\n")

        table_file = "//tmp/table_file"
        create("table", table_file)
        write_table(table_file, {"text": "info"})
        set(table_file + "/@compression_codec", "snappy")
        write_table("<append=true>" + table_file, {"text": "info"})

        command= "cat > /dev/null; cat regular_file; cat table_file"

        map(in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=[file1, "<format=yson>" + table_file])

        assert read_table("//tmp/output") == [{"value": 42}, {"a": "b"}, {"text": "info"}, {"text": "info"}]

    @pytest.mark.xfail(run = True, reason = "No support for erasure chunks in user files")
    def test_erasure_user_files(self):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        file1 = "//tmp/regular_file"
        create("file", file1)
        set(file1 + "/@erasure_codec", "lrc_12_2_2")
        write_file(file1, "{value=42};\n")
        write_file(file1, "{a=b};\n")

        table_file = "//tmp/table_file"
        create("table", table_file)
        set(table_file + "/@erasure_codec", "reed_solomon_6_3")
        for i in xrange(2):
            write_table(table_file, {"text": "info"})

        command= "cat > /dev/null; cat regular_file; cat table_file"

        map(in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=[file1, "<format=yson>" + table_file])

        assert read_table("//tmp/output") == [{"value": 42}, {"a": "b"}, {"text": "info"}, {"text": "info"}]

    @linux_only
    def run_many_output_tables(self, yamr_mode=False):
        output_tables = ["//tmp/t%d" % i for i in range(3)]

        create("table", "//tmp/t_in")
        for table_path in output_tables:
            create("table", table_path)

        write_table("//tmp/t_in", {"a": "b"})

        if yamr_mode:
            mapper = "cat  > /dev/null; echo {v = 0} >&3; echo {v = 1} >&4; echo {v = 2} >&5"
        else:
            mapper = "cat  > /dev/null; echo {v = 0} >&1; echo {v = 1} >&4; echo {v = 2} >&7"

        create("file", "//tmp/mapper.sh")
        write_file("//tmp/mapper.sh", mapper)

        map(in_="//tmp/t_in",
            out=output_tables,
            command="bash mapper.sh",
            file="//tmp/mapper.sh",
            spec={"mapper": {"use_yamr_descriptors" : yamr_mode}})

        assert read_table(output_tables[0]) == [{"v": 0}]
        assert read_table(output_tables[1]) == [{"v": 1}]
        assert read_table(output_tables[2]) == [{"v": 2}]

    @linux_only
    def test_many_output_yt(self):
        self.run_many_output_tables()

    @linux_only
    def test_many_output_yamr(self):
        self.run_many_output_tables(True)

    @linux_only
    def test_output_tables_switch(self):
        output_tables = ["//tmp/t%d" % i for i in range(3)]

        create("table", "//tmp/t_in")
        for table_path in output_tables:
            create("table", table_path)

        write_table("//tmp/t_in", {"a": "b"})
        mapper = 'cat  > /dev/null; echo "<table_index=2>#;{v = 0};{v = 1};<table_index=0>#;{v = 2}"'

        create("file", "//tmp/mapper.sh")
        write_file("//tmp/mapper.sh", mapper)

        map(in_="//tmp/t_in",
            out=output_tables,
            command="bash mapper.sh",
            file="//tmp/mapper.sh")

        assert read_table(output_tables[0]) == [{"v": 2}]
        assert read_table(output_tables[1]) == []
        assert read_table(output_tables[2]) == [{"v": 0}, {"v": 1}]

    @linux_only
    def test_tskv_input_format(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n').split('\\t')
assert input == ['tskv', 'foo=bar']
print '{hello=world}'

"""
        create("file", "//tmp/mapper.sh")
        write_file("//tmp/mapper.sh", mapper)

        create("table", "//tmp/t_out")
        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python mapper.sh",
            file="//tmp/mapper.sh",
            spec={"mapper": {"input_format": yson.loads("<line_prefix=tskv>dsv")}})

        assert read_table("//tmp/t_out") == [{"hello": "world"}]

    @linux_only
    def test_tskv_output_format(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n')
assert input == '<"table_index"=0;>#;'
input = sys.stdin.readline().strip('\\n')
assert input == '{"foo"="bar";};'
print "tskv" + "\\t" + "hello=world"
"""
        create("file", "//tmp/mapper.sh")
        write_file("//tmp/mapper.sh", mapper)

        create("table", "//tmp/t_out")
        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python mapper.sh",
            file="//tmp/mapper.sh",
            spec={"mapper": {
                    "enable_input_table_index": True,
                    "input_format": yson.loads("<format=text>yson"),
                    "output_format": yson.loads("<line_prefix=tskv>dsv")
                }})

        assert read_table("//tmp/t_out") == [{"hello": "world"}]

    @linux_only
    def test_yamr_output_format(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n')
assert input == '{"foo"="bar";};'
print "key\\tsubkey\\tvalue"

"""
        create("file", "//tmp/mapper.sh")
        write_file("//tmp/mapper.sh", mapper)

        create("table", "//tmp/t_out")
        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python mapper.sh",
            file="//tmp/mapper.sh",
            spec={"mapper": {
                    "input_format": yson.loads("<format=text>yson"),
                    "output_format": yson.loads("<has_subkey=true>yamr")
                }})

        assert read_table("//tmp/t_out") == [{"key": "key", "subkey": "subkey", "value": "value"}]

    @linux_only
    def test_yamr_input_format(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"value": "value", "subkey": "subkey", "key": "key", "a": "another"})

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n').split('\\t')
assert input == ['key', 'subkey', 'value']
print '{hello=world}'

"""
        create("file", "//tmp/mapper.sh")
        write_file("//tmp/mapper.sh", mapper)

        create("table", "//tmp/t_out")
        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python mapper.sh",
            file="//tmp/mapper.sh",
            spec={"mapper": {"input_format": yson.loads("<has_subkey=true>yamr")}})

        assert read_table("//tmp/t_out") == [{"hello": "world"}]

    @linux_only
    def test_executable_mapper(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        mapper =  \
"""
#!/bin/sh
cat > /dev/null; echo {hello=world}
"""

        create("file", "//tmp/mapper.sh")
        write_file("//tmp/mapper.sh", mapper)

        set("//tmp/mapper.sh/@executable", True)

        create("table", "//tmp/t_out")
        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="./mapper.sh",
            file="//tmp/mapper.sh")

        assert read_table("//tmp/t_out") == [{"hello": "world"}]

    def test_abort_op(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"foo": "bar"})

        op_id = map(dont_track=True,
            in_="//tmp/t",
            out="//tmp/t",
            command="sleep 1")

        path = "//sys/operations/%s/@state" % op_id
        # check running
        abort_op(op_id)
        assert get(path) == "aborted"


    @linux_only
    def test_table_index(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/out")

        write_table("//tmp/t1", {"key": "a", "value": "value"})
        write_table("//tmp/t2", {"key": "b", "value": "value"})

        mapper = \
"""
import sys
table_index = sys.stdin.readline().strip()
row = sys.stdin.readline().strip()
print row + table_index

table_index = sys.stdin.readline().strip()
row = sys.stdin.readline().strip()
print row + table_index
"""

        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        map(in_=["//tmp/t1", "//tmp/t2"],
            out="//tmp/out",
            command="python mapper.py",
            file="//tmp/mapper.py",
            spec={"mapper": {"format": yson.loads("<enable_table_index=true>yamr")}})

        expected = [{"key": "a", "value": "value0"},
                    {"key": "b", "value": "value1"}]
        self.assertItemsEqual(read_table("//tmp/out"), expected)

    def test_insane_demand(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", {"cool": "stuff"})

        with pytest.raises(YtError):
            map(in_="//tmp/t_in", out="//tmp/t_out", command="cat",
                spec={"mapper": {"memory_limit": 1000000000000}})

    def test_check_input_fully_consumed(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        command = 'python -c "import os; os.read(0, 5);"'

        op_id = map(dont_track=True, in_="//tmp/t1", out="//tmp/t2", command=command,
                spec={ "mapper": { "input_format" : "dsv", "check_input_fully_consumed": True}})
        # if all jobs failed then operation is also failed
        with pytest.raises(YtError): track_op(op_id)

        assert read_table("//tmp/t2") == []

    def test_check_input_not_fully_consumed(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        data = [{"foo": "bar"} for i in xrange(10000)]
        write_table("//tmp/t1", data)

        map(in_="//tmp/t1", out="//tmp/t2", command="head -1",
            spec={"mapper": {"input_format" : "dsv", "output_format" : "dsv"}})

        assert read_table("//tmp/t2") == [{"foo": "bar"}] 

    def test_live_preview(self):
        create_user("u")

        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"foo": "bar"})

        create("table", "//tmp/t2")
        set("//tmp/t2/@acl", [{"action": "allow", "subjects": ["u"], "permissions": ["write"]}])
        effective_acl = get("//tmp/t2/@effective_acl")

        op_id = map(dont_track=True, command="cat; sleep 1", in_="//tmp/t1", out="//tmp/t2")

        time.sleep(0.5)
        assert exists("//sys/operations/{0}/output_0".format(op_id))
        assert effective_acl == get("//sys/operations/{0}/output_0/@acl".format(op_id))

        track_op(op_id)
        assert read_table("//tmp/t2") == [{"foo": "bar"}]

    def test_row_sampling(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")

        count = 1000
        original_data = [{"index": i} for i in xrange(count)]
        write_table("//tmp/t1", original_data)

        command = "cat"
        sampling_rate = 0.5
        spec = {"job_io": {"table_reader": {"sampling_seed": 42, "sampling_rate": sampling_rate}}}

        map(in_="//tmp/t1", out="//tmp/t2", command=command, spec=spec)
        map(in_="//tmp/t1", out="//tmp/t3", command=command, spec=spec)

        new_data_t2 = read_table("//tmp/t2", verbose=False)
        new_data_t3 = read_table("//tmp/t3", verbose=False)

        assert sorted(row.items() for row in new_data_t2) == sorted(row.items() for row in new_data_t3)

        actual_rate = len(new_data_t2) * 1.0 / len(original_data)
        variation = sampling_rate * (1 - sampling_rate)
        assert sampling_rate - variation <= actual_rate <= sampling_rate + variation

@linux_only
class TestJobQuery(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler" : {
            "udf_registry_path" : "//tmp/udfs"
        }
    }

    def _init_udf_registry(self):
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

        local_bitcode_path = find_executable("test_udfs.bc")
        write_local_file(abs_path, local_bitcode_path)

    def test_query_simple(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        map(in_="//tmp/t1", out="//tmp/t2", command="cat",
            spec={"input_query": "a", "input_schema": [{"name": "a", "type": "string"}]})

        assert read_table("//tmp/t2") == [{"a": "b"}]

    def test_query_reader_projection(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b", "c": "d"})

        map(in_="//tmp/t1", out="//tmp/t2", command="cat",
            spec={"input_query": "a", "input_schema": [{"name": "a", "type": "string"}]})

        assert read_table("//tmp/t2") == [{"a": "b"}]

    def test_query_with_condition(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": i} for i in xrange(2)])

        map(in_="//tmp/t1", out="//tmp/t2", command="cat",
            spec={"input_query": "a where a > 0", "input_schema": [{"name": "a", "type": "int64"}]})

        assert read_table("//tmp/t2") == [{"a": 1}]

    def test_query_asterisk(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        rows = [
            {"a": 1, "b": 2, "c": 3},
            {"b": 5, "c": 6},
            {"a": 7, "c": 8}]
        write_table("//tmp/t1", rows)

        yamred_format = yson.to_yson_type("yamred_dsv", attributes={"has_subkey": False, "key_column_names": ["a", "b"]})
        map(in_="//tmp/t1", out="//tmp/t2", command="cat",
            spec={
                "input_query": "* where a > 0 or b > 0",
                "input_schema": [
                    {"name": "z", "type": "int64"},
                    {"name": "a", "type": "int64"},
                    {"name": "y", "type": "int64"},
                    {"name": "b", "type": "int64"},
                    {"name": "x", "type": "int64"},
                    {"name": "c", "type": "int64"},
                    {"name": "u", "type": "int64"}]})

        self.assertItemsEqual(read_table("//tmp/t2"), rows)

    def test_query_udf(self):
        self._init_udf_registry()

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": i} for i in xrange(-1,1)])

        map(in_="//tmp/t1", out="//tmp/t2", command="cat",
            spec={"input_query": "a where abs_udf(a) > 0", "input_schema": [{"name": "a", "type": "int64"}]})

        assert read_table("//tmp/t2") == [{"a": -1}]

    def test_ordered_map_many_jobs(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        original_data = [{"index": i} for i in xrange(10)]
        for row in original_data:
            write_table("<append=true>//tmp/t_input", row)

        command = "cat; echo stderr 1>&2"
        op_id = map(dont_track=True, in_="//tmp/t_input", out="//tmp/t_output", command=command,
                spec={"ordered": True, "data_size_per_job": 1})

        track_op(op_id)
        jobs = get("//sys/operations/" + op_id + "/jobs/@count")

        assert get("//sys/operations/" + op_id + "/jobs/@count") == 10
        assert read_table("//tmp/t_output") == original_data

    def test_ordered_map_remains_sorted(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        original_data = [{"key": i} for i in xrange(1000)]
        for i in xrange(10):
            write_table("<append=true>//tmp/t_input", original_data[100*i:100*(i+1)])

        command = "cat; echo stderr 1>&2"
        sorted_out = "<sorted_by=[key]>//tmp/t_output"
        op_id = map(dont_track=True, in_="//tmp/t_input", out=sorted_out, command=command,
                spec={"ordered": True, "job_count": 5})

        track_op(op_id)
        jobs = get("//sys/operations/" + op_id + "/jobs/@count")

        assert jobs == 5
        assert get("//tmp/t_output/@sorted")
        assert get("//tmp/t_output/@sorted_by") == ["key"]
        assert read_table("//tmp/t_output") == original_data

##################################################################

@mark_multicell
class TestSchedulerMapCommandsMulticell(TestSchedulerMapCommands):
    NUM_SECONDARY_MASTER_CELLS = 2
