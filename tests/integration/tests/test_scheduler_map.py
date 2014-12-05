import pytest
import time
import __builtin__
import os
import tempfile
import subprocess

from yt_env_setup import YTEnvSetup
from yt_commands import *


##################################################################

def can_perform_block_io_tests():
    try:
        subprocess.check_call(["ls", "-l", "/dev/sda"])
        return subprocess.check_output(["sudo", "-n", "-l", "dd"]).strip() == "/bin/dd"
    except AttributeError:
        # python 2.6 subprocess module does not have check_output function
        return False
    except subprocess.CalledProcessError:
        return False


block_io_mark = pytest.mark.skipif("not can_perform_block_io_tests()")


class TestWoodpecker(YTEnvSetup):
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
            "force_enable_accounting" : True,
            "iops_threshold" : 5,
            "block_io_watchdog_period" : 8000
        }
    }

    FAIL_IF_HIT_LIMIT="""
sleep 10

CURRENT_BLKIO_CGROUP=/sys/fs/cgroup/blkio`grep blkio /proc/self/cgroup | cut -d: -f 3`
echo Current blkio cgroup: $CURRENT_BLKIO_CGROUP >&2

echo "blkio.io_serviced content:" >&2
cat $CURRENT_BLKIO_CGROUP/blkio.io_serviced >&2
echo '===' >&2

echo "blkio.throttle.read_iops_device content:" >&2
CONTENT=`cat $CURRENT_BLKIO_CGROUP/blkio.throttle.read_iops_device`
echo $CONTENT >&2
echo $CONTENT | grep ' 5' 1>/dev/null
"""
    def _get_stderr(self, op_id):
        jobs_path = "//sys/operations/" + op_id + "/jobs"
        for job_id in ls(jobs_path):
            return download(jobs_path + "/" + job_id + "/stderr")

    @block_io_mark
    def test_hitlimit(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write("//tmp/t1", [{"a": "b"}])
        command="""cat
sudo -n dd if=/dev/sda of=/dev/null bs=16K count=100 iflag=direct 1>/dev/null
"""
        command += self.FAIL_IF_HIT_LIMIT
        op_id = map(dont_track=True, in_="//tmp/t1", out="//tmp/t2", command=command, spec={"max_failed_job_count": 1})

        track_op(op_id)
        print self._get_stderr(op_id)

    @block_io_mark
    def test_do_not_hitlimit(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write("//tmp/t1", [{"a": "b"}])
        command="""
cat
sudo -n dd if=/dev/sda of=/dev/null bs=1600K count=1 iflag=direct 1>/dev/null
"""
        command += self.FAIL_IF_HIT_LIMIT
        op_id = map(dont_track=True, in_="//tmp/t1", out="//tmp/t2", command=command, spec={"max_failed_job_count": 1})

        with pytest.raises(YtError):
            try:
                track_op(op_id)
            finally:
                print self._get_stderr(op_id)


class TestCGroups(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "exec_agent" : {
            "force_enable_accounting" : True,
            "enable_cgroup_memory_hierarchy" : True,
            "slot_manager" : {
                "enforce_job_control" : True,
                "enable_cgroups" : True
            }
        }
    }

    def test_failed_jobs_twice(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write("//tmp/t1", [{"foo": "bar"} for i in xrange(200)])
        op_id = map(dont_track=True, in_="//tmp/t1", out="//tmp/t2", command='trap "" HUP; bash -c "sleep 60" &; sleep $[( $RANDOM % 5 )]s; exit 42;',
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
            "force_enable_accounting" : True,
            "enable_cgroup_memory_hierarchy" : True,
            "slot_manager" : {
                "enforce_job_control" : True
            }
        }
    }

    def test_scheduler_event_log(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write("//tmp/t1", [{"a": "b"}])
        op_id = map(in_="//tmp/t1", out="//tmp/t2", command='cat; bash -c "for (( I=0 ; I<=100*1000 ; I++ )) ; do echo $(( I+I*I )); done; sleep 2" >/dev/null')

        statistics = get("//sys/operations/{0}/@progress/statistics".format(op_id))
        assert statistics["user_job"]["builtin"]["cpu"]["user"]["sum"] > 0
        assert statistics["user_job"]["builtin"]["block_io"]["bytes_read"]["sum"] is not None
        assert statistics["user_job"]["builtin"]["memory"]["rss"]["count"] > 0
        assert statistics["job_proxy"]["cpu"]["user"]["count"] == 1

        # wait for scheduler to dump the event log
        time.sleep(6)
        res = read("//sys/scheduler/event_log")
        event_types = __builtin__.set()
        for item in res:
            event_types.add(item["event_type"])
            if item["event_type"] == "job_completed":
                stats = item["statistics"]
                user_time = stats["user_job"]["builtin"]["cpu"]["user"]["max"]
                # our job should burn enough cpu
                assert user_time > 0
        assert "operation_started" in event_types

    @block_io_mark
    def test_block_io_accounting(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write("//tmp/t1", [{"a": "b"}])
        op_id = map(in_="//tmp/t1", out="//tmp/t2", command="cat; sudo -n dd if=/dev/sda of=/dev/null bs=160K count=50 iflag=direct 1>/dev/null;")

        # wait for scheduler to dump the event log
        time.sleep(6)
        res = read("//sys/scheduler/event_log")
        total_sectors = None
        exist = False
        job_completed_line_exist = False
        for item in res:
            if item["event_type"] == "job_completed" and item["operation_id"] == op_id:
                job_completed_line_exist = True
                stats = item["statistics"]
                bytes_read = stats["user_job"]["builtin"]["block_io"]["bytes_read"]["max"]
                io_read = stats["user_job"]["builtin"]["block_io"]["io_read"]["max"]
        assert job_completed_line_exist
        assert bytes_read == 160*1024*50
        assert io_read == 50


class TestUserStatistics(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "exec_agent" : {
            "force_enable_accounting" : True
        }
    }

    def test_job_statistics(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write("//tmp/t1", {"a": "b"})
        op_id = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command=(
                r'cat; python -c "'
                r'import os; '
                r'os.write(5, \"{ cpu={ k1=4; k3=7 }}; {k2=-7};{k2=1};\"); '
                r'os.close(5);"'))

        statistics = get("//sys/operations/{0}/@progress/statistics".format(op_id))
        assert statistics["user_job"]["custom"]["cpu"]["k1"]["max"] == 4
        assert statistics["user_job"]["custom"]["k2"]["count"] == 2

    def test_multiple_job_statistics(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write("//tmp/t1", [{"a": "b"} for i in range(2)])

        op_id = map(in_="//tmp/t1", out="//tmp/t2", command="cat", spec={"job_count": 2})
        statistics = get("//sys/operations/{0}/@progress/statistics".format(op_id))
        assert statistics["user_job"]["builtin"]["cpu"]["user"]["count"] == 2

    def test_job_statistics_progress(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write("//tmp/t1", [{"a": "b"} for i in xrange(2)])

        to_delete = []
        tmpdir = tempfile.mkdtemp(prefix="job_statistics_progress")
        to_delete.append(tmpdir)

        for i in range(2):
            path = os.path.join(tmpdir, str(i))
            os.mkdir(path)
            to_delete.append(path)
            if i == 0:
                keeper_filename = os.path.join(path, "keep")
                with open(keeper_filename, "w") as f:
                    f.close()
                to_delete.append(keeper_filename)

        command = '''cat > /dev/null;
            DIR={0}
            if [ "$YT_START_ROW_INDEX" = "0" ]; then
              cat $DIR/$YT_START_ROW_INDEX/keep 1>&2
              until rmdir $DIR/$YT_START_ROW_INDEX 2>/dev/null; do sleep 1; done;
            fi
            exit 0;
            '''.format(tmpdir)

        try:
            op_id = map(dont_track=True, in_="//tmp/t1", out="//tmp/t2", command=command,
                        spec={"max_failed_job_count": 1, "job_count": 2})

            tries = 0
            statistics = {}

            while not statistics:
                time.sleep(1)
                tries += 1
                print get("//sys/operations/{0}/jobs/@count".format(op_id))
                statistics = get("//sys/operations/{0}/@progress/statistics".format(op_id))
                if tries > 10:
                    break

            assert statistics["user_job"]["builtin"]["cpu"]["user"]["count"] == 1

            os.unlink(keeper_filename)
            track_op(op_id)

            statistics = get("//sys/operations/{0}/@progress/statistics".format(op_id))
            assert statistics["user_job"]["builtin"]["cpu"]["user"]["count"] == 2
        finally:
            to_delete.reverse()
            for filename in to_delete:
                try:
                    os.unlink(filename)
                except:
                    pass


class TestSchedulerMapCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    def test_empty_table(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        map(in_="//tmp/t1", out="//tmp/t2", command="cat")

        assert read("//tmp/t2") == []

    @only_linux
    def test_one_chunk(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write("//tmp/t1", {"a": "b"})
        op_id = map(dont_track=True,
            in_="//tmp/t1", out="//tmp/t2", command=r'cat; echo "{v1=\"$V1\"};{v2=\"$V2\"}"',
            spec={"mapper": {"environment": {"V1": "Some data", "V2": "$(SandboxPath)/mytmp"}},
                  "title": "MyTitle"})

        get("//sys/operations/%s/@spec" % op_id)
        track_op(op_id)

        res = read("//tmp/t2")
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
        write("//tmp/t1", original_data)

        command = "cat"
        map(in_="//tmp/t1", out="//tmp/t2", command=command)

        new_data = read("//tmp/t2", verbose=False)
        assert sorted(row.items() for row in new_data) == [[("index", i)] for i in xrange(count)]

    def test_two_inputs_at_the_same_time(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output1")
        create("table", "//tmp/t_output2")

        count = 1000
        original_data = [{"index": i} for i in xrange(count)]
        write("//tmp/t_input", original_data)

        file = "//tmp/some_file.txt"
        create("file", file)
        upload(file, "{value=42};\n")

        command = 'bash -c "cat <&0 & sleep 0.1; cat some_file.txt >&4; wait;"'
        map(in_="//tmp/t_input",
            out=["//tmp/t_output1", "//tmp/t_output2"],
            command=command,
            file=[file],
            verbose=True)

        assert read("//tmp/t_output2") == [{"value": 42}]
        assert sorted([row.items() for row in read("//tmp/t_output1")]) == [[("index", i)] for i in xrange(count)]

    def test_first_after_second(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output1")
        create("table", "//tmp/t_output2")

        count = 10000
        original_data = [{"index": i} for i in xrange(count)]
        write("//tmp/t_input", original_data)

        file1 = "//tmp/some_file.txt"
        create("file", file1)
        upload(file1, "}}}}};\n")

        command = 'cat some_file.txt >&4; cat >&4; echo "{value=42}"'
        op_id = map(dont_track=True,
                    in_="//tmp/t_input",
                    out=["//tmp/t_output1", "//tmp/t_output2"],
                    command=command,
                    file=[file1],
                    verbose=True)
        with pytest.raises(YtError):
            track_op(op_id)

    @only_linux
    def test_in_equal_to_out(self):
        create("table", "//tmp/t1")
        write("//tmp/t1", {"foo": "bar"})

        map(in_="//tmp/t1", out="<append=true>//tmp/t1", command="cat")

        assert read("//tmp/t1") == [{"foo": "bar"}, {"foo": "bar"}]

    #TODO(panin): refactor
    def _check_all_stderrs(self, op_id, expected_content, expected_count):
        jobs_path = "//sys/operations/" + op_id + "/jobs"
        assert get(jobs_path + "/@count") == expected_count
        for job_id in ls(jobs_path):
            assert download(jobs_path + "/" + job_id + "/stderr") == expected_content

    # check that stderr is captured for successfull job
    @only_linux
    def test_stderr_ok(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write("//tmp/t1", {"foo": "bar"})

        command = """cat > /dev/null; echo stderr 1>&2; echo {operation='"'$YT_OPERATION_ID'"'}';'; echo {job_index=$YT_JOB_INDEX};"""

        op_id = map(dont_track=True, in_="//tmp/t1", out="//tmp/t2", command=command)
        track_op(op_id)

        assert read("//tmp/t2") == [{"operation" : op_id}, {"job_index" : 0}]
        self._check_all_stderrs(op_id, "stderr\n", 1)

    # check that stderr is captured for failed jobs
    @only_linux
    def test_stderr_failed(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write("//tmp/t1", {"foo": "bar"})

        command = """echo "{x=y}{v=};{a=b}"; while echo xxx 2>/dev/null; do false; done; echo stderr 1>&2; cat > /dev/null;"""

        op_id = map(dont_track=True, in_="//tmp/t1", out="//tmp/t2", command=command)
        # if all jobs failed then operation is also failed
        with pytest.raises(YtError):
            track_op(op_id)

        self._check_all_stderrs(op_id, "stderr\n", 10)

    # check max_stderr_count
    @only_linux
    def test_stderr_limit(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write("//tmp/t1", {"foo": "bar"})

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
        write("//tmp/t1", [{"foo": "bar"} for i in xrange(110)])

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
        write("//tmp/t1", [{"foo": "bar"} for i in xrange(10)])

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

    def test_invalid_output_record(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write("//tmp/t1", {"key": "foo", "value": "ninja"})

        command = """awk '($1=="foo"){print "bar"}'"""

        with pytest.raises(YtError):
            map(command=command,
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={"mapper": {"format": "yamr"}})

    @only_linux
    def test_fail_context(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write("//tmp/t1", {"foo": "bar"})

        command = 'python -c "import os; os.read(0, 1);"'

        op_id = map(dont_track=True, in_="//tmp/t1", out="//tmp/t2", command=command,
                spec={ "mapper": { "input_format" : "dsv", "check_input_fully_consumed": True}})
        # if all jobs failed then operation is also failed
        with pytest.raises(YtError): track_op(op_id)

        jobs_path = "//sys/operations/" + op_id + "/jobs"
        for job_id in ls(jobs_path):
            assert len(download(jobs_path + "/" + job_id + "/fail_contexts/0")) > 0

    @only_linux
    def test_sorted_output(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in xrange(2):
            write("<append=true>//tmp/t1", {"key": "foo", "value": "ninja"})

        command = """cat >/dev/null; k1="$YT_JOB_INDEX"0; k2="$YT_JOB_INDEX"1; echo "{key=$k1; value=one}; {key=$k2; value=two}" """

        map(in_="//tmp/t1",
            out="<sorted_by=[key];append=true>//tmp/t2",
            command=command,
            spec={"job_count": 2})

        assert get("//tmp/t2/@sorted")
        assert get("//tmp/t2/@sorted_by") == ["key"]
        assert read("//tmp/t2") == [{"key":0 , "value":"one"}, {"key":1, "value":"two"}, {"key":10, "value":"one"}, {"key":11, "value":"two"}]

    def test_sorted_output_overlap(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in xrange(2):
            write("<append=true>//tmp/t1", {"key": "foo", "value": "ninja"})

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
            write("<append=true>//tmp/t1", {"key": "foo", "value": "ninja"})

        command = "cat >/dev/null; echo {key=2; value=one}; {key=1; value=two}"

        with pytest.raises(YtError):
            map(
                in_="//tmp/t1",
                out="<sorted_by=[key]>//tmp/t2",
                command=command,
                spec={"job_count": 2})

    @only_linux
    def test_job_count(self):
        create("table", "//tmp/t1")
        for i in xrange(5):
            write("<append=true>//tmp/t1", {"foo": "bar"})

        command = "cat > /dev/null; echo {hello=world}"

        def check(table_name, job_count, expected_num_records):
            create("table", table_name)
            map(in_="//tmp/t1",
                out=table_name,
                command=command,
                spec={"job_count": job_count})
            assert read(table_name) == [{"hello": "world"} for i in xrange(expected_num_records)]

        check("//tmp/t2", 3, 3)
        check("//tmp/t3", 10, 5) # number of jobs can"t be more that number of chunks

    @only_linux
    def test_with_user_files(self):
        create("table", "//tmp/input")
        write("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        file1 = "//tmp/some_file.txt"
        file2 = "//tmp/renamed_file.txt"

        create("file", file1)
        create("file", file2)

        upload(file1, "{value=42};\n")
        upload(file2, "{a=b};\n")

        create("table", "//tmp/table_file")
        write("//tmp/table_file", {"text": "info"})

        command= "cat > /dev/null; cat some_file.txt; cat my_file.txt; cat table_file;"

        map(in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=[file1, "<file_name=my_file.txt>" + file2, "<format=yson>//tmp/table_file"])

        assert read("//tmp/output") == [{"value": 42}, {"a": "b"}, {"text": "info"}]

    @only_linux
    def test_empty_user_files(self):
        create("table", "//tmp/input")
        write("//tmp/input", {"foo": "bar"})

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

        assert read("//tmp/output") == []

    @only_linux
    def test_multi_chunk_user_files(self):
        create("table", "//tmp/input")
        write("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        file1 = "//tmp/regular_file"
        create("file", file1)
        upload(file1, "{value=42};\n")
        set(file1 + "/@compression_codec", "lz4")
        upload("<append=true>" + file1, "{a=b};\n")

        table_file = "//tmp/table_file"
        create("table", table_file)
        write(table_file, {"text": "info"})
        set(table_file + "/@compression_codec", "snappy")
        write("<append=true>" + table_file, {"text": "info"})

        command= "cat > /dev/null; cat regular_file; cat table_file"

        map(in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=[file1, "<format=yson>" + table_file])

        assert read("//tmp/output") == [{"value": 42}, {"a": "b"}, {"text": "info"}, {"text": "info"}]

    @pytest.mark.xfail(run = True, reason = "No support for erasure chunks in user files")
    def test_erasure_user_files(self):
        create("table", "//tmp/input")
        write("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        file1 = "//tmp/regular_file"
        create("file", file1)
        set(file1 + "/@erasure_codec", "lrc_12_2_2")
        upload(file1, "{value=42};\n")
        upload(file1, "{a=b};\n")

        table_file = "//tmp/table_file"
        create("table", table_file)
        set(table_file + "/@erasure_codec", "reed_solomon_6_3")
        for i in xrange(2):
            write(table_file, {"text": "info"})

        command= "cat > /dev/null; cat regular_file; cat table_file"

        map(in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=[file1, "<format=yson>" + table_file])

        assert read("//tmp/output") == [{"value": 42}, {"a": "b"}, {"text": "info"}, {"text": "info"}]

    @only_linux
    def run_many_output_tables(self, yamr_mode=False):
        output_tables = ["//tmp/t%d" % i for i in range(3)]

        create("table", "//tmp/t_in")
        for table_path in output_tables:
            create("table", table_path)

        write("//tmp/t_in", {"a": "b"})

        if yamr_mode:
            mapper = "cat  > /dev/null; echo {v = 0} >&3; echo {v = 1} >&4; echo {v = 2} >&5"
        else:
            mapper = "cat  > /dev/null; echo {v = 0} >&1; echo {v = 1} >&4; echo {v = 2} >&7"

        create("file", "//tmp/mapper.sh")
        upload("//tmp/mapper.sh", mapper)

        map(in_="//tmp/t_in",
            out=output_tables,
            command="bash mapper.sh",
            file="//tmp/mapper.sh",
            spec={"mapper": {"use_yamr_descriptors" : yamr_mode}})

        assert read(output_tables[0]) == [{"v": 0}]
        assert read(output_tables[1]) == [{"v": 1}]
        assert read(output_tables[2]) == [{"v": 2}]

    @only_linux
    def test_many_output_yt(self):
        self.run_many_output_tables()

    @only_linux
    def test_many_output_yamr(self):
        self.run_many_output_tables(True)

    @only_linux
    def test_output_tables_switch(self):
        output_tables = ["//tmp/t%d" % i for i in range(3)]

        create("table", "//tmp/t_in")
        for table_path in output_tables:
            create("table", table_path)

        write("//tmp/t_in", {"a": "b"})
        mapper = 'cat  > /dev/null; echo "<table_index=2>#;{v = 0};{v = 1};<table_index=0>#;{v = 2}"'

        create("file", "//tmp/mapper.sh")
        upload("//tmp/mapper.sh", mapper)

        map(in_="//tmp/t_in",
            out=output_tables,
            command="bash mapper.sh",
            file="//tmp/mapper.sh")

        assert read(output_tables[0]) == [{"v": 2}]
        assert read(output_tables[1]) == []
        assert read(output_tables[2]) == [{"v": 0}, {"v": 1}]

    @only_linux
    def test_tskv_input_format(self):
        create("table", "//tmp/t_in")
        write("//tmp/t_in", {"foo": "bar"})

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n').split('\\t')
assert input == ['tskv', 'foo=bar']
print '{hello=world}'

"""
        create("file", "//tmp/mapper.sh")
        upload("//tmp/mapper.sh", mapper)

        create("table", "//tmp/t_out")
        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python mapper.sh",
            file="//tmp/mapper.sh",
            spec={"mapper": {"input_format": yson.loads("<line_prefix=tskv>dsv")}})

        assert read("//tmp/t_out") == [{"hello": "world"}]

    @only_linux
    def test_tskv_output_format(self):
        create("table", "//tmp/t_in")
        write("//tmp/t_in", {"foo": "bar"})

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n')
assert input == '<"table_index"=0>#;'
input = sys.stdin.readline().strip('\\n')
assert input == '{"foo"="bar"};'
print "tskv" + "\\t" + "hello=world"
"""
        create("file", "//tmp/mapper.sh")
        upload("//tmp/mapper.sh", mapper)

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

        assert read("//tmp/t_out") == [{"hello": "world"}]

    @only_linux
    def test_yamr_output_format(self):
        create("table", "//tmp/t_in")
        write("//tmp/t_in", {"foo": "bar"})

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n')
assert input == '{"foo"="bar"};'
print "key\\tsubkey\\tvalue"

"""
        create("file", "//tmp/mapper.sh")
        upload("//tmp/mapper.sh", mapper)

        create("table", "//tmp/t_out")
        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python mapper.sh",
            file="//tmp/mapper.sh",
            spec={"mapper": {
                    "input_format": yson.loads("<format=text>yson"),
                    "output_format": yson.loads("<has_subkey=true>yamr")
                }})

        assert read("//tmp/t_out") == [{"key": "key", "subkey": "subkey", "value": "value"}]

    @only_linux
    def test_yamr_input_format(self):
        create("table", "//tmp/t_in")
        write("//tmp/t_in", {"value": "value", "subkey": "subkey", "key": "key", "a": "another"})

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n').split('\\t')
assert input == ['key', 'subkey', 'value']
print '{hello=world}'

"""
        create("file", "//tmp/mapper.sh")
        upload("//tmp/mapper.sh", mapper)

        create("table", "//tmp/t_out")
        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python mapper.sh",
            file="//tmp/mapper.sh",
            spec={"mapper": {"input_format": yson.loads("<has_subkey=true>yamr")}})

        assert read("//tmp/t_out") == [{"hello": "world"}]

    @only_linux
    def test_executable_mapper(self):
        create("table", "//tmp/t_in")
        write("//tmp/t_in", {"foo": "bar"})

        mapper =  \
"""
#!/bin/sh
cat > /dev/null; echo {hello=world}
"""

        create("file", "//tmp/mapper.sh")
        upload("//tmp/mapper.sh", mapper)

        set("//tmp/mapper.sh/@executable", True)

        create("table", "//tmp/t_out")
        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="./mapper.sh",
            file="//tmp/mapper.sh")

        assert read("//tmp/t_out") == [{"hello": "world"}]

    def test_abort_op(self):
        create("table", "//tmp/t")
        write("//tmp/t", {"foo": "bar"})

        op_id = map(dont_track=True,
            in_="//tmp/t",
            out="//tmp/t",
            command="sleep 1")

        path = "//sys/operations/%s/@state" % op_id
        # check running
        abort_op(op_id)
        assert get(path) == "aborted"


    @only_linux
    def test_table_index(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/out")

        write("//tmp/t1", {"key": "a", "value": "value"})
        write("//tmp/t2", {"key": "b", "value": "value"})

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
        upload("//tmp/mapper.py", mapper)

        map(in_=["//tmp/t1", "//tmp/t2"],
            out="//tmp/out",
            command="python mapper.py",
            file="//tmp/mapper.py",
            spec={"mapper": {"format": yson.loads("<enable_table_index=true>yamr")}})

        expected = [{"key": "a", "value": "value0"},
                    {"key": "b", "value": "value1"}]
        self.assertItemsEqual(read("//tmp/out"), expected)

    def test_insane_demand(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write("//tmp/t_in", {"cool": "stuff"})

        with pytest.raises(YtError):
            map(in_="//tmp/t_in", out="//tmp/t_out", command="cat",
                spec={"mapper": {"memory_limit": 1000000000000}})

    def test_check_input_fully_consumed(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write("//tmp/t1", {"foo": "bar"})

        command = 'python -c "import os; os.read(0, 5);"'

        op_id = map(dont_track=True, in_="//tmp/t1", out="//tmp/t2", command=command,
                spec={ "mapper": { "input_format" : "dsv", "check_input_fully_consumed": True}})
        # if all jobs failed then operation is also failed
        with pytest.raises(YtError): track_op(op_id)

        op_id = map(dont_track=True, in_="//tmp/t1", out="//tmp/t2", command=command,
                spec={ "mapper": { "input_format" : "dsv", "check_input_fully_consumed": True}})
        self.assertEqual([], read("//tmp/t2"))

    def test_live_preview(self):
        create_user("u")

        create("table", "//tmp/t1")
        write("//tmp/t1", {"foo": "bar"})

        create("table", "//tmp/t2")
        set("//tmp/t2/@acl", [{"action": "allow", "subjects": ["u"], "permissions": ["write"]}])
        effective_acl = get("//tmp/t2/@effective_acl")

        op_id = map(dont_track=True, command="cat; sleep 1", in_="//tmp/t1", out="//tmp/t2")

        time.sleep(0.5)
        assert exists("//sys/operations/{0}/output_0".format(op_id))
        assert effective_acl == get("//sys/operations/{0}/output_0/@acl".format(op_id))

        track_op(op_id)
        assert read("//tmp/t2") == [{"foo": "bar"}]

