from yt_env_setup import YTEnvSetup, unix_only, wait, require_enabled_core_dump, \
    require_ytserver_root_privileges, patch_porto_env_only, skip_if_porto
from yt_commands import *

from flaky import flaky

import binascii
import itertools
import pytest
import os
import psutil
import subprocess
import time
import threading
from multiprocessing import Queue

##################################################################

porto_delta_node_config = {
    "exec_agent": {
        "slot_manager": {
            "enforce_job_control": True,
            "job_environment" : {
                # >= 19.2
                "type" : "porto",
            },
        }
    }
}

##################################################################

def get_stderr_spec(stderr_file):
    return {
        "stderr_table_path": stderr_file,
    }


def get_stderr_dict_from_cypress(operation_id):
    jobs_path = "//sys/operations/{0}/jobs".format(operation_id)
    result = {}
    for job_id, job_content in get(jobs_path).iteritems():
        if "stderr" in job_content:
            stderr_path = "{0}/{1}/stderr".format(jobs_path, job_id)
            result[job_id] = read_file(stderr_path)
    return result


def get_stderr_dict_from_table(table_path):
    result = {}
    stderr_rows = read_table("//tmp/t_stderr")
    for job_id, part_iter in itertools.groupby(stderr_rows, key=lambda x: x["job_id"]):
        job_stderr = ""
        for row in part_iter:
            job_stderr += row["data"]
        result[job_id] = job_stderr
    return result


def compare_stderr_table_and_files(stderr_table_path, operation_id):
    assert get_stderr_dict_from_table("//tmp/t_stderr") == get_stderr_dict_from_cypress(operation_id)


def expect_to_find_in_stderr_table(stderr_table_path, content):
    assert get("{0}/@sorted".format(stderr_table_path))
    assert get("{0}/@sorted_by".format(stderr_table_path)) == ["job_id", "part_index"]
    table_row_list = list(read_table(stderr_table_path))
    assert sorted(row["data"] for row in table_row_list) == sorted(content)
    job_id_list = [row["job_id"] for row in table_row_list]
    assert sorted(job_id_list) == job_id_list


class TestStderrTable(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    NUM_SECONDARY_MASTER_CELLS = 2

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            # We want to disable premature chunk list allocataion to expose YT-6219.
            "chunk_list_watermark_count": 0,
        }
    }

    @unix_only
    def test_map(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("//tmp/t_input", [{"key": i} for i in xrange(3)])

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="echo GG >&2 ; cat",
            spec=get_stderr_spec("//tmp/t_stderr"),
        )

        expect_to_find_in_stderr_table("//tmp/t_stderr", ["GG\n"])
        compare_stderr_table_and_files("//tmp/t_stderr", op.id)

    @unix_only
    def test_aborted_operation(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("//tmp/t_input", [{"key": i} for i in xrange(2)])

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command=with_breakpoint("""BREAKPOINT ; echo GG >&2 ; cat"""),
            dont_track=True,
            spec={
                "stderr_table_path": "//tmp/t_stderr",
                "job_count": 2,
                "data_size_per_sort_job": 10,
            }
        )

        jobs = wait_breakpoint(job_count=2)

        release_breakpoint(job_id=jobs[0])
        wait(lambda: op.get_job_count("running") == 1);

        op.abort()

        expect_to_find_in_stderr_table("//tmp/t_stderr", ["GG\n"])
        compare_stderr_table_and_files("//tmp/t_stderr", op.id)

    @unix_only
    def test_ordered_map(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("""<sorted_by=["key"]>//tmp/t_input""", [{"key": i} for i in xrange(3)])

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="echo GG >&2 ; cat",
            spec=get_stderr_spec("//tmp/t_stderr"),
            ordered=True
        )

        expect_to_find_in_stderr_table("//tmp/t_stderr", ["GG\n"])
        compare_stderr_table_and_files("//tmp/t_stderr", op.id)

    @unix_only
    def test_reduce(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("""<sorted_by=["key"]>//tmp/t_input""", [{"key": i} for i in xrange(3)])

        op = reduce(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="echo REDUCE > /dev/stderr ; cat",
            reduce_by=["key"],
            spec=get_stderr_spec("//tmp/t_stderr"),
        )

        expect_to_find_in_stderr_table("//tmp/t_stderr", ["REDUCE\n"])
        compare_stderr_table_and_files("//tmp/t_stderr", op.id)

    @unix_only
    def test_join_reduce(self):
        create("table", "//tmp/t_foreign")
        create("table", "//tmp/t_primary")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("""<sorted_by=["host"]>//tmp/t_foreign""",
                    [{"host": "bar"},
                     {"host": "baz"},
                     {"host": "foo"}])

        write_table("""<sorted_by=["host";"path"]>//tmp/t_primary""",
                    [{"host": "bar", "path": "/"},
                     {"host": "bar", "path": "/1"},
                     {"host": "bar", "path": "/2"},
                     {"host": "baz", "path": "/"},
                     {"host": "baz", "path": "/1"},
                     {"host": "foo", "path": "/"}])

        op = reduce(
            in_=["<foreign=true>//tmp/t_foreign", "//tmp/t_primary"],
            out="//tmp/t_output",
            command="echo REDUCE >&2 ; cat > /dev/null",
            join_by=["host"],
            reduce_by=["host", "path"],
            spec=get_stderr_spec("//tmp/t_stderr"),
        )

        expect_to_find_in_stderr_table("//tmp/t_stderr", ["REDUCE\n"])
        compare_stderr_table_and_files("//tmp/t_stderr", op.id)

    @unix_only
    def test_map_reduce(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("//tmp/t_input", [{"key": i} for i in xrange(3)])

        op = map_reduce(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            mapper_command="echo FOO >&2 ; cat",
            reducer_command="echo BAR >&2 ; cat",
            sort_by=["key"],
            spec=get_stderr_spec("//tmp/t_stderr")
        )

        expect_to_find_in_stderr_table("//tmp/t_stderr", ["FOO\n", "BAR\n"])
        compare_stderr_table_and_files("//tmp/t_stderr", op.id)

    @unix_only
    def test_map_reduce_no_map(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("//tmp/t_input", [{"key": i} for i in xrange(3)])

        op = map_reduce(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            reducer_command="echo BAR >&2 ; cat",
            sort_by=["key"],
            spec=get_stderr_spec("//tmp/t_stderr")
        )

        expect_to_find_in_stderr_table("//tmp/t_stderr", ["BAR\n"])
        compare_stderr_table_and_files("//tmp/t_stderr", op.id)

    @unix_only
    def test_map_reduce_only_reduce(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("//tmp/t_input", [{"key": i} for i in xrange(3)])

        op = map_reduce(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            reducer_command="echo BAZ >&2 ; cat",
            sort_by=["key"],
            spec=get_stderr_spec("//tmp/t_stderr")
        )

        expect_to_find_in_stderr_table("//tmp/t_stderr", ["BAZ\n"])
        compare_stderr_table_and_files("//tmp/t_stderr", op.id)

    @unix_only
    def test_map_combine_reduce(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("//tmp/t_input", [{"key": i} for i in xrange(100)])

        op = map_reduce(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            mapper_command="echo MAPPER >&2 ; cat",
            reducer_command="echo REDUCER >&2 ; cat",
            reduce_combiner_command="echo COMBINER >&2 ; cat",
            sort_by=["key"],
            spec={
                "stderr_table_path": "//tmp/t_stderr",
                "partition_count": 2,
                "map_job_count": 2,
                "data_size_per_sort_job": 10,
                "data_size_per_reduce_job": 1000,
            }
        )

        expect_to_find_in_stderr_table(
            "//tmp/t_stderr", [
                "MAPPER\n", "MAPPER\n",
                "COMBINER\n", "COMBINER\n",
                "REDUCER\n"])
        compare_stderr_table_and_files("//tmp/t_stderr", op.id)

    @unix_only
    def test_failed_jobs(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("""<sorted_by=["key"]>//tmp/t_input""", [{"key": i} for i in xrange(3)])

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_input",
                out="//tmp/t_output",
                command="echo EPIC_FAIL >&2 ; exit 1",
                spec={
                    "stderr_table_path": "//tmp/t_stderr",
                    "max_failed_job_count": 2,
                }
            )

        stderr_rows = read_table("//tmp/t_stderr")
        assert [row["data"] for row in stderr_rows] == ["EPIC_FAIL\n"] * 2
        assert get("//tmp/t_stderr/@sorted")
        assert get("//tmp/t_stderr/@sorted_by") == ["job_id", "part_index"]

    @unix_only
    def test_append_stderr_prohibited(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("""<sorted_by=["key"]>//tmp/t_input""", [{"key": i} for i in xrange(3)])

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_input",
                out="//tmp/t_output",
                command="echo EPIC_FAIL >&2 ; cat",
                spec={
                    "stderr_table_path": "<append=true>//tmp/t_stderr",
                    "max_failed_job_count": 2,
                }
            )

    @unix_only
    def test_failing_write(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("""<sorted_by=["key"]>//tmp/t_input""", [{"key": i} for i in xrange(3)])

        with pytest.raises(YtError):
            # We set max_part_size to 10MB and max_row_weight to 5MB and write 20MB of stderr.
            map(
                in_="//tmp/t_input",
                out="//tmp/t_output",
                command="""python -c 'import sys; s = "x" * (20 * 1024 * 1024) ; sys.stderr.write(s)'""",
                spec={
                    "stderr_table_path": "//tmp/t_stderr",
                    "stderr_table_writer_config": {
                        "max_row_weight": 5 * 1024 * 1024,
                        "max_part_size": 10 * 1024 * 1024,
                    },
                }
            )


    @unix_only
    def test_max_part_size(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("""<sorted_by=["key"]>//tmp/t_input""", [{"key": i} for i in xrange(1)])

        map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="""python -c 'import sys; s = "x" * (30 * 1024 * 1024) ; sys.stderr.write(s)'""",
            spec={
                "stderr_table_path": "//tmp/t_stderr",
                "stderr_table_writer_config": {
                    "max_row_weight": 128 * 1024 * 1024,
                    "max_part_size": 40 * 1024 * 1024,
                },
            }
        )

    @unix_only
    def test_big_stderr(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("""<sorted_by=["key"]>//tmp/t_input""", [{"key": 0}])

        map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="""python -c 'import sys; s = "x " * (30 * 1024 * 1024) ; sys.stderr.write(s)'""",
            spec=get_stderr_spec("//tmp/t_stderr"),
        )
        stderr_rows = read_table("//tmp/t_stderr", verbose=False)
        assert len(stderr_rows) > 1

        for item in stderr_rows:
            assert item["job_id"] == stderr_rows[0]["job_id"]

        assert str("".join(item["data"] for item in stderr_rows)) == str("x " * (30 * 1024 * 1024))

    @unix_only
    def test_scheduler_revive(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")

        # NOTE all values are of same size so our chunks are also of the same size so our
        # scheduler can split them evenly
        write_table("//tmp/t_input",               [{"key": "complete_before_scheduler_dies  "}])
        write_table("<append=%true>//tmp/t_input", [{"key": "complete_after_scheduler_restart"}])
        write_table("<append=%true>//tmp/t_input", [{"key": "complete_while_scheduler_dead   "}])

        op = map(
            dont_track=True,
            command=(
                "cat > input\n"

                # one job completes before scheduler is dead
                "grep complete_before_scheduler_dies input >/dev/null "
                "  && echo complete_before_scheduler_dies >&2\n"

                # second job completes while scheduler is dead
                "grep complete_while_scheduler_dead input >/dev/null "
                "  && {wait_scheduler_dead} "
                "  && echo complete_while_scheduler_dead >&2 \n"

                # third one completes after scheduler restart
                "grep complete_after_scheduler_restart input >/dev/null "
                "  && {wait_scheduler_restart} "
                "  && echo complete_after_scheduler_restart >&2\n"

                "cat input"
            ).format(
                wait_scheduler_dead=events_on_fs().wait_event_cmd("scheduler_dead"),
                wait_scheduler_restart=events_on_fs().wait_event_cmd("scheduler_restart")),
            format="dsv",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "job_count": 3,
                "data_size_per_job": 1,
                "max_failed_job_count": 1,
                "stderr_table_path": "//tmp/t_stderr",
            }
        )

        wait(lambda: op.get_job_count("completed") == 1);
        wait(lambda: op.get_job_count("running") == 2);

        assert op.get_job_count("total") == 3

        self.Env.kill_schedulers()

        events_on_fs().notify_event("scheduler_dead")

        # Wait some time to give `complete_while_scheduler_dead'-job time to complete.
        time.sleep(1)

        self.Env.start_schedulers()
        events_on_fs().notify_event("scheduler_restart")
        op.track()

        stderr_rows = read_table("//tmp/t_stderr")
        assert sorted(row["data"] for row in stderr_rows) == ["complete_after_scheduler_restart\n",
                                                              "complete_before_scheduler_dies\n",
                                                              "complete_while_scheduler_dead\n"]
        assert get("//tmp/t_stderr/@sorted")
        assert get("//tmp/t_stderr/@sorted_by") == ["job_id", "part_index"]


##################################################################

def random_cookie():
    return binascii.hexlify(os.urandom(16))

def queue_iterator(queue):
    while True:
        chunk = queue.get()
        if chunk is None:
            return
        yield chunk

#@flaky(max_runs=5)
class TestCoreTable(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    CORE_TABLE = "//tmp/t_core"

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "scheduler_connector": {
                "heartbeat_period": 100 # 100 msec
            },
            "job_proxy_heartbeat_period": 100, # 100 msec
            "job_controller": {
                "resource_limits": {
                    "user_slots": 2,
                    "cpu": 2,
                }
            },
            "core_forwarder_timeout": 5000
        }
    }

    @classmethod
    def modify_node_config(cls, config):
        jp_uds_name_dir = os.path.join(cls.path_to_run, "jp_socket")
        cls.JOB_PROXY_UDS_NAME_DIR = jp_uds_name_dir
        config["exec_agent"]["slot_manager"]["job_proxy_socket_name_directory"] = jp_uds_name_dir
        if not os.path.exists(jp_uds_name_dir):
            os.mkdir(jp_uds_name_dir)
        return jp_uds_name_dir

    def setup(self):
        create("table", self.CORE_TABLE)

    def teardown(self):
        core_path = os.environ.get("YT_CORE_PATH")
        if core_path is None:
            return
        for file in os.listdir(core_path):
            if file.startswith("core.bash"):
                os.remove(os.path.join(core_path, file))

    # In order to find out the correspondence between job id and user id,
    # We create a special file in self.JOB_PROXY_UDS_NAME_DIR where we put
    # the user id and job id.
    def _start_operation(self, job_count, max_failed_job_count=5, kill_self=False):
        cookie = random_cookie()

        correspondence_file_path = os.path.join(self.JOB_PROXY_UDS_NAME_DIR, cookie)
        open(correspondence_file_path, "w").close()

        os.chmod(correspondence_file_path, 0777)

        command = with_breakpoint("echo $YT_JOB_ID $UID >>{correspondence_file_path} ; BREAKPOINT ; ".format(
            correspondence_file_path=correspondence_file_path))

        if kill_self:
            command += "kill -ABRT $$ ;"

        op = vanilla(
            dont_track=True,
            spec={
                "tasks": {
                    "main": {
                        "command": command,
                        "job_count": job_count,
                    }
                },
                "core_table_path": "//tmp/t_core",
                "max_failed_job_count": max_failed_job_count
            })

        return op, correspondence_file_path

    def _get_job_uid_correspondence(self, op, correspondence_file_path):
        op.ensure_running()
        total_jobs = op.get_job_count("total")
        jobs = frozenset(wait_breakpoint(job_count=total_jobs))

        job_id_to_uid = {}

        with open(correspondence_file_path) as inf:
            for line in inf:
                job_id, uid = line.split()
                if job_id in jobs:
                    job_id_to_uid[job_id] = uid

        assert len(job_id_to_uid) == total_jobs
        return job_id_to_uid

    # This method starts a core forwarder process and passes given iterable
    # to its stdin. It returns the thread the process is being executed in.
    # The internal procedure returns:
    #   * The core forwarder process return code;
    #   * The core info dictionary that is supposed to be written in Cypress
    #   * The string with a complete core data.
    # It returns the described information by writing it into a given `ret_dict` dictionary
    # (as there is no convenient way to return a value from a thread in Python).
    def _send_core(self, uid, exec_name, pid, input_data, ret_dict, fallback_path="/dev/null"):
        def run_core_forwarder(self, uid, exec_name, pid, input_data, ret_dict, fallback_path):
            args = ["ytserver-core-forwarder", str(pid), str(uid), exec_name,
                    "1", # rlimit_core is always 1 when core forwarder is called in our case.
                    self.JOB_PROXY_UDS_NAME_DIR, fallback_path]
            print >>sys.stderr, repr(args)
            process = psutil.Popen(args, bufsize=0, stdin=subprocess.PIPE)
            size = 0
            core_data = ""
            for chunk in input_data:
                process.stdin.write(chunk)
                process.stdin.flush()
                size += len(chunk)
                core_data += chunk
            process.stdin.close()
            ret_dict["return_code"] = process.wait()
            ret_dict["core_info"] = {
                "executable_name": exec_name,
                "process_id": pid,
                "size": size
            }
            ret_dict["core_data"] = core_data

        thread = threading.Thread(target=run_core_forwarder, args=(self, uid, exec_name, pid, input_data, ret_dict, fallback_path))
        thread.start()
        return thread

    def _get_core_infos(self, op):
        jobs = get("//sys/operations/{0}/jobs".format(op.id), attributes=["core_infos"])
        return {job_id: value.attributes["core_infos"] for job_id, value in jobs.iteritems()}

    def _get_core_table_content(self, assert_rows_number_geq=0):
        rows = read_table(self.CORE_TABLE, verbose=False)
        assert len(rows) >= assert_rows_number_geq
        content = {}
        last_key = None
        for row in rows:
            key = (row["job_id"], row["core_id"], row["part_index"])
            # Check that the table is sorted.
            assert last_key is None or last_key < key
            last_key = key
            if not row["job_id"] in content:
                content[row["job_id"]] = []
            if row["core_id"] >= len(content[row["job_id"]]):
                content[row["job_id"]].append("")
            content[row["job_id"]][row["core_id"]] += row["data"]
        return content

    @skip_if_porto
    @require_ytserver_root_privileges
    @unix_only
    def test_no_cores(self):
        op, correspondence_file_path = self._start_operation(2)
        release_breakpoint()
        op.track()

        assert self._get_core_infos(op) == {}
        assert self._get_core_table_content() == {}

    @skip_if_porto
    @require_ytserver_root_privileges
    @unix_only
    def test_simple(self):
        op, correspondence_file_path = self._start_operation(2)
        job_id_to_uid = self._get_job_uid_correspondence(op, correspondence_file_path)

        job, uid = job_id_to_uid.items()[0]
        ret_dict = {}
        t = self._send_core(uid, "user_process", 42, ["core_data"], ret_dict)
        t.join()
        assert ret_dict["return_code"] == 0

        release_breakpoint()
        op.track()

        assert self._get_core_infos(op) == {job: [ret_dict["core_info"]]}
        assert self._get_core_table_content() == {job: [ret_dict["core_data"]]}

    @skip_if_porto
    @require_ytserver_root_privileges
    @unix_only
    def test_large_core(self):
        op, correspondence_file_path = self._start_operation(1)
        job_id_to_uid = self._get_job_uid_correspondence(op, correspondence_file_path)

        job, uid = job_id_to_uid.items()[0]

        ret_dict = {}
        t = self._send_core(uid, "user_process", 42, ["abcdefgh" * 10**6], ret_dict)
        t.join()
        assert ret_dict["return_code"] == 0

        release_breakpoint()
        op.track()

        assert self._get_core_infos(op) == {job: [ret_dict["core_info"]]}
        assert self._get_core_table_content(assert_rows_number_geq=2) == {job: [ret_dict["core_data"]]}

    @skip_if_porto
    @require_ytserver_root_privileges
    @unix_only
    def test_core_order(self):
        # In this test we check that cores are being processed
        # strictly in the order of their appearance.
        op, correspondence_file_path = self._start_operation(1)
        job_id_to_uid = self._get_job_uid_correspondence(op, correspondence_file_path)

        job, uid = job_id_to_uid.items()[0]

        q1 = Queue()
        ret_dict1 = {}
        t1 = self._send_core(uid, "user_process", 42, queue_iterator(q1), ret_dict1)
        q1.put("abc")
        while not q1.empty():
            time.sleep(0.1)

        time.sleep(1)

        # Check that second core writer blocks on writing to the named pipe by
        # providing a core that is sufficiently larger than pipe buffer size.
        ret_dict2 = {}
        t2 = self._send_core(uid, "user_process2", 43, ["qwert" * (2 * 10**4)], ret_dict2)

        q1.put("def")
        while not q1.empty():
            time.sleep(0.1)
        assert t2.isAlive()
        # Signalize end of the stream.
        q1.put(None)
        t1.join()
        t2.join()

        assert ret_dict1["return_code"] == 0
        assert ret_dict2["return_code"] == 0

        release_breakpoint()
        op.track()

        assert self._get_core_infos(op) == {job: [ret_dict1["core_info"], ret_dict2["core_info"]]}
        assert self._get_core_table_content() == {job: [ret_dict1["core_data"], ret_dict2["core_data"]]}

    @skip_if_porto
    @require_ytserver_root_privileges
    @unix_only
    def test_operation_fails(self):
        op, correspondence_file_path = self._start_operation(1, max_failed_job_count=1)
        job_id_to_uid = self._get_job_uid_correspondence(op, correspondence_file_path)

        job, uid = job_id_to_uid.items()[0]

        ret_dict = {}
        t = self._send_core(uid, "user_process", 42, ["core_data"], ret_dict)
        t.join()
        assert ret_dict["return_code"] == 0

        release_breakpoint()
        with pytest.raises(YtError):
            op.track()

        assert self._get_core_infos(op) == {job: [ret_dict["core_info"]]}
        assert self._get_core_table_content() == {job: [ret_dict["core_data"]]}

    @skip_if_porto
    @require_ytserver_root_privileges
    @unix_only
    def test_writing_core_to_fallback_path(self):
        ret_dict = {}
        # We use a definitely inexistent uid.
        temp_file_path = self.JOB_PROXY_UDS_NAME_DIR + random_cookie()
        t = self._send_core(12345678, "process", 42, ["core_data"], ret_dict, fallback_path=temp_file_path)
        t.join()
        assert ret_dict["return_code"] == 0
        assert open(temp_file_path).read() == "core_data"

    @skip_if_porto
    @require_ytserver_root_privileges
    @unix_only
    def test_cores_with_job_revival(self):
        op, correspondence_file_path = self._start_operation(1)
        job_id_to_uid = self._get_job_uid_correspondence(op, correspondence_file_path)

        job, uid = job_id_to_uid.items()[0]

        q = Queue()
        ret_dict1 = {}
        t = self._send_core(uid, "user_process", 42, queue_iterator(q), ret_dict1)
        q.put("abc")
        while not q.empty():
            time.sleep(0.1)

        self.Env.kill_schedulers()
        self.Env.start_schedulers()

        for job_id in wait_breakpoint():
            release_breakpoint(job_id=job_id)

        q.put("def")
        q.put(None)

        # One may think that we can check if core forwarder process finished with
        # non-zero return code because of a broken pipe, but we do not do it because
        # it really depends on system and may not be true.
        t.join()

        # First running job is discarded with is core, so we repeat the process with
        # a newly scheduled job.

        job_id_to_uid = self._get_job_uid_correspondence(op, correspondence_file_path)

        job, uid = job_id_to_uid.items()[0]

        q = Queue()
        ret_dict2 = {}
        t = self._send_core(uid, "user_process", 43, queue_iterator(q), ret_dict2)
        q.put("abc")
        while not q.empty():
            time.sleep(0.1)
        q.put("def")
        q.put(None)
        t.join()

        assert ret_dict2["return_code"] == 0

        release_breakpoint()
        op.track()

        assert self._get_core_infos(op) == {job: [ret_dict2["core_info"]]}
        assert self._get_core_table_content() == {job: [ret_dict2["core_data"]]}

    @skip_if_porto
    @require_ytserver_root_privileges
    @unix_only
    def test_timeout_while_receiving_core(self):
        op, correspondence_file_path = self._start_operation(1)
        job_id_to_uid = self._get_job_uid_correspondence(op, correspondence_file_path)

        job, uid = job_id_to_uid.items()[0]

        q = Queue()
        ret_dict = {}
        t = self._send_core(uid, "user_process", 42, queue_iterator(q), ret_dict)
        q.put("abc")
        time.sleep(7)
        q.put(None)
        t.join()

        release_breakpoint()
        op.track()
        core_infos = self._get_core_infos(op)
        assert len(core_infos[job]) == 1
        core_info = core_infos[job][0]
        assert core_info["executable_name"] == "user_process"
        assert core_info["process_id"] == 42
        assert not "size" in core_info
        assert "error" in core_info

    @skip_if_porto
    @require_enabled_core_dump
    @require_ytserver_root_privileges
    @unix_only
    def test_core_when_user_job_was_killed(self):
        op, correspondence_file_path = self._start_operation(1, kill_self=True, max_failed_job_count=1)
        job_id_to_uid = self._get_job_uid_correspondence(op, correspondence_file_path)

        job, uid = job_id_to_uid.items()[0]

        release_breakpoint()

        time.sleep(2)

        ret_dict = {}
        t = self._send_core(uid, "user_process", 42, ["core_data"], ret_dict)
        t.join()

        with pytest.raises(YtError):
            op.track()

        assert self._get_core_infos(op) == {job: [ret_dict["core_info"]]}
        assert self._get_core_table_content() == {job: [ret_dict["core_data"]]}

    @skip_if_porto
    @require_enabled_core_dump
    @require_ytserver_root_privileges
    @unix_only
    def test_core_timeout_when_user_job_was_killed(self):
        op, correspondence_file_path = self._start_operation(1, kill_self=True, max_failed_job_count=1)
        job_id_to_uid = self._get_job_uid_correspondence(op, correspondence_file_path)

        job, uid = job_id_to_uid.items()[0]

        release_breakpoint()

        time.sleep(7)

        with pytest.raises(YtError):
            op.track()

        core_infos = self._get_core_infos(op)
        assert len(core_infos[job]) == 1
        core_info = core_infos[job][0]
        assert core_info["executable_name"] == "n/a"
        assert core_info["process_id"] == -1
        assert not "size" in core_info
        assert "error" in core_info

@patch_porto_env_only(TestCoreTable)
class TestCoreTablePorto(YTEnvSetup):
    DELTA_NODE_CONFIG = porto_delta_node_config
    USE_PORTO_FOR_SERVERS = True

    @require_ytserver_root_privileges
    @unix_only
    def test_core_when_user_job_was_killed_porto(self):
        op, correspondence_file_path = self._start_operation(1, kill_self=True, max_failed_job_count=1)
        job_id_to_uid = self._get_job_uid_correspondence(op, correspondence_file_path)

        job, uid = job_id_to_uid.items()[0]

        release_breakpoint()

        time.sleep(2)

        with pytest.raises(YtError):
            op.track()

        core_info = self._get_core_infos(op)[job][0]
        assert core_info["executable_name"] == "bash"
        assert int(core_info["size"]) > 100000
        assert int(core_info["process_id"]) != -1
