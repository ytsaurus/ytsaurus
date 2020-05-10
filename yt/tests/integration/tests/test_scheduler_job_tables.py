from yt_env_setup import YTEnvSetup, unix_only, wait, require_enabled_core_dump, \
    patch_porto_env_only, skip_if_porto, is_asan_build, \
    Restarter, SCHEDULERS_SERVICE, get_porto_delta_node_config
from yt_commands import *

import yt.environment.init_operation_archive as init_operation_archive

import binascii
import itertools
import pytest
import os
import subprocess
import time
import threading
from multiprocessing import Queue

from distutils.spawn import find_executable

if arcadia_interop.yatest_common is None:
    TEST_DIR = os.path.join(os.path.dirname(__file__))

    YT_CUDA_CORE_DUMP_SIMULATOR = os.environ.get("YT_CUDA_CORE_DUMP_SIMULATOR")
    if YT_CUDA_CORE_DUMP_SIMULATOR is None:
        YT_CUDA_CORE_DUMP_SIMULATOR = find_executable("cuda_core_dump_simulator")

    YT_LIB_CUDA_CORE_DUMP_INJECTION = os.environ.get("YT_LIB_CUDA_CORE_DUMP_INJECTION")
    if YT_LIB_CUDA_CORE_DUMP_INJECTION is None:
        YT_LIB_CUDA_CORE_DUMP_INJECTION = find_executable("libcuda_core_dump_injection.so")
else:
    TEST_DIR = arcadia_interop.yatest_common.source_path("yt/tests/integration/tests")
    YT_CUDA_CORE_DUMP_SIMULATOR = arcadia_interop.yatest_common.binary_path("cuda_core_dump_simulator")
    YT_LIB_CUDA_CORE_DUMP_INJECTION = arcadia_interop.yatest_common.binary_path("libcuda_core_dump_injection.so")

##################################################################

def get_stderr_spec(stderr_file):
    return {
        "stderr_table_path": stderr_file,
    }


def get_stderr_dict_from_cypress(operation_id):
    jobs_path = get_operation_cypress_path(operation_id) + "/jobs"
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

    @authors("ermolovd")
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

    @authors("ermolovd")
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
            track=False,
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

    @authors("ermolovd")
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

    @authors("ermolovd")
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

    @authors("ermolovd")
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

    @authors("ermolovd")
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

    @authors("ermolovd")
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

    @authors("ermolovd")
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

    @authors("ermolovd")
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

    @authors("ermolovd")
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

    @authors("ermolovd")
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

    @authors("ermolovd")
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


    @authors("ermolovd")
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

    @authors("ermolovd")
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

    @authors("max42", "ermolovd")
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
            track=False,
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

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            events_on_fs().notify_event("scheduler_dead")

            # Wait some time to give `complete_while_scheduler_dead'-job time to complete.
            time.sleep(1)

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

@pytest.mark.skipif(is_asan_build(), reason="Cores are not dumped in ASAN build")
class TestCoreTable(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True
    REQUIRE_YTSERVER_ROOT_PRIVILEGES = True

    CORE_TABLE = "//tmp/t_core"

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "scheduler_connector": {
                "heartbeat_period": 100 # 100 msec
            },
            "job_reporter": {
                "enabled": True,
                "reporting_period": 10,
                "min_repeat_delay": 10,
                "max_repeat_delay": 10,
            },
            "job_proxy_heartbeat_period": 100, # 100 msec
            "job_controller": {
                "resource_limits": {
                    "user_slots": 5,
                    "cpu": 2,
                }
            },
            "core_watcher": {
                "period": 100,
                "io_timeout": 5000,
                "finalization_timeout": 5000,
                "cores_processing_timeout": 7000,
            },
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "enable_job_reporter": True,
            "operations_cleaner": {
                "enable": False,
                "analysis_period": 100,
                # Cleanup all operations
                "hard_retained_operation_count": 0,
                "clean_delay": 0,
            },
        }
    }

    def setup(self):
        create("table", self.CORE_TABLE, attributes={"replication_factor": 1})

    def teardown(self):
        core_path = os.environ.get("YT_CORE_PATH")
        if core_path is None:
            return
        for file in os.listdir(core_path):
            if file.startswith("core.bash"):
                os.remove(os.path.join(core_path, file))

    def _start_operation(self, job_count, max_failed_job_count=5, kill_self=False,
                         fail_job_on_core_dump=True, core_table_path=None, enable_cuda_gpu_core_dump=False):
        command = with_breakpoint("BREAKPOINT ; ")

        if kill_self:
            command += "kill -ABRT $$ ;"

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "main": {
                        "command": command,
                        "job_count": job_count,
                        "fail_job_on_core_dump": fail_job_on_core_dump,
                    }
                },
                "core_table_path": self.CORE_TABLE if core_table_path is None else core_table_path,
                "enable_cuda_gpu_core_dump": enable_cuda_gpu_core_dump,
                "max_failed_job_count": max_failed_job_count
            })

        job_ids = wait_breakpoint(job_count=job_count)
        return op, job_ids

    # This method simulates core dump in `job_id' job.
    # Refer to core_watcher.h for core delivery process description.
    def _send_core(self, job_id, exec_name, pid, input_data, ret_dict, open_pipe=True):
        def produce_core(self, job_id, exec_name, pid, input_data, ret_dict, open_pipe):
            node = ls("//sys/cluster_nodes")[0]
            slot_index = get("//sys/cluster_nodes/{0}/orchid/job_controller/active_jobs/scheduler/{1}/slot_index".format(node, job_id))
            sandbox_path = "{0}/runtime_data/node/0/slots/{1}".format(self.path_to_run, slot_index)
            core_pipe = "{0}/cores/core_{1}.pipe".format(sandbox_path, pid)
            core_info = "{0}/cores/core_{1}.info".format(sandbox_path, pid)
            size = 0
            core_data = ""

            with open(core_info, "w") as info:
                thread_id = 1234
                signal = 11
                container = "dummy_container"
                datetime = "dummy_datetime"

                core_info_data = ""
                core_info_data += exec_name + "\n"
                core_info_data += str(pid) + "\n"
                core_info_data += str(thread_id) + "\n"
                core_info_data += str(signal) + "\n"
                core_info_data += container + "\n"
                core_info_data += datetime + "\n"
                info.write(core_info_data)

            os.mkfifo(core_pipe)
            if open_pipe:
                try:
                    with open(core_pipe, "w") as pipe:
                        for chunk in input_data:
                            pipe.write(chunk)
                            pipe.flush()
                            size += len(chunk)
                            core_data += chunk
                    pipe.close()
                except:
                    os.remove(core_pipe)
            ret_dict["core_info"] = {
                "executable_name": exec_name,
                "process_id": pid,
                "thread_id": thread_id,
                "signal": signal,
                "container": container,
                "datetime": datetime,
                "size": size
            }
            ret_dict["core_data"] = core_data

        thread = threading.Thread(target=produce_core, args=(self, job_id, exec_name, pid, input_data, ret_dict, open_pipe))
        thread.start()
        return thread

    def _send_gpu_core(self, job_id, ret_dict):
        def produce_gpu_core(self, job_id, ret_dict):
            node = ls("//sys/cluster_nodes")[0]
            slot_index = get("//sys/cluster_nodes/{0}/orchid/job_controller/active_jobs/scheduler/{1}/slot_index".format(node, job_id))
            sandbox_path = "{0}/runtime_data/node/0/slots/{1}".format(self.path_to_run, slot_index)
            core_pipe = "{0}/cores/yt_gpu_core_dump_pipe".format(sandbox_path)

            core_producer_env = os.environ.copy()
            core_producer_env["CUDA_ENABLE_COREDUMP_ON_EXCEPTION"] = "1"
            core_producer_env["CUDA_COREDUMP_FILE"] = core_pipe
            core_producer_env["LD_PRELOAD"] = YT_LIB_CUDA_CORE_DUMP_INJECTION

            os.mkfifo(core_pipe)

            # Check whether core watcher for non-empty pipe.
            time.sleep(8)

            assert subprocess.call([YT_CUDA_CORE_DUMP_SIMULATOR], env=core_producer_env) == 0

            ret_dict["core_info"] = {
                "executable_name": "cuda_gpu_core_dump",
                "process_id": 0,
                "size": 1000000,
            }
            ret_dict["core_data"] = "a" * 1000000

        thread = threading.Thread(target=produce_gpu_core, args=(self, job_id, ret_dict))
        thread.start()
        return thread

    def _get_core_infos(self, op):
        jobs = get(op.get_path() + "/jobs", attributes=["core_infos"])
        return {job_id: value.attributes["core_infos"] for job_id, value in jobs.iteritems()}

    def _decompress_sparse_core_dump(self, core_dump):
        PAGE_SIZE = 65536
        UINT64_LENGTH = 8

        result = ""
        ptr = 0
        while ptr < len(core_dump):
            if core_dump[ptr] == "1":
                result += core_dump[ptr+1:ptr+1+PAGE_SIZE]
            else:
                assert core_dump[ptr] == "0"
                zeroes = 0
                for idx in xrange(ptr + 1 + UINT64_LENGTH, ptr, -1):
                    zeroes = 256 * zeroes + ord(core_dump[idx])
                result += "\0" * zeroes
            ptr += PAGE_SIZE + 1

        return result

    def _get_core_table_content(self, decompress_sparse_core_dump=True, assert_rows_number_geq=0):
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
        if decompress_sparse_core_dump:
            for job_id in content.keys():
                for core_id in range(len(content[job_id])):
                    content[job_id][core_id] = self._decompress_sparse_core_dump(content[job_id][core_id])
        return content

    @authors("max42", "gritukan")
    @skip_if_porto
    @unix_only
    def test_no_cores(self):
        op, job_ids = self._start_operation(2)
        release_breakpoint()
        op.track()

        assert self._get_core_infos(op) == {}
        assert self._get_core_table_content() == {}

    @authors("max42", "gritukan")
    @skip_if_porto
    @unix_only
    def test_simple(self):
        op, job_ids = self._start_operation(2)

        ret_dict = {}
        t = self._send_core(job_ids[0], "user_process", 42, ["core_data"], ret_dict)
        t.join()

        release_breakpoint()
        op.track()

        assert self._get_core_infos(op) == {job_ids[0]: [ret_dict["core_info"]]}
        assert self._get_core_table_content() == {job_ids[0]: [ret_dict["core_data"]]}

    @authors("max42", "gritukan")
    @skip_if_porto
    @unix_only
    def test_large_core(self):
        op, job_ids = self._start_operation(1)

        ret_dict = {}
        t = self._send_core(job_ids[0], "user_process", 42, ["abcdefgh" * 10**6], ret_dict)
        t.join()

        release_breakpoint()
        op.track()

        assert self._get_core_infos(op) == {job_ids[0]: [ret_dict["core_info"]]}
        assert self._get_core_table_content(assert_rows_number_geq=2) == {job_ids[0]: [ret_dict["core_data"]]}

    @authors("max42", "gritukan")
    @skip_if_porto
    @unix_only
    def test_core_order(self):
        # In this test we check that cores are being processed
        # strictly in the order of their appearance.
        op, job_ids = self._start_operation(1)

        q1 = Queue()
        ret_dict1 = {}
        t1 = self._send_core(job_ids[0], "user_process", 42, queue_iterator(q1), ret_dict1)
        q1.put("abc")
        while not q1.empty():
            time.sleep(0.1)

        time.sleep(1)

        # Check that second core writer blocks on writing to the named pipe by
        # providing a core that is sufficiently larger than pipe buffer size.
        ret_dict2 = {}
        t2 = self._send_core(job_ids[0], "user_process2", 43, ["qwert" * (2 * 10**4)], ret_dict2)

        q1.put("def")
        while not q1.empty():
            time.sleep(0.1)
        assert t2.isAlive()
        # Signalize end of the stream.
        q1.put(None)
        t1.join()
        t2.join()

        release_breakpoint()
        op.track()

        assert self._get_core_infos(op) == {job_ids[0]: [ret_dict1["core_info"], ret_dict2["core_info"]]}
        assert self._get_core_table_content() == {job_ids[0]: [ret_dict1["core_data"], ret_dict2["core_data"]]}

    @authors("gritukan", "max42")
    @pytest.mark.parametrize("fail_job_on_core_dump", [False, True])
    @skip_if_porto
    @unix_only
    def test_fail_job_on_core_dump(self, fail_job_on_core_dump):
        op, job_ids = self._start_operation(1, max_failed_job_count=1, fail_job_on_core_dump=fail_job_on_core_dump)

        ret_dict = {}
        t = self._send_core(job_ids[0], "user_process", 42, ["core_data"], ret_dict)
        t.join()

        release_breakpoint()

        if fail_job_on_core_dump:
            with raises_yt_error(UserJobProducedCoreFiles):
                op.track()
        else:
            op.track()

        assert self._get_core_infos(op) == {job_ids[0]: [ret_dict["core_info"]]}
        assert self._get_core_table_content() == {job_ids[0]: [ret_dict["core_data"]]}

    @authors("max42", "gritukan")
    @skip_if_porto
    @unix_only
    def test_cores_with_job_revival(self):
        op, job_ids = self._start_operation(1)

        q = Queue()
        ret_dict1 = {}
        t = self._send_core(job_ids[0], "user_process", 42, queue_iterator(q), ret_dict1)
        q.put("abc")
        while not q.empty():
            time.sleep(0.1)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        release_breakpoint(job_id=job_ids[0])

        q.put("def")
        q.put(None)

        # One may think that we can check if core forwarder process finished with
        # non-zero return code because of a broken pipe, but we do not do it because
        # it really depends on system and may not be true.
        t.join()

        # First running job is discarded with is core, so we repeat the process with
        # a newly scheduled job.
        job_ids = wait_breakpoint(job_count=1)

        q = Queue()
        ret_dict2 = {}
        t = self._send_core(job_ids[0], "user_process", 43, queue_iterator(q), ret_dict2)
        q.put("123")
        while not q.empty():
            time.sleep(0.1)
        q.put("456")
        q.put(None)
        t.join()

        release_breakpoint()
        op.track()

        assert self._get_core_infos(op) == {job_ids[0]: [ret_dict2["core_info"]]}
        assert self._get_core_table_content() == {job_ids[0]: [ret_dict2["core_data"]]}

    @authors("gritukan")
    @skip_if_porto
    @unix_only
    def test_core_table_account_disk_space_limit_exceeded(self):
        create_account("a")
        set_account_disk_space_limit("a", 0)
        create("table", "//tmp/t", attrbutes={"account": "a"})

        op, job_ids = self._start_operation(1, core_table_path="//tmp/t")
        ret_dict = {}
        t = self._send_core(job_ids[0], "user_process", 42, ["core_data"], ret_dict)
        t.join()

        release_breakpoint()
        op.track()

        core_infos = self._get_core_infos(op)
        assert len(core_infos[job_ids[0]]) == 1
        core_info = core_infos[job_ids[0]][0]
        assert core_info["executable_name"] == "user_process"
        assert core_info["process_id"] == 42
        assert not "size" in core_info
        assert "error" in core_info

    @authors("max42", "gritukan")
    @skip_if_porto
    @unix_only
    def test_timeout_while_receiving_core(self):
        op, job_ids = self._start_operation(1)

        q = Queue()
        ret_dict = {}
        t = self._send_core(job_ids[0], "user_process", 42, queue_iterator(q), ret_dict)
        q.put("abc")
        time.sleep(10)
        q.put(None)
        t.join()

        release_breakpoint()
        op.track()
        core_infos = self._get_core_infos(op)
        assert len(core_infos[job_ids[0]]) == 1
        core_info = core_infos[job_ids[0]][0]
        assert core_info["executable_name"] == "user_process"
        assert core_info["process_id"] == 42
        assert not "size" in core_info
        assert "error" in core_info

    @authors("gritukan")
    @skip_if_porto
    @unix_only
    def test_cores_processing_timeout(self):
        op, job_ids = self._start_operation(1)

        q = Queue()
        ret_dict = {}
        t = self._send_core(job_ids[0], "user_process", 42, queue_iterator(q), ret_dict)
        # Once sparse core page.
        page = "a" * (64 * 1024)
        q.put(page)
        time.sleep(1)

        release_breakpoint()

        for _ in range(20):
            q.put(page)
            time.sleep(0.5)

        q.put(None)
        t.join()
        op.track()
        core_infos = self._get_core_infos(op)
        assert len(core_infos[job_ids[0]]) == 1
        core_info = core_infos[job_ids[0]][0]
        assert core_info["executable_name"] == "n/a"
        assert core_info["process_id"] == -1
        assert not "size" in core_info
        assert core_info["error"]["message"] == "Cores processing timed out"

    @authors("gritukan")
    @skip_if_porto
    @unix_only
    def test_core_pipe_not_opened(self):
        op, job_ids = self._start_operation(1)

        q = Queue()
        ret_dict = {}
        t = self._send_core(job_ids[0], "user_process", 42, queue_iterator(q), ret_dict, open_pipe=False)
        time.sleep(10)
        t.join()

        release_breakpoint()
        op.track()
        core_infos = self._get_core_infos(op)
        assert len(core_infos[job_ids[0]]) == 1
        core_info = core_infos[job_ids[0]][0]
        assert core_info["executable_name"] == "user_process"
        assert core_info["process_id"] == 42
        assert not "size" in core_info
        assert "error" in core_info

    @authors("max42", "gritukan")
    @skip_if_porto
    @require_enabled_core_dump
    @unix_only
    def test_core_when_user_job_was_killed(self):
        op, job_ids = self._start_operation(1, kill_self=True, max_failed_job_count=1)

        release_breakpoint()

        time.sleep(2)

        ret_dict = {}
        t = self._send_core(job_ids[0], "user_process", 42, ["core_data"], ret_dict)
        t.join()

        with pytest.raises(YtError):
            op.track()

        assert self._get_core_infos(op) == {job_ids[0]: [ret_dict["core_info"]]}
        assert self._get_core_table_content() == {job_ids[0]: [ret_dict["core_data"]]}

    @authors("max42", "gritukan")
    @skip_if_porto
    @require_enabled_core_dump
    @unix_only
    def test_core_timeout_when_user_job_was_killed(self):
        op, job_ids = self._start_operation(1, kill_self=True, max_failed_job_count=1)

        release_breakpoint()

        time.sleep(7)

        with pytest.raises(YtError):
            op.track()

        core_infos = self._get_core_infos(op)
        assert len(core_infos[job_ids[0]]) == 1
        core_info = core_infos[job_ids[0]][0]
        assert core_info["executable_name"] == "n/a"
        assert core_info["process_id"] == -1
        assert not "size" in core_info
        assert core_info["error"]["message"] == "Timeout while waiting for a core dump"

    @authors("ignat", "gritukan")
    @skip_if_porto
    @require_enabled_core_dump
    @unix_only
    def test_core_infos_from_archive(self):
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client(), override_tablet_cell_bundle="default")

        op, job_ids = self._start_operation(2)

        ret_dict = {}
        t = self._send_core(job_ids[0], "user_process", 42, ["core_data"], ret_dict)
        t.join()

        release_breakpoint()
        op.track()

        assert self._get_core_infos(op) == {job_ids[0]: [ret_dict["core_info"]]}
        assert self._get_core_table_content() == {job_ids[0]: [ret_dict["core_data"]]}

        jobs = list_jobs(op.id, attributes=["core_infos"])["jobs"]
        assert len(jobs) == 1
        assert jobs[0]["core_infos"] == [ret_dict["core_info"]]

        clean_operations()

        list_jobs_func = lambda: list_jobs(op.id, attributes=["core_infos"])["jobs"]
        wait(lambda: len(list_jobs_func()) == 3)
        jobs = list_jobs_func()

        filtered_job_with_core = [job for job in jobs if job["id"] == job_ids[0]][0]
        assert filtered_job_with_core["core_infos"] == [ret_dict["core_info"]]

    @authors("gritukan")
    @skip_if_porto
    @unix_only
    def test_sparse_core_dump_format(self):
        op, job_ids = self._start_operation(2)

        ret_dict = {}
        t = self._send_core(job_ids[0], "user_process", 42, ["abc" * 12345 + "\0" * 54321 + "abc" * 17424], ret_dict)
        t.join()

        release_breakpoint()
        op.track()

        assert get(self.CORE_TABLE + "/@sparse") == True
        assert self._get_core_infos(op) == {job_ids[0]: [ret_dict["core_info"]]}
        assert self._get_core_table_content() == {job_ids[0]: [ret_dict["core_data"]]}

    @authors("gritukan")
    @skip_if_porto
    @unix_only
    def test_sparse_compression_rate_on_sparse_core_dump(self):
        op, job_ids = self._start_operation(2)

        ret_dict = {}
        t = self._send_core(job_ids[0], "user_process", 42, ["\0" * 10**6], ret_dict)
        t.join()

        release_breakpoint()
        op.track()

        assert get(self.CORE_TABLE + "/@sparse") == True
        assert self._get_core_infos(op) == {job_ids[0]: [ret_dict["core_info"]]}
        sparse_core_dump = self._get_core_table_content(decompress_sparse_core_dump=False)[job_ids[0]][0]
        assert len(sparse_core_dump) == 65537
        assert len(ret_dict["core_data"]) == 10**6
        assert self._get_core_table_content() == {job_ids[0]: [ret_dict["core_data"]]}

    @authors("gritukan")
    @skip_if_porto
    @unix_only
    def test_cuda_gpu_core_dump(self):
        if YT_CUDA_CORE_DUMP_SIMULATOR is None:
            pytest.skip("This test requires cuda_core_dump_simulator being built")
        if YT_LIB_CUDA_CORE_DUMP_INJECTION is None:
            pytest.skip("This test requires lib_cuda_core_dump_injection being built")

        op, job_ids = self._start_operation(1, enable_cuda_gpu_core_dump=True)

        ret_dict = {}
        t = self._send_gpu_core(job_ids[0], ret_dict)
        t.join()

        release_breakpoint()
        op.track()

        assert self._get_core_infos(op) == {job_ids[0]: [ret_dict["core_info"]]}
        assert self._get_core_table_content() == {job_ids[0]: [ret_dict["core_data"]]}

@patch_porto_env_only(TestCoreTable)
class TestCoreTablePorto(YTEnvSetup):
    DELTA_NODE_CONFIG = get_porto_delta_node_config()
    USE_PORTO_FOR_SERVERS = True
    REQUIRE_YTSERVER_ROOT_PRIVILEGES = True

    @authors("dcherednik", "gritukan")
    @unix_only
    def test_core_when_user_job_was_killed_porto(self):
        op, job_ids = self._start_operation(1, kill_self=True, max_failed_job_count=1)

        release_breakpoint()

        time.sleep(2)

        with pytest.raises(YtError):
            op.track()

        core_info = self._get_core_infos(op)[job_ids[0]][0]
        assert core_info["executable_name"] == "bash"
        assert int(core_info["size"]) > 100000
        assert int(core_info["process_id"]) != -1
        assert "thread_id" in core_info
        assert int(core_info["signal"]) == 6
        assert "container" in core_info
        assert "datetime" in core_info
