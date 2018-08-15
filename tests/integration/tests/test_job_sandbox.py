from yt_env_setup import YTEnvSetup, patch_porto_env_only
from yt_commands import *

from yt.yson import *
from yt.test_helpers import assert_items_equal, are_almost_equal

from flaky import flaky

import pytest
import time

porto_delta_node_config = {
    "exec_agent": {
        "slot_manager": {
            # <= 18.4
            "enforce_job_control": True,
            "job_environment": {
                # >= 19.2
                "type": "porto",
            },
        }
    }
}

##################################################################

class TestSandboxTmpfs(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    def test_simple(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="cat; echo 'content' > tmpfs/file; ls tmpfs/ >&2; cat tmpfs/file >&2;",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": "tmpfs",
                },
                "max_failed_job_count" : 1
            })

        jobs_path = "//sys/operations/" + op.id + "/jobs"
        assert get(jobs_path + "/@count") == 1
        words = read_file(jobs_path + "/" + ls(jobs_path)[0] + "/stderr").strip().split()
        assert ["file", "content"] == words

    def test_custom_tmpfs_path(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="cat; echo 'content' > my_dir/file; ls my_dir/ >&2; cat my_dir/file >&2;",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": "my_dir",
                }
            })

        jobs_path = "//sys/operations/" + op.id + "/jobs"
        assert get(jobs_path + "/@count") == 1
        words = read_file(jobs_path + "/" + ls(jobs_path)[0] + "/stderr").strip().split()
        assert ["file", "content"] == words

    def test_dot_tmpfs_path(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="cat; mkdir my_dir; echo 'content' > my_dir/file; ls my_dir/ >&2; cat my_dir/file >&2;",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": ".",
                }
            })

        jobs_path = "//sys/operations/" + op.id + "/jobs"
        assert get(jobs_path + "/@count") == 1
        words = read_file(jobs_path + "/" + ls(jobs_path)[0] + "/stderr").strip().split()
        assert ["file", "content"] == words

        create("file", "//tmp/test_file")
        write_file("//tmp/test_file", "".join(["0"] * (1024 * 1024 + 1)))
        map(command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": ".",
                    "file_paths": ["//tmp/test_file"]
                }
            })

        map(command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": "./",
                    "file_paths": ["//tmp/test_file"]
                }
            })

        script = "#!/usr/bin/env python\n"\
                 "import sys\n"\
                 "sys.stdout.write(sys.stdin.read())\n"\
                 "with open('test_file', 'w') as f: f.write('Hello world!')"
        create("file", "//tmp/script")
        write_file("//tmp/script", script)
        set("//tmp/script/@executable", True)

        map(command="./script.py",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 100 * 1024 * 1024,
                    "tmpfs_path": ".",
                    "copy_files": True,
                    "file_paths": ["//tmp/test_file", to_yson_type("//tmp/script", attributes={"file_name": "script.py"})]
                }
            })

        with pytest.raises(YtError):
            map(command="cat; cp test_file local_file;",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": ".",
                        "file_paths": ["//tmp/test_file"]
                    },
                    "max_failed_job_count": 1,
                })

        op = map(command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024 + 10000,
                    "tmpfs_path": ".",
                    "file_paths": ["//tmp/test_file"],
                    "copy_files": True,
                },
                "max_failed_job_count": 1,
            })

        statistics = get("//sys/operations/{0}/@progress/job_statistics".format(op.id))
        tmpfs_size = get_statistics(statistics, "user_job.tmpfs_size.$.completed.map.sum")
        assert 0.9 * 1024 * 1024 <= tmpfs_size <= 1.1 * 1024 * 1024

        with pytest.raises(YtError):
            map(command="cat",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": ".",
                        "file_paths": ["//tmp/test_file"],
                        "copy_files": True,
                    },
                    "max_failed_job_count": 1,
                })

    def test_incorrect_tmpfs_path(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_input", out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": "../",
                    }
                })

        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_input", out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": "/tmp",
                    }
                })


    def test_tmpfs_remove_failed(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        with pytest.raises(YtError):
            map(command="cat; rm -rf tmpfs",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": "tmpfs",
                    },
                    "max_failed_job_count": 1
                })

    def test_tmpfs_size_limit(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        with pytest.raises(YtError):
            map(command="set -e; cat; dd if=/dev/zero of=tmpfs/file bs=1100000 count=1",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024
                    },
                    "max_failed_job_count": 1
                })

    def test_memory_reserve_and_tmpfs(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(command="python -c 'import time; x = \"0\" * (200 * 1000 * 1000); time.sleep(2)'",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_path": "tmpfs",
                    "memory_limit": 250 * 1000 * 1000
                },
                "max_failed_job_count": 1
            })

        assert get("//sys/operations/{0}/@progress/jobs/aborted/total".format(op.id)) == 0

    def test_inner_files(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        create("file", "//tmp/file.txt")
        write_file("//tmp/file.txt", "{trump = moron};\n")

        op = map(
            command="cat; cat ./tmpfs/trump.txt",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            file=['<file_name="./tmpfs/trump.txt">//tmp/file.txt'],
            spec={
                "mapper": {
                    "tmpfs_path": "tmpfs",
                    "tmpfs_size": 1024 * 1024,
                },
                "max_failed_job_count": 1
            })

        assert get("//tmp/t_output/@row_count".format(op.id)) == 2

##################################################################

@patch_porto_env_only(TestSandboxTmpfs)
class TestSandboxTmpfsPorto(YTEnvSetup):
    DELTA_NODE_CONFIG = porto_delta_node_config
    USE_PORTO_FOR_SERVERS = True

##################################################################

class TestDisabledSandboxTmpfs(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "slot_manager": {
                "enable_tmpfs": False
            }
        }
    }

    def test_simple(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="cat; echo 'content' > tmpfs/file; ls tmpfs/ >&2; cat tmpfs/file >&2;",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": "tmpfs",
                }
            })

        jobs_path = "//sys/operations/" + op.id + "/jobs"
        assert get(jobs_path + "/@count") == 1
        words = read_file(jobs_path + "/" + ls(jobs_path)[0] + "/stderr").strip().split()
        assert ["file", "content"] == words


##################################################################

class TestFilesInSandbox(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "static_orchid_cache_update_period": 100,
        }
    }

    @flaky(max_runs=3)
    def test_operation_abort_with_lost_file(self):
        create("file", "//tmp/script", attributes={"replication_factor": 1, "executable": True})
        write_file("//tmp/script", "#!/bin/bash\ncat")

        chunk_ids = get("//tmp/script/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        replicas = get("#{0}/@stored_replicas".format(chunk_id))
        assert len(replicas) == 1
        replica_to_ban = str(replicas[0]) # str() is for attribute stripping.

        banned = False
        for node in ls("//sys/nodes"):
            if node == replica_to_ban:
                set("//sys/nodes/{0}/@banned".format(node), True)
                banned = True
        assert banned

        time.sleep(1)
        assert get("#{0}/@replication_status/default/lost".format(chunk_id))

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})
        op = map(dont_track=True,
                 command="./script",
                 in_="//tmp/t_input",
                 out="//tmp/t_output",
                 spec={
                     "mapper": {
                         "file_paths": ["//tmp/script"]
                     }
                 })

        while True:
            if op.get_job_count("running") == 1:
                break
            time.sleep(0.5)

        time.sleep(1)
        op.abort()

        time.sleep(1)
        assert op.get_state() == "aborted"
        assert are_almost_equal(get("//sys/scheduler/orchid/scheduler/cell/resource_usage/cpu"), 0)
