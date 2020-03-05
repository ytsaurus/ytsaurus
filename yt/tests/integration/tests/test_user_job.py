from yt_env_setup import (
    YTEnvSetup, porto_avaliable, get_porto_delta_node_config, unix_only,
    Restarter, SCHEDULERS_SERVICE, patch_porto_env_only
)
from yt_commands import *

import yt.common
import yt.environment.init_operation_archive as init_operation_archive
from yt.yson import *
from yt.test_helpers import are_almost_equal
from yt.common import update

from flaky import flaky

import pytest
import time
import datetime

##################################################################

class TestSandboxTmpfs(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    REQUIRE_YTSERVER_ROOT_PRIVILEGES = True

    @authors("ignat")
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

        jobs_path = op.get_path() + "/jobs"
        assert get(jobs_path + "/@count") == 1
        content = read_file(jobs_path + "/" + ls(jobs_path)[0] + "/stderr")
        words = content.strip().split()
        assert ["file", "content"] == words

    @authors("ignat")
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

        jobs_path = op.get_path() + "/jobs"
        assert get(jobs_path + "/@count") == 1
        content = read_file(jobs_path + "/" + ls(jobs_path)[0] + "/stderr")
        words = content.strip().split()
        assert ["file", "content"] == words

    @authors("ignat")
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

        jobs_path = op.get_path() + "/jobs"
        assert get(jobs_path + "/@count") == 1
        content = read_file(jobs_path + "/" + ls(jobs_path)[0] + "/stderr")
        words = content.strip().split()
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

        op = map(
            command="cat",
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

        statistics = get(op.get_path() + "/@progress/job_statistics")
        tmpfs_size = get_statistics(statistics, "user_job.tmpfs_volumes.0.max_size.$.completed.map.sum")
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

    @authors("ignat")
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


    @authors("ignat")
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

    @authors("ignat")
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

    @authors("ignat")
    def test_memory_reserve_and_tmpfs(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="python -c 'import time; x = \"0\" * (200 * 1000 * 1000); time.sleep(2)'",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_path": "tmpfs",
                    "memory_limit": 250 * 1000 * 1000
                },
                "max_failed_job_count": 1
            })

        assert get(op.get_path() + "/@progress/jobs/aborted/total") == 0

    @authors("psushin")
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

    @authors("ignat")
    def test_multiple_tmpfs_volumes(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command=
                "cat; "
                "echo 'content_1' > tmpfs_1/file; ls tmpfs_1/ >&2; cat tmpfs_1/file >&2;"
                "echo 'content_2' > tmpfs_2/file; ls tmpfs_2/ >&2; cat tmpfs_2/file >&2;",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_volumes": [
                        {
                            "size": 1024 * 1024,
                            "path": "tmpfs_1",
                        },
                        {
                            "size": 1024 * 1024,
                            "path": "tmpfs_2",
                        },
                    ]
                },
                "max_failed_job_count": 1
            })

        jobs_path = op.get_path() + "/jobs"
        assert get(jobs_path + "/@count") == 1
        content = read_file(jobs_path + "/" + ls(jobs_path)[0] + "/stderr")
        words = content.strip().split()
        assert ["file", "content_1", "file", "content_2"] == words

    @authors("ignat")
    def test_incorrect_multiple_tmpfs_volumes(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        with pytest.raises(YtError):
            map(
                command="cat",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_volumes": [
                            {
                                "path": "tmpfs",
                            },
                        ]
                    },
                    "max_failed_job_count": 1
                })

        with pytest.raises(YtError):
            map(
                command="cat",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_volumes": [
                            {
                                "size": 1024 * 1024,
                            },
                        ]
                    },
                    "max_failed_job_count": 1
                })

        with pytest.raises(YtError):
            map(
                command="cat",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_volumes": [
                            {
                                "path": "tmpfs",
                                "size": 1024 * 1024,
                            },
                            {
                                "path": "tmpfs/inner",
                                "size": 1024 * 1024,
                            },
                        ]
                    },
                    "max_failed_job_count": 1
                })

        with pytest.raises(YtError):
            map(
                command="cat",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_volumes": [
                            {
                                "path": "tmpfs/fake_inner/../",
                                "size": 1024 * 1024,
                            },
                            {
                                "path": "tmpfs/inner",
                                "size": 1024 * 1024,
                            },
                        ]
                    },
                    "max_failed_job_count": 1
                })

    @authors("ignat")
    def test_multiple_tmpfs_volumes_with_common_prefix(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command=
                "cat; "
                "echo 'content_1' > tmpfs_dir/file; ls tmpfs_dir/ >&2; cat tmpfs_dir/file >&2;"
                "echo 'content_2' > tmpfs_dir_fedor/file; ls tmpfs_dir_fedor/ >&2; cat tmpfs_dir_fedor/file >&2;",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_volumes": [
                        {
                            "size": 1024 * 1024,
                            "path": "tmpfs_dir",
                        },
                        {
                            "size": 1024 * 1024,
                            "path": "tmpfs_dir_fedor",
                        },
                    ]
                },
                "max_failed_job_count": 1
            })

        jobs_path = op.get_path() + "/jobs"
        assert get(jobs_path + "/@count") == 1
        content = read_file(jobs_path + "/" + ls(jobs_path)[0] + "/stderr")
        words = content.strip().split()
        assert ["file", "content_1", "file", "content_2"] == words

    @authors("ignat")
    def test_vanilla(self):
        op = vanilla(
            spec={
                "tasks": {
                    "a": {
                        "job_count": 2,
                        "command": 'sleep 5',
                        "tmpfs_volumes": [
                        ]
                    },
                    "b": {
                        "job_count": 1,
                        "command": 'sleep 10',
                        "tmpfs_volumes": [
                            {
                                "path": "tmpfs",
                                "size": 1024 * 1024,
                            },
                        ]
                    },
                    "c": {
                        "job_count": 3,
                        "command": 'sleep 15',
                        "tmpfs_volumes": [
                            {
                                "path": "tmpfs",
                                "size": 1024 * 1024,
                            },
                            {
                                "path": "other_tmpfs",
                                "size": 1024 * 1024,
                            },
                        ]
                    },
                },
            })

##################################################################

@pytest.mark.skip_if('not porto_avaliable()')
class TestSandboxTmpfsOverflow(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True
    REQUIRE_YTSERVER_ROOT_PRIVILEGES = True
    USE_PORTO_FOR_SERVERS = True
    DELTA_NODE_CONFIG = yt.common.update(
        get_porto_delta_node_config(),
        {
            "exec_agent": {
                "statistics_reporter": {
                    "enabled": True,
                    "reporting_period": 10,
                    "min_repeat_delay": 10,
                    "max_repeat_delay": 10,
                },
                "job_controller": {
                    "resource_limits": {
                        "memory": 6 * 1024 ** 3,
                    }
                },
                "job_reporter": {
                    "enabled": True,
                    "reporting_period": 10,
                    "min_repeat_delay": 10,
                    "max_repeat_delay": 10,
                },
            },
        })

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
            "enable_job_stderr_reporter": True,
        }
    }

    def setup(self):
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client(), override_tablet_cell_bundle="default")
        self._tmpdir = create_tmpdir("jobids")

    @authors("ignat")
    def test_multiple_tmpfs_overflow(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            track=False,
            command=with_breakpoint(
                "dd if=/dev/zero of=tmpfs_1/file  bs=1M  count=2048; ls tmpfs_1/ >&2; "
                "dd if=/dev/zero of=tmpfs_2/file  bs=1M  count=2048; ls tmpfs_2/ >&2; "
                "BREAKPOINT; "
                "python -c 'import time; x = \"A\" * (200 * 1024 * 1024); time.sleep(100);'"),
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_volumes": [
                        {
                            "size": 2 * 1024 ** 3,
                            "path": "tmpfs_1",
                        },
                        {
                            "size": 2 * 1024 ** 3,
                            "path": "tmpfs_2",
                        },
                    ],
                    "memory_limit": 4 * 1024 ** 3 + 200 * 1024 * 1024,
                },
                "max_failed_job_count": 1
            })

        op.ensure_running()

        jobs = wait_breakpoint(timeout=datetime.timedelta(seconds=300))
        assert len(jobs) == 1
        job = jobs[0]

        def get_tmpfs_size():
            job_info = get_job(op.id, job)
            try:
                sum = 0
                for key, value in job_info["statistics"]["user_job"]["tmpfs_volumes"].iteritems():
                    sum += value["max_size"]["sum"]
                return sum
            except KeyError:
                print_debug("JOB_INFO", job_info)
                return 0

        wait(lambda: get_tmpfs_size() >= 4 * 1024 ** 3)

        assert op.get_state() == "running"

        release_breakpoint()

        wait(lambda: op.get_state() == "failed")

        assert op.get_error().contains_code(1200)

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

    @authors("ignat")
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

        jobs_path = op.get_path() + "/jobs"
        assert get(jobs_path + "/@count") == 1
        content = read_file(jobs_path + "/" + ls(jobs_path)[0] + "/stderr")
        words = content.strip().split()
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

    @authors("ignat")
    @flaky(max_runs=3)
    def test_operation_abort_with_lost_file(self):
        create("file", "//tmp/script", attributes={"replication_factor": 1, "executable": True})
        write_file("//tmp/script", "#!/bin/bash\ncat")

        chunk_id = get_singular_chunk_id("//tmp/script")

        replicas = get("#{0}/@stored_replicas".format(chunk_id))
        assert len(replicas) == 1
        replica_to_ban = str(replicas[0]) # str() is for attribute stripping.

        banned = False
        for node in ls("//sys/cluster_nodes"):
            if node == replica_to_ban:
                set("//sys/cluster_nodes/{0}/@banned".format(node), True)
                banned = True
        assert banned

        wait(lambda: get("#{0}/@replication_status/default/lost".format(chunk_id)))

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})
        op = map(track=False,
                 command="./script",
                 in_="//tmp/t_input",
                 out="//tmp/t_output",
                 spec={
                     "mapper": {
                         "file_paths": ["//tmp/script"]
                     }
                 })

        wait(lambda: op.get_job_count("running") == 1)

        time.sleep(1)
        op.abort()

        wait(lambda: op.get_state() == "aborted" and are_almost_equal(get("//sys/scheduler/orchid/scheduler/cell/resource_usage/cpu"), 0))

##################################################################

class TestArtifactCacheBypass(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @authors("babenko")
    def test_bypass_artifact_cache_for_file(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        create("file", "//tmp/file")
        write_file("//tmp/file", '{"hello": "world"}')
        map(command="cat file",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "file_paths": ["<bypass_artifact_cache=%true>//tmp/file"],
                    "output_format": "json"
                }
            })

        assert read_table("//tmp/t_output") == [{"hello": "world"}]

    @authors("babenko")
    def test_bypass_artifact_cache_for_table(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        create("table", "//tmp/table")
        write_table("//tmp/table", [{"hello": "world"}])
        map(command="cat table",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "file_paths": ["<bypass_artifact_cache=%true;format=json>//tmp/table"],
                    "output_format": "json"
                }
            })

        assert read_table("//tmp/t_output") == [{"hello": "world"}]

##################################################################

@pytest.mark.skip_if('not porto_avaliable()')
class TestNetworkIsolation(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "test_network": True,
            "slot_manager": {
                "job_environment" : {
                    "type" : "porto",
                },
            }
        }
    }

    REQUIRE_YTSERVER_ROOT_PRIVILEGES = True
    USE_PORTO_FOR_SERVERS = True

    @authors("gritukan")
    def test_create_network_project_map(self):
        create("network_project_map", "//tmp/n")

    @authors("gritukan")
    def test_network_project_in_spec(self):
        create_user("u1")
        create_user("u2")
        create_network_project("n")
        set("//sys/network_projects/n/@project_id", 0xdeadbeef)
        set("//sys/network_projects/n/@acl", [make_ace("allow", "u1", "use")])

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        # Non-existent network project. Job should fail.
        with pytest.raises(YtError):
            map(command="cat",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "network_project": "x"
                    }
                })

        # User `u2` is not allowed to use `n`. Job should fail.
        with pytest.raises(YtError):
            map(command="cat",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "network_project": "n"
                    }
                },
                authenticated_user="u2")

        op = map(track=False,
                 command=with_breakpoint("echo $YT_NETWORK_PROJECT_ID >&2; hostname >&2; BREAKPOINT; cat"),
                 in_="//tmp/t_input",
                 out="//tmp/t_output",
                 spec={
                     "mapper": {
                         "network_project": "n"
                     }
                 },
                 authenticated_user="u1")

        job_id = wait_breakpoint()[0]
        network_project_id, hostname, _ = get_job_stderr(op.id, job_id).split('\n')
        assert network_project_id == str(0xdeadbeef)
        assert hostname.startswith("slot_")
        release_breakpoint()
        op.track()

##################################################################

@unix_only
class TestJobStderr(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_MASTER_CONFIG = {
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True
        }
    }

    @authors("ignat")
    def test_stderr_ok(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        command = """cat > /dev/null; echo stderr 1>&2; echo {operation='"'$YT_OPERATION_ID'"'}';'; echo {job_index=$YT_JOB_INDEX};"""

        op = map(in_="//tmp/t1", out="//tmp/t2", command=command)

        assert read_table("//tmp/t2") == [{"operation": op.id}, {"job_index": 0}]
        check_all_stderrs(op, "stderr\n", 1)

    @authors("ignat")
    def test_stderr_failed(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        command = "echo stderr 1>&2 ; exit 1"

        op = map(track=False, in_="//tmp/t1", out="//tmp/t2", command=command)

        with pytest.raises(YtError):
            op.track()

        check_all_stderrs(op, "stderr\n", 10)

    @authors("ignat")
    def test_stderr_limit(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat > /dev/null; echo stderr 1>&2; exit 125",
            spec={"max_failed_job_count": 5})

        # If all jobs failed then operation is also failed
        with pytest.raises(YtError):
            op.track()

        check_all_stderrs(op, "stderr\n", 5)

    @authors("ignat")
    def test_stderr_max_size(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat > /dev/null; python -c 'print \"head\" + \"0\" * 10000000; print \"1\" * 10000000 + \"tail\"' >&2;",
            spec={"max_failed_job_count": 1, "mapper": {"max_stderr_size": 1000000}})

        jobs_path = op.get_path() + "/jobs"
        assert get(jobs_path + "/@count") == 1
        stderr_path = "{0}/{1}/stderr".format(jobs_path, ls(jobs_path)[0])
        stderr = read_file(stderr_path, verbose=False).strip()

        # Stderr buffer size is equal to 1000000, we should add it to limit
        assert len(stderr) <= 4000000
        assert stderr[:1004] == "head" + "0" * 1000
        assert stderr[-1004:] == "1" * 1000 + "tail"
        assert "skipped" in stderr

    @authors("prime")
    def test_stderr_chunks_not_created_for_completed_jobs(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"row_id": "row_" + str(i)} for i in xrange(100)])

        # One job hangs, so that we can poke into transaction.
        command = """
                if [ "$YT_JOB_INDEX" -eq 1 ]; then
                    sleep 1000
                else
                    cat > /dev/null; echo message > /dev/stderr
                fi;"""

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=command,
            spec={"job_count": 10, "max_stderr_count": 0})

        def enough_jobs_completed():
            if not exists(op.get_path() + "/@progress"):
                return False
            progress = get(op.get_path() + "/@progress")
            if "jobs" in progress and "completed" in progress["jobs"]:
                return progress["jobs"]["completed"]["total"] > 8
            return False

        wait(enough_jobs_completed)

        stderr_tx = get(op.get_path() + "/@async_scheduler_transaction_id")
        staged_objects = get("//sys/transactions/{0}/@staged_object_ids".format(stderr_tx))
        assert sum(len(ids) for ids in staged_objects.values()) == 0, str(staged_objects)

    @authors("ignat")
    def test_stderr_of_failed_jobs(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"row_id": "row_" + str(i)} for i in xrange(20)])

        command = with_breakpoint("""
                BREAKPOINT;
                IS_FAILING_JOB=$(($YT_JOB_INDEX>=19));
                echo stderr 1>&2;
                if [ $IS_FAILING_JOB -eq 1 ]; then
                    if mkdir {lock_dir}; then
                        exit 125;
                    else
                        exit 0
                    fi;
                else
                    exit 0;
                fi;"""
                    .format(lock_dir=events_on_fs()._get_event_filename("lock_dir")))
        op = map(
            track=False,
            label="stderr_of_failed_jobs",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=command,
            spec={"max_failed_job_count": 1, "max_stderr_count": 10, "job_count": 20})

        release_breakpoint()
        with pytest.raises(YtError):
            op.track()

        # The default number of stderr is 10. We check that we have 11-st stderr of failed job,
        # that is last one.
        check_all_stderrs(op, "stderr\n", 11)

    @authors("ignat")
    def test_stderr_with_missing_tmp_quota(self):
        create_account("test_account")

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"} for _ in xrange(5)])

        op = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat > /dev/null; echo 'stderr' >&2;",
            spec={"max_failed_job_count": 1, "job_node_account": "test_account"})
        check_all_stderrs(op, "stderr\n", 1)

        multicell_sleep()
        resource_usage = get("//sys/accounts/test_account/@resource_usage")
        assert resource_usage["node_count"] >= 2
        assert resource_usage["chunk_count"] >= 1
        assert resource_usage["disk_space_per_medium"].get("default", 0) > 0

        jobs = ls(op.get_path() + "/jobs")
        get(op.get_path() + "/jobs/{}".format(jobs[0]))
        get(op.get_path() + "/jobs/{}/stderr".format(jobs[0]))
        recursive_resource_usage = get(op.get_path() + "/jobs/{0}/@recursive_resource_usage".format(jobs[0]))

        assert recursive_resource_usage["chunk_count"] == resource_usage["chunk_count"]
        assert recursive_resource_usage["disk_space_per_medium"]["default"] == \
            resource_usage["disk_space_per_medium"]["default"]
        assert recursive_resource_usage["node_count"] == resource_usage["node_count"]

        set("//sys/accounts/test_account/@resource_limits/chunk_count", 0)
        set("//sys/accounts/test_account/@resource_limits/node_count", 0)
        op = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat > /dev/null; echo 'stderr' >&2;",
            spec={"max_failed_job_count": 1, "job_node_account": "test_account"})
        check_all_stderrs(op, "stderr\n", 0)

class TestJobStderrMulticell(TestJobStderr):
    NUM_SECONDARY_MASTER_CELLS = 2

@patch_porto_env_only(TestJobStderr)
class TestJobStderrPorto(YTEnvSetup):
    DELTA_NODE_CONFIG = get_porto_delta_node_config()
    REQUIRE_YTSERVER_ROOT_PRIVILEGES = True
    USE_PORTO_FOR_SERVERS = True

##################################################################

class TestUserFiles(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_MASTER_CONFIG = {
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True
        }
    }

    @authors("ignat")
    def test_file_with_integer_name(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("//tmp/t_input", [{"hello": "world"}])

        file = "//tmp/1000"
        create("file", file)
        write_file(file, "{value=42};\n")

        map(in_="//tmp/t_input",
            out=["//tmp/t_output"],
            command="cat 1000 >&2; cat",
            file=[file],
            verbose=True)

        assert read_table("//tmp/t_output") == [{"hello": "world"}]

    @authors("ignat")
    def test_file_with_subdir(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("//tmp/t_input", [{"hello": "world"}])

        file = "//tmp/test_file"
        create("file", file)
        write_file(file, "{value=42};\n")

        map(in_="//tmp/t_input",
            out=["//tmp/t_output"],
            command="cat dir/my_file >&2; cat",
            file=[to_yson_type("//tmp/test_file", attributes={"file_name": "dir/my_file"})],
            verbose=True)

        with pytest.raises(YtError):
            map(in_="//tmp/t_input",
                out=["//tmp/t_output"],
                command="cat dir/my_file >&2; cat",
                file=[to_yson_type("//tmp/test_file", attributes={"file_name": "../dir/my_file"})],
                spec={"max_failed_job_count": 1},
                verbose=True)

        assert read_table("//tmp/t_output") == [{"hello": "world"}]

    @authors("levysotsky")
    def test_unlinked_file(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("//tmp/t_input", [{"hello": "world"}])

        file = "//tmp/test_file"
        create("file", file)
        write_file(file, "{value=42};\n")
        tx = start_transaction(timeout=30000)
        file_id = get(file + "/@id")
        assert lock(file, mode="snapshot", tx=tx)
        remove(file)

        map(in_="//tmp/t_input",
            out=["//tmp/t_output"],
            command="cat my_file; cat",
            file=[to_yson_type("#" + file_id, attributes={"file_name": "my_file"})],
            verbose=True)

        with pytest.raises(YtError):
            # TODO(levysotsky): Error is wrong.
            # Instead of '#' + file it must be '#' + file_id.
            map(in_="//tmp/t_input",
                out=["//tmp/t_output"],
                command="cat my_file; cat",
                file=[to_yson_type("#" + file)],
                spec={"max_failed_job_count": 1},
                verbose=True)

        assert read_table("//tmp/t_output") == [{"value": 42}, {"hello": "world"}]

    @authors("levysotsky")
    def test_file_names_priority(self):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"foo": "bar"})
        create("table", "//tmp/output")

        file1 = "//tmp/file1"
        file2 = "//tmp/file2"
        file3 = "//tmp/file3"
        for f in [file1, file2, file3]:
            create("file", f)
            write_file(f, "{{name=\"{}\"}};\n".format(f))
        set(file2 + "/@file_name", "file2_name_in_attribute")
        set(file3 + "/@file_name", "file3_name_in_attribute")

        map(in_="//tmp/input",
            out="//tmp/output",
            command="cat > /dev/null; cat file1; cat file2_name_in_attribute; cat file3_name_in_path",
            file=[file1, file2, to_yson_type(file3, attributes={"file_name": "file3_name_in_path"})])

        assert read_table("//tmp/output") == [{"name": "//tmp/file1"}, {"name": "//tmp/file2"}, {"name": "//tmp/file3"}]

    @authors("ignat")
    @unix_only
    def test_with_user_files(self):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        file1 = "//tmp/some_file.txt"
        file2 = "//tmp/renamed_file.txt"
        file3 = "//tmp/link_file.txt"

        create("file", file1)
        create("file", file2)

        write_file(file1, "{value=42};\n")
        write_file(file2, "{a=b};\n")
        link(file2, file3)

        create("table", "//tmp/table_file")
        write_table("//tmp/table_file", {"text": "info", "other": "trash"})

        map(in_="//tmp/input",
            out="//tmp/output",
            command="cat > /dev/null; cat some_file.txt; cat my_file.txt; cat table_file;",
            file=[file1, "<file_name=my_file.txt>" + file2, "<format=yson; columns=[text]>//tmp/table_file"])

        assert read_table("//tmp/output") == [{"value": 42}, {"a": "b"}, {"text": "info"}]

        map(in_="//tmp/input",
            out="//tmp/output",
            command="cat > /dev/null; cat link_file.txt; cat my_file.txt;",
            file=[file3, "<file_name=my_file.txt>" + file3])

        assert read_table("//tmp/output") == [{"a": "b"}, {"a": "b"}]

        with pytest.raises(YtError):
            map(in_="//tmp/input",
                out="//tmp/output",
                command="cat",
                file=["<format=invalid_format>//tmp/table_file"])

        # missing format
        with pytest.raises(YtError):
            map(in_="//tmp/input",
                out="//tmp/output",
                command="cat",
                file=["//tmp/table_file"])

    @authors("ignat")
    @unix_only
    def test_empty_user_files(self):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        file1 = "//tmp/empty_file.txt"
        create("file", file1)

        table_file = "//tmp/table_file"
        create("table", table_file)

        command = "cat > /dev/null; cat empty_file.txt; cat table_file"

        map(in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=[file1, "<format=yamr>" + table_file])

        assert read_table("//tmp/output") == []

    @authors("ignat")
    @unix_only
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

        command = "cat > /dev/null; cat regular_file; cat table_file"

        map(in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=[file1, "<format=yson>" + table_file])

        assert read_table("//tmp/output") == [{"value": 42}, {"a": "b"}, {"text": "info"}, {"text": "info"}]

    @authors("ignat")
    def test_erasure_user_files(self):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        create("file", "//tmp/regular_file", attributes={"erasure_coded": "lrc_12_2_2"})
        write_file("<append=true>//tmp/regular_file", "{value=42};\n")
        write_file("<append=true>//tmp/regular_file", "{a=b};\n")

        create("table", "//tmp/table_file", attributes={"erasure_codec": "reed_solomon_6_3"})
        write_table("<append=true>//tmp/table_file", {"text1": "info1"})
        write_table("<append=true>//tmp/table_file", {"text2": "info2"})

        command = "cat > /dev/null; cat regular_file; cat table_file"

        map(in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=["//tmp/regular_file", "<format=yson>//tmp/table_file"])

        assert read_table("//tmp/output") == [{"value": 42}, {"a": "b"}, {"text1": "info1"}, {"text2": "info2"}]

class TestUserFilesMulticell(TestUserFiles):
    NUM_SECONDARY_MASTER_CELLS = 2

@patch_porto_env_only(TestUserFiles)
class TestUserFilesPorto(YTEnvSetup):
    DELTA_NODE_CONFIG = get_porto_delta_node_config()
    REQUIRE_YTSERVER_ROOT_PRIVILEGES = True
    USE_PORTO_FOR_SERVERS = True

##################################################################

class TestSecureVault(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    secure_vault = {
        "int64": 42424243,
        "uint64": YsonUint64(1234),
        "string": "penguin",
        "boolean": True,
        "double": 3.14,
        "composite": {"token1": "SeNsItIvE", "token2": "InFo"},
    }

    def run_map_with_secure_vault(self, spec=None):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")
        merged_spec = {"secure_vault": self.secure_vault, "max_failed_job_count": 1}
        if spec is not None:
            merged_spec = update(merged_spec, spec)
        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec=merged_spec,
            command="""
                echo {YT_SECURE_VAULT=$YT_SECURE_VAULT}\;;
                echo {YT_SECURE_VAULT_int64=$YT_SECURE_VAULT_int64}\;;
                echo {YT_SECURE_VAULT_uint64=$YT_SECURE_VAULT_uint64}\;;
                echo {YT_SECURE_VAULT_string=$YT_SECURE_VAULT_string}\;;
                echo {YT_SECURE_VAULT_boolean=$YT_SECURE_VAULT_boolean}\;;
                echo {YT_SECURE_VAULT_double=$YT_SECURE_VAULT_double}\;;
                echo {YT_SECURE_VAULT_composite=\\"$YT_SECURE_VAULT_composite\\"}\;;
           """)
        return op

    def check_content(self, res):
        assert len(res) == 7
        assert res[0] == {"YT_SECURE_VAULT": self.secure_vault}
        assert res[1] == {"YT_SECURE_VAULT_int64": self.secure_vault["int64"]}
        assert res[2] == {"YT_SECURE_VAULT_uint64": self.secure_vault["uint64"]}
        assert res[3] == {"YT_SECURE_VAULT_string": self.secure_vault["string"]}
        # Boolean values are represented with 0/1.
        assert res[4] == {"YT_SECURE_VAULT_boolean": 1}
        assert res[5] == {"YT_SECURE_VAULT_double": self.secure_vault["double"]}
        # Composite values are not exported as separate environment variables.
        assert res[6] == {"YT_SECURE_VAULT_composite": ""}


    @authors("ignat")
    def test_secure_vault_not_visible(self):
        op = self.run_map_with_secure_vault()
        cypress_info = str(op.get_path() + "/@")
        scheduler_info = str(get("//sys/scheduler/orchid/scheduler/operations/{0}".format(op.id)))
        op.track()

        # Check that secure environment variables is neither presented in the Cypress node of the
        # operation nor in scheduler Orchid representation of the operation.
        for info in [cypress_info, scheduler_info]:
            for sensible_text in ["42424243", "SeNsItIvE", "InFo"]:
                assert info.find(sensible_text) == -1

    @authors("ignat")
    def test_secure_vault_simple(self):
        op = self.run_map_with_secure_vault()
        op.track()
        res = read_table("//tmp/t_out")
        self.check_content(res)

    @authors("ignat")
    def test_secure_vault_with_revive(self):
        op = self.run_map_with_secure_vault()
        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass
        op.track()
        res = read_table("//tmp/t_out")
        self.check_content(res)

    @authors("ignat")
    def test_secure_vault_with_revive_with_new_storage_scheme(self):
        op = self.run_map_with_secure_vault(spec={"enable_compatible_storage_mode": True})
        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass
        op.track()
        res = read_table("//tmp/t_out")
        self.check_content(res)

    @authors("ignat")
    def test_allowed_variable_names(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")
        with pytest.raises(YtError):
            map(track=False,
                in_="//tmp/t_in",
                out="//tmp/t_out",
                spec={"secure_vault": {"=_=": 42}},
                command="cat")
        with pytest.raises(YtError):
            map(track=False,
                in_="//tmp/t_in",
                out="//tmp/t_out",
                spec={"secure_vault": {"x" * (2**16 + 1): 42}},
                command="cat")
