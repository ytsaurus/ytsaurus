from yt_env_setup import YTEnvSetup, Restarter, SCHEDULERS_SERVICE


from yt_commands import (
    authors, print_debug, wait, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, create,
    ls, get, set, remove, link, exists, create_network_project, create_tmpdir,
    create_user, make_ace, start_transaction, lock,
    write_file, read_table,
    write_table, map,
    vanilla, run_test_vanilla, abort_job,
    list_jobs, get_job, get_job_stderr,
    sync_create_cells, get_singular_chunk_id,
    update_nodes_dynamic_config, set_node_banned, check_all_stderrs, get_statistics,
    repair_exec_node,
    make_random_string, raises_yt_error, update_controller_agent_config)


import yt_error_codes

import yt.environment.init_operation_archive as init_operation_archive
import yt.yson as yson
from yt.test_helpers.profiler import Profiler
from yt.test_helpers import are_almost_equal
from yt.common import update, YtError

from yt.packages.six.moves import xrange

from flaky import flaky

import pytest
import time
import datetime
import os

##################################################################


class TestSandboxTmpfs(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    USE_PORTO = True

    DELTA_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1,
            "default_file_replication_factor": 1,
        }
    }

    @classmethod
    def modify_node_config(cls, config):
        if not os.path.exists(cls.default_disk_path):
            os.makedirs(cls.default_disk_path)
        config["exec_agent"]["slot_manager"]["locations"][0]["path"] = cls.default_disk_path

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
                "max_failed_job_count": 1,
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        content = op.read_stderr(job_ids[0])

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
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        content = op.read_stderr(job_ids[0])
        words = content.strip().split()
        assert ["file", "content"] == words

    @authors("ignat")
    def test_tmpfs_profiling(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        map(
            track=False,
            command="cat; echo 'content' > tmpfs/file; sleep 1000;",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": "tmpfs",
                },
                "max_failed_job_count": 1,
            },
        )

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node = nodes[0]

        tmpfs_size = Profiler.at_node(self.Env.create_native_client(), node).gauge("job_controller/tmpfs/size")
        tmpfs_usage = Profiler.at_node(self.Env.create_native_client(), node).gauge("job_controller/tmpfs/usage")
        wait(lambda: tmpfs_size.get() == 1024 * 1024)
        wait(lambda: tmpfs_usage.get() > 0)
        assert tmpfs_usage.get() <= 4 * 1024

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
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        content = op.read_stderr(job_ids[0])
        words = content.strip().split()
        assert ["file", "content"] == words

        create("file", "//tmp/test_file")
        write_file("//tmp/test_file", "".join(["0"] * (1024 * 1024 + 1)))
        map(
            command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": ".",
                    "file_paths": ["//tmp/test_file"],
                }
            },
        )

        map(
            command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": "./",
                    "file_paths": ["//tmp/test_file"],
                }
            },
        )

        script = (
            "#!/usr/bin/env python\n"
            "import sys\n"
            "sys.stdout.write(sys.stdin.read())\n"
            "with open('test_file', 'w') as f: f.write('Hello world!')"
        )
        create("file", "//tmp/script")
        write_file("//tmp/script", script)
        set("//tmp/script/@executable", True)

        map(
            command="./script.py",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 100 * 1024 * 1024,
                    "tmpfs_path": ".",
                    "copy_files": True,
                    "file_paths": [
                        "//tmp/test_file",
                        yson.to_yson_type("//tmp/script", attributes={"file_name": "script.py"}),
                    ],
                }
            },
        )

        with pytest.raises(YtError):
            map(
                command="cat; cp test_file local_file;",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": ".",
                        "file_paths": ["//tmp/test_file"],
                    },
                    "max_failed_job_count": 1,
                },
            )

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
            },
        )

        statistics = get(op.get_path() + "/@progress/job_statistics")
        tmpfs_size = get_statistics(statistics, "user_job.tmpfs_volumes.0.max_size.$.completed.map.sum")
        assert 0.9 * 1024 * 1024 <= tmpfs_size <= 1.1 * 1024 * 1024

        with pytest.raises(YtError):
            map(
                command="cat",
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
                },
            )

        # Per-file `copy_file' attribute has higher priority than `copy_files' in spec.
        with pytest.raises(YtError):
            map(
                command="cat",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": ".",
                        "file_paths": ["<copy_file=%true>//tmp/test_file"],
                        "copy_files": False,
                    },
                    "max_failed_job_count": 1,
                },
            )
        map(
            command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": ".",
                    "file_paths": ["<copy_file=%false>//tmp/test_file"],
                    "copy_files": True,
                },
                "max_failed_job_count": 1,
            },
        )

    @authors("ignat")
    def test_incorrect_tmpfs_path(self):
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
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": "../",
                    }
                },
            )

        with pytest.raises(YtError):
            map(
                command="cat",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": "/tmp",
                    }
                },
            )

    @authors("ignat")
    def test_tmpfs_remove_failed(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        with pytest.raises(YtError):
            map(
                command="cat; rm -rf tmpfs",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 1024 * 1024,
                        "tmpfs_path": "tmpfs",
                    },
                    "max_failed_job_count": 1,
                },
            )

    @authors("ignat")
    def test_tmpfs_size_limit(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        with pytest.raises(YtError):
            map(
                command="set -e; cat; dd if=/dev/zero of=tmpfs/file bs=1100000 count=1",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={"mapper": {"tmpfs_size": 1024 * 1024}, "max_failed_job_count": 1},
            )

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
                "mapper": {"tmpfs_path": "tmpfs", "memory_limit": 250 * 1000 * 1000},
                "max_failed_job_count": 1,
            },
        )

        assert get(op.get_path() + "/@progress/jobs/aborted/total") == 0

    @authors("gritukan")
    def test_mmaped_file_memory_accounting(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        mapper = """
#!/usr/bin/python

import mmap, time

f = open('tmpfs/f', 'r+b')
mm = mmap.mmap(f.fileno(), 0)

s = mm.read()

time.sleep(10)
"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)
        set("//tmp/mapper.py/@executable", True)

        # String is in process' memory twice: one copy is a mmaped tmpfs file and one copy is a local variable s.
        # Process' mmap of tmpfs should not be counted.
        op = map(
            command="fallocate -l 200M tmpfs/f; python3 mapper.py",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            file="//tmp/mapper.py",
            spec={
                "mapper": {
                    "tmpfs_path": "tmpfs",
                    "memory_limit": 430 * 1024 * 1024,
                    "use_smaps_memory_tracker": True,
                },
                "max_failed_job_count": 1,
            },
        )
        op.track()
        assert get(op.get_path() + "/@progress/jobs/aborted/total") == 0

        statistics = get(op.get_path() + "/@progress/job_statistics")
        assert get_statistics(statistics, "user_job.max_memory.$.completed.map.sum") > 200 * 1024 * 1024

        # Smaps memory tracker is disabled. Job should fail.
        with pytest.raises(YtError):
            memory_limit = 430 * 1024 * 1024
            op = map(
                command="fallocate -l 200M tmpfs/f; python3 mapper.py",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                file="//tmp/mapper.py",
                spec={
                    "mapper": {
                        "tmpfs_path": "tmpfs",
                        "memory_limit": memory_limit,
                        "use_smaps_memory_tracker": False,
                    },
                    "max_failed_job_count": 1,
                },
            )
            statistics = get(op.get_path() + "/@progress/job_statistics")
            assert get_statistics(statistics, "user_job.max_memory.$.failed.map.sum") > memory_limit

        # String is in memory twice: one copy is mmaped non-tmpfs file and one copy is a local variable s.
        # Both allocations should be counted.
        with pytest.raises(YtError):
            memory_limit = 300 * 1024 * 1024
            op = map(
                command="fallocate -l 200M tmpfs/f; python3 mapper.py",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                file="//tmp/mapper.py",
                spec={
                    "mapper": {
                        "tmpfs_path": "other_tmpfs",
                        "memory_limit": memory_limit,
                        "use_smaps_memory_tracker": True,
                    },
                    "max_failed_job_count": 1,
                },
            )
            statistics = get(op.get_path() + "/@progress/job_statistics")
            assert get_statistics(statistics, "user_job.max_memory.$.failed.map.sum") > memory_limit

    @authors("psushin")
    def test_inner_files(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        create("file", "//tmp/file.txt")
        write_file("//tmp/file.txt", "{trump = moron};\n")

        map(
            command="cat; cat ./tmpfs/trump.txt",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            file=['<file_name="./tmpfs/trump.txt">//tmp/file.txt'],
            spec={
                "mapper": {
                    "tmpfs_path": "tmpfs",
                    "tmpfs_size": 1024 * 1024,
                },
                "max_failed_job_count": 1,
            },
        )

        assert get("//tmp/t_output/@row_count") == 2

    @authors("ignat")
    def test_multiple_tmpfs_volumes(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="cat; "
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
                "max_failed_job_count": 1,
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        content = op.read_stderr(job_ids[0])
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
                    "max_failed_job_count": 1,
                },
            )

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
                    "max_failed_job_count": 1,
                },
            )

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
                    "max_failed_job_count": 1,
                },
            )

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
                    "max_failed_job_count": 1,
                },
            )

    @authors("ignat")
    def test_multiple_tmpfs_volumes_with_common_prefix(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            command="cat; "
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
                "max_failed_job_count": 1,
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        content = op.read_stderr(job_ids[0])
        words = content.strip().split()
        assert ["file", "content_1", "file", "content_2"] == words

    @authors("ignat")
    def test_vanilla(self):
        vanilla(
            spec={
                "tasks": {
                    "a": {"job_count": 2, "command": "sleep 5", "tmpfs_volumes": []},
                    "b": {
                        "job_count": 1,
                        "command": "sleep 10",
                        "tmpfs_volumes": [
                            {
                                "path": "tmpfs",
                                "size": 1024 * 1024,
                            },
                        ],
                    },
                    "c": {
                        "job_count": 3,
                        "command": "sleep 15",
                        "tmpfs_volumes": [
                            {
                                "path": "tmpfs",
                                "size": 1024 * 1024,
                            },
                            {
                                "path": "other_tmpfs",
                                "size": 1024 * 1024,
                            },
                        ],
                    },
                },
            }
        )

    @authors("gritukan")
    def test_tmpfs_sandbox_and_disk_space_limit(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        # Should not apply disk space limit to sandbox.
        map(
            command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "tmpfs_size": 1024 * 1024,
                    "tmpfs_path": ".",
                    "disk_space_limit": 1024 * 1024,
                },
                "max_failed_job_count": 1,
            },
        )


##################################################################


class TestSandboxTmpfsOverflow(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True
    USE_PORTO = True
    DELTA_NODE_CONFIG = {
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
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
            "enable_job_stderr_reporter": True,
        }
    }

    def setup(self):
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )
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
                "python -c 'import time; x = \"A\" * (200 * 1024 * 1024); time.sleep(100);'"
            ),
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
                "max_failed_job_count": 1,
            },
        )

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

    DELTA_NODE_CONFIG = {"exec_agent": {"slot_manager": {"enable_tmpfs": False}}}

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
            },
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        content = op.read_stderr(job_ids[0])
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
        create(
            "file",
            "//tmp/script",
            attributes={"replication_factor": 1, "executable": True},
        )
        write_file("//tmp/script", "#!/bin/bash\ncat")

        chunk_id = get_singular_chunk_id("//tmp/script")

        replicas = get("#{0}/@stored_replicas".format(chunk_id))
        assert len(replicas) == 1
        replica_to_ban = str(replicas[0])  # str() is for attribute stripping.

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
        op = map(
            track=False,
            command="./script",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={"mapper": {"file_paths": ["//tmp/script"]}},
        )

        wait(lambda: op.get_job_count("running") == 1)

        time.sleep(1)
        op.abort()

        wait(
            lambda: op.get_state() == "aborted"
            and are_almost_equal(get("//sys/scheduler/orchid/scheduler/cell/resource_usage/cpu"), 0)
        )

    @authors("gritukan")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_rename_columns_artifact_table(self, optimize_for):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "x", "type": "int64"}],
            "optimize_for": optimize_for,
        })

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t", [{"x": 42}])
        write_table("//tmp/t_in", [{"a": "b"}])

        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            file=["<format=<format=text>yson;rename_columns={x=y}>//tmp/t"],
            command="cat t",
            spec={"mapper": {"format": yson.loads("<format=text>yson")}},
        )

        assert read_table("//tmp/t_out") == [{"y": 42}]


##################################################################


class TestArtifactCacheBypass(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    USE_PORTO = True

    @authors("babenko")
    def test_bypass_artifact_cache_for_file(self):
        counters = [
            Profiler.at_node(self.Env.create_native_client(), node).counter("job_controller/chunk_cache/cache_bypassed_artifacts_size")
            for node in sorted(ls("//sys/cluster_nodes"))
        ]

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        create("file", "//tmp/file")
        write_file("//tmp/file", '{"hello": "world"}')
        op = map(
            command="cat file",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "file_paths": ["<bypass_artifact_cache=%true>//tmp/file"],
                    "output_format": "json",
                },
                "max_failed_job_count": 1,
            },
        )

        statistics = get(op.get_path() + "/@progress/job_statistics")
        bypassed_size = get_statistics(
            statistics,
            "exec_agent.artifacts.cache_bypassed_artifacts_size.$.completed.map.sum")
        assert bypassed_size == 18

        wait(lambda: sum(counter.get_delta() for counter in counters) == 18)

        assert read_table("//tmp/t_output") == [{"hello": "world"}]

    @authors("babenko")
    def test_bypass_artifact_cache_for_table(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        create("table", "//tmp/table")
        write_table("//tmp/table", [{"hello": "world"}])
        map(
            command="cat table",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "file_paths": ["<bypass_artifact_cache=%true;format=json>//tmp/table"],
                    "output_format": "json",
                },
                "max_failed_job_count": 1,
            },
        )

        assert read_table("//tmp/t_output") == [{"hello": "world"}]

    @authors("gritukan")
    def test_sandbox_in_tmpfs_overflow(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        create("file", "//tmp/file")
        write_file("//tmp/file", "A" * 10 ** 7)

        with raises_yt_error(yt_error_codes.TmpfsOverflow):
            map(
                command="cat table",
                in_="//tmp/t_input",
                out="//tmp/t_output",
                spec={
                    "mapper": {
                        "tmpfs_size": 5 * 1024 * 1024,
                        "tmpfs_path": ".",
                        "file_paths": ["<bypass_artifact_cache=%true>//tmp/file"],
                        "output_format": "json",
                    },
                    "max_failed_job_count": 1,
                },
            )
        # In tests we crash if slot location is disabled.
        # Thus, if this test passed successfully, location was not disabled.

    @authors("gritukan")
    @pytest.mark.parametrize("bypass_artifact_cache", [False, True])
    def test_lost_artifact(self, bypass_artifact_cache):
        update_nodes_dynamic_config({
            "data_node": {
                "artifact_cache_reader": {
                    "retry_count": 1,
                    "min_backoff_time": 100,
                },
            },
        })

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        create("file", "//tmp/file", attributes={"replication_factor": 1})
        write_file("//tmp/file", "A" * 100)

        chunk_id = get_singular_chunk_id("//tmp/file")
        replica = str(get("#{}/@stored_replicas/0".format(chunk_id)))
        set_node_banned(replica, True)

        if bypass_artifact_cache:
            file_path = "<bypass_artifact_cache=%true>//tmp/file"
        else:
            file_path = "//tmp/file"

        op = map(
            track=False,
            command="cat table",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "file_paths": [file_path],
                    "output_format": "json",
                },
                "max_failed_job_count": 1,
            },
        )

        # Job should be aborted and location should not be disabled
        def check():
            try:
                return get("{0}/@progress/jobs/aborted/total".format(op.get_path())) > 0
            except YtError:
                return False
        wait(check, sleep_backoff=0.6)

        op.abort()


##################################################################


class TestUserJobIsolation(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                    "test_network": True,  # Deprecated from 21.2+
                    "porto_executor": {
                        "enable_network_isolation": False
                    }
                },
            }
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "vanilla_operation_options": {
                # NB: ytserver-exec requires many threads on start.
                "user_job_options": {
                    "thread_limit_multiplier": 5,
                    "initial_thread_limit": 100,
                },
            },
        },
    }

    USE_PORTO = True

    @authors("gritukan")
    def test_create_network_project_map(self):
        create("network_project_map", "//tmp/n")

    @authors("gritukan")
    def test_network_project_in_spec(self):
        create_user("u1")
        create_user("u2")
        create_network_project("n")
        set("//sys/network_projects/n/@project_id", 0xDEADBEEF)
        set("//sys/network_projects/n/@acl", [make_ace("allow", "u1", "use")])

        # Non-existent network project. Job should fail.
        with pytest.raises(YtError):
            op = run_test_vanilla("true", task_patch={"network_project": "x"})
            op.track()

        # User `u2` is not allowed to use `n`. Job should fail.
        with pytest.raises(YtError):
            op = run_test_vanilla("true", task_patch={"network_project": "n"}, authenticated_user="u2")
            op.track()

        op = run_test_vanilla(
            with_breakpoint("echo $YT_NETWORK_PROJECT_ID >&2; hostname >&2; BREAKPOINT"),
            task_patch={"network_project": "n"},
            authenticated_user="u1",
        )

        job_id = wait_breakpoint()[0]
        network_project_id, hostname, _ = get_job_stderr(op.id, job_id).split("\n")
        assert network_project_id == str(0xDEADBEEF)
        assert hostname.startswith("slot_")
        release_breakpoint()
        op.track()

    @authors("gritukan")
    def test_hostname_in_etc_hosts(self):
        create_network_project("n")
        set("//sys/network_projects/n/@project_id", 0xDEADBEEF)

        op = run_test_vanilla(
            with_breakpoint("getent hosts $(hostname) | awk '{ print $1 }' >&2; BREAKPOINT"),
            task_patch={"network_project": "n"},
        )

        job_id = wait_breakpoint()[0]
        assert "dead:beef" in get_job_stderr(op.id, job_id)

        release_breakpoint()
        op.track()

    @authors("gritukan")
    def test_thread_limit(self):
        def run_fork_bomb(thread_count):
            cmd = "for i in $(seq 1 {}); do nohup sleep 10 & done; wait".format(thread_count)
            op = run_test_vanilla(cmd, spec={"max_failed_job_count": 1})
            op.track()

        run_fork_bomb(50)
        with pytest.raises(YtError):
            run_fork_bomb(200)

    @authors("gritukan")
    def test_thread_count_statistics(self):
        cmd = "for i in $(seq 1 32); do nohup sleep 5 & done; wait"
        op = run_test_vanilla(cmd, spec={"max_failed_job_count": 1})
        op.track()

        statistics = get(op.get_path() + "/@progress/job_statistics")
        thread_count = get_statistics(statistics, "user_job.cpu.peak_thread_count.$.completed.task.max")

        assert 32 <= thread_count <= 42

##################################################################


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

        command = "cat > /dev/null;\n"\
                  "echo stderr 1>&2;\n"\
                  "echo \"{operation=\"\'\"\'$YT_OPERATION_ID\'\"\'}\';\';\n"\
                  "echo \"{job_index=$YT_JOB_INDEX}\";"

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
            spec={"max_failed_job_count": 5},
        )

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
            command='cat > /dev/null; python -c \'print "head" + "0" * 10000000; print "1" * 10000000 + "tail"\' >&2;',
            spec={"max_failed_job_count": 1, "mapper": {"max_stderr_size": 1000000}},
        )

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        stderr = op.read_stderr(job_ids[0]).strip()

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
            spec={"job_count": 2, "max_stderr_count": 0},
        )

        def enough_jobs_completed():
            if not exists(op.get_path() + "/@progress"):
                return False
            progress = get(op.get_path() + "/@progress")
            if "jobs" in progress and "completed" in progress["jobs"]:
                return progress["jobs"]["completed"]["total"] >= 1
            return False

        wait(enough_jobs_completed, sleep_backoff=0.6)

        stderr_tx = get(op.get_path() + "/@async_scheduler_transaction_id")
        staged_objects = get("//sys/transactions/{0}/@staged_object_ids".format(stderr_tx))
        assert sum(len(ids) for ids in staged_objects.values()) == 0, str(staged_objects)

    @authors("ignat")
    def test_stderr_of_failed_jobs(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"row_id": "row_" + str(i)} for i in xrange(20)])

        command = with_breakpoint(
            """
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
                fi;""".format(
                lock_dir=events_on_fs()._get_event_filename("lock_dir")
            )
        )
        op = map(
            track=False,
            label="stderr_of_failed_jobs",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=command,
            spec={"max_failed_job_count": 1, "max_stderr_count": 10, "job_count": 20},
        )

        release_breakpoint()
        with pytest.raises(YtError):
            op.track()

        # The default number of stderr is 10. We check that we have 11-st stderr of failed job,
        # that is last one.
        check_all_stderrs(op, "stderr\n", 11)


class TestJobStderrMulticell(TestJobStderr):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestJobStderrPorto(TestJobStderr):
    USE_PORTO = True


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
    @pytest.mark.parametrize("copy_files", [False, True])
    def test_file_with_integer_name(self, copy_files):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("//tmp/t_input", [{"hello": "world"}])

        file = "//tmp/1000"
        create("file", file)
        write_file(file, "{value=42};\n")

        map(
            in_="//tmp/t_input",
            out=["//tmp/t_output"],
            command="cat 1000 >&2; cat",
            file=[file],
            verbose=True,
            spec={"mapper": {"copy_files": copy_files}},
        )

        assert read_table("//tmp/t_output") == [{"hello": "world"}]

    @authors("ignat")
    @pytest.mark.parametrize("copy_files", [False, True])
    def test_file_with_subdir(self, copy_files):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("//tmp/t_input", [{"hello": "world"}])

        file = "//tmp/test_file"
        create("file", file)
        write_file(file, "{value=42};\n")

        map(
            in_="//tmp/t_input",
            out=["//tmp/t_output"],
            command="cat dir/my_file >&2; cat",
            file=[yson.to_yson_type("//tmp/test_file", attributes={"file_name": "dir/my_file"})],
            spec={"mapper": {"copy_files": copy_files}},
            verbose=True,
        )

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_input",
                out=["//tmp/t_output"],
                command="cat dir/my_file >&2; cat",
                file=[yson.to_yson_type("//tmp/test_file", attributes={"file_name": "../dir/my_file"})],
                spec={"max_failed_job_count": 1, "mapper": {"copy_files": copy_files}},
                verbose=True,
            )

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

        map(
            in_="//tmp/t_input",
            out=["//tmp/t_output"],
            command="cat my_file; cat",
            file=[yson.to_yson_type("#" + file_id, attributes={"file_name": "my_file"})],
            verbose=True,
        )

        with pytest.raises(YtError):
            # TODO(levysotsky): Error is wrong.
            # Instead of '#' + file it must be '#' + file_id.
            map(
                in_="//tmp/t_input",
                out=["//tmp/t_output"],
                command="cat my_file; cat",
                file=[yson.to_yson_type("#" + file)],
                spec={"max_failed_job_count": 1},
                verbose=True,
            )

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
            write_file(f, '{{name="{}"}};\n'.format(f))
        set(file2 + "/@file_name", "file2_name_in_attribute")
        set(file3 + "/@file_name", "file3_name_in_attribute")

        map(
            in_="//tmp/input",
            out="//tmp/output",
            command="cat > /dev/null; cat file1; cat file2_name_in_attribute; cat file3_name_in_path",
            file=[
                file1,
                file2,
                yson.to_yson_type(file3, attributes={"file_name": "file3_name_in_path"}),
            ],
        )

        assert read_table("//tmp/output") == [
            {"name": "//tmp/file1"},
            {"name": "//tmp/file2"},
            {"name": "//tmp/file3"},
        ]

    @authors("ignat")
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

        map(
            in_="//tmp/input",
            out="//tmp/output",
            command="cat > /dev/null; cat some_file.txt; cat my_file.txt; cat table_file;",
            file=[
                file1,
                "<file_name=my_file.txt>" + file2,
                "<format=yson; columns=[text]>//tmp/table_file",
            ],
        )

        assert read_table("//tmp/output") == [
            {"value": 42},
            {"a": "b"},
            {"text": "info"},
        ]

        map(
            in_="//tmp/input",
            out="//tmp/output",
            command="cat > /dev/null; cat link_file.txt; cat my_file.txt;",
            file=[file3, "<file_name=my_file.txt>" + file3],
        )

        assert read_table("//tmp/output") == [{"a": "b"}, {"a": "b"}]

        with pytest.raises(YtError):
            map(
                in_="//tmp/input",
                out="//tmp/output",
                command="cat",
                file=["<format=invalid_format>//tmp/table_file"],
            )

        # missing format
        with pytest.raises(YtError):
            map(
                in_="//tmp/input",
                out="//tmp/output",
                command="cat",
                file=["//tmp/table_file"],
            )

    @authors("ignat")
    def test_empty_user_files(self):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        file1 = "//tmp/empty_file.txt"
        create("file", file1)

        table_file = "//tmp/table_file"
        create("table", table_file)

        command = "cat > /dev/null; cat empty_file.txt; cat table_file"

        map(
            in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=[file1, "<format=yamr>" + table_file],
        )

        assert read_table("//tmp/output") == []

    @authors("ignat")
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

        map(
            in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=[file1, "<format=yson>" + table_file],
        )

        assert read_table("//tmp/output") == [
            {"value": 42},
            {"a": "b"},
            {"text": "info"},
            {"text": "info"},
        ]

    @authors("ignat")
    @pytest.mark.parametrize("erasure_codec", ["reed_solomon_6_3", "isa_reed_solomon_6_3"])
    def test_erasure_user_files(self, erasure_codec):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"foo": "bar"})

        create("table", "//tmp/output")

        create("file", "//tmp/regular_file", attributes={"erasure_coded": "lrc_12_2_2"})
        write_file("<append=true>//tmp/regular_file", "{value=42};\n")
        write_file("<append=true>//tmp/regular_file", "{a=b};\n")

        create(
            "table",
            "//tmp/table_file",
            attributes={"erasure_codec": erasure_codec},
        )
        write_table("<append=true>//tmp/table_file", {"text1": "info1"})
        write_table("<append=true>//tmp/table_file", {"text2": "info2"})

        command = "cat > /dev/null; cat regular_file; cat table_file"

        map(
            in_="//tmp/input",
            out="//tmp/output",
            command=command,
            file=["//tmp/regular_file", "<format=yson>//tmp/table_file"],
        )

        assert read_table("//tmp/output") == [
            {"value": 42},
            {"a": "b"},
            {"text1": "info1"},
            {"text2": "info2"},
        ]


class TestUserFilesMulticell(TestUserFiles):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestUserFilesPorto(TestUserFiles):
    USE_PORTO = True


##################################################################


class TestSecureVault(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    secure_vault = {
        "int64": 42424243,
        "uint64": yson.YsonUint64(1234),
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
                echo -e "{YT_SECURE_VAULT=$YT_SECURE_VAULT};"
                echo -e "{YT_SECURE_VAULT_int64=$YT_SECURE_VAULT_int64};"
                echo -e "{YT_SECURE_VAULT_uint64=$YT_SECURE_VAULT_uint64};"
                echo -e "{YT_SECURE_VAULT_string=$YT_SECURE_VAULT_string};"
                echo -e "{YT_SECURE_VAULT_boolean=$YT_SECURE_VAULT_boolean};"
                echo -e "{YT_SECURE_VAULT_double=$YT_SECURE_VAULT_double};"
                echo -e "{YT_SECURE_VAULT_composite=\\"$YT_SECURE_VAULT_composite\\"};"
           """,
        )
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
            map(
                track=False,
                in_="//tmp/t_in",
                out="//tmp/t_out",
                spec={"secure_vault": {"=_=": 42}},
                command="cat",
            )
        with pytest.raises(YtError):
            map(
                track=False,
                in_="//tmp/t_in",
                out="//tmp/t_out",
                spec={"secure_vault": {"x" * (2 ** 16 + 1): 42}},
                command="cat",
            )

    @authors("gepardo")
    def test_secure_vault_limit(self):
        secure_vault_len = len(yson.dumps(self.secure_vault))

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")

        update_controller_agent_config("secure_vault_length_limit", secure_vault_len - 1)
        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                spec={"secure_vault": self.secure_vault},
                command="cat",
            )

        update_controller_agent_config("secure_vault_length_limit", secure_vault_len)
        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"secure_vault": self.secure_vault},
            command="cat",
        )


##################################################################


class TestUserJobMonitoring(YTEnvSetup):
    USE_DYNAMIC_TABLES = True
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    USE_PORTO = True

    PROFILING_PERIOD = 5 * 1000

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_proxy_heartbeat_period": 100,
            "job_reporter": {
                "enabled": True,
                "reporting_period": 10,
                "min_repeat_delay": 10,
                "max_repeat_delay": 10,
            },
            "job_controller": {
                "resource_limits": {
                    "user_slots": 20,
                    "cpu": 20,
                },
                "gpu_manager": {"test_resource": True, "test_gpu_count": 8},
            },
        },
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
            "enable_job_stderr_reporter": True,
            "alerts_update_period": 100,
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "user_job_monitoring": {
                "max_monitored_user_jobs_per_operation": 2,
                "max_monitored_user_jobs_per_agent": 7,
            },
        },
    }

    def setup(self):
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(
            self.Env.create_native_client(),
            override_tablet_cell_bundle="default",
        )

    @authors("levysotsky")
    @pytest.mark.parametrize("default_sensor_names", [True, False])
    def test_basic(self, default_sensor_names):
        monitoring_config = {
            "enable": True,
        }
        if not default_sensor_names:
            monitoring_config["sensor_names"] = ["cpu/user", "gpu/utilization_power"]

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT; for (( c=1; c>0; c++ )); do : ; done"),
            job_count=1,
            task_patch={
                "monitoring": monitoring_config,
                "gpu_limit": 1,
                "enable_gpu_layers": False,
            },
        )
        job_id, = wait_breakpoint()
        release_breakpoint()

        expected_time = 10 * 1000  # 10 seconds.
        job_info = get_job(op.id, job_id)
        node = job_info["address"]
        descriptor = job_info["monitoring_descriptor"]

        profiler = Profiler.at_node(self.Env.create_native_client(), node)

        cpu_user_counter = profiler.counter("user_job/cpu/user", tags={"job_descriptor": descriptor})
        wait(lambda: cpu_user_counter.get_delta() >= expected_time)

        controller_agent_address = get(op.get_path() + "/@controller_agent_address")
        controller_agent_orchid = "//sys/controller_agents/instances/{}/orchid/controller_agent".format(
            controller_agent_address
        )
        incarnation_id = get("{}/incarnation_id".format(controller_agent_orchid))
        expected_job_descriptor = "{}/0".format(incarnation_id)
        assert descriptor == expected_job_descriptor

        for _ in range(10):
            time.sleep(0.5)
            assert profiler.get(
                "user_job/gpu/utilization_power",
                {"job_descriptor": expected_job_descriptor, "gpu_slot": "0"},
                postprocessor=float) == 0

    @authors("levysotsky")
    def test_limits(self):
        controller_agents = ls("//sys/controller_agents/instances")
        assert len(controller_agents) == 1
        agent_alerts_path = "//sys/controller_agents/instances/{}/@alerts".format(
            controller_agents[0]
        )

        def get_agent_alerts():
            return get(agent_alerts_path)

        def run_op(job_count):
            breakpoint_name = make_random_string()
            op = run_test_vanilla(
                with_breakpoint("BREAKPOINT", breakpoint_name=breakpoint_name),
                job_count=job_count,
                task_patch={
                    "monitoring": {
                        "enable": True,
                        "sensor_names": ["cpu/user"],
                    },
                },
            )
            wait_breakpoint(breakpoint_name=breakpoint_name, job_count=job_count)
            op.breakpoint_name = breakpoint_name
            return op

        op = run_op(2)

        # No alerts here as limit per operation is 2.
        @wait_assert
        def no_alerts():
            assert len(ls(op.get_path() + "/@alerts")) == 0
            assert len(get_agent_alerts()) == 0

        release_breakpoint(breakpoint_name=op.breakpoint_name)
        op.track()

        op = run_op(3)

        # Expect an alert here as limit per operation is 2.
        @wait_assert
        def op_alert():
            assert ls(op.get_path() + "/@alerts") == ["user_job_monitoring_limited"]
            assert len(get_agent_alerts()) == 0

        release_breakpoint(breakpoint_name=op.breakpoint_name)
        op.track()

        ops = [run_op(2) for _ in range(3)]

        @wait_assert  # noqa: F811
        def no_alerts():
            for op in ops:
                # Expect no alerts here as limit per agent is 7.
                assert len(ls(op.get_path() + "/@alerts")) == 0
                assert len(get_agent_alerts()) == 0

        next_op = run_op(2)

        # Expect an alert here as limit per agent is 7.
        @wait_assert
        def agent_alert():
            assert ls(next_op.get_path() + "/@alerts") == ["user_job_monitoring_limited"]
            assert len(get_agent_alerts()) == 1
            assert "Limit of monitored user jobs per controller agent reached" in get_agent_alerts()[0]["message"]

        for op in ops:
            release_breakpoint(breakpoint_name=op.breakpoint_name)
            op.track()

        for job in list_jobs(next_op.id)["jobs"]:
            abort_job(job["id"])
        wait_breakpoint(breakpoint_name=next_op.breakpoint_name, job_count=2)

        @wait_assert  # noqa: F811
        def no_alerts():
            assert len(ls(op.get_path() + "/@alerts")) == 0
            assert len(get_agent_alerts()) == 0

        release_breakpoint(breakpoint_name=next_op.breakpoint_name)
        next_op.track()

    @authors("levysotsky")
    def test_bad_spec(self):
        with pytest.raises(YtError):
            run_test_vanilla(
                "echo",
                track=True,
                job_count=1,
                task_patch={
                    "monitoring": {
                        "enable": True,
                        "sensor_names": ["nonexistent_sensor"],
                    },
                },
            )


##################################################################


class TestRepairExecNode(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    USE_PORTO = True

    DELTA_NODE_CONFIG = {
        "data_node": {
            "disk_health_checker": {
                "check_period": 1000,
            },
        },
        "logging": {
            "abort_on_alert": False,
        },
    }

    @authors("alexkolodezny")
    def test_repair_exec_node(self):
        update_nodes_dynamic_config({"data_node": {"terminate_on_location_disable": False}})

        node_id = list(get("//sys/cluster_nodes"))[0]

        locations = get("//sys/cluster_nodes/{0}/orchid/config/exec_agent/slot_manager/locations".format(node_id))

        for location in locations:
            with open("{}/disabled".format(location["path"]), "w") as f:
                f.write("{foo=bar}")

        def is_disabled():
            with raises_yt_error(required=False) as err:
                op = run_test_vanilla("sleep 0.1")
                op.track()
            return len(err) > 0

        wait(is_disabled)

        with raises_yt_error(yt_error_codes.NoOnlineNodeToScheduleJob):
            op = run_test_vanilla("sleep 0.1")
            op.track()

        with raises_yt_error('Unknown location: "[unknownslot]"'):
            repair_exec_node(node_id, ["unknownslot"])

        with raises_yt_error("Lock file is found"):
            repair_exec_node(node_id, ["slot0"])

        for location in locations:
            os.remove("{}/disabled".format(location["path"]))

        repair_exec_node(node_id, ["slot0"])

        wait(lambda: not is_disabled())


##################################################################


class TestArtifactInvalidFormat(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    USE_PORTO = True

    def _prepare_tables(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"first": "second"})
        create("table", "//tmp/table")
        write_table("//tmp/table", [{"key_first": "value_first", "key_second": 42},
                                    {"key_first": "Value_second", "key_second": 43}])

    def _run_map_with_format(self, fmt):
        map(
            command='echo "{\\"key1\\": \\"value1\\", \\"size\\": $(wc -l <table)}"',
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "mapper": {
                    "file_paths": ["<format=" + fmt + ">//tmp/table"],
                    "output_format": "json"
                },
                "max_failed_job_count": 1,
            },
        )

    @authors("gepardo")
    def test_bad_skiff(self):
        # This test reproduces YT-14726.
        self._prepare_tables()
        bad_skiff_format = """<
    "table_skiff_schemas" = [
        "$table"
    ];
    "skiff_schema_registry" = {
        "table" = {
            "children" = [
                {
                    "name" = "key_first";
                    "wire_type" = "string32"
                };
                {
                    "wire_type" = "int64"
                };
            ];
            "wire_type" = "tuple"
        }
    }
>skiff"""
        with raises_yt_error(yt_error_codes.InvalidFormat):
            self._run_map_with_format(bad_skiff_format)

    @authors("gepardo")
    def test_bad_protobuf(self):
        self._prepare_tables()
        bad_protobuf_format = "protobuf"
        with raises_yt_error(yt_error_codes.InvalidFormat):
            self._run_map_with_format(bad_protobuf_format)

    @authors("gepardo")
    def test_bad_format(self):
        self._prepare_tables()
        with raises_yt_error('Invalid format name "bad-format"'):
            self._run_map_with_format("bad-format")

    @authors("gepardo")
    def test_bad_dsv(self):
        self._prepare_tables()
        bad_dsv_format = "<key_value_separator = %true>dsv"
        with raises_yt_error(yt_error_codes.InvalidFormat):
            self._run_map_with_format(bad_dsv_format)

    @authors("gepardo")
    def test_bad_yamred_dsv(self):
        self._prepare_tables()
        bad_dsv_format = '<enable_escaping = "some_long_string">yamred_dsv'
        with raises_yt_error(yt_error_codes.InvalidFormat):
            self._run_map_with_format(bad_dsv_format)

    @authors("gepardo")
    def test_bad_schemaful_dsv(self):
        self._prepare_tables()
        bad_dsv_format = '<enable_escaping = "some_long_string">schemaful_dsv'
        with raises_yt_error(yt_error_codes.InvalidFormat):
            self._run_map_with_format(bad_dsv_format)

    @authors("gepardo")
    def test_bad_web_json(self):
        self._prepare_tables()
        bad_web_json_format = '<field_weight_limit = -42>web_json'
        with raises_yt_error(yt_error_codes.InvalidFormat):
            self._run_map_with_format(bad_web_json_format)


##################################################################


class TestJobProxyFailBeforeStart(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @authors("alexkolodezny")
    def test_job_proxy_fail_before_start(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        create("table", "//tmp/t_stderr")
        write_table("//tmp/t_in", {"foo": "bar"})

        op = map(
            in_="//tmp/t_in",
            out=["//tmp/t_out"],
            command="cat",
            spec={
                "stderr_table_path": "//tmp/t_stderr",
                "job_testing_options": {"fail_before_job_start": True}
            },
            track=False)

        with raises_yt_error("Fail before job started"):
            op.track()
