from yt_env_setup import (YTEnvSetup, is_asan_build, Restarter, NODES_SERVICE)

from yt_commands import (
    authors, ls, get, set, create, write_table, wait, run_test_vanilla,
    with_breakpoint, wait_breakpoint, release_breakpoint, vanilla, map,
    disable_scheduler_jobs_on_node, enable_scheduler_jobs_on_node,
    interrupt_job, update_nodes_dynamic_config, set_nodes_banned,
    abort_job, run_sleeping_vanilla, set_node_banned, extract_statistic_v2,
    get_allocation_id_from_job_id, alter_table, write_file, print_debug,
    wait_no_assert,
)

from yt_helpers import JobCountProfiler, profiler_factory, is_uring_supported, is_uring_disabled

from yt.common import YtError

import pytest
import builtins

import json
import os


##################################################################


class TestJobs(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    @authors("pogorelov")
    def test_uninterruptible_jobs(self):
        aborted_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "interruption_unsupported"})

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=1)

        wait_breakpoint()

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        disable_scheduler_jobs_on_node(nodes[0], "test")

        wait(lambda: aborted_job_profiler.get_job_count_delta() == 1)

        release_breakpoint()

        enable_scheduler_jobs_on_node(nodes[0])

        op.track()

    @authors("pogorelov")
    def test_interruption_timeout(self):
        aborted_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "interruption_timeout"})

        command = """(trap "sleep 1000" SIGINT; BREAKPOINT)"""

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "tasks_a": {
                        "job_count": 1,
                        "command": with_breakpoint(command),
                        "interruption_signal": "SIGINT",
                        "signal_root_process_only": True,
                        "restart_exit_code": 5,
                    }
                },
                "max_failed_job_count": 1,
            },
        )

        (job_id,) = wait_breakpoint()

        interrupt_job(job_id, interrupt_timeout=2000)

        wait(lambda: aborted_job_profiler.get_job_count_delta() == 1)

        release_breakpoint()

        op.track()

    @authors("arkady-e1ppa")
    def test_job_proxy_exit_profiling(self):
        update_nodes_dynamic_config(value=True, path="/exec_node/job_controller/profile_job_proxy_process_exit")
        nodes = ls("//sys/cluster_nodes")
        profilers = [profiler_factory().at_node(node) for node in nodes]
        exit_ok = [profiler.counter("job_controller/job_proxy_process_exit/zero_exit_code") for profiler in profilers]

        op = run_test_vanilla("echo 1> null", job_count=3)

        op.wait_for_state("completed")

        wait(lambda: any([counter.get_delta() > 0 for counter in exit_ok]))

        op.track()


@authors("khlebnikov")
class TestJobsCri(TestJobs):
    JOB_ENVIRONMENT_TYPE = "cri"


class TestJobsDisabled(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 100,
                "user_slots": 100,
            },
        }
    }

    def _get_op_job(self, op):
        wait(lambda: op.list_jobs())

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        return job_ids[0]

    def teardown_method(self, method):
        node_address = ls("//sys/cluster_nodes")[0]
        if (get("//sys/cluster_nodes/{}/@alerts".format(node_address))):
            with Restarter(self.Env, NODES_SERVICE):
                pass
        super(TestJobsDisabled, self).teardown_method(method)

    @authors("pogorelov")
    def test_job_abort_on_fatal_alert(self):
        node_address = ls("//sys/cluster_nodes")[0]
        assert not get("//sys/cluster_nodes/{}/@alerts".format(node_address))

        aborted_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "node_with_disabled_jobs"})

        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "waiting_for_job_cleanup_timeout": 500,
                    }
                }

            },
        })

        op1 = run_sleeping_vanilla(
            spec={"job_testing_options": {"delay_in_cleanup": 1000}, "sanity_check_delay": 60 * 1000},
        )

        job_id_1 = self._get_op_job(op1)

        op2 = run_sleeping_vanilla()

        abort_job(job_id_1)

        wait(lambda: aborted_job_profiler.get_job_count_delta() >= 1)

        op1.abort()
        op2.abort()

    @authors("krasovav")
    def test_job_abort_on_cleanup_timeout(self):
        node_address = ls("//sys/cluster_nodes")[0]
        assert not get("//sys/cluster_nodes/{}/@alerts".format(node_address))

        aborted_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "node_with_disabled_jobs"})

        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "job_cleanup_timeout": 500,
                    }
                }

            },
        })

        op = run_sleeping_vanilla(
            spec={"job_testing_options": {"delay_in_cleanup": 1000}, "sanity_check_delay": 60 * 1000},
        )

        job_id = self._get_op_job(op)

        abort_job(job_id)

        wait(lambda: aborted_job_profiler.get_job_count_delta() >= 1)

        op.abort()


class TestNodeBanned(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 1,
                "user_slots": 1,
            },
        }
    }

    @authors("pogorelov")
    def test_job_abort_on_node_banned(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node = nodes[0]

        path = "job_controller/job_final_state"
        counter = profiler_factory().at_node(node, fixed_tags={"state": "aborted"}).counter(path)

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))

        wait_breakpoint()

        set_node_banned(node, True)

        wait(lambda: counter.get_delta(verbose=True) != 0)

        op.abort()


class TestJobProxyCallFailed(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "job_proxy": {
                        "testing_config": {
                            "fail_on_job_proxy_spawned_call": True,
                        },
                    },
                },
            },
        }
    }

    @authors("pogorelov")
    def test_on_job_proxy_spawned_call_fail(self):
        aborted_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "other"})

        op = run_test_vanilla("sleep 1", job_count=1)

        wait(lambda: aborted_job_profiler.get_job_count_delta() >= 1)

        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_proxy": {
                        "testing_config": {
                            "fail_on_job_proxy_spawned_call": False,
                        },
                    },
                },
            },
        })

        op.track()


class TestJobProxyPreparationFailed(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "job_proxy": {
                        "testing_config": {
                            "fail_preparation": True,
                        },
                    },
                },
            },
        }
    }

    @authors("coteeq")
    def test_job_proxy_preparation_failed(self):
        aborted_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "job_proxy_failed"})
        failed_job_profiler = JobCountProfiler(
            "failed", tags={"tree": "default", "job_type": "vanilla"})

        op = run_test_vanilla("sleep 1", job_count=1)

        wait(lambda: aborted_job_profiler.get_job_count_delta() >= 5)

        assert failed_job_profiler.get_job_count_delta() == 0

        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_proxy": {
                        "testing_config": {
                            "fail_preparation": False,
                        },
                    },
                },
            },
        })

        op.track()


class TestJobStatistics(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    NODE_IO_ENGINE_TYPE = "thread_pool"

    @authors("ngc224")
    def test_io_statistics(self):
        create("table", "//tmp/t_input")
        write_table("//tmp/t_input", {"foo": "bar"})

        create("table", "//tmp/t_output")

        op = map(
            command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            track=True,
        )

        statistics = op.get_statistics()
        chunk_reader_statistics = statistics["chunk_reader_statistics"]

        assert extract_statistic_v2(chunk_reader_statistics, "session_count") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "retry_count") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "pass_count") > 0

        assert extract_statistic_v2(chunk_reader_statistics, "data_bytes_transmitted") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "data_bytes_read_from_disk") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "data_blocks_read_from_disk") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "data_io_requests") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "wasted_data_bytes_read_from_disk", summary_type="count") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "wasted_data_blocks_read_from_disk", summary_type="count") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "meta_bytes_read_from_disk", summary_type="count") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "meta_bytes_transmitted", summary_type="count") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "meta_io_requests", summary_type="count") > 0

        assert extract_statistic_v2(chunk_reader_statistics, "block_count") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "prefetched_block_count", summary_type="count") > 0

    @authors("artemagafonov")
    def test_vanilla_statistics(self):
        op = run_test_vanilla("true", track=True)

        statistics = op.get_statistics()
        pipes_statistic = statistics["user_job"]["pipes"]

        assert "data" not in statistics or ("data" in statistics and "input" not in statistics["data"])
        assert "input" not in pipes_statistic
        assert "output" in pipes_statistic


@authors("ngc224")
@pytest.mark.skipif(not is_uring_supported() or is_uring_disabled(), reason="io_uring is not available on this host")
class TestJobStatisticsUring(YTEnvSetup):
    NUM_MASTERS = TestJobStatistics.NUM_MASTERS
    NUM_NODES = TestJobStatistics.NUM_NODES
    NUM_SCHEDULERS = TestJobStatistics.NUM_SCHEDULERS

    NODE_IO_ENGINE_TYPE = "uring"

    test_io_statistics = TestJobStatistics.test_io_statistics


@pytest.mark.skipif(is_asan_build(), reason="This test does not work under ASAN")
class TestAllocationReuse(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    USE_PORTO = True

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 1,
                "user_slots": 1,
            },
        },
        "exec_node": {
            "job_proxy": {
                "check_user_job_memory_limit": False,
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "allocation": {
                        "enable_multiple_jobs": True,
                    },
                },
            },
        },
    }

    @authors("pogorelov")
    def test_simple(self):
        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1})

        write_table("//tmp/t_in", [{"foo": "bar"}] * 2)

        op = map(
            wait_for_jobs=True,
            track=False,
            command=with_breakpoint("BREAKPOINT ; cat"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"data_size_per_job": 1, "enable_multiple_jobs_in_allocation": True},
        )

        job_ids = wait_breakpoint()
        assert len(job_ids) == 1

        job_id1 = job_ids[0]

        allocation_id1 = get_allocation_id_from_job_id(job_id1)

        release_breakpoint(job_id=job_id1)

        job_ids = wait_breakpoint()
        assert len(job_ids) == 1

        job_id2 = job_ids[0]

        assert job_id1 != job_id2

        allocation_id2 = get_allocation_id_from_job_id(job_id2)

        assert allocation_id1 == allocation_id2

        release_breakpoint()

        op.track()

    @authors("pogorelov")
    def test_shrink_resources(self):
        job_count = 2
        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1})
        create("file", "//tmp/mapper.py", attributes={"replication_factor": 1})

        write_table("//tmp/t_in", [{"key": 0} for i in range(job_count)])

        memory = 500 * 10 ** 6

        script = with_breakpoint(
f"""
import os
import subprocess
import time

from random import randint

def rndstr(n):
    s = ''
    for i in range(100):
        s += chr(randint(ord('a'), ord('z')))
    return s * (n // 100)

cmd = '''BREAKPOINT'''

def breakpoint():
    subprocess.call(cmd, shell=True)

job_index = int(os.environ['YT_JOB_INDEX'])

if job_index != 0:
    breakpoint()

a = list()
while len(a) * 100000 < {memory}:
    a.append(rndstr(100000))

if job_index == 0:
    breakpoint()
""" # noqa
        ).encode("ascii")

        print_debug("Script is ", script)

        write_file("//tmp/mapper.py", script, attributes={"executable": True})

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python3 mapper.py",
            job_count=job_count,
            spec={
                "data_weight_per_job": 1,
                "mapper": {
                    "memory_limit": 200 * 10 ** 6,
                    "file_paths": ["//tmp/mapper.py"],
                    "user_job_memory_digest_lower_bound": 1.0,
                    "user_job_memory_digest_upper_bound": 1.0,
                    "user_job_memory_digest_default_value": 1.0,
                    "job_proxy_memory_digest": {
                        "lower_bound": 1.0,
                        "upper_bound": 1.0,
                        "default_value": 1.0,
                    },
                },
                "enable_multiple_jobs_in_allocation": True,
            })

        job_id1, = wait_breakpoint(job_count=1)

        allocation_id = get_allocation_id_from_job_id(job_id1)

        print_debug(f"First job is {job_id1}")

        first_job_memory = 0

        allocation_orchid_path = op.get_job_node_orchid_path(job_id1) + f"/exec_node/job_controller/allocations/{allocation_id}"

        def check_overdraft():
            nonlocal first_job_memory

            orchid = get(allocation_orchid_path)

            print_debug("Resource usage: {}, initial resource demand: {}".format(orchid["base_resource_usage"], orchid["initial_resource_demand"]))

            first_job_memory = orchid["base_resource_usage"]["user_memory"]

            return orchid["base_resource_usage"]["user_memory"] > orchid["initial_resource_demand"]["user_memory"]

        wait(check_overdraft)

        release_breakpoint(job_id=job_id1)

        job_id2, = wait_breakpoint(job_count=1)

        print_debug(f"Second job is {job_id2}")

        assert allocation_id == get_allocation_id_from_job_id(job_id2)

        orchid = get(allocation_orchid_path)

        print_debug("Job2 resource usage: {}".format(orchid["base_resource_usage"]))

        assert orchid["base_resource_usage"]["user_memory"] < first_job_memory
        assert orchid["base_resource_usage"]["user_memory"] == orchid["initial_resource_demand"]["user_memory"]

        release_breakpoint()

        op.track()


@authors("dann239")
class TestJobOomScore(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "footprint_memory": 0,
        },
    }

    def _get_oom_score_adj(self, pid: int) -> int:
        return int(open(f"/proc/{pid}/oom_score_adj").read())

    def _get_ppid(self, pid: int) -> int:
        return int(open(f"/proc/{pid}/stat").read().split()[3])

    def _find_pid(self, inner_pid: int, cgroup: str) -> int:
        for dir in os.listdir("/proc"):
            try:
                pid = int(dir)
                if cgroup != open(f"/proc/{pid}/cgroup").read():
                    continue
                for line in open(f"/proc/{pid}/status").readlines():
                    if not line.startswith("NSpid:"):
                        continue
                    if int(line.split()[-1]) == inner_pid:
                        return pid
                    break
                else:
                    assert False, "No relevant line in status"
            except Exception:
                continue
        assert False, "Could not find pid"

    def test_job_oom_score(self):
        target_score = 67
        memory = int(500 * (2 ** 20))

        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_proxy": {
                        "oom_score_adj_on_exceeded_memory_reserve": target_score,
                    },
                },
            },
        })

        script = with_breakpoint(
f"""
import json
import multiprocessing
import os
import sys
import subprocess
import time

data = {{
    "pid": int(os.getpid()),
    "cgroup": open("/proc/self/cgroup").read(),
}}

print(json.dumps(data), file=sys.stderr, flush=True)

def eat_memory():
    data = "a" * {memory}
    time.sleep(100500)

eater = multiprocessing.Process(target=eat_memory)
eater.start()

subprocess.call('''BREAKPOINT''', shell=True, executable="/bin/bash")
eater.terminate()

time.sleep(100500)
"""  # noqa
        ).encode()

        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        write_table("//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out", attributes={"replication_factor": 1})

        create("file", "//tmp/script.py", attributes={"replication_factor": 1})
        write_file("//tmp/script.py", script)

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            job_count=1,
            command="python3 script.py",
            spec={
                "mapper": {
                    "file_paths": ["//tmp/script.py"],
                    "memory_limit": memory * 2,
                    "memory_reserve_factor": 0.5,
                    "job_proxy_memory_digest": {
                        "lower_bound": 1e-8,
                        "upper_bound": 1.,
                        "default_value": 1e-8,
                        "relative_precision": 1.,
                    },
                },
            },
        )

        job_id, = wait_breakpoint(job_count=1)

        data_str = op.read_stderr(job_id).decode()
        data = json.loads(data_str)
        pid = self._find_pid(data["pid"], data["cgroup"])

        # Check user job oom_score_adj.
        wait(lambda: self._get_oom_score_adj(pid) == target_score)
        release_breakpoint()
        wait(lambda: self._get_oom_score_adj(pid) == 0)

        if not self.USE_PORTO:
            # Check job proxy oom_score_adj.
            assert self._get_oom_score_adj(self._get_ppid(pid)) == target_score


@authors("dann239")
class TestJobOomScorePorto(TestJobOomScore):
    USE_PORTO = True


@pytest.mark.skipif(is_asan_build(), reason="This test does not work under ASAN")
class TestAllocationSizeIncrease(YTEnvSetup):
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    USE_PORTO = True

    DELTA_NODE_CONFIG = {
        "resource_limits": {
            # Each job proxy occupies about 100MB.
            "user_jobs": {
                "type": "static",
                "value": 500 * 10 ** 6,
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 1,
                "user_slots": 1,
            },
        },
        "exec_node": {
            "job_proxy": {
                "check_user_job_memory_limit": False,
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "allocation": {
                        "enable_multiple_jobs": True,
                    },
                },
            },
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "user_job_resource_overdraft_memory_multiplier": 1.1,
            "memory_digest_resource_overdraft_factor": 1.0,
            "user_job_memory_reserve_quantile": 0.0,
        },
    }

    @authors("pogorelov")
    def test_increase_resources(self):
        aborted_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "map", "abort_reason": "resource_overdraft"})
        completed_job_profiler = JobCountProfiler(
            "completed", tags={"tree": "default", "job_type": "map"})

        job_count = 2
        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1})
        create("file", "//tmp/mapper.py", attributes={"replication_factor": 1})

        write_table("//tmp/t_in", [{"key": 0} for i in range(job_count)])

        memory = 500 * 10 ** 6

        script = with_breakpoint(
f"""
import os
import subprocess
import time

from random import randint

def rndstr(n):
    s = ''
    for i in range(100):
        s += chr(randint(ord('a'), ord('z')))
    return s * (n // 100)

cmd = '''BREAKPOINT'''

def breakpoint():
    subprocess.call(cmd, shell=True)

job_index = int(os.environ['YT_JOB_INDEX'])

breakpoint()

if job_index == 0:
    a = list()
    while len(a) * 100000 < {memory}:
        a.append(rndstr(100000))

breakpoint()
""" # noqa
        ).encode("ascii")

        print_debug("Script is ", script)

        write_file("//tmp/mapper.py", script, attributes={"executable": True})

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python3 mapper.py",
            job_count=job_count,
            spec={
                "data_weight_per_job": 1,
                "mapper": {
                    "memory_limit": 200 * 10 ** 6,
                    "file_paths": ["//tmp/mapper.py"],
                    "user_job_memory_digest_lower_bound": 0.9,
                    "user_job_memory_digest_default_value": 0.9,
                    "job_proxy_memory_digest": {
                        "lower_bound": 1.0,
                        "upper_bound": 1.0,
                        "default_value": 1.0,
                    },
                },
                "enable_multiple_jobs_in_allocation": True,
                "sanity_check_delay": 600 * 1000,
            })

        job_ids = wait_breakpoint(job_count=2)

        print_debug(f"First job ids are {job_ids}")

        for job_id in job_ids:
            release_breakpoint(job_id=job_id)

        wait(lambda: aborted_job_profiler.get_job_count_delta() > 0)

        def get_running_job_ids():
            running_jobs = op.get_running_jobs()
            job_ids = running_jobs.keys()
            print_debug(f"Running job ids are {job_ids}")

            return job_ids

        def check_new_two_jobs():
            running_job_ids = get_running_job_ids()
            if len(running_job_ids) != 2:
                return False

            if builtins.set(job_ids) == builtins.set(running_job_ids):
                return False

            assert job_ids[0] in running_job_ids or job_ids[1] in running_job_ids

            return True

        wait(check_new_two_jobs)

        new_job_ids = get_running_job_ids()

        print_debug(f"New job ids are {new_job_ids}")
        new_job_id = None
        for job_id in new_job_ids:
            if job_id not in job_ids:
                new_job_id = job_id
                break
        preserved_job_id = (builtins.set(job_ids) & builtins.set(new_job_ids)).pop()

        assert new_job_id is not None

        node_to_kill = op.get_node(new_job_id)

        # Release breakpoints for aborted job and new job
        for job_id in job_ids:
            if job_id not in new_job_ids:
                try:
                    release_breakpoint(job_id=job_id)
                except RuntimeError:
                    # NB(pogorelov):Aborted job may be aborted before or after second breakpoint reached.
                    print_debug(f"Job {job_id} is not waiting on breakpoint")
                    pass

        wait_breakpoint(job_count=2)
        print_debug(f"Killing node {node_to_kill}")
        self.Env.kill_nodes(addresses=[node_to_kill])
        release_breakpoint(job_id=new_job_id)

        try:
            wait(lambda: len(get_running_job_ids()) == 1)

            running_job_ids = list(get_running_job_ids())

            print_debug(f"Running job ids are {running_job_ids}")

            running_job_id = running_job_ids[0]
            assert running_job_id == preserved_job_id

            allocation_id = get_allocation_id_from_job_id(preserved_job_id)
            allocation_orchid_path = op.get_job_node_orchid_path(preserved_job_id) + f"/exec_node/job_controller/allocations/{allocation_id}"
            first_job_orchid = get(allocation_orchid_path)
            first_job_memory_usage = first_job_orchid["base_resource_usage"]["user_memory"]
            print_debug(f"First job memory usage is {first_job_memory_usage}")

            release_breakpoint(job_id=preserved_job_id)
            wait(lambda: completed_job_profiler.get_job_count_delta() == 1)

            second_job_id, = wait_breakpoint(job_count=1)

            print_debug(f"Second job id is {second_job_id}")
            assert allocation_id == get_allocation_id_from_job_id(second_job_id)

            second_job_orchid = get(op.get_job_node_orchid_path(second_job_id) + f"/exec_node/job_controller/allocations/{allocation_id}")
            second_job_memory_usage = second_job_orchid["base_resource_usage"]["user_memory"]
            print_debug(f"Second job memory usage is {second_job_memory_usage}")

            assert second_job_memory_usage > first_job_memory_usage
        finally:
            self.Env.start_nodes(addresses=[node_to_kill])

    @authors("pogorelov")
    def test_allocation_aborted_on_resource_lack(self):
        resource_overdraft_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "map", "abort_reason": "resource_overdraft"})
        node_resource_overdraft_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "map", "abort_reason": "node_resource_overcommit"})

        completed_job_profiler = JobCountProfiler(
            "completed", tags={"tree": "default", "job_type": "map"})

        job_count = 2
        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1})
        create("file", "//tmp/mapper.py", attributes={"replication_factor": 1})

        write_table("//tmp/t_in", [{"key": 0} for i in range(job_count)])

        memory = 500 * 10 ** 6

        script = with_breakpoint(
f"""
import os
import subprocess
import time

from random import randint

def rndstr(n):
    s = ''
    for i in range(100):
        s += chr(randint(ord('a'), ord('z')))
    return s * (n // 100)

cmd = '''BREAKPOINT'''

def breakpoint():
    subprocess.call(cmd, shell=True)

job_index = int(os.environ['YT_JOB_INDEX'])

breakpoint()

if job_index == 0:
    a = list()
    while len(a) * 100000 < {memory}:
        a.append(rndstr(100000))

breakpoint()
""" # noqa
        ).encode("ascii")

        print_debug("Script is ", script)

        write_file("//tmp/mapper.py", script, attributes={"executable": True})

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python3 mapper.py",
            job_count=job_count,
            spec={
                "testing": {
                    "settle_job_delay": {
                        "duration": 3000,
                        "type": "async",
                    },
                },
                "data_weight_per_job": 1,
                "mapper": {
                    "memory_limit": 200 * 10 ** 6,
                    "file_paths": ["//tmp/mapper.py"],
                    "user_job_memory_digest_lower_bound": 0.9,
                    "user_job_memory_digest_default_value": 0.9,
                    "job_proxy_memory_digest": {
                        "lower_bound": 1.0,
                        "upper_bound": 1.0,
                        "default_value": 1.0,
                    },
                },
                "enable_multiple_jobs_in_allocation": True,
                "sanity_check_delay": 600 * 1000,
            })

        job_ids = wait_breakpoint(job_count=2)

        print_debug(f"First job ids are {job_ids}")

        for job_id in job_ids:
            release_breakpoint(job_id=job_id)

        wait(lambda: resource_overdraft_job_profiler.get_job_count_delta() > 0)

        def get_running_job_ids():
            running_jobs = op.get_running_jobs()
            job_ids = running_jobs.keys()
            print_debug(f"Running job ids are {job_ids}")

            return job_ids

        def check_new_two_jobs():
            running_job_ids = get_running_job_ids()
            if len(running_job_ids) != 2:
                return False

            if builtins.set(job_ids) == builtins.set(running_job_ids):
                return False

            assert job_ids[0] in running_job_ids or job_ids[1] in running_job_ids

            return True

        wait(check_new_two_jobs)

        new_job_ids = get_running_job_ids()

        print_debug(f"New job ids are {new_job_ids}")
        new_job_id = None
        for job_id in new_job_ids:
            if job_id not in job_ids:
                new_job_id = job_id
                break
        preserved_job_id = (builtins.set(job_ids) & builtins.set(new_job_ids)).pop()

        assert new_job_id is not None

        node_to_kill = op.get_node(new_job_id)

        # Release breakpoints for aborted job and new job
        for job_id in job_ids:
            if job_id not in new_job_ids:
                try:
                    release_breakpoint(job_id=job_id)
                except RuntimeError:
                    # NB(pogorelov):Aborted job may be aborted before or after second breakpoint reached.
                    print_debug(f"Job {job_id} is not waiting on breakpoint")
                    pass

        wait_breakpoint(job_count=2)
        print_debug(f"Killing node {node_to_kill}")
        set_nodes_banned([node_to_kill], True, wait_for_scheduler=True)
        release_breakpoint(job_id=new_job_id)

        wait(lambda: len(get_running_job_ids()) == 1)

        running_job_ids = list(get_running_job_ids())

        print_debug(f"Running job ids are {running_job_ids}")

        running_job_id = running_job_ids[0]
        assert running_job_id == preserved_job_id

        allocation_id = get_allocation_id_from_job_id(preserved_job_id)
        allocation_orchid_path = op.get_job_node_orchid_path(preserved_job_id) + f"/exec_node/job_controller/allocations/{allocation_id}"
        first_job_orchid = get(allocation_orchid_path)
        first_job_memory_usage = first_job_orchid["base_resource_usage"]["user_memory"]
        print_debug(f"First job memory usage is {first_job_memory_usage}")

        release_breakpoint(job_id=preserved_job_id)
        wait(lambda: completed_job_profiler.get_job_count_delta() == 1)

        orchid_path = op.get_job_tracker_orchid_path()

        def check_allocation_has_no_running_job(allocation_id):
            allocation_jobs = get(f"{orchid_path}/allocations/{allocation_id}/jobs")
            has_running_job = False

            for _, job_info in allocation_jobs.items():
                if job_info["stage"] == "running":
                    has_running_job = True
                    break

            return not has_running_job

        wait(lambda: check_allocation_has_no_running_job(allocation_id))

        update_nodes_dynamic_config(
            value={
                "user_jobs": {
                    "type": "static",
                    "value": first_job_memory_usage
                }
            },
            path="resource_limits/memory_limits"
        )

        assert node_resource_overdraft_job_profiler.get_job_count_delta() == 0

        wait(lambda: node_resource_overdraft_job_profiler.get_job_count_delta() == 1)

        set_nodes_banned([node_to_kill], False, wait_for_scheduler=True)


@pytest.mark.skipif(is_asan_build(), reason="This test does not work under ASAN")
class TestFreeUserJobsMemoryWatermark(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "resource_limits": {
            # Each job proxy occupies about 100MB.
            "user_jobs": {
                "type": "static",
                "value": 500 * 10 ** 6,
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 1,
                "user_slots": 1,
            },
        },
        "exec_node": {
            "job_proxy": {
                "check_user_job_memory_limit": False,
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "job_resource_manager": {
                "free_user_job_memory_watermark_multiplier": 0.9,
            },
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "available_exec_nodes_check_period": 100,
            "footprint_memory": 10 * 10 ** 6,
        },
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "min_spare_allocation_resources_on_node": {
                "cpu": 0.5,
                "user_slots": 1,
            },
        },
    }

    @authors("pogorelov")
    def test_node_send_limits_to_scheduler_considering_watermark(self):
        with pytest.raises(YtError):
            run_test_vanilla("sleep 0.1", track=True, job_count=1, task_patch={
                "memory_limit": 300 * 10 ** 6,
                "user_job_memory_digest_lower_bound": 1.0,
                "user_job_memory_digest_default_value": 1.0,
            })

    @authors("pogorelov")
    def test_node_does_not_considering_watermark_in_resource_overdraft_check(self):
        aborted_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "resource_overdraft"})
        completed_job_profiler = JobCountProfiler(
            "completed", tags={"tree": "default", "job_type": "vanilla"})

        memory = 300 * 10 ** 6

        script = with_breakpoint(
f"""
import os
import subprocess
import time

from random import randint

def rndstr(n):
    s = ''
    for i in range(100):
        s += chr(randint(ord('a'), ord('z')))
    return s * (n // 100)

cmd = '''BREAKPOINT'''

def breakpoint():
    subprocess.call(cmd, shell=True)

a = list()
while len(a) * 100000 < {memory}:
    a.append(rndstr(100000))

breakpoint()
""" # noqa
        ).encode("ascii")

        print_debug("Script is ", script)

        create("file", "//tmp/script.py", attributes={"replication_factor": 1})

        write_file("//tmp/script.py", script, attributes={"executable": True})

        op = run_test_vanilla("python3 script.py", job_count=1, task_patch={
            "memory_limit": 30 * 10 ** 6,
            "user_job_memory_digest_lower_bound": 1.0,
            "user_job_memory_digest_default_value": 1.0,
            "sanity_check_delay": 600 * 1000,
            "job_proxy_memory_digest": {
                "lower_bound": 0.1,
                "upper_bound": 0.1,
                "default_value": 0.1,
            },
            "file_paths": ["//tmp/script.py"],
        })

        job_id, = wait_breakpoint(job_count=1)

        # op.track()

        def check_memory_usage():
            orchid = op.get_job_node_orchid(job_id)
            assert orchid["base_resource_usage"]["user_memory"] > 250000000

        wait_no_assert(check_memory_usage)

        release_breakpoint()
        op.track()

        wait(lambda: completed_job_profiler.get_job_count_delta() == 1)
        assert aborted_job_profiler.get_job_count_delta() == 0


class TestNodeAddressResolveFailed(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "testing": {
                            "fail_address_resolve": True,
                        },
                    },
                },
            },
        },
    }

    @authors("pogorelov")
    def test_resolve_failed(self):
        aborted_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "address_resolve_failed"})

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=1)

        wait(lambda: aborted_job_profiler.get_job_count_delta() > 0)

        update_nodes_dynamic_config(value=False, path="exec_node/job_controller/job_common/testing/fail_address_resolve")

        wait_breakpoint(job_count=1)

        release_breakpoint()

        op.track()


class TestGroupOutOfOrderBlocks(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "chunk_client_dispatcher": {
            "chunk_reader_pool_size": 1,
        },
    }

    @authors("ngc224")
    @pytest.mark.parametrize("group_out_of_order_blocks", [True, False])
    def test_group_out_of_order_blocks(self, group_out_of_order_blocks):
        create("table", "//tmp/t_input")
        set("//tmp/t_input/@optimize_for", b"scan")
        set("//tmp/t_input/@replication_factor", self.NUM_NODES)

        column_count = 10
        row_count = 100

        schema = [
            dict(
                name=f'field_{column_index}',
                type='string',
            ) for column_index in range(column_count)
        ]

        data = [
            {
                f'field_{column_index}': f'value_{row_index}'
                for column_index in range(column_count)
            } for row_index in range(row_count)
        ]

        alter_table("//tmp/t_input", schema=schema)

        write_table("//tmp/t_input", data)

        assert get("//tmp/t_input/@chunk_count") == 1

        create("table", "//tmp/t_output")
        set("//tmp/t_output/@replication_factor", self.NUM_NODES)

        op = map(
            command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec=dict(
                job_io=dict(
                    table_reader=dict(
                        group_out_of_order_blocks=group_out_of_order_blocks,
                    ),
                ),
                job_count=1,
            ),
            track=True,
        )

        assert get("//tmp/t_output/@row_count") == row_count

        statistics = op.get_statistics()
        chunk_reader_statistics = statistics["chunk_reader_statistics"]
        expected_data_io_requests = 1 if group_out_of_order_blocks else column_count

        assert extract_statistic_v2(chunk_reader_statistics, "data_io_requests") == expected_data_io_requests
        assert extract_statistic_v2(chunk_reader_statistics, "block_count") == column_count


class TestExeNodeProfiling(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "scheduler_connector": {
                    "use_profiling_tags_from_scheduler": True,
                }
            }
        },
    }

    @authors("coteeq")
    def test_exe_node_tags_from_scheduler(self):
        node, = ls("//sys/exec_nodes")
        profiler = profiler_factory().at_node(node)

        def get_labels(tree):
            return {
                "pool_tree": tree,
                "origin": "scheduler",
            }

        # Just a random sensor that should have non-null value.
        sensor = "job_controller/active_job_count"

        wait(lambda: profiler.get(sensor, tags=get_labels("default")) == 0.0)

        # Sanity check.
        assert profiler.get(sensor, tags=get_labels("<unknown>")) is None

        set("//sys/pool_trees/default/@config/node_tag_filter", "!other")

        set(f"//sys/cluster_nodes/{node}/@user_tags", ["other"])
        wait(lambda: "other" in get(f"//sys/scheduler/orchid/scheduler/nodes/{node}/tags"))

        wait(lambda: profiler.get(sensor, tags=get_labels("<unknown>")) == 0.0)
        assert profiler.get(sensor, tags=get_labels("default")) is None


##################################################################
