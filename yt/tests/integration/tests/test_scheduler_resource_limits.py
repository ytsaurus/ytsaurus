import pytest

from yt_env_setup import YTEnvSetup, unix_only, patch_porto_env_only, get_porto_delta_node_config, porto_avaliable
from yt_commands import *

from flaky import flaky

import socket
import __builtin__

###############################################################################################

"""
This test only works when suid bit is set.
"""

def check_memory_limit(op):
    jobs_path = op.get_path() + "/jobs"
    for job_id in ls(jobs_path):
        inner_errors = get(jobs_path + "/" + job_id + "/@error/inner_errors")
        assert "Memory limit exceeded" in inner_errors[0]["message"]

@pytest.mark.skip_if('not porto_avaliable()')
class TestSchedulerMemoryLimits(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    REQUIRE_YTSERVER_ROOT_PRIVILEGES = True
    USE_PORTO_FOR_SERVERS = True

    DELTA_NODE_CONFIG = get_porto_delta_node_config()

    #pytest.mark.xfail(run = False, reason = "Set-uid-root before running.")
    @authors("psushin")
    @unix_only
    def test_map(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"value": "value", "subkey": "subkey", "key": "key", "a": "another"})

        mapper = \
"""
a = list()
while True:
    a.append(''.join(['xxx'] * 10000))
"""

        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        create("table", "//tmp/t_out")

        op = map(track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python mapper.py",
            file="//tmp/mapper.py",
            spec={"max_failed_job_count": 5})

        # if all jobs failed then operation is also failed
        with pytest.raises(YtError):
            op.track()
        # ToDo: check job error messages.
        check_memory_limit(op)

    @authors("max42", "ignat")
    @unix_only
    def test_dirty_sandbox(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"value": "value", "subkey": "subkey", "key": "key", "a": "another"})

        create("table", "//tmp/t_out")

        command = "cat > /dev/null; mkdir ./tmpxxx; echo 1 > ./tmpxxx/f1; chmod 700 ./tmpxxx;"
        map(in_="//tmp/t_in", out="//tmp/t_out", command=command)

@pytest.mark.skip_if('not porto_avaliable()')
class TestMemoryReserveFactor(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    REQUIRE_YTSERVER_ROOT_PRIVILEGES = True
    DELTA_NODE_CONFIG = get_porto_delta_node_config()
    USE_PORTO_FOR_SERVERS = True

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "event_log": {
                "flush_period": 100
            },
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "event_log": {
                "flush_period": 100
            },
            "user_job_memory_digest_precision" : 0.05,
        }
    }

    @authors("max42")
    @unix_only
    def test_memory_reserve_factor(self):
        job_count = 30

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"key" : i} for i in range(job_count)])

        mapper = \
"""
#!/usr/bin/python
import time

from random import randint
def rndstr(n):
    s = ''
    for i in range(100):
        s += chr(randint(ord('a'), ord('z')))
    return s * (n // 100)

a = list()
while len(a) * 100000 < 7e7:
    a.append(rndstr(100000))

time.sleep(5.0)
"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)
        set("//tmp/mapper.py/@executable", True)
        create("table", "//tmp/t_out")

        time.sleep(1)
        op = map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python mapper.py",
            file="//tmp/mapper.py",
            spec={ "resource_limits" : {"cpu" : 1},
                   "job_count" : job_count,
                   "mapper" : {"memory_limit": 10**8}})

        time.sleep(1)
        event_log = read_table("//sys/scheduler/event_log")
        last_memory_reserve = None
        for event in event_log:
            if event["event_type"] == "job_completed" and event["operation_id"] == op.id:
                print_debug(
                    event["job_id"],
                    event["statistics"]["user_job"]["memory_reserve"]["sum"],
                    event["statistics"]["user_job"]["max_memory"]["sum"])
                last_memory_reserve = int(event["statistics"]["user_job"]["memory_reserve"]["sum"])
        assert not last_memory_reserve is None
        assert 5e7 <= last_memory_reserve <= 10e7

###############################################################################################

class TestContainerCpuLimit(YTEnvSetup):
    DELTA_NODE_CONFIG = get_porto_delta_node_config()
    USE_PORTO_FOR_SERVERS = True
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    @authors("max42")
    @flaky(max_runs=3)
    def test_container_cpu_limit(self):
        op = vanilla(spec={"tasks": {"main": {"command": "timeout 5s md5sum /dev/zero || true",
                                              "job_count": 1,
                                              "set_container_cpu_limit": True,
                                              "cpu_limit": 0.1,
                                              }}})
        statistics = get(op.get_path() + "/@progress/job_statistics")
        cpu_usage = get_statistics(statistics, "user_job.cpu.user.$.completed.vanilla.max")
        assert cpu_usage < 2500

###############################################################################################

class TestUpdateInstanceLimits(YTEnvSetup):
    DELTA_NODE_CONFIG = {
        "instance_limits_update_period": 200,
        "resource_limits_update_period": 200,
        "resource_limits": {
            "node_dedicated_cpu": 1,
            "user_jobs": {
                "type": "dynamic",
            },
            "tablet_static": {
                "type": "static",
                "value": 10**9,
            },
            "tablet_dynamic": {
                "type": "dynamic",
            },
        },
        "exec_agent": {
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            }
        }
    }

    USE_PORTO_FOR_SERVERS = True
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    @authors("psushin", "gritukan")
    def test_update_cpu_limits(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node = nodes[0]
        self.Env.set_nodes_cpu_limit(4)
        wait(lambda: int(get("//sys/cluster_nodes/{}/@resource_limits/cpu".format(node))) == 3)
        self.Env.set_nodes_cpu_limit(3)
        wait(lambda: int(get("//sys/cluster_nodes/{}/@resource_limits/cpu".format(node))) == 2)

    @authors("gritukan")
    def test_update_memory_limits(self):
        # User jobs memory limit is approximately (total_memory - 10**9) / 2.
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node = nodes[0]

        precision = 10**8
        self.Env.set_nodes_memory_limit(15 * 10**8)
        wait(lambda: abs(get("//sys/cluster_nodes/{}/@resource_limits/user_memory".format(node)) - 25 * 10**7) <= precision)
        self.Env.set_nodes_memory_limit(2 * 10**9)
        wait(lambda: abs(get("//sys/cluster_nodes/{}/@resource_limits/user_memory".format(node)) - 5 * 10**8) <= precision)
        self.Env.set_nodes_memory_limit(10**9 - 1)
        wait(lambda: int(get("//sys/cluster_nodes/{}/@resource_limits/user_memory".format(node))) == 0)

###############################################################################################

class TestSchedulerGpu(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @classmethod
    def modify_node_config(cls, config):
        if not hasattr(cls, "node_counter"):
            cls.node_counter = 0
        cls.node_counter += 1
        if cls.node_counter == 1:
            config["exec_agent"]["job_controller"]["resource_limits"]["gpu"] = 1
            config["exec_agent"]["job_controller"]["test_gpu_resource"] = True

    @authors("renadeen")
    def test_job_count(self):
        gpu_nodes = [node for node in ls("//sys/cluster_nodes") if get("//sys/cluster_nodes/{}/@resource_limits/gpu".format(node)) > 0]
        assert len(gpu_nodes) == 1
        gpu_node = gpu_nodes[0]

        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", [{"foo": i} for i in range(3)])

        op = map(
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_="//tmp/in",
            out="//tmp/out",
            spec={"mapper": {
                "gpu_limit": 1,
                "enable_gpu_layers": False,
            }},
            track=False)

        wait_breakpoint()

        jobs = op.get_running_jobs()
        assert len(jobs) == 1
        assert jobs.values()[0]["address"] == gpu_node

    @authors("ignat")
    def test_min_share_resources(self):
        create_pool("gpu_pool", attributes={"min_share_resources": {"gpu": 1}})
        wait(lambda: get(scheduler_orchid_pool_path("gpu_pool") + "/min_share_resources/gpu") == 1)
        wait(lambda: get(scheduler_orchid_pool_path("gpu_pool") + "/min_share_ratio") == 1.0)

###############################################################################################

class TestPorts(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "start_port": 20000,
                "port_count": 3,
                "waiting_jobs_timeout": 1000,
                "resource_limits": {
                    "user_slots": 2,
                    "cpu": 2
                }
            },
        },
    }

    @authors("ignat")
    def test_simple(self):
        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        write_table("//tmp/t_in", [{"a": 0}])

        create("table", "//tmp/t_out", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out_other", attributes={"replication_factor": 1})

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command=with_breakpoint("echo $YT_PORT_0 >&2; echo $YT_PORT_1 >&2; if [ -n \"$YT_PORT_2\" ]; then echo 'FAILED' >&2; fi; cat; BREAKPOINT"),
            spec={
                "mapper": {
                    "port_count": 2,
                }
            })

        jobs = wait_breakpoint()
        assert len(jobs) == 1

        ## Not enough ports
        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out_other",
                command="cat",
                spec={
                    "mapper": {
                        "port_count": 2,
                    },
                    "max_failed_job_count": 1,
                    "fail_on_job_restart": True,
                })

        release_breakpoint()
        op.track()

        stderr = read_file(op.get_path() + "/jobs/" + jobs[0] + "/stderr")
        assert "FAILED" not in stderr
        ports = __builtin__.map(int, stderr.split())
        assert len(ports) == 2
        assert ports[0] != ports[1]

        assert all(port >= 20000 and port < 20003 for port in ports)


        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="echo $YT_PORT_0 >&2; echo $YT_PORT_1 >&2; if [ -n \"$YT_PORT_2\" ]; then echo 'FAILED' >&2; fi; cat",
            spec={
                "mapper": {
                    "port_count": 2,
                }
            })

        jobs_path = op.get_path() + "/jobs"
        assert exists(jobs_path)
        jobs = ls(jobs_path)
        assert len(jobs) == 1

        stderr = read_file(op.get_path() + "/jobs/" + jobs[0] + "/stderr")
        assert "FAILED" not in stderr
        ports = __builtin__.map(int, stderr.split())
        assert len(ports) == 2
        assert ports[0] != ports[1]

        assert all(port >= 20000 and port < 20003 for port in ports)

    @authors("max42")
    def test_preliminary_bind(self):
        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1})
        write_table("//tmp/t_in", [{"a": 1}])

        server_socket = None
        try:
            try:
                server_socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                server_socket.bind(("::1", 20001))
            except Exception as err:
                pytest.skip("Caught following exception while trying to bind to port 20001: {}".format(err))
                return

            # We run test several times to make sure that ports did not stuck inside node.
            for iteration in range(3):
                if iteration in [0, 1]:
                    expected_ports = [{"port": 20000}, {"port": 20002}]
                else:
                    server_socket.close()
                    server_socket = None
                    expected_ports = [{"port": 20000}, {"port": 20001}]

                map(in_="//tmp/t_in",
                    out="//tmp/t_out",
                    command='echo "{port=$YT_PORT_0}; {port=$YT_PORT_1}"',
                    spec={
                        "mapper": {
                            "port_count": 2,
                            "format": "yson",
                        }
                    })

                ports = read_table("//tmp/t_out")
                assert ports == expected_ports
        finally:
            if server_socket is not None:
                server_socket.close()

