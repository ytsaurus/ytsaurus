from yt_env_setup import YTEnvSetup, is_asan_build

from yt_commands import (
    authors, print_debug, wait, wait_breakpoint, release_breakpoint, with_breakpoint, create,
    ls, get,
    set, exists, create_pool, read_file, write_file, read_table, write_table, map,
    update_nodes_dynamic_config)

from yt_scheduler_helpers import scheduler_orchid_pool_path

from yt.common import YtError

import pytest

import socket
import time
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


class TestSchedulerMemoryLimits(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_PORTO = True

    # pytest.mark.xfail(run = False, reason = "Set-uid-root before running.")
    @authors("psushin")
    def test_map(self):
        create("table", "//tmp/t_in")
        write_table(
            "//tmp/t_in",
            {"value": "value", "subkey": "subkey", "key": "key", "a": "another"},
        )

        mapper = """
a = list()
while True:
    a.append(''.join(['xxx'] * 10000))
"""

        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        create("table", "//tmp/t_out")

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python mapper.py",
            file="//tmp/mapper.py",
            spec={"max_failed_job_count": 5},
        )

        # if all jobs failed then operation is also failed
        with pytest.raises(YtError):
            op.track()
        # ToDo: check job error messages.
        check_memory_limit(op)

    @authors("max42", "ignat")
    def test_dirty_sandbox(self):
        create("table", "//tmp/t_in")
        write_table(
            "//tmp/t_in",
            {"value": "value", "subkey": "subkey", "key": "key", "a": "another"},
        )

        create("table", "//tmp/t_out")

        command = "cat > /dev/null; mkdir ./tmpxxx; echo 1 > ./tmpxxx/f1; chmod 700 ./tmpxxx;"
        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command=command,
            spec={"max_failed_job_count": 1},
        )


class TestMemoryReserveFactor(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_PORTO = True

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "event_log": {"flush_period": 100},
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "event_log": {"flush_period": 100},
            "user_job_memory_digest_precision": 0.05,
        }
    }

    @pytest.mark.skipif(is_asan_build(), reason="This test does not work under ASAN")
    @authors("max42")
    def test_memory_reserve_factor(self):
        job_count = 30

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"key": i} for i in range(job_count)])

        mapper = """
#!/usr/bin/python
import time

from random import randint
def rndstr(n):
    s = ''
    for i in range(100):
        s += chr(randint(ord('a'), ord('z')))
    return s * (n // 100)

a = list()
while len(a) * 100000 < 14e7:
    a.append(rndstr(100000))

time.sleep(5.0)
"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)
        set("//tmp/mapper.py/@executable", True)
        create("table", "//tmp/t_out")

        time.sleep(1)
        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python mapper.py",
            file="//tmp/mapper.py",
            spec={
                "resource_limits": {"cpu": 1},
                "job_count": job_count,
                "mapper": {"memory_limit": 2 * 10 ** 8},
            },
        )

        time.sleep(1)
        event_log = read_table("//sys/scheduler/event_log")
        last_memory_reserve = None
        for event in event_log:
            if event["event_type"] == "job_completed" and event["operation_id"] == op.id:
                print_debug(
                    event["job_id"],
                    event["statistics"]["user_job"]["memory_reserve"]["sum"],
                    event["statistics"]["user_job"]["max_memory"]["sum"],
                )
                last_memory_reserve = int(event["statistics"]["user_job"]["memory_reserve"]["sum"])
        assert last_memory_reserve is not None
        assert 1e8 <= last_memory_reserve <= 2e8


###############################################################################################


class TestContainerCpuLimit(YTEnvSetup):
    USE_PORTO = True
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "map_operation_options": {
                "set_container_cpu_limit": True,
                "cpu_limit_overcommit_multiplier": 2,
                "initial_cpu_limit_overcommit": 0.05,
            },
        }
    }

    def get_job_container_cpu_limit(self):
        from porto import Connection

        conn = Connection()

        containers = self.Env.list_node_subcontainers(0)
        assert containers
        user_job_container = [x for x in containers if x.endswith("/uj")][0]
        # Strip last three symbols "/uj"
        slot_container = user_job_container[:-3]

        return conn.GetProperty(slot_container, "cpu_limit")

    @authors("psushin", "max42")
    def test_container_cpu_limit_spec(self):
        create("table", "//tmp/t1", attributes={"replication_factor": 1})
        write_table("//tmp/t1", [{"foo": 1}])

        create("table", "//tmp/t2", attributes={"replication_factor": 1})

        op = map(
            wait_for_jobs=True,
            track=False,
            command=with_breakpoint("BREAKPOINT ; cat"),
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "mapper": {
                    "cpu_limit": 0.1,
                    "set_container_cpu_limit": True,
                }
            },
        )

        wait_breakpoint()
        assert self.get_job_container_cpu_limit() == "0.1c"
        release_breakpoint()
        op.track()

    @authors("psushin")
    def test_container_cpu_limit_no_spec(self):
        create("table", "//tmp/t1", attributes={"replication_factor": 1})
        write_table("//tmp/t1", [{"foo": 1}])

        create("table", "//tmp/t2", attributes={"replication_factor": 1})

        op = map(
            wait_for_jobs=True,
            track=False,
            command=with_breakpoint("BREAKPOINT ; cat"),
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"mapper": {"cpu_limit": 0.1}},
        )
        wait_breakpoint()
        # Cpu limit is set due to map_operation_options.
        assert self.get_job_container_cpu_limit() == "0.25c"
        release_breakpoint()
        op.track()


###############################################################################################


class TestUpdateInstanceLimits(YTEnvSetup):
    DELTA_NODE_CONFIG = {
        "instance_limits_update_period": 200,
        "resource_limits_update_period": 200,
        "dynamic_config_manager": {
            "enable_unrecognized_options_alert": True,
        },
        "resource_limits": {
            "node_dedicated_cpu": 1,
            "memory_limits": {
                "user_jobs": {
                    "type": "dynamic",
                },
                "tablet_static": {
                    "type": "static",
                    "value": 10 ** 9,
                },
                "tablet_dynamic": {
                    "type": "dynamic",
                },
            },
        },
        "exec_agent": {
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            }
        },
    }

    USE_PORTO = True
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

        precision = 10 ** 8
        self.Env.set_nodes_memory_limit(15 * 10 ** 8)
        wait(
            lambda: abs(get("//sys/cluster_nodes/{}/@resource_limits/user_memory".format(node)) - 25 * 10 ** 7)
            <= precision
        )
        self.Env.set_nodes_memory_limit(2 * 10 ** 9)
        wait(
            lambda: abs(get("//sys/cluster_nodes/{}/@resource_limits/user_memory".format(node)) - 5 * 10 ** 8)
            <= precision
        )
        self.Env.set_nodes_memory_limit(10 ** 9 - 1)
        wait(lambda: int(get("//sys/cluster_nodes/{}/@resource_limits/user_memory".format(node))) == 0)

    @authors("gritukan")
    def test_dynamic_resource_limits_config(self):
        precision = 10 ** 8

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node = nodes[0]

        self.Env.set_nodes_memory_limit(15 * 10 ** 8)
        self.Env.set_nodes_cpu_limit(4)
        wait(
            lambda: abs(get("//sys/cluster_nodes/{}/@resource_limits/user_memory".format(node)) - 25 * 10 ** 7)
            <= precision
        )
        wait(lambda: int(get("//sys/cluster_nodes/{}/@resource_limits/cpu".format(node))) == 3)

        update_nodes_dynamic_config(
            {
                "resource_limits": {
                    "node_dedicated_cpu": 2,
                    "total_cpu": 5,
                    "cpu_per_tablet_slot": 1,
                    "free_memory_watermark": 100000,
                    "memory_limits": {
                        "user_jobs": {
                            "type": "static",
                            "value": 12345678,
                        },
                        "tablet_static": {
                            "type": "static",
                            "value": 10 ** 9,
                        },
                        "tablet_dynamic": {
                            "type": "dynamic",
                        },
                    },
                }
            }
        )

        wait(lambda: int(get("//sys/cluster_nodes/{}/@resource_limits/user_memory".format(node))) == 12345678)
        wait(lambda: int(get("//sys/cluster_nodes/{}/@resource_limits/cpu".format(node))) == 2)

        update_nodes_dynamic_config(
            {
                "resource_limits": {
                    "node_dedicated_cpu": 1,
                    "total_cpu": 5,
                    "cpu_per_tablet_slot": 1,
                    "free_memory_watermark": 100000,
                    "memory_limits": {
                        "user_jobs": {
                            "type": "dynamic",
                        },
                        "tablet_static": {
                            "type": "static",
                            "value": 10 ** 9,
                        },
                        "tablet_dynamic": {
                            "type": "dynamic",
                        },
                    },
                }
            }
        )

        wait(
            lambda: abs(get("//sys/cluster_nodes/{}/@resource_limits/user_memory".format(node)) - 25 * 10 ** 7)
            <= precision
        )
        wait(lambda: int(get("//sys/cluster_nodes/{}/@resource_limits/cpu".format(node))) == 3)


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
            config["exec_agent"]["job_controller"]["resource_limits"]["user_slots"] = 4
            config["exec_agent"]["job_controller"]["resource_limits"]["cpu"] = 4
            config["exec_agent"]["job_controller"]["gpu_manager"] = {
                "test_resource": True,
                "test_gpu_count": 4,
            }

    def setup_method(self, method):
        super(TestSchedulerGpu, self).setup_method(method)
        set("//sys/pool_trees/default/@config/main_resource", "gpu")

    @authors("renadeen")
    def test_job_count(self):
        gpu_nodes = [
            node
            for node in ls("//sys/cluster_nodes")
            if get("//sys/cluster_nodes/{}/@resource_limits/gpu".format(node)) > 0
        ]
        assert len(gpu_nodes) == 1
        gpu_node = gpu_nodes[0]

        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", [{"foo": i} for i in range(3)])

        op = map(
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_="//tmp/in",
            out="//tmp/out",
            spec={
                "mapper": {
                    "gpu_limit": 1,
                    "enable_gpu_layers": False,
                }
            },
            track=False,
        )

        wait_breakpoint()

        jobs = op.get_running_jobs()
        assert len(jobs) == 1
        assert jobs.values()[0]["address"] == gpu_node

    @authors("ignat")
    def test_min_share_resources(self):
        create_pool("gpu_pool", attributes={"min_share_resources": {"gpu": 1}})
        wait(lambda: get(scheduler_orchid_pool_path("gpu_pool") + "/min_share_resources/gpu") == 1)
        wait(lambda: get(scheduler_orchid_pool_path("gpu_pool") + "/min_share_ratio") == 0.25)

    @authors("ignat")
    def test_packing(self):
        def check_gpus(op, gpu_indexes):
            jobs = op.get_running_jobs()
            if len(jobs) != 1:
                return False
            assert jobs.values()[0]["address"] == gpu_node
            job_info = get(
                "//sys/cluster_nodes/{}/orchid/job_controller/active_jobs/scheduler/{}".format(gpu_node, jobs.keys()[0])
            )
            job_gpu_indexes = sorted([device["device_number"] for device in job_info["exec_attributes"]["gpu_devices"]])
            if job_gpu_indexes != sorted(gpu_indexes):
                return False
            return True

        gpu_nodes = [
            node
            for node in ls("//sys/cluster_nodes")
            if get("//sys/cluster_nodes/{}/@resource_limits/gpu".format(node)) > 0
        ]
        assert len(gpu_nodes) == 1
        gpu_node = gpu_nodes[0]

        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        write_table("//tmp/in", [{"foo": i} for i in range(3)])

        op1 = map(
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_="//tmp/in",
            out="//tmp/out1",
            spec={
                "mapper": {
                    "gpu_limit": 1,
                    "enable_gpu_layers": False,
                }
            },
            track=False,
        )

        wait(lambda: get("//sys/cluster_nodes/{}/@resource_usage/gpu".format(gpu_node)) == 1)

        wait(lambda: check_gpus(op1, [0]))

        op2 = map(
            command=with_breakpoint("cat ; BREAKPOINT"),
            in_="//tmp/in",
            out="//tmp/out2",
            spec={
                "mapper": {
                    "gpu_limit": 2,
                    "enable_gpu_layers": False,
                }
            },
            track=False,
        )

        wait(lambda: get("//sys/cluster_nodes/{}/@resource_usage/gpu".format(gpu_node)) == 3)

        wait(lambda: check_gpus(op2, [2, 3]))


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
                "resource_limits": {"user_slots": 2, "cpu": 2},
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
            command=with_breakpoint(
                "echo $YT_PORT_0 >&2; echo $YT_PORT_1 >&2; "
                "if [ -n \"$YT_PORT_2\" ]; then echo 'FAILED' >&2; fi; cat; BREAKPOINT"
            ),
            spec={
                "mapper": {
                    "port_count": 2,
                }
            },
        )

        jobs = wait_breakpoint()
        assert len(jobs) == 1

        # Not enough ports
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
                },
            )

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
            command="echo $YT_PORT_0 >&2; echo $YT_PORT_1 >&2; "
                    "if [ -n \"$YT_PORT_2\" ]; then echo 'FAILED' >&2; fi; cat",
            spec={
                "mapper": {
                    "port_count": 2,
                }
            },
        )

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

                map(
                    in_="//tmp/t_in",
                    out="//tmp/t_out",
                    command='echo "{port=$YT_PORT_0}; {port=$YT_PORT_1}"',
                    spec={
                        "mapper": {
                            "port_count": 2,
                            "format": "yson",
                        }
                    },
                )

                ports = read_table("//tmp/t_out")
                assert ports == expected_ports
        finally:
            if server_socket is not None:
                server_socket.close()
