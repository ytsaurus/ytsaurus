import pytest

from yt_env_setup import YTEnvSetup, unix_only, patch_porto_env_only
from yt_commands import *

from flaky import flaky

import sys


##################################################################

"""
This test only works when suid bit is set.
"""

porto_delta_node_config = {
    "exec_agent": {
        "slot_manager": {
            "enforce_job_control": True,                              # <= 18.4
            "job_environment" : {
                "type" : "porto",                                     # >= 19.2
            },
        }
    }
}

def check_memory_limit(op):
    jobs_path = op.get_path() + "/jobs"
    for job_id in ls(jobs_path):
        inner_errors = get(jobs_path + "/" + job_id + "/@error/inner_errors")
        assert "Memory limit exceeded" in inner_errors[0]["message"]

class TestSchedulerMemoryLimits(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    # This is a mix of options for 18.4 and 18.5
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "enable_cgroups": True,                                       # <= 18.4
            "supported_cgroups": ["cpuacct", "blkio", "cpu"],   # <= 18.4
            "slot_manager": {
                "enforce_job_control": True,                              # <= 18.4
                "memory_watchdog_period" : 100,                           # <= 18.4
                "job_environment" : {
                    "type" : "cgroups",                                   # >= 18.5
                    "memory_watchdog_period" : 100,                       # >= 18.5
                    "supported_cgroups": [                                # >= 18.5
                        "cpuacct",
                        "blkio",
                        "cpu"],
                },
            }
        }
    }

    #pytest.mark.xfail(run = False, reason = "Set-uid-root before running.")
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

        op = map(dont_track=True,
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

    @unix_only
    def test_dirty_sandbox(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"value": "value", "subkey": "subkey", "key": "key", "a": "another"})

        create("table", "//tmp/t_out")

        command = "cat > /dev/null; mkdir ./tmpxxx; echo 1 > ./tmpxxx/f1; chmod 700 ./tmpxxx;"
        map(in_="//tmp/t_in", out="//tmp/t_out", command=command)

@patch_porto_env_only(TestSchedulerMemoryLimits)
class TestSchedulerMemoryLimitsPorto(YTEnvSetup):
    DELTA_NODE_CONFIG = porto_delta_node_config
    USE_PORTO_FOR_SERVERS = True

class TestMemoryReserveFactor(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    # This is a mix of options for 18.4 and 18.5
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "enable_cgroups": True,                                       # <= 18.4
            "supported_cgroups": ["cpuacct", "blkio", "cpu"],   # <= 18.4
            "slot_manager": {
                "enforce_job_control": True,                              # <= 18.4
                "memory_watchdog_period" : 100,                           # <= 18.4
                "job_environment" : {
                    "type" : "cgroups",                                   # >= 18.5
                    "memory_watchdog_period" : 100,                       # >= 18.5
                    "supported_cgroups": [                                # >= 18.5
                        "cpuacct",
                        "blkio",
                        "cpu"],
                },
            }
        }
    }

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

    @unix_only
    def test_memory_reserve_factor(self):
        job_count = 30

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"key" : i} for i in range(job_count)])

        mapper = \
"""
#!/usr/bin/python
from random import randint
def rndstr(n):
    s = ''
    for i in range(100):
        s += chr(randint(ord('a'), ord('z')))
    return s * (n // 100)

a = list()
while len(a) * 100000 < 7e7:
    a.append(rndstr(100000))

"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)
        set("//tmp/mapper.py/@executable", True)
        create("table", "//tmp/t_out")

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
                print >>sys.stderr, \
                    event["job_id"], \
                    event["statistics"]["user_job"]["memory_reserve"]["sum"], \
                    event["statistics"]["user_job"]["max_memory"]["sum"]
                last_memory_reserve = int(event["statistics"]["user_job"]["memory_reserve"]["sum"])
        assert not last_memory_reserve is None
        assert 6e7 <= last_memory_reserve <= 10e7

@patch_porto_env_only(TestMemoryReserveFactor)
class TestMemoryReserveFactorPorto(YTEnvSetup):
    DELTA_NODE_CONFIG = porto_delta_node_config
    USE_PORTO_FOR_SERVERS = True
