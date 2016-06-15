import pytest

from yt_env_setup import YTEnvSetup, unix_only
from yt_commands import *


##################################################################

"""
This test only works when suid bit is set.
"""

def check_memory_limit(op):
    jobs_path = "//sys/operations/" + op.id + "/jobs"
    for job_id in ls(jobs_path):
        inner_errors = get(jobs_path + "/" + job_id + "/@error/inner_errors")
        assert "Memory limit exceeded" in inner_errors[0]["message"]

class TestSchedulerMemoryLimits(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    # This is a mix of options for 18.3 and 18.4
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "enable_cgroups": True,                                       # <= 18.3
            "supported_cgroups": ["cpuacct", "blkio", "memory", "cpu"],   # <= 18.3
            "slot_manager": {
                "enforce_job_control": True,                              # <= 18.3
                "memory_watchdog_period" : 100,                           # <= 18.3
                "job_environment" : {
                    "type" : "cgroups",                                   # >= 18.4
                    "memory_watchdog_period" : 100,                       # >= 18.4
                    "supported_cgroups": [                                # >= 18.4
                        "cpuacct", 
                        "blkio", 
                        "memory", 
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
