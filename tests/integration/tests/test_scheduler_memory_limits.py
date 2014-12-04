import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import sys

##################################################################

'''
This test only works when suid bit is set.
'''

def check_memory_limit(op_id):
    jobs_path = '//sys/operations/' + op_id + '/jobs'
    for job_id in ls(jobs_path):
        inner_errors = get(jobs_path + '/' + job_id + '/@error/inner_errors')
        assert 'Memory limit exceeded' in inner_errors[0]['message']

class TestSchedulerMemoryLimits(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        'exec_agent' : {
            'force_enable_accounting' : 'true',
            'enable_cgroup_memory_hierarchy' : 'true',
            'slot_manager' : {
                'enforce_job_control'    : 'true',
                'enable_cgroups'         : 'false',
                'memory_watchdog_period' : 100
            }
        }
    }

    #pytest.mark.xfail(run = False, reason = 'Set-uid-root before running.')
    @only_linux
    def test_map(self):
        create('table', '//tmp/t_in')
        write('//tmp/t_in', {"value": "value", "subkey": "subkey", "key": "key", "a": "another"})

        mapper = \
"""
a = list()
while True:
    a.append(''.join(['xxx'] * 10000))
"""

        create('file', '//tmp/mapper.py')
        upload('//tmp/mapper.py', mapper)

        create('table', '//tmp/t_out')

        op_id = map(dont_track=True,
             in_='//tmp/t_in',
             out='//tmp/t_out',
             command='python mapper.py',
             file='//tmp/mapper.py',
             opt=['/spec/max_failed_job_count=5'])

        # if all jobs failed then operation is also failed
        with pytest.raises(YtError): track_op(op_id)
        # ToDo: check job error messages.
        check_memory_limit(op_id)

    @only_linux
    def test_dirty_sandbox(self):
        create('table', '//tmp/t_in')
        write('//tmp/t_in', {"value": "value", "subkey": "subkey", "key": "key", "a": "another"})

        create('table', '//tmp/t_out')

        command = 'cat > /dev/null; mkdir ./tmpxxx; echo 1 > ./tmpxxx/f1; chmod 700 ./tmpxxx;'
        map(in_='//tmp/t_in', out='//tmp/t_out', command=command)
