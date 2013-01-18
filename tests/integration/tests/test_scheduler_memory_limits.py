import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import unittest
##################################################################

def check_memory_limit(op_id):
    jobs_path = '//sys/operations/' + op_id + '/jobs'
    for job_id in ls(jobs_path):
        inner_errors = get(jobs_path + '/' + job_id + '/@error/inner_errors')
        assert 'Memory limit exceeded' in inner_errors[0]['message']

class TestSchedulerMemoryLimits(YTEnvSetup, unittest.TestCase):
    NUM_MASTERS = 3
    NUM_NODES = 5
    START_SCHEDULER = True

    DELTA_NODE_CONFIG = {'exec_agent' :
        {'enforce_job_control' : 'true',
         'memory_watchdog_period' : 100}}


    #pytest.mark.xfail(run = False, reason = 'Set-uid-root before running.')
    def test_map(self):
        create('table', '//tmp/t_in')
        write_str('//tmp/t_in', '{value=value;subkey=subkey;key=key;a=another}')

        mapper = \
"""
a = list()
while True:
    a.append(''.join(['xxx'] * 10000))
"""
        upload('//tmp/mapper.py', mapper)

        create('table', '//tmp/t_out')

        op_id = map('--dont_track',
             in_='//tmp/t_in',
             out='//tmp/t_out',
             command="python mapper.py",
             file='//tmp/mapper.py')

        # if all jobs failed then operation is also failed
        with pytest.raises(YTError): track_op(op_id)
        # ToDo: check job error messages.
        check_memory_limit(op_id)
