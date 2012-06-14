import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestSchedulerCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 5
    NUM_SCHEDULERS = 1

    def test_map_one_chunk(self):
    	create('table', '//tmp/t1')
    	create('table', '//tmp/t2')
    	write('//tmp/t1', '{a=b}')
    	map(input='//tmp/t1', out='//tmp/t2', mapper='cat')

    	assert read_table('//tmp/t2') == [{'a' : 'b'}]


