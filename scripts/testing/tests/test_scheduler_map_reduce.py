import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

from yt_env import TOOLS_ROOTDIR
import os

from collections import defaultdict

##################################################################

@pytest.mark.xfail(run = False, reason = 'Issue #355')
class TestSchedulerMapReduceCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 5
    NUM_SCHEDULERS = 1


    def test_wc(self):
        tx_id = start_transaction()

        text = \
"""
So, so you think you can tell Heaven from Hell,
blue skies from pain.
Can you tell a green field from a cold steel rail?
A smile from a veil?
Do you think you can tell?
And did they get you to trade your heroes for ghosts?
Hot ashes for trees?
Hot air for a cool breeze?
Cold comfort for change?
And did you exchange a walk on part in the war for a lead role in a cage?
How I wish, how I wish you were here.
We're just two lost souls swimming in a fish bowl, year after year,
Running over the same old ground.
What have you found? The same old fears.
Wish you were here.
"""
        
        # remove punctuation from text
        stop_symbols = ",.?"
        for s in stop_symbols:
            text = text.replace(s, ' ')

        create('table', '//tmp/t_in')
        for line in text.split('\n'):
            write('//tmp/t_in', {'line': line}, tx=tx_id)
        
        upload_file('//tmp/mapper.py', os.path.join(TOOLS_ROOTDIR, 'wc_mapper.py'), tx=tx_id)
        create('table', '//tmp/t_map_out', tx=tx_id)
        map(in_='//tmp/t_in',
            out='//tmp/t_map_out',
            mapper='python mapper.py',
            file='//tmp/mapper.py',
            tx=tx_id)

        create('table', '//tmp/t_reduce_in', tx=tx_id)
        sort(in_='//tmp/t_map_out',
             out='//tmp/reduce_in',
             key_columns='word',
             tx=tx_id)

        upload_file('//tmp/reducer.py', os.path.join(TOOLS_ROOTDIR, 'wc_reducer.py'), tx=tx_id)
        create('table', '//tmp/t_out')
        reduce(in_='//tmp/t_reduce_in',
               out='//tmp/t_out',
               reducer='python reducer.py',
               file='//tmp/reducer.py',
               tx=tx_id)
           
        commit_transaction(tx=tx_id)

        # count the desired output
        expected = defaultdict(int)
        for word in text.split():
            expected[word] += 1

        assert read('//tmp/t_out') == [expected]
