import pytest

from yt_env_setup import YTEnvSetup, TOOLS_ROOTDIR
from yt_commands import *

import os
import sys

from collections import defaultdict

##################################################################

class TestSchedulerMapReduceCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    def do_run_test(self, method):
        tx = start_transaction()

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

        create('table', '//tmp/t_in', tx=tx)
        create('table', '//tmp/t_map_out', tx=tx)
        create('table', '//tmp/t_reduce_in', tx=tx)
        create('table', '//tmp/t_out', tx=tx)

        for line in text.split('\n'):
            write('<append=true>//tmp/t_in', {'line': line}, tx=tx)

        create('file', '//tmp/yt_streaming.py')
        create('file', '//tmp/mapper.py')
        create('file', '//tmp/reducer.py')

        upload_file('//tmp/yt_streaming.py', os.path.join(TOOLS_ROOTDIR, 'yt_streaming.py'), tx=tx)
        upload_file('//tmp/mapper.py', os.path.join(TOOLS_ROOTDIR, 'wc_mapper.py'), tx=tx)
        upload_file('//tmp/reducer.py', os.path.join(TOOLS_ROOTDIR, 'wc_reducer.py'), tx=tx)


        if method == 'map_sort_reduce':
            map(in_='//tmp/t_in',
                out='//tmp/t_map_out',
                command='python mapper.py',
                file=['//tmp/mapper.py', '//tmp/yt_streaming.py'],
                opt='/spec/mapper/format=dsv',
                tx=tx)

            sort(in_='//tmp/t_map_out',
                 out='//tmp/t_reduce_in',
                 sort_by='word',
                 tx=tx)

            reduce(in_='//tmp/t_reduce_in',
                   out='//tmp/t_out',
                   command='python reducer.py',
                   file=['//tmp/reducer.py', '//tmp/yt_streaming.py'],
                   opt='/spec/reducer/format=dsv',
                   tx=tx)
        elif method == 'map_reduce':
            map_reduce(in_='//tmp/t_in',
                       out='//tmp/t_out',
                       sort_by='word',
                       mapper_command='python mapper.py',
                       mapper_file=['//tmp/mapper.py', '//tmp/yt_streaming.py'],
                       reduce_combiner_command='python reducer.py',
                       reduce_combiner_file=['//tmp/reducer.py', '//tmp/yt_streaming.py'],
                       reducer_command='python reducer.py',
                       reducer_file=['//tmp/reducer.py', '//tmp/yt_streaming.py'],
                       opt=['/spec/partition_count=2', 
                            '/spec/map_job_count=2', 
                            '/spec/mapper/format=dsv', 
                            '/spec/reduce_combiner/format=dsv', 
                            '/spec/reducer/format=dsv',
                            '/spec/data_size_per_sort_job=10'],
                       tx=tx)
        elif method == 'map_reduce_1p':
            map_reduce(in_='//tmp/t_in',
                       out='//tmp/t_out',
                       sort_by='word',
                       mapper_command='python mapper.py',
                       mapper_file=['//tmp/mapper.py', '//tmp/yt_streaming.py'],
                       reducer_command='python reducer.py',
                       reducer_file=['//tmp/reducer.py', '//tmp/yt_streaming.py'],
                       opt=['/spec/partition_count=1', '/spec/mapper/format=dsv', '/spec/reducer/format=dsv'],
                       tx=tx)
        elif method == 'reduce_combiner_dev_null':
            map_reduce(in_='//tmp/t_in',
                       out='//tmp/t_out',
                       sort_by='word',
                       mapper_command='python mapper.py',
                       mapper_file=['//tmp/mapper.py', '//tmp/yt_streaming.py'],
                       reduce_combiner_command='cat >/dev/null',
                       reducer_command='python reducer.py',
                       reducer_file=['//tmp/reducer.py', '//tmp/yt_streaming.py'],
                       opt=['/spec/partition_count=2', 
                            '/spec/map_job_count=2', 
                            '/spec/mapper/format=dsv', 
                            '/spec/reduce_combiner/format=dsv', 
                            '/spec/reducer/format=dsv',
                            '/spec/data_size_per_sort_job=10'],
                       tx=tx)

        commit_transaction(tx)

        # count the desired output
        expected = defaultdict(int)
        for word in text.split():
            expected[word] += 1

        output = []
        if method != "reduce_combiner_dev_null":
            for word, count in expected.items():
                output.append( {'word': word, 'count': str(count)} )
            self.assertItemsEqual(read('//tmp/t_out'), output)
        else:
            self.assertItemsEqual(read('//tmp/t_out'), output)

    @only_linux
    def test_map_sort_reduce(self):
        self.do_run_test('map_sort_reduce')

    @only_linux
    def test_map_reduce(self):
        self.do_run_test('map_reduce')

    @only_linux
    def test_map_reduce_1partition(self):
        self.do_run_test('map_reduce_1p')

    @only_linux
    def test_map_reduce_reduce_combiner_dev_null(self):
        self.do_run_test('reduce_combiner_dev_null')

    @only_linux
    def test_many_output_tables(self):
        create('table', '//tmp/t_in')
        create('table', '//tmp/t_out1')
        create('table', '//tmp/t_out2')
        write('//tmp/t_in', {'line': "some_data"})
        map_reduce(in_='//tmp/t_in',
                   out=['//tmp/t_out1', '//tmp/t_out2'],
                   sort_by='line',
                   reducer_command='cat',
                   opt='/spec/reducer/format=dsv')

    def test_intermediate_live_preview(self):
        create_user('u')
        acl = [{"action": "allow", "subjects": ["u"], "permissions": ["write"]}]

        create('table', '//tmp/t1')
        write('//tmp/t1', {"foo": "bar"})
        create('table', '//tmp/t2')

        op_id = map_reduce(dont_track=True, mapper_command="cat", reducer_command="cat; sleep 1",
                           in_='//tmp/t1', out='//tmp/t2',
                           sort_by=["foo"], spec={"intermediate_data_acl": acl})

        time.sleep(0.5)
        assert exists('//sys/operations/{0}/intermediate'.format(op_id))
        assert acl == get('//sys/operations/{0}/intermediate/@acl'.format(op_id))

        track_op(op_id)
        assert read('//tmp/t2') == [{"foo": "bar"}]

    def test_incorrect_intermediate_data_acl(self):
        create_user('u')
        acl = [{"action": "allow", "subjects": ["u"], "permissions": ["blabla"]}]

        create('table', '//tmp/t1')
        write('//tmp/t1', {"foo": "bar"})
        create('table', '//tmp/t2')

        with pytest.raises(YtError):
            map_reduce(mapper_command="cat", reducer_command="cat",
                       in_='//tmp/t1', out='//tmp/t2',
                       sort_by=["foo"], spec={"intermediate_data_acl": acl})
