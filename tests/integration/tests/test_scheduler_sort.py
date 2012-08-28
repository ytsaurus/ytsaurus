import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestSchedulerSortCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    START_SCHEDULER = True

    def test_simple(self):
        v1 = {'key' : 'aaa'}
        v2 = {'key' : 'bb'}
        v3 = {'key' : 'bbxx'}
        v4 = {'key' : 'zfoo'}
        v5 = {'key' : 'zzz'}

        create('table', '//tmp/t_in')
        write('//tmp/t_in', [v3, v5, v1, v2, v4]) # some random order

        create('table', '//tmp/t_out')

        sort(in_='//tmp/t_in',
             out='//tmp/t_out',
             sort_by='key')

        assert read('//tmp/t_out') == [v1, v2, v3, v4, v5]
        assert get('//tmp/t_out/@sorted') ==  'true'
        assert get('//tmp/t_out/@sorted_by') ==  ['key']

    # the same as test_simple but within transaction        
    def test_simple_transacted(self):
        tx = start_transaction()

        v1 = {'key' : 'aaa'}
        v2 = {'key' : 'bb'}
        v3 = {'key' : 'bbxx'}
        v4 = {'key' : 'zfoo'}
        v5 = {'key' : 'zzz'}

        create('table', '//tmp/t_in', tx=tx)
        write('//tmp/t_in', [v3, v5, v1, v2, v4], tx=tx) # some random order

        create('table', '//tmp/t_out', tx=tx)

        sort(in_='//tmp/t_in',
             out='//tmp/t_out',
             sort_by='key',
             tx=tx)

        commit_transaction(tx)

        assert read('//tmp/t_out') == [v1, v2, v3, v4, v5]
        assert get('//tmp/t_out/@sorted') ==  'true'
        assert get('//tmp/t_out/@sorted_by') ==  ['key']

    def test_empty_columns(self):
        create('table', '//tmp/t_in')
        create('table', '//tmp/t_out')

        write('//tmp/t_in', {'foo': 'bar'})

        with pytest.raises(YTError):
            sort(in_='//tmp/t_in',
                 out='//tmp/t_out',
                 sort_by='')
       
    def test_empty_in(self):
        create('table', '//tmp/t_in')
        create('table', '//tmp/t_out')

        sort(in_='//tmp/t_in',
             out='//tmp/t_out',
             sort_by='key')
         
        assert read('//tmp/t_out') == []

    def test_non_empty_out(self):
        create('table', '//tmp/t_in')
        create('table', '//tmp/t_out')

        write('//tmp/t_in', {'foo': 'bar'})
        write('//tmp/t_out', {'hello': 'world'})

        with pytest.raises(YTError):
            sort(in_='//tmp/t_in',
             out='//tmp/t_out',
             sort_by='foo')

    def test_missing_column(self):
        v1 = {'key' : 'aaa'}
        v2 = {'key' : 'bb'}
        v3 = {'key' : 'bbxx'}
        v4 = {'key' : 'zfoo'}
        v5 = {'key' : 'zzz'}

        create('table', '//tmp/t_in')
        write('//tmp/t_in', [v3, v5, v1, v2, v4]) # some random order

        create('table', '//tmp/t_out')

        sort(in_='//tmp/t_in',
             out='//tmp/t_out',
             sort_by='missing_key')

        assert len(read('//tmp/t_out')) == 5 # check only the number of raws

    def test_composite_key(self):
        v1 = {'key': -7, 'subkey': 'bar', 'value': 'v1'}
        v2 = {'key': -7, 'subkey': 'foo', 'value': 'v2'}
        v3 = {'key': 12, 'subkey': 'a', 'value': 'v3'}
        v4 = {'key': 12, 'subkey': 'z', 'value': 'v4'}
        v5 = {'key': 500, 'subkey': 'foo', 'value': 'v5'}

        create('table', '//tmp/t_in')
        write('//tmp/t_in', [v2, v5, v1, v4, v3]) # some random order

        create('table', '//tmp/t_out')

        sort(in_='//tmp/t_in',
             out='//tmp/t_out',
             sort_by='key; subkey')

        assert read('//tmp/t_out') == [v1, v2, v3, v4, v5]

        create('table', '//tmp/t_another_out')
        sort(in_='//tmp/t_out',
             out='//tmp/t_another_out',
             sort_by='subkey; key')

        assert read('//tmp/t_another_out') == [v3, v1, v2, v5, v4]

    def test_many_inputs(self):
        v1 = {'key': -7, 'value': 'v1'}
        v2 = {'key': -3, 'value': 'v2'}
        v3 = {'key': 0, 'value': 'v3'}
        v4 = {'key': 12, 'value': 'v4'}
        v5 = {'key': 500, 'value': 'v5'}
        v6 = {'key': 100500, 'value': 'v6'}

        create('table', '//tmp/in1')
        create('table', '//tmp/in2')

        write('//tmp/in1', [v5, v1, v4]) # some random order
        write('//tmp/in2', [v3, v6, v2]) # some random order

        create('table', '//tmp/t_out')
        sort(in_=['//tmp/in1', '//tmp/in2'],
             out='//tmp/t_out',
             sort_by='key')

        assert read('//tmp/t_out') == [v1, v2, v3, v4, v5, v6]

    def sort_with_options(self, **kwargs):
        input = '//tmp/in'
        output = '//tmp/out'
        create('table', input)
        create('table', output)
        write(input, [{'key': num} for num in xrange(5, 0, -1)])
        sort(in_=[input], out=output, sort_by='key', **kwargs)
        assert read(output) == [{'key': num} for num in xrange(1, 6)]

    def test_one_partition_no_merge(self):
        self.sort_with_options(opt='/spec/partition_count=1')

    def test_one_partition_with_merge(self):
        self.sort_with_options(opt=['/spec/partition_count=1', '/spec/max_weight_per_sort_job=1'])

    def test_two_partitions_no_merge(self):
        self.sort_with_options(opt='/spec/partition_count=2')

    def test_two_partitions_with_merge(self):
        self.sort_with_options(opt=['/spec/partition_count=2', '/spec/max_weight_per_sort_job=1'])
