import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestSchedulerMergeCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 5
    NUM_SCHEDULERS = 1

    def _prepare_tables(self):
        t1 = '//tmp/t1'
        create('table', t1)
        v1 = [{'key' + str(i) : 'value' + str(i)} for i in xrange(3)]
        for v in v1:
            write(t1, v)

        t2 = '//tmp/t2'
        create('table', t2)
        v2 = [{'another_key' + str(i) : 'another_value' + str(i)} for i in xrange(4)]
        for v in v2:
            write(t2, v)

        self.t1 = t1
        self.t2 = t2
        
        self.v1 = v1
        self.v2 = v2

        create('table', '//tmp/t_out')

    # usual cases
    def test_merge_unordered(self):
        self._prepare_tables()

        merge(mode='unordered',
              input=[self.t1, self.t2], 
              out='//tmp/t_out')
        
        self.assertItemsEqual(read('//tmp/t_out'), self.v1 + self.v2)
        assert get('//tmp/t_out/@chunk_count') == 7

    def test_merge_unordered_combine(self):
        self._prepare_tables()

        merge('--combine',
              mode='unordered',
              input=[self.t1, self.t2],
              out='//tmp/t_out')

        self.assertItemsEqual(read('//tmp/t_out'), self.v1 + self.v2)
        assert get('//tmp/t_out/@chunk_count') == 1

    def test_merge_ordered(self):
        self._prepare_tables()

        merge(mode='ordered',
              input=[self.t1, self.t2],
              out='//tmp/t_out')

        assert read('//tmp/t_out') == self.v1 + self.v2
        assert get('//tmp/t_out/@chunk_count') ==7

    def test_merge_ordered_combine(self):
        self._prepare_tables()

        merge('--combine',
              mode='ordered',
              input=[self.t1, self.t2],
              out='//tmp/t_out')

        assert read('//tmp/t_out') == self.v1 + self.v2
        assert get('//tmp/t_out/@chunk_count') == 1

    def test_merge_sorted(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')

        write_str('//tmp/t1', '{a = 1}; {a = 10}; {a = 100}', sorted_by='a')
        write_str('//tmp/t2', '{a = 2}; {a = 3}; {a = 15}', sorted_by='a')

        create('table', '//tmp/t_out')
        merge(mode='sorted',
              input=['//tmp/t1', '//tmp/t2'],
              out='//tmp/t_out')

        assert read('//tmp/t_out') == [{'a': 1}, {'a': 2}, {'a': 3}, {'a': 10}, {'a': 15}, {'a': 100}]
        assert get('//tmp/t_out/@chunk_count') == 1

    def test_merge_sorted_combine(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')

        write_str('//tmp/t1', '{a = 1}; {a = 10}; {a = 100}', sorted_by='a')
        write_str('//tmp/t2', '{a = 2}; {a = 3}; {a = 15}', sorted_by='a')

        create('table', '//tmp/t_out')
        merge('--combine',
              mode='sorted',
              input=['//tmp/t1', '//tmp/t2'],
              out='//tmp/t_out')

        assert read('//tmp/t_out') == [{'a': 1}, {'a': 2}, {'a': 3}, {'a': 10}, {'a': 15}, {'a': 100}]
        assert get('//tmp/t_out/@chunk_count') == 1

    def test_merge_sorted_key_columns(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')

        a1 = {'a': 1, 'b': 20}
        a2 = {'a': 10, 'b': 1}
        a3 = {'a': 10, 'b': 2}

        b1 = {'a': 2, 'c': 10}
        b2 = {'a': 10, 'c': 0}
        b3 = {'a': 15, 'c': 5}

        write('//tmp/t1', [a1, a2, a3], sorted_by='a;b')
        write('//tmp/t2', [b1, b2, b3], sorted_by='a;c')

        create('table', '//tmp/t_out')

        # error when sorted_by of input tables is different and key_columns is not set
        with pytest.raises(YTError): 
            merge(mode='sorted',
              input=['//tmp/t1', '//tmp/t2'],
              out='//tmp/t_out')

        # now key_columns are set
        merge(mode='sorted',
              input=['//tmp/t1', '//tmp/t2'],
              out='//tmp/t_out',
              key_colums='a')

        result = read('//tmp/t_out')
        assert result[:2] == [a1, b1]
        self.assertItemsEqual(result[2:5], [a2, a3, b2])
        assert result[5] == b3
        
