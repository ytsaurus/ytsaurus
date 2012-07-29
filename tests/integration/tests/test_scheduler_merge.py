import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestSchedulerMergeCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 5
    START_SCHEDULER = True

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
    def test_unordered(self):
        self._prepare_tables()

        merge(mode='unordered',
              in_=[self.t1, self.t2], 
              out='//tmp/t_out')
        
        self.assertItemsEqual(read('//tmp/t_out'), self.v1 + self.v2)
        assert get('//tmp/t_out/@chunk_count') == 7

    def test_unordered_combine(self):
        self._prepare_tables()

        merge('--combine',
              mode='unordered',
              in_=[self.t1, self.t2],
              out='//tmp/t_out')

        self.assertItemsEqual(read('//tmp/t_out'), self.v1 + self.v2)
        assert get('//tmp/t_out/@chunk_count') == 1

    def test_ordered(self):
        self._prepare_tables()

        merge(mode='ordered',
              in_=[self.t1, self.t2],
              out='//tmp/t_out')

        assert read('//tmp/t_out') == self.v1 + self.v2
        assert get('//tmp/t_out/@chunk_count') ==7

    def test_ordered_combine(self):
        self._prepare_tables()

        merge('--combine',
              mode='ordered',
              in_=[self.t1, self.t2],
              out='//tmp/t_out')

        assert read('//tmp/t_out') == self.v1 + self.v2
        assert get('//tmp/t_out/@chunk_count') == 1

    def test_sorted(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')

        write_str('//tmp/t1', '{a = 1}; {a = 10}; {a = 100}', sorted_by='a')
        write_str('//tmp/t2', '{a = 2}; {a = 3}; {a = 15}', sorted_by='a')

        create('table', '//tmp/t_out')
        merge(mode='sorted',
              in_=['//tmp/t1', '//tmp/t2'],
              out='//tmp/t_out')

        assert read('//tmp/t_out') == [{'a': 1}, {'a': 2}, {'a': 3}, {'a': 10}, {'a': 15}, {'a': 100}]
        assert get('//tmp/t_out/@chunk_count') == 1 # resulting number of chunks is always equal to 1 (as long they are small)

    def test_sorted_combine(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')

        write_str('//tmp/t1', '{a = 1}; {a = 10}; {a = 100}', sorted_by='a')
        write_str('//tmp/t2', '{a = 2}; {a = 3}; {a = 15}', sorted_by='a')

        create('table', '//tmp/t_out')
        merge('--combine',
              mode='sorted',
              in_=['//tmp/t1', '//tmp/t2'],
              out='//tmp/t_out')

        assert read('//tmp/t_out') == [{'a': 1}, {'a': 2}, {'a': 3}, {'a': 10}, {'a': 15}, {'a': 100}]
        assert get('//tmp/t_out/@chunk_count') == 1

    def test_sorted_key_columns(self):
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

        # error when sorted_by of input tables are different and key_columns is not set
        with pytest.raises(YTError): 
            merge(mode='sorted',
              in_=['//tmp/t1', '//tmp/t2'],
              out='//tmp/t_out')

        # now key_columns are set
        merge(mode='sorted',
              in_=['//tmp/t1', '//tmp/t2'],
              out='//tmp/t_out',
              key_columns='a')

        result = read('//tmp/t_out')
        assert result[:2] == [a1, b1]
        self.assertItemsEqual(result[2:5], [a2, a3, b2])
        assert result[5] == b3

    def test_empty_in(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        create('table', '//tmp/t_out')

        v = {'foo': 'bar'}
        write('//tmp/t1', v)

        merge(mode='ordered',
               in_=['//tmp/t1', '//tmp/t2'],
               out='//tmp/t_out')
       
        assert read('//tmp/t_out') == [v]

    def test_non_empty_out(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        create('table', '//tmp/t_out')

        v1 = {'value': 1}
        v2 = {'value': 2}
        v3 = {'value': 3}

        write('//tmp/t1', v1)
        write('//tmp/t2', v2)
        write('//tmp/t_out', v3)

        merge(mode='ordered',
               in_=['//tmp/t1', '//tmp/t2'],
               out='//tmp/t_out')
       
        assert read('//tmp/t_out') == [v3, v1, v2]

    def test_multiple_in(self):
        create('table', '//tmp/t_in')
        create('table', '//tmp/t_out')

        v = {'foo': 'bar'}

        write('//tmp/t_in', v)

        merge(mode='ordered',
               in_=['//tmp/t_in', '//tmp/t_in', '//tmp/t_in'],
               out='//tmp/t_out')
       
        assert read('//tmp/t_out') == [v, v, v]

    def test_in_equal_to_out(self):
        create('table', '//tmp/t_in')

        v = {'foo': 'bar'}

        write('//tmp/t_in', v)
        write('//tmp/t_in', v)


        merge('--combine',
               mode='ordered',
               in_='//tmp/t_in',
               out='//tmp/t_in')
       
        assert read('//tmp/t_in') == [v, v, v, v]
        assert get('//tmp/t_in/@chunk_count') == 3 # only result of merge is combined


