import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *


##################################################################

class TestSchedulerMergeCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    def _prepare_tables(self):
        t1 = '//tmp/t1'
        create('table', t1)
        v1 = [{'key' + str(i) : 'value' + str(i)} for i in xrange(3)]
        for v in v1:
            write("<append=true>" + t1, v)

        t2 = '//tmp/t2'
        create('table', t2)
        v2 = [{'another_key' + str(i) : 'another_value' + str(i)} for i in xrange(4)]
        for v in v2:
            write("<append=true>" + t2, v)

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

        merge(combine_chunks=True,
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

        merge(combine_chunks=True,
              mode='ordered',
              in_=[self.t1, self.t2],
              out='//tmp/t_out')

        assert read('//tmp/t_out') == self.v1 + self.v2
        assert get('//tmp/t_out/@chunk_count') == 1

    def test_sorted(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')

        write('//tmp/t1', [{"a": 1}, {"a": 10}, {"a": 100}], sorted_by='a')
        write('//tmp/t2', [{"a": 2}, {"a": 3}, {"a": 15}], sorted_by='a')

        create('table', '//tmp/t_out')
        merge(mode='sorted',
              in_=['//tmp/t1', '//tmp/t2'],
              out='//tmp/t_out')

        assert read('//tmp/t_out') == [{'a': 1}, {'a': 2}, {'a': 3}, {'a': 10}, {'a': 15}, {'a': 100}]
        assert get('//tmp/t_out/@chunk_count') == 1 # resulting number of chunks is always equal to 1 (as long they are small)

    def test_sorted_trivial(self):
        create('table', '//tmp/t1')

        write('//tmp/t1', [{"a": 1}, {"a": 10}, {"a": 100}], sorted_by='a')

        create('table', '//tmp/t_out')
        merge(combine_chunks=True,
              mode='sorted',
              in_=['//tmp/t1'],
              out='//tmp/t_out')

        assert read('//tmp/t_out') == [{'a': 1}, {'a': 10}, {'a': 100}]
        assert get('//tmp/t_out/@chunk_count') == 1 # resulting number of chunks is always equal to 1 (as long they are small)

    def test_sorted_with_same_chunks(self):
        t1 = '//tmp/t1'
        t2 = '//tmp/t2'
        v = [{'key1' : 'value1'}]

        create("table", t1)
        write(t1, v[0])
        sort(in_=t1,
             out=t1,
             sort_by="key1")
        copy(t1, t2)

        create("table", "//tmp/t_out")
        merge(mode='sorted',
              in_=[t1, t2],
              out='//tmp/t_out')
        self.assertItemsEqual(read('//tmp/t_out'), sorted(v + v))

    def test_sorted_combine(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')

        write('//tmp/t1', [{"a": 1}, {"a": 10}, {"a": 100}], sorted_by='a')
        write('//tmp/t2', [{"a": 2}, {"a": 3}, {"a": 15}], sorted_by='a')

        create('table', '//tmp/t_out')
        merge(combine_chunks=True,
              mode='sorted',
              in_=['//tmp/t1', '//tmp/t2'],
              out='//tmp/t_out')

        assert read('//tmp/t_out') == [{'a': 1}, {'a': 2}, {'a': 3}, {'a': 10}, {'a': 15}, {'a': 100}]
        assert get('//tmp/t_out/@chunk_count') == 1

    def test_sorted_passthrough(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        create('table', '//tmp/t3')

        write('//tmp/t1', [{"k": "a", "s": 0}, {"k": "b", "s": 1}], sorted_by=['k', 's'])
        write('//tmp/t2', [{"k": "b", "s": 2}, {"k": "c", "s": 0}], sorted_by=['k', 's'])
        write('//tmp/t3', [{"k": "b", "s": 0}, {"k": "b", "s": 3}], sorted_by=['k', 's'])

        create('table', '//tmp/t_out')
        merge(mode='sorted',
              in_=['//tmp/t1', '//tmp/t2', '//tmp/t3', '//tmp/t2[(b, 3) : (b, 7)]'],
              out='//tmp/t_out',
              merge_by='k')

        res = read('//tmp/t_out')
        expected = [
            {'k' : 'a', 's' : 0},
            {'k' : 'b', 's' : 1},
            {'k' : 'b', 's' : 0},
            {'k' : 'b', 's' : 3},
            {'k' : 'b', 's' : 2},
            {'k' : 'c', 's' : 0}]

        self.assertItemsEqual(res, expected)

        merge(mode='sorted',
              in_=['//tmp/t1', '//tmp/t2', '//tmp/t3'],
              out='//tmp/t_out',
              merge_by='k')

        res = read('//tmp/t_out')
        self.assertItemsEqual(res, expected)

        assert get('//tmp/t_out/@chunk_count') == 3

        merge(mode='sorted',
              in_=['//tmp/t1', '//tmp/t2', '//tmp/t3'],
              out='//tmp/t_out',
              merge_by=['k', 's'])

        res = read('//tmp/t_out')
        expected = [
            {'k' : 'a', 's' : 0},
            {'k' : 'b', 's' : 0},
            {'k' : 'b', 's' : 1},
            {'k' : 'b', 's' : 2},
            {'k' : 'b', 's' : 3},
            {'k' : 'c', 's' : 0} ]

        for i, j in zip(res, expected):
            self.assertItemsEqual(i, j)

        assert get('//tmp/t_out/@chunk_count') == 1

    def test_sorted_with_maniacs(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        create('table', '//tmp/t3')

        write('//tmp/t1', [{"a": 3}, {"a": 3}, {"a": 3}], sorted_by='a')
        write('//tmp/t2', [{"a": 2}, {"a": 3}, {"a": 15}], sorted_by='a')
        write('//tmp/t3', [{"a": 1}, {"a": 3}], sorted_by='a')

        create('table', '//tmp/t_out')
        merge(combine_chunks=True,
              mode='sorted',
              in_=['//tmp/t1', '//tmp/t2', '//tmp/t3'],
              out='//tmp/t_out',
              opt='/spec/data_size_per_job=1')

        assert read('//tmp/t_out') == [{'a': 1}, {'a': 2}, {'a': 3}, {'a': 3}, {'a': 3}, {'a': 3}, {'a': 3}, {'a': 15}]
        assert get('//tmp/t_out/@chunk_count') == 3

    def test_sorted_by(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')

        a1 = {'a': 1, 'b': 20}
        a2 = {'a': 10, 'b': 1}
        a3 = {'a': 10, 'b': 2}

        b1 = {'a': 2, 'c': 10}
        b2 = {'a': 10, 'c': 0}
        b3 = {'a': 15, 'c': 5}

        write('//tmp/t1', [a1, a2, a3], sorted_by=['a', 'b'])
        write('//tmp/t2', [b1, b2, b3], sorted_by=['a', 'c'])

        create('table', '//tmp/t_out')

        # error when sorted_by of input tables are different and merge_by is not set
        with pytest.raises(YtError):
            merge(mode='sorted',
                  in_=['//tmp/t1', '//tmp/t2'],
                  out='//tmp/t_out')

        # now merge_by is set
        merge(mode='sorted',
              in_=['//tmp/t1', '//tmp/t2'],
              out='//tmp/t_out',
              merge_by='a')

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
               out='<append=true>//tmp/t_out')

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

        write('<append=true>//tmp/t_in', v)
        write('<append=true>//tmp/t_in', v)


        merge(combine_chunks=True,
               mode='ordered',
               in_='//tmp/t_in',
               out='<append=true>//tmp/t_in')

        assert read('//tmp/t_in') == [v, v, v, v]
        assert get('//tmp/t_in/@chunk_count') == 3 # only result of merge is combined


