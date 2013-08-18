import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import sys

##################################################################

class TestSchedulerReduceCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    @pytest.mark.skipif("not sys.platform.startswith(\"linux\")")
    def test_tricky_chunk_boundaries(self):
        create('table', '//tmp/in1')
        write(
            '//tmp/in1',
            [
                {'key': "0", 'value': 1},
                {'key': "2", 'value': 2}
            ],
            sorted_by = ['key', 'value'])

        create('table', '//tmp/in2')
        write(
            '//tmp/in2',
            [
                {'key': "2", 'value': 6},
                {'key': "5", 'value': 8}
            ],
            sorted_by = ['key', 'value'])

        create('table', '//tmp/out')

        reduce(
            in_ = ['//tmp/in1{key}', '//tmp/in2{key}'],
            out = ['<sorted_by=[key]>//tmp/out'],
            command = 'uniq',
            reduce_by = 'key',
            opt = ['/spec/reducer/format=<line_prefix=tskv>dsv',
                   '/spec/data_size_per_job=1'])

        assert read('//tmp/out') == \
            [
                {'key': "0"},
                {'key': "2"},
                {'key': "5"}
            ]

        assert get('//tmp/out/@sorted') == 'true'

    @pytest.mark.skipif("not sys.platform.startswith(\"linux\")")
    def test_cat(self):
        create('table', '//tmp/in1')
        write(
            '//tmp/in1',
            [
                {'key': 0, 'value': 1},
                {'key': 2, 'value': 2},
                {'key': 4, 'value': 3},
                {'key': 7, 'value': 4}
            ],
            sorted_by = 'key')

        create('table', '//tmp/in2')
        write(
            '//tmp/in2',
            [
                {'key': -1,'value': 5},
                {'key': 1, 'value': 6},
                {'key': 3, 'value': 7},
                {'key': 5, 'value': 8}
            ],
            sorted_by = 'key')

        create('table', '//tmp/out')

        reduce(
            in_ = ['//tmp/in1', '//tmp/in2'],
            out = ['<sorted_by=[key]>//tmp/out'],
            command = 'cat')

        assert read('//tmp/out') == \
            [
                {'key': -1,'value': 5},
                {'key': 0, 'value': 1},
                {'key': 1, 'value': 6},
                {'key': 2, 'value': 2},
                {'key': 3, 'value': 7},
                {'key': 4, 'value': 3},
                {'key': 5, 'value': 8},
                {'key': 7, 'value': 4}
            ]

        assert get('//tmp/out/@sorted') == 'true'


    @pytest.mark.skipif("not sys.platform.startswith(\"linux\")")
    def test_cat_primary(self):
        create('table', '//tmp/in1')
        write(
            '//tmp/in1',
            [
                {'key': 0, 'value': 1},
                {'key': 2, 'value': 2},
                {'key': 4, 'value': 3},
                {'key': 7, 'value': 4}
            ],
            sorted_by = 'key; value')

        create('table', '//tmp/in2')
        write(
            '//tmp/in2',
            [
                {'key': 8,'value': 5},
                {'key': 9, 'value': 6},
            ],
            sorted_by = 'key;value')

        create('table', '//tmp/in3')
        write(
            '//tmp/in3',
            [ {'key': 8,'value': 1}, ],
            sorted_by = 'key;value')

        create('table', '//tmp/in4')
        write(
            '//tmp/in4',
            [ {'key': 10,'value': 1}, ],
            sorted_by = 'key;value')

        create('table', '//tmp/out1')
        create('table', '//tmp/out2')

        reduce(
            in_ = ['<primary=true>//tmp/in1', '<primary=true>//tmp/in2', '//tmp/in3', '//tmp/in4'],
            out = ['<sorted_by=[key]>//tmp/out1', '<sorted_by=[key]>//tmp/out2'],
            command = 'cat>/dev/fd/4',
            reduce_by = 'key')

        print read('//tmp/out1')
        print read('//tmp/out2')

        print get('//tmp/out1/@chunk_ids')

        assert read('//tmp/out1') == \
            [
                {'key': 0, 'value': 1},
                {'key': 2, 'value': 2},
                {'key': 4, 'value': 3},
                {'key': 7, 'value': 4}
            ]

        assert read('//tmp/out2') == \
            [
                {'key': 8,'value': 1},
                {'key': 8,'value': 5},
                {'key': 9, 'value': 6},
                {'key': 10,'value': 1},
            ]

        assert get('//tmp/out1/@sorted') == 'true'
        assert get('//tmp/out2/@sorted') == 'true'

    @pytest.mark.skipif("not sys.platform.startswith(\"linux\")")
    def test_maniac_chunk(self):
        create('table', '//tmp/in1')
        write(
            '//tmp/in1',
            [
                {'key': 0, 'value': 1},
                {'key': 2, 'value': 9}
            ],
            sorted_by = 'key')

        create('table', '//tmp/in2')
        write(
            '//tmp/in2',
            [
                {'key': 2, 'value': 6},
                {'key': 2, 'value': 7},
                {'key': 2, 'value': 8}
            ],
            sorted_by = 'key')

        create('table', '//tmp/out')

        reduce(
            in_ = ['//tmp/in1', '//tmp/in2'],
            out = ['<sorted_by=[key]>//tmp/out'],
            command = 'cat')

        assert read('//tmp/out') == \
            [
                {'key': 0, 'value': 1},
                {'key': 2, 'value': 6},
                {'key': 2, 'value': 9},
                {'key': 2, 'value': 7},
                {'key': 2, 'value': 8}
            ]

        assert get('//tmp/out/@sorted') == 'true'


    def test_empty_in(self):
        create('table', '//tmp/in')

        # TODO(panin): replace it with sort of empty input (when it will be fixed)
        write('//tmp/in', {'foo': 'bar'}, sorted_by='a')
        erase('//tmp/in')

        create('table', '//tmp/out')

        reduce(
            in_ = '//tmp/in',
            out = '//tmp/out',
            command = 'cat')

        assert read('//tmp/out') == []

    def test_unsorted_input(self):
        create('table', '//tmp/in')
        create('table', '//tmp/out')
        write('//tmp/in', {'foo': 'bar'})

        with pytest.raises(YtError):
            reduce(
                in_ = '//tmp/in',
                out = '//tmp/out',
                command = 'cat')

    def test_non_prefix(self):
        create('table', '//tmp/in')
        create('table', '//tmp/out')
        write('//tmp/in', {'key': '1', 'subkey': '2'}, sorted_by=['key', 'subkey'])

        with pytest.raises(YtError):
            reduce(
                in_ = '//tmp/in',
                out = '//tmp/out',
                command = 'cat',
                reduce_by='subkey')

    @pytest.mark.skipif("not sys.platform.startswith(\"linux\")")
    def test_many_output_tables(self):
        output_tables = ['//tmp/t%d' % i for i in range(3)]

        create('table', '//tmp/t_in')
        for table_path in output_tables:
            create('table', table_path)

        write_str('//tmp/t_in', '{k=10}', sorted_by='k')

        reducer = \
"""
cat  > /dev/null
echo {v = 0} >&1
echo {v = 1} >&4
echo {v = 2} >&7

"""
        create('file', '//tmp/reducer.sh')
        upload('//tmp/reducer.sh', reducer)

        reduce(in_='//tmp/t_in',
            out=output_tables,
            command='bash reducer.sh',
            file='//tmp/reducer.sh')

        assert read(output_tables[0]) == [{'v': 0}]
        assert read(output_tables[1]) == [{'v': 1}]
        assert read(output_tables[2]) == [{'v': 2}]

