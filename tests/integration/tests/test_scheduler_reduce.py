import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestSchedulerReduceCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 5
    START_SCHEDULER = True

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
            out = ['//tmp/out'],
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

        assert get('//tmp/out/@sorted') == 'false'
        
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

        with pytest.raises(YTError):
            reduce(
                in_ = '//tmp/in',
                out = '//tmp/out',
                command = 'cat')

    def test_non_prefix(self):
        create('table', '//tmp/in')
        create('table', '//tmp/out')
        write('//tmp/in', {'key': '1', 'subkey': '2'}, sorted_by='key;subkey')

        with pytest.raises(YTError):
            reduce(
                in_ = '//tmp/in',
                out = '//tmp/out',
                command = 'cat',
                reduce_by='subkey')

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
        upload('//tmp/reducer.sh', reducer)

        reduce(in_='//tmp/t_in',
            out=output_tables,
            command='bash reducer.sh',
            file='//tmp/reducer.sh')

        assert read(output_tables[0]) == [{'v': 0}]
        assert read(output_tables[1]) == [{'v': 1}]
        assert read(output_tables[2]) == [{'v': 2}]

