import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestSchedulerSortCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 5
    NUM_SCHEDULERS = 1

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
             key_columns='key')

        assert read('//tmp/t_out') == [v1, v2, v3, v4, v5]
        assert get('//tmp/t_out/@sorted') ==  'true'
        assert get('//tmp/t_out/@key_columns') ==  ['key']

    # the same as test_simple but within transaction        
    def test_simple_transacted(self):
        tx_id = start_transaction()

        v1 = {'key' : 'aaa'}
        v2 = {'key' : 'bb'}
        v3 = {'key' : 'bbxx'}
        v4 = {'key' : 'zfoo'}
        v5 = {'key' : 'zzz'}

        create('table', '//tmp/t_in', tx=tx_id)
        write('//tmp/t_in', [v3, v5, v1, v2, v4], tx=tx_id) # some random order

        create('table', '//tmp/t_out', tx=tx_id)

        sort(in_='//tmp/t_in',
             out='//tmp/t_out',
             key_columns='key', tx=tx_id)

        commit_transaction(tx=tx_id)

        assert read('//tmp/t_out') == [v1, v2, v3, v4, v5]
        assert get('//tmp/t_out/@sorted') ==  'true'
        assert get('//tmp/t_out/@key_columns') ==  ['key']

    def test_empty_columns(self):
        create('table', '//tmp/t_in')
        create('table', '//tmp/t_out')

        write('//tmp/t_in', {'foo': 'bar'})

        with pytest.raises(YTError):
            sort(in_='//tmp/t_in',
             out='//tmp/t_out',
             key_columns='')
       
    def test_empty_in(self):
        create('table', '//tmp/t_in')
        create('table', '//tmp/t_out')

        sort(in_='//tmp/t_in',
             out='//tmp/t_out',
             key_columns='key')
         
        assert read('//tmp/t_out') == []

    def test_non_empty_out(self):
        create('table', '//tmp/t_in')
        create('table', '//tmp/t_out')

        write('//tmp/t_in', {'foo': 'bar'})
        write('//tmp/t_out', {'hello': 'world'})

        with pytest.raises(YTError):
            sort(in_='//tmp/t_in',
             out='//tmp/t_out',
             key_columns='foo')

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
             key_columns='missing_key')

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
             key_columns='key; subkey')

        assert read('//tmp/t_out') == [v1, v2, v3, v4, v5]

        create('table', '//tmp/t_another_out')
        sort(in_='//tmp/t_out',
             out='//tmp/t_another_out',
             key_columns='subkey; key')

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
             key_columns='key')

        assert read('//tmp/t_out') == [v1, v2, v3, v4, v5, v6]
