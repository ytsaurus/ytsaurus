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
        
    def test_empty_columns(self):
        create('table', '//tmp/t_in')
        create('table', '//tmp/t_out')

        write('//tmp/t_in', {'foo': 'bar'})

        with pytest.raises(YTError):
            sort(in_='//tmp/t_in',
             out='//tmp/t_out',
             key_columns='')
       
    @pytest.mark.xfail(run = False, reason = 'Issue #353')
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
