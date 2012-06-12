
import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *


class TestCypressCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 0

    def test_invalid_cases(self):
        # path not starting with /
        with pytest.raises(YTError): set('a', '20')

        # path starting with single /
        with pytest.raises(YTError): set('/a', '20')

        # empty path
        with pytest.raises(YTError): set('', '20')

        # empty token in path
        with pytest.raises(YTError): set('//a//b', '30')

        # change the type of root
        with pytest.raises(YTError): set('/', '[]')

        # set the root to the empty map
        # expect_error( set('/', '{}'))

        # remove the root
        with pytest.raises(YTError): remove('/')
        # get non existent child
        with pytest.raises(YTError): get('//b')

        # remove non existent child
        with pytest.raises(YTError): remove('//b')

    def test_list(self):
        set('//list', '[1;2;"some string"]')
        assert get('//list') == '[1;2;"some string"]'

        set('//list/+', '100')
        assert get('//list') == '[1;2;"some string";100]'

        set('//list/^0', '200')
        assert get('//list') == '[200;1;2;"some string";100]'

        set('//list/^0', '500')
        assert get('//list') == '[500;200;1;2;"some string";100]'

        set('//list/2^', '1000')
        assert get('//list') == '[500;200;1;1000;2;"some string";100]'

        set('//list/3', '777')
        assert get('//list') == '[500;200;1;777;2;"some string";100]'

        remove('//list/4')
        assert get('//list') == '[500;200;1;777;"some string";100]'

        remove('//list/4')
        assert get('//list') == '[500;200;1;777;100]'

        remove('//list/0')
        assert get('//list') == '[200;1;777;100]'

        set('//list/+', 'last')
        assert get('//list') == '[200;1;777;100;"last"]'

        set('//list/^0', 'first')
        assert get('//list') == '["first";200;1;777;100;"last"]'

        remove('//list')

    def test_map(self):
        set('//map', '{hello=world; list=[0;a;{}]; n=1}')
        assert get_py('//map') == {"hello":"world","list":[0,"a",{}],"n":1}

        set('//map/hello', 'not_world')
        assert get_py('//map') == {"hello":"not_world","list":[0,"a",{}],"n":1}

        set('//map/list/2/some', 'value')
        assert get_py('//map') == {"hello":"not_world","list":[0,"a",{"some":"value"}],"n":1}

        remove('//map/n')
        assert get_py('//map') ==  {"hello":"not_world","list":[0,"a",{"some":"value"}]}

        set('//map/list', '[]')
        assert get_py('//map') == {"hello":"not_world","list":[]}

        set('//map/list/+', '{}')
        set('//map/list/0/a', '1')
        assert get_py('//map') == {"hello":"not_world","list":[{"a":1}]}

        set('//map/list/^0', '{}')
        set('//map/list/0/b', '2')
        assert get_py('//map') == {"hello":"not_world","list":[{"b":2},{"a":1}]}

        remove('//map/hello')
        assert get_py('//map') == {"list":[{"b":2},{"a":1}]}

        remove('//map/list')
        assert get_py('//map') == {}

        remove('//map')


    def test_attributes(self):
        set('//t', '<attr=100;mode=rw> {nodes=[1; 2]}')
        assert get('//t/@attr') == '100'
        assert get('//t/@mode') == '"rw"'

        remove('//t/@')
        with pytest.raises(YTError): get('//t/@attr')
        with pytest.raises(YTError): get('//t/@mode')

        # changing attributes
        set('//t/a', '< author=ignat > []')
        assert get('//t/a') == '[]'
        assert get('//t/a/@author') == '"ignat"'

        set('//t/a/@author', '"not_ignat"')
        assert get('//t/a/@author') == '"not_ignat"'

        # nested attributes (actually shows <>)
        set('//t/b', '<dir = <file = <>-100> #> []')
        assert get('//t/b/@dir/@') == '{"file"=<>-100}'
        assert get('//t/b/@dir/@file') == '<>-100'
        assert get('//t/b/@dir/@file/@') == '{}'

        # a couple of attributes
        set('//t', '<key1=value1,key2=value2>{}')
        assert get('//t/@key1') == '"value1"'
        assert get('//t/@key2') == '"value2"'

        remove('//t')


