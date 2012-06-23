import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestCypressCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 0

    def test_invalid_cases(self):
        # path not starting with /
        with pytest.raises(YTError): set('a', 20)

        # path starting with single /
        with pytest.raises(YTError): set('/a', 20)

        # empty path
        with pytest.raises(YTError): set('', 20)

        # empty token in path
        with pytest.raises(YTError): set('//tmp/a//b', 20)

        # change the type of root
        with pytest.raises(YTError): set('/', [])

        # set the root to the empty map
        # with pytest.raises(YTError): set('/', {}))

        # remove the root
        with pytest.raises(YTError): remove('/')
        # get non existent child
        with pytest.raises(YTError): get('//tmp/b')

        # remove non existent child
        with pytest.raises(YTError): remove('//tmp/b')

        # can't create entity node inside cypress
        with pytest.raises(YTError): set_str('//tmp/entity', '#')

    def test_list(self):
        set('//tmp/list', [1,2,"some string"])
        assert get('//tmp/list') == [1,2,"some string"]

        set('//tmp/list/+', 100)
        assert get('//tmp/list') == [1,2,"some string",100]

        set('//tmp/list/^0', 200)
        assert get('//tmp/list') == [200,1,2,"some string",100]

        set('//tmp/list/^0', 500)
        assert get('//tmp/list') == [500,200,1,2,"some string",100]

        set('//tmp/list/2^', 1000)
        assert get('//tmp/list') == [500,200,1,1000,2,"some string",100]

        set('//tmp/list/3', 777)
        assert get('//tmp/list') == [500,200,1,777,2,"some string",100]

        remove('//tmp/list/4')
        assert get('//tmp/list') == [500,200,1,777,"some string",100]

        remove('//tmp/list/4')
        assert get('//tmp/list') == [500,200,1,777,100]

        remove('//tmp/list/0')
        assert get('//tmp/list') == [200,1,777,100]

        set('//tmp/list/+', 'last')
        assert get('//tmp/list') == [200,1,777,100,"last"]

        set('//tmp/list/^0', 'first')
        assert get('//tmp/list') == ["first",200,1,777,100,"last"]

    def test_map(self):
        set('//tmp/map', {'hello': 'world', 'list':[0,'a',{}], 'n': 1})
        assert get('//tmp/map') == {"hello":"world","list":[0,"a",{}],"n":1}

        set('//tmp/map/hello', 'not_world')
        assert get('//tmp/map') == {"hello":"not_world","list":[0,"a",{}],"n":1}

        set('//tmp/map/list/2/some', 'value')
        assert get('//tmp/map') == {"hello":"not_world","list":[0,"a",{"some":"value"}],"n":1}

        remove('//tmp/map/n')
        assert get('//tmp/map') ==  {"hello":"not_world","list":[0,"a",{"some":"value"}]}

        set('//tmp/map/list', [])
        assert get('//tmp/map') == {"hello":"not_world","list":[]}

        set('//tmp/map/list/+', {})
        set('//tmp/map/list/0/a', 1)
        assert get('//tmp/map') == {"hello":"not_world","list":[{"a":1}]}

        set('//tmp/map/list/^0', {})
        set('//tmp/map/list/0/b', 2)
        assert get('//tmp/map') == {"hello":"not_world","list":[{"b":2},{"a":1}]}

        remove('//tmp/map/hello')
        assert get('//tmp/map') == {"list":[{"b":2},{"a":1}]}

        remove('//tmp/map/list')
        assert get('//tmp/map') == {}

    def test_attributes(self):
        set_str('//tmp/t', '<attr=100;mode=rw> {nodes=[1; 2]}')
        assert get('//tmp/t/@attr') == 100
        assert get('//tmp/t/@mode') == "rw"

        remove('//tmp/t/@')
        with pytest.raises(YTError): get('//tmp/t/@attr')
        with pytest.raises(YTError): get('//tmp/t/@mode')

        # changing attributes
        set_str('//tmp/t/a', '< author=ignat > []')
        assert get('//tmp/t/a') == []
        assert get('//tmp/t/a/@author') == "ignat"

        set('//tmp/t/a/@author', "not_ignat")
        assert get('//tmp/t/a/@author') == "not_ignat"

        # nested attributes (actually shows <>)
        set_str('//tmp/t/b', '<dir = <file = <>-100> #> []')
        assert get_str('//tmp/t/b/@dir/@') == '{"file"=<>-100}'
        assert get_str('//tmp/t/b/@dir/@file') == '<>-100'
        assert get_str('//tmp/t/b/@dir/@file/@') == '{}'

        # set attributes directly
        set('//tmp/t/@', {'key1': 'value1', 'key2': 'value2'})
        assert get('//tmp/t/@key1') == "value1"
        assert get('//tmp/t/@key2') == "value2"

    def test_format_json(self):
        # check input format for json
        set_str('//tmp/json_in', '{"list": [1,2,{"string": "this"}]}', format="json")
        assert get('//tmp/json_in') == {"list": [1, 2, {"string": "this"}]}

        # check output format for json
        set('//tmp/json_out', {'list': [1, 2, {'string': 'this'}]})
        assert get_str('//tmp/json_out', format="json") == '{"list":[1,2,{"string":"this"}]}'

    def test_remove(self):
        # remove items from map
        set('//tmp/map', {"a" : "b", "c": "d"})
        assert get('//tmp/map/@count') == 2
        remove('//tmp/map/*')
        assert get('//tmp/map') == {}
        assert get('//tmp/map/@count') == 0

        # remove items from list
        set('//tmp/list', [10, 20, 30])
        assert get('//tmp/list/@count') == 3
        remove('//tmp/list/*')
        assert get('//tmp/list') == []
        assert get('//tmp/list/@count') == 0

