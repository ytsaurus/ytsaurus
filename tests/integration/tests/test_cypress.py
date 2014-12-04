import pytest
import time

from yt_env_setup import YTEnvSetup
from yt_commands import *
from yt.yson import to_yson_type


##################################################################

class TestCypress(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 0

    def test_root(self):
        # should not crash
        get('//@')

    def test_invalid_cases(self):
        # path not starting with /
        with pytest.raises(YtError): set('a', 20)

        # path starting with single /
        with pytest.raises(YtError): set('/a', 20)

        # empty path
        with pytest.raises(YtError): set('', 20)

        # empty token in path
        with pytest.raises(YtError): set('//tmp/a//b', 20)

        # change the type of root
        with pytest.raises(YtError): set('/', [])

        # set the root to the empty map
        # with pytest.raises(YtError): set('/', {}))

        # remove the root
        with pytest.raises(YtError): remove('/')
        # get non existent child
        with pytest.raises(YtError): get('//tmp/b')

        # remove non existent child
        with pytest.raises(YtError): remove('//tmp/b')

        # can't create entity node inside cypress
        with pytest.raises(YtError): set('//tmp/entity', None)

    def test_remove(self):
        with pytest.raises(YtError): remove('//tmp/x', recursive=False)
        with pytest.raises(YtError): remove('//tmp/x')
        remove('//tmp/x', force=True)

        with pytest.raises(YtError): remove('//tmp/1', recursive=False)
        with pytest.raises(YtError): remove('//tmp/1')
        remove('//tmp/1', force=True)

        create("map_node", "//tmp/x/1/y", recursive=True)
        with pytest.raises(YtError): remove('//tmp/x', recursive=False)
        with pytest.raises(YtError): remove('//tmp/x', recursive=False, force=True)
        remove('//tmp/x/1/y', recursive=False)
        remove('//tmp/x')

    def test_list(self):
        set('//tmp/list', [1,2,"some string"])
        assert get('//tmp/list') == [1,2,"some string"]

        set('//tmp/list/end', 100)
        assert get('//tmp/list') == [1,2,"some string",100]

        set('//tmp/list/before:0', 200)
        assert get('//tmp/list') == [200,1,2,"some string",100]

        set('//tmp/list/before:0', 500)
        assert get('//tmp/list') == [500,200,1,2,"some string",100]

        set('//tmp/list/after:2', 1000)
        assert get('//tmp/list') == [500,200,1,1000,2,"some string",100]

        set('//tmp/list/3', 777)
        assert get('//tmp/list') == [500,200,1,777,2,"some string",100]

        remove('//tmp/list/4')
        assert get('//tmp/list') == [500,200,1,777,"some string",100]

        remove('//tmp/list/4')
        assert get('//tmp/list') == [500,200,1,777,100]

        remove('//tmp/list/0')
        assert get('//tmp/list') == [200,1,777,100]

        set('//tmp/list/end', 'last')
        assert get('//tmp/list') == [200,1,777,100,"last"]

        set('//tmp/list/before:0', 'first')
        assert get('//tmp/list') == ["first",200,1,777,100,"last"]

        set('//tmp/list/begin', 'very_first')
        assert get('//tmp/list') == ["very_first","first",200,1,777,100,"last"]

    def test_list_command(self):
        def list(path, **kwargs):
            kwargs["path"] = path
            return yson.loads(command('list', kwargs))

        set('//tmp/map', {"a": 1, "b": 2, "c": 3})
        assert list('//tmp/map') == ["a", "b", "c"]

        set('//tmp/map', {"a": 1, "a": 2})
        assert list('//tmp/map', max_size=1) == ["a"]

        list("//sys/chunks")
        list("//sys/accounts")
        list("//sys/transactions")

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

        set('//tmp/map/list/end', {})
        set('//tmp/map/list/0/a', 1)
        assert get('//tmp/map') == {"hello":"not_world","list":[{"a":1}]}

        set('//tmp/map/list/begin', {})
        set('//tmp/map/list/0/b', 2)
        assert get('//tmp/map') == {"hello":"not_world","list":[{"b":2},{"a":1}]}

        remove('//tmp/map/hello')
        assert get('//tmp/map') == {"list":[{"b":2},{"a":1}]}

        remove('//tmp/map/list')
        assert get('//tmp/map') == {}

    def test_attributes(self):
        set('//tmp/t', '<attr=100;mode=rw> {nodes=[1; 2]}', is_raw=True)
        assert get('//tmp/t/@attr') == 100
        assert get('//tmp/t/@mode') == "rw"

        remove('//tmp/t/@*')
        with pytest.raises(YtError): get('//tmp/t/@attr')
        with pytest.raises(YtError): get('//tmp/t/@mode')

        # changing attributes
        set('//tmp/t/a', '<author = ignat> []', is_raw=True)
        assert get('//tmp/t/a') == []
        assert get('//tmp/t/a/@author') == "ignat"

        set('//tmp/t/a/@author', "not_ignat")
        assert get('//tmp/t/a/@author') == "not_ignat"

        # nested attributes (actually shows <>)
        set('//tmp/t/b', '<dir = <file = <>-100> #> []', is_raw=True)
        assert get('//tmp/t/b/@dir/@') == {"file": -100}
        assert get('//tmp/t/b/@dir/@file') == -100
        assert get('//tmp/t/b/@dir/@file/@') == {}

        # set attributes directly
        set('//tmp/t/@', {'key1': 'value1', 'key2': 'value2'})
        assert get('//tmp/t/@key1') == "value1"
        assert get('//tmp/t/@key2') == "value2"

        # error cases
        # typo (extra slash)
        with pytest.raises(YtError): get('//tmp/t/@/key1')
        # change type
        with pytest.raises(YtError): set('//tmp/t/@', 1)
        with pytest.raises(YtError): set('//tmp/t/@', 'a')
        with pytest.raises(YtError): set('//tmp/t/@', [])
        with pytest.raises(YtError): set('//tmp/t/@', [1, 2, 3])

    def test_attributes_tx_read(self):
        set('//tmp/t', '<attr=100> 123', is_raw=True)
        assert get('//tmp/t') == 123
        assert get('//tmp/t/@attr') == 100
        assert 'attr' in get('//tmp/t/@')

        tx = start_transaction()
        assert get('//tmp/t/@attr', tx = tx) == 100
        assert 'attr' in get('//tmp/t/@', tx = tx)

    def test_format_json(self):
        # check input format for json
        set('//tmp/json_in', '{"list": [1,2,{"string": "this"}]}', is_raw=True, input_format="json")
        assert get('//tmp/json_in') == {"list": [1, 2, {"string": "this"}]}

        # check output format for json
        set('//tmp/json_out', {'list': [1, 2, {'string': 'this'}]})
        assert get('//tmp/json_out', is_raw=True, output_format="json") == '{"list":[1,2,{"string":"this"}]}'

    def test_map_remove_all1(self):
        # remove items from map
        set('//tmp/map', {"a" : "b", "c": "d"})
        assert get('//tmp/map/@count') == 2
        remove('//tmp/map/*')
        assert get('//tmp/map') == {}
        assert get('//tmp/map/@count') == 0

    def test_map_remove_all2(self):
        set('//tmp/map', {'a' : 1})
        tx = start_transaction()
        set('//tmp/map', {'b' : 2}, tx = tx)
        remove('//tmp/map/*', tx = tx)
        assert get('//tmp/map', tx = tx) == {}
        assert get('//tmp/map/@count', tx = tx) == 0
        commit_transaction(tx)
        assert get('//tmp/map') == {}
        assert get('//tmp/map/@count') == 0

    def test_list_remove_all(self):
        # remove items from list
        set('//tmp/list', [10, 20, 30])
        assert get('//tmp/list/@count') == 3
        remove('//tmp/list/*')
        assert get('//tmp/list') == []
        assert get('//tmp/list/@count') == 0

    def test_attr_remove_all1(self):
        # remove items from attributes
        set('//tmp/attr', '<_foo=bar;_key=value>42', is_raw=True);
        remove('//tmp/attr/@*')
        with pytest.raises(YtError): get('//tmp/attr/@_foo')
        with pytest.raises(YtError): get('//tmp/attr/@_key')

    def test_attr_remove_all2(self):
        set('//tmp/@a', 1)
        tx = start_transaction()
        set('//tmp/@b', 2, tx = tx)
        remove('//tmp/@*', tx = tx)
        with pytest.raises(YtError): get('//tmp/@a', tx = tx)
        with pytest.raises(YtError): get('//tmp/@b', tx = tx)
        commit_transaction(tx)
        with pytest.raises(YtError): get('//tmp/@a')
        with pytest.raises(YtError): get('//tmp/@b')

    def test_copy_simple1(self):
        set('//tmp/a', 1)
        copy('//tmp/a', '//tmp/b')
        assert get('//tmp/b') == 1

    def test_copy_simple2(self):
        set('//tmp/a', [1, 2, 3])
        copy('//tmp/a', '//tmp/b')
        assert get('//tmp/b') == [1, 2, 3]

    def test_copy_simple3(self):
        set('//tmp/a', '<x=y> 1', is_raw=True)
        copy('//tmp/a', '//tmp/b')
        assert get('//tmp/b/@x') == 'y'

    def test_copy_simple4(self):
        set('//tmp/a', {'x1' : 'y1', 'x2' : 'y2'})
        assert get('//tmp/a/@count') == 2

        copy('//tmp/a', '//tmp/b')
        assert get('//tmp/b/@count') == 2

    def test_copy_simple5(self):
        set("//tmp/a", { 'b' : 1 })
        assert get('//tmp/a/b/@path') == '//tmp/a/b'

        copy('//tmp/a', '//tmp/c')
        assert get('//tmp/c/b/@path') == '//tmp/c/b'

        remove('//tmp/a')
        assert get('//tmp/c/b/@path') == '//tmp/c/b'

    def test_copy_simple6(self):
        with pytest.raises(YtError): copy('//tmp', '//tmp/a')

    def test_copy_simple7(self):
        tx = start_transaction()
        with pytest.raises(YtError): copy('#' + tx, '//tmp/t')

    def test_copy_simple8(self):
        create('map_node', '//tmp/a')
        create('table', '//tmp/a/t')
        link('//tmp/a', '//tmp/b')
        copy('//tmp/b/t', '//tmp/t')

    def test_copy_tx1(self):
        tx = start_transaction()

        set('//tmp/a', {'x1' : 'y1', 'x2' : 'y2'}, tx=tx)
        assert get('//tmp/a/@count', tx=tx) == 2

        copy('//tmp/a', '//tmp/b', tx=tx)
        assert get('//tmp/b/@count', tx=tx) == 2

        commit_transaction(tx)

        assert get('//tmp/a/@count') == 2
        assert get('//tmp/b/@count') == 2

    def test_copy_tx2(self):
        set('//tmp/a', {'x1' : 'y1', 'x2' : 'y2'})

        tx = start_transaction()

        remove('//tmp/a/x1', tx=tx)
        assert get('//tmp/a/@count', tx=tx) == 1

        copy('//tmp/a', '//tmp/b', tx=tx)
        assert get('//tmp/b/@count', tx=tx) == 1

        commit_transaction(tx)

        assert get('//tmp/a/@count') == 1
        assert get('//tmp/b/@count') == 1

    def test_copy_account1(self):
        create_account('a1')
        create_account('a2')

        set('//tmp/a1', {})
        set('//tmp/a1/@account', 'a1')
        set('//tmp/a2', {})
        set('//tmp/a2/@account', 'a2')

        set('//tmp/a1/x', {'y' : 'z'})
        copy('//tmp/a1/x', '//tmp/a2/x')

        assert get('//tmp/a2/@account') == 'a2'
        assert get('//tmp/a2/x/@account') == 'a2'
        assert get('//tmp/a2/x/y/@account') == 'a2'

    def test_copy_account2(self):
        create_account('a1')
        create_account('a2')

        set('//tmp/a1', {})
        set('//tmp/a1/@account', 'a1')
        set('//tmp/a2', {})
        set('//tmp/a2/@account', 'a2')

        set('//tmp/a1/x', {'y' : 'z'})
        copy('//tmp/a1/x', '//tmp/a2/x', opt=['/preserve_account=true'])

        assert get('//tmp/a2/@account') == 'a2'
        assert get('//tmp/a2/x/@account') == 'a1'
        assert get('//tmp/a2/x/y/@account') == 'a1'

    def test_copy_unexisting_path(self):
        with pytest.raises(YtError): copy('//tmp/x', '//tmp/y')

    def test_copy_cannot_have_children(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        with pytest.raises(YtError): copy('//tmp/t2', '//tmp/t1/xxx')

    def test_copy_table_compression_codec(self):
        create('table', '//tmp/t1')
        assert get('//tmp/t1/@compression_codec') == 'lz4'
        set('//tmp/t1/@compression_codec', 'gzip_normal')
        copy('//tmp/t1', '//tmp/t2')
        assert get('//tmp/t2/@compression_codec') == 'gzip_normal'

    def test_copy_id1(self):
        set('//tmp/a', 123)
        a_id = get('//tmp/a/@id')
        copy('#' + a_id, '//tmp/b')
        assert get('//tmp/b') == 123

    def test_copy_id2(self):
        set('//tmp/a', 123)
        tmp_id = get('//tmp/@id')
        copy('#' + tmp_id + '/a', '//tmp/b')
        assert get('//tmp/b') == 123

    def test_copy_preserve_account1(self):
        create_account('max')
        create('table', '//tmp/t1')
        set('//tmp/t1/@account', 'max')
        copy('//tmp/t1', '//tmp/t2') # preserve is OFF
        assert get('//tmp/t2/@account') == 'tmp'

    def test_copy_preserve_account2(self):
        create_account('max')
        create('table', '//tmp/t1')
        set('//tmp/t1/@account', 'max')
        copy('//tmp/t1', '//tmp/t2', opt=['/preserve_account=true']) # preserve is ON
        assert get('//tmp/t2/@account') == 'max'

    def test_move_simple1(self):
        set('//tmp/a', 1)
        move('//tmp/a', '//tmp/b')
        assert get('//tmp/b') == 1
        with pytest.raises(YtError): get('//tmp/a')

    def test_move_simple2(self):
        set('//tmp/a', 1)

        tx = start_transaction()
        lock('//tmp/a', tx=tx)

        with pytest.raises(YtError): move('//tmp/a', '//tmp/b')
        assert not exists('//tmp/b')

    def test_move_simple3(self):
        with pytest.raises(YtError): move('//tmp', '//tmp/a')

    def test_move_preserve_account1(self):
        create_account('max')
        create('table', '//tmp/t1')
        set('//tmp/t1/@account', 'max')
        move('//tmp/t1', '//tmp/t2', preserve_account=False) # preserve is OFF
        assert get('//tmp/t2/@account') == 'tmp'

    def test_move_preserve_account2(self):
        create_account('max')
        create('table', '//tmp/t1')
        set('//tmp/t1/@account', 'max')
        move('//tmp/t1', '//tmp/t2') # preserve is ON
        assert get('//tmp/t2/@account') == 'max'

    def test_embedded_attributes(self):
        set("//tmp/a", {})
        set("//tmp/a/@attr", {"key": "value"})
        set("//tmp/a/@attr/key/@embedded_attr", "emb")
        assert get("//tmp/a/@attr") == {"key": to_yson_type("value", attributes={"embedded_attr": "emb"})}
        assert get("//tmp/a/@attr/key") == to_yson_type("value", attributes={"embedded_attr": "emb"})
        assert get("//tmp/a/@attr/key/@embedded_attr") == "emb"

    def test_get_with_attributes(self):
        set('//tmp/a', {})
        assert get('//tmp', attr=['type']) == to_yson_type({"a": to_yson_type({}, {"type": "map_node"})}, {"type": "map_node"})

    def test_list_with_attributes(self):
        set('//tmp/a', {})
        assert ls('//tmp', attr=['type']) == [to_yson_type("a", attributes={"type": "map_node"})]

    def test_get_list_with_attributes_virtual_maps(self):
        tx = start_transaction()

        assert get('//sys/transactions', attr=['type']) == {tx: to_yson_type(None, attributes={"type": "transaction"})}
        assert ls('//sys/transactions', attr=['type']) == [to_yson_type(tx, attributes={"type": "transaction"})]

        abort_transaction(tx)

    def test_exists(self):
        assert exists("//tmp")
        assert not exists("//tmp/a")
        assert not exists("//tmp/a/f/e")
        assert not exists("//tmp/a/1/e")
        assert not exists("//tmp/a/2/1")

        set("//tmp/1", {})
        assert exists("//tmp/1")
        assert not exists("//tmp/1/2")

        set("//tmp/a", {})
        assert exists("//tmp/a")

        set("//tmp/a/@list", [10])
        assert exists("//tmp/a/@list")
        assert exists("//tmp/a/@list/0")
        assert not exists("//tmp/a/@list/1")

        assert not exists("//tmp/a/@attr")
        set("//tmp/a/@attr", {"key": "value"})
        assert exists("//tmp/a/@attr")

        assert exists("//sys/operations")
        assert exists("//sys/transactions")
        assert not exists("//sys/xxx")
        assert not exists("//sys/operations/xxx")

    def test_remove_tx1(self):
        set('//tmp/a', 1)
        assert get('//tmp/@id') == get('//tmp/a/@parent_id')
        tx = start_transaction()
        remove('//tmp/a', tx=tx)
        assert get('//tmp/@id') == get('//tmp/a/@parent_id')
        abort_transaction(tx)
        assert get('//tmp/@id') == get('//tmp/a/@parent_id')

    def test_create(self):
        remove("//tmp/*")
        create("map_node", "//tmp/some_node")

        with pytest.raises(YtError): create("map_node", "//tmp/a/b")
        create("map_node", "//tmp/a/b", recursive=True)

        with pytest.raises(YtError): create("map_node", "//tmp/a/b")
        create("map_node", "//tmp/a/b", ignore_existing=True)

        with pytest.raises(YtError): create("table", "//tmp/a/b", ignore_existing=True)

    def test_link1(self):
        with pytest.raises(YtError): link("//tmp/a", "//tmp/b")

    def test_link2(self):
        set("//tmp/t1", 1)
        link("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t2") == 1
        assert get("//tmp/t2/@type") == "int64_node"
        assert get("//tmp/t1/@id") == get("//tmp/t2/@id")
        assert get("//tmp/t2&/@type") == "link"
        assert not get("//tmp/t2&/@broken")

        set("//tmp/t1", 2)
        assert get("//tmp/t2") == 2

    def test_link3(self):
        set("//tmp/t1", 1)
        link("//tmp/t1", "//tmp/t2")
        remove("//tmp/t1")
        assert get("//tmp/t2&/@broken")

    def test_link4(self):
        set("//tmp/t1", 1)
        link("//tmp/t1", "//tmp/t2")

        tx = start_transaction()
        id = get("//tmp/t1/@id")
        lock("#%s" % id, mode = "snapshot", tx = tx)

        remove("//tmp/t1")

        assert get("#%s" % id, tx = tx) == 1
        assert get("//tmp/t2&/@broken")
        with pytest.raises(YtError): read("//tmp/t2")

    def test_link5(self):
        set("//tmp/t1", 1)
        set("//tmp/t2", 2)
        with pytest.raises(YtError): link("//tmp/t1", "//tmp/t2")

    def test_link6(self):
        create("table", "//tmp/a")
        link("//tmp/a", "//tmp/b")

        assert exists("//tmp/a")
        assert exists("//tmp/b")
        assert exists("//tmp/b&")
        assert exists("//tmp/b/@id")
        assert exists("//tmp/b/@row_count")
        assert exists("//tmp/b&/@target_id")
        assert not exists("//tmp/b/@x")
        assert not exists("//tmp/b/x")
        assert not exists("//tmp/b&/@x")
        assert not exists("//tmp/b&/x")

        remove("//tmp/a")

        assert not exists("//tmp/a")
        assert not exists("//tmp/b")
        assert exists("//tmp/b&")
        assert not exists("//tmp/b/@id")
        assert not exists("//tmp/b/@row_count")
        assert exists("//tmp/b&/@target_id")
        assert not exists("//tmp/b/@x")
        assert not exists("//tmp/b/x")
        assert not exists("//tmp/b&/@x")
        assert not exists("//tmp/b&/x")

    def test_link7(self):
        tx = start_transaction()
        set("//tmp/t1", 1, tx=tx)
        link("//tmp/t1", "//tmp/l1", tx=tx)
        assert get("//tmp/l1", tx=tx) == 1

    def test_access_stat1(self):
        time.sleep(1.0)
        c1 = get('//tmp/@access_counter')
        time.sleep(1.0)
        c2 = get('//tmp/@access_counter')
        assert c2 == c1

    def test_access_stat2(self):
        time.sleep(1.0)
        c1 = get('//tmp/@access_counter')
        tx = start_transaction()
        lock('//tmp', mode = 'snapshot', tx = tx)
        time.sleep(1.0)
        c2 = get('//tmp/@access_counter', tx = tx)
        assert c2 == c1 + 1

    def test_access_stat3(self):
        time.sleep(1.0)
        c1 = get('//tmp/@access_counter')
        get('//tmp/@')
        time.sleep(1.0)
        c2 = get('//tmp/@access_counter')
        assert c1 == c2

    def test_access_stat4(self):
        time.sleep(1.0)
        c1 = get('//tmp/@access_counter')
        assert exists('//tmp')
        time.sleep(1.0)
        c2 = get('//tmp/@access_counter')
        assert c1 == c2

    def test_access_stat5(self):
        time.sleep(1.0)
        c1 = get('//tmp/@access_counter')
        assert exists('//tmp/@id')
        time.sleep(1.0)
        c2 = get('//tmp/@access_counter')
        assert c1 == c2

    def test_access_stat6(self):
        time.sleep(1.0)
        c1 = get('//tmp/@access_counter')
        ls('//tmp/@')
        time.sleep(1.0)
        c2 = get('//tmp/@access_counter')
        assert c1 == c2

    def test_access_stat7(self):
        time.sleep(1.0)
        c1 = get('//tmp/@access_counter')
        ls('//tmp')
        time.sleep(1.0)
        c2 = get('//tmp/@access_counter')
        assert c2 == c1 + 1

    def test_access_stat8(self):
        create('table', '//tmp/t')
        assert get('//tmp/t/@access_time') == get('//tmp/t/@creation_time')

    def test_access_stat9(self):
        create('table', '//tmp/t1')
        copy('//tmp/t1', '//tmp/t2')
        assert get('//tmp/t2/@access_time') == get('//tmp/t2/@creation_time')

    def test_access_stat_suppress1(self):
        time.sleep(1.0)
        c1 = get('//tmp/@access_counter')
        get('//tmp', opt=['/suppress_access_tracking=true'])
        time.sleep(1.0)
        c2 = get('//tmp/@access_counter')
        assert c1 == c2

    def test_access_stat_suppress2(self):
        time.sleep(1.0)
        c1 = get('//tmp/@access_counter')
        ls('//tmp', opt=['/suppress_access_tracking=true'])
        time.sleep(1.0)
        c2 = get('//tmp/@access_counter')
        assert c1 == c2

    def test_access_stat_suppress3(self):
        time.sleep(1.0)
        create('table', '//tmp/t')
        c1 = get('//tmp/t/@access_counter')
        read('//tmp/t', opt=['/suppress_access_tracking=true'])
        time.sleep(1.0)
        c2 = get('//tmp/t/@access_counter')
        assert c1 == c2

    def test_access_stat_suppress4(self):
        time.sleep(1.0)
        create('file', '//tmp/f')
        c1 = get('//tmp/f/@access_counter')
        download('//tmp/f', opt=['/suppress_access_tracking=true'])
        time.sleep(1.0)
        c2 = get('//tmp/f/@access_counter')
        assert c1 == c2

    def test_chunk_maps(self):
        gc_collect()
        assert get('//sys/chunks/@count') == 0
        assert get('//sys/underreplicated_chunks/@count') == 0
        assert get('//sys/overreplicated_chunks/@count') == 0

    def test_list_attributes(self):
        create('map_node', '//tmp/map', attributes={'user_attr1': 10})
        set('//tmp/map/@user_attr2', 'abc')
        assert sorted(get('//tmp/map/@user_attribute_keys')) == sorted(['user_attr1', 'user_attr2'])

        create('table', '//tmp/table')
        assert get('//tmp/table/@user_attribute_keys') == []

        create('file', '//tmp/file')
        assert get('//tmp/file/@user_attribute_keys') == []

    def test_boolean(self):
        yson_format = yson.loads("<boolean_as_string=false>yson")
        set("//tmp/boolean", "%true", is_raw=True)
        assert get("//tmp/boolean/@type") == "boolean_node"
        assert get("//tmp/boolean", output_format=yson_format)

    def test_uint64(self):
        yson_format = yson.loads("yson")
        set("//tmp/my_uint", "123456u", is_raw=True)
        assert get("//tmp/my_uint/@type") == "uint64_node"
        assert get("//tmp/my_uint", output_format=yson_format) == 123456
