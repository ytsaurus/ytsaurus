import sys
import time
import yt_client
from mapreduce.yt.python.yt_stuff import yt_stuff
from yt.yson.yson_types import YsonInt64, YsonUint64, YsonString, YsonUnicode, YsonMap, YsonList
from yt.yson.convert import to_yson_type


def test_get(yt_stuff):
    client = yt_client.Client('localhost:{}'.format(yt_stuff.yt_proxy_port))
    yt = yt_stuff.get_yt_client()
    yt.create('map_node', '//test_get', ignore_existing=True)

    # test get string
    yt.create('string_node', '//test_get/string_node')
    yt.set('//test_get/string_node', 'ABCDEFG')
    assert client.get('//test_get/string_node') == 'ABCDEFG'

    # test get int64
    yt.create('int64_node', '//test_get/int64')
    yt.set('//test_get/int64', 13)
    assert client.get('//test_get/int64') == 13

    # test get uint64
    yt.create('uint64_node', '//test_get/uint64')
    yt.set('//test_get/uint64', 42)
    assert client.get('//test_get/uint64') == 42
    yt.set('//test_get/uint64', 2**64 - 1)
    assert client.get('//test_get/uint64') == 2**64 - 1

    # test get list
    l = [1, 2, 'a', 'b', ['x', 'z'], {'a': 1, 'b': 2}]
    yt.create('list_node', '//test_get/list')
    yt.set('//test_get/list', l)
    assert client.get('//test_get/list') == l

    # test get table attrs
    yt.create('table', '//test_get/table')
    yt.set('//test_get/table/@attr1', 'string_attr')
    yt.set('//test_get/table/@attr2', ['a', 'b'])
    yt.set('//test_get/table/@attr3', {'a': 1, 'b': 2})
    assert client.get('//test_get/table/@attr1') == 'string_attr'
    assert client.get('//test_get/table/@attr2') == ['a', 'b']
    assert client.get('//test_get/table/@attr3') == {'a': 1, 'b': 2}
    all_attrs = client.get('//test_get/table/@')
    assert 'attr1' in all_attrs
    assert 'attr2' in all_attrs
    assert 'attr3' in all_attrs
    my_attrs = client.get('//test_get/table/@', attributes=['attr1', 'attr3'])
    assert 'attr1' in my_attrs
    assert 'attr3' in my_attrs
    assert len(my_attrs) == 2

    # test get non-existing
    try:
        client.get('//test_get/XXX')
        assert False
    except Exception as e:
        assert 'Node //test_get has no child with key "XXX"' in str(e)

    try:
        client.get('//test_get/table/@attr100')
        assert False
    except Exception as e:
        assert 'Attribute "attr100" is not found' in str(e)

    # TODO: test max_size parameter(does it work??)


def test_set(yt_stuff):
    client = yt_client.Client('localhost:{}'.format(yt_stuff.yt_proxy_port))
    yt = yt_stuff.get_yt_client()
    yt.create('map_node', '//test_set', ignore_existing=True)

    yt.create('string_node', '//test_set/string')
    client.set('//test_set/string', 'XXX')
    assert yt.get('//test_set/string') == 'XXX'
    client.set('//test_set/string', 'ZZZ')
    assert yt.get('//test_set/string') == 'ZZZ'
    client.set('//test_set/string/@xxx', {'a': 1, 'b': 2})
    assert yt.get('//test_set/string/@xxx') == {'a': 1, 'b': 2}
    client.set('//test_set/string/@yyy', ['a', 2, 'c'])
    assert yt.get('//test_set/string/@yyy') == ['a', 2, 'c']
    client.set('//test_set/string/@ui64', 2**64 - 1)
    assert yt.get('//test_set/string/@ui64') == 2**64 - 1


def test_get_set_yson(yt_stuff):
    client = yt_client.Client('localhost:{}'.format(yt_stuff.yt_proxy_port))
    yt = yt_stuff.get_yt_client()
    test_root = '//test_get_set_yson'
    attrs = {'attr_key': 'attr_value'}
    yt.create('map_node', test_root, ignore_existing=True)

    # test entity (test via attributes because there is no entity type node in the YT cypress)
    entity_node = test_root + '/entity_node'
    val_with_entity_attr = to_yson_type('dummy', {"k" : to_yson_type(None)})
    yt.create('string_node', entity_node)
    client.set(entity_node, val_with_entity_attr)
    assert client.get(entity_node, attributes=['k'], yson=True) == val_with_entity_attr

    # test string
    string_node = test_root + '/string_node'
    string_val = to_yson_type('ABCDEFG', attrs)
    yt.create('string_node', string_node)
    client.set(string_node, string_val)
    assert client.get(string_node, attributes=['attr_key'], yson=True) == string_val

    # test int64
    int64_node = test_root + '/int64_node'
    int64_val = to_yson_type(13, attrs)
    yt.create('int64_node', int64_node)
    client.set(int64_node, int64_val)
    assert client.get(int64_node, attributes=['attr_key'], yson=True) == int64_val

    # test uint64
    uint64_node = test_root + '/uint64_node'
    uint64_val = YsonUint64(13)
    uint64_val.attributes = attrs
    yt.create('uint64_node', uint64_node)
    client.set(uint64_node, uint64_val)
    assert client.get(uint64_node, attributes=['attr_key'], yson=True) == uint64_val

    # test list
    list_node = test_root + '/list_node'
    list_val = to_yson_type([1, 2, 'a', 'b', ['x', 'z'], {'a': 1, 'b': 2}], attrs)
    yt.create('list_node', list_node)
    client.set(list_node, list_val)
    assert client.get(list_node, attributes=['attr_key'], yson=True) == list_val


def test_exists(yt_stuff):
    client = yt_client.Client('localhost:{}'.format(yt_stuff.yt_proxy_port))
    yt = yt_stuff.get_yt_client()
    yt.create('map_node', '//test_exists', ignore_existing=True)

    yt.create('table', '//test_exists/table')
    yt.create('string_node', '//test_exists/string')

    assert client.exists('//test_exists')
    assert client.exists('//test_exists/table')
    assert client.exists('//test_exists/string')
    assert not client.exists('//test_exists/tablexxx')
    assert not client.exists('//test_exists/string/x')
    assert not client.exists('//test_exists/asdasd')


def test_create(yt_stuff):
    client = yt_client.Client('localhost:{}'.format(yt_stuff.yt_proxy_port))
    yt = yt_stuff.get_yt_client()
    yt.create('map_node', '//test_create', ignore_existing=True)

    # test create base node types
    types = [
        'string_node',
        'int64_node',
        'uint64_node',
        'double_node',
        'boolean_node',
        'map_node',
        'list_node',
        'file',
        'table',
        'document',
    ]

    for _type in types:
        client.create(_type, '//test_create/{}'.format(_type))
        assert yt.exists('//test_create/{}'.format(_type))
        assert yt.get('//test_create/{}/@type'.format(_type)) == _type

    # test recursive parameter
    try:
        client.create('string_node', '//test_create/a/b/c/s')
        assert False
    except Exception:
        pass
    client.create('string_node', '//test_create/a/b/c/s', recursive=True)
    assert yt.exists('//test_create/a/b/c/s')

    # test ignore_existing parameter
    _id = client.create('string_node', '//test_create/existing')
    try:
        client.create('string_node', '//test_create/existing')
        assert False
    except Exception:
        pass
    assert _id == client.create('string_node', '//test_create/existing', ignore_existing=True)
    try:
        client.create('table', '//test_create/existing', ignore_existing=True)
        assert False
    except Exception:
        pass

    # test force parameter
    _id = client.create('string_node', '//test_create/force')
    assert _id != client.create('table', '//test_create/force', force=True)

    # test attributes parameter
    client.create(
        'file',
        '//test_create/file_with_attrs',
        attributes={'attr1': 1, 'attr2': 'abc', 'attr3': ['a', 1], 'attr4': {'a': 1, 'b': 2}}
    )
    assert yt.get('//test_create/file_with_attrs/@attr1') == 1
    assert yt.get('//test_create/file_with_attrs/@attr2') == 'abc'
    assert yt.get('//test_create/file_with_attrs/@attr3') == ['a', 1]
    assert yt.get('//test_create/file_with_attrs/@attr4') == {'a': 1, 'b': 2}


def wait_true(f, timeout):
    start = time.time()
    while not f():
        assert time.time() - start < timeout
        time.sleep(0.1)


def retry(f, n):
    for i in xrange(n):
        try:
            return f()
        except Exception:
            if i + 1 == n:
                raise
            time.sleep(0.5)
            continue


def test_mount_unmount(yt_stuff):
    client = yt_client.Client('localhost:{}'.format(yt_stuff.yt_proxy_port))
    yt = yt_stuff.get_yt_client()
    yt.create('map_node', '//test_mount', ignore_existing=True)

    client.create_table(
        '//test_mount/table',
        attributes={
            'dynamic': 'true',
            'schema': [
                {'name': 'hash', 'type': 'uint64', 'sort_order': 'ascending', 'expression': 'farm_hash(key) % 128'},
                {'name': 'key', 'type': 'string', 'sort_order': 'ascending'},
                {'name': 'value', 'type': 'string'},
            ],
            'pivot_keys': [
                [],
                [yt_client.node_ui64(32)],
                [yt_client.node_ui64(64)],
                [yt_client.node_ui64(96)],
                [yt_client.node_ui64(128)]
            ],
            'disable_tablet_balancer': 'true',
        }
    )

    # test mount/unmount
    retry(lambda: client.mount_table('//test_mount/table'), 10)
    wait_true(lambda: all(t['state'] == 'mounted' for t in client.get('//test_mount/table/@tablets')), 15)
    retry(lambda: client.unmount_table('//test_mount/table'), 10)
    wait_true(lambda: all(t['state'] == 'unmounted' for t in client.get('//test_mount/table/@tablets')), 15)

    # TODO: test first_tablet_index, last_tablet_index, cell_id, freeze


def test_insert(yt_stuff):
    client = yt_client.Client('localhost:{}'.format(yt_stuff.yt_proxy_port))
    yt = yt_stuff.get_yt_client()
    yt.create('map_node', '//test_insert', ignore_existing=True)

    client.create_table(
        '//test_insert/table',
        attributes={
            'dynamic': 'true',
            'schema': [
                {'name': 'hash', 'type': 'uint64', 'sort_order': 'ascending', 'expression': 'farm_hash(key) % 128'},
                {'name': 'key', 'type': 'string', 'sort_order': 'ascending'},
                {'name': 'value', 'type': 'string'},
                {'name': 'count', 'type': 'uint64', 'aggregate': 'sum'}
            ],
            'pivot_keys': [
                [],
                [yt_client.node_ui64(32)],
                [yt_client.node_ui64(64)],
                [yt_client.node_ui64(96)],
                [yt_client.node_ui64(128)]
            ],
            'disable_tablet_balancer': 'true',
        }
    )
    retry(lambda: client.mount_table('//test_insert/table'), 10)
    wait_true(lambda: all(t['state'] == 'mounted' for t in client.get('//test_insert/table/@tablets')), 15)

    # test insert
    client.insert_rows(
        '//test_insert/table',
        [
            {'key': 'a', 'value': '1'},
            {'key': 'b', 'value': '2'},
        ]
    )
    rows = client.select_rows('key, value from [//test_insert/table] where key="a"')
    assert len(rows) == 1
    assert rows[0] == {'key': 'a', 'value': '1'}

    # test update
    client.insert_rows('//test_insert/table', [{'key': 'a', 'count': 0}])
    client.insert_rows('//test_insert/table', [{'key': 'b', 'count': 0}], update=True)

    rows = client.select_rows('key, value from [//test_insert/table] where key="a" or key="b"')
    assert len(rows) == 2
    kv = {row['key']: row['value'] for row in rows}
    assert kv['a'] is None
    assert kv['b'] == '2'

    # test aggregate
    client.insert_rows('//test_insert/table', [{'key': 'x', 'value': 'y', 'count': yt_client.node_ui64(1)}])
    rows = client.select_rows('key, value, count from [//test_insert/table] where key="x"')
    assert len(rows) == 1
    assert rows[0]['count'] == 1
    client.insert_rows('//test_insert/table', [{'key': 'x', 'value': 'y', 'count': yt_client.node_ui64(1)}], aggregate=True)
    rows = client.select_rows('key, value, count from [//test_insert/table] where key="x"')
    assert len(rows) == 1
    assert rows[0]['count'] == 2


def test_lookup(yt_stuff):
    client = yt_client.Client('localhost:{}'.format(yt_stuff.yt_proxy_port))
    yt = yt_stuff.get_yt_client()
    yt.create('map_node', '//test_lookup', ignore_existing=True)

    client.create_table(
        '//test_lookup/table',
        attributes={
            'dynamic': 'true',
            'schema': [
                {'name': 'hash', 'type': 'uint64', 'sort_order': 'ascending', 'expression': 'farm_hash(key) % 128'},
                {'name': 'key', 'type': 'string', 'sort_order': 'ascending'},
                {'name': 'value', 'type': 'string'},
            ],
            'pivot_keys': [
                [],
                [yt_client.node_ui64(32)],
                [yt_client.node_ui64(64)],
                [yt_client.node_ui64(96)],
                [yt_client.node_ui64(128)]
            ],
            'disable_tablet_balancer': 'true',
        }
    )

    retry(lambda: client.mount_table('//test_lookup/table'), 10)
    wait_true(lambda: all(t['state'] == 'mounted' for t in client.get('//test_lookup/table/@tablets')), 15)

    client.insert_rows('//test_lookup/table', [{'key': 'a', 'value': 'x'}, {'key': 'b', 'value': 'y'}])
    rows = client.lookup_rows('//test_lookup/table', [{'key': 'a'}])
    assert len(rows) == 1
    assert len(rows[0]) == 3
    assert rows[0]['key'] == 'a'
    assert rows[0]['value'] == 'x'

    rows = client.lookup_rows('//test_lookup/table', [{'key': 'a'}], columns=['key', 'value'])
    assert rows == [{'key': 'a', 'value': 'x'}]

    rows = client.lookup_rows('//test_lookup/table', [{'key': 'a'}, {'key': 'b'}], columns=['key'])
    assert rows == [{'key': 'a'}, {'key': 'b'}]

    rows = client.lookup_rows('//test_lookup/table', [{'key': 'c'}])
    assert len(rows) == 0

    rows = client.lookup_rows('//test_lookup/table', [{'key': 'c'}, {'key': 'a'}], columns=['value'], keep_missing_rows=True)
    assert rows == [None, {'value': 'x'}]


def test_lookup_versioned(yt_stuff):
    client = yt_client.Client('localhost:{}'.format(yt_stuff.yt_proxy_port))
    yt = yt_stuff.get_yt_client()
    yt.create('map_node', '//test_lookup_yson', ignore_existing=True)

    test_table = '//test_lookup_yson/table'
    client.create_table(
        test_table,
        attributes={
            'dynamic': 'true',
            'schema': [
                {'name': 'hash', 'type': 'uint64', 'sort_order': 'ascending', 'expression': 'farm_hash(key) % 128'},
                {'name': 'key', 'type': 'string', 'sort_order': 'ascending'},
                {'name': 'value', 'type': 'string'},
            ],
            'pivot_keys': [
                [],
                [YsonUint64(32)],
                [YsonUint64(64)],
                [YsonUint64(96)],
                [YsonUint64(128)]
            ],
            'disable_tablet_balancer': 'true',
        }
    )

    retry(lambda: client.mount_table(test_table), 10)
    wait_true(lambda: all(t['state'] == 'mounted' for t in client.get(test_table + '/@tablets')), 15)

    # Test yson result and versioned=True additional attributes
    client.insert_rows(test_table, [to_yson_type({'key': 'a', 'value': 'x'})])
    rows = client.lookup_rows(test_table, [{'key': 'a'}], yson=True, versioned=True)
    assert len(rows) == 1
    row = rows[0]
    assert len(row.attributes["write_timestamps"]) == 1
    timestamp = row.attributes["write_timestamps"][0]
    assert isinstance(row, YsonMap)
    assert isinstance(row['hash'], YsonUint64)
    assert isinstance(row['key'], YsonString)
    assert isinstance(row['value'], YsonList)
    assert isinstance(row['value'][0], YsonString)
    assert len(row) == 3
    assert row['key'] == 'a'
    assert row['value'] == [to_yson_type('x', {'aggregate': False, 'timestamp': timestamp})]


def test_delete_rows(yt_stuff):
    client = yt_client.Client('localhost:{}'.format(yt_stuff.yt_proxy_port))
    yt = yt_stuff.get_yt_client()
    yt.create('map_node', '//test_delete_rows', ignore_existing=True)

    test_table = '//test_delete_rows/table'
    client.create_table(
        test_table,
        attributes={
            'dynamic': 'true',
            'schema': [
                {'name': 'hash', 'type': 'uint64', 'sort_order': 'ascending', 'expression': 'farm_hash(key) % 128'},
                {'name': 'key', 'type': 'string', 'sort_order': 'ascending'},
                {'name': 'value', 'type': 'string'},
            ],
            'pivot_keys': [
                [],
                [YsonUint64(32)],
                [YsonUint64(64)],
                [YsonUint64(96)],
                [YsonUint64(128)]
            ],
            'disable_tablet_balancer': 'true',
        }
    )

    retry(lambda: client.mount_table(test_table), 10)
    wait_true(lambda: all(t['state'] == 'mounted' for t in client.get(test_table + '/@tablets')), 15)

    rows_before_delete = [{'key': 'a', 'value': 'x'}, {'key': 'b', 'value': 'y'}]
    client.insert_rows(test_table, rows_before_delete)
    # Check rows is really inserted just in case
    rows = client.lookup_rows(test_table, [{'key': 'a'}, {'key': 'b'}], columns=['key', 'value'])
    assert rows == rows_before_delete

    client.delete_rows(test_table, [{'key': 'b'}])

    rows = client.lookup_rows(test_table, [{'key': 'a'}, {'key': 'b'}], columns=['key', 'value'])
    assert rows == [{'key': 'a', 'value': 'x'}]


def test_atomicity_none(yt_stuff):
    client = yt_client.Client('localhost:{}'.format(yt_stuff.yt_proxy_port))
    yt = yt_stuff.get_yt_client()
    yt.create('map_node', '//test_atomicity_none', ignore_existing=True)

    test_table = '//test_atomicity_none/table'
    client.create_table(
        test_table,
        attributes={
            'dynamic': 'true',
            'atomicity': 'none',
            'schema': [
                {'name': 'hash', 'type': 'uint64', 'sort_order': 'ascending', 'expression': 'farm_hash(key) % 128'},
                {'name': 'key', 'type': 'string', 'sort_order': 'ascending'},
                {'name': 'value', 'type': 'string'},
            ],
            'pivot_keys': [
                [],
                [YsonUint64(32)],
                [YsonUint64(64)],
                [YsonUint64(96)],
                [YsonUint64(128)]
            ],
            'disable_tablet_balancer': 'true',
        }
    )

    retry(lambda: client.mount_table(test_table), 10)
    wait_true(lambda: all(t['state'] == 'mounted' for t in client.get(test_table + '/@tablets')), 15)

    client.insert_rows(test_table, [{'key': 'a', 'value': 'x'}, {'key': 'b', 'value': 'y'}], atomicity='none')
    client.delete_rows(test_table, [{'key': 'b'}], atomicity='none')

    rows = client.lookup_rows(test_table, [{'key': 'a'}, {'key': 'b'}], columns=['key', 'value'])
    assert rows == [{'key': 'a', 'value': 'x'}]
