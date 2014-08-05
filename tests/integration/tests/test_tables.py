import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *
from time import sleep


##################################################################

class TestTables(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    def test_invalid_type(self):
        with pytest.raises(YtError): read('//tmp')
        with pytest.raises(YtError): write('//tmp', [])

    def test_simple(self):
        create('table', '//tmp/table')

        assert read('//tmp/table') == []
        assert get('//tmp/table/@row_count') == 0
        assert get('//tmp/table/@chunk_count') == 0

        write('//tmp/table', {"b": "hello"})
        assert read('//tmp/table') == [{"b":"hello"}]
        assert get('//tmp/table/@row_count') == 1
        assert get('//tmp/table/@chunk_count') == 1

        write('<append=true>//tmp/table', [{"b": "2", "a": "1"}, {"x": "10", "y": "20", "a": "30"}])
        assert read('//tmp/table') == [{"b": "hello"}, {"a":"1", "b":"2"}, {"a":"30", "x":"10", "y":"20"}]
        assert get('//tmp/table/@row_count') == 3
        assert get('//tmp/table/@chunk_count') == 2

    def test_sorted_write(self):
        create('table', '//tmp/table')

        write('//tmp/table', [{"key": 0}, {"key": 1}, {"key": 2}, {"key": 3}], sorted_by="key")

        assert get('//tmp/table/@sorted') ==  'true'
        assert get('//tmp/table/@sorted_by') ==  ['key']
        assert get('//tmp/table/@row_count') ==  4

        # sorted flag is discarded when writing to sorted table
        write('<append=true>//tmp/table', {"key": 4})
        assert get('//tmp/table/@sorted') ==  'false'
        with pytest.raises(YtError): get('//tmp/table/@sorted_by')

    def test_append_overwrite_write(self):
        # Default (append).
        # COMPAT(ignat): When migrating to overwrite semantics, change this to 1.
        create('table', '//tmp/table1')
        assert get('//tmp/table1/@row_count') == 0
        write('//tmp/table1', {"a": 0})
        assert get('//tmp/table1/@row_count') == 1
        write('//tmp/table1', {"a": 1})
        assert get('//tmp/table1/@row_count') == 1

        # Append
        create('table', '//tmp/table2')
        assert get('//tmp/table2/@row_count') == 0
        write('<append=true>//tmp/table2', {"a": 0})
        assert get('//tmp/table2/@row_count') == 1
        write('<append=true>//tmp/table2', {"a": 1})
        assert get('//tmp/table2/@row_count') == 2

        # Overwrite
        create('table', '//tmp/table3')
        assert get('//tmp/table3/@row_count') == 0
        write('<append=false>//tmp/table3', {"a": 0})
        assert get('//tmp/table3/@row_count') == 1
        write('<append=false>//tmp/table3', {"a": 1})
        assert get('//tmp/table3/@row_count') == 1

    def test_invalid_cases(self):
        create('table', '//tmp/table')

        # we can write only list fragments
        with pytest.raises(YtError): write('<append=true>//tmp/table', yson.loads('string'))
        with pytest.raises(YtError): write('<append=true>//tmp/table', yson.loads('100'))
        with pytest.raises(YtError): write('<append=true>//tmp/table', yson.loads('3.14'))

        # we can write sorted data only to empty table
        write('<append=true>//tmp/table', {'foo': 'bar'}, sorted_by='foo')
        with pytest.raises(YtError):
            write('"<append=true>" + //tmp/table', {'foo': 'zzz_bar'}, sorted_by='foo')


        content = "some_data"
        create('file', '//tmp/file')
        upload('//tmp/file', content)
        with pytest.raises(YtError): read('//tmp/file')

    def test_row_index_selector(self):
        create('table', '//tmp/table')

        write('//tmp/table', [{"a": 0}, {"b": 1}, {"c": 2}, {"d": 3}])

        # closed ranges
        assert read('//tmp/table[#0:#2]') == [{'a': 0}, {'b' : 1}] # simple
        assert read('//tmp/table[#-1:#1]') == [{'a': 0}] # left < min
        assert read('//tmp/table[#2:#5]') == [{'c': 2}, {'d': 3}] # right > max
        assert read('//tmp/table[#-10:#-5]') == [] # negative indexes

        assert read('//tmp/table[#1:#1]') == [] # left = right
        assert read('//tmp/table[#3:#1]') == [] # left > right

        # open ranges
        assert read('//tmp/table[:]') == [{'a': 0}, {'b' : 1}, {'c' : 2}, {'d' : 3}]
        assert read('//tmp/table[:#3]') == [{'a': 0}, {'b' : 1}, {'c' : 2}]
        assert read('//tmp/table[#2:]') == [{'c' : 2}, {'d' : 3}]

        # reading key selectors from unsorted table
        with pytest.raises(YtError): read('//tmp/table[:a]')

    def test_row_key_selector(self):
        create('table', '//tmp/table')

        v1 = {'s' : 'a', 'i': 0,    'd' : 15.5}
        v2 = {'s' : 'a', 'i': 10,   'd' : 15.2}
        v3 = {'s' : 'b', 'i': 5,    'd' : 20.}
        v4 = {'s' : 'b', 'i': 20,   'd' : 20.}
        v5 = {'s' : 'c', 'i': -100, 'd' : 10.}

        values = [v1, v2, v3, v4, v5]
        write('//tmp/table', values, sorted_by=['s', 'i', 'd'])

        # possible empty ranges
        assert read('//tmp/table[a : a]') == []
        assert read('//tmp/table[(a, 1) : (a, 10)]') == []
        assert read('//tmp/table[b : a]') == []
        assert read('//tmp/table[(c, 0) : (a, 10)]') == []
        assert read('//tmp/table[(a, 10, 1e7) : (b, )]') == []

        # some typical cases
        assert read('//tmp/table[(a, 4) : (b, 20, 18.)]') == [v2, v3]
        assert read('//tmp/table[c:]') == [v5]
        assert read('//tmp/table[:(a, 10)]') == [v1]
        assert read('//tmp/table[:(a, 11)]') == [v1, v2]
        assert read('//tmp/table[:]') == [v1, v2, v3, v4, v5]

        # combination of row and key selectors
        assert read('//tmp/table{i}[aa: (b, 10)]') == [{'i' : 5}]
        assert read('//tmp/table{a: o}[(b, 0): (c, 0)]') == \
            [
                {'i': 5, 'd' : 20.},
                {'i': 20,'d' : 20.},
                {'i': -100, 'd' : 10.}
            ]

        # limits of different types
        assert read('//tmp/table[#0:zz]') == [v1, v2, v3, v4, v5]


    def test_column_selector(self):
        create('table', '//tmp/table')

        write('//tmp/table', {"a": 1, "aa": 2, "b": 3, "bb": 4, "c": 5})
        # empty columns
        assert read('//tmp/table{}') == [{}]

        # single columms
        assert read('//tmp/table{a}') == [{'a' : 1}]
        assert read('//tmp/table{a, }') == [{'a' : 1}] # extra comma
        assert read('//tmp/table{a, a}') == [{'a' : 1}]
        assert read('//tmp/table{c, b}') == [{'b' : 3, 'c' : 5}]
        assert read('//tmp/table{zzzzz}') == [{}] # non existent column

        # range columns
        # closed ranges
        with pytest.raises(YtError): read('//tmp/table{a:a}')  # left = right
        with pytest.raises(YtError): read('//tmp/table{b:a}')  # left > right

        assert read('//tmp/table{aa:b}') == [{'aa' : 2}]  # (+, +)
        assert read('//tmp/table{aa:bx}') == [{'aa' : 2, 'b' : 3, 'bb' : 4}]  # (+, -)
        assert read('//tmp/table{aaa:b}') == [{}]  # (-, +)
        assert read('//tmp/table{aaa:bx}') == [{'b' : 3, 'bb' : 4}] # (-, -)

        # open ranges
        # from left
        assert read('//tmp/table{:aa}') == [{'a' : 1}] # +
        assert read('//tmp/table{:aaa}') == [{'a' : 1, 'aa' : 2}] # -

        # from right
        assert read('//tmp/table{bb:}') == [{'bb' : 4, 'c' : 5}] # +
        assert read('//tmp/table{bz:}') == [{'c' : 5}] # -
        assert read('//tmp/table{xxx:}') == [{}]

        # fully open
        assert read('//tmp/table{:}') == [{'a' :1, 'aa': 2,  'b': 3, 'bb' : 4, 'c': 5}]

        # mixed column keys
        assert read('//tmp/table{aa, a:bb}') == [{'a' : 1, 'aa' : 2, 'b': 3}]

    def test_shared_locks_two_chunks(self):
        create('table', '//tmp/table')
        tx = start_transaction()

        write('<append=true>//tmp/table', {"a": 1}, tx=tx)
        write('<append=true>//tmp/table', {"b": 2}, tx=tx)

        assert read('//tmp/table') == []
        assert read('//tmp/table', tx=tx) == [{'a':1}, {'b':2}]

        commit_transaction(tx)
        assert read('//tmp/table') == [{'a':1}, {'b':2}]

    def test_shared_locks_three_chunks(self):
        create('table', '//tmp/table')
        tx = start_transaction()

        write('<append=true>//tmp/table', {"a": 1}, tx=tx)
        write('<append=true>//tmp/table', {"b": 2}, tx=tx)
        write('<append=true>//tmp/table', {"c": 3}, tx=tx)

        assert read('//tmp/table') == []
        assert read('//tmp/table', tx=tx) == [{'a':1}, {'b':2}, {'c' : 3}]

        commit_transaction(tx)
        assert read('//tmp/table') == [{'a':1}, {'b':2}, {'c' : 3}]

    def test_shared_locks_parallel_tx(self):
        create('table', '//tmp/table')

        write('//tmp/table', {"a": 1})

        tx1 = start_transaction()
        tx2 = start_transaction()

        write('<append=true>//tmp/table', {"b": 2}, tx=tx1)

        write('<append=true>//tmp/table', {"c": 3}, tx=tx2)
        write('<append=true>//tmp/table', {"d": 4}, tx=tx2)

        # check which records are seen from different transactions
        assert read('//tmp/table') == [{'a' : 1}]
        assert read('//tmp/table', tx = tx1) == [{'a' : 1}, {'b': 2}]
        assert read('//tmp/table', tx = tx2) == [{'a' : 1}, {'c': 3}, {'d' : 4}]

        commit_transaction(tx2)
        assert read('//tmp/table') == [{'a' : 1}, {'c': 3}, {'d' : 4}]
        assert read('//tmp/table', tx = tx1) == [{'a' : 1}, {'b': 2}]

        # now all records are in table in specific order
        commit_transaction(tx1)
        assert read('//tmp/table') == [{'a' : 1}, {'c': 3}, {'d' : 4}, {'b' : 2}]

    def test_shared_locks_nested_tx(self):
        create('table', '//tmp/table')

        v1 = {'k' : 1}
        v2 = {'k' : 2}
        v3 = {'k' : 3}
        v4 = {'k' : 4}

        outer_tx = start_transaction()

        write('//tmp/table', v1, tx=outer_tx)

        inner_tx = start_transaction(tx=outer_tx)

        write('<append=true>//tmp/table', v2, tx=inner_tx)
        assert read('//tmp/table', tx=outer_tx) == [v1]
        assert read('//tmp/table', tx=inner_tx) == [v1, v2]

        write('<append=true>//tmp/table', v3, tx=outer_tx) # this won't be seen from inner
        assert read('//tmp/table', tx=outer_tx) == [v1, v3]
        assert read('//tmp/table', tx=inner_tx) == [v1, v2]

        write('<append=true>//tmp/table', v4, tx=inner_tx)
        assert read('//tmp/table', tx=outer_tx) == [v1, v3]
        assert read('//tmp/table', tx=inner_tx) == [v1, v2, v4]

        commit_transaction(inner_tx)
        self.assertItemsEqual(read('//tmp/table', tx=outer_tx), [v1, v2, v4, v3]) # order is not specified

        commit_transaction(outer_tx)

    def test_codec_in_writer(self):
        create('table', '//tmp/table')
        set('//tmp/table/@compression_codec', "gzip_best_compression")
        write('//tmp/table', {"b": "hello"})

        assert read('//tmp/table') == [{"b":"hello"}]

        chunk_id = get("//tmp/table/@chunk_ids/0")
        assert get('#%s/@compression_codec' % chunk_id) == "gzip_best_compression"

    def test_copy(self):
        create('table', '//tmp/t')
        write('//tmp/t', {"a": "b"})

        assert read('//tmp/t') == [{'a' : 'b'}]

        copy('//tmp/t', '//tmp/t2')
        assert read('//tmp/t2') == [{'a' : 'b'}]

        assert get('//tmp/t2/@resource_usage') == get('//tmp/t/@resource_usage')
        assert get('//tmp/t2/@replication_factor') == get('//tmp/t/@replication_factor')

        remove('//tmp/t')
        assert read('//tmp/t2') == [{'a' : 'b'}]

        remove('//tmp/t2')
        for chunk in get_chunks():
            nodes = get("#%s/@owning_nodes" % chunk)
            for t in ["//tmp/t", "//tmp/t2"]:
                assert t not in nodes

    def test_copy_to_the_same_table(self):
        create('table', '//tmp/t')
        write('//tmp/t', {"a": "b"})

        with pytest.raises(YtError): copy('//tmp/t', '//tmp/t')

    def test_copy_tx(self):
        create('table', '//tmp/t')
        write('//tmp/t', {"a": "b"})

        tx = start_transaction()
        assert read('//tmp/t', tx=tx) == [{'a' : 'b'}]
        copy('//tmp/t', '//tmp/t2', tx=tx)
        assert read('//tmp/t2', tx=tx) == [{'a' : 'b'}]

        #assert get('//tmp/@recursive_resource_usage') == {"disk_space" : 438}
        #assert get('//tmp/@recursive_resource_usage', tx=tx) == {"disk_space" : 2 * 438}
        commit_transaction(tx)

        assert read('//tmp/t2') == [{'a' : 'b'}]

        remove('//tmp/t')
        assert read('//tmp/t2') == [{'a' : 'b'}]

        remove('//tmp/t2')
        for chunk in get_chunks():
            nodes = get("#%s/@owning_nodes" % chunk)
            for t in ["//tmp/t", "//tmp/t2"]:
                assert t not in nodes

    def test_copy_not_sorted(self):
        create('table', '//tmp/t1')
        assert get('//tmp/t1/@sorted') == 'false'
        assert get('//tmp/t1/@key_columns') == []

        copy('//tmp/t1', '//tmp/t2')
        assert get('//tmp/t2/@sorted') == 'false'
        assert get('//tmp/t2/@key_columns') == []

    def test_copy_sorted(self):
        create('table', '//tmp/t1')
        sort(in_='//tmp/t1', out='//tmp/t1', sort_by='key')
        assert get('//tmp/t1/@sorted') == 'true'
        assert get('//tmp/t1/@key_columns') == ['key']

        copy('//tmp/t1', '//tmp/t2')
        assert get('//tmp/t2/@sorted') == 'true'
        assert get('//tmp/t2/@key_columns') == ['key']

    def test_remove_create_under_transaction(self):
        create("table", "//tmp/table_xxx")
        tx = start_transaction()

        remove("//tmp/table_xxx", tx=tx)
        create("table", "//tmp/table_xxx", tx=tx)

    def test_transaction_staff(self):
        create("table", "//tmp/table_xxx")

        tx = start_transaction()
        remove("//tmp/table_xxx", tx=tx)
        inner_tx = start_transaction(tx=tx)
        get("//tmp", tx=inner_tx)

    def test_exists(self):
        assert not exists("//tmp/t")
        assert not exists("<append=true>//tmp/t")

        create("table", "//tmp/t")
        assert exists("//tmp/t")
        assert not exists("//tmp/t/x")
        assert not exists("//tmp/t/1")
        assert not exists("//tmp/t/1/t")
        assert exists("<append=false>//tmp/t")
        assert exists("//tmp/t[:#100]")
        assert exists("//tmp/t/@")
        assert exists("//tmp/t/@chunk_ids")

    def test_invalid_channels_in_create(self):
        with pytest.raises(YtError): create('table', '//tmp/t', attributes={'channels': '123'})

    def test_replication_factor_attr(self):
        create('table', '//tmp/t')
        assert get('//tmp/t/@replication_factor') == 3

        with pytest.raises(YtError): remove('//tmp/t/@replication_factor')
        with pytest.raises(YtError): set('//tmp/t/@replication_factor', 0)
        with pytest.raises(YtError): set('//tmp/t/@replication_factor', {})

        tx = start_transaction()
        with pytest.raises(YtError): set('//tmp/t/@replication_factor', 2, tx=tx)

    def test_replication_factor_static(self):
        create('table', '//tmp/t')
        set('//tmp/t/@replication_factor', 2)

        write('//tmp/t', {'foo' : 'bar'})

        chunk_ids = get('//tmp/t/@chunk_ids')
        assert len(chunk_ids) == 1

        chunk_id = chunk_ids[0]
        assert get('#' + chunk_id + '/@replication_factor') == 2

    def test_recursive_resource_usage(self):
        create('table', '//tmp/t1')
        write('//tmp/t1', {"a": "b"})
        copy('//tmp/t1', '//tmp/t2')

        assert get('//tmp/t1/@resource_usage')['disk_space'] + \
               get('//tmp/t2/@resource_usage')['disk_space'] == \
               get('//tmp/@recursive_resource_usage')['disk_space']

    def test_chunk_tree_balancer(self):
        create('table', '//tmp/t')
        for i in xrange(0, 40):
            write('<append=true>//tmp/t', {'a' : 'b'})
        chunk_list_id = get('//tmp/t/@chunk_list_id')
        statistics = get('#' + chunk_list_id + '/@statistics')
        assert statistics['chunk_count'] == 40
        assert statistics['chunk_list_count'] == 2
        assert statistics['row_count'] == 40
        assert statistics['rank'] == 2

    @pytest.mark.skipif('True') # very long test
    def test_chunk_tree_balancer_deep(self):
        create('table', '//tmp/t')
        tx_stack = list()
        tx = start_transaction()
        tx_stack.append(tx)

        for i in xrange(0, 1000):
            write('<append=true>//tmp/t', {'a' : i}, tx=tx)

        chunk_list_id = get('//tmp/t/@chunk_list_id', tx=tx)
        statistics = get('#' + chunk_list_id + '/@statistics', tx=tx)
        assert statistics['chunk_count'] == 1000
        assert statistics['chunk_list_count'] == 2001
        assert statistics['row_count'] == 1000
        assert statistics['rank'] == 1001

        tbl_a = read('//tmp/t', tx=tx)

        commit_transaction(tx)
        sleep(1.0)

        chunk_list_id = get('//tmp/t/@chunk_list_id')
        statistics = get('#' + chunk_list_id + '/@statistics')
        assert statistics['chunk_count'] == 1000
        assert statistics['chunk_list_count'] == 2
        assert statistics['row_count'] == 1000
        assert statistics['rank'] == 2

        assert tbl_a == read('//tmp/t')

    def _check_replication_factor(self, path, expected_rf):
        chunk_ids = get(path + '/@chunk_ids')
        for id in chunk_ids:
            assert get('#' + id + '/@replication_factor') == expected_rf

    def test_vital_update(self):
        create('table', '//tmp/t')
        for i in xrange(0, 5):
            write('<append=true>//tmp/t', {'a' : 'b'})

        def check_vital_chunks(is_vital):
            chunk_ids = get('//tmp/t/@chunk_ids')
            for id in chunk_ids:
                assert get('#' + id + '/@vital') == is_vital

        assert get('//tmp/t/@vital') == 'true'
        check_vital_chunks('true')

        set('//tmp/t/@vital', 'false')
        assert get('//tmp/t/@vital') == 'false'
        sleep(2)

        check_vital_chunks('false')

    def test_replication_factor_update1(self):
        create('table', '//tmp/t')
        for i in xrange(0, 5):
            write('<append=true>//tmp/t', {'a' : 'b'})
        set('//tmp/t/@replication_factor', 4)
        sleep(2)
        self._check_replication_factor('//tmp/t', 4)

    def test_replication_factor_update2(self):
        create('table', '//tmp/t')
        tx = start_transaction()
        for i in xrange(0, 5):
            write('<append=true>//tmp/t', {'a' : 'b'}, tx=tx)
        set('//tmp/t/@replication_factor', 4)
        commit_transaction(tx)
        sleep(2)
        self._check_replication_factor('//tmp/t', 4)

    def test_replication_factor_update3(self):
        create('table', '//tmp/t')
        tx = start_transaction()
        for i in xrange(0, 5):
            write('<append=true>//tmp/t', {'a' : 'b'}, tx=tx)
        set('//tmp/t/@replication_factor', 2)
        commit_transaction(tx)
        sleep(2)
        self._check_replication_factor('//tmp/t', 2)

    def test_key_columns1(self):
        create('table', '//tmp/t', opt=['/attributes/key_columns=[a;b]'])
        assert get('//tmp/t/@sorted') == 'true'
        assert get('//tmp/t/@key_columns') == ['a', 'b']

    def test_key_columns2(self):
        create('table', '//tmp/t')
        write('//tmp/t', {'a' : 'b'})
        with pytest.raises(YtError): set('//tmp/t/@key_columns', ['a', 'b'])

    def test_key_columns3(self):
        create('table', '//tmp/t')
        with pytest.raises(YtError): set('//tmp/t/@key_columns', 123)

    def test_statistics1(self):
        table = '//tmp/t'
        create('table', table)
        set('//tmp/t/@compression_codec', 'snappy')
        write(table, {"foo": "bar"})

        for i in xrange(8):
            merge(in_=[table, table], out="<append=true>" + table)

        chunk_count = 3**8
        assert len(get('//tmp/t/@chunk_ids')) == chunk_count

        codec_info = get('//tmp/t/@compression_statistics')
        assert codec_info['snappy']['chunk_count'] == chunk_count

        erasure_info = get('//tmp/t/@erasure_statistics')
        assert erasure_info['none']['chunk_count'] == chunk_count

    @only_linux
    def test_statistics2(self):
        tableA = '//tmp/a'
        create('table', tableA)
        write(tableA, {"foo": "bar"})

        tableB = '//tmp/b'
        create('table', tableB)
        set(tableB + '/@compression_codec', 'snappy')

        map(in_=[tableA], out=[tableB], command="cat")

        codec_info = get(tableB + '/@compression_statistics')
        assert codec_info.keys() == ['snappy']

    def test_json_format(self):
        create('table', '//tmp/t')
        write('//tmp/t', '{"x":"0"}\n{"x":"1"}', input_format="json", is_raw=True)
        assert '{"x":"0"}\n{"x":"1"}\n' == read('//tmp/t', output_format="json")

    def test_boolean(self):
        create('table', '//tmp/t')
        format = yson.loads("<boolean_as_string=false;format=text>yson")
        write('//tmp/t', '{x=%false};{x=%true};{x=false};', input_format=format, is_raw=True)
        assert '{"x"=%false};\n{"x"=%true};\n{"x"="false"};\n' == read('//tmp/t', output_format=format)

    def test_uint64(self):
        create('table', '//tmp/t')
        format = yson.loads("<format=text>yson")
        write('//tmp/t', '{x=1u};{x=4u};{x=9u};', input_format=format, is_raw=True)
        assert '{"x"=1u};\n{"x"=4u};\n{"x"=9u};\n' == read('//tmp/t', output_format=format)
