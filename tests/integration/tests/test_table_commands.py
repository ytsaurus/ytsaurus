import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestTableCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5

    # test that chunks are not available from chunk_lists
    # Issue #198
    def test_chunk_ids(self):
        create('table', '//tmp/t')
        write_str('//tmp/t', '{a=10}')

        chunk_ids = ls('//sys/chunks')
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        with pytest.raises(YTError): get('//sys/chunk_lists/"' + chunk_id + '"')

    def test_simple(self):
        create('table', '//tmp/table')

        assert read('//tmp/table') == []
        assert get('//tmp/table/@row_count') == 0
        assert get('//tmp/table/@chunk_count') == 0

        write_str('//tmp/table', '{b="hello"}')
        assert read('//tmp/table') == [{"b":"hello"}]
        assert get('//tmp/table/@row_count') == 1
        assert get('//tmp/table/@chunk_count') == 1

        write_str('//tmp/table', '{b="2";a="1"};{x="10";y="20";a="30"}')
        assert read('//tmp/table') == [{"b": "hello"}, {"a":"1", "b":"2"}, {"a":"30", "x":"10", "y":"20"}]
        assert get('//tmp/table/@row_count') == 3
        assert get('//tmp/table/@chunk_count') == 2

    def test_sorted_write(self):
        create('table', '//tmp/table')

        write_str('//tmp/table', '{key = 0}; {key = 1}; {key = 2}; {key = 3}', sorted_by='key')

        assert get('//tmp/table/@sorted') ==  'true'
        assert get('//tmp/table/@sorted_by') ==  ['key']
        assert get('//tmp/table/@row_count') ==  4

        # sorted flag is discarded when writing to sorted table
        write_str('//tmp/table', '{key = 4}')
        assert get('//tmp/table/@sorted') ==  'false'
        with pytest.raises(YTError): get('//tmp/table/@sorted_by')

    def test_invalid_cases(self):
        create('table', '//tmp/table')

        # we can write only list fragments
        with pytest.raises(YTError): write_str('//tmp/table', 'string')
        with pytest.raises(YTError): write_str('//tmp/table', '100')
        with pytest.raises(YTError): write_str('//tmp/table', '3.14')
        with pytest.raises(YTError): write_str('//tmp/table', '<>')

        # we can write sorted data only to empty table
        write('//tmp/table', {'foo': 'bar'}, sorted_by='foo')
        with pytest.raises(YTError):
            write('//tmp/table', {'foo': 'zzz_bar'}, sorted_by='foo')


    def test_row_index_selector(self):
        create('table', '//tmp/table')

        write_str('//tmp/table', '{a = 0}; {b = 1}; {c = 2}; {d = 3}')

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
        with pytest.raises(YTError): read('//tmp/table[:a]')

    def test_row_key_selector(self):
        create('table', '//tmp/table')

        v1 = {'s' : 'a', 'i': 0,    'd' : 15.5}
        v2 = {'s' : 'a', 'i': 10,   'd' : 15.2}
        v3 = {'s' : 'b', 'i': 5,    'd' : 20.}
        v4 = {'s' : 'b', 'i': 20,   'd' : 20.}
        v5 = {'s' : 'c', 'i': -100, 'd' : 10.}

        values = [v1, v2, v3, v4, v5]
        write('//tmp/table', values, sorted_by='s;i;d')

        # possible empty ranges
        assert read('//tmp/table[a : a]') == []
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
        with pytest.raises(YTError): assert read('//tmp/table[#0:zz]')


    def test_column_selector(self):
        create('table', '//tmp/table')

        write_str('//tmp/table', '{a = 1; aa = 2; b = 3; bb = 4; c = 5}')
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
        with pytest.raises(YTError): read('//tmp/table{a:a}')  # left = right
        with pytest.raises(YTError): read('//tmp/table{b:a}')  # left > right

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

        write_str('//tmp/table', '{a=1}', tx=tx)
        write_str('//tmp/table', '{b=2}', tx=tx)

        assert read('//tmp/table') == []
        assert read('//tmp/table', tx=tx) == [{'a':1}, {'b':2}]

        commit_transaction(tx)
        assert read('//tmp/table') == [{'a':1}, {'b':2}]

    def test_shared_locks_three_chunks(self):
        create('table', '//tmp/table')
        tx = start_transaction()

        write_str('//tmp/table', '{a=1}', tx=tx)
        write_str('//tmp/table', '{b=2}', tx=tx)
        write_str('//tmp/table', '{c=3}', tx=tx)

        assert read('//tmp/table') == []
        assert read('//tmp/table', tx=tx) == [{'a':1}, {'b':2}, {'c' : 3}]

        commit_transaction(tx)
        assert read('//tmp/table') == [{'a':1}, {'b':2}, {'c' : 3}]

    def test_shared_locks_parallel_tx(self):
        create('table', '//tmp/table')

        write_str('//tmp/table', '{a=1}')

        tx1 = start_transaction()
        tx2 = start_transaction()

        write_str('//tmp/table', '{b=2}', tx=tx1)

        write_str('//tmp/table', '{c=3}', tx=tx2)
        write_str('//tmp/table', '{d=4}', tx=tx2)

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

        write('//tmp/table', v2, tx=inner_tx)
        assert read('//tmp/table', tx=outer_tx) == [v1]
        assert read('//tmp/table', tx=inner_tx) == [v1, v2]

        write('//tmp/table', v3, tx=outer_tx) # this won't be seen from inner
        assert read('//tmp/table', tx=outer_tx) == [v1, v3]
        assert read('//tmp/table', tx=inner_tx) == [v1, v2]

        write('//tmp/table', v4, tx=inner_tx)
        assert read('//tmp/table', tx=outer_tx) == [v1, v3]
        assert read('//tmp/table', tx=inner_tx) == [v1, v2, v4]

        commit_transaction(inner_tx)
        self.assertItemsEqual(read('//tmp/table', tx=outer_tx), [v1, v2, v4, v3]) # order is not specified

        commit_transaction(outer_tx)

    def test_random_symbols(self):
        for i in xrange(10):
            create('table', '//tmp/table')
            f = 'some_random_file.txt'
            os.system('rm %s' % f)
            os.system('dd if=/dev/urandom of=%s bs=128 count=1 2>/dev/null' % f)
            data = open(f, 'rt').read()
            # here is used manual run because of checking error codes and messages
            p = run_command('write', '//tmp/table')
            stdout, stderr = p.communicate(data)

            # check error message and return code to be sure that there was no coredump
            assert p.returncode == 1
            assert "Could not read symbol" in stderr
            assert "Stack trace" not in stderr

            remove('//tmp/table')

    def test_codec_in_writer(self):
        create('table', '//tmp/table')
        write_str('//tmp/table', '{b="hello"}', opt="/table_writer/codec_id=gzip_best_compression")

        assert read('//tmp/table') == [{"b":"hello"}]

        chunk_id = get("//tmp/table/@chunk_ids/0")
        assert get('#"%s"/@codec_id' % chunk_id) == "gzip_best_compression"

