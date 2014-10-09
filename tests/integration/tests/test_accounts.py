import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *


##################################################################

class TestAccounts(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    def _get_account_disk_space(self, account):
        return get('//sys/accounts/{0}/@resource_usage/disk_space'.format(account))

    def _get_account_node_count(self, account):
        return get('//sys/accounts/{0}/@resource_usage/node_count'.format(account))

    def _get_account_committed_disk_space(self, account):
        return get('//sys/accounts/{0}/@committed_resource_usage/disk_space'.format(account))

    def _get_account_disk_space_limit(self, account):
        return get('//sys/accounts/{0}/@resource_limits/disk_space'.format(account))

    def _set_account_disk_space_limit(self, account, value):
        set('//sys/accounts/{0}/@resource_limits/disk_space'.format(account), value)

    def _get_account_node_count_limit(self, account):
        return get('//sys/accounts/{0}/@resource_limits/node_count'.format(account))

    def _set_account_node_count_limit(self, account, value):
        set('//sys/accounts/{0}/@resource_limits/node_count'.format(account), value)

    def _is_account_over_disk_space_limit(self, account):
        return get('//sys/accounts/{0}/@over_disk_space_limit'.format(account))

    def _is_account_over_node_count_limit(self, account):
        return get('//sys/accounts/{0}/@over_node_count_limit'.format(account))
    
    def _get_tx_disk_space(self, tx, account):
        return get('#{0}/@resource_usage/{1}/disk_space'.format(tx, account))

    def _get_node_disk_space(self, path, *args, **kwargs):
        return get('{0}/@resource_usage/disk_space'.format(path), *args, **kwargs)


    def test_init(self):
        self.assertItemsEqual(ls('//sys/accounts'), ['sys', 'tmp'])
        assert get('//@account') == 'sys'
        assert get('//sys/@account') == 'sys'
        assert get('//tmp/@account') == 'tmp'
        assert get('//home/@account') == 'tmp'

    def test_account_create1(self):
        create_account('max')
        self.assertItemsEqual(ls('//sys/accounts'), ['sys', 'tmp', 'max'])
        assert self._get_account_disk_space('max') == 0
        assert self._get_account_node_count('max') == 0

    def test_account_create2(self):
        with pytest.raises(YtError): create_account('sys')
        with pytest.raises(YtError): create_account('tmp')

    def test_account_remove_builtin(self):
        with pytest.raises(YtError): remove_account('sys')
        with pytest.raises(YtError): remove_account('tmp')

    def test_account_create3(self):
        create_account('max')
        with pytest.raises(YtError): create_account('max')

    def test_account_attr1(self):
        set('//tmp/a', {})
        assert get('//tmp/a/@account') == 'tmp'

    def test_account_attr2(self):
        # should not crash
        get('//sys/accounts/tmp/@')

    def test_account_attr3(self):
        set('//tmp/a', {'x' : 1, 'y' : 2})
        assert get('//tmp/a/@account') == 'tmp'
        assert get('//tmp/a/x/@account') == 'tmp'
        assert get('//tmp/a/y/@account') == 'tmp'
        copy('//tmp/a', '//tmp/b')
        assert get('//tmp/b/@account') == 'tmp'
        assert get('//tmp/b/x/@account') == 'tmp'
        assert get('//tmp/b/y/@account') == 'tmp'

    def test_account_attr4(self):
        create_account('max')
        assert self._get_account_node_count('max') == 0

        set('//tmp/a', {})

        tmp_node_count = self._get_account_node_count('tmp')
        set('//tmp/a/@account', 'max')
        assert self._get_account_node_count('tmp') == tmp_node_count - 1
        assert self._get_account_node_count('max') == 1

    def test_account_attr5(self):
        create_account('max')
        set('//tmp/a', {})
        tx = start_transaction()
        with pytest.raises(YtError): set('//tmp/a/@account', 'max', tx=tx)

    def test_remove1(self):
        create_account('max')
        remove_account('max')

    def test_remove2(self):
        create_account('max')
        set('//tmp/a', {})
        set('//tmp/a/@account', 'max')
        set('//tmp/a/@account', 'sys')
        remove_account('max')

    def test_remove3(self):
        create_account('max')
        set('//tmp/a', {})
        set('//tmp/a/@account', 'max')
        with pytest.raises(YtError): remove_account('max')

    def test_file1(self):
        assert self._get_account_disk_space('tmp') == 0

        content = "some_data"
        create('file', '//tmp/f1')
        upload('//tmp/f1', content)
        space = self._get_account_disk_space('tmp')
        assert space > 0

        create('file', '//tmp/f2')
        upload('//tmp/f2', content)
        assert self._get_account_disk_space('tmp') == 2 * space

        remove('//tmp/f1')
        gc_collect()
        assert self._get_account_disk_space('tmp') == space

        remove('//tmp/f2')
        gc_collect()
        assert self._get_account_disk_space('tmp') == 0

    def test_file2(self):
        content = "some_data"
        create('file', '//tmp/f')
        upload('//tmp/f', content)
        space = self._get_account_disk_space('tmp')

        create_account('max')
        set('//tmp/f/@account', 'max')

        assert self._get_account_disk_space('tmp') == 0
        assert self._get_account_disk_space('max') == space

        remove('//tmp/f')
        gc_collect()
        assert self._get_account_disk_space('max') == 0

    def test_file3(self):
        create_account('max')

        assert self._get_account_disk_space('max') == 0

        content = "some_data"
        create('file', '//tmp/f', attributes={"account": "max"})
        upload('//tmp/f', content)
        assert self._get_account_disk_space('max') > 0

        remove('//tmp/f')
        gc_collect()
        assert self._get_account_disk_space('max') == 0

    def test_file4(self):
        create_account('max')

        content = "some_data"
        create('file', '//tmp/f', attributes={"account": "max"})
        upload('//tmp/f', content)
        space = self._get_account_disk_space('max')
        assert space > 0

        rf  = get('//tmp/f/@replication_factor')
        set('//tmp/f/@replication_factor', rf * 2)

        assert self._get_account_disk_space('max') == space * 2

    def test_table1(self):
        create('table', '//tmp/t')
        write('//tmp/t', {'a' : 'b'})
        assert self._get_account_disk_space('tmp') > 0

    def test_table2(self):
        create('table', '//tmp/t')

        tx = start_transaction()
        for i in xrange(0, 5):
            write('//tmp/t', {'a' : 'b'}, tx=tx)
            account_space = self._get_account_disk_space('tmp')
            tx_space = self._get_tx_disk_space(tx, 'tmp')
            assert account_space > 0
            assert account_space == tx_space
            assert self._get_node_disk_space('//tmp/t') == 0
            assert self._get_node_disk_space('//tmp/t', tx=tx) == tx_space
            last_space = tx_space

        commit_transaction(tx)
        assert self._get_node_disk_space('//tmp/t') == last_space

    def test_table3(self):
        create('table', '//tmp/t')
        write('//tmp/t', {'a' : 'b'})
        space1 = self._get_account_disk_space('tmp')
        assert space1 > 0

        tx = start_transaction()
        write('//tmp/t', {'xxxx' : 'yyyy'}, tx=tx)
        space2 = self._get_tx_disk_space(tx, 'tmp')
        assert space1 != space2
        assert self._get_account_disk_space('tmp') == space1 + space2
        assert self._get_node_disk_space('//tmp/t') == space1
        assert self._get_node_disk_space('//tmp/t', tx=tx) == space2

        commit_transaction(tx)
        assert self._get_account_disk_space('tmp') == space2
        assert self._get_node_disk_space('//tmp/t') == space2

    def test_table4(self):
        tx = start_transaction()
        create('table', '//tmp/t', tx=tx)
        write('//tmp/t', {'a' : 'b'}, tx=tx)
        assert self._get_account_disk_space('tmp') > 0
        abort_transaction(tx)
        assert self._get_account_disk_space('tmp') == 0

    def test_table5(self):
        tmp_node_count = self._get_account_node_count('tmp')

        create('table', '//tmp/t')
        write('//tmp/t', {'a' : 'b'})
        assert self._get_account_node_count('tmp') == tmp_node_count + 1
        space = self._get_account_disk_space('tmp')
        assert space > 0

        create_account('max')

        set('//tmp/t/@account', 'max')
        assert self._get_account_node_count('tmp') == tmp_node_count
        assert self._get_account_node_count('max') == 1
        assert self._get_account_disk_space('tmp') == 0
        assert self._get_account_disk_space('max') == space

        set('//tmp/t/@account', 'tmp')
        assert self._get_account_node_count('tmp') == tmp_node_count + 1
        assert self._get_account_node_count('max') == 0
        assert self._get_account_disk_space('tmp') == space
        assert self._get_account_disk_space('max') == 0

    def test_table6(self):
        create('table', '//tmp/t')

        tx = start_transaction()
        write('//tmp/t', {'a' : 'b'}, tx=tx)
        space = self._get_node_disk_space('//tmp/t', tx=tx)
        assert space > 0
        assert self._get_account_disk_space('tmp') == space

        tx2 = start_transaction(tx=tx)
        assert self._get_node_disk_space('//tmp/t', tx=tx2) == space
        write('<append=true>//tmp/t', {'a' : 'b'}, tx=tx2)
        assert self._get_node_disk_space('//tmp/t', tx=tx2) == space * 2
        assert self._get_account_disk_space('tmp') == space * 2

        commit_transaction(tx2)
        assert self._get_node_disk_space('//tmp/t', tx=tx) == space * 2
        assert self._get_account_disk_space('tmp') == space * 2
        commit_transaction(tx)
        assert self._get_node_disk_space('//tmp/t') == space * 2
        assert self._get_account_disk_space('tmp') == space * 2

    def test_node_count_limits1(self):
        create_account('max')
        assert self._is_account_over_node_count_limit('max') == 'false'
        self._set_account_node_count_limit('max', 1000)
        self._set_account_node_count_limit('max', 2000)
        self._set_account_node_count_limit('max', 0)
        assert self._is_account_over_node_count_limit('max') == 'true'
        with pytest.raises(YtError): self._set_account_node_count_limit('max', -1)

    def test_node_count_limits2(self):
        create_account('max')
        assert self._get_account_node_count('max') == 0
        create('table', '//tmp/t')
        set('//tmp/t/@account', 'max')
        assert self._get_account_node_count('max') == 1

    def test_node_count_limits3(self):
        create_account('max')
        create('table', '//tmp/t')
        self._set_account_node_count_limit('max', 0)
        with pytest.raises(YtError): set('//tmp/t/@account', 'max')

    def test_disk_space_limits1(self):
        create_account('max')
        assert self._is_account_over_disk_space_limit('max') == 'false'
        self._set_account_disk_space_limit('max', 1000)
        self._set_account_disk_space_limit('max', 2000)
        self._set_account_disk_space_limit('max', 0)
        assert self._is_account_over_disk_space_limit('max') == 'false'
        with pytest.raises(YtError): self._set_account_disk_space_limit('max', -1)

    def test_disk_space_limits2(self):
        create_account('max')
        self._set_account_disk_space_limit('max', 1000000)

        create('table', '//tmp/t')
        set('//tmp/t/@account', 'max')

        write('//tmp/t', {'a' : 'b'})
        assert self._is_account_over_disk_space_limit('max') == 'false'

        self._set_account_disk_space_limit('max', 0)
        assert self._is_account_over_disk_space_limit('max') == 'true'
        with pytest.raises(YtError): write('//tmp/t', {'a' : 'b'})

        self._set_account_disk_space_limit('max', self._get_account_disk_space('max'))
        assert self._is_account_over_disk_space_limit('max') == 'false'
        write('<append=true>//tmp/t', {'a' : 'b'})
        assert self._is_account_over_disk_space_limit('max') == 'true'

    def test_disk_space_limits3(self):
        create_account('max')
        self._set_account_disk_space_limit('max', 1000000)

        content = "some_data"

        create('file', '//tmp/f1', attributes={"account": "max"})
        upload('//tmp/f1', content)
        assert self._is_account_over_disk_space_limit('max') == 'false'

        self._set_account_disk_space_limit('max', 0)
        assert self._is_account_over_disk_space_limit('max') == 'true'

        create('file', '//tmp/f2', attributes={"account": "max"})
        with pytest.raises(YtError): upload('//tmp/f2', content)

        self._set_account_disk_space_limit('max', self._get_account_disk_space('max'))
        assert self._is_account_over_disk_space_limit('max') == 'false'

        create('file', '//tmp/f3', attributes={"account": "max"})
        upload('//tmp/f3', content)
        assert self._is_account_over_disk_space_limit('max') == 'true'

    def test_disk_space_limits5(self):
        content = "some_data"

        create('map_node', '//tmp/a')
        create('file', '//tmp/a/f1')
        upload('//tmp/a/f1', content)
        create('file', '//tmp/a/f2')
        upload('//tmp/a/f2', content)

        disk_space = get('//tmp/a/f1/@resource_usage/disk_space')
        assert get('//tmp/a/f2/@resource_usage/disk_space') == disk_space

        create_account('max')
        create('map_node', '//tmp/b')
        set('//tmp/b/@account', 'max')

        self._set_account_disk_space_limit('max', disk_space * 2 + 1)
        copy('//tmp/a', '//tmp/b/a')
        assert self._get_account_disk_space('max') == disk_space * 2
        assert exists('//tmp/b/a')

        remove('//tmp/b/a')
        gc_collect()
        assert self._get_account_disk_space('max') == 0

        self._set_account_disk_space_limit('max', 0)
        with pytest.raises(YtError): copy('//tmp/a', '//tmp/b/a')
        gc_collect()
        assert self._get_account_disk_space('max') == 0
        assert self._get_account_node_count('max') == 1
        assert not exists('//tmp/b/a')

    def test_committed_usage(self):
        assert self._get_account_committed_disk_space('tmp') == 0

        create('table', '//tmp/t')
        write('//tmp/t', {'a' : 'b'})
        space = get('//tmp/t/@resource_usage/disk_space')
        assert space > 0
        assert self._get_account_committed_disk_space('tmp') == space

        tx = start_transaction()
        write('<append=true>//tmp/t', {'a' : 'b'}, tx=tx)
        assert self._get_account_committed_disk_space('tmp') == space

        commit_transaction(tx)
        assert self._get_account_committed_disk_space('tmp') == space * 2

    def test_copy(self):
        create_account('a1')
        create_account('a2')

        create('map_node', '//tmp/x1', attributes={"account": "a1"})
        assert get('//tmp/x1/@account') == 'a1'

        create('map_node', '//tmp/x2', attributes={"account": "a2"})
        assert get('//tmp/x2/@account') == 'a2'

        create('table', '//tmp/x1/t')
        assert get('//tmp/x1/t/@account') == 'a1'

        write('//tmp/x1/t', {'a' : 'b'})
        space = self._get_account_disk_space('a1')
        assert space > 0
        assert space == self._get_account_committed_disk_space('a1')

        copy('//tmp/x1/t', '//tmp/x2/t')
        assert get('//tmp/x2/t/@account') == 'a2'

        assert space == self._get_account_disk_space('a2')
        assert space == self._get_account_committed_disk_space('a2')

    def test_rename_success(self):
        create_account('a1')
        set('//sys/accounts/a1/@name', 'a2')
        assert get('//sys/accounts/a2/@name') == 'a2'

    def test_rename_fail(self):
        create_account('a1')
        create_account('a2')
        with pytest.raises(YtError): set('//sys/accounts/a1/@name', 'a2')
