import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestAccounts(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    START_SCHEDULER = False


    def _get_account_disk_space(self, account):
        return get('//sys/accounts/{0}/@resource_usage/disk_space'.format(account))

    def _get_account_node_count(self, account):
        return get('//sys/accounts/{0}/@node_count'.format(account))

    def _get_tx_disk_space(self, tx, account):
        return get('#{0}/@resource_usage/{1}/disk_space'.format(tx, account))

    def _get_node_disk_space(self, path, *args, **kwargs):
        return get('{0}/@resource_usage/disk_space'.format(path), *args, **kwargs)


    def test_init(self):
        self.assertItemsEqual(ls('//sys/accounts'), ['sys', 'tmp'])
        assert get('//@account') == 'sys'
        assert get('//sys/@account') == 'sys'
        assert get('//tmp/@account') == 'tmp'
        assert get('//home/@account') == 'sys'

    def test_account_create1(self):
        create_account('max')
        self.assertItemsEqual(ls('//sys/accounts'), ['sys', 'tmp', 'max'])
        assert self._get_account_disk_space('max') == 0
        assert self._get_account_node_count('max') == 0

    def test_account_create2(self):
        with pytest.raises(YTError): create_account('sys')
        with pytest.raises(YTError): create_account('tmp')
        with pytest.raises(YTError): remove_account('sys')
        with pytest.raises(YTError): remove_account('tmp')

    def test_account_attr1(self):
        set('//tmp/a', {})
        assert get('//tmp/a/@account') == 'tmp'

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
        with pytest.raises(YTError): set('//tmp/a/@account', 'max', tx=tx)

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
        with pytest.raises(YTError): remove_account('max')

    def test_file1(self):
        assert self._get_account_disk_space('tmp') == 0

        content = "some_data"
        upload('//tmp/f1', content)
        space = self._get_account_disk_space('tmp')
        assert space > 0

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
        upload('//tmp/f', content, opt=['/attributes/account=max'])
        assert self._get_account_disk_space('max') > 0

        remove('//tmp/f')
        gc_collect()
        assert self._get_account_disk_space('max') == 0

    def test_file4(self):
        create_account('max')

        content = "some_data"
        upload('//tmp/f', content, opt=['/attributes/account=max'])
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
        write('<overwrite=true>//tmp/t', {'xxxx' : 'yyyy'}, tx=tx)
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
