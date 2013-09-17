import pytest
import sys

import yt.yson

from yt_env_setup import YTEnvSetup
from yt_commands import *


##################################################################

class TestAcls(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    START_SCHEDULER = True

    def test_default_acl_sanity(self):
        create_user('u')
        with pytest.raises(YTError): set('/', {}, user='u')
        with pytest.raises(YTError): set('//sys', {}, user='u')
        with pytest.raises(YTError): set('//sys/a', 'b', user='u')
        with pytest.raises(YTError): set('/a', 'b', user='u')
        with pytest.raises(YTError): remove('//sys', user='u')
        with pytest.raises(YTError): remove('//sys/tmp', user='u')
        with pytest.raises(YTError): remove('//sys/home', user='u')
        with pytest.raises(YTError): set('//sys/home/a', 'b', user='u')
        set('//tmp/a', 'b', user='u')
        ls('//tmp', user='guest')
        with pytest.raises(YTError): set('//tmp/c', 'd', user='guest')

    def _to_list(self, x):
        if isinstance(x, str):
            return [x]
        else:
            return x

    def _make_ace(self, action, subjects, permissions):
        return {'action' : action, 'subjects' : self._to_list(subjects), 'permissions' : self._to_list(permissions)}

    def _test_denying_acl(self, rw_path, rw_user, acl_path, acl_subject):
        set(rw_path, 'b', user=rw_user)
        assert get(rw_path, user=rw_user) == 'b'

        set(rw_path, 'c', user=rw_user)
        assert get(rw_path, user=rw_user) == 'c'

        set(acl_path + '/@acl/end', self._make_ace('deny', acl_subject, 'write'))
        with pytest.raises(YTError): set(rw_path, 'd', user=rw_user)
        assert get(rw_path, user=rw_user) == 'c'

        remove(acl_path + '/@acl/-1')
        set(acl_path + '/@acl/end', self._make_ace('deny', acl_subject, ['read', 'write']))
        with pytest.raises(YTError): get(rw_path, user=rw_user)
        with pytest.raises(YTError): set(rw_path, 'd', user=rw_user)

    def test_denying_acl1(self):
        create_user('u')
        self._test_denying_acl('//tmp/a', 'u', '//tmp/a', 'u')

    def test_denying_acl2(self):
        create_user('u')
        create_group('g')
        add_member('u', 'g')
        self._test_denying_acl('//tmp/a', 'u', '//tmp/a', 'g')

    def test_denying_acl3(self):
        create_user('u')
        set('//tmp/p', {})
        self._test_denying_acl('//tmp/p/a', 'u', '//tmp/p', 'u')

    def _test_allowing_acl(self, rw_path, rw_user, acl_path, acl_subject):
        set(rw_path, 'a')

        with pytest.raises(YTError): set(rw_path, 'b', user=rw_user)

        set(acl_path + '/@acl/end', self._make_ace('allow', acl_subject, 'write'))
        set(rw_path, 'c', user=rw_user)

        remove(acl_path + '/@acl/-1')
        set(acl_path + '/@acl/end', self._make_ace('allow', acl_subject, 'read'))
        with pytest.raises(YTError): set(rw_path, 'd', user=rw_user)

    def test_allowing_acl1(self):
        self._test_allowing_acl('//tmp/a', 'guest', '//tmp/a', 'guest')

    def test_allowing_acl2(self):
        create_group('g')
        add_member('guest', 'g')
        self._test_allowing_acl('//tmp/a', 'guest', '//tmp/a', 'g')

    def test_allowing_acl3(self):
        set('//tmp/p', {})
        self._test_allowing_acl('//tmp/p/a', 'guest', '//tmp/p', 'guest')

    def test_schema_acl1(self):
        create_user('u')
        create('table', '//tmp/t1', user='u')
        set('//sys/schemas/table/@acl/end', self._make_ace('deny', 'u', 'create'))
        with pytest.raises(YTError): create('table', '//tmp/t2', user='u')

    def test_schema_acl2(self):
        create_user('u')
        start_transaction(user='u')
        set('//sys/schemas/transaction/@acl/end', self._make_ace('deny', 'u', 'create'))
        with pytest.raises(YTError): start_transaction(user='u')

    def test_user_destruction(self):
        old_acl = get('//tmp/@acl')

        create_user('u')
        set('//tmp/@acl/end', self._make_ace('deny', 'u', 'write'))

        remove_user('u')
        assert get('//tmp/@acl') == old_acl

    def test_group_destruction(self):
        old_acl = get('//tmp/@acl')

        create_group('g')
        set('//tmp/@acl/end', self._make_ace('deny', 'g', 'write'))

        remove_group('g')
        assert get('//tmp/@acl') == old_acl

    def test_account_acl(self):
        create_account('a')
        create_user('u')

        with pytest.raises(YTError): create('table', '//tmp/t', user='u', opt=['/attributes/account=a'])

        create('table', '//tmp/t', user='u')
        assert get('//tmp/t/@account') == 'tmp'

        with pytest.raises(YTError): set('//tmp/t/@account', 'a', user='u')

        set('//sys/accounts/a/@acl/end', self._make_ace('allow', 'u', 'use'))
        with pytest.raises(YTError): set('//tmp/t/@account', 'a', user='u')
        set('//tmp/@acl/end', self._make_ace('allow', 'u', 'administer'))
        set('//tmp/t/@account', 'a', user='u')
        assert get('//tmp/t/@account') == 'a'

    def _prepare_scheduler_test(self):
        create_user('u')
        create_account('a')

        create('table', '//tmp/t1')
        write('//tmp/t1', {'a' : 'b'})
        
        create('table', '//tmp/t2')

        # just a sanity check
        map(in_='//tmp/t1', out='//tmp/t2', command='cat', user='u')

    @pytest.mark.skipif("not sys.platform.startswith(\"linux\")")
    def test_scheduler_in_acl(self):
        self._prepare_scheduler_test()
        set('//tmp/t1/@acl/end', self._make_ace('deny', 'u', 'read'))
        with pytest.raises(YTError): map(in_='//tmp/t1', out='//tmp/t2', command='cat', user='u')

    @pytest.mark.skipif("not sys.platform.startswith(\"linux\")")
    def test_scheduler_out_acl(self):
        self._prepare_scheduler_test()
        set('//tmp/t2/@acl/end', self._make_ace('deny', 'u', 'write'))
        with pytest.raises(YTError): map(in_='//tmp/t1', out='//tmp/t2', command='cat', user='u')

    @pytest.mark.skipif("not sys.platform.startswith(\"linux\")")
    def test_scheduler_account_quota(self):
        self._prepare_scheduler_test()
        set('//tmp/t2/@account', 'a')
        set('//sys/accounts/a/@acl/end', self._make_ace('allow', 'u', 'use'))
        # account "a" still has zero disk space limit
        with pytest.raises(YTError): map(in_='//tmp/t1', out='//tmp/t2', command='cat', user='u')

    def test_inherit1(self):
        set('//tmp/p', {})
        set('//tmp/p/@inherit_acl', 'false')
        
        create_user('u')
        with pytest.raises(YTError): set('//tmp/p/a', 'b', user='u')
        with pytest.raises(YTError): ls('//tmp/p', user='u')

        set('//tmp/p/@acl/end', self._make_ace('allow', 'u', ['read', 'write']))
        set('//tmp/p/a', 'b', user='u')
        self.assertItemsEqual(ls('//tmp/p', user='u'), ['a'])
        assert get('//tmp/p/a', user='u') == 'b'

    def test_create_in_tx1(self):
        create_user('u')
        tx = start_transaction()
        create('table', '//tmp/a', tx=tx, user='u')
        assert read('//tmp/a', tx=tx, user='u') == []

    def test_create_in_tx2(self):
        create_user('u')
        tx = start_transaction()
        create('table', '//tmp/a/b/c', '--recursive', tx=tx, user='u')
        assert read('//tmp/a/b/c', tx=tx, user='u') == []

    @pytest.mark.xfail(run = False, reason = 'In progress')
    def test_snapshot_remove(self):
        set('//tmp/a', {'b' : {'c' : 'd'}})
        path = '#' + get('//tmp/a/b/c/@id')
        create_user('u')
        assert get(path, user='u') == 'd'
        tx = start_transaction()
        lock(path, mode='snapshot', tx=tx)
        assert get(path, user='u', tx=tx) == 'd'
        remove('//tmp/a')
        assert get(path, user='u', tx=tx) == 'd'

    @pytest.mark.xfail(run = False, reason = 'In progress')
    def test_snapshot_no_inherit(self):
        set('//tmp/a', 'b')
        assert get('//tmp/a/@inherit_acl') == 'true'
        tx = start_transaction()
        lock('//tmp/a', mode='snapshot', tx=tx)
        assert get('//tmp/a/@inherit_acl', tx=tx) == 'false'

    def test_administer_permission1(self):
        create_user('u')
        create('table', '//tmp/t')
        with pytest.raises(YTError): set('//tmp/t/@acl', [], user='u')

    def test_administer_permission2(self):
        create_user('u')
        create('table', '//tmp/t')
        set('//tmp/@acl/end', self._make_ace('allow', 'u', 'administer'))
        set('//tmp/t/@acl', [], user='u')

    def test_user_rename_success(self):
        create_user('u1')
        set('//sys/users/u1/@name', 'u2')
        assert get('//sys/users/u2/@name') == 'u2'

    def test_user_rename_fail(self):
        create_user('u1')
        create_user('u2')
        with pytest.raises(YTError): set('//sys/users/u1/@name', 'u2')

    def test_deny_create(self):
        create_user('u')
        with pytest.raises(YTError): create('account_map', '//tmp/accounts', user='u')

    def test_deny_copy(self):
        create_user('u')
        with pytest.raises(YTError): copy('//sys', '//tmp/sys', user='u')

    def test_document1(self):
        create_user('u')
        create('document', '//tmp/d')
        set('//tmp/d', {"foo":{}})
        set('//tmp/d/@inherit_acl', 'false')

        assert get_str('//tmp', user='u') == '{"d"=#}'
        with pytest.raises(YTError): get('//tmp/d', user='u') == {'foo': {}}
        with pytest.raises(YTError): get('//tmp/d/@value', user='u')
        with pytest.raises(YTError): get('//tmp/d/foo', user='u')
        with pytest.raises(YTError): set('//tmp/d/foo', {}, user='u')
        with pytest.raises(YTError): set('//tmp/d/@value', {}, user='u')
        with pytest.raises(YTError): set('//tmp/d', {'foo':{}}, user='u')
        assert ls('//tmp', user='u') == ['d']
        with pytest.raises(YTError): ls('//tmp/d', user='u')
        with pytest.raises(YTError): ls('//tmp/d/foo', user='u')
        assert exists('//tmp/d', user='u')
        with pytest.raises(YTError): exists('//tmp/d/@value', user='u')
        with pytest.raises(YTError): exists('//tmp/d/foo', user='u')
        with pytest.raises(YTError): remove('//tmp/d/foo', user='u')
        with pytest.raises(YTError): remove('//tmp/d', user='u')

    def test_document2(self):
        create_user('u')
        create('document', '//tmp/d')
        set('//tmp/d', {"foo":{}})
        set('//tmp/d/@inherit_acl', 'false')
        set('//tmp/d/@acl/end', self._make_ace('allow', 'u', 'read'))

        assert get_str('//tmp', user='u') == '{"d"=#}'
        assert get('//tmp/d', user='u') == {'foo': {}}
        assert get('//tmp/d/@value', user='u') == {'foo': {}}
        assert get('//tmp/d/foo', user='u') == {}
        with pytest.raises(YTError): set('//tmp/d/foo', {}, user='u')
        with pytest.raises(YTError): set('//tmp/d/@value', {}, user='u')
        with pytest.raises(YTError): set('//tmp/d', {'foo':{}}, user='u')
        assert ls('//tmp', user='u') == ['d']
        assert ls('//tmp/d', user='u') == ['foo']
        assert ls('//tmp/d/foo', user='u') == []
        assert exists('//tmp/d', user='u')
        assert exists('//tmp/d/@value', user='u')
        assert exists('//tmp/d/foo', user='u')
        with pytest.raises(YTError): remove('//tmp/d/foo', user='u')
        with pytest.raises(YTError): remove('//tmp/d', user='u')

    def test_document3(self):
        create_user('u')
        create('document', '//tmp/d')
        set('//tmp/d', {"foo":{}})
        set('//tmp/d/@inherit_acl', 'false')
        set('//tmp/d/@acl/end', self._make_ace('allow', 'u', ['read', 'write']))

        assert get_str('//tmp', user='u') == '{"d"=#}'
        assert get('//tmp/d', user='u') == {'foo': {}}
        assert get('//tmp/d/@value', user='u') == {'foo': {}}
        assert get('//tmp/d/foo', user='u') == {}
        set('//tmp/d/foo', {}, user='u')
        set('//tmp/d/@value', {}, user='u')
        set('//tmp/d', {'foo':{}}, user='u')
        assert ls('//tmp', user='u') == ['d']
        assert ls('//tmp/d', user='u') == ['foo']
        assert ls('//tmp/d/foo', user='u') == []
        assert exists('//tmp/d', user='u')
        assert exists('//tmp/d/@value', user='u')
        assert exists('//tmp/d/foo', user='u')
        remove('//tmp/d/foo', user='u')
        remove('//tmp/d', user='u')
