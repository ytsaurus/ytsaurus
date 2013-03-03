import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *


##################################################################

class TestAcls(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    START_SCHEDULER = True

    def test_init(self):
        self.assertItemsEqual(get('//sys/groups/everyone/@members'), ['root', 'guest'])
        self.assertItemsEqual(get('//sys/groups/users/@members'), ['root'])

        self.assertItemsEqual(get('//sys/users/root/@member_of'), ['users', 'everyone'])
        self.assertItemsEqual(get('//sys/users/guest/@member_of'), ['everyone'])

        self.assertItemsEqual(get('//sys/users/root/@member_of_closure'), ['users', 'everyone'])
        self.assertItemsEqual(get('//sys/users/guest/@member_of_closure'), ['everyone'])

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

    def test_create_user1(self):
        create_user('max')
        assert get('//sys/users/max/@name') == 'max'
        assert 'max' in get('//sys/groups/everyone/@members')
        self.assertItemsEqual(get('//sys/users/max/@member_of'), ['users', 'everyone'])

    def test_create_user2(self):
        create_user('max')
        with pytest.raises(YTError): create_user('max')
        with pytest.raises(YTError): create_group('max')

    def test_create_group1(self):
        create_group('devs')
        assert get('//sys/groups/devs/@name') == 'devs'

    def test_create_group2(self):
        create_group('devs')
        with pytest.raises(YTError): create_user('devs')
        with pytest.raises(YTError): create_group('devs')

    def test_user_remove_builtin(self):
        with pytest.raises(YTError): remove_user('root')
        with pytest.raises(YTError): remove_user('guest')

    def test_group_remove_builtin(self):
        with pytest.raises(YTError): remove_group('everyone')
        with pytest.raises(YTError): remove_group('users')

    def test_membership1(self):
        create_user('max')
        create_group('devs')
        add_member('max', 'devs')
        assert get('//sys/groups/devs/@members') == ['max']
        self.assertItemsEqual(get('//sys/groups/devs/@members'), ['max'])

    def test_membership2(self):
        create_user('u1')
        create_user('u2')
        create_group('g1')
        create_group('g2')
        
        add_member('u1', 'g1')
        add_member('g2', 'g1')
        add_member('u2', 'g2')

        self.assertItemsEqual(get('//sys/groups/g1/@members'), ['u1', 'g2'])
        self.assertItemsEqual(get('//sys/groups/g2/@members'), ['u2'])

        self.assertItemsEqual(get('//sys/users/u1/@member_of'), ['g1', 'users', 'everyone'])
        self.assertItemsEqual(get('//sys/users/u2/@member_of'), ['g2', 'users', 'everyone'])

        self.assertItemsEqual(get('//sys/users/u1/@member_of_closure'), ['g1', 'users', 'everyone'])
        self.assertItemsEqual(get('//sys/users/u2/@member_of_closure'), ['g1', 'g2', 'users', 'everyone'])

        remove_member('g2', 'g1')

        self.assertItemsEqual(get('//sys/groups/g1/@members'), ['u1'])
        self.assertItemsEqual(get('//sys/groups/g2/@members'), ['u2'])

        self.assertItemsEqual(get('//sys/users/u1/@member_of'), ['g1', 'users', 'everyone'])
        self.assertItemsEqual(get('//sys/users/u2/@member_of'), ['g2', 'users', 'everyone'])

        self.assertItemsEqual(get('//sys/users/u1/@member_of_closure'), ['g1', 'users', 'everyone'])
        self.assertItemsEqual(get('//sys/users/u2/@member_of_closure'), ['g2', 'users', 'everyone'])

    def test_membership3(self):
        create_group('g1')
        create_group('g2')
        create_group('g3')

        add_member('g2', 'g1')
        add_member('g3', 'g2')
        with pytest.raises(YTError): add_member('g1', 'g3')

    def test_membership4(self):
        create_user('u')
        create_group('g')
        add_member('u', 'g')
        remove_user('u')
        assert get('//sys/groups/g/@members') == []

    def test_membership5(self):
        create_user('u')
        create_group('g')
        add_member('u', 'g')
        remove_group('g')
        self.assertItemsEqual(get('//sys/users/u/@member_of'), ['users', 'everyone'])

    def test_membership6(self):
        create_user('u')
        create_group('g')
        
        with pytest.raises(YTError): remove_member('u', 'g')

        add_member('u', 'g')
        with pytest.raises(YTError): add_member('u', 'g')

    def test_modify_builtin(self):
        create_user('u')
        with pytest.raises(YTError): remove_member('u', 'everyone')
        with pytest.raises(YTError): remove_member('u', 'users')
        with pytest.raises(YTError): add_member('u', 'everyone')
        with pytest.raises(YTError): add_member('u', 'users')

    def _to_list(self, x):
        if isinstance(x, str):
            return [x]
        else:
            return x

    def _make_ace(self, action, subjects, permissions):
        return {'action' : action, 'subjects' : self._to_list(subjects), 'permissions' : self._to_list(permissions)}

    def _test_acl_rw(self, rw_path, acl_path, acl_subject):
        set(rw_path, 'b', user='u')
        assert get(rw_path, user='u') == 'b'

        set(rw_path, 'c', user='u')
        assert get(rw_path, user='u') == 'c'

        set(acl_path + '/@acl/end', self._make_ace('deny', acl_subject, 'write'))
        with pytest.raises(YTError): set(rw_path, 'd', user='u')
        assert get(rw_path, user='u') == 'c'

        remove(acl_path + '/@acl/-1')
        set(acl_path + '/@acl/end', self._make_ace('deny', acl_subject, ['read', 'write']))
        with pytest.raises(YTError): get(rw_path, user='u')
        with pytest.raises(YTError): set(rw_path, 'd', user='u')

    def test_acl1(self):
        create_user('u')
        self._test_acl_rw('//tmp/a', '//tmp/a', 'u')

    def test_acl2(self):
        create_user('u')
        create_group('g')
        add_member('u', 'g')
        self._test_acl_rw('//tmp/a', '//tmp/a', 'g')

    def test_acl3(self):
        create_user('u')
        set('//tmp/p', {})
        self._test_acl_rw('//tmp/p/a', '//tmp/p', 'u')
