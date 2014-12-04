import pytest
import sys

import yt.yson

from yt_env_setup import YTEnvSetup
from yt_commands import *
from time import sleep


##################################################################

class TestUsers(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0

    def test_user_ban1(self):
        create_user('u')

        assert not get('//sys/users/u/@banned')
        get('//tmp', user='u')

        set('//sys/users/u/@banned', True)
        assert get('//sys/users/u/@banned')
        with pytest.raises(YtError): get('//tmp', user='u')

        set('//sys/users/u/@banned', False)
        assert not get('//sys/users/u/@banned')
 
        get('//tmp', user='u')

    def test_user_ban2(self):
        with pytest.raises(YtError): set('//sys/users/root/@banned', True)

    def test_request_rate1(self):
        create_user('u')
        with pytest.raises(YtError): set('//sys/users/u/@request_rate_limit', -1.0)

    def test_request_rate2(self):
        create_user('u')
        set('//sys/users/u/@request_rate_limit', 1.0)

    def test_access_counter1(self):
        create_user('u')
        assert get('//sys/users/u/@request_counter') == 0

        ls('//tmp', user='u')
        sleep(1.0)
        assert get('//sys/users/u/@request_counter') == 1

    def test_builtin_init(self):
        self.assertItemsEqual(get('//sys/groups/everyone/@members'), ['users', 'guest'])
        self.assertItemsEqual(get('//sys/groups/users/@members'), ['superusers'])
        self.assertItemsEqual(get('//sys/groups/superusers/@members'), ['root'])

        self.assertItemsEqual(get('//sys/users/root/@member_of'), ['superusers'])
        self.assertItemsEqual(get('//sys/users/guest/@member_of'), ['everyone'])

        self.assertItemsEqual(get('//sys/users/root/@member_of_closure'), ['superusers', 'users', 'everyone'])
        self.assertItemsEqual(get('//sys/users/guest/@member_of_closure'), ['everyone'])

    def test_create_user1(self):
        create_user('max')
        assert get('//sys/users/max/@name') == 'max'
        assert 'max' in get('//sys/groups/users/@members')
        self.assertItemsEqual(get('//sys/users/max/@member_of'), ['users'])

    def test_create_user2(self):
        create_user('max')
        with pytest.raises(YtError): create_user('max')
        with pytest.raises(YtError): create_group('max')

    def test_create_group1(self):
        create_group('devs')
        assert get('//sys/groups/devs/@name') == 'devs'

    def test_create_group2(self):
        create_group('devs')
        with pytest.raises(YtError): create_user('devs')
        with pytest.raises(YtError): create_group('devs')

    def test_user_remove_builtin(self):
        with pytest.raises(YtError): remove_user('root')
        with pytest.raises(YtError): remove_user('guest')

    def test_group_remove_builtin(self):
        with pytest.raises(YtError): remove_group('everyone')
        with pytest.raises(YtError): remove_group('users')

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

        self.assertItemsEqual(get('//sys/users/u1/@member_of'), ['g1', 'users'])
        self.assertItemsEqual(get('//sys/users/u2/@member_of'), ['g2', 'users'])

        self.assertItemsEqual(get('//sys/users/u1/@member_of_closure'), ['g1', 'users', 'everyone'])
        self.assertItemsEqual(get('//sys/users/u2/@member_of_closure'), ['g1', 'g2', 'users', 'everyone'])

        remove_member('g2', 'g1')

        self.assertItemsEqual(get('//sys/groups/g1/@members'), ['u1'])
        self.assertItemsEqual(get('//sys/groups/g2/@members'), ['u2'])

        self.assertItemsEqual(get('//sys/users/u1/@member_of'), ['g1', 'users'])
        self.assertItemsEqual(get('//sys/users/u2/@member_of'), ['g2', 'users'])

        self.assertItemsEqual(get('//sys/users/u1/@member_of_closure'), ['g1', 'users', 'everyone'])
        self.assertItemsEqual(get('//sys/users/u2/@member_of_closure'), ['g2', 'users', 'everyone'])

    def test_membership3(self):
        create_group('g1')
        create_group('g2')
        create_group('g3')

        add_member('g2', 'g1')
        add_member('g3', 'g2')
        with pytest.raises(YtError): add_member('g1', 'g3')

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
        self.assertItemsEqual(get('//sys/users/u/@member_of'), ['g', 'users'])
        remove_group('g')
        self.assertItemsEqual(get('//sys/users/u/@member_of'), ['users'])

    def test_membership6(self):
        create_user('u')
        create_group('g')
        
        with pytest.raises(YtError): remove_member('u', 'g')

        add_member('u', 'g')
        with pytest.raises(YtError): add_member('u', 'g')

    def test_modify_builtin(self):
        create_user('u')
        with pytest.raises(YtError): remove_member('u', 'everyone')
        with pytest.raises(YtError): remove_member('u', 'users')
        with pytest.raises(YtError): add_member('u', 'everyone')
        with pytest.raises(YtError): add_member('u', 'users')

