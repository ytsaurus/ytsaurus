import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *


##################################################################

class TestAcls(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    START_SCHEDULER = True

    def test_init(self):
        self.assertItemsEqual(get('//sys/groups/everyone'), ['root', 'guest'])
        self.assertItemsEqual(get('//sys/groups/users'), ['root'])

    def test_create_user(self):
        create_user('max')
        assert get('//sys/users/max/@name') == 'max'
        assert 'max' in get('//sys/groups/everyone/@members')
        assert 'max' in get('//sys/groups/users/@members')
