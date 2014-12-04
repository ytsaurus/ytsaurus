from yt_env_setup import YTEnvSetup
from yt_commands import *
from time import sleep

##################################################################

class TestNodeTracker(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    def test_ban(self):
        nodes = ls('//sys/nodes')
        assert len(nodes) == 3

        test_node = nodes[0]
        assert get('//sys/nodes/%s/@state' % test_node) == 'online'

        set('//sys/nodes/%s/@banned' % test_node, True)
        sleep(1)
        assert get('//sys/nodes/%s/@state' % test_node) == 'offline'

        set('//sys/nodes/%s/@banned' % test_node, False)
        sleep(1)
        assert get('//sys/nodes/%s/@state' % test_node) == 'online'

    def test_node_attr(self): #regression
        ls('//sys/nodes', attr=['statistics'])
