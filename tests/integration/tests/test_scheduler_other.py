
import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *
import time


##################################################################

class TestSchedulerOther(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "chunk_scratch_period" : 500
        }
    }

    DELTA_NODE_CONFIG = {
        'exec_agent' : {
            'slot_manager' : {
                'enable_cgroups' : 'false'
            },
        }
    }
    def _set_banned_flag(self, value):
        if value:
            flag = 'true'
            state = 'offline'
        else:
            flag = 'false'
            state = 'online'

        nodes = get("//sys/nodes")
        assert len(nodes) == 1
        address = nodes.keys()[0]
        set("//sys/nodes/%s/@banned" % address, flag)

        # Give it enough time to register or unregister the node
        time.sleep(1.0)
        assert get("//sys/nodes/%s/@state" % address) == state
        print 'Node is %s' % state

    def _prepare_tables(self):
        create('table', '//tmp/t_in')
        set('//tmp/t_in/@replication_factor', 1)
        write('//tmp/t_in', {"foo": "bar"})

        create('table', '//tmp/t_out')
        set('//tmp/t_out/@replication_factor', 1)

    def test_strategies(self):
        self._prepare_tables()
        self._set_banned_flag(True)

        print 'Fail strategy'
        with pytest.raises(YtError):
            op_id = map(dont_track=True, in_='//tmp/t_in', out='//tmp/t_out', command='cat', opt=['/spec/unavailable_chunk_strategy=fail'])
            track_op(op_id)

        print 'Skip strategy'
        map(in_='//tmp/t_in', out='//tmp/t_out', command='cat', opt=['/spec/unavailable_chunk_strategy=skip'])
        assert read('//tmp/t_out') == []

        print 'Wait strategy'
        op_id = map(dont_track=True, in_='//tmp/t_in', out='//tmp/t_out', command='cat',  opt=['/spec/unavailable_chunk_strategy=wait'])

        self._set_banned_flag(False)
        track_op(op_id)

        assert read('//tmp/t_out') == [ {'foo' : 'bar'} ]

    def test_revive(self):
        self._prepare_tables()

        op_id = map(dont_track=True, in_='//tmp/t_in', out='//tmp/t_out', command='cat; sleep 3')

        time.sleep(2)
        self.Env._kill_service("scheduler")
        self.Env.start_schedulers("scheduler")

        track_op(op_id)

        assert read('//tmp/t_out') == [ {'foo' : 'bar'} ]


