import pytest
import time

from yt_env_setup import YTEnvSetup
from yt_commands import *
from yt.yson import to_yson_type


##################################################################

class TestRff(YTEnvSetup):
    NUM_MASTERS = 5
    NUM_NODES = 0
    DELTA_MASTER_CONFIG = {
        "hydra" : {
            "max_commit_batch_delay" : 1000
        }
    }

    def test_plain_read(self):
        set('//tmp/x', 123)
        for i in xrange(100):
            assert get("//tmp/x", read_from="follower") == 123

    def test_sync(self):
        for i in xrange(100):
            set('//tmp/x', i)
            assert get("//tmp/x", read_from="follower") == i

    def test_leader_forwarding(self):
        for i in xrange(100):
            assert not get("//sys/nodes/@chunk_replicator_enabled", read_from="follower")


