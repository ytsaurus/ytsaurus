import pytest
import time
import datetime

from yt_env_setup import YTEnvSetup
from yt_commands import *
from yt.yson import to_yson_type


##################################################################

class TestRff(YTEnvSetup):
    NUM_MASTERS = 5
    NUM_NONVOTING_MASTERS = 2
    NUM_NODES = 3
    DELTA_MASTER_CONFIG = {
        "hydra" : {
            "max_commit_batch_delay" : 1000
        }
    }

    def test_plain_read_table(self):
        set('//tmp/x', 123)
        for i in xrange(100):
            assert get("//tmp/x", read_from="follower") == 123

    def test_sync(self):
        for i in xrange(100):
            set('//tmp/x', i)
            assert get("//tmp/x", read_from="follower") == i

    def test_access_stat(self):
        time.sleep(1.0)
        c1 = get("//tmp/@access_counter")
        for i in xrange(100):
            assert ls('//tmp', read_from="follower") == []
        time.sleep(1.0)
        c2 = get("//tmp/@access_counter")
        assert c2 == c1 + 100

    def test_request_stat(self):
        create_user("u")
        assert get("//sys/users/u/@request_count") == 0
        for i in xrange(100):
            ls("//tmp", authenticated_user="u", read_from="follower")
        time.sleep(1.0)
        assert get("//sys/users/u/@request_count") == 100

    def test_leader_fallback(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        assert ls("//sys/lost_vital_chunks", read_from="follower") == []

        assert all(not c.attributes["replication_status"]["default"]["overreplicated"]
                   for c in ls("//sys/chunks", attributes=["replication_status"], read_from="follower"))

        assert all(n.attributes["state"] == "online"
                   for n in ls("//sys/nodes", attributes=["state"], read_from="follower"))

        assert get("//sys/@chunk_replicator_enabled", read_from="follower")

        tx = start_transaction()
        last_ping_time = datetime.strptime(get("#" + tx + "/@last_ping_time", read_from="follower"), "%Y-%m-%dT%H:%M:%S.%fZ")
        now = datetime.utcnow()
        assert last_ping_time < now
        assert (now - last_ping_time).total_seconds() < 3

##################################################################

class TestRffMulticell(TestRff):
    NUM_SECONDARY_MASTER_CELLS = 2
