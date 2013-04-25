import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import time


##################################################################

class TestErasure(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 20

    def _do_test_simple(self, option):
        create('table', '//tmp/table')

        assert read('//tmp/table') == []
        assert get('//tmp/table/@row_count') == 0
        assert get('//tmp/table/@chunk_count') == 0

        write_str('//tmp/table', '{b="hello"}', config_opt=option)
        assert read('//tmp/table') == [{"b":"hello"}]
        assert get('//tmp/table/@row_count') == 1
        assert get('//tmp/table/@chunk_count') == 1

        write_str(
            '<append=true>//tmp/table', '{b="2";a="1"};{x="10";y="20";a="30"}',
            config_opt=option)
        assert read('//tmp/table') == [{"b": "hello"}, {"a":"1", "b":"2"}, {"a":"30", "x":"10", "y":"20"}]
        assert get('//tmp/table/@row_count') == 3
        assert get('//tmp/table/@chunk_count') == 2

    def test_reed_solomon(self):
        self._do_test_simple('/table_writer/erasure_codec=reed_solomon')

    def test_lrc(self):
        self._do_test_simple('/table_writer/erasure_codec=lrc')

    def _is_chunk_ok(self, chunk_id):
        if get("#%s/@lost" % chunk_id) != "false":
            return False
        if get("#%s/@available" % chunk_id) != "true":
            return False
        if get("#%s/@data_missing" % chunk_id) != "false":
            return False
        if get("#%s/@parity_missing" % chunk_id) != "false":
            return False
        return True

    def _test_repair(self, codec, replica_count, data_replica_count):
        remove('//tmp/table', '--force')
        create('table', '//tmp/table')
        write_str('//tmp/table', '{b="hello"}', config_opt="/table_writer/erasure_codec=" + codec)

        chunk_ids = get("//tmp/table/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        replicas = get("#%s/@stored_replicas" % chunk_id)
        assert len(replicas) == replica_count

        assert self._is_chunk_ok(chunk_id)

        for r in replicas:
            replica_index = r.attributes["index"]
            node_index = (int(r.rsplit(":", 1)[1]) - self.Env._ports["node"]) / 2
            print "Banning node %d containing replica %d" % (node_index, replica_index)
            set("//sys/nodes/%s/@banned" % r, "true")

            # Give it enough time to unregister the node
            time.sleep(1.0)
            assert get("//sys/nodes/%s/@state" % r) == "offline"

            ok = False
            for i in xrange(10):
                if self._is_chunk_ok(chunk_id):
                    ok = True
                    break
                time.sleep(1.0)

            assert ok
            assert read('//tmp/table') == [{"b":"hello"}]

            set("//sys/nodes/%s/@banned" % r, "false")

    def test_reed_solomon_repair(self):
        self._test_repair("reed_solomon", 9, 6)

    def test_lrc_repair(self):
        self._test_repair("lrc", 16, 12)
