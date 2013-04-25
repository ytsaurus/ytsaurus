import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import time


##################################################################

class TestErasure(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 32

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

    def test_repair(self):
        for codec, replicas_count, data_replicas_count in [("reed_solomon", 9, 6), ("lrc", 16, 12)]:
            remove('//tmp/table', '--force')
            create('table', '//tmp/table')
            write_str('//tmp/table', '{b="hello"}', config_opt="/table_writer/erasure_codec=" + codec)

            chunks = get("//tmp/table/@chunk_ids")
            assert len(chunks) == 1

            replicas = get("//sys/chunks/%s/@stored_replicas" % chunks[0])
            assert len(replicas) == replicas_count

            assert len(get("//sys/data_missing_chunks")) == 0
            assert len(get("//sys/parity_missing_chunks")) == 0
            for r in replicas:
                index = r.attributes["index"]
                node_index = (int(r.rsplit(":", 1)[1]) - self.Env._ports["node"]) / 2
                print "NODE_INDEX", node_index, "INDEX", index
                for p, name in self.Env.process_to_kill:
                    if name == "node-%d" % node_index:
                        self.kill_process(p, name)
               
                # It is slightly larger than chunk_refresh_delay and online_node_timeout
                time.sleep(0.8)
                if index < data_replicas_count:
                    assert len(get("//sys/data_missing_chunks")) == 1
                else:
                    assert len(get("//sys/parity_missing_chunks")) == 1

                ok = False
                for i in xrange(10):
                    if len(get("//sys/data_missing_chunks")) == 0 and len(get("//sys/parity_missing_chunks")) == 0:
                        ok = True
                        break
                    time.sleep(1.0)

                assert ok
                assert read('//tmp/table') == [{"b":"hello"}]

