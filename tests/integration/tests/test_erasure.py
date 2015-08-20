from yt_env_setup import YTEnvSetup, mark_multicell
from yt_commands import *

import time


##################################################################

class TestErasure(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 20
    NUM_SCHEDULERS = 1

    def _do_test_simple(self, erasure_codec):
        create("table", "//tmp/table")
        set("//tmp/table/@erasure_codec", erasure_codec)

        assert read_table("//tmp/table") == []
        assert get("//tmp/table/@row_count") == 0
        assert get("//tmp/table/@chunk_count") == 0

        write_table("//tmp/table", {"b": "hello"})
        assert read_table("//tmp/table") == [{"b":"hello"}]
        assert get("//tmp/table/@row_count") == 1
        assert get("//tmp/table/@chunk_count") == 1

        write_table("<append=true>//tmp/table", [{"b": "2", "a": "1"}, {"x": "10", "y": "20", "a": "30"}])
        assert read_table("//tmp/table") == [{"b": "hello"}, {"a":"1", "b":"2"}, {"a":"30", "x":"10", "y":"20"}]
        assert get("//tmp/table/@row_count") == 3
        assert get("//tmp/table/@chunk_count") == 2

    def test_reed_solomon(self):
        self._do_test_simple("reed_solomon_6_3")

    def test_lrc(self):
        self._do_test_simple("lrc_12_2_2")

    def _is_chunk_ok(self, chunk_id):
        if get("#%s/@lost" % chunk_id):
            return False
        if not get("#%s/@available" % chunk_id):
            return False
        if get("#%s/@data_missing" % chunk_id):
            return False
        if get("#%s/@parity_missing" % chunk_id):
            return False
        return True

    def _test_repair(self, codec, replica_count, data_replica_count):
        remove("//tmp/table", force=True)
        create("table", "//tmp/table")
        set("//tmp/table/@erasure_codec", codec)
        write_table("//tmp/table", {"b": "hello"})

        chunk_ids = get("//tmp/table/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        replicas = get("#%s/@stored_replicas" % chunk_id)
        assert len(replicas) == replica_count

        assert self._is_chunk_ok(chunk_id)

        for r in replicas:
            replica_index = r.attributes["index"]
            port = int(r.rsplit(":", 1)[1])
            node_index = filter(lambda x: x == port, self.Env._ports["node"])[0]
            print "Banning node %d containing replica %d" % (node_index, replica_index)
            set("//sys/nodes/%s/@banned" % r, True)

            # Give it enough time to unregister the node
            time.sleep(1.0)
            assert get("//sys/nodes/%s/@state" % r) == "offline"

            ok = False
            for i in xrange(10):
                if self._is_chunk_ok(chunk_id):
                    ok = True
                    break
                time.sleep(0.2)

            assert ok
            assert read_table("//tmp/table") == [{"b":"hello"}]

            set("//sys/nodes/%s/@banned" % r, False)

    def test_reed_solomon_repair(self):
        self._test_repair("reed_solomon_6_3", 9, 6)

    def test_lrc_repair(self):
        self._test_repair("lrc_12_2_2", 16, 12)

    def test_map(self):
        create("table", "//tmp/t1")
        set("//tmp/t1/@erasure_codec", "reed_solomon_6_3")
        create("table", "//tmp/t2")
        set("//tmp/t2/@erasure_codec", "lrc_12_2_2")
        write_table("//tmp/t1", {"a": "b"})
        map(in_="//tmp/t1", out="//tmp/t2", command="cat")

        assert read_table("//tmp/t2") == [{"a" : "b"}]

    def test_sort(self):
        v1 = {"key" : "aaa"}
        v2 = {"key" : "bb"}
        v3 = {"key" : "bbxx"}
        v4 = {"key" : "zfoo"}
        v5 = {"key" : "zzz"}

        create("table", "//tmp/t_in")
        set("//tmp/t_in/@erasure_codec", "lrc_12_2_2")
        write_table("//tmp/t_in", [v3, v5, v1, v2, v4]) # some random order

        create("table", "//tmp/t_out")
        set("//tmp/t_in/@erasure_codec", "reed_solomon_6_3")

        sort(in_="//tmp/t_in",
             out="//tmp/t_out",
             sort_by="key")

        assert read_table("//tmp/t_out") == [v1, v2, v3, v4, v5]
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") ==  ["key"]

    def test_part_ids(self):
        create("table", "//tmp/t")
        set("//tmp/t/@erasure_codec", "lrc_12_2_2")
        write_table("//tmp/t", {"a": "b"})
        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        parts = chunk_id.split("-")
        for x in xrange(103, 119):
            part_id = "%s-%s-%s%x-%s" % (parts[0], parts[1], parts[2][:-2], x, parts[3])
            assert get("#" + part_id + "/@id") == chunk_id
            
##################################################################

@mark_multicell
class TestErasureMulticell(TestErasure):
    NUM_SECONDARY_MASTER_CELLS = 2

