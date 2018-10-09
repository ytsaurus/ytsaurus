import pytest

import time

from yt_env_setup import YTEnvSetup, wait
from yt_commands import *
from yt_driver_bindings import BufferedStream
from yt.common import YtResponseError
import yt.yson as yson

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
        status =  get("#%s/@replication_status/default" % chunk_id)
        if status["lost"]:
            return False
        if status["data_missing"]:
            return False
        if status["parity_missing"]:
            return False
        if not get("#%s/@available" % chunk_id):
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
            address = str(r)
            print >>sys.stderr, "Banning node %s containing replica %d" % (address, replica_index)
            set_node_banned(address, True)
            wait(lambda: self._is_chunk_ok(chunk_id))
            assert read_table("//tmp/table") == [{"b":"hello"}]
            set_node_banned(r, False)

    def _get_blocks_count(self, chunk_id, replica, replica_index):
        address = str(replica)
        parts = [int(s, 16) for s in chunk_id.split("-")]
        parts[2] = (parts[2] / 2 ** 16) * (2 ** 16) + 103 + replica_index
        node_chunk_id = "-".join(hex(i)[2:] for i in parts)
        return get("//sys/nodes/{0}/orchid/stored_chunks/{1}".format(address, node_chunk_id))["block_count"]

    def _prepare_table(self):
        for node in ls("//sys/nodes"):
            set("//sys/nodes/{0}/@resource_limits_overrides".format(node), {"repair_slots": 0})

        remove("//tmp/table", force=True)
        create("table", "//tmp/table", attributes={"erasure_codec": "lrc_12_2_2"})
        content = [{"a": "x" * 1024} for _ in xrange(12)]
        write_table("//tmp/table",
                    content,
                    table_writer={"block_size": 1024})

        # check if there is 1 chunk exactly
        chunk_ids = get("//tmp/table/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        # check if there is exactly one block in each part
        replicas = get("#{0}/@stored_replicas".format(chunk_id))
        assert len(replicas) == 16
        for index, replica in enumerate(replicas[:12]):
            blocks_count = self._get_blocks_count(chunk_id, replica, index)
            assert blocks_count == 1

        return replicas, content

    def _test_fetching_specs(self, chunk_strategy):
        replicas, _ = self._prepare_table()
        replica = replicas[3]
        address_to_ban = str(replica)
        set_node_banned(address_to_ban, True)
        time.sleep(1)

        has_failed = None

        try:
            read_table("//tmp/table",
                       table_reader={
                           "unavailable_chunk_strategy": chunk_strategy,
                           "pass_count": 1,
                           "retry_count": 1,
                       })
        except YtResponseError:
            has_failed = True
        else:
            has_failed = False
        finally:
            set_node_banned(address_to_ban, False)

        return has_failed

    def test_slow_read(self):
        replicas, _ = self._prepare_table()
            
        correct_data = read_table("//tmp/table")

        set("//sys/@config/chunk_manager/enable_chunk_replicator", False, recursive=True)
        wait(lambda: not get("//sys/@chunk_replicator_enabled"))

        try:
            replica = replicas[3]
            address_to_ban = str(replica)
            set_node_banned(address_to_ban, True)
            time.sleep(1)

            start = time.time()
            data = read_table("//tmp/table",
                       table_reader={
                           "block_rpc_timeout": 1000,
                           "meta_rpc_timeout": 1000,
                           # So much passes will take more than 1 seconds and we detect that reader is slow.
                           "pass_count": 100,
                           "replication_reader_timeout": 1000
                       },
                       verbose=False)
            end = time.time()
            time_passed = end - start
            set_node_banned(address_to_ban, False)
            assert time_passed <= 10
            assert data == correct_data
        finally:
            set("//sys/@config/chunk_manager/enable_chunk_replicator", True, recursive=True)
            wait(lambda: get("//sys/@chunk_replicator_enabled"))

    def test_throw_error(self):
        has_failed = self._test_fetching_specs("throw_error")
        assert has_failed, "Expected to fail due to unavailable chunk specs"

    def test_repair_works(self):
        has_failed = self._test_fetching_specs("restore")
        assert has_failed == False, "Expected successful read"

    def _test_repair_on_spot(self, allow_repair):
        replicas, content = self._prepare_table()

        replica = replicas[3]
        window_size = 1024
        output_stream = BufferedStream(size=window_size)
        response = read_table("//tmp/table",
                              table_reader={
                                  "window_size": window_size,
                                  "group_size": window_size,
                                  "pass_count": 1,
                                  "retry_count": 1,
                                  "enable_auto_repair": allow_repair
                              },
                              output_stream=output_stream,
                              return_response=True)

        full_output = output_stream.read(window_size)

        address_to_ban = str(replica)
        set_node_banned(address_to_ban, True)

        time.sleep(1)

        while True:
            bytes = output_stream.read(window_size)
            if not bytes:
                break
            full_output += bytes

        response.wait()
        set_node_banned(address_to_ban, False)

        if allow_repair:
            if not response.is_ok():
                error = YtResponseError(response.error())
                assert False, str(error)
            test_output = list(yson.loads(full_output, yson_type="list_fragment"))
            assert test_output == content
        else:
            assert not response.is_ok(), "Read finished successfully, but expected to fail (due to unavailable part and disabled repairing)"

    def test_repair_on_spot_successful(self):
        self._test_repair_on_spot(True)

    def test_repair_on_spot_failed(self):
        self._test_repair_on_spot(False)

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

    def test_slice_erasure_chunks_by_parts(self):
        create("table", "//tmp/t1")
        set("//tmp/t1/@erasure_codec", "lrc_12_2_2")
        write_table("//tmp/t1", [{"a": "b"}] * 240)
        create("table", "//tmp/t2")

        op1 = map(in_="//tmp/t1", out="//tmp/t2", command="cat", spec={"slice_erasure_chunks_by_parts": True})
        chunk_count = get(op1.get_path() + "/@progress/data_flow_graph/edges/source/map/statistics/chunk_count")
        assert chunk_count == 12

        op2 = map(in_="//tmp/t1", out="//tmp/t2", command="cat", spec={"slice_erasure_chunks_by_parts": False})
        chunk_count = get(op2.get_path() + "/@progress/data_flow_graph/edges/source/map/statistics/chunk_count")
        assert chunk_count == 1

    def test_erasure_attribute_in_output_table(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"a": "b"})

        create("table", "//tmp/t2")
        map(in_="//tmp/t1", out="<erasure_codec=lrc_12_2_2>//tmp/t2", command="cat")
        assert get("//tmp/t2/@erasure_codec") == "lrc_12_2_2"

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

    def test_write_table_with_erasure(self):
        create("table", "//tmp/table")

        write_table("<erasure_codec=none>//tmp/table", [{"key": 0}])
        assert "none" == get("//tmp/table/@erasure_codec")

        write_table("<erasure_codec=lrc_12_2_2>//tmp/table", [{"key": 0}])
        assert "lrc_12_2_2" == get("//tmp/table/@erasure_codec")

        with pytest.raises(YtError):
            write_table("<append=true;erasure_codec=lrc_12_2_2>//tmp/table", [{"key": 0}])

    def test_write_file_with_erasure(self):
        create("file", "//tmp/f")

        write_file("<erasure_codec=lrc_12_2_2>//tmp/f", "a")
        assert get("//tmp/f/@erasure_codec") == "lrc_12_2_2"

        write_file("<erasure_codec=none>//tmp/f", "a")
        assert get("//tmp/f/@erasure_codec") == "none"

        with pytest.raises(YtError):
            write_file("<append=true;compression_codec=none>//tmp/f", "a")

##################################################################

class TestErasureMulticell(TestErasure):
    NUM_SECONDARY_MASTER_CELLS = 2

