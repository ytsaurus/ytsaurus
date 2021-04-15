import pytest

import time

from datetime import datetime
from datetime import timedelta
from yt_env_setup import YTEnvSetup, wait
from yt_commands import *
from yt_driver_bindings import BufferedStream
from yt.common import YtResponseError
import yt.yson as yson

##################################################################


class TestErasureBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 20
    NUM_SCHEDULERS = 1

    def _is_chunk_ok(self, chunk_id):
        status = get("#%s/@replication_status/default" % chunk_id)
        if status["lost"]:
            return False
        if status["data_missing"]:
            return False
        if status["parity_missing"]:
            return False
        if not get("#%s/@available" % chunk_id):
            return False
        return True

    def _get_blocks_count(self, chunk_id, replica, replica_index):
        address = str(replica)
        parts = [int(s, 16) for s in chunk_id.split("-")]
        parts[2] = (parts[2] / 2 ** 16) * (2 ** 16) + 103 + replica_index
        node_chunk_id = "-".join(hex(i)[2:] for i in parts)
        return get("//sys/cluster_nodes/{0}/orchid/stored_chunks/{1}".format(address, node_chunk_id))["block_count"]

    def _prepare_table(self, erasure_codec, dynamic=False):
        for node in ls("//sys/cluster_nodes"):
            set(
                "//sys/cluster_nodes/{0}/@resource_limits_overrides".format(node),
                {"repair_slots": 0},
            )

        remove("//tmp/table", force=True)
        if not dynamic:
            create("table", "//tmp/table", attributes={"erasure_codec": erasure_codec})
        else:
            schema = [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"}]
            create_dynamic_table(
                "//tmp/table",
                schema=schema,
                chunk_writer={"block_size": 1024},
                erasure_codec=erasure_codec,
                compression_codec="none")
            sync_mount_table("//tmp/table")

        content = [{"key": i, "value": "x" * 1024} for i in xrange(12)]

        if not dynamic:
            write_table("//tmp/table",
                        content,
                        table_writer={"block_size": 1024})
        else:
            insert_rows("//tmp/table", content)
            sync_flush_table("//tmp/table")

        # check if there is 1 chunk exactly
        chunk_id = get_singular_chunk_id("//tmp/table")

        # check if there is exactly one block in each part
        replicas = get("#{0}/@stored_replicas".format(chunk_id))
        assert len(replicas) == 16
        for index, replica in enumerate(replicas[:12]):
            blocks_count = self._get_blocks_count(chunk_id, replica, index)
            assert blocks_count == 1

        return replicas, content

    def _test_fetching_specs(self, chunk_strategy, erasure_codec):
        replicas, _ = self._prepare_table(erasure_codec)
        replica = replicas[3]
        address_to_ban = str(replica)
        set_node_banned(address_to_ban, True)
        time.sleep(1)

        has_failed = None

        try:
            read_table(
                "//tmp/table",
                table_reader={
                    "unavailable_chunk_strategy": chunk_strategy,
                    "pass_count": 1,
                    "retry_count": 1,
                },
            )
        except YtResponseError:
            has_failed = True
        else:
            has_failed = False
        finally:
            set_node_banned(address_to_ban, False)

        return has_failed


class TestErasure(TestErasureBase):
    NUM_TEST_PARTITIONS = 2

    def _do_test_simple(self, erasure_codec):
        create("table", "//tmp/table")
        set("//tmp/table/@erasure_codec", erasure_codec)

        assert read_table("//tmp/table") == []
        assert get("//tmp/table/@row_count") == 0
        assert get("//tmp/table/@chunk_count") == 0

        write_table("//tmp/table", {"b": "hello"})
        assert read_table("//tmp/table") == [{"b": "hello"}]
        assert get("//tmp/table/@row_count") == 1
        assert get("//tmp/table/@chunk_count") == 1

        write_table(
            "<append=true>//tmp/table",
            [{"b": "2", "a": "1"}, {"x": "10", "y": "20", "a": "30"}],
        )
        assert read_table("//tmp/table") == [
            {"b": "hello"},
            {"a": "1", "b": "2"},
            {"a": "30", "x": "10", "y": "20"},
        ]
        assert get("//tmp/table/@row_count") == 3
        assert get("//tmp/table/@chunk_count") == 2

    @authors("psushin", "ignat", "akozhikhov")
    @pytest.mark.parametrize("erasure_codec", ["isa_lrc_12_2_2", "lrc_12_2_2", "reed_solomon_6_3", "reed_solomon_3_3", "isa_reed_solomon_6_3"])
    def test_codecs_simple(self, erasure_codec):
        self._do_test_simple(erasure_codec)

    @authors("akozhikhov")
    @pytest.mark.parametrize("erasure_codec", ["isa_lrc_12_2_2", "lrc_12_2_2"])
    def test_slow_read(self, erasure_codec):
        replicas, _ = self._prepare_table(erasure_codec)

        correct_data = read_table("//tmp/table")

        set("//sys/@config/chunk_manager/enable_chunk_replicator", False, recursive=True)
        wait(lambda: not get("//sys/@chunk_replicator_enabled"))

        try:
            replica = replicas[3]
            address_to_ban = str(replica)
            set_node_banned(address_to_ban, True)
            time.sleep(1)

            start = time.time()
            data = read_table(
                "//tmp/table",
                table_reader={
                    "block_rpc_timeout": 1000,
                    "meta_rpc_timeout": 1000,
                    # So much passes will take more than 1 seconds and we detect that reader is slow.
                    "pass_count": 100,
                    "replication_reader_timeout": 1000,
                },
                verbose=False,
            )
            end = time.time()
            time_passed = end - start
            set_node_banned(address_to_ban, False)
            assert time_passed <= 10
            assert data == correct_data
        finally:
            set(
                "//sys/@config/chunk_manager/enable_chunk_replicator",
                True,
                recursive=True,
            )
            wait(lambda: get("//sys/@chunk_replicator_enabled"))

    @authors("akozhikhov")
    @pytest.mark.parametrize("erasure_codec", ["isa_lrc_12_2_2", "lrc_12_2_2"])
    def test_throw_error(self, erasure_codec):
        has_failed = self._test_fetching_specs("throw_error", erasure_codec)
        assert has_failed, "Expected to fail due to unavailable chunk specs"

    @authors("akozhikhov")
    @pytest.mark.parametrize("erasure_codec", ["isa_lrc_12_2_2", "lrc_12_2_2"])
    def test_repair_works(self, erasure_codec):
        has_failed = self._test_fetching_specs("restore", erasure_codec)
        assert not has_failed, "Expected successful read"

    def _test_repair_on_spot(self, allow_repair, erasure_codec):
        replicas, content = self._prepare_table(erasure_codec)

        replica = replicas[3]
        window_size = 1024
        output_stream = BufferedStream(size=window_size)
        response = read_table(
            "//tmp/table",
            table_reader={
                "window_size": window_size,
                "group_size": window_size,
                "pass_count": 1,
                "retry_count": 1,
                "enable_auto_repair": allow_repair,
            },
            output_stream=output_stream,
            return_response=True,
        )

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
            assert (
                not response.is_ok()
            ), "Read finished successfully, but expected to fail (due to unavailable part and disabled repairing)"

    @authors("akozhikhov")
    @pytest.mark.parametrize("erasure_codec", ["isa_lrc_12_2_2", "lrc_12_2_2"])
    def test_repair_on_spot_successful(self, erasure_codec):
        self._test_repair_on_spot(True, erasure_codec)

    @authors("akozhikhov")
    @pytest.mark.parametrize("erasure_codec", ["isa_lrc_12_2_2", "lrc_12_2_2"])
    def test_repair_on_spot_failed(self, erasure_codec):
        self._test_repair_on_spot(False, erasure_codec)

    def _test_repair(self, codec, replica_count, data_replica_count):
        remove("//tmp/table", force=True)
        create("table", "//tmp/table")
        set("//tmp/table/@erasure_codec", codec)
        write_table("//tmp/table", {"b": "hello"})

        chunk_id = get_singular_chunk_id("//tmp/table")

        replicas = get("#%s/@stored_replicas" % chunk_id)
        assert len(replicas) == replica_count

        assert self._is_chunk_ok(chunk_id)

        for r in replicas:
            replica_index = r.attributes["index"]
            address = str(r)
            print_debug("Banning node %s containing replica %d" % (address, replica_index))
            set_node_banned(address, True)
            wait(lambda: self._is_chunk_ok(chunk_id))
            assert read_table("//tmp/table") == [{"b": "hello"}]
            set_node_banned(r, False)

    @authors("ignat")
    def test_reed_solomon_6_3_repair(self):
        self._test_repair("reed_solomon_6_3", 9, 6)

    @authors("psushin", "ignat")
    def test_lrc_repair(self):
        self._test_repair("lrc_12_2_2", 16, 12)

    @authors("akozhikhov")
    def test_reed_solomon_3_3_repair(self):
        self._test_repair("reed_solomon_3_3", 6, 3)

    @authors("akozhikhov")
    def test_isa_reed_solomon_6_3_repair(self):
        self._test_repair("isa_reed_solomon_6_3", 9, 6)

    @authors("akozhikhov")
    def test_isa_lrc_repair(self):
        self._test_repair("isa_lrc_12_2_2", 16, 12)

    @authors("psushin", "ignat")
    def test_map(self):
        create("table", "//tmp/t1")
        set("//tmp/t1/@erasure_codec", "reed_solomon_6_3")
        create("table", "//tmp/t2")
        set("//tmp/t2/@erasure_codec", "lrc_12_2_2")
        write_table("//tmp/t1", {"a": "b"})
        map(in_="//tmp/t1", out="//tmp/t2", command="cat")

        assert read_table("//tmp/t2") == [{"a": "b"}]

    @authors("max42")
    @pytest.mark.parametrize("erasure_codec", ["isa_lrc_12_2_2", "lrc_12_2_2"])
    def test_slice_erasure_chunks_by_parts(self, erasure_codec):
        create("table", "//tmp/t1")
        set("//tmp/t1/@erasure_codec", erasure_codec)
        write_table("//tmp/t1", [{"a": "b"}] * 240)
        create("table", "//tmp/t2")

        op1 = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat",
            spec={"slice_erasure_chunks_by_parts": True},
        )
        chunk_count = get(op1.get_path() + "/@progress/data_flow_graph/edges/source/map/statistics/chunk_count")
        assert chunk_count == 12

        op2 = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat",
            spec={"slice_erasure_chunks_by_parts": False},
        )
        chunk_count = get(op2.get_path() + "/@progress/data_flow_graph/edges/source/map/statistics/chunk_count")
        assert chunk_count == 1

    @authors("prime")
    @pytest.mark.parametrize("erasure_codec", ["isa_lrc_12_2_2", "lrc_12_2_2"])
    def test_erasure_attribute_in_output_table(self, erasure_codec):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"a": "b"})

        create("table", "//tmp/t2")
        map(in_="//tmp/t1", out="<erasure_codec={}>//tmp/t2".format(erasure_codec), command="cat")
        assert get("//tmp/t2/@erasure_codec") == erasure_codec

    @authors("ignat")
    def test_sort(self):
        v1 = {"key": "aaa"}
        v2 = {"key": "bb"}
        v3 = {"key": "bbxx"}
        v4 = {"key": "zfoo"}
        v5 = {"key": "zzz"}

        create("table", "//tmp/t_in")
        set("//tmp/t_in/@erasure_codec", "lrc_12_2_2")
        write_table("//tmp/t_in", [v3, v5, v1, v2, v4])  # some random order

        create("table", "//tmp/t_out")
        set("//tmp/t_in/@erasure_codec", "reed_solomon_6_3")

        sort(in_="//tmp/t_in", out="//tmp/t_out", sort_by="key")

        assert read_table("//tmp/t_out") == [v1, v2, v3, v4, v5]
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["key"]

    @authors("babenko")
    @pytest.mark.parametrize("erasure_codec", ["isa_lrc_12_2_2", "lrc_12_2_2"])
    def test_part_ids(self, erasure_codec):
        create("table", "//tmp/t")
        set("//tmp/t/@erasure_codec", erasure_codec)
        write_table("//tmp/t", {"a": "b"})
        chunk_id = get_singular_chunk_id("//tmp/t")
        parts = chunk_id.split("-")
        for x in xrange(103, 119):
            part_id = "%s-%s-%s%x-%s" % (parts[0], parts[1], parts[2][:-2], x, parts[3])
            assert get("#" + part_id + "/@id") == chunk_id

    @authors("prime")
    @pytest.mark.parametrize("erasure_codec", ["isa_lrc_12_2_2", "lrc_12_2_2"])
    def test_write_table_with_erasure(self, erasure_codec):
        create("table", "//tmp/table")

        write_table("<erasure_codec=none>//tmp/table", [{"key": 0}])
        assert "none" == get("//tmp/table/@erasure_codec")

        write_table("<erasure_codec={}>//tmp/table".format(erasure_codec), [{"key": 0}])
        assert erasure_codec == get("//tmp/table/@erasure_codec")

        with pytest.raises(YtError):
            write_table("<append=true;erasure_codec={}>//tmp/table".format(erasure_codec), [{"key": 0}])

    @authors("prime")
    @pytest.mark.parametrize("erasure_codec", ["isa_lrc_12_2_2", "lrc_12_2_2"])
    def test_write_file_with_erasure(self, erasure_codec):
        create("file", "//tmp/f")

        write_file("<erasure_codec={}>//tmp/f".format(erasure_codec), "a")
        assert get("//tmp/f/@erasure_codec") == erasure_codec

        write_file("<erasure_codec=none>//tmp/f", "a")
        assert get("//tmp/f/@erasure_codec") == "none"

        with pytest.raises(YtError):
            write_file("<append=true;compression_codec=none>//tmp/f", "a")

    @authors("shakurov")
    def test_part_loss_time(self):
        # Ban 4 nodes so that banning any more would result in an inability to repair.
        nodes = ls("//sys/cluster_nodes")
        set_banned_flag(True, nodes[:4])

        create("table", "//tmp/t1", attributes={"erasure_codec": "lrc_12_2_2"})
        write_table("//tmp/t1", {"a": "b"})

        chunk_id = get_singular_chunk_id("//tmp/t1")

        replicas = get("#%s/@stored_replicas" % chunk_id)
        assert len(replicas) == 16

        assert self._is_chunk_ok(chunk_id)

        now = datetime.utcnow()
        set_node_banned(str(replicas[0]), True)
        wait(lambda: get("#" + chunk_id + "/@part_loss_time") != None) # noqa
        part_loss_time = get("#" + chunk_id + "/@part_loss_time")
        # You gotta love python's datetime for this.
        part_loss_time = part_loss_time.strip("Z")
        part_loss_time = datetime.strptime(part_loss_time, "%Y-%m-%dT%H:%M:%S.%f")
        assert part_loss_time > now
        assert part_loss_time < now + timedelta(seconds=30.0)

        assert exists("//sys/oldest_part_missing_chunks/" + chunk_id)

        set_node_banned(str(replicas[0]), False)
        wait(lambda: get("#" + chunk_id + "/@part_loss_time") == None) # noqa

        assert not exists("//sys/oldest_part_missing_chunks/" + chunk_id)

##################################################################


class TestErasureMulticell(TestErasure):
    NUM_TEST_PARTITIONS = 3
    NUM_SECONDARY_MASTER_CELLS = 2

##################################################################


class TestDynamicTablesErasure(TestErasureBase):
    USE_DYNAMIC_TABLES = True
    DELTA_NODE_CONFIG = {
        "data_node": {
            "block_cache": {
                "compressed_data": {
                    "capacity": 0
                },
                "uncompressed_data": {
                    "capacity": 0
                }
            }
        }
    }

    @authors("akozhikhov")
    def test_erasure_reader_failures(self):
        set("//sys/@config/tablet_manager/store_chunk_reader", {
            "pass_count": 1,
            "retry_count": 1,
            "slow_reader_expiration_timeout": 1000,
            "replication_reader_failure_timeout": 10000})

        sync_create_cells(1)

        replicas, content = self._prepare_table("isa_lrc_12_2_2", dynamic=True)

        def _read():
            try:
                rows = lookup_rows("//tmp/table", [{"key": i} for i in range(12)])
                return rows == content
            except:
                return False
        def _failing_read():
            with raises_yt_error("Not enough parts"):
                lookup_rows("//tmp/table", [{"key": i} for i in range(12)])

        # Readers are initialized.
        assert _read()

        chunk_id = get_singular_chunk_id("//tmp/table")
        replicas = get("#{0}/@stored_replicas".format(chunk_id))
        banned_nodes = replicas[:4]
        set_banned_flag(True, banned_nodes)
        time.sleep(1)

        # Banned replicas' replication readers are marked as failed.
        _failing_read()

        set_banned_flag(False, banned_nodes[:-2])
        time.sleep(1)

        # Failure timeout will expire later.
        _failing_read()

        wait(lambda: _read())
