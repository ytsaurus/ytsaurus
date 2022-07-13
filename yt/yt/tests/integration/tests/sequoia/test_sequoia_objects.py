from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, get, remove, get_singular_chunk_id, lookup_rows, write_table, read_table, wait,
    exists, concatenate, select_rows)

import yt.yson as yson

import yt_proto.yt.client.chunk_client.proto.chunk_meta_pb2 as chunk_meta_pb2

##################################################################


class TestSequoiaObjects(YTEnvSetup):
    USE_SEQUOIA = True

    def _get_id_hash(self, chunk_id):
        id_part = chunk_id.split('-')[3]
        return int(id_part, 16)

    def _get_key(self, chunk_id):
        return {
            "id_hash": self._get_id_hash(chunk_id),
            "id": chunk_id,
        }

    @authors("gritukan")
    def test_estimated_creation_time(self):
        object_id = "543507cc-00000000-12345678-abcdef01"
        creation_time = {'min': '2012-12-21T08:34:56.000000Z', 'max': '2012-12-21T08:34:57.000000Z'}
        assert get("//sys/estimated_creation_time/{}".format(object_id)) == creation_time

    @authors("gritukan")
    def test_sequoia_chunk(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"x": 1}])
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#{}/@sequoia".format(chunk_id))
        assert get("#{}/@aevum".format(chunk_id)) != "none"

        select_rows("* from [//sys/sequoia/chunk_meta_extensions]")
        assert len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(chunk_id)])) == 1

    @authors("aleksandra-zh")
    def test_confirm_sequoia_chunk(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"x": 1}])
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#{}/@sequoia".format(chunk_id))
        assert get("#{}/@aevum".format(chunk_id)) != "none"

        exts = lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(chunk_id)])
        assert len(exts) == 1
        raw_misc_ext = yson.get_bytes(exts[0]["misc_ext"])
        misc_ext = chunk_meta_pb2.TMiscExt()
        misc_ext.ParseFromString(raw_misc_ext)
        assert misc_ext.row_count == 1

    @authors("aleksandra-zh")
    def test_remove_sequoia_chunk(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"x": 1}])
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#{}/@sequoia".format(chunk_id))
        assert get("#{}/@aevum".format(chunk_id)) != "none"

        assert len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(chunk_id)])) == 1

        remove("//tmp/t")

        wait(lambda: not exists("#{}".format(chunk_id)))
        wait(lambda: len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(chunk_id)])) == 0)


class TestSequoiaObjectsMulticell(TestSequoiaObjects):
    NUM_SECONDARY_MASTER_CELLS = 3

    @authors("aleksandra-zh")
    def test_remove_foreign_sequoia_chunk(self):
        create("table", "//tmp/t1", attributes={"external_cell_tag": 12})
        write_table("<append=true>//tmp/t1", {"a": "b"})

        create("table", "//tmp/t2", attributes={"external_cell_tag": 13})
        write_table("<append=true>//tmp/t2", {"c": "d"})

        create("table", "//tmp/t", attributes={"external_cell_tag": 13})
        concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/t")

        t1_chunk_id = get_singular_chunk_id("//tmp/t1")
        assert len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(t1_chunk_id)])) == 1

        t2_chunk_id = get_singular_chunk_id("//tmp/t2")
        assert len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(t2_chunk_id)])) == 1

        remove("//tmp/t")
        assert len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(t1_chunk_id)])) == 1
        assert len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(t2_chunk_id)])) == 1

        remove("//tmp/t2")
        wait(lambda: not exists("#{}".format(t2_chunk_id)))
        wait(lambda: len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(t2_chunk_id)])) == 0)
        assert len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(t1_chunk_id)])) == 1

        remove("//tmp/t1")
        wait(lambda: not exists("#{}".format(t1_chunk_id)))
        wait(lambda: len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(t1_chunk_id)])) == 0)
        assert len(lookup_rows("//sys/sequoia/chunk_meta_extensions", [self._get_key(t2_chunk_id)])) == 0
