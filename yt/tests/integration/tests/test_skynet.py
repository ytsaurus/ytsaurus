from yt_env_setup import YTEnvSetup, wait
from yt_commands import *

import yt.yson as yson

import urllib2
import hashlib
import pytest

##################################################################

class TestSkynet(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "use_new_http_server": True,
        "data_node": {
            "enable_experimental_skynet_http_api": True
        }
    }

    SKYNET_TABLE_SCHEMA = make_schema([
        {"name": "sky_share_id", "type": "uint64", "sort_order": "ascending", "group": "meta"},
        {"name": "filename", "type": "string", "group": "meta"},
        {"name": "part_index", "type": "int64", "group": "meta"},
        {"name": "sha1", "type": "string", "group": "meta"},
        {"name": "md5", "type": "string", "group": "meta"},
        {"name": "data_size", "type": "int64", "group": "meta"},
        {"name": "data", "type": "string", "group": "data"},
    ], strict=True)

    def test_locate_single_part(self):
        create("table", "//tmp/table")

        write_table("//tmp/table", {"a": 1})

        chunk = get("//tmp/table/@chunk_ids")[0]

        info = locate_skynet_share("//tmp/table[#0:#1]")

        chunk_specs = info["chunk_specs"]
        assert 1 == len(chunk_specs)
        assert chunk_specs[0]["chunk_id"] == chunk
        assert chunk_specs[0]["row_index"] == 0
        assert chunk_specs[0]["row_count"] == 1

    def test_locate_multiple_parts(self):
        create("table", "//tmp/table")

        write_table("//tmp/table", [{"a": 1}, {"a": 2}])
        write_table("<append=%true>//tmp/table", [{"a": 3}, {"a": 4}])
        write_table("<append=%true>//tmp/table", [{"a": 5}, {"a": 6}])

        chunks = get("//tmp/table/@chunk_ids")

        info = locate_skynet_share("//tmp/table[#1:#5]")

        chunk_specs = info["chunk_specs"]
        assert 3 == len(chunk_specs)
        for spec in chunk_specs:
            spec.pop("replicas")

        assert chunk_specs[0] == {'chunk_id': chunks[0], 'lower_limit': {'row_index': 1}, 'upper_limit': {'row_index': 2}, 'row_index': 0, 'range_index': 0, 'row_count': 1}
        assert chunk_specs[1] == {'chunk_id': chunks[1], 'row_index': 2, 'range_index': 0, 'row_count': 2}
        assert chunk_specs[2] == {'chunk_id': chunks[2], 'lower_limit': {'row_index': 0}, 'upper_limit': {'row_index': 1}, 'row_index': 4, 'range_index': 0, 'row_count': 1}

    def test_multiple_ranges(self):
        create("table", "//tmp/table")

        write_table("//tmp/table", [{"a": 1}, {"a": 2}])
        chunk = get("//tmp/table/@chunk_ids")[0]

        info = locate_skynet_share("//tmp/table[#0:#1,#1:#2]")

        chunk_specs = info["chunk_specs"]
        for spec in chunk_specs:
            spec.pop("replicas")

        assert len(chunk_specs) == 2
        assert chunk_specs[0] == {'chunk_id': chunk, 'lower_limit': {'row_index': 0}, 'upper_limit': {'row_index': 1}, 'row_index': 0, 'range_index': 0, 'row_count': 1}
        assert chunk_specs[1] == {'chunk_id': chunk, 'lower_limit': {'row_index': 1}, 'upper_limit': {'row_index': 2}, 'row_index': 0, 'range_index': 1, 'row_count': 1}

    def test_node_locations(self):
        create("table", "//tmp/table", attributes={
            "replication_factor": 5
        })

        write_table("//tmp/table", [{"a": 1}])
        write_table("<append=%true>//tmp/table", [{"a": 2}])

        def table_fully_replicated():
            for chunk_id in get("//tmp/table/@chunk_ids"):
                if len(get("//sys/chunks/{}/@stored_replicas".format(chunk_id))) != 5:
                    return False
            return True
        wait(table_fully_replicated)

        info = locate_skynet_share("//tmp/table[#0:#2]")

        assert len(info["nodes"]) == 5
        ids = [n["node_id"] for n in info["nodes"]]

        for spec in info["chunk_specs"]:
            assert len(spec["replicas"]) == 5
            for n in spec["replicas"]:
                assert n in ids

    def get_skynet_part(self, node_id, replicas, **kwargs):
        for replica in replicas:
            if replica["node_id"] == node_id:
                http_address = replica["addresses"]["default"]
                break
        else:
            raise KeyError(node_id)

        url = "http://{}/read_skynet_part?{}".format(
            http_address,
            "&".join("{}={}".format(k, v) for k, v in kwargs.items()))
        return urllib2.urlopen(url).read()

    def test_http_checks_access(self):
        create("table", "//tmp/table")
        write_table("//tmp/table", [
            {"part_index": 0, "data": "abc"}
        ])

        info = locate_skynet_share("//tmp/table[#0:#2]")

        chunk = info["chunk_specs"][0]
        chunk_id = chunk["chunk_id"]
        assert chunk["replicas"] > 0
        for node in info["nodes"]:
            node_id = node["node_id"]
            if node_id in chunk["replicas"]:
                break
        else:
            assert False, "Node not found: {}, {}".format(chunk["replicas"], str(info["nodes"]))

        with pytest.raises(urllib2.HTTPError):
            self.get_skynet_part(node_id, info["nodes"], chunk_id=chunk_id,
                lower_row_index=0, upper_row_index=2, start_part_index=0)

    def test_download_single_part_by_http(self):
        create("table", "//tmp/table", attributes={
            "enable_skynet_sharing": True,
            "schema": TestSkynet.SKYNET_TABLE_SCHEMA,
        })

        write_table("//tmp/table", [
            {"filename": "X", "part_index": 0, "data": "abc"}
        ])

        info = locate_skynet_share("//tmp/table[#0:#2]")

        chunk = info["chunk_specs"][0]
        chunk_id = chunk["chunk_id"]
        assert chunk["replicas"] > 0
        for node in info["nodes"]:
            node_id = node["node_id"]
            if node_id in chunk["replicas"]:
                break
        else:
            assert False, "Node not found: {}, {}".format(chunk["replicas"], str(info["nodes"]))

        assert "abc" == self.get_skynet_part(node_id, info["nodes"], chunk_id=chunk_id,
                                             lower_row_index=0, upper_row_index=1, start_part_index=0)

    def test_http_edge_cases(self):
        create("table", "//tmp/table", attributes={
            "enable_skynet_sharing": True,
            "optimize_for": "scan",
            "schema": TestSkynet.SKYNET_TABLE_SCHEMA,
        })

        def to_skynet_chunk(data):
            return data * (4 * 1024 * 1024 / len(data))

        write_table("//tmp/table", [
            {"filename": "a", "part_index": 0, "data": to_skynet_chunk("a1")},
            {"filename": "a", "part_index": 1, "data": "a2"},
            {"filename": "b", "part_index": 0, "data": "b1"},
            {"filename": "c", "part_index": 0, "data": to_skynet_chunk("c1")},
            {"filename": "c", "part_index": 1, "data": to_skynet_chunk("c2")},
            {"filename": "c", "part_index": 2, "data": to_skynet_chunk("c3")},
        ])
        write_table("<append=%true>//tmp/table", [
            {"filename": "c", "part_index": 3, "data": to_skynet_chunk("c4")},
            {"filename": "c", "part_index": 4, "data": to_skynet_chunk("c5")},
            {"filename": "c", "part_index": 5, "data": "c6"},
        ])

        info = locate_skynet_share("//tmp/table[#0:#9]")

        chunk_1 = info["chunk_specs"][0]["chunk_id"]
        node_1 = info["chunk_specs"][0]["replicas"][0]
        chunk_2 = info["chunk_specs"][1]["chunk_id"]
        node_2 = info["chunk_specs"][1]["replicas"][0]

        test_queries = [
            (node_1, chunk_1, to_skynet_chunk("a1") + "a2", 0, 2, 0),
            (node_1, chunk_1, "b1", 2, 3, 0),
            (node_1, chunk_1, to_skynet_chunk("c1") + to_skynet_chunk("c2") + to_skynet_chunk("c3"), 3, 6, 0),
            (node_2, chunk_2, to_skynet_chunk("c4") + to_skynet_chunk("c5") + "c6", 0, 3, 3)
        ]

        for node, chunk, result, lower_row_index, upper_row_index, part_index in test_queries:
            assert result == self.get_skynet_part(node, info["nodes"], chunk_id=chunk,
                                                  lower_row_index=lower_row_index,
                                                  upper_row_index=upper_row_index,
                                                  start_part_index=part_index)

    def test_operation_output(self):
        create("table", "//tmp/input")
        write_table("//tmp/input", {"sky_share_id": 0, "filename": "test.txt", "part_index": 0, "data": "foobar"})

        create("table", "//tmp/table", attributes={
            "enable_skynet_sharing": True,
            "schema": TestSkynet.SKYNET_TABLE_SCHEMA,
        })

        map(in_="//tmp/input", out="//tmp/table", command="cat")
        row = read_table("//tmp/table")[0]
        assert 6 == row["data_size"]
        assert "sha1" in row
        assert "md5" in row

    def test_skynet_hashes(self):
        create("table", "//tmp/table", attributes={
            "enable_skynet_sharing": True,
            "schema": TestSkynet.SKYNET_TABLE_SCHEMA,
        })

        def to_skynet_chunk(data):
            return data * (4 * 1024 * 1024 / len(data))

        write_table("//tmp/table", [
            {"filename": "a", "part_index": 0, "data": to_skynet_chunk("a1")},
            {"filename": "a", "part_index": 1, "data": "a2"},
            {"filename": "b", "part_index": 0, "data": "b1"},
            {"filename": "c", "part_index": 0, "data": to_skynet_chunk("c1")},
            {"filename": "c", "part_index": 1, "data": to_skynet_chunk("c2")},
            {"filename": "c", "part_index": 2, "data": to_skynet_chunk("c3")},
        ])

        file_content = {}
        for row in read_table("//tmp/table"):
            assert hashlib.sha1(row["data"]).digest() == row["sha1"], str(row)

            file_content[row["filename"]] = file_content.get(row["filename"], "") + row["data"]
            assert hashlib.md5(file_content[row["filename"]]).digest() == row["md5"]
