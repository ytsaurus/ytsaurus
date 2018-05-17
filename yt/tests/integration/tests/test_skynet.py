from yt_env_setup import YTEnvSetup, wait
from yt_commands import *

import yt.yson as yson

import random
import requests
import hashlib
import pytest
import subprocess
import string

##################################################################

SKYNET_TABLE_SCHEMA = make_schema([
    {"name": "sky_share_id", "type": "uint64", "sort_order": "ascending", "group": "meta"},
    {"name": "filename", "type": "string", "sort_order": "ascending", "group": "meta"},
    {"name": "part_index", "type": "int64", "sort_order": "ascending", "group": "meta"},
    {"name": "sha1", "type": "string", "group": "meta"},
    {"name": "md5", "type": "string", "group": "meta"},
    {"name": "data_size", "type": "int64", "group": "meta"},
    {"name": "data", "type": "string", "group": "data"},
], strict=True)

##################################################################

class TestSkynetIntegration(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

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

        rsp = requests.get("http://{}/read_skynet_part".format(http_address), params=kwargs)
        rsp.raise_for_status()
        return rsp.content

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

        with pytest.raises(requests.HTTPError):
            self.get_skynet_part(node_id, info["nodes"], chunk_id=chunk_id,
                lower_row_index=0, upper_row_index=2, start_part_index=0)

    def test_write_null_fields(self):
        create("table", "//tmp/table", attributes={
            "enable_skynet_sharing": True,
            "schema": SKYNET_TABLE_SCHEMA,
        })

        with pytest.raises(YtError):
            write_table("//tmp/table", [{}])
        
    def test_download_single_part_by_http(self):
        create("table", "//tmp/table", attributes={
            "enable_skynet_sharing": True,
            "schema": SKYNET_TABLE_SCHEMA,
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
            "schema": SKYNET_TABLE_SCHEMA,
            "chunk_writer": {"desired_chunk_weight": 4 * 4 * 1024 * 1024},
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
        write_table("//tmp/input", [
            {"sky_share_id": 0, "filename": "test.txt", "part_index": 0, "data": "foobar"},
            {"sky_share_id": 1, "filename": "test.txt", "part_index": 0, "data": "foobar"}])

        create("table", "//tmp/table", attributes={
            "enable_skynet_sharing": True,
            "schema": SKYNET_TABLE_SCHEMA,
        })

        map(in_="//tmp/input", out="//tmp/table", command="cat")
        row = read_table("//tmp/table", verbose=False)[0]
        assert 6 == row["data_size"]
        assert "sha1" in row
        assert "md5" in row

        create("table", "//tmp/table2", attributes={
            "enable_skynet_sharing": True,
            "schema": SKYNET_TABLE_SCHEMA,
        })

        map(in_="//tmp/input", out="//tmp/table2", command="cat")

        create("table", "//tmp/merged", attributes={
            "enable_skynet_sharing": True,
            "schema": SKYNET_TABLE_SCHEMA,
        })

        merge(mode="sorted",
            in_=["//tmp/table", "//tmp/table2"],
            out="//tmp/merged")

    def test_same_filename_in_two_shards(self):
        create("table", "//tmp/table", attributes={
            "enable_skynet_sharing": True,
            "schema": SKYNET_TABLE_SCHEMA,
        })
        
        write_table("//tmp/table", [
            {"sky_share_id": 0, "filename": "a", "part_index": 0, "data": "aaa"},
            {"sky_share_id": 1, "filename": "a", "part_index": 0, "data": "aaa"},
        ])
        
    def test_skynet_hashes(self):
        create("table", "//tmp/table", attributes={
            "enable_skynet_sharing": True,
            "schema": SKYNET_TABLE_SCHEMA,
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
        for row in read_table("//tmp/table", verbose=False):
            assert hashlib.sha1(row["data"]).digest() == row["sha1"], str(row)

            file_content[row["filename"]] = file_content.get(row["filename"], "") + row["data"]
            assert hashlib.md5(file_content[row["filename"]]).digest() == row["md5"]

##################################################################

class TestSkynetManager(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    ENABLE_PROXY = True
    ENABLE_RPC_PROXY = True
    NUM_SKYNET_MANAGERS = 2

    def prepare_table(self, table_path):
        create("table", table_path, attributes={
            "enable_skynet_sharing": True,
            "schema": SKYNET_TABLE_SCHEMA,
            "chunk_writer": {"desired_chunk_weight": 1 * 1024 * 1024},
        })
        write_table(table_path, [
            {"filename": "test0.txt", "part_index": 0, "data": "testtesttest"}
        ])

        write_table("<append=%true>" + table_path, [
            {
                "filename": "test1.bin",
                "part_index": i,
                "data": ''.join(random.choice(string.ascii_uppercase) * 1024 for _ in range(4 * 1024))
            } for i in range(3)])

    def share(self, path):
        url = "http://localhost:{}/api/v1/share".format(self.Env.configs["skynet_manager"][0]["port"])
        for _ in range(60):
            rsp = requests.post(url, headers={
                "X-Yt-Parameters": yson.dumps({"cluster": "local", "path": path})
            })

            if rsp.status_code == 200:
                return rsp.content

            if rsp.status_code == 202:
                time.sleep(1)
                continue

            if "X-YT-Error" in rsp.headers:
                raise RuntimeError("/api/v1/share failed: " + rsp.headers["X-YT-Error"])
            rsp.raise_for_status()

        raise RuntimeError("Failed to share {} in 60 seconds".format(path))

    def discover(self, manager_idx, rbtorrentid):
        url = "http://localhost:{}/api/v1/discover?rb_torrent_id={}".format(
            self.Env.configs["skynet_manager"][manager_idx]["port"],
            rbtorrentid)

        rsp = requests.get(url)
        rsp.raise_for_status()
        return rsp.json

    def test_create_share(self):
        self.prepare_table("//tmp/table")
        rbtorrentid = self.share("//tmp/table")

        subprocess.check_call(["sky", "get", "-p", "-d", self.path_to_run + "/test_download_0", rbtorrentid])

    def test_no_table(self):
        with pytest.raises(RuntimeError):
            self.share("//tmp/no_table")

    def test_empty_file(self):
        create("table", "//tmp/table_with_empty_file", attributes={
            "enable_skynet_sharing": True,
            "schema": SKYNET_TABLE_SCHEMA,
        })
        write_table("//tmp/table_with_empty_file", [
            {"filename": "empty.txt", "part_index": 0, "data": ""}
        ])
        rbtorrentid = self.share("//tmp/table_with_empty_file")

        subprocess.check_call(["sky", "get", "-p", "-d", self.path_to_run + "/test_download_2", rbtorrentid])

    def test_wrong_table_attributes(self):
        create("table", "//tmp/table_with_wrong_attrs")
        pass

    def test_wrong_table_schema(self):
        pass

    def test_wrong_table_content(self):
        pass
            
    def test_duplicate_table_content(self):
        self.prepare_table("//tmp/orig_table")
        copy("//tmp/orig_table", "//tmp/copy_table")

        rbtorrentid0 = self.share("//tmp/orig_table")
        rbtorrentid1 = self.share("//tmp/copy_table")
        assert rbtorrentid0 == rbtorrentid1

    def test_table_remove(self):
        self.prepare_table("//tmp/removed_table")
        rbtorrentid = self.share("//tmp/removed_table")
        remove("//tmp/removed_table")

        def share_is_removed():
            try:
                self.discover(0, rbtorrentid)
                return False
            except requests.RequestException as e:
                return e.response.status_code == 400

        wait(share_is_removed)
        
    def test_replication(self):
        self.prepare_table("//tmp/replicated_table")
        rbtorrentid = self.share("//tmp/replicated_table")

        def share_is_available():
            try:
                self.discover(1, rbtorrentid)
                return True
            except requests.RequestException:
                return False

        wait(share_is_available)

    def test_recovery(self):
        self.prepare_table("//tmp/recovered_table")
        rbtorrentid = self.share("//tmp/recovered_table")

        self.Env.kill_service("skynet_manager")
        self.Env.start_skynet_managers(sync=False)

        subprocess.check_call(["sky", "get", "-p", "-d", self.path_to_run + "/test_download_1", rbtorrentid])

    def test_many_shards(self):
        create("table", "//tmp/sharded_table", attributes={
            "enable_skynet_sharing": True,
            "schema": SKYNET_TABLE_SCHEMA,
        })

        second_file = ''.join(random.choice(string.ascii_uppercase) for _ in range(128))

        write_table("//tmp/sharded_table", [
            {"sky_share_id": 0, "filename": "a", "part_index": 0, "data": "aaa"},
            {"sky_share_id": 1, "filename": "a", "part_index": 0, "data": second_file},
        ])

        rbtorrentid0 = self.share("//tmp/sharded_table[0u]")
        rbtorrentid1 = self.share("//tmp/sharded_table[1u]")

        subprocess.check_call(["sky", "get", "-t", "60", "-p", "-d", self.path_to_run + "/test_download_2", rbtorrentid1])
        assert second_file == open(self.path_to_run + "/test_download_2/a").read()
