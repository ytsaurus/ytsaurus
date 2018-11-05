from yt_env_setup import YTEnvSetup, wait
from yt_commands import *

from flaky import flaky

import yt.yson as yson
import yt.packages.requests as requests

import json
import random
import hashlib
import pytest
import subprocess
import string
import socket

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

REQUESTS_TABLE_SCHEMA = make_schema([
    {"name": "hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(table_path)"},
    {"name": "table_path", "type": "string", "sort_order": "ascending"},
    {"name": "options", "type": "string", "sort_order": "ascending"},
    {"name": "state", "type": "string"},
    {"name": "update_time", "type": "uint64"},
    {"name": "owner_id", "type": "string"},
    {"name": "error", "type": "any"},
    {"name": "progress", "type": "any"},
    {"name": "resources", "type": "any"}
])

RESOURCES_TABLE_SCHEMA = make_schema([
    {"name": "resource_id", "type": "string", "sort_order": "ascending"},
    {"name": "duplicate_id", "type": "string", "sort_order": "ascending"},
    {"name": "table_range", "type": "string"},
    {"name": "meta", "type": "string"}
])

FILES_TABLE_SCHEMA = make_schema([
    {"name": "resource_id", "type": "string", "sort_order": "ascending"},
    {"name": "duplicate_id", "type": "string", "sort_order": "ascending"},
    {"name": "filename", "type": "string", "sort_order": "ascending"},
    {"name": "sha1", "type": "string"}
])

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

    
    def wait_skynet_manager(self):
        url = "http://localhost:{}/debug/healthcheck".format(self.Env.configs["skynet_manager"][0]["port"])

        def skynet_manager_ready():
            try:
                rsp = requests.get(url)
            except (requests.exceptions.RequestException, socket.error):
                return False

            return True

        wait(skynet_manager_ready)
        

    def setup(self):
        sync_create_cells(1)
        create("map_node", "//sys/skynet_manager")
        create("table", "//sys/skynet_manager/requests", attributes={
            "dynamic": True,
            "schema": REQUESTS_TABLE_SCHEMA})
        sync_mount_table("//sys/skynet_manager/requests")
        create("table", "//sys/skynet_manager/resources", attributes={
            "dynamic": True,
            "schema": RESOURCES_TABLE_SCHEMA})
        sync_mount_table("//sys/skynet_manager/resources")
        create("table", "//sys/skynet_manager/files", attributes={
            "dynamic": True,
            "schema": FILES_TABLE_SCHEMA})
        sync_mount_table("//sys/skynet_manager/files")

    def teardown(self):
        sync_unmount_table("//sys/skynet_manager/requests")
        sync_unmount_table("//sys/skynet_manager/resources")
        sync_unmount_table("//sys/skynet_manager/files")
        remove("//sys/skynet_manager")

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

    def share(self, path, key_columns=None):
        url = "http://localhost:{}/api/v1/share".format(self.Env.configs["skynet_manager"][0]["port"])
        for _ in range(60):
            params = {"cluster": "local", "path": path}
            if key_columns is not None:
                params["key_columns"] = key_columns
            rsp = requests.post(url, headers={
                "X-Yt-Parameters": yson.dumps(params)
            })

            if rsp.status_code == 200:
                if key_columns:
                    return rsp.json()
                else:
                    return rsp.content

            if rsp.status_code == 202:
                progress = json.loads(rsp.headers["X-YT-Progress"])
                print "Progress %s" % str(progress)
                time.sleep(1)
                continue

            if "X-YT-Error" in rsp.headers:
                raise YtError("Share failed",
                    attributes={"path": path, "key_columns": key_columns},
                    inner_errors=[json.loads(rsp.headers["X-YT-Error"])])
            rsp.raise_for_status()

        raise RuntimeError("Failed to share {} in 60 seconds".format(path))

    @flaky(max_runs=5)
    def test_create_share(self):
        self.prepare_table("//tmp/table")
        rbtorrentid = self.share("//tmp/table")

        subprocess.check_call(["sky", "get", "-p", "-d", self.path_to_run + "/test_download_0", rbtorrentid])

    def test_no_table(self):
        with pytest.raises(YtError):
            self.share("//tmp/no_table")

    @flaky(max_runs=5)
    def test_empty_file(self):
        self.wait_skynet_manager()
        
        create("table", "//tmp/table_with_empty_file", attributes={
            "enable_skynet_sharing": True,
            "schema": SKYNET_TABLE_SCHEMA,
        })
        write_table("//tmp/table_with_empty_file", [
            {"filename": "empty.txt", "part_index": 0, "data": ""}
        ])
        rbtorrentid = self.share("//tmp/table_with_empty_file")

        subprocess.check_call(["sky", "get", "-p", "-d", self.path_to_run + "/test_download_2", rbtorrentid])

    def get_debug_links(self, rbtorrentid, manager_index=0):
        url = "http://localhost:{}/debug/links/{}".format(
            self.Env.configs["skynet_manager"][manager_index]["port"],
            rbtorrentid)

        rsp = requests.get(url)
        if rsp.status_code == 404:
            return None
        elif rsp.status_code == 200:
            return rsp.json()
        else:
            rsp.raise_for_status()

    def test_wrong_table_attributes(self):
        create("table", "//tmp/table_with_wrong_attrs")
        with pytest.raises(YtError):
            self.share("//tmp/table_with_wrong_attrs")

    def test_duplicate_table_content(self):
        self.prepare_table("//tmp/orig_table")
        copy("//tmp/orig_table", "//tmp/copy_table")

        rbtorrentid0 = self.share("//tmp/orig_table")
        rbtorrentid1 = self.share("//tmp/copy_table")
        assert rbtorrentid0 == rbtorrentid1

    def test_table_remove(self):
        self.prepare_table("//tmp/removed_table")
        rbtorrentid = self.share("//tmp/removed_table")
        resource_id = rbtorrentid.split(":")[1]
        remove("//tmp/removed_table")

        def share_is_removed():
            try:
                return self.get_debug_links(resource_id) is None
            except requests.exceptions.RequestException as e:
                pass
        wait(share_is_removed)

    def test_replication(self):
        self.prepare_table("//tmp/replicated_table")
        rbtorrentid = self.share("//tmp/replicated_table")
        resource_id = rbtorrentid.split(":")[1]

        assert self.get_debug_links(resource_id, 0) is not None
        def share_is_replicated():
            return self.get_debug_links(resource_id, 1) is not None

        wait(share_is_replicated)

    @flaky(max_runs=5)
    def test_recovery(self):
        self.wait_skynet_manager()

        self.prepare_table("//tmp/recovered_table")
        rbtorrentid = self.share("//tmp/recovered_table")

        self.Env.kill_service("skynet_manager")
        self.Env.start_skynet_managers(sync=False)

        subprocess.check_call(["sky", "get", "-p", "-d", self.path_to_run + "/test_download_1", rbtorrentid])

    @flaky(max_runs=5)
    def test_many_shards(self):
        self.wait_skynet_manager()

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

    @flaky(max_runs=5)
    def test_share_many(self):
        self.wait_skynet_manager()

        create("table", "//tmp/many_share_table", attributes={
            "enable_skynet_sharing": True,
            "schema": SKYNET_TABLE_SCHEMA,
        })

        write_table("//tmp/many_share_table", [
            {"sky_share_id": 0, "filename": "a", "part_index": 0, "data": "foo111"},
            {"sky_share_id": 1, "filename": "a", "part_index": 0, "data": "bar222"},
            {"sky_share_id": 1, "filename": "b", "part_index": 0, "data": "zog333"},
            {"sky_share_id": 3, "filename": "a", "part_index": 0, "data": "aba444"},
        ])

        reply = self.share("//tmp/many_share_table", ["sky_share_id"])

        assert len(reply["torrents"]) == 3
        assert [k["key"] for k in reply["torrents"]] == [[0], [1], [3]]

        subprocess.check_call(["sky", "get", "-t", "60", "-p", "-d", self.path_to_run + "/test_download_3", reply["torrents"][1]["rbtorrent"]])
