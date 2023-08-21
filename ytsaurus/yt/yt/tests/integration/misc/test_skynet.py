from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait, create, get, set, locate_skynet_share, read_table,
    write_table, map,
    merge, get_singular_chunk_id)

from yt_type_helpers import make_schema

from yt.common import YtError
import yt.packages.requests as requests

import hashlib
import pytest
import os

##################################################################

SKYNET_TABLE_SCHEMA = make_schema(
    [
        {
            "name": "sky_share_id",
            "type": "uint64",
            "sort_order": "ascending",
            "group": "meta",
        },
        {
            "name": "filename",
            "type": "string",
            "sort_order": "ascending",
            "group": "meta",
        },
        {
            "name": "part_index",
            "type": "int64",
            "sort_order": "ascending",
            "group": "meta",
        },
        {"name": "sha1", "type": "string", "group": "meta"},
        {"name": "md5", "type": "string", "group": "meta"},
        {"name": "data_size", "type": "int64", "group": "meta"},
        {"name": "data", "type": "string", "group": "data"},
    ],
    strict=True,
)

##################################################################


class TestSkynetIntegration(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    @authors("prime")
    def test_locate_single_part(self):
        create("table", "//tmp/table")

        write_table("//tmp/table", {"a": 1})

        chunk = get_singular_chunk_id("//tmp/table")

        info = locate_skynet_share("//tmp/table[#0:#1]")

        chunk_specs = info["chunk_specs"]
        assert 1 == len(chunk_specs)
        assert chunk_specs[0]["chunk_id"] == chunk
        assert chunk_specs[0]["row_index"] == 0
        assert chunk_specs[0]["row_count"] == 1

    @authors("prime")
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

        assert chunk_specs[0] == {
            "chunk_id": chunks[0],
            "lower_limit": {"row_index": 1},
            "upper_limit": {"row_index": 2},
            "row_index": 0,
            "range_index": 0,
            "row_count": 1,
        }
        assert chunk_specs[1] == {
            "chunk_id": chunks[1],
            "row_index": 2,
            "range_index": 0,
            "row_count": 2,
        }
        assert chunk_specs[2] == {
            "chunk_id": chunks[2],
            "lower_limit": {"row_index": 0},
            "upper_limit": {"row_index": 1},
            "row_index": 4,
            "range_index": 0,
            "row_count": 1,
        }

    @authors("prime")
    def test_multiple_ranges(self):
        create("table", "//tmp/table")

        write_table("//tmp/table", [{"a": 1}, {"a": 2}])
        chunk = get_singular_chunk_id("//tmp/table")

        info = locate_skynet_share("//tmp/table[#0:#1,#1:#2]")

        chunk_specs = info["chunk_specs"]
        for spec in chunk_specs:
            spec.pop("replicas")

        assert len(chunk_specs) == 2
        assert chunk_specs[0] == {
            "chunk_id": chunk,
            "lower_limit": {"row_index": 0},
            "upper_limit": {"row_index": 1},
            "row_index": 0,
            "range_index": 0,
            "row_count": 1,
        }
        assert chunk_specs[1] == {
            "chunk_id": chunk,
            "lower_limit": {"row_index": 1},
            "upper_limit": {"row_index": 2},
            "row_index": 0,
            "range_index": 1,
            "row_count": 1,
        }

    @authors("prime")
    def test_node_locations(self):
        create("table", "//tmp/table", attributes={"replication_factor": 5})

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

        if "http_range" in kwargs:
            http_start, http_end = kwargs.pop("http_range")

            rsp = requests.get("http://{}/read_skynet_part".format(http_address), headers={
                "Range": "bytes={}-{}".format(http_start, http_end)
            }, params=kwargs)

            if rsp.status_code == 500:
                raise Exception(rsp.text)

            assert rsp.status_code == 206
            assert rsp.headers["Content-Range"] == "bytes {}-{}/*".format(http_start, http_end)

            rsp.raise_for_status()
            return rsp.content

        rsp = requests.get("http://{}/read_skynet_part".format(http_address), params=kwargs)
        if rsp.status_code == 500:
            raise Exception(rsp.text)
        rsp.raise_for_status()
        return rsp.content

    @authors("prime")
    def test_http_checks_access(self):
        create("table", "//tmp/table")
        write_table("//tmp/table", [{"part_index": 0, "data": "abc"}])

        info = locate_skynet_share("//tmp/table[#0:#2]")

        chunk = info["chunk_specs"][0]
        chunk_id = chunk["chunk_id"]
        assert len(chunk["replicas"]) > 0
        for node in info["nodes"]:
            node_id = node["node_id"]
            if node_id in chunk["replicas"]:
                break
        else:
            assert False, "Node not found: {}, {}".format(chunk["replicas"], str(info["nodes"]))

        with pytest.raises(Exception):
            self.get_skynet_part(
                node_id,
                info["nodes"],
                chunk_id=chunk_id,
                lower_row_index=0,
                upper_row_index=2,
                start_part_index=0,
            )

    @authors("prime")
    def test_write_null_fields(self):
        create(
            "table",
            "//tmp/table",
            attributes={
                "enable_skynet_sharing": True,
                "schema": SKYNET_TABLE_SCHEMA,
            },
        )

        with pytest.raises(YtError):
            write_table("//tmp/table", [{}])

    @authors("prime")
    def test_download_single_part_by_http(self):
        create(
            "table",
            "//tmp/table",
            attributes={
                "enable_skynet_sharing": True,
                "schema": SKYNET_TABLE_SCHEMA,
            },
        )

        write_table("//tmp/table", [{"filename": "X", "part_index": 0, "data": "abc"}])

        info = locate_skynet_share("//tmp/table[#0:#2]")

        chunk = info["chunk_specs"][0]
        chunk_id = chunk["chunk_id"]
        assert len(chunk["replicas"]) > 0
        for node in info["nodes"]:
            node_id = node["node_id"]
            if node_id in chunk["replicas"]:
                break
        else:
            assert False, "Node not found: {}, {}".format(chunk["replicas"], str(info["nodes"]))

        assert b"abc" == self.get_skynet_part(
            node_id,
            info["nodes"],
            chunk_id=chunk_id,
            lower_row_index=0,
            upper_row_index=1,
            start_part_index=0,
        )

    @authors("prime")
    def test_http_edge_cases(self):
        create(
            "table",
            "//tmp/table",
            attributes={
                "enable_skynet_sharing": True,
                "optimize_for": "scan",
                "schema": SKYNET_TABLE_SCHEMA,
                "chunk_writer": {"desired_chunk_weight": 4 * 4 * 1024 * 1024},
            },
        )

        def to_skynet_chunk(data):
            return data * (4 * 1024 * 1024 // len(data))

        write_table(
            "//tmp/table",
            [
                {"filename": "a", "part_index": 0, "data": to_skynet_chunk(b"a1")},
                {"filename": "a", "part_index": 1, "data": b"a2"},
                {"filename": "b", "part_index": 0, "data": b"b1"},
                {"filename": "c", "part_index": 0, "data": to_skynet_chunk(b"c1")},
                {"filename": "c", "part_index": 1, "data": to_skynet_chunk(b"c2")},
                {"filename": "c", "part_index": 2, "data": to_skynet_chunk(b"c3")},
                {"filename": "c", "part_index": 3, "data": to_skynet_chunk(b"c4")},
                {"filename": "c", "part_index": 4, "data": to_skynet_chunk(b"c5")},
                {"filename": "c", "part_index": 5, "data": b"c6"},
            ],
        )

        info = locate_skynet_share("//tmp/table[#0:#9]")

        chunk_1 = info["chunk_specs"][0]["chunk_id"]
        node_1 = info["chunk_specs"][0]["replicas"][0]
        chunk_2 = info["chunk_specs"][1]["chunk_id"]
        node_2 = info["chunk_specs"][1]["replicas"][0]

        test_queries = [
            (node_1, chunk_1, to_skynet_chunk(b"a1") + b"a2", 0, 2, 0),
            (node_1, chunk_1, b"b1", 2, 3, 0),
            (
                node_1,
                chunk_1,
                to_skynet_chunk(b"c1") + to_skynet_chunk(b"c2") + to_skynet_chunk(b"c3"),
                3,
                6,
                0,
            ),
            (
                node_2,
                chunk_2,
                to_skynet_chunk(b"c4") + to_skynet_chunk(b"c5") + b"c6",
                0,
                3,
                3,
            ),
        ]

        for (
            node,
            chunk,
            result,
            lower_row_index,
            upper_row_index,
            part_index,
        ) in test_queries:
            assert result == self.get_skynet_part(
                node,
                info["nodes"],
                chunk_id=chunk,
                lower_row_index=lower_row_index,
                upper_row_index=upper_row_index,
                start_part_index=part_index,
            )

    @authors("prime")
    def test_operation_output(self):
        create("table", "//tmp/input")
        write_table(
            "//tmp/input",
            [
                {
                    "sky_share_id": 0,
                    "filename": "test.txt",
                    "part_index": 0,
                    "data": "foobar",
                },
                {
                    "sky_share_id": 1,
                    "filename": "test.txt",
                    "part_index": 0,
                    "data": "foobar",
                },
            ],
        )

        create(
            "table",
            "//tmp/table",
            attributes={
                "enable_skynet_sharing": True,
                "schema": SKYNET_TABLE_SCHEMA,
            },
        )

        map(in_="//tmp/input", out="//tmp/table", command="cat")
        row = read_table("//tmp/table", verbose=False)[0]
        assert 6 == row["data_size"]
        assert "sha1" in row
        assert "md5" in row

        create(
            "table",
            "//tmp/table2",
            attributes={
                "enable_skynet_sharing": True,
                "schema": SKYNET_TABLE_SCHEMA,
            },
        )

        map(in_="//tmp/input", out="//tmp/table2", command="cat")

        create(
            "table",
            "//tmp/merged",
            attributes={
                "enable_skynet_sharing": True,
                "schema": SKYNET_TABLE_SCHEMA,
            },
        )

        merge(mode="sorted", in_=["//tmp/table", "//tmp/table2"], out="//tmp/merged")

    @authors("prime")
    def test_same_filename_in_two_shards(self):
        create(
            "table",
            "//tmp/table",
            attributes={
                "enable_skynet_sharing": True,
                "schema": SKYNET_TABLE_SCHEMA,
            },
        )

        write_table(
            "//tmp/table",
            [
                {"sky_share_id": 0, "filename": "a", "part_index": 0, "data": "aaa"},
                {"sky_share_id": 1, "filename": "a", "part_index": 0, "data": "aaa"},
            ],
        )

    @authors("prime")
    def test_skynet_hashes(self):
        create(
            "table",
            "//tmp/table",
            attributes={
                "enable_skynet_sharing": True,
                "schema": SKYNET_TABLE_SCHEMA,
            },
        )

        def to_skynet_chunk(data):
            return data * (4 * 1024 * 1024 // len(data))

        write_table(
            "//tmp/table",
            [
                {"filename": "a", "part_index": 0, "data": to_skynet_chunk("a1")},
                {"filename": "a", "part_index": 1, "data": "a2"},
                {"filename": "b", "part_index": 0, "data": "b1"},
                {"filename": "c", "part_index": 0, "data": to_skynet_chunk("c1")},
                {"filename": "c", "part_index": 1, "data": to_skynet_chunk("c2")},
                {"filename": "c", "part_index": 2, "data": to_skynet_chunk("c3")},
            ],
        )

        file_content = {}
        for row in read_table("//tmp/table", verbose=False):
            assert hashlib.sha1(row["data"].encode("ascii")).digest() == row["sha1"], str(row)

            file_content[row["filename"]] = file_content.get(row["filename"], b"") + row["data"].encode("ascii")
            assert hashlib.md5(file_content[row["filename"]]).digest() == row["md5"]

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_chunk_merge_skynet_share(self, merge_mode):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        create(
            "table",
            "//tmp/table",
            attributes={
                "enable_skynet_sharing": True,
                "schema": SKYNET_TABLE_SCHEMA,
            },
        )

        write_table("<append=true>//tmp/table", {"filename": "a", "part_index": 0, "data": "a1"})
        write_table("<append=true>//tmp/table", {"filename": "b", "part_index": 0, "data": "a2"})
        write_table("<append=true>//tmp/table", {"filename": "c", "part_index": 0, "data": "a3"})

        info = read_table("//tmp/table")

        chunk_ids = get("//tmp/table/@chunk_ids")
        assert get("#{0}/@shared_to_skynet".format(chunk_ids[0]))

        set("//tmp/table/@chunk_merger_mode", merge_mode)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)

        wait(lambda: get("//tmp/table/@resource_usage/chunk_count") == 1)
        assert read_table("//tmp/table") == info

        chunk_ids = get("//tmp/table/@chunk_ids")
        assert get("#{0}/@shared_to_skynet".format(chunk_ids[0]))

    @authors("prime")
    def test_http_range(self):
        create(
            "table",
            "//tmp/table",
            attributes={
                "enable_skynet_sharing": True,
                "schema": SKYNET_TABLE_SCHEMA,
            },
        )

        file = os.urandom(12 * 1024 * 1024 + 42)

        write_table(
            "//tmp/table",
            [
                {"filename": "a", "part_index": 0, "data": file[:4*1024*1024]},
                {"filename": "a", "part_index": 1, "data": file[4*1024*1024:8*1024*1024]},
                {"filename": "a", "part_index": 2, "data": file[8*1024*1024:12*1024*1024]},
                {"filename": "a", "part_index": 3, "data": file[12*1024*1024:]},
            ],
        )

        chunk = get_singular_chunk_id("//tmp/table")
        info = locate_skynet_share("//tmp/table")
        node = info["chunk_specs"][0]["replicas"][0]

        for http_range in [
            (0, 1),
            (0, len(file)),
            (0, len(file)-1),
            (1, len(file)-1),
            (1, len(file)),
            (0, len(file)-4*1024*1024),
            (1, len(file)-4*1024*1024),
            (0, len(file)-41),
            (1, len(file)-41),
            (0, len(file)-42),
            (1, len(file)-42),
            (0, len(file)-43),
            (1, len(file)-43),

            (8*1024*1024, 12*1024*1024),
            (8*1024*1024, len(file)),
            (8*1024*1024, len(file)-41),
            (8*1024*1024, len(file)-42),
            (8*1024*1024, len(file)-43),
        ]:
            assert file[http_range[0]:http_range[1]] == self.get_skynet_part(
                node,
                info["nodes"],
                chunk_id=chunk,
                lower_row_index=0,
                upper_row_index=4,
                start_part_index=0,
                http_range=(http_range[0], http_range[1]-1)
            )

        for http_range in [
            (4*1024*1024 - 1, 4*1024*1024 - 1),
            (100000000, 100000000),
            (42, 41),
        ]:
            with pytest.raises(Exception):
                self.get_skynet_part(
                    node,
                    info["nodes"],
                    chunk_id=chunk,
                    lower_row_index=0,
                    upper_row_index=4,
                    start_part_index=0,
                    http_range=(http_range[0], http_range[1]-1)
                )
