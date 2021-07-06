from yt_env_setup import YTEnvSetup, wait

from yt_commands import (  # noqa
    authors, print_debug, wait, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists,
    locate_skynet_share,
    create_account, create_network_project, create_tmpdir, create_user, create_group,
    create_pool, create_pool_tree, remove_pool_tree,
    create_data_center, create_rack, create_table,
    make_ace, check_permission, add_member,
    make_batch_request, execute_batch, get_batch_error,
    start_transaction, abort_transaction, commit_transaction, lock,
    insert_rows, select_rows, lookup_rows, delete_rows, trim_rows, alter_table,
    read_file, write_file, read_table, write_table, write_local_file,
    map, reduce, map_reduce, join_reduce, merge, vanilla, sort, erase, remote_copy,
    run_test_vanilla, run_sleeping_vanilla,
    abort_job, list_jobs, get_job, abandon_job, interrupt_job,
    get_job_fail_context, get_job_input, get_job_stderr, get_job_spec,
    dump_job_context, poll_job_shell,
    abort_op, complete_op, suspend_op, resume_op,
    get_operation, list_operations, clean_operations,
    get_operation_cypress_path, scheduler_orchid_pool_path,
    scheduler_orchid_default_pool_tree_path, scheduler_orchid_operation_path,
    scheduler_orchid_default_pool_tree_config_path, scheduler_orchid_path,
    scheduler_orchid_node_path, scheduler_orchid_pool_tree_config_path, scheduler_orchid_pool_tree_path,
    mount_table, wait_for_tablet_state,
    sync_create_cells, sync_mount_table, sync_unmount_table,
    sync_freeze_table, sync_unfreeze_table, sync_reshard_table,
    sync_flush_table, sync_compact_table,
    get_first_chunk_id, get_singular_chunk_id, get_chunk_replication_factor, multicell_sleep,
    update_nodes_dynamic_config, update_controller_agent_config,
    update_op_parameters, enable_op_detailed_logs,
    set_node_banned, set_banned_flag, set_account_disk_space_limit,
    check_all_stderrs,
    create_test_tables, create_dynamic_table, PrepareTables,
    get_statistics,
    make_random_string, raises_yt_error,
    build_snapshot,
    get_driver, execute_command)

from yt_type_helpers import make_schema

from yt.common import YtError
import yt.packages.requests as requests

import hashlib
import pytest

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

        rsp = requests.get("http://{}/read_skynet_part".format(http_address), params=kwargs)
        rsp.raise_for_status()
        return rsp.content

    @authors("prime")
    def test_http_checks_access(self):
        create("table", "//tmp/table")
        write_table("//tmp/table", [{"part_index": 0, "data": "abc"}])

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
        assert chunk["replicas"] > 0
        for node in info["nodes"]:
            node_id = node["node_id"]
            if node_id in chunk["replicas"]:
                break
        else:
            assert False, "Node not found: {}, {}".format(chunk["replicas"], str(info["nodes"]))

        assert "abc" == self.get_skynet_part(
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
            return data * (4 * 1024 * 1024 / len(data))

        write_table(
            "//tmp/table",
            [
                {"filename": "a", "part_index": 0, "data": to_skynet_chunk("a1")},
                {"filename": "a", "part_index": 1, "data": "a2"},
                {"filename": "b", "part_index": 0, "data": "b1"},
                {"filename": "c", "part_index": 0, "data": to_skynet_chunk("c1")},
                {"filename": "c", "part_index": 1, "data": to_skynet_chunk("c2")},
                {"filename": "c", "part_index": 2, "data": to_skynet_chunk("c3")},
                {"filename": "c", "part_index": 3, "data": to_skynet_chunk("c4")},
                {"filename": "c", "part_index": 4, "data": to_skynet_chunk("c5")},
                {"filename": "c", "part_index": 5, "data": "c6"},
            ],
        )

        info = locate_skynet_share("//tmp/table[#0:#9]")

        chunk_1 = info["chunk_specs"][0]["chunk_id"]
        node_1 = info["chunk_specs"][0]["replicas"][0]
        chunk_2 = info["chunk_specs"][1]["chunk_id"]
        node_2 = info["chunk_specs"][1]["replicas"][0]

        test_queries = [
            (node_1, chunk_1, to_skynet_chunk("a1") + "a2", 0, 2, 0),
            (node_1, chunk_1, "b1", 2, 3, 0),
            (
                node_1,
                chunk_1,
                to_skynet_chunk("c1") + to_skynet_chunk("c2") + to_skynet_chunk("c3"),
                3,
                6,
                0,
            ),
            (
                node_2,
                chunk_2,
                to_skynet_chunk("c4") + to_skynet_chunk("c5") + "c6",
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
            return data * (4 * 1024 * 1024 / len(data))

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
            assert hashlib.sha1(row["data"]).digest() == row["sha1"], str(row)

            file_content[row["filename"]] = file_content.get(row["filename"], "") + row["data"]
            assert hashlib.md5(file_content[row["filename"]]).digest() == row["md5"]

    @authors("aleksandra-zh")
    def test_chunk_merge_skynet_share(self):
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

        set("//tmp/table/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/table/@resource_usage/chunk_count") == 1)
        assert read_table("//tmp/table") == info

        chunk_ids = get("//tmp/table/@chunk_ids")
        assert get("#{0}/@shared_to_skynet".format(chunk_ids[0]))
