from yt_env_setup import YTEnvSetup

from yt_commands import (  # noqa
    authors, print_debug, wait, retry, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists, concatenate,
    create_account, remove_account,
    create_network_project, create_tmpdir, create_user, create_group, create_medium,
    create_pool, create_pool_tree, remove_pool_tree,
    create_data_center, create_rack, create_table, create_proxy_role,
    create_tablet_cell_bundle, remove_tablet_cell_bundle, create_tablet_cell, create_table_replica,
    make_ace, check_permission, add_member, remove_member, remove_group, remove_user,
    remove_network_project,
    make_batch_request, execute_batch, get_batch_error,
    start_transaction, abort_transaction, commit_transaction, lock,
    externalize, internalize,
    insert_rows, select_rows, lookup_rows, delete_rows, trim_rows, alter_table,
    read_file, write_file, read_table, write_table, write_local_file, read_blob_table,
    read_journal, write_journal, truncate_journal, wait_until_sealed,
    map, reduce, map_reduce, join_reduce, merge, vanilla, sort, erase, remote_copy,
    run_test_vanilla, run_sleeping_vanilla,
    abort_job, list_jobs, get_job, abandon_job, interrupt_job,
    get_job_fail_context, get_job_input, get_job_stderr, get_job_spec, get_job_input_paths,
    dump_job_context, poll_job_shell,
    abort_op, complete_op, suspend_op, resume_op,
    get_operation, list_operations, clean_operations,
    get_operation_cypress_path, scheduler_orchid_pool_path,
    scheduler_orchid_default_pool_tree_path, scheduler_orchid_operation_path,
    scheduler_orchid_default_pool_tree_config_path, scheduler_orchid_path,
    scheduler_orchid_node_path, scheduler_orchid_pool_tree_config_path, scheduler_orchid_pool_tree_path,
    mount_table, unmount_table, freeze_table, unfreeze_table, reshard_table, remount_table, generate_timestamp,
    reshard_table_automatic, wait_for_tablet_state, wait_for_cells,
    get_tablet_infos, get_table_pivot_keys, get_tablet_leader_address,
    sync_create_cells, sync_mount_table, sync_unmount_table,
    sync_freeze_table, sync_unfreeze_table, sync_reshard_table,
    sync_flush_table, sync_compact_table, sync_remove_tablet_cells,
    sync_reshard_table_automatic, sync_balance_tablet_cells,
    get_first_chunk_id, get_singular_chunk_id, get_chunk_replication_factor, multicell_sleep,
    update_nodes_dynamic_config, update_controller_agent_config,
    update_op_parameters, enable_op_detailed_logs,
    set_node_banned, set_banned_flag,
    set_account_disk_space_limit, set_node_decommissioned,
    get_account_disk_space, get_account_committed_disk_space,
    check_all_stderrs,
    create_test_tables, create_dynamic_table, PrepareTables,
    get_statistics, get_recursive_disk_space, get_chunk_owner_disk_space, cluster_resources_equal,
    make_random_string, raises_yt_error,
    build_snapshot, build_master_snapshots,
    gc_collect, is_multicell, clear_metadata_caches,
    get_driver, Driver, execute_command, generate_uuid,
    AsyncLastCommittedTimestamp, MinTimestamp)

from yt_type_helpers import make_schema

from yt.common import YtError
import yt.yson as yson

import pytest

from time import sleep

#################################################################


def _schematize_row(row, schema):
    result = {}
    for column in schema:
        name = column["name"]
        result[name] = row.get(name, yson.YsonEntity())
    return result


def _schematize_rows(rows, schema):
    return [_schematize_row(row, schema) for row in rows]


class TestChunkMerger(YTEnvSetup):
    NUM_MASTERS = 5
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True
    ENABLE_BULK_INSERT = True

    DELTA_MASTER_CONFIG = {
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True,
            "chunk_merger": {
                "max_chunks_to_merge": 5
            }
        }
    }

    def _abort_chunk_merger_txs(self):
        txs = []
        for tx in ls("//sys/transactions", attributes=["title"]):
            title = tx.attributes.get("title", "")
            if "Chunk merger" in title:
                txs.append(tx)

        assert len(txs) > 0
        for tx in txs:
            abort_transaction(tx)

    @authors("aleksandra-zh")
    def test_merge_attributes(self):
        create("table", "//tmp/t")

        assert not get("//tmp/t/@enable_chunk_merger")
        set("//tmp/t/@enable_chunk_merger", True)
        assert get("//tmp/t/@enable_chunk_merger")

        create_account("a")
        assert get("//sys/accounts/a/@merge_job_rate_limit") == 0
        set("//sys/accounts/a/@merge_job_rate_limit", 7)
        assert get("//sys/accounts/a/@merge_job_rate_limit") == 7

    @authors("aleksandra-zh")
    def test_merge1(self):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        create("table", "//tmp/t")
        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "d"})
        write_table("<append=true>//tmp/t", {"a": "e"})

        assert get("//tmp/t/@resource_usage/chunk_count") > 1
        rows = read_table("//tmp/t")

        set("//tmp/t/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)
        assert read_table("//tmp/t") == rows

    @authors("aleksandra-zh")
    def test_merge2(self):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        create("table", "//tmp/t")
        write_table("//tmp/t", [
            {"a": 10},
            {"b": 50}
        ])
        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        assert get("//tmp/t/@resource_usage/chunk_count") > 1
        rows = read_table("//tmp/t")

        set("//tmp/t/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)
        assert read_table("//tmp/t") == rows

    @authors("aleksandra-zh")
    def test_merge_job_counter(self):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        create("table", "//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        assert get("//tmp/t/@resource_usage/chunk_count") > 1
        rows = read_table("//tmp/t")

        set("//tmp/t/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t/@merge_job_counter") > 0)

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)
        assert read_table("//tmp/t") == rows
        wait(lambda: get("//tmp/t/@merge_job_counter") == 0)

    @authors("aleksandra-zh")
    def test_abort_merge_tx(self):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        create("table", "//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        rows = read_table("//tmp/t")

        self._abort_chunk_merger_txs()

        set("//tmp/t/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        rows = read_table("//tmp/t")
        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)
        assert read_table("//tmp/t") == rows

    @authors("aleksandra-zh")
    def test_merge_job_accounting(self):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        create_account("a")
        create("table", "//tmp/t1", attributes={"account": "a"})

        write_table("<append=true>//tmp/t1", {"a": "b"})
        write_table("<append=true>//tmp/t1", {"b": "c"})
        write_table("<append=true>//tmp/t1", {"c": "d"})

        create_account("b")
        create("table", "//tmp/t2", attributes={"account": "b"})

        write_table("<append=true>//tmp/t2", {"a": "b"})
        write_table("<append=true>//tmp/t2", {"b": "c"})
        write_table("<append=true>//tmp/t2", {"c": "d"})

        set("//tmp/t1/@enable_chunk_merger", True)
        set("//tmp/t2/@enable_chunk_merger", True)

        set("//sys/accounts/b/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t2/@resource_usage/chunk_count") == 1)
        assert get("//tmp/t1/@resource_usage/chunk_count") > 1

        set("//sys/accounts/a/@merge_job_rate_limit", 10)
        wait(lambda: get("//tmp/t1/@resource_usage/chunk_count") == 1)

        self._abort_chunk_merger_txs()

    @authors("aleksandra-zh")
    def test_copy_merge(self):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        create("table", "//tmp/t")
        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})

        copy("//tmp/t", "//tmp/t1")
        rows = read_table("//tmp/t")

        set("//tmp/t1/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t1/@resource_usage/chunk_count") == 1)
        assert get("//tmp/t/@resource_usage/chunk_count") > 1

        assert read_table("//tmp/t") == rows
        assert read_table("//tmp/t1") == rows

    @authors("aleksandra-zh")
    def test_schedule_again(self):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        create("table", "//tmp/t")
        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        assert get("//tmp/t/@resource_usage/chunk_count") > 1
        rows = read_table("//tmp/t")

        set("//tmp/t/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)
        assert read_table("//tmp/t") == rows

        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})
        assert get("//tmp/t/@resource_usage/chunk_count") > 1
        rows = read_table("//tmp/t")

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)
        assert read_table("//tmp/t") == rows

    @authors("aleksandra-zh")
    def test_merge_does_not_overwrite_data(self):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        create("table", "//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        set("//tmp/t/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t/@merge_job_counter") > 0)

        write_table("//tmp/t", {"q": "r"})
        write_table("<append=true>//tmp/t", {"w": "t"})
        write_table("<append=true>//tmp/t", {"e": "y"})

        rows = read_table("//tmp/t")

        assert get("//tmp/t/@resource_usage/chunk_count") > 1
        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)

        assert read_table("//tmp/t") == rows

    @authors("aleksandra-zh")
    def test_remove(self):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        create("table", "//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        assert get("//tmp/t/@resource_usage/chunk_count") > 1

        set("//tmp/t/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t/@merge_job_counter") > 0)

        remove("//tmp/t")

        # Just hope nothing crashes.
        sleep(5)

    @authors("aleksandra-zh")
    def test_schema(self):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "key", "type": "int64"},
                        {"name": "value", "type": "string"},
                    ]
                ),
            },
        )

        write_table("<append=true>//tmp/t", {"key": 1, "value": "a"})
        write_table("<append=true>//tmp/t", {"key": 2, "value": "b"})
        write_table("<append=true>//tmp/t", {"key": 3, "value": "c"})

        rows = read_table("//tmp/t")

        set("//tmp/t/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)

        assert read_table("//tmp/t") == rows

    @authors("aleksandra-zh")
    def test_sorted(self):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "key", "type": "int64", "sort_order": "ascending"},
                        {"name": "value", "type": "string"},
                    ]
                ),
            },
        )

        write_table("<append=true>//tmp/t", {"key": 1, "value": "a"})
        write_table("<append=true>//tmp/t", {"key": 2, "value": "b"})
        write_table("<append=true>//tmp/t", {"key": 3, "value": "c"})

        rows = read_table("//tmp/t")

        set("//tmp/t/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)

        assert read_table("//tmp/t") == rows

    @authors("aleksandra-zh")
    def test_merge_merge(self):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        create("table", "//tmp/t1")
        write_table("<append=true>//tmp/t1", {"a": "b"})
        write_table("<append=true>//tmp/t1", {"b": "c"})

        create("table", "//tmp/t2")
        write_table("<append=true>//tmp/t2", {"key": 1, "value": "a"})
        write_table("<append=true>//tmp/t2", {"key": 2, "value": "b"})

        create("table", "//tmp/t")
        merge(mode="unordered", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t")

        rows = read_table("//tmp/t")

        set("//tmp/t/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)

        assert read_table("//tmp/t") == rows

    @authors("aleksandra-zh")
    def test_merge_chunks_exceed_max_chunk_to_merge_limit(self):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        create("table", "//tmp/t")
        for i in range(10):
            write_table("<append=true>//tmp/t", {"a": "b"})

        assert get("//tmp/t/@resource_usage/chunk_count") == 10
        rows = read_table("//tmp/t")

        set("//tmp/t/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") <= 2)
        assert read_table("//tmp/t") == rows

        # Initiate another merge.
        write_table("<append=true>//tmp/t", {"a": "b"})
        rows = read_table("//tmp/t")

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)
        assert read_table("//tmp/t") == rows

    @authors("aleksandra-zh")
    def test_merge_job_rate_limit_permission(self):
        create_account("a")
        create_user("u")
        acl = [
            make_ace("allow", "u", ["use", "modify_children"], "object_only"),
            make_ace("allow", "u", ["write", "remove", "administer"], "descendants_only"),
        ]
        set("//sys/account_tree/a/@acl", acl)

        create_account("b", "a", authenticated_user="u")

        with pytest.raises(YtError):
            set("//sys/accounts/a/@merge_job_rate_limit", 10, authenticated_user="u")

        with pytest.raises(YtError):
            set("//sys/accounts/b/@merge_job_rate_limit", 10, authenticated_user="u")

        create("table", "//tmp/t")
        with pytest.raises(YtError):
            set("//tmp/t/@enable_chunk_merger", True, authenticated_user="u")

    @authors("aleksandra-zh")
    def test_do_not_crash_on_dynamic_table(self):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        sync_create_cells(1)
        create("table", "//tmp/t1")

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ]

        create_dynamic_table("//tmp/t2", schema=schema)
        sync_mount_table("//tmp/t2")

        insert_rows("//tmp/t2", [{"key": 1, "value": "1"}])
        insert_rows("//tmp/t2", [{"key": 2, "value": "1"}])

        sync_unmount_table("//tmp/t2")
        sync_mount_table("//tmp/t2")

        write_table("<append=true>//tmp/t1", {"key": 3, "value": "1"})

        map(in_="//tmp/t1", out="<append=true>//tmp/t2", command="cat")

        set("//tmp/t2/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        # Just do not crash, please.
        sleep(10)

    @authors("aleksandra-zh")
    def test_compression_codec(self):
        codec = "lz4"
        create("table", "//tmp/t", attributes={"compression_codec": codec})

        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert get("#{0}/@compression_codec".format(chunk_ids[0])) == codec

        rows = read_table("//tmp/t")

        set("//tmp/t/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)
        assert read_table("//tmp/t") == rows

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert get("#{0}/@compression_codec".format(chunk_ids[0])) == codec

    @authors("aleksandra-zh")
    def test_change_compression_codec(self):
        codec1 = "lz4"
        codec2 = "zstd_17"
        create("table", "//tmp/t", attributes={"compression_codec": codec1})

        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert get("#{0}/@compression_codec".format(chunk_ids[0])) == codec1

        rows = read_table("//tmp/t")

        set("//tmp/t/@compression_codec", codec2)

        set("//tmp/t/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)
        assert read_table("//tmp/t") == rows

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert get("#{0}/@compression_codec".format(chunk_ids[0])) == codec2

    @authors("aleksandra-zh")
    def test_erasure1(self):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        codec = "lrc_12_2_2"
        create("table", "//tmp/t", attributes={"erasure_codec": codec})

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        info = read_table("//tmp/t")

        set("//tmp/t/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)
        assert read_table("//tmp/t") == info

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert get("#{0}/@erasure_codec".format(chunk_ids[0])) == codec

    @authors("aleksandra-zh")
    def test_erasure2(self):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        codec = "lrc_12_2_2"
        none_codec = "none"
        create("table", "//tmp/t", attributes={"erasure_codec": codec})

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        info = read_table("//tmp/t")

        set("//tmp/t/@erasure_codec", none_codec)

        set("//tmp/t/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)
        assert read_table("//tmp/t") == info

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert not exists("#{0}/@erasure_codec".format(chunk_ids[0]))

    @authors("aleksandra-zh")
    def test_erasure3(self):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        codec = "lrc_12_2_2"
        none_codec = "none"
        create("table", "//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        set("//tmp/t/@erasure_codec", codec)
        write_table("<append=true>//tmp/t", {"b": "c"})
        set("//tmp/t/@erasure_codec", none_codec)
        write_table("<append=true>//tmp/t", {"c": "d"})

        info = read_table("//tmp/t")

        set("//tmp/t/@erasure_codec", codec)

        set("//tmp/t/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)
        assert read_table("//tmp/t") == info

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert get("#{0}/@erasure_codec".format(chunk_ids[0])) == codec

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_optimize_for(self, optimize_for):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)
        create("table", "//tmp/t", attributes={"optimize_for": optimize_for})

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "d"})

        assert get("//tmp/t/@resource_usage/chunk_count") > 1
        rows = read_table("//tmp/t")

        set("//tmp/t/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)
        assert read_table("//tmp/t") == rows

        chunk_format = "table_schemaless_horizontal" if optimize_for == "lookup" else "table_unversioned_columnar"
        chunk_ids = get("//tmp/t/@chunk_ids")
        assert get("#{0}/@chunk_format".format(chunk_ids[0])) == chunk_format

    @authors("babenko")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_nonstrict_schema(self, optimize_for):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        schema = make_schema(
            [
                {"name": "a", "type": "string"},
                {"name": "b", "type": "string"},
                {"name": "c", "type": "int64"}
            ],
            strict=False
        )
        create("table", "//tmp/t", attributes={"optimize_for": optimize_for, "schema": schema})

        rows1 = [{"a": "a" + str(i), "b": "b" + str(i), "c": i, "x": "x" + str(i)} for i in xrange(0, 10)]
        write_table("<append=true>//tmp/t", rows1)
        rows2 = [{"a": "a" + str(i), "b": "b" + str(i), "c": i, "y": "y" + str(i)} for i in xrange(10, 20)]
        write_table("<append=true>//tmp/t", rows2)
        rows3 = [{"a": "a" + str(i), "b": "b" + str(i), "c": i, "z": "z" + str(i)} for i in xrange(20, 30)]
        write_table("<append=true>//tmp/t", rows3)

        assert get("//tmp/t/@resource_usage/chunk_count") > 1
        assert read_table("//tmp/t") == rows1 + rows2 + rows3

        set("//tmp/t/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)
        assert read_table("//tmp/t") == rows1 + rows2 + rows3

        chunk_format = "table_schemaless_horizontal" if optimize_for == "lookup" else "table_unversioned_columnar"
        chunk_ids = get("//tmp/t/@chunk_ids")
        assert get("#{0}/@chunk_format".format(chunk_ids[0])) == chunk_format

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_alter_schema(self, optimize_for):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        schema1 = make_schema(
            [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"},
            ]
        )
        schema2 = make_schema(
            [
                {"name": "key", "type": "int64"},
                {"name": "another_value", "type": "string"},
                {"name": "value", "type": "string"},
            ]
        )

        create("table", "//tmp/t", attributes={"schema": schema1, "optimize_for": optimize_for})
        write_table("<append=true>//tmp/t", {"key": 1, "value": "a"})
        alter_table("//tmp/t", schema=schema2)
        write_table("<append=true>//tmp/t", {"key": 2, "another_value": "z", "value": "b"})
        write_table("<append=true>//tmp/t", {"key": 3, "value": "c"})
        create("table", "//tmp/t1", attributes={"schema": schema2, "optimize_for": optimize_for})
        write_table("<append=true>//tmp/t1", {"key": 4, "another_value": "x", "value": "e"})
        concatenate(["//tmp/t1", "//tmp/t", "//tmp/t1", "//tmp/t"], "//tmp/t")

        rows = read_table("//tmp/t")

        set("//tmp/t/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)
        merged_rows = read_table("//tmp/t")

        assert _schematize_rows(rows, schema2) == _schematize_rows(merged_rows, schema2)

    @authors("aleksandra-zh")
    def test_inherit_enable_chunk_merger(self):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        create("map_node", "//tmp/d")
        set("//tmp/d/@enable_chunk_merger", True)

        create("table", "//tmp/d/t")
        write_table("<append=true>//tmp/d/t", {"a": "b"})
        write_table("<append=true>//tmp/d/t", {"a": "c"})
        write_table("<append=true>//tmp/d/t", {"a": "d"})

        assert get("//tmp/d/t/@resource_usage/chunk_count") > 1
        info = read_table("//tmp/d/t")

        set("//tmp/d/@enable_chunk_merger", True)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/d/t/@resource_usage/chunk_count") == 1)
        assert read_table("//tmp/d/t") == info


class TestChunkMergerMulticell(TestChunkMerger):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestChunkMergerPortal(TestChunkMergerMulticell):
    ENABLE_TMP_PORTAL = True
    NUM_SECONDARY_MASTER_CELLS = 3
