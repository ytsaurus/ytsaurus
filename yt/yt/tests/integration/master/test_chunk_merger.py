from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, raises_yt_error, wait, create, ls, get, set, copy, remove,
    exists, concatenate, move, lookup_rows,
    create_account, create_user, make_ace, insert_rows,
    alter_table, read_table, write_table, map, merge,
    sync_create_cells, sync_mount_table, update_nodes_dynamic_config,
    start_transaction, abort_transaction, commit_transaction,
    sync_unmount_table, create_dynamic_table, wait_for_sys_config_sync,
    get_singular_chunk_id, get_driver)

from yt.test_helpers import assert_items_equal

from yt_helpers import (
    get_chunk_owner_master_cell_counters,
    profiler_factory)

from yt_type_helpers import make_schema

import yt.yson as yson

from yt.yson.yson_types import YsonMap

import pytest

from time import sleep

from functools import reduce

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
    ENABLE_MULTIDAEMON = False  # Use profiling. TODO(nadya73): Fix it.
    NUM_TEST_PARTITIONS = 16

    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True
    ENABLE_BULK_INSERT = True
    ENABLE_RPC_PROXY = True
    DRIVER_BACKEND = "rpc"

    DELTA_MASTER_CONFIG = {
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True,
        }
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "chunk_manager": {
            "chunk_merger": {
                "enable": True,
                "max_chunk_count": 5,
                "create_chunks_period": 100,
                "schedule_period": 100,
                "session_finalization_period": 100,
                "shallow_merge_validation_probability": 100,
            }
        }
    }

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        config["resource_limits"] = {
            "memory_limits": {
                "system_jobs": {
                    "type": "static",
                    "value": 1024**3,
                }
            }
        }

    def _get_chunk_merger_txs(self):
        txs = []
        for tx in ls("//sys/transactions", attributes=["title"]):
            title = tx.attributes.get("title", "")
            if "Chunk merger" in title:
                txs.append(tx)
        return txs

    def _abort_chunk_merger_txs(self):
        txs = self._get_chunk_merger_txs()
        for tx in txs:
            abort_transaction(tx)

    def _remove_merge_quotas(self, table_path):
        account = get("{}/@account".format(table_path))

        set("//sys/accounts/{}/@merge_job_rate_limit".format(account), 0)
        set("//sys/accounts/{}/@chunk_merger_node_traversal_concurrency".format(account), 0)

    def _wait_for_merge(self, table_path, merge_mode, max_merged_chunks=1, schema=None):
        assert get("{}/@chunk_count".format(table_path)) > 1

        rows = read_table(table_path)
        if schema is not None:
            rows = _schematize_rows(rows, schema)

        account = get("{}/@account".format(table_path))
        chunk_ids = get("{}/@chunk_ids".format(table_path))

        set("//sys/accounts/{}/@merge_job_rate_limit".format(account), 10)
        set("//sys/accounts/{}/@chunk_merger_node_traversal_concurrency".format(account), 1)
        if merge_mode is not None:
            set("{}/@chunk_merger_mode".format(table_path), merge_mode)

        wait(lambda: get("{}/@chunk_merger_status".format(table_path)) == "not_in_merge_pipeline")
        wait(lambda: get("{}/@chunk_ids".format(table_path)) != chunk_ids)
        wait(lambda:  get("{}/@chunk_count".format(table_path)) <= max_merged_chunks)

        merged_rows = read_table(table_path)
        if schema is not None:
            merged_rows = _schematize_rows(merged_rows, schema)

        assert merged_rows == rows

    @authors("cherepashka")
    def test_complex_row_values_for_shallow_merge_validation(self):
        schema = make_schema(
            [
                {"name": "a", "type": "any", "required": False},
                {"name": "b", "type": "any", "required": False},
            ],
            strict=False
        )
        create("table", "//tmp/t", attributes={"schema": schema})
        self._remove_merge_quotas("//tmp/t")
        for _ in range(4):
            write_table("<append=true>//tmp/t", YsonMap({"a": YsonMap({"a": "aba", "b": "caba"}), "b": "caba"}))
        self._wait_for_merge("//tmp/t", "auto")

    @authors("aleksandra-zh", "danilalexeev")
    def test_merge_attributes(self):
        create("table", "//tmp/t")

        assert get("//tmp/t/@chunk_merger_mode") == "none"
        set("//tmp/t/@chunk_merger_mode", "deep")
        assert get("//tmp/t/@chunk_merger_mode") == "deep"
        set("//tmp/t/@chunk_merger_mode", "shallow")
        assert get("//tmp/t/@chunk_merger_mode") == "shallow"
        set("//tmp/t/@chunk_merger_mode", "auto")
        assert get("//tmp/t/@chunk_merger_mode") == "auto"

        with raises_yt_error("Error parsing EChunkMergerMode"):
            set("//tmp/t/@chunk_merger_mode", "sdjkfhdskj")

        create_account("a")

        set("//sys/accounts/a/@merge_job_rate_limit", 7)
        with raises_yt_error("cannot be negative"):
            set("//sys/accounts/a/@merge_job_rate_limit", -1)
        assert get("//sys/accounts/a/@merge_job_rate_limit") == 7

        set("//sys/accounts/a/@chunk_merger_node_traversal_concurrency", 12)
        with raises_yt_error("cannot be negative"):
            set("//sys/accounts/a/@chunk_merger_node_traversal_concurrency", -1)
        assert get("//sys/accounts/a/@chunk_merger_node_traversal_concurrency") == 12

        create_account("d")

        data = {'max_chunk_count': 3, 'max_compressed_data_size': 7}
        set("//sys/accounts/d/@chunk_merger_criteria", data)
        assert get("//sys/accounts/d/@chunk_merger_criteria") == data

        with raises_yt_error("must be positive"):
            set("//sys/accounts/d/@chunk_merger_criteria/max_row_count", -1)
        set("//sys/accounts/d/@chunk_merger_criteria/max_row_count", 2)
        assert get("//sys/accounts/d/@chunk_merger_criteria/max_row_count") == 2

    @authors("danilalexeev")
    def test_remove_account_criteria(self):
        create_account("d")
        assert not exists("//sys/accounts/d/@chunk_merger_criteria")

        data = {'max_uncompressed_data_size': 512, 'max_input_chunk_data_weight': 128}
        set("//sys/accounts/d/@chunk_merger_criteria", data)
        assert get("//sys/accounts/d/@chunk_merger_criteria") == data

        remove("//sys/accounts/d/@chunk_merger_criteria")
        assert not exists("//sys/accounts/d/@chunk_merger_criteria")

    @authors("aleksandra-zh", "cherepashka")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    @pytest.mark.parametrize("transactional", [False, True])
    def test_merge1(self, merge_mode, transactional):
        create("table", "//tmp/t")
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "d"})
        write_table("<append=true>//tmp/t", {"a": "e"})

        if transactional:
            tx = start_transaction()
            set("//tmp/t/@chunk_merger_mode", merge_mode, tx=tx)
            assert get("//tmp/t/@chunk_merger_mode", tx=tx) == merge_mode
            assert get("//tmp/t/@chunk_merger_mode") == "none"

            commit_transaction(tx)
            assert get("//tmp/t/@chunk_merger_mode") == merge_mode
        else:
            set("//tmp/t/@chunk_merger_mode", merge_mode)

        self._wait_for_merge("//tmp/t", None)

        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#{}/@data_weight".format(chunk_id)) > 0

    @authors("aleksandra-zh", "cherepashka")
    @pytest.mark.parametrize("transactional", [False, True])
    def test_merge2(self, transactional):
        create("table", "//tmp/t")
        self._remove_merge_quotas("//tmp/t")

        write_table("//tmp/t", [
            {"a": 10},
            {"b": 50}
        ])
        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        if transactional:
            tx = start_transaction()
            set("//tmp/t/@chunk_merger_mode", "deep", tx=tx)
            assert get("//tmp/t/@chunk_merger_mode", tx=tx) == "deep"
            assert get("//tmp/t/@chunk_merger_mode") == "none"

            commit_transaction(tx)
            assert get("//tmp/t/@chunk_merger_mode") == "deep"
        else:
            set("//tmp/t/@chunk_merger_mode", "deep")

        self._wait_for_merge("//tmp/t", None)

    @authors("aleksandra-zh", "cherepashka")
    @pytest.mark.parametrize("transactional", [False, True])
    def test_auto_merge1(self, transactional):
        create("table", "//tmp/t", attributes={"compression_codec": "lz4"})
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "d"})

        set("//tmp/t/@compression_codec", "zstd_17")
        write_table("<append=true>//tmp/t", {"q": "e"})

        if transactional:
            tx = start_transaction()
            set("//tmp/t/@chunk_merger_mode", "auto", tx=tx)
            assert get("//tmp/t/@chunk_merger_mode", tx=tx) == "auto"
            assert get("//tmp/t/@chunk_merger_mode") == "none"

            commit_transaction(tx)
            assert get("//tmp/t/@chunk_merger_mode") == "auto"
        else:
            set("//tmp/t/@chunk_merger_mode", "auto")

        self._wait_for_merge("//tmp/t", None)

    @authors("aleksandra-zh", "cherepashka")
    @pytest.mark.parametrize("transactional", [False, True])
    def test_auto_merge2(self, transactional):
        create("table", "//tmp/t", attributes={"compression_codec": "lz4"})
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "d"})

        set("//tmp/t/@compression_codec", "zstd_17")
        write_table("<append=true>//tmp/t", {"q": "e"})

        set("//sys/@config/chunk_manager/chunk_merger/min_shallow_merge_chunk_count", 3)
        wait_for_sys_config_sync()

        if transactional:
            tx = start_transaction()
            set("//tmp/t/@chunk_merger_mode", "auto", tx=tx)
            assert get("//tmp/t/@chunk_merger_mode", tx=tx) == "auto"
            assert get("//tmp/t/@chunk_merger_mode") == "none"

            commit_transaction(tx)
            assert get("//tmp/t/@chunk_merger_mode") == "auto"
        else:
            set("//tmp/t/@chunk_merger_mode", "auto")

        self._wait_for_merge("//tmp/t", None, max_merged_chunks=2)

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_merge_static_to_dynamic(self, merge_mode):
        sync_create_cells(1)

        schema = make_schema(
            [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
            unique_keys=True
        )
        create("table", "//tmp/t", attributes={"schema": schema})
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"key": 1, "value": "a"})
        write_table("<append=true>//tmp/t", {"key": 2, "value": "b"})
        write_table("<append=true>//tmp/t", {"key": 3, "value": "c"})

        rows = read_table("//tmp/t")

        self._wait_for_merge("//tmp/t", merge_mode)
        alter_table("//tmp/t", dynamic=True)
        sync_mount_table("//tmp/t")

        keys = [{"key": i} for i in range(1, 4)]
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

        merged_rows = read_table("//tmp/t")
        assert _schematize_rows(rows, schema) == _schematize_rows(merged_rows, schema)

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_merge_remove(self, merge_mode):
        create("table", "//tmp/t")
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "d"})

        self._wait_for_merge("//tmp/t", merge_mode)

        for i in range(10):
            write_table("<append=true>//tmp/t", {"a": "b"})

        wait(lambda: get("//tmp/t/@chunk_merger_status") == "not_in_merge_pipeline")
        remove("//tmp/t")

        wait(lambda: get("//sys/chunk_lists/@count") == len(get("//sys/chunk_lists")))

    @authors("aleksandra-zh")
    def test_merge_does_not_conflict_with_tx_append(self):
        create("table", "//tmp/t")
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        tx = start_transaction()
        write_table("<append=true>//tmp/t", {"d": "e"}, tx=tx)
        rows = read_table("//tmp/t", tx=tx)

        self._wait_for_merge("//tmp/t", "deep")
        commit_transaction(tx)

        assert get("//tmp/t/@chunk_count") == 2
        assert read_table("//tmp/t") == rows

    @authors("aleksandra-zh")
    def test_merge_does_not_conflict_with_tx_overwrite(self):
        create("table", "//tmp/t")
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        tx = start_transaction()
        set("//tmp/t/@chunk_merger_mode", "deep")
        write_table("//tmp/t", {"d": "e"}, tx=tx)
        rows = read_table("//tmp/t", tx=tx)

        self._wait_for_merge("//tmp/t", None)
        commit_transaction(tx)

        assert get("//tmp/t/@chunk_count") == 1
        assert read_table("//tmp/t") == rows

    @authors("aleksandra-zh")
    def test_chunk_merger_status(self):
        create("table", "//tmp/t")
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        assert get("//tmp/t/@chunk_count") > 1
        rows = read_table("//tmp/t")

        self._wait_for_merge("//tmp/t", "deep")

        assert read_table("//tmp/t") == rows

    @authors("aleksandra-zh", "gritukan")
    def test_abort_merge_tx(self):
        create("table", "//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        rows = read_table("//tmp/t")

        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)
        set("//tmp/t/@chunk_merger_mode", "deep")

        for _ in range(10):
            wait(lambda: len(self._get_chunk_merger_txs()) > 0)
            self._abort_chunk_merger_txs()

        wait(lambda: get("//tmp/t/@chunk_count") == 1)
        assert read_table("//tmp/t") == rows

    @authors("aleksandra-zh")
    def test_merge_job_accounting1(self):
        create_account("a")
        create("table", "//tmp/t1", attributes={"account": "a"})
        self._remove_merge_quotas("//tmp/t1")

        write_table("<append=true>//tmp/t1", {"a": "b"})
        write_table("<append=true>//tmp/t1", {"b": "c"})
        write_table("<append=true>//tmp/t1", {"c": "d"})

        create_account("b")
        copy("//tmp/t1", "//tmp/t2")
        set("//tmp/t2/@account", "b")

        self._wait_for_merge("//tmp/t2", "deep")

        wait(lambda: get("//sys/accounts/b/@resource_usage/chunk_count") == 1)

        self._abort_chunk_merger_txs()

    @authors("aleksandra-zh")
    def test_merge_job_accounting2(self):
        create_account("a")
        create("table", "//tmp/t1", attributes={"account": "a"})
        self._remove_merge_quotas("//tmp/t1")

        write_table("<append=true>//tmp/t1", {"a": "b"})
        write_table("<append=true>//tmp/t1", {"b": "c"})
        write_table("<append=true>//tmp/t1", {"c": "d"})

        create_account("b")
        copy("//tmp/t1", "//tmp/t2")
        set("//tmp/t2/@account", "b")

        set("//tmp/t1/@chunk_merger_mode", "deep")
        self._wait_for_merge("//tmp/t2", "deep")

        assert get("//tmp/t1/@chunk_count") > 1

        set("//sys/accounts/a/@merge_job_rate_limit", 10)
        set("//sys/accounts/a/@chunk_merger_node_traversal_concurrency", 1)
        write_table("<append=true>//tmp/t1", {"c": "d"})

        wait(lambda: get("//tmp/t1/@chunk_count") == 1)

        self._abort_chunk_merger_txs()

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_copy_merge1(self, merge_mode):
        create("table", "//tmp/t")
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})

        copy("//tmp/t", "//tmp/t1")
        rows = read_table("//tmp/t")

        self._wait_for_merge("//tmp/t1", merge_mode)

        assert get("//tmp/t/@chunk_count") > 1
        assert read_table("//tmp/t") == rows

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_copy_merge2(self, merge_mode):
        create("table", "//tmp/t")
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "c"})

        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)
        set("//tmp/t/@chunk_merger_mode", merge_mode)

        copy("//tmp/t", "//tmp/t1")

        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)

        wait(lambda: get("//tmp/t/@chunk_count") == 1)
        wait(lambda: get("//tmp/t1/@chunk_count") == 1)

        assert get("//tmp/t/@chunk_list_id") != get("//tmp/t1/@chunk_list_id")

    @authors("aleksandra-zh")
    def test_copy_move(self):
        create("table", "//tmp/t")
        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "d"})

        set("//tmp/t/@chunk_merger_mode", "deep")
        copy("//tmp/t", "//tmp/t1")
        move("//tmp/t1", "//tmp/t2")

        assert get("//tmp/t2/@chunk_merger_mode") == "deep"

    @authors("aleksandra-zh")
    def test_schedule_again(self):
        create("table", "//tmp/t")
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        self._wait_for_merge("//tmp/t", "deep")

        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        rows = read_table("//tmp/t")
        wait(lambda: get("//tmp/t/@chunk_count") == 1)

        assert read_table("//tmp/t") == rows

    @authors("danilalexeev")
    def test_account_priority(self):
        create_account("d")
        set("//sys/accounts/d/@chunk_merger_criteria/max_row_count", 3)
        create("table", "//tmp/t", attributes={"account": "d"})

        write_table("//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})
        write_table("<append=true>//tmp/t", {"q": "d"})

        set("//sys/accounts/d/@merge_job_rate_limit", 10)
        set("//sys/accounts/d/@chunk_merger_node_traversal_concurrency", 1)
        set("//tmp/t/@chunk_merger_mode", "deep")

        try:
            # [{"a": "b"}, {"b": "c"}], [{"c": "d"}, {"q": "d"}]
            wait(lambda: get("//tmp/t/@chunk_count") == 2)
        finally:
            self._abort_chunk_merger_txs()

    @authors("aleksandra-zh")
    def test_chunk_tail(self):
        set("//sys/@config/chunk_manager/chunk_merger/max_chunk_count", 2)
        set("//sys/@config/chunk_manager/chunk_merger/max_row_count", 4)
        wait_for_sys_config_sync()

        create("table", "//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})
        write_table("<append=true>//tmp/t", {"q": "d"})

        rows = [{"a" : "b"} for _ in range(10)]
        write_table("<append=true>//tmp/t", rows)
        write_table("<append=true>//tmp/t", rows)

        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)
        set("//tmp/t/@chunk_merger_mode", "deep")

        # [{"a": "b"}, {"b": "c"}], [{"c": "d"}, {"q": "d"}], rows, rows
        wait(lambda: get("//tmp/t/@chunk_count") == 4)
        wait(lambda: get("//tmp/t/@chunk_merger_status") == "not_in_merge_pipeline")

        traversal_info1 = get("//tmp/t/@chunk_merger_traversal_info")
        assert traversal_info1["chunk_count"] > 0

        write_table("<append=true>//tmp/t", {"c": "d"})
        write_table("<append=true>//tmp/t", {"q": "d"})

        # [{"a": "b"}, {"b": "c"}], [{"c": "d"}, {"q": "d"}], rows, rows, [{"c": "d"}, {"q": "d"}]
        wait(lambda: get("//tmp/t/@chunk_count") == 5)

        traversal_info2 = get("//tmp/t/@chunk_merger_traversal_info")
        assert traversal_info2["chunk_count"] > traversal_info1["chunk_count"]
        assert traversal_info2["config_version"] == traversal_info1["config_version"]

        set("//sys/@config/chunk_manager/chunk_merger/max_chunk_count", 10)
        set("//sys/@config/chunk_manager/chunk_merger/max_row_count", 100)
        wait_for_sys_config_sync()

        write_table("<append=true>//tmp/t", {"q": "d"})

        wait(lambda: get("//tmp/t/@chunk_count") == 1)
        wait(lambda: get("//tmp/t/@chunk_merger_status") == "not_in_merge_pipeline")

        traversal_info3 = get("//tmp/t/@chunk_merger_traversal_info")
        assert traversal_info3["config_version"] > traversal_info2["config_version"]

    @authors("aleksandra-zh")
    def test_chunks_from_one_chunklist(self):
        set("//sys/@config/chunk_manager/chunk_merger/reschedule_merge_on_success", True)

        create("table", "//tmp/t")
        self._remove_merge_quotas("//tmp/t")
        for _ in range(32):
            write_table("<append=true>//tmp/t", {"a": "b"})

        concatenate(["//tmp/t", "//tmp/t"], "//tmp/t")

        get("//tmp/t/@chunk_count")

        set("//sys/@config/chunk_manager/chunk_merger/max_jobs_per_chunklist", 2)

        set("//sys/@config/chunk_manager/chunk_merger/max_chunk_count", 10)
        self._wait_for_merge("//tmp/t", "deep")

        assert get("//tmp/t/@chunk_count") == 1

    @authors("cookiedoth")
    @pytest.mark.parametrize("with_erasure", [False, True])
    def test_multiple_merge(self, with_erasure):
        if with_erasure:
            create("table", "//tmp/t", attributes={"erasure_codec": "lrc_12_2_2"})
        else:
            create("table", "//tmp/t")
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)
        set("//tmp/t/@chunk_merger_mode", "deep")

        for i in range(3):
            write_table("<append=true>//tmp/t", {str(2 * i): str(2 * i)})
            wait(lambda: get("//tmp/t/@chunk_count") == 1)

    @authors("aleksandra-zh")
    def test_merge_does_not_overwrite_data(self):
        create("table", "//tmp/t")

        all_rows = [{"a{}".format(i): "b{}".format(i)} for i in range(6)]

        write_table("<append=true>//tmp/t", all_rows[0])
        write_table("<append=true>//tmp/t", all_rows[1])
        write_table("<append=true>//tmp/t", all_rows[2])

        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)
        set("//tmp/t/@chunk_merger_mode", "deep")

        write_table("//tmp/t", all_rows[3])
        write_table("<append=true>//tmp/t", all_rows[4])
        write_table("<append=true>//tmp/t", all_rows[5])

        wait(lambda: get("//tmp/t/@chunk_count") == 1)

        assert read_table("//tmp/t") == all_rows[3:6]

    @authors("aleksandra-zh")
    def test_remove(self):
        create("table", "//tmp/t")
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        assert get("//tmp/t/@chunk_count") > 1

        self._wait_for_merge("//tmp/t", "deep")

        remove("//tmp/t")

        # Just hope nothing crashes.
        sleep(5)

    @authors("aleksandra-zh")
    def test_better_remove(self):
        create("table", "//tmp/t")
        # To preserve chunklist.
        copy("//tmp/t", "//tmp/t1")

        set("//sys/@config/chunk_manager/chunk_merger/max_chunks_per_iteration", 7)
        set("//sys/@config/chunk_manager/chunk_merger/delay_between_iterations", 3000)

        for i in range(10):
            write_table("<append=true>//tmp/t", {"a": "b"})

        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)
        set("//tmp/t/@chunk_merger_mode", "deep")

        sleep(1)

        remove("//tmp/t")

        # Just hope nothing crashes.
        sleep(5)

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_schema(self, merge_mode):
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
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"key": 1, "value": "a"})
        write_table("<append=true>//tmp/t", {"key": 2, "value": "b"})
        write_table("<append=true>//tmp/t", {"key": 3, "value": "c"})

        self._wait_for_merge("//tmp/t", merge_mode)

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_sorted(self, merge_mode):
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
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"key": 1, "value": "a"})
        write_table("<append=true>//tmp/t", {"key": 2, "value": "b"})
        write_table("<append=true>//tmp/t", {"key": 3, "value": "c"})

        self._wait_for_merge("//tmp/t", merge_mode)

    @authors("aleksandra-zh")
    def test_merge_merge(self):
        create("table", "//tmp/t1")

        write_table("<append=true>//tmp/t1", {"a": "b"})
        write_table("<append=true>//tmp/t1", {"b": "c"})

        create("table", "//tmp/t2")
        write_table("<append=true>//tmp/t2", {"key": 1, "value": "a"})
        write_table("<append=true>//tmp/t2", {"key": 2, "value": "b"})

        create("table", "//tmp/t")
        self._remove_merge_quotas("//tmp/t")

        merge(mode="unordered", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t")

        self._wait_for_merge("//tmp/t", "deep")

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_merge_chunks_exceed_max_chunk_to_merge_limit(self, merge_mode):
        create("table", "//tmp/t")
        self._remove_merge_quotas("//tmp/t")
        for i in range(10):
            write_table("<append=true>//tmp/t", {"a": "b"})

        assert get("//tmp/t/@chunk_count") == 10
        rows = read_table("//tmp/t")

        self._wait_for_merge("//tmp/t", merge_mode, max_merged_chunks=2)
        assert read_table("//tmp/t") == rows

        # Initiate another merge.
        write_table("<append=true>//tmp/t", {"a": "b"})
        rows = read_table("//tmp/t")

        wait(lambda: get("//tmp/t/@chunk_count") == 1)
        assert read_table("//tmp/t") == rows

    @authors("aleksandra-zh")
    def test_max_nodes_being_merged(self):
        create("table", "//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        set("//sys/@config/chunk_manager/chunk_merger/enable_queue_size_limit_changes", True)
        set("//sys/@config/chunk_manager/chunk_merger/max_nodes_being_merged", 0)

        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)
        set("//tmp/t/@chunk_merger_mode", "deep")

        wait(lambda: get("//tmp/t/@chunk_merger_status") == "awaiting_merge")
        assert (get("//tmp/t/@chunk_count") == 3)

        set("//sys/@config/chunk_manager/chunk_merger/max_nodes_being_merged", 10)
        # Trigger merge again.
        set("//tmp/t/@chunk_merger_mode", "deep")
        wait(lambda: get("//tmp/t/@chunk_count") == 1)

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

        with raises_yt_error("Access denied"):
            set("//sys/accounts/a/@merge_job_rate_limit", 10, authenticated_user="u")
        with raises_yt_error("Access denied"):
            set("//sys/accounts/a/@chunk_merger_node_traversal_concurrency", 10, authenticated_user="u")

        with raises_yt_error("Access denied"):
            set("//sys/accounts/b/@merge_job_rate_limit", 10, authenticated_user="u")
        with raises_yt_error("Access denied"):
            set("//sys/accounts/b/@chunk_merger_node_traversal_concurrency", 10, authenticated_user="u")

        create("table", "//tmp/t")
        create("map_node", "//tmp/m")

        with raises_yt_error("Access denied"):
            set("//tmp/t/@chunk_merger_mode", "shallow", authenticated_user="u")
        with raises_yt_error("Access denied"):
            set("//tmp/t/@chunk_merger_mode", "deep", authenticated_user="u")
        with raises_yt_error("Access denied"):
            set("//tmp/m/@chunk_merger_mode", "shallow", authenticated_user="u")

        set("//sys/@config/chunk_manager/chunk_merger/allow_setting_chunk_merger_mode", True)

        set("//tmp/t/@chunk_merger_mode", "shallow", authenticated_user="u")
        set("//tmp/t/@chunk_merger_mode", "deep", authenticated_user="u")
        set("//tmp/m/@chunk_merger_mode", "shallow", authenticated_user="u")

    @authors("aleksandra-zh")
    def test_ban_from_using_chunk_merger(self):
        set("//sys/@config/chunk_manager/chunk_merger/respect_account_specific_toggle", True)

        create("table", "//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        set("//sys/accounts/tmp/@allow_using_chunk_merger", False)

        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)
        set("//tmp/t/@chunk_merger_mode", "deep")

        wait(lambda: get("//tmp/t/@chunk_merger_status") == "not_in_merge_pipeline")
        assert (get("//tmp/t/@chunk_count") == 3)

        set("//sys/accounts/tmp/@allow_using_chunk_merger", True)
        # Trigger merge again.
        set("//tmp/t/@chunk_merger_mode", "deep")
        wait(lambda: get("//tmp/t/@chunk_count") == 1)

    @authors("aleksandra-zh")
    def test_do_not_crash_on_dynamic_table(self):
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

        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)
        set("//tmp/t2/@chunk_merger_mode", "deep")

        # Just do not crash, please.
        sleep(10)

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_compression_codec(self, merge_mode):
        codec = "lz4"
        create("table", "//tmp/t", attributes={"compression_codec": codec})
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b", "b": "c"})
        write_table("<append=true>//tmp/t", {"a": "d", "b": "e"})

        for chunk_id in get("//tmp/t/@chunk_ids"):
            assert get("#{}/@compression_codec".format(chunk_id)) == codec

        self._wait_for_merge("//tmp/t", merge_mode)

        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#{}/@compression_codec".format(chunk_id)) == codec

    @authors("aleksandra-zh")
    def test_change_compression_codec(self):
        codec1 = "lz4"
        codec2 = "zstd_17"
        create("table", "//tmp/t", attributes={"compression_codec": codec1})
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"b": "c"})
        write_table("<append=true>//tmp/t", {"c": "d"})

        for chunk_id in get("//tmp/t/@chunk_ids"):
            assert get("#{}/@compression_codec".format(chunk_id)) == codec1

        set("//tmp/t/@compression_codec", codec2)

        self._wait_for_merge("//tmp/t", "deep")

        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#{}/@compression_codec".format(chunk_id)) == codec2

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_erasure1(self, merge_mode):
        codec = "lrc_12_2_2"
        create("table", "//tmp/t", attributes={"erasure_codec": codec})
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "d"})

        self._wait_for_merge("//tmp/t", merge_mode)

        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#{}/@erasure_codec".format(chunk_id)) == codec

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_erasure2(self, merge_mode):
        codec = "lrc_12_2_2"
        none_codec = "none"
        create("table", "//tmp/t", attributes={"erasure_codec": codec})
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "d"})

        set("//tmp/t/@erasure_codec", none_codec)

        self._wait_for_merge("//tmp/t", merge_mode)

        chunk_id = get_singular_chunk_id("//tmp/t")
        assert not exists("#{}/@erasure_codec".format(chunk_id))

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_erasure3(self, merge_mode):
        codec = "lrc_12_2_2"
        none_codec = "none"
        create("table", "//tmp/t")
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        set("//tmp/t/@erasure_codec", codec)
        write_table("<append=true>//tmp/t", {"a": "c"})
        set("//tmp/t/@erasure_codec", none_codec)
        write_table("<append=true>//tmp/t", {"a": "d"})

        set("//tmp/t/@erasure_codec", codec)

        self._wait_for_merge("//tmp/t", merge_mode)

        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#{}/@erasure_codec".format(chunk_id)) == codec

    @authors("aleksandra-zh")
    @pytest.mark.parametrize(
        "optimize_for, merge_mode",
        [("scan", "deep"), ("scan", "shallow"), ("lookup", "deep"), ("lookup", "shallow")]
    )
    def test_optimize_for(self, optimize_for, merge_mode):
        create("table", "//tmp/t", attributes={"optimize_for": optimize_for})
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "d"})

        self._wait_for_merge("//tmp/t", merge_mode)

        chunk_format = "table_unversioned_schemaless_horizontal" if optimize_for == "lookup" else "table_unversioned_columnar"
        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#{}/@chunk_format".format(chunk_id)) == chunk_format

    @authors("aleksandra-zh")
    @pytest.mark.parametrize(
        "optimize_for, merge_mode",
        [("scan", "deep"), ("scan", "shallow"), ("lookup", "deep"), ("lookup", "shallow")]
    )
    def test_read_rows(self, optimize_for, merge_mode):
        create("table", "//tmp/t", attributes={"optimize_for": optimize_for})
        self._remove_merge_quotas("//tmp/t")
        set("//tmp/t/@chunk_writer", {"block_size": 1})

        write_table("<append=true>//tmp/t", [{"a": "z"}, {"b": "a"}, {"c": "q"}])
        write_table("<append=true>//tmp/t", [{"a": "x"}, {"b": "s"}, {"c": "w"}])
        write_table("<append=true>//tmp/t", [{"a": "c"}, {"b": "d"}, {"c": "e"}])

        self._wait_for_merge("//tmp/t", merge_mode)

        assert read_table("//tmp/t[#-1:#2]") == [{"a": "z"}, {"b": "a"}]
        assert read_table("//tmp/t[#2:#4]") == [{"c": "q"}, {"a": "x"}]
        assert read_table("//tmp/t[#5:#8]") == [{"c": "w"}, {"a": "c"}, {"b": "d"}]
        assert read_table("//tmp/t[#4:#5]") == [{"b": "s"}]
        assert read_table("//tmp/t[#7:#11]") == [{"b": "d"}, {"c": "e"}]

    @authors("aleksandra-zh")
    @pytest.mark.parametrize(
        "optimize_for, merge_mode",
        [("scan", "deep"), ("scan", "shallow"), ("lookup", "deep"), ("lookup", "shallow")]
    )
    def test_row_key_selector(self, optimize_for, merge_mode):
        create("table", "//tmp/t", attributes={"optimize_for": optimize_for})
        self._remove_merge_quotas("//tmp/t")
        set("//tmp/t/@chunk_writer", {"block_size": 1})

        v1 = {"s": "a", "i": 0, "d": 15.5}
        v2 = {"s": "a", "i": 10, "d": 15.2}
        v3 = {"s": "b", "i": 5, "d": 20.0}
        write_table("<append=true>//tmp/t", [v1, v2], sorted_by=["s", "i", "d"])

        v4 = {"s": "b", "i": 20, "d": 20.0}
        v5 = {"s": "c", "i": -100, "d": 10.0}
        write_table("<append=true>//tmp/t", [v3, v4, v5], sorted_by=["s", "i", "d"])

        self._wait_for_merge("//tmp/t", merge_mode)

        assert read_table("//tmp/t[a : a]") == []
        assert read_table("//tmp/t[(a, 1) : (a, 10)]") == []
        assert read_table("//tmp/t[b : a]") == []
        assert read_table("//tmp/t[(c, 0) : (a, 10)]") == []
        assert read_table("//tmp/t[(a, 10, 1e7) : (b, )]") == []

        assert read_table("//tmp/t[c:]") == [v5]
        assert read_table("//tmp/t[:(a, 10)]") == [v1]
        assert read_table("//tmp/t[:(a, 10),:(a, 10)]") == [v1, v1]
        assert read_table("//tmp/t[:(a, 11)]") == [v1, v2]
        assert read_table("//tmp/t[:]") == [v1, v2, v3, v4, v5]
        assert read_table("//tmp/t[a : b , b : c]") == [v1, v2, v3, v4]
        assert read_table("//tmp/t[a]") == [v1, v2]
        assert read_table("//tmp/t[(a,10)]") == [v2]
        assert read_table("//tmp/t[a,c]") == [v1, v2, v5]

        assert read_table("//tmp/t{s, d}[aa: (b, 10)]") == [{"s": "b", "d": 20.0}]
        assert read_table("//tmp/t[#0:c]") == [v1, v2, v3, v4]

    @authors("aleksandra-zh")
    @pytest.mark.parametrize(
        "optimize_for, merge_mode",
        [("scan", "deep"), ("scan", "shallow"), ("lookup", "deep"), ("lookup", "shallow")]
    )
    def test_column_selector(self, optimize_for, merge_mode):
        create("table", "//tmp/t", attributes={"optimize_for": optimize_for})
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"a": 1, "aa": 2, "b": 3, "bb": 4, "c": 5})
        write_table("<append=true>//tmp/t", {"a": 11, "aa": 22, "b": 33, "bb": 44, "c": 55})
        write_table("<append=true>//tmp/t", {"a": 111, "aa": 222, "b": 333, "bb": 444, "c": 555})

        copy("//tmp/t", "//tmp/t1")
        self._wait_for_merge("//tmp/t", merge_mode)

        assert get("//tmp/t1/@chunk_count") > 1

        assert read_table("//tmp/t{}") == read_table("//tmp/t1{}")

        assert read_table("//tmp/t{a}") == read_table("//tmp/t1{a}")
        assert read_table("//tmp/t{a, }") == read_table("//tmp/t1{a, }")
        assert read_table("//tmp/t{a, a}") == read_table("//tmp/t1{a, a}")
        assert read_table("//tmp/t{c, b}") == read_table("//tmp/t1{c, b}")
        assert read_table("//tmp/t{zzzzz}") == read_table("//tmp/t1{zzzzz}")

        assert read_table("//tmp/t{a}") == read_table("//tmp/t1{a}")
        assert read_table("//tmp/t{a, }") == read_table("//tmp/t1{a, }")
        assert read_table("//tmp/t{a, a}") == read_table("//tmp/t1{a, a}")
        assert read_table("//tmp/t{c, b}") == read_table("//tmp/t1{c, b}")
        assert read_table("//tmp/t{zzzzz}") == read_table("//tmp/t1{zzzzz}")

    @authors("babenko", "h0pless")
    @pytest.mark.parametrize(
        "optimize_for, merge_mode",
        [("scan", "auto"), ("lookup" , "auto"), ("scan", "deep"), ("lookup" , "deep")]
    )
    def test_nonstrict_schema(self, optimize_for, merge_mode):
        schema = make_schema(
            [
                {"name": "a", "type": "string"},
                {"name": "b", "type": "string"},
                {"name": "c", "type": "int64"}
            ],
            strict=False
        )
        create("table", "//tmp/t", attributes={"optimize_for": optimize_for, "schema": schema})
        self._remove_merge_quotas("//tmp/t")

        rows1 = [{"a": "a" + str(i), "b": "b" + str(i), "c": i, "x": "x" + str(i)} for i in range(0, 10)]
        write_table("<append=true>//tmp/t", rows1)
        rows2 = [{"a": "a" + str(i), "b": "b" + str(i), "c": i, "y": "y" + str(i)} for i in range(10, 20)]
        write_table("<append=true>//tmp/t", rows2)
        rows3 = [{"a": "a" + str(i), "b": "b" + str(i), "c": i, "z": "z" + str(i)} for i in range(20, 30)]
        write_table("<append=true>//tmp/t", rows3)
        assert read_table("//tmp/t") == rows1 + rows2 + rows3

        fallback_counters = get_chunk_owner_master_cell_counters("//tmp/t", "chunk_server/chunk_merger/auto_merge_fallback_count")

        self._wait_for_merge("//tmp/t", merge_mode)

        if merge_mode == "auto":
            wait(lambda: sum(counter.get_delta() for counter in fallback_counters) > 0)

        chunk_format = "table_unversioned_schemaless_horizontal" if optimize_for == "lookup" else "table_unversioned_columnar"
        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#{}/@chunk_format".format(chunk_id)) == chunk_format

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_alter_schema(self, optimize_for):
        set("//sys/@config/chunk_manager/chunk_merger/max_chunk_count", 10)
        wait_for_sys_config_sync()

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
        self._remove_merge_quotas("//tmp/t")

        write_table("<append=true>//tmp/t", {"key": 1, "value": "a"})
        alter_table("//tmp/t", schema=schema2)
        write_table("<append=true>//tmp/t", {"key": 2, "another_value": "z", "value": "b"})
        write_table("<append=true>//tmp/t", {"key": 3, "value": "c"})
        create("table", "//tmp/t1", attributes={"schema": schema2, "optimize_for": optimize_for})
        write_table("<append=true>//tmp/t1", {"key": 4, "another_value": "x", "value": "e"})
        concatenate(["//tmp/t1", "//tmp/t", "//tmp/t1", "//tmp/t"], "//tmp/t")

        self._wait_for_merge("//tmp/t", "deep", schema=schema2)

    @authors("aleksandra-zh")
    def test_alter_schema_auto(self):
        set("//sys/@config/chunk_manager/chunk_merger/max_chunk_count", 10)
        wait_for_sys_config_sync()

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

        create("table", "//tmp/t", attributes={"schema": schema1})
        write_table("<append=true>//tmp/t", {"key": 1, "value": "a"})
        alter_table("//tmp/t", schema=schema2)
        write_table("<append=true>//tmp/t", {"key": 2, "another_value": "z", "value": "b"})
        write_table("<append=true>//tmp/t", {"key": 3, "value": "c"})
        create("table", "//tmp/t1", attributes={"schema": schema2})
        write_table("<append=true>//tmp/t1", {"key": 4, "another_value": "x", "value": "e"})
        concatenate(["//tmp/t1", "//tmp/t", "//tmp/t1", "//tmp/t"], "//tmp/t")

        rows = read_table("//tmp/t")

        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)
        set("//tmp/t/@chunk_merger_mode", "shallow")

        # Must not crash.
        sleep(5)

        wait(lambda: get("//tmp/t/@chunk_merger_status") == "not_in_merge_pipeline")
        merged_rows = read_table("//tmp/t")
        assert _schematize_rows(rows, schema2) == _schematize_rows(merged_rows, schema2)

    @authors("aleksandra-zh")
    def test_different_key_column_count(self):
        set("//sys/@config/chunk_manager/chunk_merger/max_chunk_count", 10)
        wait_for_sys_config_sync()

        schema1 = make_schema(
            [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ]
        )
        schema2 = make_schema(
            [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string", "sort_order": "ascending"},
            ]
        )
        schema3 = make_schema(
            [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "qw", "type": "string", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ]
        )

        create("table", "//tmp/t1", attributes={"schema": schema2})
        write_table("<append=true>//tmp/t1", {"key": 1, "value": "a"})
        write_table("<append=true>//tmp/t1", {"key": 2, "value": "b"})

        create("table", "//tmp/t2", attributes={"schema": schema1})
        self._remove_merge_quotas("//tmp/t2")

        concatenate(["//tmp/t1"], "//tmp/t2")

        self._wait_for_merge("//tmp/t2", "auto", 1, schema1)

        create("table", "//tmp/t3", attributes={"schema": schema1})
        self._remove_merge_quotas("//tmp/t3")

        write_table("<append=true>//tmp/t3", {"key": 1, "value": "a"})
        write_table("<append=true>//tmp/t3", {"key": 2, "value": "b"})
        alter_table("//tmp/t3", schema=schema3)

        self._wait_for_merge("//tmp/t3", "auto", 1, schema3)

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_inherit_chunk_merger_mode(self, merge_mode):
        create("map_node", "//tmp/d")
        set("//tmp/d/@chunk_merger_mode", merge_mode)

        create("table", "//tmp/d/t")
        assert get("//tmp/d/t/@chunk_merger_mode") == merge_mode
        write_table("<append=true>//tmp/d/t", {"a": "b"})
        write_table("<append=true>//tmp/d/t", {"a": "c"})
        write_table("<append=true>//tmp/d/t", {"a": "d"})

        assert get("//tmp/d/t/@chunk_count") > 1
        rows = read_table("//tmp/d/t")

        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)

        wait(lambda: get("//tmp/d/t/@chunk_count") == 1)
        assert read_table("//tmp/d/t") == rows

    @authors("vovamelnikov")
    def test_queue_profiling(self):
        set("//sys/@config/chunk_manager/chunk_merger/max_chunks_per_iteration", 0)
        wait_for_sys_config_sync()

        accounts = [f"a{i}" for i in range(5)]

        def __divide_limit(val):
            if isinstance(val, dict):
                return {key: __divide_limit(val[key]) for key in val}
            else:
                return val // 10

        tmp_limits = get("//sys/accounts/tmp/@resource_limits")
        limits = __divide_limit(tmp_limits)
        for account in accounts:
            create_account(account, "tmp")
            set(f"//sys/accounts/{account}/@resource_limits", limits)

        for i in range(10):
            create("table", f"//tmp/t{i}")

        primary_addresses = ls("//sys/primary_masters")
        profilers = [profiler_factory().at_primary_master(master_address) for master_address in primary_addresses]

        secondary_masters = get("//sys/secondary_masters")
        for tag in secondary_masters:
            addrs = [address for address in secondary_masters[tag]]
            profilers += [profiler_factory().at_secondary_master(tag, master_address) for master_address in addrs]

        for i in range(10):
            for _j in range(2):
                write_table(f"<append=true>//tmp/t{i}", {"a": "b"})
            set(f"//tmp/t{i}/@account", accounts[i // 2])
            # Trigger merge.
            set(f"//tmp/t{i}/@chunk_merger_mode", 'deep')

        # Wait for next iteration of merger.
        sleep(2)

        nodes_being_merged_list = list(
            reduce(
                lambda a, b: a + b,
                [profiler.gauge("chunk_server/chunk_merger/nodes_being_merged").get_all() for profiler in profilers],
            )
        )

        def __alive_account(account):
            return account[0] != "<" and account[-1] != ">"

        nodes_being_merged_by_alive_accounts = [
            item for item in nodes_being_merged_list if item["tags"].keys() == {"account"} and __alive_account(item["tags"]["account"])
        ]

        def get_entire_number(items):
            return reduce(lambda a, b: a + b, [item["value"] for item in items])

        for i in range(5):
            selected_items = [item for item in nodes_being_merged_by_alive_accounts if item["tags"]["account"] == accounts[i]]
            assert get_entire_number(selected_items) == 2.0

    @authors("cherepashka")
    def test_prohibition_of_merging_chunks_with_hunk_columns(self):
        schema = [
            {"name": "column", "type": "string", "sort_order": "ascending"},
            {"name": "hunk_column", "type": "any", "max_inline_hunk_size": 5},
        ]

        create("table", "//tmp/t", attributes={"schema": schema})
        write_table("//tmp/t", {"column": "123456"})
        chunk_ids = get("//tmp/t/@chunk_ids")

        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//tmp/t/@chunk_merger_mode", "deep")

        assert get("//tmp/t/@chunk_merger_status") == "not_in_merge_pipeline"
        assert get("//tmp/t/@chunk_ids") == chunk_ids

    @authors("cherepashka")
    def test_alter_with_hunk_columns(self):
        schema = [
            {"name": "column", "type": "string", "sort_order": "ascending"},
        ]

        schema_with_hunk_columns = [
            {"name": "column", "type": "string", "sort_order": "ascending"},
            {"name": "hunk_column", "type": "any", "max_inline_hunk_size": 5},
        ]

        create("table", "//tmp/t", attributes={"schema": schema})
        write_table("//tmp/t", {"column": "123456"})
        chunk_ids = get("//tmp/t/@chunk_ids")

        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//tmp/t/@chunk_merger_mode", "deep")

        alter_table("//tmp/t", schema=schema_with_hunk_columns)

        wait(lambda: get("//tmp/t/@chunk_merger_status") == "not_in_merge_pipeline")
        assert get("//tmp/t/@chunk_ids") == chunk_ids

    @authors("kivedernikov")
    def test_max_chunk_meta_size_limiter(self):
        create("table", "//tmp/t")
        write_table("<append=true>//tmp/t", {"a": "b"})
        chunk = get("//tmp/t/@chunk_ids")[0]
        pivot = get(f"#{chunk}/@master_meta_size") // 2
        set("//sys/@config/chunk_manager/chunk_merger/max_chunk_meta_size", pivot)

        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "d"})
        assert len(get("//tmp/t/@chunk_ids")) == 3

        wait(lambda: get("//tmp/t/@chunk_merger_status") == "not_in_merge_pipeline")
        assert len(get("//tmp/t/@chunk_ids")) == 3

    @authors("kivedernikov")
    def test_max_chunk_meta_size_limiter2(self):
        create("table", "//tmp/t")
        write_table("<append=true>//tmp/t", {"a": "b"})
        chunk = get("//tmp/t/@chunk_ids")[0]
        pivot = get(f"#{chunk}/@master_meta_size") + 1
        set("//sys/@config/chunk_manager/chunk_merger/max_chunk_meta_size", pivot)

        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "d"})

        self._wait_for_merge("//tmp/t", "deep")
        assert len(get("//tmp/t/@chunk_ids")) == 1

    @authors("cherepashka")
    @pytest.mark.parametrize("merge_mode", ["auto", "deep", "shallow"])
    @pytest.mark.parametrize("operation", [copy, move])
    def test_inherit_chunk_merger_mode_after_copy(self, merge_mode, operation):
        set("//sys/@config/cypress_manager/enable_inherit_attributes_during_copy", True)

        create("map_node", "//tmp/d")
        set("//tmp/d/@chunk_merger_mode", merge_mode)
        assert get("//tmp/d/@chunk_merger_mode") == merge_mode

        create("table", "//aba/d1/d2/t", recursive=True)
        set("//aba/d1/d2/t/@chunk_merger_mode", "none")
        self._remove_merge_quotas("//aba/d1/d2/t")
        operation("//aba/d1", "//tmp/d/d1")
        assert get("//tmp/d/d1/d2/t/@chunk_merger_mode") == merge_mode

        write_table("<append=true>//tmp/d/d1/d2/t", {"a": "b"})
        write_table("<append=true>//tmp/d/d1/d2/t", {"a": "c"})
        write_table("<append=true>//tmp/d/d1/d2/t", {"a": "d"})
        self._wait_for_merge("//tmp/d/d1/d2/t", None)
        remove("//aba", force=True)

    @authors("kazachonok")
    def test_shallow_merge_cancellation(self):
        update_nodes_dynamic_config({
            "data_node": {
                "testing_options": {
                    "chunk_cancellation_delay": 10000,
                },
            },
        })
        schema = make_schema(
            [
                {"name": "a", "type": "string"},
                {"name": "b", "type": "string"},
                {"name": "c", "type": "int64"}
            ],
            strict=False
        )
        create("table", "//tmp/t", attributes={"schema": schema})
        self._remove_merge_quotas("//tmp/t")

        rows1 = [{"a": "a" + str(i), "b": "b" + str(i), "c": i, "x": "x" + str(i)} for i in range(0, 10)]
        write_table("<append=true>//tmp/t", rows1)
        rows2 = [{"a": "a" + str(i), "b": "b" + str(i), "c": i, "y": "y" + str(i)} for i in range(10, 20)]
        write_table("<append=true>//tmp/t", rows2)
        rows3 = [{"a": "a" + str(i), "b": "b" + str(i), "c": i, "z": "z" + str(i)} for i in range(20, 30)]
        write_table("<append=true>//tmp/t", rows3)
        assert read_table("//tmp/t") == rows1 + rows2 + rows3

        fallback_counters = get_chunk_owner_master_cell_counters("//tmp/t", "chunk_server/chunk_merger/auto_merge_fallback_count")

        assert sum(counter.get_delta() for counter in fallback_counters) == 0

        self._wait_for_merge("//tmp/t", "auto")

        wait(lambda: sum(counter.get_delta() for counter in fallback_counters) > 0)


class TestChunkMergerMulticell(TestChunkMerger):
    ENABLE_MULTIDAEMON = False  # Checks structured logs.
    NUM_TEST_PARTITIONS = 6

    NUM_SECONDARY_MASTER_CELLS = 3

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_teleportation(self, merge_mode):
        create("table", "//tmp/t1", attributes={"external_cell_tag": 12})
        write_table("<append=true>//tmp/t1", {"a": "b"})
        write_table("<append=true>//tmp/t1", {"a": "c"})

        create("table", "//tmp/t2", attributes={"external_cell_tag": 13})
        write_table("<append=true>//tmp/t2", {"a": "d"})
        write_table("<append=true>//tmp/t2", {"a": "e"})

        create("table", "//tmp/t", attributes={"external_cell_tag": 13})
        self._remove_merge_quotas("//tmp/t")

        concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/t")

        assert get("//tmp/t1/@external_cell_tag") == 12
        assert get("//tmp/t2/@external_cell_tag") == 13
        assert get("//tmp/t/@external_cell_tag") == 13

        chunk_ids = get("//tmp/t/@chunk_ids")
        remove("//tmp/t1")
        remove("//tmp/t2")

        self._wait_for_merge("//tmp/t", merge_mode)

        for chunk_id in chunk_ids:
            wait(lambda: not exists("#{}".format(chunk_id)))


class TestChunkMergerPortal(TestChunkMergerMulticell):
    ENABLE_MULTIDAEMON = False  # Checks strucured logs.
    NUM_TEST_PARTITIONS = 6

    ENABLE_TMP_PORTAL = True
    NUM_SECONDARY_MASTER_CELLS = 3

    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host", "transaction_coordinator"]},
        "11": {"roles": ["cypress_node_host"]},
        "12": {"roles": ["chunk_host"]},
        "13": {"roles": ["chunk_host"]},
    }

    @authors("cherepashka")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_cross_portal_merge(self, merge_mode):
        create("table", "//tmp/t1")
        self._remove_merge_quotas("//tmp/t1")

        write_table("<append=true>//tmp/t1", {"a": "b"})
        write_table("<append=true>//tmp/t1", {"a": "c"})
        write_table("<append=true>//tmp/t1", {"a": "d"})
        write_table("<append=true>//tmp/t1", {"a": "e"})
        set("//tmp/t1/@chunk_merger_mode", merge_mode)

        copy("//tmp/t1", "//home/t2", recursive=True)

        self._wait_for_merge("//home/t2", None)


@pytest.mark.enabled_multidaemon
class TestTableDataStatisticsConsistency(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 1

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "chunk_manager": {
            "chunk_merger": {
                "enable": True,
            }
        }
    }

    @authors("danilalexeev")
    def test_data_statistics_consistency_on_copy(self):
        table_id = create("table", "//tmp/t", attirbutes={"external_cell_tag": 11})
        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "d"})

        set("//sys/@config/tablet_manager/multicell_gossip/table_statistics_gossip_period", 100500)

        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)
        set("//tmp/t/@chunk_merger_mode", "deep")

        # Wait for merge
        driver = get_driver(1)
        wait(lambda: get(f"#{table_id}/@chunk_count", driver=driver) == 1)

        copy("//tmp/t", "//tmp/t1")
        assert get("//tmp/t/@chunk_count") == 3
        assert get("//tmp/t1/@chunk_count") == 3

        set("//sys/@config/tablet_manager/multicell_gossip/table_statistics_gossip_period", 1000)

        wait(lambda: get("//tmp/t/@chunk_count") == 1
             and get("//tmp/t1/@chunk_count") == 1)


class TestShallowMergeValidation(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # Doesn't work because of abort_on_alert.
    NUM_MASTERS = 3
    NUM_NODES = 3

    DELTA_NODE_CONFIG = {
        "logging": {
            "abort_on_alert": False,
        },
    }

    DELTA_MASTER_CONFIG = {
        "logging": {
            "abort_on_alert": False,
        },
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "chunk_manager": {
            "chunk_merger": {
                "max_chunk_count": 5,
                "create_chunks_period": 100,
                "schedule_period": 100,
                "session_finalization_period": 100,
                "shallow_merge_validation_probability": 100,
            }
        }
    }

    @authors("gritukan")
    @pytest.mark.parametrize("fail_validation", ["fail_shallow_merge_validation", "fail_chunk_meta_validation"])
    def test_validation_failed(self, fail_validation):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)
        update_nodes_dynamic_config({
            "data_node": {
                "merge_chunks_job": {
                    fail_validation: True,
                },
            },
        })

        create("table", "//tmp/t")

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "d"})

        set("//sys/@config/chunk_manager/chunk_merger/enable", True)
        set("//tmp/t/@chunk_merger_mode", "shallow")
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)

        wait(lambda: not get("//sys/@config/chunk_manager/chunk_merger/enable"))
