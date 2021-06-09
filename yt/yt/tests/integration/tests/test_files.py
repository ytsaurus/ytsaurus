from yt_env_setup import YTEnvSetup

from yt_commands import (  # noqa
    authors, print_debug, wait, wait_assert, retry, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists, concatenate,
    create_account, create_network_project, create_tmpdir, create_user, create_group,
    create_pool, create_pool_tree, remove_pool_tree,
    create_data_center, create_rack, create_table,
    create_tablet_cell_bundle, remove_tablet_cell_bundle, create_tablet_cell, create_table_replica,
    make_ace, check_permission, add_member, remove_member, remove_group, remove_user,
    remove_network_project,
    make_batch_request, execute_batch, get_batch_error,
    start_transaction, abort_transaction, commit_transaction, lock,
    insert_rows, select_rows, lookup_rows, delete_rows, trim_rows, alter_table,
    read_file, write_file, read_table, write_table, write_local_file, read_blob_table,
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
    set_node_banned, set_banned_flag, set_account_disk_space_limit, set_node_decommissioned,
    check_all_stderrs,
    create_test_tables, create_dynamic_table, PrepareTables,
    get_statistics, get_recursive_disk_space, get_chunk_owner_disk_space,
    make_random_string, raises_yt_error,
    build_snapshot, is_multicell,
    get_driver, Driver, execute_command,
    AsyncLastCommittedTimestamp)

from yt.common import YtError

import pytest

import hashlib
from io import TextIOBase

##################################################################


class TestFiles(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5

    @authors("babenko", "ignat")
    def test_invalid_type(self):
        with pytest.raises(YtError):
            read_file("//tmp")
        with pytest.raises(YtError):
            write_file("//tmp", "")

    @authors("ignat")
    def test_simple(self):
        content = "some_data"
        create("file", "//tmp/file")
        write_file("//tmp/file", content)
        assert read_file("//tmp/file") == content

        chunk_id = get_singular_chunk_id("//tmp/file")

        assert get("//tmp/file/@uncompressed_data_size") == len(content)

        remove("//tmp/file")

        wait(lambda: not exists("#%s" % chunk_id))

    @authors("psushin", "babenko")
    def test_empty(self):
        create("file", "//tmp/file")
        write_file("//tmp/file", "")
        assert read_file("//tmp/file") == ""

        assert get("//tmp/file/@chunk_ids") == []
        assert get("//tmp/file/@uncompressed_data_size") == 0

    @authors("ignat")
    def test_read_interval(self):
        content = "".join(["data"] * 100)
        create("file", "//tmp/file")
        write_file("//tmp/file", content, file_writer={"block_size": 8})

        offset = 9
        length = 212
        assert read_file("//tmp/file", offset=offset) == content[offset:]
        assert read_file("//tmp/file", length=length) == content[:length]
        assert read_file("//tmp/file", offset=offset, length=length) == content[offset:offset + length]

        with pytest.raises(YtError):
            assert read_file("//tmp/file", length=-1)

        with pytest.raises(YtError):
            assert read_file("//tmp", length=0)

        with pytest.raises(YtError):
            assert read_file("//tmp", length=1)

        assert get("//tmp/file/@uncompressed_data_size") == len(content)

    @authors("acid")
    def test_read_all_intervals(self):
        content = "".join(chr(c) for c in range(ord("a"), ord("a") + 8))
        create("file", "//tmp/file")
        write_file("//tmp/file", content, file_writer={"block_size": 3})

        for offset in range(len(content)):
            for length in range(0, len(content) - offset):
                assert read_file("//tmp/file", offset=offset, length=length) == content[offset:offset + length]

    @authors("babenko", "ignat")
    def test_copy(self):
        content = "some_data"
        create("file", "//tmp/f")
        write_file("//tmp/f", content)

        chunk_id = get_singular_chunk_id("//tmp/f")

        assert read_file("//tmp/f") == content
        copy("//tmp/f", "//tmp/f2")
        assert read_file("//tmp/f2") == content

        assert get("//tmp/f2/@resource_usage") == get("//tmp/f/@resource_usage")
        assert get("//tmp/f2/@replication_factor") == get("//tmp/f/@replication_factor")

        remove("//tmp/f")
        assert read_file("//tmp/f2") == content

        remove("//tmp/f2")

        wait(lambda: not exists("#%s" % chunk_id))

    @authors("babenko", "ignat")
    def test_copy_tx(self):
        content = "some_data"
        create("file", "//tmp/f")
        write_file("//tmp/f", content)

        chunk_id = get_singular_chunk_id("//tmp/f")

        tx = start_transaction()
        assert read_file("//tmp/f", tx=tx) == content
        copy("//tmp/f", "//tmp/f2", tx=tx)
        assert read_file("//tmp/f2", tx=tx) == content
        commit_transaction(tx)

        assert read_file("//tmp/f2") == content

        remove("//tmp/f")
        assert read_file("//tmp/f2") == content

        remove("//tmp/f2")

        wait(lambda: not exists("#%s" % chunk_id))

    @authors("babenko", "ignat")
    def test_replication_factor_attr(self):
        content = "some_data"
        create("file", "//tmp/f")
        write_file("//tmp/f", content)

        get("//tmp/f/@replication_factor")

        with pytest.raises(YtError):
            remove("//tmp/f/@replication_factor")
        with pytest.raises(YtError):
            set("//tmp/f/@replication_factor", 0)
        with pytest.raises(YtError):
            set("//tmp/f/@replication_factor", {})

        tx = start_transaction()
        with pytest.raises(YtError):
            set("//tmp/f/@replication_factor", 2, tx=tx)

    @authors("psushin", "ignat")
    def test_append(self):
        content = "some_data"
        create("file", "//tmp/f")
        write_file("//tmp/f", content)
        write_file("<append=true>//tmp/f", content)

        assert len(get("//tmp/f/@chunk_ids")) == 2
        assert get("//tmp/f/@uncompressed_data_size") == 18
        assert read_file("//tmp/f") == content + content

    @authors("ignat")
    def test_overwrite(self):
        content = "some_data"
        create("file", "//tmp/f")
        write_file("//tmp/f", content)
        write_file("//tmp/f", content)

        assert len(get("//tmp/f/@chunk_ids")) == 1
        assert get("//tmp/f/@uncompressed_data_size") == 9
        assert read_file("//tmp/f") == content

    @authors("babenko", "ignat")
    def test_upload_inside_tx(self):
        create("file", "//tmp/f")

        tx = start_transaction()

        content = "some_data"
        write_file("//tmp/f", content, tx=tx)

        assert read_file("//tmp/f") == ""
        assert read_file("//tmp/f", tx=tx) == content

        commit_transaction(tx)

        assert read_file("//tmp/f") == content

    @authors("ignat")
    def test_concatenate(self):
        create("file", "//tmp/fa")
        write_file("//tmp/fa", "a")
        assert read_file("//tmp/fa") == "a"

        create("file", "//tmp/fb")
        write_file("//tmp/fb", "b")
        assert read_file("//tmp/fb") == "b"

        create("file", "//tmp/f")

        concatenate(["//tmp/fa", "//tmp/fb"], "//tmp/f")
        assert read_file("//tmp/f") == "ab"

        concatenate(["//tmp/fa", "//tmp/fb"], "<append=true>//tmp/f")
        assert read_file("//tmp/f") == "abab"

    @authors("ignat")
    def test_concatenate_incorrect_types(self):
        create("file", "//tmp/f1")
        create("file", "//tmp/f2")
        create("table", "//tmp/t")

        with pytest.raises(YtError):
            concatenate(["//tmp/f1", "//tmp/f2"], "//tmp/t")

        with pytest.raises(YtError):
            concatenate(["//tmp/f1", "//tmp/t"], "//tmp/t")

        with pytest.raises(YtError):
            concatenate(["//tmp", "//tmp/t"], "//tmp/t")

    @authors("prime")
    def test_set_compression(self):
        create("file", "//tmp/f")

        write_file("<compression_codec=lz4>//tmp/f", "a")
        assert get("//tmp/f/@compression_codec") == "lz4"

        write_file("<compression_codec=none>//tmp/f", "a")
        assert get("//tmp/f/@compression_codec") == "none"

        with pytest.raises(YtError):
            write_file("<append=true;compression_codec=none>//tmp/f", "a")

    @authors("ignat", "kiselyovp")
    def test_compute_hash(self):
        create("file", "//tmp/fcache")
        create("file", "//tmp/fcache2")
        create("file", "//tmp/fcache3")
        create("file", "//tmp/fcache4")
        create("file", "//tmp/fcache5")

        write_file("<append=%true>//tmp/fcache", "abacaba", compute_md5=True)

        assert get("//tmp/fcache/@md5") == "129296d4fd2ade2b2dbc402d4564bf81" == hashlib.md5("abacaba").hexdigest()
        assert exists("//tmp/fcache/@md5")

        write_file("<append=%true>//tmp/fcache", "new", compute_md5=True)
        assert get("//tmp/fcache/@md5") == "12ef1dfdbbb50c2dfd2b4119bac9dee5" == hashlib.md5("abacabanew").hexdigest()

        write_file("//tmp/fcache2", "abacaba")
        assert not exists("//tmp/fcache2/@md5")

        write_file("//tmp/fcache3", "test", compute_md5=True)
        assert get("//tmp/fcache3/@md5") == "098f6bcd4621d373cade4e832627b4f6" == hashlib.md5("test").hexdigest()

        write_file("//tmp/fcache3", "test2", compute_md5=True)
        assert get("//tmp/fcache3/@md5") == hashlib.md5("test2").hexdigest()

        concatenate(["//tmp/fcache", "//tmp/fcache3"], "//tmp/fcache4")
        assert not exists("//tmp/fcache4/@md5")

        with pytest.raises(YtError):
            write_file("<append=%true>//tmp/fcache4", "data", compute_md5=True)

        with pytest.raises(YtError):
            set("//tmp/fcache/@md5", "test")

        write_file("//tmp/fcache5", "", compute_md5=True)
        assert get("//tmp/fcache5/@md5") == "d41d8cd98f00b204e9800998ecf8427e" == hashlib.md5("").hexdigest()


##################################################################


class TestFilesMulticell(TestFiles):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestFilesPortal(TestFilesMulticell):
    ENABLE_TMP_PORTAL = True


class TestFilesRpcProxy(TestFiles):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True


##################################################################


class TestFileErrorsRpcProxy(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 2
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True
    DELTA_RPC_DRIVER_CONFIG = {"default_streaming_stall_timeout": 1500}

    class FaultyStringStream(TextIOBase):
        def __init__(self, data):
            self._position = 0
            self._data = data

        def read(self, size):
            if size < 0:
                raise ValueError()

            if self._position == len(self._data):
                raise RuntimeError("surprise")

            result = self._data[self._position:self._position + size]
            self._position = min(self._position + size, len(self._data))

            return result

    @authors("kiselyovp")
    def test_faulty_client(self):
        create("file", "//tmp/file")
        write_file("//tmp/file", "abacaba")

        tx = start_transaction()
        with pytest.raises(YtError):
            write_file(
                "<append=true>//tmp/file",
                None,
                input_stream=self.FaultyStringStream("dabacaba"),
                tx=tx,
            )

        wait(lambda: get("//sys/transactions/{0}/@nested_transaction_ids".format(tx)) == [])
        assert read_file("//tmp/file") == "abacaba"

    @authors("kiselyovp")
    def test_faulty_server(self):
        create("file", "//tmp/file")
        write_file("//tmp/file", "abacaba")
        assert read_file("//tmp/file") == "abacaba"

        nodes = ls("//sys/cluster_nodes")
        set_node_banned(nodes[0], True)

        with pytest.raises(YtError):
            write_file("<append=true>//tmp/file", "dabacaba")

        set_node_banned(nodes[1], True)
        with pytest.raises(YtError):
            read_file("//tmp/file")

        set_node_banned(nodes[0], False)
        set_node_banned(nodes[1], False)
        assert retry(lambda: read_file("//tmp/file")) == "abacaba"


##################################################################


class TestBigFilesRpcProxy(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True

    @authors("kiselyovp")
    def test_big_files(self):
        alphabet_size = 25
        data = ""
        for i in xrange(alphabet_size):
            data += chr(ord("a") + i) + data

        create("file", "//tmp/abacaba")
        write_file("//tmp/abacaba", data)

        contents = read_file("//tmp/abacaba", verbose=False)
        assert contents == data


class TestBigFilesWithCompressionRpcProxy(TestBigFilesRpcProxy):
    DELTA_RPC_DRIVER_CONFIG = {
        "request_codec": "lz4",
        "response_codec": "quick_lz",
        "enable_legacy_rpc_codecs": False,
    }
