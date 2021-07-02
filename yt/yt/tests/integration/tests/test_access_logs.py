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

from yt_helpers import get_current_time
from yt_type_helpers import make_schema

import json
import os

##################################################################


@authors("shakurov", "avmatrosov")
class TestAccessLog(YTEnvSetup):
    NUM_MASTERS = 2
    NUM_SCHEDULERS = 1
    NUM_NONVOTING_MASTERS = 1
    NUM_NODES = 3
    USE_DYNAMIC_TABLES = True

    CELL_TAG_TO_DIRECTORIES = {0: "//tmp/access_log"}

    def _log_lines(self, path, directory):
        with open(path, "r") as fd:
            for line in fd:
                try:
                    line_json = json.loads(line)
                except ValueError:
                    continue
                if line_json.get("path", "").startswith(directory):
                    yield line_json

    def _is_node_in_logs(self, node, path, directory):
        if not os.path.exists(path):
            return False
        for line_json in self._log_lines(path, directory):
            if line_json.get("path", "") == directory + "/" + node:
                return True
        return False

    def _validate_entries_against_log(self, present_entries, missing_entries=[], cell_tag_to_directory=None):
        if cell_tag_to_directory is None:
            cell_tag_to_directory = self.CELL_TAG_TO_DIRECTORIES
        written_logs = []
        for master_tag, directory in cell_tag_to_directory.items():
            path = os.path.join(self.path_to_run, "logs/master-{}-1.access.json.log".format(master_tag))
            barrier_record = "{}-{}".format(master_tag, generate_timestamp())
            create("table", "{}/{}".format(directory, barrier_record))
            wait(lambda: self._is_node_in_logs(barrier_record, path, directory), iter=120, sleep_backoff=1.0)
            written_logs.extend([line_json for line_json in self._log_lines(path, directory)])

        def _check_entry_is_in_log(log, line_json):
            for key, value in log.iteritems():
                if key in ["attributes", "transaction_id", "operation_type"]:
                    if line_json.get("transaction_info") is None or \
                            key not in line_json.get("transaction_info") or \
                            line_json.get("transaction_info")[key] != value:
                        return False
                elif line_json.get(key) != value:
                    return False
            return True

        for log in present_entries:
            assert any(_check_entry_is_in_log(log, line_json) for line_json in
                       written_logs), "Entry {} is not present in access log".format(log)

        for log in missing_entries:
            assert not any(_check_entry_is_in_log(log, line_json) for line_json in
                           written_logs), "Entry {} is present in access log".format(log)

    @classmethod
    def modify_master_config(cls, config, tag, index):
        config["logging"]["flush_period"] = 100
        config["logging"]["rules"].append(
            {
                "min_level": "debug",
                "writers": ["access"],
                "include_categories": ["Access"],
                "message_format": "structured",
            }
        )
        config["logging"]["writers"]["access"] = {
            "type": "file",
            "file_name": os.path.join(cls.path_to_run, "logs/master-{}-{}.access.json.log".format(tag, index)),
            "accepted_message_format": "structured",
        }

    def test_create(self):
        log_list = []

        map_node_id = create("map_node", "//tmp/access_log")
        log_list.append({"path": "//tmp/access_log", "method": "Create", "type": "map_node", "id": map_node_id})

        table_id = create("table", "//tmp/access_log/t1")
        log_list.append({"path": "//tmp/access_log/t1", "method": "Create", "type": "table", "id": table_id})

        create("table", "//tmp/access_log/t1", ignore_existing=True)
        log_list.append(
            {
                "path": "//tmp/access_log/t1",
                "method": "Create",
                "type": "table",
                "id": table_id,
                "existing": "true",
            }
        )

        table_id2 = create("table", "//tmp/access_log/t1", force=True)
        assert table_id != table_id2
        log_list.append({"path": "//tmp/access_log/t1", "method": "Create", "type": "table", "id": table_id2})

        self._validate_entries_against_log(log_list)

    def test_logs(self):
        log_list = []

        map_node_id = create("map_node", "//tmp/access_log")
        log_list.append({"path": "//tmp/access_log", "method": "Create", "type": "map_node", "id": map_node_id})

        a_id = create("table", "//tmp/access_log/a")
        log_list.append({"path": "//tmp/access_log/a", "method": "Create", "type": "table", "id": a_id})

        set("//tmp/access_log/a/@abc", "abc")
        log_list.append({"path": "//tmp/access_log/a/@abc", "method": "Set", "type": "table", "id": a_id})

        get("//tmp/access_log/a/@abc")
        log_list.append({"path": "//tmp/access_log/a/@abc", "method": "Get", "type": "table", "id": a_id})

        b_id = copy("//tmp/access_log/a", "//tmp/access_log/b")
        log_list.append(
            {
                "method": "Copy",
                "type": "table",
                "id": a_id,
                "path": "//tmp/access_log/a",
                "destination_id": b_id,
                "destination_path": "//tmp/access_log/b",
            }
        )

        ls("//tmp/access_log")
        log_list.append({"path": "//tmp/access_log", "method": "List", "type": "map_node", "id": map_node_id})

        remove("//tmp/access_log/b")
        log_list.append({"path": "//tmp/access_log/b", "method": "Remove", "type": "table", "id": b_id})

        some_table_id = create("table", "//tmp/access_log/some_table")
        b_id = create("table", "//tmp/access_log/b")

        exists("//tmp/access_log/b")
        log_list.append({"path": "//tmp/access_log/b", "method": "Exists", "type": "table", "id": b_id})

        other_table_id = copy("//tmp/access_log/some_table", "//tmp/access_log/other_table")
        log_list.append(
            {
                "method": "Copy",
                "type": "table",
                "id": some_table_id,
                "path": "//tmp/access_log/some_table",
                "destination_id": other_table_id,
                "destination_path": "//tmp/access_log/other_table",
            }
        )

        some_node_id = create("map_node", "//tmp/access_log/some_node")

        some_node_b_id = move("//tmp/access_log/other_table", "//tmp/access_log/some_node/b")
        log_list.append(
            {
                "method": "Move",
                "type": "table",
                "id": other_table_id,
                "path": "//tmp/access_log/other_table",
                "destination_id": some_node_b_id,
                "destination_path": "//tmp/access_log/some_node/b",
            }
        )

        q_id = link("//tmp/access_log/b", "//tmp/access_log/some_node/q")
        log_list.append(
            {
                "method": "Link",
                "type": "link",
                "id": q_id,
                "path": "//tmp/access_log/some_node/q",
                "destination_path": "//tmp/access_log/b",
            }
        )

        get("//tmp/access_log/some_node/q")
        log_list.append({"path": "//tmp/access_log/b", "method": "Get", "type": "table", "id": b_id})

        remove("//tmp/access_log/some_node", recursive=True)
        log_list.append(
            {
                "path": "//tmp/access_log/some_node",
                "method": "Remove",
                "type": "map_node",
                "id": some_node_id
            }
        )

        self._validate_entries_against_log(log_list)

    def test_transaction_logs(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        log_list = []

        create("map_node", "//tmp/access_log")

        a_id = create("table", "//tmp/access_log/a", tx=tx1)
        log_list.append(
            {
                "method": "Create",
                "type": "table",
                "id": a_id,
                "path": "//tmp/access_log/a",
                "transaction_id": str(tx1),
            }
        )

        set("//tmp/access_log/a/@test", "test", tx=tx2)
        log_list.append(
            {
                "method": "Set",
                "type": "table",
                "id": a_id,
                "path": "//tmp/access_log/a/@test",
                "transaction_id": str(tx2),
            }
        )

        b_id = copy("//tmp/access_log/a", "//tmp/access_log/b", tx=tx2)
        log_list.append(
            {
                "method": "Copy",
                "type": "table",
                "id": a_id,
                "path": "//tmp/access_log/a",
                "destination_id": b_id,
                "destination_path": "//tmp/access_log/b",
                "transaction_id": str(tx2),
            }
        )

        exists("//tmp/access_log/a", tx=tx2)
        log_list.append(
            {
                "method": "Exists",
                "type": "table",
                "id": a_id,
                "path": "//tmp/access_log/a",
                "transaction_id": str(tx2),
            }
        )

        self._validate_entries_against_log(log_list)

    def test_log_dynamic_config(self):
        enabled_logs = []
        disabled_logs = []

        create("map_node", "//tmp/access_log")

        create("table", "//tmp/access_log/enabled")
        enabled_logs.append({"method": "Create", "path": "//tmp/access_log/enabled", "type": "table"})

        set("//sys/@config/security_manager/enable_access_log", False)

        ts = str(generate_timestamp())
        create("table", "//tmp/access_log/{}".format(ts))
        disabled_logs.append({"method": "Create", "path": "//tmp/access_log/{}".format(ts), "type": "table"})

        set("//sys/@config/security_manager/enable_access_log", True)

        self._validate_entries_against_log(present_entries=enabled_logs, missing_entries=disabled_logs)

    def test_original_path(self):
        log_list = []

        create("map_node", "//tmp/access_log")
        create("map_node", "//tmp/access_log/original")

        link("//tmp/access_log/original", "//tmp/access_log/linked")

        create("table", "//tmp/access_log/linked/t")
        log_list.append(
            {
                "path": "//tmp/access_log/original/t",
                "method": "Create",
                "original_path": "//tmp/access_log/linked/t",
            }
        )

        copy("//tmp/access_log/linked/t", "//tmp/access_log/linked/t2")
        log_list.append(
            {
                "path": "//tmp/access_log/original/t",
                "method": "Copy",
                "original_path": "//tmp/access_log/linked/t",
                "destination_path": "//tmp/access_log/original/t2",
                "original_destination_path": "//tmp/access_log/linked/t2",
            }
        )

        set("//tmp/access_log/linked/t/@test", "test")
        log_list.append(
            {
                "path": "//tmp/access_log/original/t/@test",
                "method": "Set",
                "original_path": "//tmp/access_log/linked/t/@test",
            }
        )

        self._validate_entries_against_log(log_list)

    def test_table_logs(self):
        sync_create_cells(1)
        log_list = []

        create("map_node", "//tmp/access_log")
        create_dynamic_table("//tmp/access_log/table", schema=[{"name": "value", "type": "string"}])

        sync_mount_table("//tmp/access_log/table")
        log_list.append({"path": "//tmp/access_log/table", "method": "PrepareMount"})
        log_list.append({"path": "//tmp/access_log/table", "method": "CommitMount"})

        remount_table("//tmp/access_log/table")
        log_list.append({"path": "//tmp/access_log/table", "method": "PrepareRemount"})
        log_list.append({"path": "//tmp/access_log/table", "method": "CommitRemount"})

        sync_freeze_table("//tmp/access_log/table")
        log_list.append({"path": "//tmp/access_log/table", "method": "PrepareFreeze"})
        log_list.append({"path": "//tmp/access_log/table", "method": "CommitFreeze"})

        sync_unfreeze_table("//tmp/access_log/table")
        log_list.append({"path": "//tmp/access_log/table", "method": "PrepareUnfreeze"})
        log_list.append({"path": "//tmp/access_log/table", "method": "CommitUnfreeze"})

        sync_unmount_table("//tmp/access_log/table")
        log_list.append({"path": "//tmp/access_log/table", "method": "PrepareUnmount"})
        log_list.append({"path": "//tmp/access_log/table", "method": "CommitUnmount"})

        sync_reshard_table("//tmp/access_log/table", 1)
        log_list.append({"path": "//tmp/access_log/table", "method": "PrepareReshard"})
        log_list.append({"path": "//tmp/access_log/table", "method": "CommitReshard"})

        self._validate_entries_against_log(log_list)

    def test_alter(self):
        log_list = []

        create("map_node", "//tmp/access_log")
        create("table", "//tmp/access_log/table")
        schema1 = make_schema(
            [{"name": "a", "type": "string", "required": False}],
            strict=False,
            unique_keys=False,
        )
        alter_table("//tmp/access_log/table", schema=schema1)

        log_list.append({"path": "//tmp/access_log/table", "method": "Alter"})
        self._validate_entries_against_log(log_list)

    def test_expiration_time(self):
        log_list = []

        create("map_node", "//tmp/access_log")
        create(
            "table",
            "//tmp/access_log/table",
            attributes={"expiration_time": str(get_current_time())}
        )

        wait(lambda: not exists("//tmp/access_log/table"))

        log_list.append({"path": "//tmp/access_log/table", "method": "TtlRemove"})
        self._validate_entries_against_log(log_list)

    def test_write_table(self):
        log_list = []

        create("map_node", "//tmp/access_log")
        table_id = create("table", "//tmp/access_log/t1")

        write_table("//tmp/access_log/t1", {"foo": "bar"})

        log_list.append(
            {
                "path": "//tmp/access_log/t1",
                "type": "table",
                "id": table_id,
                "method": "BeginUpload",
                "mode": "overwrite"
            }
        )

        self._validate_entries_against_log(log_list)
        write_table("<append=%true>//tmp/access_log/t1", {"foo2": "bar2"})

        log_list.append(
            {
                "path": "//tmp/access_log/t1",
                "type": "table",
                "id": table_id,
                "method": "BeginUpload",
                "mode": "append"
            }
        )

        self._validate_entries_against_log(log_list)

    def test_sort(self):
        log_list = []

        create("map_node", "//tmp/access_log")
        create("table", "//tmp/access_log/t1")
        table_id2 = create("table", "//tmp/access_log/t2")

        write_table("<append=%true>//tmp/access_log/t1", {"a": 25, "b": "foo"})
        write_table("<append=%true>//tmp/access_log/t1", {"a": 100, "b": "bar"})
        write_table("<append=%true>//tmp/access_log/t1", {"a": 41, "b": "foobar"})
        write_table("<append=%true>//tmp/access_log/t1", {"a": 23, "b": "barfoo"})

        sort(in_="//tmp/access_log/t1", out="//tmp/access_log/t2", sort_by=["a"])

        log_list.append(
            {
                "path": "//tmp/access_log/t2",
                "type": "table",
                "id": table_id2,
                "method": "BeginUpload",
                "operation_type": "sort"
            }
        )

        self._validate_entries_against_log(log_list)


##################################################################


class TestAccessLogPortal(TestAccessLog):
    NUM_SECONDARY_MASTER_CELLS = 3
    ENABLE_TMP_PORTAL = True

    CELL_TAG_TO_DIRECTORIES = {1: "//tmp/access_log"}

    @authors("shakurov", "s-v-m")
    def test_logs_portal(self):
        log_list = []

        create("map_node", "//tmp/access_log")
        doc_id = create("document", "//tmp/access_log/doc")

        create("portal_entrance", "//tmp/access_log/p1", attributes={"exit_cell_tag": 2})
        moved_doc_id = move("//tmp/access_log/doc", "//tmp/access_log/p1/doc")
        log_list.append(
            {
                "method": "BeginCopy",
                "type": "document",
                "id": doc_id,
                "path": "//tmp/access_log/doc",
            })
        log_list.append(
            {
                "method": "EndCopy",
                "type": "document",
                "id": moved_doc_id,
                "path": "//tmp/access_log/p1/doc",
            }
        )

        self._validate_entries_against_log(log_list, cell_tag_to_directory={
            1: "//tmp/access_log",
            2: "//tmp/access_log/p1"})
