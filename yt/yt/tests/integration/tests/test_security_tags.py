from yt_env_setup import YTEnvSetup

from yt_commands import (  # noqa
    authors, print_debug, wait, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists, concatenate,
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
    get_driver, Driver, execute_command)

from yt.environment.helpers import assert_items_equal
from yt.common import YtError

import pytest

##################################################################


class TestSecurityTags(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @authors("babenko")
    def test_security_tags_empty_by_default(self):
        create("table", "//tmp/t")
        assert get("//tmp/t/@security_tags") == []

    @authors("babenko")
    def test_set_security_tags_upon_create(self):
        create("table", "//tmp/t", attributes={"security_tags": ["tag1", "tag2"]})
        assert_items_equal(get("//tmp/t/@security_tags"), ["tag1", "tag2"])

    @authors("babenko")
    def test_write_table_with_security_tags(self):
        create("table", "//tmp/t")

        write_table("<security_tags=[tag1;tag2]>//tmp/t", [{"a": "b"}])
        assert_items_equal(get("//tmp/t/@security_tags"), ["tag1", "tag2"])

        write_table("//tmp/t", [{"a": "b"}])
        assert_items_equal(get("//tmp/t/@security_tags"), [])

        write_table("<security_tags=[tag3]>//tmp/t", [{"a": "b"}])
        assert_items_equal(get("//tmp/t/@security_tags"), ["tag3"])

        write_table("<security_tags=[]>//tmp/t", [{"a": "b"}])
        assert_items_equal(get("//tmp/t/@security_tags"), [])

    @authors("babenko")
    def test_write_table_with_security_tags_append(self):
        create("table", "//tmp/t")

        write_table("<security_tags=[tag1;tag2]>//tmp/t", [{"a": "b"}])
        write_table("<append=true>//tmp/t", [{"c": "d"}])
        assert_items_equal(get("//tmp/t/@security_tags"), ["tag1", "tag2"])

    @authors("babenko")
    def test_write_file_with_security_tags(self):
        create("file", "//tmp/f")

        write_file("<security_tags=[tag1;tag2]>//tmp/f", "test")
        assert_items_equal(get("//tmp/f/@security_tags"), ["tag1", "tag2"])

        write_file("//tmp/f", "test")
        assert_items_equal(get("//tmp/f/@security_tags"), [])

        write_file("<security_tags=[tag3]>//tmp/f", "test")
        assert_items_equal(get("//tmp/f/@security_tags"), ["tag3"])

        write_file("<security_tags=[]>//tmp/f", "test")
        assert_items_equal(get("//tmp/f/@security_tags"), [])

    @authors("babenko")
    def test_write_file_with_security_tags_append(self):
        create("file", "//tmp/f")

        write_file("<security_tags=[tag1;tag2]>//tmp/f", "test")
        write_file("<append=true>//tmp/f", "test")
        assert_items_equal(get("//tmp/f/@security_tags"), ["tag1", "tag2"])

    @authors("babenko")
    def test_overwrite_table_in_tx(self):
        create("table", "//tmp/t")

        write_table("<security_tags=[tag1;tag2]>//tmp/t", [{"a": "b"}])

        tx = start_transaction()
        assert_items_equal(get("//tmp/t/@security_tags", tx=tx), ["tag1", "tag2"])
        write_table("<security_tags=[tag2;tag3]>//tmp/t", [{"c": "d"}], tx=tx)
        assert_items_equal(get("//tmp/t/@security_tags", tx=tx), ["tag2", "tag3"])
        commit_transaction(tx)

        assert_items_equal(get("//tmp/t/@security_tags"), ["tag2", "tag3"])

    @authors("babenko")
    def test_append_table_in_tx(self):
        create("table", "//tmp/t")

        write_table("<security_tags=[tag1;tag2]>//tmp/t", [{"a": "b"}])

        tx = start_transaction()
        assert_items_equal(get("//tmp/t/@security_tags", tx=tx), ["tag1", "tag2"])
        write_table("<security_tags=[tag2;tag3];append=%true>//tmp/t", [{"c": "d"}], tx=tx)
        assert_items_equal(get("//tmp/t/@security_tags", tx=tx), ["tag1", "tag2", "tag3"])
        commit_transaction(tx)

        assert_items_equal(get("//tmp/t/@security_tags"), ["tag1", "tag2", "tag3"])

    @authors("babenko")
    def test_operation(self):
        create("table", "//tmp/t_in1", attributes={"security_tags": ["tag1"]})
        create("table", "//tmp/t_in2", attributes={"security_tags": ["tag2"]})

        create("table", "//tmp/t_out1", attributes={"security_tags": ["xxx"]})
        create("table", "//tmp/t_out2", attributes={"security_tags": ["xxx"]})
        create("table", "//tmp/t_out3", attributes={"security_tags": ["tag3"]})

        create("file", "//tmp/f", attributes={"security_tags": ["tag4"]})

        map(
            command="cat",
            in_=["//tmp/t_in1", "//tmp/t_in2"],
            out=[
                "//tmp/t_out1",
                "<security_tags=[tag5]>//tmp/t_out2",
                "<append=%true;security_tags=[tag6]>//tmp/t_out3",
            ],
            spec={
                "mapper": {"file_paths": ["//tmp/f"]},
                "additional_security_tags": ["tag0"],
            },
        )
        assert_items_equal(get("//tmp/t_out1/@security_tags"), ["tag0", "tag1", "tag2", "tag4"])
        assert_items_equal(get("//tmp/t_out2/@security_tags"), ["tag5"])
        assert_items_equal(get("//tmp/t_out3/@security_tags"), ["tag3", "tag6"])

    @authors("babenko")
    def test_update_security_tags1(self):
        create("table", "//tmp/t")
        assert_items_equal(get("//tmp/t/@security_tags"), [])

        set("//tmp/t/@security_tags", ["tag1", "tag2"])
        assert_items_equal(get("//tmp/t/@security_tags"), ["tag1", "tag2"])

    @authors("babenko")
    def test_update_security_tags2(self):
        tx = start_transaction()

        create("table", "//tmp/t", tx=tx)
        assert_items_equal(get("//tmp/t/@security_tags", tx=tx), [])

        set("//tmp/t/@security_tags", ["tag1", "tag2"], tx=tx)
        assert_items_equal(get("//tmp/t/@security_tags", tx=tx), ["tag1", "tag2"])

        commit_transaction(tx)

        assert_items_equal(get("//tmp/t/@security_tags"), ["tag1", "tag2"])

    @authors("babenko")
    def test_cannot_update_security_tags_in_append_mode(self):
        create("table", "//tmp/t")

        tx = start_transaction()
        write_table("<append=%true>//tmp/t", [{"a": "b"}], tx=tx)
        with pytest.raises(YtError):
            set("//tmp/t/@security_tags", ["tag1", "tag2"], tx=tx)

    @authors("babenko")
    def test_can_update_security_tags_in_overwrite_mode1(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"a": "b"}])

        tx = start_transaction()
        write_table("//tmp/t", [{"a": "b"}], tx=tx)
        set("//tmp/t/@security_tags", ["tag1", "tag2"], tx=tx)

        commit_transaction(tx)
        assert_items_equal(get("//tmp/t/@security_tags"), ["tag1", "tag2"])
        assert_items_equal(read_table("//tmp/t"), [{"a": "b"}])

    @authors("babenko")
    def test_can_update_security_tags_in_overwrite_mode2(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"a": "b"}])

        tx = start_transaction()
        write_table("//tmp/t", [{"a": "b"}], tx=tx)
        set("//tmp/t/@security_tags", ["tag1", "tag2"], tx=tx)
        write_table("<append=%true>//tmp/t", [{"c": "d"}], tx=tx)

        commit_transaction(tx)
        assert_items_equal(get("//tmp/t/@security_tags"), ["tag1", "tag2"])
        assert_items_equal(read_table("//tmp/t"), [{"a": "b"}, {"c": "d"}])

    @authors("babenko")
    def test_update_security_tags_involves_exclusive_lock(self):
        create("table", "//tmp/t")

        tx = start_transaction()
        lock("//tmp/t", mode="shared", tx=tx)

        with pytest.raises(YtError):
            set("//tmp/t/@security_tags", ["tag1", "tag2"])

    @authors("babenko")
    def test_concatenate(self):
        create("table", "//tmp/t1", attributes={"security_tags": ["tag1", "tag2"]})
        write_table("<append=%true>//tmp/t1", [{"key": "x"}])

        create("table", "//tmp/t2", attributes={"security_tags": ["tag3"]})
        write_table("<append=%true>//tmp/t2", [{"key": "y"}])

        create("table", "//tmp/union")

        concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")
        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "y"}]
        assert_items_equal(get("//tmp/union/@security_tags"), ["tag1", "tag2", "tag3"])

    @authors("babenko")
    def test_concatenate_append(self):
        create("table", "//tmp/t1", attributes={"security_tags": ["tag1", "tag2"]})
        write_table("<append=%true>//tmp/t1", [{"key": "x"}])

        create("table", "//tmp/t2", attributes={"security_tags": ["tag3"]})
        write_table("<append=%true>//tmp/t2", [{"key": "y"}])

        create("table", "//tmp/union", attributes={"security_tags": ["tag4"]})
        write_table("<append=%true>//tmp/union", [{"key": "z"}])

        concatenate(["//tmp/t1", "//tmp/t2"], "<append=%true>//tmp/union")
        assert read_table("//tmp/union") == [{"key": "z"}, {"key": "x"}, {"key": "y"}]
        assert_items_equal(get("//tmp/union/@security_tags"), ["tag1", "tag2", "tag3", "tag4"])

    @authors("babenko")
    def test_concatenate_override(self):
        create("table", "//tmp/t1", attributes={"security_tags": ["tag1", "tag2"]})
        write_table("<append=%true>//tmp/t1", [{"key": "x"}])

        create("table", "//tmp/t2", attributes={"security_tags": ["tag3"]})
        write_table("<append=%true>//tmp/t2", [{"key": "y"}])

        create("table", "//tmp/union")

        concatenate(["//tmp/t1", "//tmp/t2"], "<security_tags=[tag0]>//tmp/union")
        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "y"}]
        assert_items_equal(get("//tmp/union/@security_tags"), ["tag0"])

    @authors("babenko")
    def test_tag_naming_on_set(self):
        create("table", "//tmp/t")
        with pytest.raises(YtError):
            set("//tmp/t/@security_tags", [""])
        with pytest.raises(YtError):
            set("//tmp/t/@security_tags", ["a" * 129])
        set("//tmp/t/@security_tags", ["a" * 128])

    @authors("babenko")
    def test_tag_naming_on_create(self):
        with pytest.raises(YtError):
            create("table", "//tmp/t", attributes={"security_tags": [""]})
        with pytest.raises(YtError):
            create("table", "//tmp/t", attributes={"security_tags": ["a" * 129]})
        create("table", "//tmp/t", attributes={"security_tags": ["a" * 128]})

    @authors("babenko")
    def test_tag_naming_on_write(self):
        create("table", "//tmp/t")
        with pytest.raises(YtError):
            write_table('<security_tags=[""]>//tmp/t', [])
        with pytest.raises(YtError):
            write_table('<security_tags=["' + "a" * 129 + '"]>//tmp/t', [])
        write_table('<security_tags=["' + "a" * 128 + '"]>//tmp/t', [])


##################################################################


class TestSecurityTagsMulticell(TestSecurityTags):
    NUM_SECONDARY_MASTER_CELLS = 1
