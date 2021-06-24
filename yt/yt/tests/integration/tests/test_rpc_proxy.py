from proxy_format_config import _TestProxyFormatConfigBase

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
    start_transaction, abort_transaction, commit_transaction, ping_transaction, lock,
    externalize, internalize,
    insert_rows, select_rows, lookup_rows, delete_rows, trim_rows, alter_table, explain_query,
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
    get_driver, Driver, execute_command, generate_uuid, Operation,
    AsyncLastCommittedTimestamp, MinTimestamp)

from yt_type_helpers import make_schema

from yt.wrapper import JsonFormat
from yt.common import YtError, YtResponseError
import yt.yson as yson

from flaky import flaky
import pytest

from copy import deepcopy
from random import shuffle
import time

##################################################################


class TestRpcProxy(YTEnvSetup):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    NUM_RPC_PROXIES = 1

    DELTA_RPC_PROXY_CONFIG = {
        "retry_request_queue_size_limit_exceeded": False,
        "discovery_service": {
            "proxy_update_period": 100
        },
        "access_checker": {
            "enabled": True,
            "cache": {
                "expire_after_access_time": 100,
            },
        },
    }

    @authors("shakurov")
    def test_non_sticky_transactions_dont_stick(self):
        tx = start_transaction(timeout=1000)
        wait(lambda: not exists("//sys/transactions/" + tx))

    @authors("prime")
    def test_dynamic_config(self):
        proxy_name = ls("//sys/rpc_proxies")[0]

        set(
            "//sys/rpc_proxies/@config",
            {"tracing": {"user_sample_rate": {"prime": 1.0}}},
        )

        def config_updated():
            config = get("//sys/rpc_proxies/" + proxy_name + "/orchid/dynamic_config_manager/effective_config")
            return "prime" in config["tracing"]["user_sample_rate"]

        wait(config_updated)

    @authors("gritukan")
    def test_access_checker(self):
        rpc_proxies = ls("//sys/rpc_proxies")
        assert len(rpc_proxies) == 1
        proxy_name = rpc_proxies[0]

        def check_access(user):
            try:
                get("//sys/@config", authenticated_user=user)
            except YtError:
                return False
            return True

        create("rpc_proxy_role_map", "//sys/rpc_proxy_roles")

        create_user("u")
        create_proxy_role("r1", "rpc")
        create_proxy_role("r2", "rpc")

        set("//sys/rpc_proxy_roles/r1/@acl", [make_ace("deny", "u", "use")])
        set("//sys/rpc_proxy_roles/r2/@acl", [make_ace("allow", "u", "use")])

        # "u" is not allowed to use proxies with role "r1".
        set("//sys/rpc_proxies/" + proxy_name + "/@role", "r1")
        wait(lambda: not check_access("u"))

        # "u" is allowed to use proxies with role "r2".
        set("//sys/rpc_proxies/" + proxy_name + "/@role", "r2")
        wait(lambda: check_access("u"))

        # Now "u" is not allowed to use proxies with role "r2".
        set("//sys/rpc_proxy_roles/r2/@acl", [make_ace("deny", "u", "use")])
        wait(lambda: not check_access("u"))

        # There is no node for proxy role "r3". By default we allow access to
        # proxies with unknown role.
        set("//sys/rpc_proxies/" + proxy_name + "/@role", "r3")
        wait(lambda: check_access("u"))

        # Set proxy role back to "r2". User "u" still can't use it.
        set("//sys/rpc_proxies/" + proxy_name + "/@role", "r2")
        wait(lambda: not check_access("u"))

        # Disable access checker via dynamic config. Now "u" can use proxy.
        set("//sys/rpc_proxies/@config", {"access_checker": {"enabled": False}})
        wait(lambda: check_access("u"))

        # Enable access checker via dynamic config. And "u" is banned again.
        set("//sys/rpc_proxies/@config", {"access_checker": {"enabled": True}})
        wait(lambda: not check_access("u"))


##################################################################


class TestRpcProxyBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True

    _schema_dicts = [
        {"name": "index", "type": "int64"},
        {"name": "str", "type": "string"},
    ]
    _schema = make_schema(_schema_dicts, strict=True)

    _schema_dicts_sorted = [
        {"name": "index", "type": "int64", "sort_order": "ascending"},
        {"name": "str", "type": "string"},
    ]
    _schema_sorted = make_schema(_schema_dicts_sorted, strict=True, unique_keys=True)
    _sample_index = 241
    _sample_text = "sample text"
    _sample_line = {"index": _sample_index, "str": _sample_text}

    def _create_simple_table(self, path, data=[], dynamic=True, sorted=True, **kwargs):
        schema = self._schema_sorted if sorted else self._schema
        create("table", path, attributes={"dynamic": dynamic, "schema": schema}, **kwargs)

        if dynamic:
            sync_create_cells(1)
        if not data:
            return
        if dynamic:
            sync_mount_table(path)
            insert_rows(path, data)
            sync_unmount_table(path)
        else:
            write_table(path, data)

    def _start_simple_operation(self, cmd, **kwargs):
        self._create_simple_table("//tmp/t_in", data=[self._sample_line], sorted=True, dynamic=True)
        self._create_simple_table("//tmp/t_out", dynamic=False, sorted=True)

        return map(in_="//tmp/t_in", out="//tmp/t_out", track=False, mapper_command=cmd, **kwargs)

    def _start_simple_operation_on_fs(self, event_name="barrier", **kwargs):
        return self._start_simple_operation(events_on_fs().wait_event_cmd(event_name), **kwargs)

    def _start_simple_operation_with_breakpoint(self, cmd_with_breakpoint="BREAKPOINT", **kwargs):
        return self._start_simple_operation(events_on_fs().with_breakpoint(cmd_with_breakpoint), **kwargs)

    def _prepare_output_table(self):
        alter_table("//tmp/t_out", dynamic=True, schema=self._schema_sorted)
        sync_mount_table("//tmp/t_out")


##################################################################


class TestRpcProxyClientRetries(TestRpcProxyBase):
    NUM_NODES = 2

    DELTA_RPC_DRIVER_CONFIG = {
        "enable_retries": True,
        "retry_backoff_time": 100,
        "retry_attempts": 15,
        "retry_timeout": 2000,
        "default_total_streaming_timeout": 1000,
        "proxy_list_update_period": 1000,
        "proxy_list_retry_period": 100,
    }

    DELTA_RPC_PROXY_CONFIG = {
        "retry_request_queue_size_limit_exceeded": False,
        "discovery_service": {
            "proxy_update_period": 100
        },
    }

    DELTA_MASTER_CONFIG = {
        "object_service": {
            "sticky_user_error_expire_time": 0
        }
    }

    @classmethod
    def setup_class(cls):
        super(TestRpcProxyBase, cls).setup_class()
        native_config = deepcopy(cls.Env.configs["driver"])
        native_config["connection_type"] = "native"
        native_config["api_version"] = 3
        cls.native_driver = Driver(native_config)

    @authors("kiselyovp")
    # TODO(kiselyovp): a temporary measure, see YT-13024.
    @flaky
    def test_proxy_banned(self):
        rpc_proxy_addresses = ls("//sys/rpc_proxies")
        try:
            for i in xrange(5):
                set(
                    "//sys/rpc_proxies/{0}/@banned".format(rpc_proxy_addresses[i % self.NUM_RPC_PROXIES]),
                    True,
                )
                time.sleep(0.1)
                get("//@")
                set(
                    "//sys/rpc_proxies/{0}/@banned".format(rpc_proxy_addresses[i % self.NUM_RPC_PROXIES]),
                    False,
                )
        finally:
            for address in rpc_proxy_addresses:
                set(
                    "//sys/rpc_proxies/{0}/@banned".format(address),
                    False,
                    driver=self.native_driver,
                )

    @authors("kiselyovp")
    # TODO(kiselyovp): a temporary measure, see YT-13024.
    @flaky
    def test_proxy_banned_sticky(self):
        rpc_proxy_addresses = ls("//sys/rpc_proxies")
        try:
            tx = start_transaction(sticky=True)
            fails = 0
            for i, address in enumerate(rpc_proxy_addresses):
                set("//sys/rpc_proxies/{0}/@banned".format(address), True)
                time.sleep(0.2)
                start = time.time()
                try:
                    ping_transaction(tx)
                except YtError:
                    fails += 1
                end = time.time()
                assert end - start < 1.4
                set("//sys/rpc_proxies/{0}/@banned".format(address), False)
            assert fails == 1
        finally:
            for address in rpc_proxy_addresses:
                set(
                    "//sys/rpc_proxies/{0}/@banned".format(address),
                    False,
                    driver=self.native_driver,
                )

    @authors("kiselyovp")
    def test_request_queue_size_limit_exceeded(self):
        create_user("u")
        set("//sys/users/u/@request_queue_size_limit", 0)
        start = time.time()
        with pytest.raises(YtError):
            create("map_node", "//tmp/test", authenticated_user="u")
        end = time.time()
        assert end - start >= 1.4

        rsp = create("map_node", "//tmp/test", authenticated_user="u", return_response=True)
        time.sleep(0.1)
        assert not rsp.is_set()
        set("//sys/users/u/@request_queue_size_limit", 1)
        rsp.wait()
        if not rsp.is_ok():
            raise YtResponseError(rsp.error())
        assert exists("//tmp/test")

    @authors("kiselyovp")
    def test_streaming_without_retries(self):
        create("file", "//tmp/file")
        write_file("//tmp/file", "abacaba")
        assert read_file("//tmp/file") == "abacaba"

        create("table", "//tmp/table")
        write_table("//tmp/table", {"a": "b"})
        assert read_table("//tmp/table") == [{"a": "b"}]

        nodes = ls("//sys/cluster_nodes")
        set_node_banned(nodes[0], True)
        try:
            start = time.time()
            with pytest.raises(YtError):
                write_file("//tmp/file", "dabacaba")
            end = time.time()
            assert end - start < 1.4
        finally:
            set_node_banned(nodes[0], False)


##################################################################


class TestOperationsRpcProxy(TestRpcProxyBase):
    @authors("kiselyovp")
    def test_map_reduce_simple(self):
        self._create_simple_table("//tmp/t_in", data=[self._sample_line])

        self._create_simple_table("//tmp/t_out", dynamic=False)
        map_reduce(in_="//tmp/t_in", out="//tmp/t_out", sort_by="index", reducer_command="cat")

        self._prepare_output_table()

        assert len(select_rows("* from [//tmp/t_out]")) == 1
        assert len(lookup_rows("//tmp/t_out", [{"index": self._sample_index - 2}])) == 0
        assert len(lookup_rows("//tmp/t_out", [{"index": self._sample_index}])) == 1

    @authors("kiselyovp")
    def test_sort(self):
        size = 10 ** 3
        original_table = [{"index": num, "str": "number " + str(num)} for num in range(size)]
        new_table = deepcopy(original_table)
        shuffle(new_table)

        self._create_simple_table("//tmp/t_in1", data=new_table[:size / 2], sorted=False)
        self._create_simple_table("//tmp/t_in2", data=new_table[size / 2:], sorted=False)
        self._create_simple_table("//tmp/t_out", dynamic=False, sorted=True)
        sort(in_=["//tmp/t_in1", "//tmp/t_in2"], out="//tmp/t_out", sort_by="index")

        self._prepare_output_table()

        assert select_rows("* from [//tmp/t_out] LIMIT " + str(2 * size)) == original_table

    @authors("kiselyovp")
    def test_abort_operation(self):
        op = self._start_simple_operation_with_breakpoint()
        wait(lambda: op.get_state() == "running")

        op.abort()

        wait(lambda: op.get_state() == "aborted")

    @authors("kiselyovp")
    def test_complete_operation(self):
        op = self._start_simple_operation_with_breakpoint()
        wait(lambda: op.get_state() == "running")

        op.complete()

        op.track()
        assert op.get_state() == "completed"

    @authors("kiselyovp")
    def test_suspend_resume_operation(self):
        op = self._start_simple_operation_with_breakpoint()
        wait(lambda: op.get_state() == "running")

        op.suspend(abort_running_jobs=True)
        wait(lambda: get(op.get_path() + "/@suspended"))
        assert op.get_state() == "running"
        events_on_fs().release_breakpoint()
        time.sleep(2)
        assert get(op.get_path() + "/@suspended")
        assert op.get_state() == "running"

        op.resume()
        op.track()
        assert op.get_state() == "completed"

    @authors("kiselyovp")
    def test_update_op_params_check_perms(self):
        op = self._start_simple_operation_with_breakpoint()
        wait(lambda: op.get_state() == "running")

        create_user("u")

        update_op_parameters(op.id, parameters={"acl": [make_ace("allow", "u", ["read", "manage"])]})
        # No exception.
        op.complete(authenticated_user="u")

        events_on_fs().release_breakpoint()
        op.track()


##################################################################


class TestDumpJobContextRpcProxy(TestRpcProxyBase):
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_reporter": {
                "enabled": True,
                "reporting_period": 10,
                "min_repeat_delay": 10,
                "max_repeat_delay": 10,
            }
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
            "enable_job_stderr_reporter": True,
        }
    }

    @authors("kiselyovp")
    def test_dump_job_context(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"foo": "bar"})

        op = map(
            track=False,
            label="dump_job_context",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"mapper": {"input_format": "json", "output_format": "json"}},
        )

        jobs = wait_breakpoint()
        # Wait till job starts reading input
        wait(lambda: get(op.get_path() + "/controller_orchid/running_jobs/" + jobs[0] + "/progress") >= 0.5)

        dump_job_context(jobs[0], "//tmp/input_context")

        release_breakpoint()
        op.track()

        context = read_file("//tmp/input_context")
        assert get("//tmp/input_context/@description/type") == "input_context"
        assert JsonFormat().loads_row(context)["foo"] == "bar"


##################################################################


class TestPessimisticQuotaCheckRpcProxy(TestRpcProxyBase):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 0

    REPLICATOR_REACTION_TIME = 3.5

    DELTA_RPC_PROXY_CONFIG = {
        "api_service": {
            "security_manager": {
                "user_cache": {
                    "expire_after_successful_update_time": 1000,
                    "refresh_time": 100,
                    "expire_after_failed_update_time": 100,
                    "expire_after_access_time": 100,
                }
            }
        }
    }

    def _set_account_chunk_count_limit(self, account, value):
        set("//sys/accounts/{0}/@resource_limits/chunk_count".format(account), value)

    def _set_account_tablet_count_limit(self, account, value):
        set("//sys/accounts/{0}/@resource_limits/tablet_count".format(account), value)

    def _is_account_disk_space_limit_violated(self, account):
        return get("//sys/accounts/{0}/@violated_resource_limits/disk_space".format(account))

    def _is_account_chunk_count_limit_violated(self, account):
        return get("//sys/accounts/{0}/@violated_resource_limits/chunk_count".format(account))

    @authors("kiselyovp")
    def test_chunk_count_limits(self):
        create_account("max")
        self._set_account_tablet_count_limit("max", 100500)

        self._create_simple_table("//tmp/t", [self._sample_line])
        create("map_node", "//tmp/a")
        set("//tmp/a/@account", "max")

        self._set_account_chunk_count_limit("max", 0)
        with pytest.raises(YtError):
            copy("//tmp/t", "//tmp/a/t")
        assert not exists("//tmp/a/t")
        copy("//tmp/t", "//tmp/a/t", pessimistic_quota_check=False)
        assert exists("//tmp/a/t")

    @authors("kiselyovp")
    def test_disk_space_limits(self):
        create_account("max")
        self._set_account_tablet_count_limit("max", 100500)

        self._create_simple_table("//tmp/t", [self._sample_line])
        create("map_node", "//tmp/a")
        set("//tmp/a/@account", "max")

        set_account_disk_space_limit("max", 0)
        with pytest.raises(YtError):
            copy("//tmp/t", "//tmp/a/t")
        assert not exists("//tmp/a/t")
        copy("//tmp/t", "//tmp/a/t", pessimistic_quota_check=False)
        assert exists("//tmp/a/t")

    @authors("savrus")
    def test_user_ban(self):
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        create_user("a")
        set("//sys/users/a/@banned", True)
        with pytest.raises(YtError):
            explain_query("1 from [//tmp/t]", authenticated_user="a")
        set("//sys/users/a/@banned", False)
        time.sleep(0.5)
        explain_query("1 from [//tmp/t]", authenticated_user="a")


##################################################################


class TestPessimisticQuotaCheckMulticellRpcProxy(TestPessimisticQuotaCheckRpcProxy):
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_SCHEDULERS = 1


##################################################################


class TestModifyRowsRpcProxy(TestRpcProxyBase):
    BATCH_CAPACITY = 10
    DELTA_RPC_DRIVER_CONFIG = {"modify_rows_batch_capacity": BATCH_CAPACITY}

    def _test_modify_rows_batching(self, request_count, key_count, tx_type="tablet"):
        self._create_simple_table("//tmp/table")
        sync_mount_table("//tmp/table")

        tx = start_transaction(type=tx_type, sticky=True)

        for i in range(request_count):
            insert_rows(
                "//tmp/table",
                [{"index": i % key_count, "str": str(i / key_count)}],
                tx=tx,
            )

        commit_transaction(tx)

        expected_result = [{"index": i, "str": str((request_count - i - 1) // key_count)} for i in range(key_count)]
        assert select_rows("* from [//tmp/table]") == expected_result

        sync_unmount_table("//tmp/table")
        remove("//tmp/table")

    @authors("kiselyovp")
    def test_modify_rows_batching(self):
        self._test_modify_rows_batching(60, 7, "tablet")
        self._test_modify_rows_batching(65, 7, "master")


##################################################################


class TestRpcProxyWithoutDiscovery(TestRpcProxyBase):
    NUM_RPC_PROXIES = 1
    ENABLE_HTTP_PROXY = False
    NUM_HTTP_PROXIES = 0
    DELTA_RPC_PROXY_CONFIG = {"discovery_service": {"enable": False}}

    @authors("sashbel")
    def test_proxy_without_discovery(self):
        # check discovery dir is empty
        assert ls("//sys/rpc_proxies") == []

        self._create_simple_table("//tmp/t_in", data=[self._sample_line])
        sync_mount_table("//tmp/t_in")
        assert select_rows("* from [//tmp/t_in]") == [self._sample_line]


##################################################################


class TestCompressionRpcProxy(YTEnvSetup):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True
    USE_DYNAMIC_TABLES = True

    DELTA_DRIVER_CONFIG = {
        "request_codec": "lz4",
        "response_codec": "quick_lz",
    }

    @authors("babenko")
    def test_simple_rpc_calls(self):
        set("//tmp/@foo", 1)
        get("//tmp/@foo")

    @authors("babenko")
    def test_rpc_streaming(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})
        read_table("//tmp/t")

    @authors("babenko")
    def test_rpc_attachments(self):
        sync_create_cells(1)
        create_dynamic_table(
            "//tmp/d",
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
        )
        sync_mount_table("//tmp/d")
        insert_rows("//tmp/d", [{"key": 0, "value": "foo"}])
        lookup_rows("//tmp/d", [{"key": 0}])


class TestModernCompressionRpcProxy(TestCompressionRpcProxy):
    DELTA_DRIVER_CONFIG = {
        "request_codec": "lz4",
        "response_codec": "quick_lz",
        "enable_legacy_rpc_codecs": False,
    }


##################################################################


class TestRpcProxyFormatConfig(TestRpcProxyBase, _TestProxyFormatConfigBase):
    def setup(self):
        proxy_name = ls("//sys/rpc_proxies")[0]

        set("//sys/rpc_proxies/@config", {"formats": self.FORMAT_CONFIG})

        def config_updated():
            config = get("//sys/rpc_proxies/" + proxy_name + "/orchid/dynamic_config_manager/effective_config")
            return config \
                .get("formats", {}) \
                .get("yamred_dsv", {}) \
                .get("user_overrides", {}) \
                .get("good_user", False)

        wait(config_updated)

    def _do_run_operation(self, op_type, spec, user, use_start_op):
        operation = Operation()
        params = {
            "operation_type": op_type,
            "spec": spec,
            "authenticated_user": user,
        }
        operation.id = yson.loads(execute_command("start_op", params))["operation_id"]
        return operation

    def _test_format_enable(self, format, user, enable):
        self._test_format_enable_operations(format, user, enable)

    def _test_format_defaults(self, format, user, content, expected_format):
        assert str(expected_format) == "yson"
        yson_format = expected_format.attributes.get("format", "text")
        expected_content_operation = yson.dumps(content, yson_format=yson_format, yson_type="list_fragment")
        self._test_format_defaults_operations(format, user, content, expected_content_operation)
