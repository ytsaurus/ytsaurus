from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait, wait_breakpoint, release_breakpoint, with_breakpoint, create, ls, get,
    set, remove,
    exists, make_ace, start_transaction, lock, insert_rows, lookup_rows, write_table, map, vanilla, abort_op,
    complete_op, suspend_op, resume_op, get_operation, list_operations, run_test_vanilla,
    clean_operations, get_operation_cypress_path, sync_create_cells,
    sync_mount_table, sync_unmount_table, update_controller_agent_config,
    update_op_parameters, raises_yt_error)

import yt_error_codes

from yt_helpers import profiler_factory

import yt.environment.init_operations_archive as init_operations_archive

from yt.common import uuid_to_parts, YtError

import pytest


def _get_orchid_operation_path(op_id):
    return "//sys/scheduler/orchid/scheduler/operations/{0}/progress".format(op_id)


def _get_operation_from_cypress(op_id):
    result = get(get_operation_cypress_path(op_id) + "/@")
    if "full_spec" in result:
        result["full_spec"].attributes.pop("opaque", None)
    result["type"] = result["operation_type"]
    result["id"] = op_id
    return result


def _get_operation_from_archive(op_id):
    id_hi, id_lo = uuid_to_parts(op_id)
    archive_table_path = "//sys/operations_archive/ordered_by_id"
    rows = lookup_rows(archive_table_path, [{"id_hi": id_hi, "id_lo": id_lo}])
    if rows:
        return rows[0]
    else:
        return {}


def get_running_job_count(op_id):
    wait(lambda: "brief_progress" in get_operation(op_id, attributes=["brief_progress"]))
    result = get_operation(op_id, attributes=["brief_progress"])
    return result["brief_progress"]["jobs"]["running"]


class TestGetOperation(YTEnvSetup):
    NUM_TEST_PARTITIONS = 4
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_DRIVER_CONFIG = {
        "default_get_operation_timeout": 10 * 1000,
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "operations_cleaner": {
                "enable": False,
                "analysis_period": 100,
                # Cleanup all operations
                "hard_retained_operation_count": 0,
                "clean_delay": 0,
            },
            "static_orchid_cache_update_period": 100,
            "alerts_update_period": 100,
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operation_build_progress_period": 100,
        }
    }

    def setup_method(self, method):
        super(TestGetOperation, self).setup_method(method)
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )

    def teardown_method(self, method):
        remove("//sys/operations_archive", force=True)
        super(TestGetOperation, self).teardown_method(method)

    def clean_build_time(self, operation_result):
        del operation_result["brief_progress"]["build_time"]
        del operation_result["progress"]["build_time"]

    @authors("omgronny", "babenko", "ignat")
    def test_get_operation(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        op = map(
            track=False,
            label="get_job_stderr",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"mapper": {"input_format": "json", "output_format": "json"}},
        )
        wait_breakpoint()

        wait(lambda: exists(op.get_path()))

        wait(lambda: get_running_job_count(op.id) == 1)

        res_get_operation = get_operation(op.id, include_scheduler=True)
        res_cypress = _get_operation_from_cypress(op.id)
        res_orchid_progress = get(_get_orchid_operation_path(op.id))

        def filter_attrs(attrs):
            PROPER_ATTRS = [
                "id",
                "authenticated_user",
                "brief_spec",
                "runtime_parameters",
                "finish_time",
                "type",
                # COMPAT(levysotsky): Old name for "type"
                "operation_type",
                "provided_spec",
                "result",
                "start_time",
                "state",
                "suspended",
                "spec",
                "unrecognized_spec",
                "full_spec",
                "scheduling_attributes_per_pool_tree",
                "slot_index_per_pool_tree",
                "alerts",
                "controller_features",
            ]
            return {key: attrs[key] for key in PROPER_ATTRS if key in attrs}

        assert filter_attrs(res_get_operation) == filter_attrs(res_cypress)

        res_get_operation_progress = res_get_operation["progress"]

        assert res_get_operation["operation_type"] == res_get_operation["type"]

        for key in res_orchid_progress:
            if key != "build_time":
                assert key in res_get_operation_progress

        release_breakpoint()
        op.track()

        res_cypress_finished = _get_operation_from_cypress(op.id)

        clean_operations()

        res_get_operations_archive = get_operation(op.id)

        for key in res_get_operations_archive.keys():
            if key in res_cypress:
                assert res_get_operations_archive[key] == res_cypress_finished[key]

        res_get_operations_archive_raw = _get_operation_from_archive(op.id)

        for key in res_get_operations_archive_raw.keys():
            if key in res_cypress and key not in ["start_time", "finish_time"]:
                assert res_get_operations_archive_raw[key] == res_cypress_finished[key]

    # Check that operation that has not been saved by operation cleaner
    # is reported correctly (i.e. "No such operation").
    # Actually, cleaner is disabled in this test,
    # but we emulate its work by removing operation node from Cypress.
    @authors("omgronny")
    def test_get_operation_dropped_by_cleaner(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])
        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
        )
        wait_breakpoint()

        wait(lambda: _get_operation_from_archive(op.id))

        release_breakpoint()
        op.track()

        remove(op.get_path(), force=True)
        with raises_yt_error(yt_error_codes.NoSuchOperation):
            get_operation(op.id)

    @authors("ilpauzner")
    def test_progress_merge(self):
        update_controller_agent_config("enable_operation_progress_archivation", False)
        self.do_test_progress_merge()

    def do_test_progress_merge(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        op = map(
            track=False,
            label="get_job_stderr",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"mapper": {"input_format": "json", "output_format": "json"}},
        )
        wait_breakpoint()

        id_hi, id_lo = uuid_to_parts(op.id)
        archive_table_path = "//sys/operations_archive/ordered_by_id"
        brief_progress = {"ivan": "ivanov", "build_time": "2100-01-01T00:00:00.000000Z"}
        progress = {
            "semen": "semenych",
            "semenych": "gorbunkov",
            "build_time": "2100-01-01T00:00:00.000000Z",
        }

        insert_rows(
            archive_table_path,
            [
                {
                    "id_hi": id_hi,
                    "id_lo": id_lo,
                    "brief_progress": brief_progress,
                    "progress": progress,
                }
            ],
            update=True,
        )

        wait(lambda: _get_operation_from_archive(op.id))

        res_get_operation_new = get_operation(op.id)
        self.clean_build_time(res_get_operation_new)
        assert res_get_operation_new["brief_progress"] == {"ivan": "ivanov"}
        assert res_get_operation_new["progress"] == {
            "semen": "semenych",
            "semenych": "gorbunkov",
        }

        release_breakpoint()
        op.track()

        clean_operations()
        res_get_operation_new = get_operation(op.id)
        self.clean_build_time(res_get_operation_new)
        assert res_get_operation_new["brief_progress"] != {"ivan": "ivanov"}
        assert res_get_operation_new["progress"] != {
            "semen": "semenych",
            "semenych": "gorbunkov",
        }

    @authors("ignat")
    @pytest.mark.parametrize("annotations", [{}, {"foo": "abc"}])
    def test_attributes(self, annotations):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        op = map(
            track=False,
            label="get_job_stderr",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"mapper": {"input_format": "json", "output_format": "json"}, "annotations": annotations},
        )
        wait_breakpoint()

        wait(lambda: _get_operation_from_archive(op.id))
        assert list(get_operation(op.id, attributes=["state"])) == ["state"]
        assert list(get_operation(op.id, attributes=["progress"])) == ["progress"]

        with pytest.raises(YtError):
            get_operation(op.id, attributes=["PYSCH"])

        for read_from in ("cache", "follower"):
            res_get_operation = get_operation(
                op.id,
                attributes=["progress", "state"],
                include_scheduler=True,
                read_from=read_from,
            )
            wait(lambda: "state" in get(op.get_path() + "/@", attributes=["state"]))
            res_cypress = get(op.get_path() + "/@", attributes=["progress", "state"])

            assert sorted(list(res_get_operation)) == ["progress", "state"]
            assert "state" in sorted(list(res_cypress))
            assert res_get_operation["state"] == res_cypress["state"]
            assert ("alerts" in res_get_operation) == ("alerts" in res_cypress)
            if "alerts" in res_get_operation and "alerts" in res_cypress:
                assert res_get_operation["alerts"] == res_cypress["alerts"]

        with raises_yt_error(yt_error_codes.NoSuchAttribute):
            get_operation(op.id, attributes=["nonexistent-attribute-ZZZ"])

        assert get_operation(op.id, attributes=["runtime_parameters"])["runtime_parameters"]["annotations"] == annotations

        release_breakpoint()
        op.track()

        clean_operations()

        assert list(get_operation(op.id, attributes=["progress"])) == ["progress"]

        requesting_attributes = [
            "progress",
            "runtime_parameters",
            "scheduling_attributes_per_pool_tree",
            "slot_index_per_pool_tree",
            "state",
        ]
        res_get_operations_archive = get_operation(op.id, attributes=requesting_attributes)
        assert sorted(list(res_get_operations_archive)) == requesting_attributes
        assert res_get_operations_archive["state"] == "completed"
        assert (
            res_get_operations_archive["runtime_parameters"]["scheduling_options_per_pool_tree"]["default"]["pool"]
            == "root"
        )
        assert res_get_operations_archive["runtime_parameters"]["annotations"] == annotations
        assert res_get_operations_archive["slot_index_per_pool_tree"]["default"] == 0
        assert res_get_operations_archive["scheduling_attributes_per_pool_tree"]["default"]["slot_index"] == 0

        with raises_yt_error(yt_error_codes.NoSuchAttribute):
            get_operation(op.id, attributes=["nonexistent-attribute-ZZZ"])

    @authors("gritukan")
    def test_task_names_attribute_vanilla(self):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "master": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT"),
                    },
                    "slave": {
                        "job_count": 2,
                        "command": with_breakpoint("BREAKPOINT"),
                    },
                },
            },
        )

        wait(lambda: len(op.get_running_jobs()) == 3)

        def check_task_names():
            assert sorted(list(get_operation(op.id, attributes=["task_names"])["task_names"])) == ["master", "slave"]

        check_task_names()

        release_breakpoint()
        op.track()
        clean_operations()

        check_task_names()

    @authors("omgronny")
    def test_get_operation_and_half_deleted_operation_node(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        op = map(in_="//tmp/t1", out="//tmp/t2", command="cat")

        tx = start_transaction(timeout=300 * 1000)
        lock(
            op.get_path(),
            mode="shared",
            child_key="completion_transaction_id",
            transaction_id=tx,
        )

        cleaner_path = "//sys/scheduler/config/operations_cleaner"
        set(cleaner_path + "/enable", True, recursive=True)
        wait(lambda: not exists("//sys/operations/" + op.id))
        wait(lambda: exists(op.get_path()))
        wait(lambda: "state" in get_operation(op.id))

    @authors("kiselyovp", "ilpauzner")
    def test_not_existing_operation(self):
        with raises_yt_error(yt_error_codes.NoSuchOperation):
            get_operation("00000000-00000000-0000000-00000001")

    @authors("omgronny")
    def test_archive_progress(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        op = map(
            track=False,
            label="get_job_stderr",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"mapper": {"input_format": "json", "output_format": "json"}},
        )
        wait_breakpoint()

        wait(lambda: _get_operation_from_archive(op.id))

        row = _get_operation_from_archive(op.id)
        assert "progress" in row
        assert "brief_progress" in row

        release_breakpoint()
        op.track()

    @authors("omgronny")
    def test_archive_failure(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        # Unmount table to check that controller agent writes to Cypress during archive unavailability.
        sync_unmount_table("//sys/operations_archive/ordered_by_id")

        op = map(
            track=False,
            label="get_job_stderr",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"mapper": {"input_format": "json", "output_format": "json"}},
        )

        wait_breakpoint()
        wait(lambda: _get_operation_from_cypress(op.id).get("brief_progress", {}).get("jobs", {}).get("running") == 1)
        wait(lambda: _get_operation_from_cypress(op.id).get("progress", {}).get("jobs", {}).get("running") == 1)

        res_api = get_operation(op.id)
        self.clean_build_time(res_api)
        res_cypress = _get_operation_from_cypress(op.id)
        self.clean_build_time(res_cypress)

        assert res_api["brief_progress"] == res_cypress["brief_progress"]
        assert res_api["progress"] == res_cypress["progress"]

        sync_mount_table("//sys/operations_archive/ordered_by_id")

        wait(lambda: _get_operation_from_archive(op.id))
        assert _get_operation_from_archive(op.id).get("brief_progress", {}).get("jobs", {}).get("running") == 1
        assert _get_operation_from_archive(op.id).get("progress", {}).get("jobs", {}).get("running") == 1
        release_breakpoint()
        op.track()

        res_api = get_operation(op.id)
        res_cypress = _get_operation_from_cypress(op.id)

        assert res_api["brief_progress"]["jobs"]["running"] == 0
        # Brief progress in Cypress is obsolete as archive is up now.
        assert res_cypress["brief_progress"]["jobs"]["running"] > 0

        assert res_api["progress"]["jobs"]["running"] == 0
        # Progress in Cypress is obsolete as archive is up now.
        assert res_cypress["progress"]["jobs"]["running"] > 0

        sync_unmount_table("//sys/operations_archive/ordered_by_id")

        with raises_yt_error(yt_error_codes.RetriableArchiveError):
            get_operation(op.id, maximum_cypress_progress_age=0)

        if self.ENABLE_RPC_PROXY:
            proxy = ls("//sys/rpc_proxies")[0]
            profiler = profiler_factory().at_rpc_proxy(proxy)

            get_operation_from_archive_success_counter = profiler.counter("native_client/get_operation_from_archive_success_count")
            get_operation_from_archive_timeout_counter = profiler.counter("native_client/get_operation_from_archive_timeout_count")
            get_operation_from_archive_failure_counter = profiler.counter("native_client/get_operation_from_archive_failure_count")

            with raises_yt_error(yt_error_codes.RetriableArchiveError):
                get_operation(op.id, maximum_cypress_progress_age=0)

            assert get_operation_from_archive_success_counter.get_delta() == 0
            assert get_operation_from_archive_timeout_counter.get_delta() == 0
            wait(lambda: get_operation_from_archive_failure_counter.get_delta() > 0)

        sync_mount_table("//sys/operations_archive/ordered_by_id")

        clean_operations()

        res_api = get_operation(op.id)
        res_archive = _get_operation_from_archive(op.id)

        assert res_api["brief_progress"] == res_archive["brief_progress"]
        assert res_api["progress"] == res_archive["progress"]

        # Unmount table again and check that error is _not_ "No such operation".
        sync_unmount_table("//sys/operations_archive/ordered_by_id")
        with raises_yt_error(yt_error_codes.TabletNotMounted):
            get_operation(op.id)

    @authors("omgronny")
    def test_get_operation_no_archive(self):
        remove("//sys/operations_archive", force=True)
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
        )

        wait_breakpoint()
        wait(lambda: _get_operation_from_cypress(op.id).get("brief_progress", {}).get("jobs", {}).get("running") == 1)

        res_api = get_operation(op.id)
        self.clean_build_time(res_api)
        res_cypress = _get_operation_from_cypress(op.id)
        self.clean_build_time(res_cypress)

        assert res_api["brief_progress"] == res_cypress["brief_progress"]
        assert res_api["progress"] == res_cypress["progress"]

    @authors("ignat")
    def test_get_operation_profiler(self):
        if not self.ENABLE_RPC_PROXY:
            pytest.skip()

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        op = run_test_vanilla("sleep 1", job_count=1)

        clean_operations()

        proxy = ls("//sys/rpc_proxies")[0]
        profiler = profiler_factory().at_rpc_proxy(proxy)

        get_operation_from_archive_success_counter = profiler.counter("native_client/get_operation_from_archive_success_count")
        get_operation_from_archive_timeout_counter = profiler.counter("native_client/get_operation_from_archive_timeout_count")
        get_operation_from_archive_failure_counter = profiler.counter("native_client/get_operation_from_archive_failure_count")

        get_operation(op.id)

        wait(lambda: get_operation_from_archive_success_counter.get_delta() == 1)
        assert get_operation_from_archive_timeout_counter.get_delta() == 0
        assert get_operation_from_archive_failure_counter.get_delta() == 0

    @authors("omgronny")
    def test_get_scheduling_attributes_per_pool_tree(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        op = map(
            track=False,
            label="get_job_stderr",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"mapper": {"input_format": "json", "output_format": "json"}},
        )
        wait_breakpoint()

        wait(lambda: _get_operation_from_archive(op.id))
        assert get_operation(op.id)["scheduling_attributes_per_pool_tree"] == {"default": {"slot_index": 0}}

        release_breakpoint()
        clean_operations()

        assert get_operation(op.id)["scheduling_attributes_per_pool_tree"] == {"default": {"slot_index": 0}}


##################################################################


class TestGetOperationRpcProxy(TestGetOperation):
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    NUM_RPC_PROXIES = 1
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True

    DELTA_RPC_PROXY_CONFIG = {
        "cluster_connection": {
            "default_get_operation_timeout": 10 * 1000,
        },
    }


class TestGetOperationHeavyRuntimeParameters(TestGetOperation):
    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "operations_cleaner": {
                "enable": False,
                "analysis_period": 100,
                # Cleanup all operations
                "hard_retained_operation_count": 0,
                "clean_delay": 0,
            },
            "static_orchid_cache_update_period": 100,
            "alerts_update_period": 100,
            "enable_heavy_runtime_parameters": True,
        },
    }


##################################################################


class TestOperationAliases(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 3

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operations_cleaner": {
                "enable": False,
                # Analyze all operations each 100ms
                "analysis_period": 100,
                # Wait each batch to remove not more than 100ms
                "remove_batch_timeout": 100,
                # Wait each batch to archive not more than 100ms
                "archive_batch_timeout": 100,
                # Retry sleeps
                "min_archivation_retry_sleep_delay": 100,
                "max_archivation_retry_sleep_delay": 110,
                # Leave no more than 5 completed operations
                "soft_retained_operation_count": 0,
                # Operations older than 50ms can be considered for removal
                "clean_delay": 50,
            },
            "static_orchid_cache_update_period": 100,
            "alerts_update_period": 100,
        }
    }

    def setup_method(self, method):
        super(TestOperationAliases, self).setup_method(method)
        # Init operations archive.
        sync_create_cells(1)

    @authors("max42")
    def test_aliases(self):
        with pytest.raises(YtError):
            # Alias should start with *.
            vanilla(
                spec={
                    "tasks": {"main": {"command": "sleep 1000", "job_count": 1}},
                    "alias": "my_op",
                }
            )

        op = vanilla(
            spec={
                "tasks": {"main": {"command": "sleep 1000", "job_count": 1}},
                "alias": "*my_op",
            },
            track=False,
        )

        assert ls("//sys/scheduler/orchid/scheduler/operations") == [op.id, "*my_op"]
        wait(lambda: op.get_state() == "running")

        def get_op(path):
            data = get(path)
            del data["progress"]
            return data

        assert get_op("//sys/scheduler/orchid/scheduler/operations/" + op.id) == get_op(
            "//sys/scheduler/orchid/scheduler/operations/\\*my_op"
        )

        assert list_operations()["operations"][0]["brief_spec"]["alias"] == "*my_op"

        # It is not allowed to use alias of already running operation.
        with pytest.raises(YtError):
            vanilla(
                spec={
                    "tasks": {"main": {"command": "sleep 1000", "job_count": 1}},
                    "alias": "*my_op",
                }
            )

        suspend_op("*my_op")
        assert get(op.get_path() + "/@suspended")
        resume_op("*my_op")
        assert not get(op.get_path() + "/@suspended")
        update_op_parameters("*my_op", parameters={"acl": [make_ace("allow", "u", ["manage", "read"])]})
        assert len(get(op.get_path() + "/@alerts")) == 1
        wait(lambda: get(op.get_path() + "/@state") == "running")
        abort_op("*my_op")
        assert get(op.get_path() + "/@state") == "aborted"

        with pytest.raises(YtError):
            complete_op("*my_another_op")

        with pytest.raises(YtError):
            complete_op("my_op")

        # Now using alias *my_op is ok.
        op = vanilla(
            spec={
                "tasks": {"main": {"command": "sleep 1000", "job_count": 1}},
                "alias": "*my_op",
            },
            track=False,
        )

        wait(lambda: get(op.get_path() + "/@state") == "running")

        complete_op("*my_op")
        assert get(op.get_path() + "/@state") == "completed"

    @authors("max42")
    def test_get_operation_latest_archive_version(self):
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )

        # When no operation is assigned to an alias, get_operation should return an error.
        with pytest.raises(YtError):
            get_operation("*my_op", include_runtime=True)

        op = vanilla(
            spec={
                "tasks": {"main": {"command": "sleep 1000", "job_count": 1}},
                "alias": "*my_op",
            },
            track=False,
        )
        wait(lambda: get(op.get_path() + "/@state") == "running")

        with pytest.raises(YtError):
            # It is impossible to resolve aliases without including runtime.
            get_operation("*my_op", include_runtime=False)

        info = get_operation("*my_op", include_runtime=True)
        assert info["id"] == op.id
        assert info["type"] == "vanilla"

        op.complete()

        # Operation should still be exposed via Orchid, and get_operation will extract information from there.
        assert get(r"//sys/scheduler/orchid/scheduler/operations/\*my_op") == {"operation_id": op.id}
        info = get_operation("*my_op", include_runtime=True)
        assert info["id"] == op.id
        assert info["type"] == "vanilla"

        assert exists(op.get_path())

        clean_operations()

        # Alias should become removed from the Orchid (but it may happen with some visible delay, so we wait for it).
        wait(lambda: not exists("//sys/scheduler/orchid/scheduler/operations/\\*my_op"))
        # But get_operation should still work as expected.
        info = get_operation("*my_op", include_runtime=True)
        assert info["id"] == op.id
        assert info["type"] == "vanilla"


class TestOperationAliasesRpcProxy(TestOperationAliases):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True
