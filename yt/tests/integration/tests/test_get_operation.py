from yt_env_setup import YTEnvSetup
from yt_commands import *

import yt.environment.init_operation_archive as init_operation_archive
from yt.test_helpers import wait

from test_rpc_proxy import create_input_table

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

class TestGetOperation(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

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

    def setup(self):
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client(), override_tablet_cell_bundle="default")

    def teardown(self):
        remove("//sys/operations_archive")

    def test_get_operation(self):
        create_input_table("//tmp/t1",
            [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}],
            [{"name": "foo", "type": "string"}],
            driver_backend=self.DRIVER_BACKEND)

        create("table", "//tmp/t2")

        op = map(
            dont_track=True,
            label="get_job_stderr",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={
                "mapper": {
                    "input_format": "json",
                    "output_format": "json"
                }
            })
        wait_breakpoint()

        wait(lambda: exists(op.get_path()))
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs/running") == 1)

        res_get_operation = get_operation(op.id, include_scheduler=True)
        res_cypress = _get_operation_from_cypress(op.id)
        res_orchid_progress = get(_get_orchid_operation_path(op.id))

        def filter_attrs(attrs):
            PROPER_ATTRS = [
                "id",
                "authenticated_user",
                "brief_progress",
                "brief_spec",
                "runtime_parameters",
                "finish_time",
                "type",
                # COMPAT(levysotsky): Old name for "type"
                "operation_type",
                "result",
                "start_time",
                "state",
                "suspended",
                "spec",
                "unrecognized_spec",
                "full_spec",
                "slot_index_per_pool_tree",
                "alerts",
            ]
            return {key: attrs[key] for key in PROPER_ATTRS if key in attrs}

        assert filter_attrs(res_get_operation) == filter_attrs(res_cypress)

        res_get_operation_progress = res_get_operation["progress"]

        for key in res_orchid_progress:
            if key != "build_time":
                assert key in res_get_operation_progress

        release_breakpoint()
        op.track()

        res_cypress_finished = _get_operation_from_cypress(op.id)

        clean_operations()

        res_get_operation_archive = get_operation(op.id)

        del res_cypress_finished["progress"]["build_time"]
        del res_get_operation_archive["progress"]["build_time"]
        for key in res_get_operation_archive.keys():
            if key in res_cypress:
                assert res_get_operation_archive[key] == res_cypress_finished[key]

    def test_attributes(self):
        create_input_table("//tmp/t1",
            [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}],
            [{"name": "foo", "type": "string"}],
            driver_backend=self.DRIVER_BACKEND)

        create("table", "//tmp/t2")

        op = map(
            dont_track=True,
            label="get_job_stderr",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={
                "mapper": {
                    "input_format": "json",
                    "output_format": "json"
                }
            })
        wait_breakpoint()

        assert list(get_operation(op.id, attributes=["state"])) == ["state"]
        with pytest.raises(YtError):
            get_operation(op.id, attributes=["PYSCH"])

        for read_from in ("cache", "follower"):
            res_get_operation = get_operation(op.id, attributes=["progress", "state"], include_scheduler=True, read_from=read_from)
            res_cypress = get(op.get_path() + "/@", attributes=["progress", "state"])

            assert sorted(list(res_get_operation)) == ["progress", "state"]
            assert sorted(list(res_cypress)) == ["progress", "state"]
            assert res_get_operation["state"] == res_cypress["state"]
            assert ("alerts" in res_get_operation) == ("alerts" in res_cypress)
            if "alerts" in res_get_operation and "alerts" in res_cypress:
                assert res_get_operation["alerts"] == res_cypress["alerts"]

        release_breakpoint()
        op.track()

        clean_operations()

        requesting_attributes = ["progress", "runtime_parameters", "slot_index_per_pool_tree", "state"]
        res_get_operation_archive = get_operation(op.id, attributes=requesting_attributes)
        assert sorted(list(res_get_operation_archive)) == requesting_attributes
        assert res_get_operation_archive["state"] == "completed"
        assert res_get_operation_archive["runtime_parameters"]["scheduling_options_per_pool_tree"]["default"]["pool"] == "root"
        assert res_get_operation_archive["slot_index_per_pool_tree"]["default"] == 0
        with pytest.raises(YtError):
            get_operation(op.id, attributes=["PYSCH"])

    def test_get_operation_and_half_deleted_operation_node(self):
        create_input_table("//tmp/t1",
            [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}],
            [{"name": "foo", "type": "string"}],
            driver_backend=self.DRIVER_BACKEND)

        create("table", "//tmp/t2")

        op = map(in_="//tmp/t1",
            out="//tmp/t2",
            command="cat")

        tx = start_transaction(timeout=300 * 1000)
        lock(op.get_path(),
            mode="shared",
            child_key="completion_transaction_id",
            transaction_id=tx)

        try:
            cleaner_path = "//sys/scheduler/config/operations_cleaner"
            set(cleaner_path + "/enable", True, recursive=True)
            wait(lambda: not exists("//sys/operations/" + op.id))
            wait(lambda: exists(op.get_path()))
            wait(lambda: "state" in get_operation(op.id))
        finally:
            set(cleaner_path + "/enable", False)

##################################################################

class TestGetOperationRpcProxy(TestGetOperation):
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_PROXY = True

