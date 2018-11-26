from yt_env_setup import YTEnvSetup
from yt_commands import *

import yt.environment.init_operation_archive as init_operation_archive
from yt.test_helpers import wait

from operations_archive import clean_operations

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

    def setup(self):
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client())

    def teardown(self):
        remove("//sys/operations_archive")

    def test_get_operation(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

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

        clean_operations(self.Env.create_native_client())

        res_get_operation_archive = get_operation(op.id)

        del res_cypress_finished["progress"]["build_time"]
        del res_get_operation_archive["progress"]["build_time"]
        for key in res_get_operation_archive.keys():
            if key in res_cypress:
                assert res_get_operation_archive[key] == res_cypress_finished[key]

    def test_attributes(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

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

        release_breakpoint()
        op.track()

        clean_operations(self.Env.create_native_client())

        requesting_attributes = ["progress", "runtime_parameters", "slot_index_per_pool_tree", "state"]
        res_get_operation_archive = get_operation(op.id, attributes=requesting_attributes)
        assert sorted(list(res_get_operation_archive)) == requesting_attributes
        assert res_get_operation_archive["state"] == "completed"
        assert res_get_operation_archive["runtime_parameters"]["scheduling_options_per_pool_tree"]["default"]["pool"] == "root"
        assert res_get_operation_archive["slot_index_per_pool_tree"]["default"] == 0
        with pytest.raises(YtError):
            get_operation(op.id, attributes=["PYSCH"])

    def test_get_operation_and_half_deleted_operation_node(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        op = map(in_="//tmp/t1",
            out="//tmp/t2",
            command="cat")

        tx = start_transaction(timeout=300 * 1000)
        lock(op.get_path(),
            mode="shared",
            child_key="completion_transaction_id",
            transaction_id=tx)

        clean_operations(self.Env.create_native_client())
        assert not exists("//sys/operations/" + op.id)
        assert exists(op.get_path())
        assert "state" in get_operation(op.id)
