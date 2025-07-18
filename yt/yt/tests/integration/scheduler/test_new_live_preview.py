from yt_env_setup import YTEnvSetup
from yt_commands import (
    authors, print_debug, wait, wait_breakpoint, release_breakpoint, with_breakpoint, create,
    get, exists, concatenate, create_user, create_test_tables, ls, make_ace, add_member,
    read_table, write_table, map, map_reduce, reduce, sort,
    sync_create_cells, sync_mount_table)

from yt_type_helpers import make_schema

import yt.yson as yson

from yt.common import YtError

import pytest


##################################################################


@pytest.mark.enabled_multidaemon
class TestNewLivePreview(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_SCHEDULERS = 1
    NUM_NODES = 3
    USE_DYNAMIC_TABLES = True
    ENABLE_BULK_INSERT = True

    def setup_method(self, method):
        super(TestNewLivePreview, self).setup_method(method)
        sync_create_cells(1)

    @authors("max42", "gritukan")
    def test_new_live_preview_simple(self):
        data = [{"foo": i} for i in range(3)]

        create("table", "//tmp/t1")
        write_table("//tmp/t1", data)

        create("table", "//tmp/t2")

        op = map(
            wait_for_jobs=True,
            track=False,
            command=with_breakpoint("BREAKPOINT ; cat"),
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"data_size_per_job": 1},
        )

        jobs = wait_breakpoint(job_count=3)

        assert exists(op.get_path() + "/controller_orchid")

        release_breakpoint(job_id=jobs[0])
        release_breakpoint(job_id=jobs[1])
        wait(lambda: op.get_job_count("completed") == 2)

        live_preview_path = op.get_path() + "/controller_orchid/data_flow_graph/vertices/map/live_previews/0"
        live_preview_data = read_table(live_preview_path)
        assert len(live_preview_data) == 2
        assert all(record in data for record in live_preview_data)

        create("table", "//tmp/lp")
        concatenate([live_preview_path], "//tmp/lp")

        release_breakpoint(job_id=jobs[2])
        op.track()

        live_preview_data = read_table("//tmp/lp")
        assert len(live_preview_data) == 2
        assert all(record in data for record in live_preview_data)

    @authors("max42", "gritukan")
    def test_new_live_preview_intermediate_data_acl(self):
        create_user("u1")
        create_user("u2")

        data = [{"foo": i} for i in range(3)]

        create("table", "//tmp/t1")
        write_table("//tmp/t1", data)

        create("table", "//tmp/t2")

        op = map(
            wait_for_jobs=True,
            track=False,
            command=with_breakpoint("BREAKPOINT ; cat"),
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "data_size_per_job": 1,
                "acl": [make_ace("allow", "u1", "read")],
            },
        )

        jobs = wait_breakpoint(job_count=2)

        assert exists(op.get_path() + "/controller_orchid")

        release_breakpoint(job_id=jobs[0])
        release_breakpoint(job_id=jobs[1])
        wait(lambda: op.get_job_count("completed") == 2)

        read_table(
            op.get_path() + "/controller_orchid/data_flow_graph/vertices/map/live_previews/0",
            authenticated_user="u1",
        )

        with pytest.raises(YtError):
            read_table(
                op.get_path() + "/controller_orchid/data_flow_graph/vertices/map/live_previews/0",
                authenticated_user="u2",
            )

    @authors("max42", "gritukan")
    def test_new_live_preview_ranges(self):
        create("table", "//tmp/t1")
        for i in range(3):
            write_table("<append=%true>//tmp/t1", [{"a": i}])

        create("table", "//tmp/t2")

        op = map_reduce(
            wait_for_jobs=True,
            track=False,
            mapper_command='for ((i=0; i<3; i++)); do echo "{a=$(($YT_JOB_INDEX*3+$i))};"; done',
            reducer_command=with_breakpoint("cat; BREAKPOINT"),
            reduce_by="a",
            sort_by=["a"],
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"map_job_count": 3, "partition_count": 1},
        )

        wait(lambda: op.get_job_count("completed") == 3)

        assert exists(op.get_path() + "/controller_orchid")

        live_preview_path = (
            op.get_path() + "/controller_orchid/data_flow_graph/vertices/partition_map(0)/live_previews/0"
        )
        live_preview_data = read_table(live_preview_path)

        assert len(live_preview_data) == 9

        # We try all possible combinations of chunk and row index ranges and check that everything works as expected.
        expected_all_ranges_data = []
        all_ranges = []
        for lower_row_index in list(range(10)) + [None]:
            for upper_row_index in list(range(10)) + [None]:
                for lower_chunk_index in list(range(4)) + [None]:
                    for upper_chunk_index in list(range(4)) + [None]:
                        lower_limit = dict()
                        real_lower_index = 0
                        if lower_row_index is not None:
                            lower_limit["row_index"] = lower_row_index
                            real_lower_index = max(real_lower_index, lower_row_index)
                        if lower_chunk_index is not None:
                            lower_limit["chunk_index"] = lower_chunk_index
                            real_lower_index = max(real_lower_index, lower_chunk_index * 3)

                        upper_limit = dict()
                        real_upper_index = 9
                        if upper_row_index is not None:
                            upper_limit["row_index"] = upper_row_index
                            real_upper_index = min(real_upper_index, upper_row_index)
                        if upper_chunk_index is not None:
                            upper_limit["chunk_index"] = upper_chunk_index
                            real_upper_index = min(real_upper_index, upper_chunk_index * 3)

                        all_ranges.append({"lower_limit": lower_limit, "upper_limit": upper_limit})
                        expected_all_ranges_data += [live_preview_data[real_lower_index:real_upper_index]]

        all_ranges_path = (
            b"<"
            + yson.dumps({"ranges": all_ranges}, yson_type="map_fragment", yson_format="text")
            + b">"
            + live_preview_path.encode("ascii")
        )

        all_ranges_data = read_table(all_ranges_path, verbose=False)

        position = 0
        for i, range_ in enumerate(expected_all_ranges_data):
            if all_ranges_data[position:position + len(range_)] != range_:
                print_debug(f"Position: {position}, range: {all_ranges[i]}")
                print_debug(f"Expected: {range_}")
                print_debug(f"Actual: {all_ranges_data[position:position + len(range_)]}")
                assert all_ranges_data[position:position + len(range_)] == range_
            position += len(range_)

        release_breakpoint()
        op.track()

    @authors("max42", "gritukan")
    def test_disabled_live_preview(self):
        create_user("robot-root")
        add_member("robot-root", "superusers")

        data = [{"foo": i} for i in range(3)]

        create("table", "//tmp/t1")
        write_table("//tmp/t1", data)

        create("table", "//tmp/t2")

        # Run operation with given params and return a tuple (live preview created, suppression alert set)
        def check_live_preview(enable_legacy_live_preview=None, authenticated_user=None, index=None, expect_suppression_alert=False):
            op = map(
                wait_for_jobs=True,
                track=False,
                command=with_breakpoint("BREAKPOINT ; cat", breakpoint_name=str(index)),
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={
                    "data_size_per_job": 1,
                    "enable_legacy_live_preview": enable_legacy_live_preview,
                },
                authenticated_user=authenticated_user,
            )

            wait_breakpoint(job_count=2, breakpoint_name=str(index))

            async_transaction_id = get(op.get_path() + "/@async_scheduler_transaction_id")
            live_preview_created = exists(op.get_path() + "/output_0", tx=async_transaction_id)
            if expect_suppression_alert:
                wait(lambda: "legacy_live_preview_suppressed" in op.get_alerts())

            op.abort()

            return live_preview_created

        combinations = [
            (None, "root", True, False),
            (True, "root", True, False),
            (False, "root", False, False),
            (None, "robot-root", False, True),
            (True, "robot-root", True, False),
            (False, "robot-root", False, False),
        ]

        for i, combination in enumerate(combinations):
            (
                enable_legacy_live_preview,
                authenticated_user,
                live_preview_created,
                expect_suppression_alert,
            ) = combination
            assert (
                check_live_preview(
                    enable_legacy_live_preview=enable_legacy_live_preview,
                    authenticated_user=authenticated_user,
                    index=i,
                    expect_suppression_alert=expect_suppression_alert,
                )
                == live_preview_created
            )

    @authors("galtsev")
    @pytest.mark.parametrize("auto_merge", [False, True])
    @pytest.mark.parametrize("operation", ["map", "ordered_map", "reduce", "map_reduce"])
    def test_live_preview_before_auto_merge(self, auto_merge, operation):
        row_count = 10
        in_ = "//tmp/t_in"
        out = "//tmp/t_out"
        create_test_tables(row_count)

        spec = {
            "data_size_per_job": 1,
            "pivot_keys": [[str(i)] for i in range(2, row_count, 3)],
            "reducer": {
                "format": "json",
            },
        }
        if auto_merge:
            spec["auto_merge"] = {"mode": "relaxed"}

        if operation in ("map", "ordered_map"):
            ordered = operation == "ordered_map"

            op = map(
                in_=in_,
                out=out,
                command=with_breakpoint("BREAKPOINT; cat"),
                ordered=ordered,
                spec=spec,
                track=False,
            )
        elif operation == "reduce":
            sort(in_=in_, out=in_, sort_by="x")

            op = reduce(
                in_=in_,
                out=out,
                command=with_breakpoint("BREAKPOINT; head -n1"),
                reduce_by=["x"],
                spec=spec,
                track=False,
            )
        elif operation == "map_reduce":
            op = map_reduce(
                in_=in_,
                out=out,
                reducer_command=with_breakpoint("BREAKPOINT; head -n1"),
                reduce_by=["x"],
                sort_by=["x"],
                spec=spec,
                track=False,
            )
        else:
            assert False, f"Unknown operation {operation}"

        complete_jobs = 2
        jobs = wait_breakpoint(job_count=complete_jobs)
        for job_id in jobs[:complete_jobs]:
            release_breakpoint(job_id=job_id)

        operation_path = op.get_path()
        controller_orchid = f"{operation_path}/controller_orchid"
        wait(lambda: exists(controller_orchid))

        live_preview_table = f"{controller_orchid}/live_previews/output_0"
        wait(lambda: len(read_table(live_preview_table)) == complete_jobs)

        release_breakpoint()
        op.track()

    @authors("galtsev")
    def test_new_live_preview_is_disabled_for_dynamic_tables(self):
        schema = make_schema(
            [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "int64"},
            ],
            unique_keys=True,
            strict=True,
        )

        data = [{"key": i, "value": i} for i in range(3)]

        create("table", "//tmp/t1", attributes={"schema": schema})
        write_table("//tmp/t1", data)

        create("table", "//tmp/t2", attributes={"dynamic": True, "schema": schema})
        sync_mount_table("//tmp/t2")

        op = map(
            wait_for_jobs=True,
            track=False,
            command=with_breakpoint("BREAKPOINT ; cat"),
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "allow_output_dynamic_tables": True,
                "data_size_per_job": 1,
            },
        )

        jobs = wait_breakpoint(job_count=3)

        assert exists(op.get_path() + "/controller_orchid")

        release_breakpoint(job_id=jobs[0])
        release_breakpoint(job_id=jobs[1])
        wait(lambda: op.get_job_count("completed") == 2)

        live_preview_path = op.get_path() + "/controller_orchid/live_previews"
        assert ls(live_preview_path) == []

        release_breakpoint(job_id=jobs[2])
        op.track()


@pytest.mark.enabled_multidaemon
class TestNewLivePreviewMulticell(TestNewLivePreview):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2
