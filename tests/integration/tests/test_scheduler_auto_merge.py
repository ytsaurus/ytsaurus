import pytest
import yt.yson as yson

from yt_env_setup import YTEnvSetup, unix_only
from yt_commands import *

from time import sleep

##################################################################

class TestSchedulerAutoMerge(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 8
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_jobs_update_period": 10,
            "chunk_unstage_period": 10,
        },
    }

    DELTA_MASTER_CONFIG = {
        "object_manager": {
            "gc_sweep_period": 10
        }
    }

    # Most common way of this test to fail is to run into state when no job can be scheduled but the
    # operation is still incomplete. In this case it should fail within the timeout given below.
    @pytest.mark.timeout(240)
    def test_auto_merge_does_not_stuck(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        parameters = [
            [9, 4, 2],
            [16, 5, 3],
            [16, 3, 5], # these values seem to be invalid, but still shouldn't lead to hangup
            [100, 28, 14],
            [9, 20, 15],
            [9, 20, 3],
        ]

        for row_count, max_intermediate_chunk_count, chunk_count_per_merge_job in parameters:
            write_table("//tmp/t_in", [{"a" : i} for i in range(row_count)])

            op = map(
                dont_track=True,
                in_="//tmp/t_in",
                out="<auto_merge=%true>//tmp/t_out",
                command="cat",
                spec={
                    "auto_merge": {
                        "max_intermediate_chunk_count": max_intermediate_chunk_count,
                        "chunk_count_per_merge_job": chunk_count_per_merge_job
                    },
                    "data_size_per_job": 1
                })
            op.track()
            assert get("//tmp/t_out/@chunk_count") == \
                   (row_count - 1) // min(chunk_count_per_merge_job, max_intermediate_chunk_count) + 1
            assert get("//tmp/t_out/@row_count") == row_count

    @pytest.mark.timeout(240)
    def test_account_chunk_limit(self):
        create_account("acc")
        set("//sys/accounts/acc/@resource_limits/chunk_count", 50)

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        set("//tmp/t_in/@account", "acc")
        set("//tmp/t_out/@account", "acc")

        row_count = 300
        write_table("//tmp/t_in", [{"a" : i} for i in range(row_count)])

        op = map(
            dont_track=True,
            in_="//tmp/t_in",
            out="<auto_merge=%true>//tmp/t_out",
            command="cat",
            spec={
                "auto_merge": {
                    "max_intermediate_chunk_count": 35,
                    "chunk_count_per_merge_job": 20,
                },
                "data_size_per_job": 1
            })

        peak_chunk_count = 0
        while True:
            state = op.get_state()
            if state == "completed":
                break
            if op.get_state() == "failed":
                op.track() # this should raise an exception
            current_chunk_count = get("//sys/accounts/acc/@resource_usage/chunk_count")
            peak_chunk_count = max(peak_chunk_count, current_chunk_count)
            sleep(0.5)

        print >>sys.stderr, "peak_chunk_count =", peak_chunk_count

        assert get("//tmp/t_out/@row_count") == row_count

    def test_several_auto_merge_output_tables(self):
        create_account("acc")
        set("//sys/accounts/acc/@resource_limits/chunk_count", 35)

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")
        set("//tmp/t_in/@account", "acc")
        set("//tmp/t_out1/@account", "acc")
        set("//tmp/t_out2/@account", "acc")

        row_count = 100
        write_table("//tmp/t_in", [{"a" : i} for i in range(row_count)])

        op = map(
            dont_track=True,
            in_="//tmp/t_in",
            out=["<auto_merge=%true>//tmp/t_out1", "<auto_merge=%true>//tmp/t_out2"],
            command="read x; echo $x >&$(($x % 2 * 3 + 1))",
            format="<columns=[a]>schemaful_dsv",
            spec={
                "auto_merge": {
                    "max_intermediate_chunk_count": 20,
                    "chunk_count_per_merge_job": 15,
                },
                "mapper": {
                    "format": yson.loads("<columns=[a]>schemaful_dsv")
                },
                "data_size_per_job": 1
            })

        peak_chunk_count = 0
        while True:
            state = op.get_state()
            if state == "completed":
                break
            if op.get_state() == "failed":
                op.track() # this should raise an exception
            current_chunk_count = get("//sys/accounts/acc/@resource_usage/chunk_count")
            peak_chunk_count = max(peak_chunk_count, current_chunk_count)
            sleep(0.5)
        print >>sys.stderr, "peak_chunk_count =", peak_chunk_count

        assert get("//tmp/t_out1/@row_count") == row_count // 2
        assert get("//tmp/t_out2/@row_count") == row_count // 2

    @pytest.mark.timeout(240)
    def test_only_auto_merge_output_table(self):
        create_account("acc")
        set("//sys/accounts/acc/@resource_limits/chunk_count", 40)

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")
        set("//tmp/t_in/@account", "acc")
        set("//tmp/t_out1/@account", "acc")
        set("//tmp/t_out2/@account", "acc")

        row_count = 100
        write_table("//tmp/t_in", [{"a" : i} for i in range(row_count)])

        op = map(
            dont_track=True,
            in_="//tmp/t_in",
            out=["//tmp/t_out1", "<auto_merge=%true>//tmp/t_out2"],
            command="read x; if [[ $(($x % 10)) == 0 ]]; then echo $x >&1; else echo $x >&4; fi",
            spec={
                "auto_merge": {
                    "max_intermediate_chunk_count": 20,
                    "chunk_count_per_merge_job": 15,
                },
                "mapper": {
                    "format": yson.loads("<columns=[a]>schemaful_dsv")
                },
                "data_size_per_job": 1
            })

        peak_chunk_count = 0
        while True:
            state = op.get_state()
            if state == "completed":
                break
            if op.get_state() == "failed":
                op.track() # this should raise an exception
            current_chunk_count = get("//sys/accounts/acc/@resource_usage/chunk_count")
            peak_chunk_count = max(peak_chunk_count, current_chunk_count)
            sleep(0.5)
        print >>sys.stderr, "peak_chunk_count =", peak_chunk_count

        assert get("//tmp/t_out1/@row_count") == row_count // 10
        assert get("//tmp/t_out2/@row_count") == row_count * 9 // 10


    @pytest.mark.timeout(240)
    def test_auto_merge_with_schema_and_append(self):
        schema_in = yson.loads("<unique_keys=%false;strict=%true>[{name=a;type=int64;sort_order=ascending};{name=b;type=string}]")
        create("table", "//tmp/t_in", attributes={"schema": schema_in})

        schema_out = yson.loads("<unique_keys=%false;strict=%true>[{name=a;type=int64};{name=b;type=string}]")
        create("table", "//tmp/t_out", attributes={"schema": schema_out})

        data = [{"a": i, "b" : str(i * i)} for i in range(10)];
        write_table("<append=%true>//tmp/t_in", data)

        init_content = [{"a": -1, "b": "1"}]
        write_table("<append=%true>//tmp/t_out", init_content)

        op = map(
            dont_track=True,
            in_="//tmp/t_in",
            out=["<auto_merge=%true; append=%true>//tmp/t_out"],
            command="cat",
            spec={
                "auto_merge": {
                    "max_intermediate_chunk_count": 4,
                    "chunk_count_per_merge_job": 2,
                },
                "data_size_per_job": 1
            })
        op.track()

        assert get("//tmp/t_out/@row_count") == 11
        assert get("//tmp/t_out/@chunk_count") == 6
        content = read_table("//tmp/t_out")
        assert sorted(content[1:]) == sorted(data)
        assert content[:1] == init_content
        assert get("//tmp/t_out/@schema") == schema_out
