from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait, wait_breakpoint, release_breakpoint, with_breakpoint,
    create, exists, get, read_table, write_table,
    merge, map, reduce, join_reduce, map_reduce, sorted_dicts)

import pytest


@pytest.mark.enabled_multidaemon
class TestJobSplitter(YTEnvSetup):
    ENABLE_MULTIDAEMON = True

    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_allocations_update_period": 10,
        }
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 5,
                "cpu": 5,
                "memory": 5 * 1024 ** 3,
            }
        },
    }

    JOB_SPLITTER_CONFIG = {
        "min_job_time": 5000,
        "min_total_data_weight": 1024,
        "update_period": 100,
        "candidate_percentile": 0.8,
        "max_jobs_per_split": 3,
        "job_logging_period": 0,
        "max_input_table_count": 10,
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 10,
            "operation_options": {
                "spec_template": {
                    "use_new_sorted_pool": False,
                    "foreign_table_lookup_keys_threshold": 1000,
                },
            },
            "map_operation_options": {
                "job_splitter": JOB_SPLITTER_CONFIG,
            },
            "reduce_operation_options": {
                "job_splitter": JOB_SPLITTER_CONFIG,
            },
            "join_reduce_operation_options": {
                "job_splitter": JOB_SPLITTER_CONFIG,
            },
            "map_reduce_operation_options": {
                "job_splitter": JOB_SPLITTER_CONFIG,
            },
            "sorted_merge_operation_options": {
                "job_splitter": JOB_SPLITTER_CONFIG,
            },
            "ordered_merge_operation_options": {
                "job_splitter": JOB_SPLITTER_CONFIG,
            },
        }
    }

    @authors("psushin")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    @pytest.mark.parametrize("use_new_sorted_pool", [False, True])
    def test_join_reduce_job_splitter(self, sort_order, use_new_sorted_pool):
        if sort_order == "descending" and not use_new_sorted_pool:
            pytest.skip("This test requires new sorted pool")

        create("table", "//tmp/in_1")
        for j in range(20):
            x = j if sort_order == "ascending" else 19 - j
            rows = [
                {
                    "key": "%08d" % (x * 4 + i),
                    "value": "(t_1)",
                    "data": "a" * (1024 * 1024),
                }
                for i in range(4)
            ]
            if sort_order == "descending":
                rows = rows[::-1]
            write_table(
                "<append=true>//tmp/in_1",
                rows,
                sorted_by=[{"name": "key", "sort_order": sort_order}],
                table_writer={
                    "block_size": 1024,
                },
            )

        create("table", "//tmp/in_2")
        for j in range(20):
            x = j if sort_order == "ascending" else 19 - j
            rows = [{"key": "(%08d)" % ((j * 10 + i) / 2), "value": "(t_2)"} for i in range(10)]
            if sort_order == "descending":
                rows = rows[::-1]
            write_table(
                "//tmp/in_2",
                rows,
                sorted_by=[{"name": "key", "sort_order": sort_order}],
                table_writer={
                    "block_size": 1024,
                },
            )

        input_ = ["<foreign=true>//tmp/in_2", "//tmp/in_1"]
        output = "//tmp/output"
        create("table", output)

        command = with_breakpoint(
            """
if [ "$YT_JOB_INDEX" == 0 ]; then
    BREAKPOINT
fi
while read ROW; do
    if [ "$YT_JOB_INDEX" == 0 ]; then
        sleep 3
    fi
    echo "$ROW"
done
"""
        )

        op = join_reduce(
            track=False,
            label="split_job",
            in_=input_,
            out=output,
            command=command,
            join_by=[{"name": "key", "sort_order": sort_order}],
            spec={
                "reducer": {
                    "format": "dsv",
                },
                "data_size_per_job": 17 * 1024 * 1024,
                "max_failed_job_count": 1,
                "job_io": {
                    "buffer_row_count": 1,
                },
                "use_new_sorted_pool": use_new_sorted_pool,
            },
        )

        wait_breakpoint(job_count=1)
        wait(lambda: op.get_job_count("completed") >= 4)
        release_breakpoint()
        op.track()

        completed = get(op.get_path() + "/@progress/jobs/completed")
        interrupted = completed["interrupted"]
        assert completed["total"] >= 6
        assert interrupted["job_split"] >= 1

    @authors("gritukan")
    def test_job_splitting(self):
        pytest.skip("Job splitting + lost jobs = no way.")
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        expected = []
        for i in range(20):
            row = {"a": str(i), "b": "x" * 10**6}
            write_table("<append=%true>//tmp/t_in", row)
            expected.append({"a": str(i)})

        slow_cat = """
while read ROW; do
    if [ "$YT_JOB_COOKIE" == 0 ]; then
        sleep 5
    else
        sleep 0.1
    fi
    echo "$ROW"
done
"""
        op = map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            mapper_command=slow_cat,
            reducer_command="cat",
            sort_by=["key"],
            spec={
                "mapper": {"format": "dsv"},
                "reducer": {"format": "dsv"},
                "data_size_per_map_job": 14 * 1024 * 1024,
                "partition_count": 2,
                "map_job_io": {
                    "buffer_row_count": 1,
                },
            })

        assert get(op.get_path() + "/controller_orchid/progress/tasks/0/task_name") == "partition_map(0)"

        path = op.get_path() + "/controller_orchid/progress/tasks/0/job_counter/completed/interrupted/job_split"
        assert get(path) == 1

        assert sorted_dicts(read_table("//tmp/t_out{a}", verbose=False)) == sorted_dicts(expected)

    @authors("gritukan")
    def test_job_speculation(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        expected = []
        for i in range(20):
            row = {"a": str(i), "b": "x" * 10**6}
            write_table("<append=%true>//tmp/t_in", row)
            expected.append({"a": str(i)})

        mapper = """
while read ROW; do
    if [ "$YT_JOB_INDEX" == 0 ]; then
        sleep 5
    else
        sleep 0.1
    fi
    echo "$ROW"
done
"""
        op = map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            mapper_command=mapper,
            reducer_command="cat",
            sort_by=["a"],
            spec={
                "mapper": {"format": "dsv"},
                "reducer": {"format": "dsv"},
                "data_size_per_map_job": 14 * 1024 * 1024,
                "partition_count": 2,
                "enable_job_splitting": False,
                "reduce_job_io": {
                    "testing_options": {"pipe_delay": 1000},
                    "buffer_row_count": 1,
                }
            })
        op.track()

        assert get(op.get_path() + "/@progress/tasks/0/task_name") == "partition_map(0)"

        path = op.get_path() + "/@progress/tasks/0/speculative_job_counter/aborted/scheduled/speculative_run_won"
        assert get(path) == 1

    @authors("klyachin")
    @pytest.mark.parametrize("ordered", [False, True])
    def test_map_job_splitter(self, ordered):
        create("table", "//tmp/in_1")
        write_table(
            "//tmp/in_1",
            [{"key": "%08d" % i, "value": "(t_1)", "data": "a" * (1024 * 1024)} for i in range(20)],
        )

        input_ = "//tmp/in_1"
        output = "//tmp/output"
        create("table", output)

        command = """
while read ROW; do
    if [ "$YT_JOB_INDEX" == 0 ]; then
        sleep 10
    else
        sleep 0.1
    fi
    echo "$ROW"
done
"""

        op = map(
            ordered=ordered,
            track=False,
            in_=input_,
            out=output,
            command=command,
            spec={
                "mapper": {
                    "format": "dsv",
                },
                "data_size_per_job": 21 * 1024 * 1024,
                "max_failed_job_count": 1,
                "job_io": {
                    "buffer_row_count": 1,
                },
            },
        )

        op.track()

        completed = get(op.get_path() + "/@progress/jobs/completed")
        interrupted = completed["interrupted"]
        assert completed["total"] >= 2
        assert interrupted["job_split"] >= 1
        expected = read_table("//tmp/in_1", verbose=False)
        for row in expected:
            del row["data"]
        got = read_table(output, verbose=False)
        for row in got:
            del row["data"]
        assert sorted_dicts(got) == sorted_dicts(expected)

    @authors("babenko")
    def test_job_splitter_max_input_table_count(self):
        create("table", "//tmp/in_1")
        write_table(
            "//tmp/in_1",
            [{"key": "%08d" % i, "value": "(t_1)", "data": "a" * (1024 * 1024)} for i in range(20)],
        )

        input_ = "//tmp/in_1"
        output = "//tmp/output"
        create("table", output)

        op = map(
            in_=[input_] * (10 + 1),
            out=output,
            command="sleep 5; echo '{a=1}'")
        op.track()

        completed = get(op.get_path() + "/@progress/jobs/completed")
        interrupted = completed["interrupted"]
        assert interrupted["job_split"] == 0

    @authors("max42")
    @pytest.mark.parametrize("mode", ["sorted", "ordered"])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    @pytest.mark.parametrize("use_new_sorted_pool", [False, True])
    def test_merge_job_splitter(self, mode, sort_order, use_new_sorted_pool):
        if sort_order == "descending" and not use_new_sorted_pool:
            pytest.skip("This test requires new sorted pool")

        create(
            "table",
            "//tmp/t_in",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": sort_order},
                    {"name": "b", "type": "string"},
                ]
            },
        )
        create("table", "//tmp/t_out")
        rows = []
        for i in range(20):
            rows.append({"a": i, "b": "x" * 10 ** 4})
        if sort_order == "descending":
            rows = rows[::-1]
        for row in rows:
            write_table("<append=%true>//tmp/t_in", row)

        op = merge(
            track=False,
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            spec={
                "force_transform": True,
                "mode": mode,
                "job_io": {
                    "testing_options": {"pipe_delay": 500},
                    "buffer_row_count": 1,
                },
                "use_new_sorted_pool": use_new_sorted_pool,
            },
        )

        wait(lambda: exists(op.get_path() + "/controller_orchid/progress/tasks/0/job_splitter"))

        op.track()

        completed = get(op.get_path() + "/@progress/jobs/completed")
        interrupted = completed["interrupted"]
        assert completed["total"] >= 2
        assert interrupted["job_split"] >= 1
        assert read_table("//tmp/t_out", verbose=False) == rows

    @authors("klyachin")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    @pytest.mark.parametrize("use_new_sorted_pool", [False, True])
    def test_reduce_job_splitter(self, sort_order, use_new_sorted_pool):
        if sort_order == "descending" and not use_new_sorted_pool:
            pytest.skip("This test requires new sorted pool")

        create("table", "//tmp/in_1")
        for j in range(5):
            x = j if sort_order == "ascending" else 4 - j
            rows = [
                {
                    "key": "%08d" % (x * 4 + i),
                    "value": "(t_1)",
                    "data": "a" * (1024 * 1024),
                }
                for i in range(4)
            ]
            if sort_order == "descending":
                rows = rows[::-1]
            write_table(
                "<append=true>//tmp/in_1",
                rows,
                sorted_by=[
                    {"name": "key", "sort_order": sort_order},
                    {"name": "value", "sort_order": sort_order},
                ],
                table_writer={
                    "block_size": 1024,
                },
            )

        create("table", "//tmp/in_2")
        rows = [{"key": "(%08d)" % (i / 2), "value": "(t_2)"} for i in range(40)]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table(
            "//tmp/in_2",
            rows,
            sorted_by=[{"name": "key", "sort_order": sort_order}],
        )

        input_ = ["<foreign=true>//tmp/in_2"] + ["//tmp/in_1"] * 5
        output = "//tmp/output"
        create("table", output)

        command = """
while read ROW; do
    if [ "$YT_JOB_INDEX" == 0 ]; then
        sleep 2
    else
        sleep 0.2
    fi
    echo "$ROW"
done
"""

        op = reduce(
            track=False,
            label="split_job",
            in_=input_,
            out=output,
            command=command,
            reduce_by=[
                {"name": "key", "sort_order": sort_order},
                {"name": "value", "sort_order": sort_order},
            ],
            join_by=[{"name": "key", "sort_order": sort_order}],
            spec={
                "reducer": {
                    "format": "dsv",
                },
                "data_size_per_job": 21 * 1024 * 1024,
                "max_failed_job_count": 1,
                "job_io": {
                    "buffer_row_count": 1,
                },
                "job_splitter": {
                    "enable_job_speculation": False,
                },
                "use_new_sorted_pool": use_new_sorted_pool,
            },
        )

        op.track()

        completed = get(op.get_path() + "/@progress/jobs/completed")
        interrupted = completed["interrupted"]
        assert completed["total"] >= 6
        assert interrupted["job_split"] >= 1
