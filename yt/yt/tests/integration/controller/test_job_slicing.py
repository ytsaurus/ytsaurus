from yt_env_setup import YTEnvSetup


from yt_commands import (
    authors, wait, create, get, write_table, map, merge, raises_yt_error,
    get_table_columnar_statistics, sorted_dicts, set_nodes_banned, set_node_banned,
    read_table, interrupt_job, wait_breakpoint, release_breakpoint, with_breakpoint,
)

from yt_type_helpers import (
    make_schema
)

import pytest
import random
import string
import time


class TestJobSlicingBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @staticmethod
    def _get_completed_summary(summaries):
        result = None
        for summary in summaries:
            if summary["tags"]["job_state"] == "completed":
                assert not result
                result = summary
        assert result
        return result["summary"]


@pytest.mark.enabled_multidaemon
class TestCompressedDataSizePerJob(TestJobSlicingBase):
    ENABLE_MULTIDAEMON = True

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operation_options": {
                "spec_template": {
                    "input_table_columnar_statistics": {
                        "enabled": True,
                        "mode": "from_nodes",
                    },
                },
            },
            # Ensure that data_weight_per_job will not be set from default
            # if compressed_data_size_per_job is set by user.
            "map_operation_options": {
                "data_weight_per_job": 1,
            },
            "merge_operation_options": {
                "data_weight_per_job": 1,
            },
        },
    }

    @staticmethod
    def _make_random_string(size) -> str:
        return ''.join(random.choice(string.ascii_letters) for _ in range(size))

    def _check_initial_job_estimation_and_track(self, op, expected_job_count, abs_error=0):
        wait(lambda: get(op.get_path() + "/@suspended"))
        wait(lambda: get(op.get_path() + "/@progress", default=False))
        progress = get(op.get_path() + "/@progress")
        assert progress["jobs"]["pending"] == progress["jobs"]["total"]
        # Check that job count is correctly estimated, before any job is scheduled.
        assert abs(progress["jobs"]["total"] - expected_job_count) <= abs_error
        op.resume()
        op.track()

    @authors("apollo1321")
    @pytest.mark.parametrize("operation", ["merge", "map"])
    @pytest.mark.parametrize("use_data_weight", [False, True])
    @pytest.mark.parametrize("use_compressed_data_size", [False, True])
    def test_operation_with_column_groups_ordered(self, operation, use_data_weight, use_compressed_data_size):
        if not use_data_weight and not use_compressed_data_size:
            pytest.skip()

        create("table", "//tmp/t_in", attributes={
            "schema": make_schema([
                {"name": "small", "type": "string", "group": "custom"},
                {"name": "large_1", "type": "string"},
                {"name": "large_2", "type": "string", "group": "custom"},
            ]),
            "optimize_for": "scan",
        })

        for _ in range(5):
            write_table(
                "<append=%true>//tmp/t_in",
                [
                    {
                        "small": self._make_random_string(100),
                        "large_1": self._make_random_string(8000),
                        "large_2": self._make_random_string(2000),
                    }
                    for _ in range(2)
                ]
            )

        assert get("//tmp/t_in/@chunk_count") == 5

        op_function = merge if operation == "merge" else map

        op = op_function(
            in_="//tmp/t_in{small}",
            out="<create=true>//tmp/t_out",
            spec={
                "suspend_operation_after_materialization": True,
            } | ({
                "force_transform": True,
                "mode": "ordered",
            } if operation == "merge" else {}) | ({
                "ordered": True,
                "mapper": {"command": "cat > /dev/null"},
            } if operation == "map" else {}) | ({
                "data_weight_per_job": 700,
            } if use_data_weight else {}) | ({
                "compressed_data_size_per_job": 9000,
            } if use_compressed_data_size else {}),
            track=False,
        )

        self._check_initial_job_estimation_and_track(op, 3 if use_compressed_data_size else 2)

        progress = get(op.get_path() + "/@progress")

        input_statistics = progress["job_statistics_v2"]["data"]["input"]

        if use_compressed_data_size:
            assert self._get_completed_summary(input_statistics["compressed_data_size"])["max"] <= 9000

        assert self._get_completed_summary(input_statistics["data_weight"])["max"] <= 700
        assert progress["jobs"]["completed"]["total"] == 3 if use_compressed_data_size else 2

    @authors("apollo1321")
    def test_map_operation_explicit_job_count_ordered(self):
        # NB(apollo1321): Merge operation does not takes into account excplicitly set job_count.
        # NB(apollo1321): Ordered map operation provides job count guarantee only for job_count == 1.
        create("table", "//tmp/t_in")
        for i in range(5):
            write_table(
                "<append=true>//tmp/t_in",
                [{"col1": i, "col2": self._make_random_string(1000)} for _ in range(30)]
            )

        op = map(
            in_="//tmp/t_in",
            out="<create=true>//tmp/t_out",
            command="cat > /dev/null",
            spec={
                "suspend_operation_after_materialization": True,
                "ordered": False,
                "compressed_data_size_per_job": 1,
                "data_weight_per_job": 1,
                "job_count": 1,
            },
            track=False,
        )

        self._check_initial_job_estimation_and_track(op, 1)

        progress = get(op.get_path() + "/@progress")

        assert len(progress["tasks"]) == 1

        # Ensure that max_compressed_data_size does not affect the explicitly set job_count.
        assert progress["jobs"]["completed"]["total"] == 1

    @authors("apollo1321")
    def test_compressed_data_size_and_explicit_job_count_unordered_map(self):
        """Ensure that compressed_data_size does not affect the explicitly set job_count."""
        # NB(apollo1321): Merge operation does not takes into account excplicitly set job_count.

        create("table", "//tmp/t_in", attributes={
            "schema": make_schema([
                {"name": "col1", "type": "string", "group": "custom"},
                {"name": "col2", "type": "string", "group": "custom"},
            ]),
            "optimize_for": "scan",
        })

        row_count = 8
        for i in range(row_count):
            if i % 2 == 0:
                row = {
                    "col1": "a",
                    "col2": self._make_random_string(2000),
                }
            else:
                row = {
                    "col1": "a" * 2000,
                }

            write_table("<append=%true>//tmp/t_in", row)

        for job_count in [1, 4, 6, 7, 8, 9]:
            op = map(
                in_="//tmp/t_in{col1}",
                out="<create=true>//tmp/t_out",
                command="cat > /dev/null",
                spec={
                    "suspend_operation_after_materialization": True,
                    "ordered": False,
                    "job_count": job_count,
                },
                track=False,
            )

            self._check_initial_job_estimation_and_track(op, min(job_count, row_count))

            progress = get(op.get_path() + "/@progress")

            assert progress["jobs"]["completed"]["total"] == min(job_count, row_count)

    @authors("apollo1321")
    @pytest.mark.parametrize("operation", ["map", "merge"])
    @pytest.mark.parametrize("use_max_constraints", [True, False])
    def test_operation_with_skewed_input_data_ordered(self, operation, use_max_constraints):
        """
        Test the scenario when chunk sizes are distributed like this
        (w.r.t. read size selectivity):

        +--------+--------------+------------+
        |        | #  #       # |            | <~ data_weight_per_job = 3900
        | Data   | #  #       # |            |
        | Weight | #  #       # |            |
        |        | #  #  ...  # | _  _  _  _ |
        +--------+--------------+------------+
        | Compr. |              |            |
        | Data   |              |            | <~ [max_]compressed_data_size_per_job = 2200
        | Size   | _  _  ...  _ | #  #  #  # |
        +--------+--------------+------------+
        | Chunk  | 1  2  ...  10| 11 12 13 14|
        +--------+--------------+------------+
        |  Jobs  | 1| 2| ...| 10| 11   | 12  |
        +--------+--------------+------------+

        Height of a column of # defines the size of data read from the chunk.

        # - 1000 bytes
        _ - negligible small size

        Previous tests could work without max_compressed_data_per_job support
        in chunk pools, because data_weight_per_job is updated internally
        based on max_compressed_data_per_job. However, such approach will not
        work when the size distribution of chunks is skewed.

        Job counts:
         - By max_compressed_data_size ~ ceil(5200  / 2200) = 3
         - By data_weight              ~ ceil(40000 / 3900) = 11

        Initially, jobs will be sliced by data_weight. For the last 4 chunks
        data weight is negligible and jobs will be sliced by max compressed
        data size. In the end we should get 12 jobs.

        NB: Unordered merge/map may flap in this scenario, so test them
        separetely with simpler input.
        """

        create("table", "//tmp/t_in", attributes={
            "schema": make_schema([
                {"name": "col1", "type": "string", "group": "custom"},
                {"name": "col2", "type": "string", "group": "custom"},
            ]),
            "optimize_for": "scan",
        })

        for _ in range(10):
            write_table("<append=%true>//tmp/t_in", {
                "col1": "a" * 4000
            })

        # Ensure that compression and rle took place and compressed data size is small.
        assert get("//tmp/t_in/@compressed_data_size") < 1200
        assert get("//tmp/t_in/@data_weight") >= 40000

        # We will have to read both col1 and col2 here. Data weight will be small,
        # but compressed_data_size will be large.
        for _ in range(4):
            write_table("<append=%true>//tmp/t_in", {
                "col1": "a",
                "col2": self._make_random_string(1000),
            })

        assert get("//tmp/t_in/@compressed_data_size") > 5000
        assert get("//tmp/t_in/@data_weight") <= 45000

        op_function = merge if operation == "merge" else map

        op = op_function(
            in_="//tmp/t_in{col1}",
            out="<create=true>//tmp/t_out",
            spec={
                "data_weight_per_job": 3900,
            } | ({
                "max_compressed_data_size_per_job": 2200,
            } if use_max_constraints else {
                "compressed_data_size_per_job": 2200,
            }) | ({
                "force_transform": True,
                "mode": "ordered",
            } if operation == "merge" else {}) | ({
                "ordered": True,
                "mapper": {"command": "cat > /dev/null"},
            } if operation == "map" else {}),
        )

        progress = get(op.get_path() + "/@progress")
        input_statistics = progress["job_statistics_v2"]["data"]["input"]
        assert self._get_completed_summary(input_statistics["compressed_data_size"])["max"] <= 2200
        assert progress["jobs"]["completed"]["total"] == 12

    @authors("apollo1321")
    @pytest.mark.parametrize("operation", ["map", "merge"])
    @pytest.mark.parametrize("use_max_constraints", [True, False])
    def test_operation_with_skewed_input_data_unordered(self, operation, use_max_constraints):
        """
        This test verifies that the [max_]compressed_data_size_per_job remains
        unaffected by data_weight_per_job, even implicitly via job_count. The
        distribution of chunk sizes is as follows (w.r.t. read size
        selectivity):

        +-------+---------------+
        |       | #             | <- 4000
        | Data  | #             |
        | Weight| #             |
        |       | #  _  _  _  _ | <~ data_weight_per_job = 50
        +-------+---------------+
        |       | #  #  #  #  # | <- 4000
        | Compr.| #  #  #  #  # |
        | Data  | #  #  #  #  # |
        | Size  | #  #  #  #  # |
        +-------+---------------+
        | Chunk | 1  2  3  4  5 |
        +-------+---------------+

        Height of a column of # defines the size of data read from the chunk.

        # - 1000 bytes
        _ - negligible small size

        Job counts:
         - By [max_]compressed_data_size ~ ceil(22000 / 100000) = 1
         - By data_weight                ~ ceil(4000 / 50)      = 80

        NB: User job size constraints take into account the number of rows. So
        job count by data weight will be 5 instead of 80 in map operation.

        Unordered chunk pool does not guarantee sequential ordering of input
        chunks and may process input chunks in the following orders:
         1. [B][SSSS] - 2 jobs
         2. [SB][SSS] - 2 jobs
         3. [SSB][SS] - 2 jobs
         4. [SSSB][S] - 2 jobs
         5. [SSSSB]   - 1 job

        Where:
         - "B": data_weight = 4000
         - "S": data_weight -> 0
        """

        create("table", "//tmp/t_in", attributes={
            "schema": make_schema([
                {"name": "col1", "type": "string", "group": "custom"},
                {"name": "col2", "type": "string", "group": "custom"},
            ]),
            "optimize_for": "scan",
        })

        write_table("<append=%true>//tmp/t_in", {
            "col1": "a" * 4000,
            "col2": self._make_random_string(4000),
        })

        # Ensure that compression and rle took place and compressed data size is small.
        assert get("//tmp/t_in/@compressed_data_size") < 4200
        assert get("//tmp/t_in/@data_weight") >= 8000

        for _ in range(4):
            write_table("<append=%true>//tmp/t_in", {
                "col1": "a",
                "col2": self._make_random_string(4000),
            })

        data_weight_col1 = get_table_columnar_statistics("[\"//tmp/t_in{col1}\"]")[0]["column_data_weights"]["col1"]
        assert 3900 <= data_weight_col1 <= 4100
        assert 20000 <= get("//tmp/t_in/@compressed_data_size") <= 22000

        op_function = merge if operation == "merge" else map

        op = op_function(
            in_="//tmp/t_in{col1}",
            out="<create=true>//tmp/t_out",
            spec={
                "data_weight_per_job": 50,
                "suspend_operation_after_materialization": True,
            } | ({
                "max_compressed_data_size_per_job": 100000,
            } if use_max_constraints else {
                "compressed_data_size_per_job": 100000,
            }) | ({
                "force_transform": True,
                "mode": "unordered",
            } if operation == "merge" else {}) | ({
                "ordered": False,
                "mapper": {"command": "cat > /dev/null"},
            } if operation == "map" else {}),
            track=False,
        )

        if operation == "map":
            # See test description for explanation.
            self._check_initial_job_estimation_and_track(op, 5)
        else:
            self._check_initial_job_estimation_and_track(op, 80, abs_error=3)

        progress = get(op.get_path() + "/@progress")
        assert 1 <= progress["jobs"]["completed"]["total"] <= 2

    @authors("apollo1321")
    @pytest.mark.parametrize("operation", ["map", "merge"])
    def test_operation_fails_on_max_compressed_data_size_per_job_violation(self, operation):
        create("table", "//tmp/t_in")

        write_table("<append=%true>//tmp/t_in", {
            "col": self._make_random_string(4000),
        })

        op_function = merge if operation == "merge" else map

        op = op_function(
            in_="//tmp/t_in",
            out="<create=true>//tmp/t_out",
            spec={
                "max_compressed_data_size_per_job": 3000,
            } | ({
                "force_transform": True,
            } if operation == "merge" else {}) | ({
                "mapper": {"command": "cat > /dev/null"},
            } if operation == "map" else {}),
            track=False,
        )

        with raises_yt_error("Maximum allowed compressed data size per job exceeds the limit"):
            op.track()

    @authors("apollo1321")
    @pytest.mark.parametrize("operation", ["map", "merge"])
    @pytest.mark.parametrize("mode", ["ordered", "unordered"])
    @pytest.mark.parametrize("use_max_constraints", [True, False])
    def test_slice_by_compressed_data_size_and_data_weight_alternately(self, operation, mode, use_max_constraints):
        """
        Test that input can be sliced by both [max_]compressed_data_size_per_job
        and [max_]data_weight_per_job. The distribution of chunk sizes is as
        follows (w.r.t. read size selectivity):

        +-------+---------------+
        |       | #     #     # | <- 4000 bytes
        | Data  | #     #     # | <~ compressed_data_size_per_job = 3500 bytes
        | Weight| #     #     # |
        |       | #  #  #  #  # | <- 1000 bytes
        +-------+---------------+
        |       |    #     #    | <- 4000 bytes
        | Compr.|    #     #    | <~ data_weight_per_job = 3500 bytes
        | Data  |    #     #    |
        | Size  | #  #  #  #  # | <- 1000 bytes
        +-------+---------------+
        | Chunk | 1  2  3  4  5 |
        +-------+---------------+

        Height of a column of # defines the size of data read from the chunk.
        # - 1000 bytes

        NB: In unordered mode chunk indexes are just for convenience, they do
        not guarantee ordering in unordered pools.
        """

        create("table", "//tmp/t_in", attributes={
            "schema": make_schema([
                {"name": "col1", "type": "string", "group": "custom"},
                {"name": "col2", "type": "string", "group": "custom"},
            ]),
            "optimize_for": "scan",
        })

        for i in range(5):
            if i % 2 == 0:
                write_table("<append=%true>//tmp/t_in", {
                    "col1": "a" * 4000,
                    "col2": self._make_random_string(900),
                })
            else:
                write_table("<append=%true>//tmp/t_in", {
                    "col1": "a" * 1000,
                    "col2": self._make_random_string(3900),
                })

        data_weight_col1 = get_table_columnar_statistics("[\"//tmp/t_in{col1}\"]")[0]["column_data_weights"]["col1"]
        assert 13700 <= data_weight_col1 <= 14300
        assert 11000 <= get("//tmp/t_in/@compressed_data_size") <= 12000

        op_function = merge if operation == "merge" else map

        op = op_function(
            in_="//tmp/t_in{col1}",
            out="<create=true>//tmp/t_out",
            spec={
                "suspend_operation_after_materialization": True,
            } | ({
                "data_weight_per_job": 7000,
                "max_compressed_data_size_per_job": 7000,
                "max_data_weight_per_job": 7000,
            } if use_max_constraints else {
                "data_weight_per_job": 3500,
                "compressed_data_size_per_job": 3500,
            }) | ({
                "force_transform": True,
                "mode": mode,
            } if operation == "merge" else {}) | ({
                "ordered": mode == "ordered",
                "mapper": {"command": "cat > /dev/null"},
            } if operation == "map" else {}),
            track=False,
        )

        self._check_initial_job_estimation_and_track(op, 5)

        progress = get(op.get_path() + "/@progress")
        assert progress["jobs"]["completed"]["total"] == 5

        input_statistics = progress["job_statistics_v2"]["data"]["input"]

        assert self._get_completed_summary(input_statistics["compressed_data_size"])["max"] <= 7000
        assert self._get_completed_summary(input_statistics["data_weight"])["max"] <= 7000

    @authors("apollo1321")
    @pytest.mark.parametrize("operation", ["map", "merge"])
    def test_slice_by_compressed_data_size_unordered(self, operation):
        """
        Test that input can be sliced only by compressed_data_size. The
        distribution of chunk sizes is as follows (w.r.t. read size
        selectivity):

        +-------+------------+
        |       | #  #       | <- 40000 bytes
        | Compr.| #  #       | <- compressed_data_size_per_job = 30000 bytes
        | Data  | #  #  #  # | <- 20000 bytes
        | Size  | #  #  #  # |
        +-------+------------+
        | Chunk | 1  2  3  4 |
        +-------+------------+

        Unordered chunk pool does not guarantee sequential ordering of input
        chunks and may process input chunks in the following orders:
         1. [B][B][SS] - 3 jobs
         2. [B][SB][S] - 3 jobs
         3. [B][SS][B] - 3 jobs
         4. [SB][B][S] - 3 jobs
         5. [SB][SB]   - 2 jobs
         6. [SS][B][B] - 3 jobs

        Where:
         - "B": CS = 40000 bytes
         - "S": CS = 20000 bytes
        """

        create("table", "//tmp/t_in", attributes={
            "schema": make_schema([
                {"name": "col1", "type": "string", "group": "custom"},
                {"name": "col2", "type": "string", "group": "custom"},
            ]),
            "optimize_for": "scan",
        })

        write_table("<append=%true>//tmp/t_in", {
            "col1": "a" * 1,
            "col2": self._make_random_string(40000),
        })

        write_table("<append=%true>//tmp/t_in", {
            "col1": "a" * 50_000,
            "col2": self._make_random_string(40000),
        })

        write_table("<append=%true>//tmp/t_in", {
            "col1": "a" * 250_000,
            "col2": self._make_random_string(20000),
        })

        write_table("<append=%true>//tmp/t_in", {
            "col1": "a" * 500_000,
            "col2": self._make_random_string(20000),
        })

        assert 120000 <= get("//tmp/t_in/@compressed_data_size") <= 130000

        op_function = merge if operation == "merge" else map

        op = op_function(
            in_="//tmp/t_in{col1}",
            out="<create=true>//tmp/t_out",
            spec={
                "suspend_operation_after_materialization": True,
                "compressed_data_size_per_job": 30000,
            } | ({
                "force_transform": True,
                "mode": "unordered",
            } if operation == "merge" else {}) | ({
                "ordered": False,
                "mapper": {"command": "cat > /dev/null"},
            } if operation == "map" else {}),
            track=False,
        )

        if operation == "map":
            # NB: User job size constraints take into account the number of
            # rows. So job count will be 4 instead of 5 in map operation.
            self._check_initial_job_estimation_and_track(op, 4)
        else:
            self._check_initial_job_estimation_and_track(op, 5)

        progress = get(op.get_path() + "/@progress")
        assert 2 <= progress["jobs"]["completed"]["total"] <= 3


class TestJobSlicing(TestJobSlicingBase):
    @authors("apollo1321")
    @pytest.mark.parametrize("strict_data_weight_per_job_verification", [False, True])
    def test_invalid_data_weight_per_job_alert(self, strict_data_weight_per_job_verification):
        create("table", "//tmp/t_in")

        write_table("<append=%true>//tmp/t_in", {
            "col1": "a" * 100
        })

        op = map(
            in_="//tmp/t_in",
            out="<create=true>//tmp/t_out",
            spec={
                "max_data_weight_per_job": 1000,
                "data_weight_per_job": 100000,
                "mapper": {"command": "cat > /dev/null"},
                "strict_data_weight_per_job_verification": strict_data_weight_per_job_verification,
            },
            track=False,
        )

        if strict_data_weight_per_job_verification:
            with raises_yt_error('\"data_weight_per_job\" cannot be greater than \"max_data_weight_per_job\"'):
                op.track()
        else:
            op.track()
            assert "invalid_data_weight_per_job" in op.get_alerts()

    @authors("apollo1321")
    def test_unordered_map_with_job_interruptions(self):
        # The purpose of this test is to check that

        create("table", "//tmp/t_in", attributes={
            "schema": make_schema([
                {"name": "col1", "type": "string"},
            ]),
            "optimize_for": "scan",
        })

        chunk_count = 10

        for _ in range(chunk_count):
            write_table(
                "<append=true>//tmp/t_in",
                [{"col1": "a"}],
                table_writer={"block_size": 1}
            )

        user_slots = self.NUM_NODES

        command = with_breakpoint(f"""
if [ "$YT_JOB_INDEX" -lt {user_slots} ]; then
    i=0
    while read ROW && [ "$i" -lt 100 ]; do
        ((i++))
        echo "$ROW"
    done
    BREAKPOINT
    while read ROW; do
        echo "$ROW"
    done
else
    cat
fi
""")
        op = map(
            track=False,
            in_="//tmp/t_in{col1}",
            out="<create=true>//tmp/t_out",
            command=command,
            spec={
                "mapper": {
                    "format": "dsv",
                },
                "max_failed_job_count": 1,
                "job_io": {
                    "buffer_row_count": 1,
                    "pipe_capacity": 1,
                },
                "max_compressed_data_size_per_job": int(get("//tmp/t_in/@compressed_data_size") / (chunk_count * 0.8)),
                "max_job_count": 1,
                "resource_limits": {"user_slots": user_slots},
            },
        )

        jobs = wait_breakpoint(job_count=user_slots)
        for job_id in jobs:
            interrupt_job(job_id, interrupt_timeout=10000)

        release_breakpoint()

        op.track()

        progress = get(op.get_path() + "/@progress")
        actual_compressed_data_size = self._get_completed_summary(progress["job_statistics_v2"]["data"]["input"]["compressed_data_size"])["sum"]
        estimated_compressed_data_size = progress["estimated_input_statistics"]["compressed_data_size"]

        # Actual size may be up to 2x estimated size due to job restarts.
        assert estimated_compressed_data_size * 0.9 <= actual_compressed_data_size <= estimated_compressed_data_size * 2.1


@pytest.mark.enabled_multidaemon
class TestJobSlicingWithLostInput(TestJobSlicingBase):
    ENABLE_MULTIDAEMON = True

    NUM_NODES = 5

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 5,
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operations_update_period": 5,
            "running_allocations_update_period": 5,
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

    @authors("apollo1321")
    @pytest.mark.parametrize("use_new_slicing_implementation", [False, True])
    def test_lost_input_chunks_with_explicit_job_count(self, use_new_slicing_implementation):
        create("table", "//tmp/t_in", attributes={
            "schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
            "replication_factor": 1,
        })

        data = []
        chunk_count = 30
        for i in range(chunk_count):
            row = [
                {"key": i, "value": str(i) * 1000},
            ]
            data += row
            write_table("<append=true>//tmp/t_in", row)

        assert len(get("//tmp/t_in/@chunk_ids")) == chunk_count
        all_nodes = list(get("//sys/cluster_nodes"))
        assert len(all_nodes) == self.NUM_NODES
        alive_node = all_nodes[0]
        job_count = 10
        data_weight_per_job = (get("//tmp/t_in/@data_weight") + job_count - 1) // job_count

        op = map(
            track=False,
            command="cat",
            in_="//tmp/t_in",
            out="<create=%true>//tmp/t_out",
            spec={
                "scheduling_tag_filter": alive_node,
                "suspend_operation_after_materialization": True,
                "resource_limits": {"user_slots": 4},
                "job_count": job_count,
                "job_io": {
                    "table_reader": {
                        # Fail fast
                        "retry_count": 1,
                    }
                },
                "use_new_slicing_implementation_in_unordered_pool": use_new_slicing_implementation,
            }
        )

        wait(lambda: get(op.get_path() + "/@suspended"))
        set_nodes_banned(all_nodes[1:], True)

        # Chunk pool output cookies will be extracted after resume.
        op.resume()

        wait(lambda: op.get_job_count("aborted") > 0)
        wait(lambda: op.get_job_count("suspended") > 0)

        for i in range(1, self.NUM_NODES):
            set_node_banned(all_nodes[i], False)
            time.sleep(1)

        op.track()

        assert sorted_dicts(read_table("//tmp/t_out")) == sorted_dicts(data)

        progress = get(op.get_path() + "/@progress")
        assert progress["jobs"]["completed"]["total"] == job_count

        if use_new_slicing_implementation:
            # Check that data weight is distributed evenly.
            assert self._get_completed_summary(progress["job_statistics_v2"]["data"]["input"]["data_weight"])["max"] <= data_weight_per_job * 1.5
