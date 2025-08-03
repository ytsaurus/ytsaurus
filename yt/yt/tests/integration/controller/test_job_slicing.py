from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait, create, get, write_table, map, merge, raises_yt_error,
    get_table_columnar_statistics
)

from yt_type_helpers import (
    make_schema
)

import pytest
import random
import string


@pytest.mark.enabled_multidaemon
class TestCompressedDataSizePerJob(YTEnvSetup):
    ENABLE_MULTIDAEMON = True

    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

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

    MAX_COMPRESSED_DATA_SIZE_PER_JOB = 9000
    DATA_WEIGHT_PER_JOB = 700

    @staticmethod
    def _make_random_string(size) -> str:
        return ''.join(random.choice(string.ascii_letters) for _ in range(size))

    @staticmethod
    def get_completed_summary(summaries):
        result = None
        for summary in summaries:
            if summary["tags"]["job_state"] == "completed":
                assert not result
                result = summary
        assert result
        return result["summary"]

    def _setup_tables(self):
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

        self._setup_tables()

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
                "data_weight_per_job": self.DATA_WEIGHT_PER_JOB,
            } if use_data_weight else {}) | ({
                "max_compressed_data_size_per_job": self.MAX_COMPRESSED_DATA_SIZE_PER_JOB,
                # XXX(apollo1321): Remove after compressed_data_size_per_job will be supported for ordered pool.
                "compressed_data_size_per_job": 1,
            } if use_compressed_data_size else {}),
            track=False,
        )

        self._check_initial_job_estimation_and_track(op, 3 if use_compressed_data_size else 2)

        progress = get(op.get_path() + "/@progress")

        input_statistics = progress["job_statistics_v2"]["data"]["input"]

        if use_compressed_data_size:
            assert self.get_completed_summary(input_statistics["compressed_data_size"])["max"] <= self.MAX_COMPRESSED_DATA_SIZE_PER_JOB

        assert self.get_completed_summary(input_statistics["data_weight"])["max"] <= self.DATA_WEIGHT_PER_JOB
        assert progress["jobs"]["completed"]["total"] == 3 if use_compressed_data_size else 2

    @authors("apollo1321")
    @pytest.mark.skip(reason="Rewrite to compressed data size per job")
    @pytest.mark.parametrize("mode", ["ordered"])
    def test_map_operation_explicit_job_count(self, mode):
        # NB(apollo1321): Merge operation does not takes into account excplicitly set job_count.
        # NB(apollo1321): Ordered map operation provides job count guarantee only for job_count == 1.
        self._setup_tables()

        op = map(
            in_="//tmp/t_in{small}",
            out="//tmp/t_out",
            command="cat > /dev/null",
            spec={
                "suspend_operation_after_materialization": True,
                "ordered": mode == "ordered",
                "max_compressed_data_size_per_job": self.MAX_COMPRESSED_DATA_SIZE_PER_JOB,
                "data_weight_per_job": self.DATA_WEIGHT_PER_JOB,
                "job_count": 1,
            },
            track=False,
        )

        self._check_initial_job_estimation_and_track(op, 1)

        progress = get(op.get_path() + "/@progress")

        assert len(progress["tasks"]) == 1
        assert progress["tasks"][0]["task_name"] == "map" if mode == "unordered" else "ordered_map"

        # Ensure that max_compressed_data_size does not affect the explicitly set job_count.
        assert progress["jobs"]["completed"]["total"] == 1

    @authors("apollo1321")
    def test_compressed_data_size_and_explicit_job_count_unordered_map(self):
        """Ensure that compressed_data_size does not affect the explicitly set job_count."""
        # NB(apollo1321): Merge operation does not takes into account excplicitly set job_count.

        create("table", "//tmp/t_in")

        rows_batches = [
            [{"k": self._make_random_string(10)}, {"k": self._make_random_string(10)}],
            [{"k": self._make_random_string(5000)}],
            [{"k": self._make_random_string(10)}, {"k": self._make_random_string(10)}],
            [{"k": self._make_random_string(20)}],
        ]

        for rows in rows_batches:
            write_table("<append=true>//tmp/t_in", rows)

        for job_count in [1, 4, 6]:
            op = map(
                in_="//tmp/t_in{small}",
                out="<create=true>//tmp/t_out",
                command="cat > /dev/null",
                spec={
                    "suspend_operation_after_materialization": True,
                    "ordered": False,
                    "data_weight_per_job": 1,
                    "compressed_data_size_per_job": 1,
                    "job_count": job_count,
                },
                track=False,
            )

            self._check_initial_job_estimation_and_track(op, job_count)

            progress = get(op.get_path() + "/@progress")

            assert progress["jobs"]["completed"]["total"] == job_count

    @authors("apollo1321")
    @pytest.mark.parametrize("operation", ["map", "merge"])
    def test_operation_with_skewed_input_data_ordered(self, operation):
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
        | Data   |              |            | <~ max_compressed_data_size_per_job = 2200
        | Size   | _  _  ...  _ | #  #  #  # |
        +--------+--------------+------------+
        | Chunk  | 1  2  ...  10| 11 12 13 14|
        +--------+--------------+------------+
        |  Jobs  | 1| 2| ...| 10| 11   | 12  |
        +--------+--------------+------------+

        Height of a column of # defines the size of chunk.

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
                "max_compressed_data_size_per_job": 2200,
            } | ({
                "force_transform": True,
                "mode": "ordered",
            } if operation == "merge" else {}) | ({
                "ordered": True,
                "mapper": {"command": "cat > /dev/null"},
            } if operation == "map" else {}),
        )

        progress = get(op.get_path() + "/@progress")
        input_statistics = progress["job_statistics_v2"]["data"]["input"]
        assert self.get_completed_summary(input_statistics["compressed_data_size"])["max"] <= 2200
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

        Height of a column of # defines the size of chunk.

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
    @pytest.mark.parametrize("use_max_constraints", [True, False])
    def test_slice_by_compressed_data_size_and_data_weight_unordered(self, operation, use_max_constraints):
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

        Height of a column of # defines the size of chunk.
        # - 1000 bytes

        NB: Chunk indexes are just for convenience, they do not guarantee
        ordering in unordered pools.
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
                "mode": "unordered",
            } if operation == "merge" else {}) | ({
                "ordered": False,
                "mapper": {"command": "cat > /dev/null"},
            } if operation == "map" else {}),
            track=False,
        )

        self._check_initial_job_estimation_and_track(op, 5)

        progress = get(op.get_path() + "/@progress")
        assert progress["jobs"]["completed"]["total"] == 5

        input_statistics = progress["job_statistics_v2"]["data"]["input"]

        assert self.get_completed_summary(input_statistics["compressed_data_size"])["max"] <= 7000
        assert self.get_completed_summary(input_statistics["data_weight"])["max"] <= 7000

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
