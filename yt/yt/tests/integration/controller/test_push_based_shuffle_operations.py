from yt_env_setup import YTEnvSetup, Restarter, CONTROLLER_AGENTS_SERVICE, NODES_SERVICE

from yt_commands import (
    abandon_job, abort_job, authors, create, create_account, create_domestic_medium, create_tmpdir, get, ls,
    map_reduce, raises_yt_error, read_table, release_breakpoint, set_account_disk_space_limit, set_nodes_banned,
    sort, wait, wait_breakpoint, with_breakpoint, write_table)

from yt_type_helpers import make_schema

import yt.yson as yson

import os
import time

import pytest


##################################################################


class TestPushBasedShuffleBase(YTEnvSetup):
    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "testing_options": {
                "enable_snapshot_cycle_after_materialization": False,
            },
        },
    }

    @staticmethod
    def _sorted_by_key(rows):
        return sorted(rows, key=lambda row: row["key"])


class TestPushBasedShuffleKeyValueBase(TestPushBasedShuffleBase):
    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "sort_operation_options": {
                "min_partition_size": 1,
                "min_uncompressed_block_size": 1,
            },
            "map_reduce_operation_options": {
                "min_partition_size": 1,
                "min_uncompressed_block_size": 1,
            },
        },
    }

    SCHEMA = make_schema(
        [
            {"name": "key", "type": "string", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ],
        strict=True,
        unique_keys=False,
    )

    UNSORTED_SCHEMA = make_schema(
        [
            {"name": "key", "type": "string"},
            {"name": "value", "type": "string"},
        ],
        strict=True,
        unique_keys=False,
    )


##################################################################


class TestPushBasedShuffleValidation(TestPushBasedShuffleBase):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    SCHEMA = make_schema(
        [{"name": "key", "type": "string", "sort_order": "ascending"}],
        strict=True,
        unique_keys=False,
    )

    UNSORTED_SCHEMA = make_schema(
        [{"name": "key", "type": "string"}],
        strict=True,
        unique_keys=False,
    )

    ROWS = [{"key": "a"}]

    def _create_tables(self, in_schema=SCHEMA, out_schema=SCHEMA, rows=None, chunks=None):
        def attributes(schema):
            attributes = {"replication_factor": 1}
            if schema is not None:
                attributes["schema"] = schema
            return attributes

        create("table", "//tmp/t_in", force=True, attributes=attributes(in_schema))
        create("table", "//tmp/t_out", force=True, attributes=attributes(out_schema))
        if chunks is not None:
            for chunk in chunks:
                write_table("<append=%true>//tmp/t_in", chunk)
        else:
            write_table("//tmp/t_in", rows if rows is not None else self.ROWS)

    @staticmethod
    def _get_task_names(op):
        return [task["task_name"] for task in get(op.get_path() + "/@progress/tasks")]

    def _run_map_reduce(self, mapper_command=None, in_schema=SCHEMA, out_schema=SCHEMA, rows=None, chunks=None, **spec):
        self._create_tables(in_schema=in_schema, out_schema=out_schema, rows=rows, chunks=chunks)
        spec.setdefault("use_push_based_shuffle", True)
        kwargs = {}
        if mapper_command is not None:
            kwargs["mapper_command"] = mapper_command
        return map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by=["key"],
            reducer_command="cat",
            spec={key: value for key, value in spec.items() if value is not None},
            **kwargs,
        )

    def _run_sort(self, in_schema=SCHEMA, out_schema=SCHEMA, rows=None, chunks=None, **spec):
        self._create_tables(in_schema=in_schema, out_schema=out_schema, rows=rows, chunks=chunks)
        spec.setdefault("use_push_based_shuffle", True)
        spec.setdefault("pivot_keys", [["b"]])
        return sort(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by=["key"],
            spec={key: value for key, value in spec.items() if value is not None},
        )

    @authors("apollo1321")
    def test_small_partition_is_sorted_in_single_job(self):
        rows = [{"key": key} for key in ["a", "c", "b"]]

        op = self._run_map_reduce(rows=rows, in_schema=self.UNSORTED_SCHEMA)
        assert self._sorted_by_key(read_table("//tmp/t_out")) == self._sorted_by_key(rows)
        task_names = self._get_task_names(op)
        assert "partition_reduce" in task_names
        assert "intermediate_sort" not in task_names
        assert "sorted_reduce" not in task_names

        op = self._run_sort(rows=rows, in_schema=self.UNSORTED_SCHEMA, pivot_keys=[["b"]])
        assert read_table("//tmp/t_out") == self._sorted_by_key(rows)
        task_names = self._get_task_names(op)
        assert "final_sort" in task_names
        assert "intermediate_sort" not in task_names
        assert "sorted_merge" not in task_names

    @authors("apollo1321")
    def test_equal_keys_are_sorted_in_single_job(self):
        rows = [{"key": "a"} for _ in range(10)]

        op = self._run_map_reduce(rows=rows, in_schema=self.UNSORTED_SCHEMA, partition_count=1)
        assert read_table("//tmp/t_out") == rows
        task_names = self._get_task_names(op)
        assert "partition_reduce" in task_names
        assert "intermediate_sort" not in task_names
        assert "sorted_reduce" not in task_names

    @authors("apollo1321")
    @pytest.mark.parametrize("use_push_based_shuffle", [False, True])
    def test_maniac_partition_is_sorted_only_with_push_based_shuffle(self, use_push_based_shuffle):
        chunks = [[{"key": "a"}, {"key": "b"}] for _ in range(10)]
        rows = [row for chunk in chunks for row in chunk]

        op = self._run_sort(
            chunks=chunks,
            in_schema=self.UNSORTED_SCHEMA,
            use_push_based_shuffle=use_push_based_shuffle,
            pivot_keys=None,
            partition_count=10,
            partition_job_count=2,
            data_weight_per_sort_job=1)
        assert read_table("//tmp/t_out") == self._sorted_by_key(rows)
        task_names = self._get_task_names(op)
        assert ("unordered_merge" in task_names) != use_push_based_shuffle

    @authors("apollo1321")
    def test_large_partition_is_sorted_and_merged(self):
        chunks = [[{"key": "c"}, {"key": "a"}], [{"key": "e"}, {"key": "b"}, {"key": "d"}]]
        rows = [row for chunk in chunks for row in chunk]

        op = self._run_map_reduce(
            chunks=chunks,
            in_schema=self.UNSORTED_SCHEMA,
            data_weight_per_sort_job=1,
            partition_count=1,
            map_job_count=2)
        assert self._sorted_by_key(read_table("//tmp/t_out")) == self._sorted_by_key(rows)
        task_names = self._get_task_names(op)
        assert "intermediate_sort" in task_names
        assert "sorted_reduce" in task_names
        assert "partition_reduce" not in task_names

        op = self._run_sort(
            chunks=chunks,
            in_schema=self.UNSORTED_SCHEMA,
            data_weight_per_sort_job=1,
            partition_count=2,
            partition_job_count=2,
            pivot_keys=[])
        assert read_table("//tmp/t_out") == self._sorted_by_key(rows)
        task_names = self._get_task_names(op)
        assert "intermediate_sort" in task_names
        assert "sorted_merge" in task_names

    @authors("apollo1321")
    def test_value_columns_are_preserved_by_sorted_merge(self):
        schema = make_schema(
            [
                {"name": "key", "type": "string"},
                {"name": "value", "type": "int64"},
            ],
            strict=True,
            unique_keys=False,
        )
        chunks = [
            [{"key": "c", "value": 3}, {"key": "a", "value": 1}],
            [{"key": "e", "value": 5}, {"key": "b", "value": 2}, {"key": "d", "value": 4}],
        ]
        rows = sorted(
            (row for chunk in chunks for row in chunk),
            key=lambda row: row["key"])

        self._run_map_reduce(
            chunks=chunks,
            in_schema=schema,
            out_schema=None,
            data_weight_per_sort_job=1,
            partition_count=1,
            map_job_count=2)
        assert self._sorted_by_key(read_table("//tmp/t_out")) == rows

        self._run_sort(
            chunks=chunks,
            in_schema=schema,
            out_schema=None,
            data_weight_per_sort_job=1,
            partition_count=2,
            partition_job_count=2,
            pivot_keys=[])
        assert read_table("//tmp/t_out") == rows

    @authors("apollo1321")
    def test_input_column_order_differs_from_sort_order(self):
        schema = make_schema(
            [
                {"name": "value", "type": "int64"},
                {"name": "key", "type": "string"},
            ],
            strict=True,
            unique_keys=False,
        )
        rows = [{"value": index, "key": key} for index, key in enumerate("edcba")]

        self._run_map_reduce(rows=rows, in_schema=schema, out_schema=None, partition_count=3)
        assert self._sorted_by_key(read_table("//tmp/t_out")) == self._sorted_by_key(rows)

    @authors("apollo1321")
    def test_descending_sort_order(self):
        descending_schema = make_schema(
            [{"name": "key", "type": "string", "sort_order": "descending"}],
            strict=True,
            unique_keys=False,
        )
        rows = [{"key": key} for key in ["a", "c", "b", "e", "d"]]
        self._create_tables(
            in_schema=self.UNSORTED_SCHEMA,
            out_schema=descending_schema,
            rows=rows)

        sort(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by=[{"name": "key", "sort_order": "descending"}],
            spec={
                "use_push_based_shuffle": True,
                "partition_count": 2,
                "data_weight_per_sort_job": 1,
            },
        )

        assert read_table("//tmp/t_out") == self._sorted_by_key(rows)[::-1]

    @authors("apollo1321")
    @pytest.mark.parametrize("operation", ["map_reduce", "sort"])
    def test_single_row(self, operation):
        getattr(self, f"_run_{operation}")()
        assert read_table("//tmp/t_out") == [{"key": "a"}]

    @authors("apollo1321")
    def test_simple_sort_ignores_push_based_shuffle(self):
        self._run_sort(in_schema=None, out_schema=None, pivot_keys=[])
        assert read_table("//tmp/t_out") == [{"key": "a"}]

    @authors("apollo1321")
    def test_intermediate_stream_schema_must_be_strict(self):
        with raises_yt_error("requires a strict intermediate stream schema"):
            self._run_map_reduce(in_schema=None)
        with raises_yt_error("requires a strict intermediate stream schema"):
            self._run_sort(in_schema=None, out_schema=None)

    @authors("apollo1321")
    @pytest.mark.parametrize(
        "option,value",
        [
            ("enable_partitioned_data_balancing", True),
            ("enable_final_partitions_merging", True),
            ("intermediate_direct_upload_node_count", 1),
            ("enable_table_index_if_has_trivial_mapper", True),
            ("input_query", "key"),
            ("disable_sorted_input_in_reducer", True),
            ("probing_ratio", 1),
        ],
    )
    def test_unsupported_option(self, option, value):
        with raises_yt_error(f"\"{option}\""):
            self._run_map_reduce(**{option: value})

    @authors("apollo1321")
    @pytest.mark.parametrize(
        "spec",
        [
            {"intermediate_data_replication_factor": 1, "min_intermediate_data_replication_factor": 2},
            {"max_partition_factor": 2, "partition_count": 4},
        ],
    )
    def test_accepted_option(self, spec):
        self._run_map_reduce(**spec)

    @authors("apollo1321")
    @pytest.mark.parametrize(
        "spec,error",
        [
            ({"reduce_combiner": {"command": "cat"}}, "Reduce combiners are not supported by push-based shuffle"),
            (
                {"reducer": {"command": "cat", "enable_input_table_index": True}},
                "control attribute is not supported by push-based shuffle",
            ),
            (
                {"map_job_io": {"table_writer": {"max_buffer_size": 1024 * 1024}}},
                "Partition job writer buffer is too small for push-based shuffle",
            ),
        ],
    )
    def test_rejected_spec(self, spec, error):
        with raises_yt_error(error):
            self._run_map_reduce(**spec)

    @authors("apollo1321")
    def test_nontrivial_mapper_stream_requirements(self):
        stream_schema = [{"name": "key", "type": "string", "sort_order": "ascending"}]

        with raises_yt_error("mapper to declare exactly one output stream"):
            self._run_map_reduce(mapper_command="cat")

        with raises_yt_error("requires a strict mapper output stream schema"):
            self._run_map_reduce(
                mapper_command="cat",
                mapper={
                    "output_streams": [
                        {"schema": make_schema(stream_schema, strict=False)},
                    ],
                },
            )

        self._run_map_reduce(
            mapper_command="cat",
            mapper={
                "output_streams": [{"schema": stream_schema}],
            },
        )

    @authors("apollo1321")
    def test_empty_mapper_output(self):
        self._run_map_reduce(
            mapper_command="cat > /dev/null",
            mapper={
                "output_streams": [{"schema": self.SCHEMA}],
            },
        )
        assert read_table("//tmp/t_out") == []


##################################################################


class TestPushBasedShuffleStreaming(TestPushBasedShuffleKeyValueBase):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    def _create_tables(self, chunks):
        create("table", "//tmp/t_in", force=True, attributes={
            "replication_factor": 1,
            "schema": self.UNSORTED_SCHEMA,
        })
        create("table", "//tmp/t_out", force=True, attributes={
            "replication_factor": 1,
            "schema": self.UNSORTED_SCHEMA,
        })
        for chunk in chunks:
            write_table("<append=%true>//tmp/t_in", chunk)

    @staticmethod
    def _make_rows():
        return [
            {"key": "%05d" % index, "value": "x" * 64 * 1024}
            for index in range(200)
        ]

    def _start_map_reduce(self, rows, mapper_command, map_job_count=2, mapper_format=None, **spec):
        self._create_tables(chunks=[rows[index::2] for index in range(2)])
        mapper = {"output_streams": [{"schema": self.SCHEMA}]}
        if mapper_format is not None:
            mapper["format"] = mapper_format
        return map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by=["key"],
            reducer_command="cat",
            mapper_command=with_breakpoint(mapper_command),
            spec={
                "use_push_based_shuffle": True,
                "partition_count": 2,
                "map_job_count": map_job_count,
                "data_weight_per_sort_job": 1,
                "mapper": mapper,
                **spec,
            },
            track=False,
        )

    @staticmethod
    def _get_intermediate_sort_job_counter(op):
        for task in get(op.get_path() + "/@progress/tasks", default=[]):
            if task["task_name"] == "intermediate_sort":
                return task["job_counter"]
        return None

    def _wait_for_intermediate_sort_job(self, op):
        def intermediate_sort_job_completed():
            job_counter = self._get_intermediate_sort_job_counter(op)
            return job_counter is not None and job_counter["completed"]["total"] > 0

        wait(intermediate_sort_job_completed, timeout=60)

    @authors("apollo1321")
    def test_intermediate_sort_starts_before_mappers_finish(self):
        rows = self._make_rows()
        op = self._start_map_reduce(rows, "cat; BREAKPOINT")

        wait_breakpoint(job_count=2)
        self._wait_for_intermediate_sort_job(op)

        release_breakpoint()
        op.track()

        assert self._sorted_by_key(read_table("//tmp/t_out")) == self._sorted_by_key(rows)

    @authors("apollo1321")
    def test_sort_jobs_streamed_into_dispatched_partition_are_scheduled(self):
        rows = self._make_rows()
        second_batch_command = with_breakpoint(
            "tail -n +101 input; BREAKPOINT",
            breakpoint_name="second_batch")
        op = self._start_map_reduce(
            rows,
            "cat > input; head -n 100 input; BREAKPOINT; " + second_batch_command,
            map_job_count=1,
            mapper_format="dsv")

        wait_breakpoint()

        def first_batch_sorted():
            job_counter = self._get_intermediate_sort_job_counter(op)
            return (
                job_counter is not None and
                job_counter["completed"]["total"] > 0 and
                job_counter["running"] == 0 and
                job_counter["pending"] == 0
            )

        wait(first_batch_sorted, timeout=30)
        first_batch_job_count = self._get_intermediate_sort_job_counter(op)["completed"]["total"]

        release_breakpoint()
        wait_breakpoint(breakpoint_name="second_batch")
        wait(
            lambda: self._get_intermediate_sort_job_counter(op)["completed"]["total"] > first_batch_job_count,
            timeout=30)

        release_breakpoint(breakpoint_name="second_batch")
        op.track()

        assert self._sorted_by_key(read_table("//tmp/t_out")) == self._sorted_by_key(rows)

    @authors("apollo1321")
    def test_aborted_mapper_rows_are_not_visible(self):
        rows = self._make_rows()
        op = self._start_map_reduce(rows, "cat; BREAKPOINT")

        job_ids = wait_breakpoint(job_count=2)
        self._wait_for_intermediate_sort_job(op)

        abort_job(job_ids[0])
        release_breakpoint()
        op.track()

        assert self._sorted_by_key(read_table("//tmp/t_out")) == self._sorted_by_key(rows)

    @authors("apollo1321")
    def test_shuffle_job_count_limit_is_checked_while_mappers_run(self):
        op = self._start_map_reduce(self._make_rows(), "cat; BREAKPOINT", max_shuffle_job_count=1)

        wait(lambda: op.get_state() == "failed", timeout=30)
        with raises_yt_error("Too many shuffle jobs"):
            op.track()

    @authors("apollo1321")
    def test_abandoned_mapper_rows_are_not_visible(self):
        rows = self._make_rows()
        input_dir = create_tmpdir("mapper_input")
        op = self._start_map_reduce(rows, "tee {}/$YT_JOB_ID; BREAKPOINT".format(input_dir))

        job_ids = wait_breakpoint(job_count=2)
        self._wait_for_intermediate_sort_job(op)

        abandon_job(job_ids[0])
        release_breakpoint()
        op.track()

        with open(os.path.join(input_dir, job_ids[0]), "rb") as input_file:
            abandoned_keys = {str(row["key"]) for row in yson.loads(input_file.read(), yson_type="list_fragment")}
        assert abandoned_keys

        expected_rows = [row for row in rows if row["key"] not in abandoned_keys]
        assert self._sorted_by_key(read_table("//tmp/t_out")) == self._sorted_by_key(expected_rows)


##################################################################


class TestPushBasedShuffleSessionFailures(TestPushBasedShuffleKeyValueBase):
    ENABLE_MULTIDAEMON = False
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    ROWS = [
        {"key": "%05d" % index, "value": "x" * 64 * 1024}
        for index in range(120)
    ]

    def _create_tables(self):
        for path in ["//tmp/t_in", "//tmp/t_out"]:
            create("table", path, force=True, attributes={
                "replication_factor": 1,
                "schema": self.UNSORTED_SCHEMA,
            })
        for index in range(3):
            write_table("<append=%true>//tmp/t_in", self.ROWS[index::3])

    def _run_map_reduce(self, with_intermediate_sort):
        return map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by=["key"],
            reducer_command="cat",
            mapper_command=with_breakpoint("cat; BREAKPOINT"),
            spec={
                "use_push_based_shuffle": True,
                "partition_count": 2,
                "map_job_count": 3,
                "data_weight_per_sort_job": 1 if with_intermediate_sort else 1024 ** 3,
                "max_failed_job_count": 10,
                "mapper": {"output_streams": [{"schema": self.SCHEMA}]},
            },
            track=False,
        )

    @staticmethod
    def _get_journal_chunk_nodes():
        nodes = set()
        for chunk in ls("//sys/chunks", attributes=["type", "stored_replicas"]):
            if chunk.attributes["type"] != "journal_chunk":
                continue
            for replica in chunk.attributes["stored_replicas"]:
                nodes.add(str(replica))
        return sorted(nodes)

    @authors("apollo1321")
    def test_unavailable_shuffle_chunk_is_awaited(self):
        self._create_tables()
        op = self._run_map_reduce(with_intermediate_sort=True)

        job_ids = wait_breakpoint(job_count=3)
        release_breakpoint(job_id=job_ids[0])

        journal_chunk_nodes = self._get_journal_chunk_nodes()
        assert journal_chunk_nodes

        set_nodes_banned(journal_chunk_nodes, True)
        time.sleep(5)
        assert op.get_state() == "running"

        set_nodes_banned(journal_chunk_nodes, False)
        release_breakpoint()
        op.track()

        assert self._sorted_by_key(read_table("//tmp/t_out")) == self._sorted_by_key(self.ROWS)

    @authors("apollo1321")
    @pytest.mark.parametrize("with_intermediate_sort", [False, True])
    def test_sessions_are_reopened_after_node_restart(self, with_intermediate_sort):
        self._create_tables()
        op = self._run_map_reduce(with_intermediate_sort)

        wait_breakpoint(job_count=3)

        with Restarter(self.Env, NODES_SERVICE):
            pass

        release_breakpoint()
        op.track()

        assert self._sorted_by_key(read_table("//tmp/t_out")) == self._sorted_by_key(self.ROWS)


##################################################################


class TestPushBasedShuffleRevival(TestPushBasedShuffleBase):
    ENABLE_MULTIDAEMON = False
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
            "snapshot_writer": {
                "upload_replication_factor": 1,
                "min_upload_replication_factor": 1,
            },
        },
    }

    @authors("apollo1321")
    def test_revival_is_rejected(self):
        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1})
        write_table("//tmp/t_in", [{"key": "a"}, {"key": "b"}])

        op = map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by=["key"],
            reducer_command="cat",
            mapper_command=with_breakpoint("cat; BREAKPOINT"),
            spec={
                "use_push_based_shuffle": True,
                "partition_count": 2,
                "mapper": {
                    "output_streams": [{
                        "schema": make_schema(
                            [{"name": "key", "type": "string", "sort_order": "ascending"}],
                            strict=True,
                            unique_keys=False),
                    }],
                },
            },
            track=False,
        )

        wait_breakpoint()
        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        release_breakpoint()

        with raises_yt_error("Cannot revive an operation that uses push-based shuffle"):
            op.track()


##################################################################


class TestPushBasedShuffleIntermediateAccountAndMedium(TestPushBasedShuffleBase):
    ENABLE_MULTIDAEMON = False
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    STORE_LOCATION_COUNT = 2

    ACCOUNT = "push_based_shuffle"
    MEDIUM = "push_based_shuffle"

    SCHEMA = make_schema(
        [{"name": "key", "type": "string", "sort_order": "ascending"}],
        strict=True,
        unique_keys=False,
    )

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        config["data_node"]["store_locations"][1]["medium_name"] = cls.MEDIUM

    @classmethod
    def on_masters_started(cls):
        create_domestic_medium(cls.MEDIUM)

    @staticmethod
    def _get_journal_chunk_requisitions():
        return [
            chunk.attributes["requisition"]
            for chunk in ls("//sys/chunks", attributes=["type", "requisition"])
            if chunk.attributes["type"] == "journal_chunk"
        ]

    @authors("apollo1321")
    def test_intermediate_account_and_medium_are_used(self):
        create_account(self.ACCOUNT)
        set_account_disk_space_limit(self.ACCOUNT, 2 ** 30, self.MEDIUM)

        for path in ["//tmp/t_in", "//tmp/t_out"]:
            create("table", path, attributes={"replication_factor": 1, "schema": self.SCHEMA})
        write_table("//tmp/t_in", [{"key": "a"}])

        op = map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by=["key"],
            reducer_command="cat",
            mapper_command=with_breakpoint("cat; BREAKPOINT"),
            spec={
                "use_push_based_shuffle": True,
                "partition_count": 2,
                "intermediate_data_account": self.ACCOUNT,
                "intermediate_data_medium": self.MEDIUM,
                "mapper": {"output_streams": [{"schema": self.SCHEMA}]},
            },
            track=False,
        )

        wait_breakpoint()
        wait(lambda: len(self._get_journal_chunk_requisitions()) >= 2)

        for requisition in self._get_journal_chunk_requisitions():
            assert [(entry["account"], entry["medium"]) for entry in requisition] == [(self.ACCOUNT, self.MEDIUM)]

        release_breakpoint()
        op.track()

        assert read_table("//tmp/t_out") == [{"key": "a"}]
