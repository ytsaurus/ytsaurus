from functools import partial
from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    CONTROLLER_AGENTS_SERVICE,
    NODES_SERVICE
)

from yt_commands import (
    authors,
    copy,
    create,
    create_user,
    extract_statistic_v2,
    get,
    get_job,
    exists,
    join_reduce,
    ls,
    raises_yt_error,
    read_table,
    release_breakpoint,
    set,
    start_transaction,
    update_controller_agent_config,
    update_nodes_dynamic_config,
    wait_breakpoint,
    with_breakpoint,
    write_table,
    sorted_dicts,
    get_driver,
    write_file,
    map,
    map_reduce,
    merge,
    reduce,
    sort,
    set_all_nodes_banned,
    wait,
    wait_for_nodes,
)

from yt_helpers import profiler_factory
from yt.common import YtError
from yt.common import YtResponseError
import yt.yson as yson

from textwrap import dedent
import pytest
from random import Random
import time
import datetime


##################################################################


@pytest.mark.enabled_multidaemon
class TestSchedulerRemoteOperationCommandsBase(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_TEST_PARTITIONS = 5

    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    NUM_REMOTE_CLUSTERS = 1

    NUM_MASTERS_REMOTE_0 = 1
    NUM_SCHEDULERS_REMOTE_0 = 0

    REMOTE_CLUSTER_NAME = "remote_0"

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
            "remote_copy_operation_options": {
                "spec_template": {
                    "use_remote_master_caches": True,
                },
            },
            "remote_operations": {
                "remote_0": {
                    "allowed_users": ["root"],
                },
            },
        },
    }

    @classmethod
    def setup_class(cls):
        super(TestSchedulerRemoteOperationCommandsBase, cls).setup_class()
        cls.remote_driver = get_driver(cluster=cls.REMOTE_CLUSTER_NAME)

    def to_remote_path(self, path):
        return f"<cluster={self.REMOTE_CLUSTER_NAME}>{path}"


##################################################################


class TestSchedulerRemoteOperationCommands(TestSchedulerRemoteOperationCommandsBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts.

    @authors("coteeq")
    def test_map_empty_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2")

        map(
            in_=self.to_remote_path("//tmp/t1"),
            out="//tmp/t2",
            command="cat",
        )

        assert read_table("//tmp/t2") == []
        assert not get("//tmp/t2/@sorted")

    @authors("coteeq")
    def test_map_only_remote_table(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2")

        data = [{"a": 1}, {"a": 2}]
        write_table("<append=%true>//tmp/t1", data, driver=self.remote_driver)

        map(
            in_=self.to_remote_path("//tmp/t1"),
            out="//tmp/t2",
            command="cat",
        )

        assert sorted_dicts(read_table("//tmp/t2")) == sorted_dicts(data)
        assert not get("//tmp/t2/@sorted")

    @authors("coteeq")
    def test_map_remote_and_local_table(self):
        n_chunks = 2
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t1")
        create("table", "//tmp/t_out")
        data1 = [{"a": 1}, {"a": 2}]
        data2 = [{"a": 10}, {"a": 20}]
        for _ in range(n_chunks):
            write_table("<append=%true>//tmp/t1", data1, driver=self.remote_driver)
            write_table("<append=%true>//tmp/t1", data2)

        map(
            in_=[
                self.to_remote_path("//tmp/t1"),
                "//tmp/t1",
            ],
            out="//tmp/t_out",
            command="cat",
            spec={
                "mapper": {
                    "input_format": "json",
                    "output_format": "json",
                    "enable_input_table_index": False,
                },
            },
        )

        assert sorted_dicts(read_table("//tmp/t_out")) == sorted_dicts((data1 + data2) * n_chunks)
        assert not get("//tmp/t_out/@sorted")

    def _upload_mapper_and_reducer(self):
        mapper = dedent(
            """
            import json
            import sys
            rows = []
            for row in sys.stdin:
                rows.append(json.loads(row))
                if rows[-1]['a'] is None:
                    print(row, file=sys.stderr)
                    raise RuntimeError()
            for row in rows:
                print(json.dumps({'a': row['a'] * 10}))
            """
        )

        reducer = dedent(
            """
            import json
            import sys
            rows = []
            for row in sys.stdin:
                rows.append(json.loads(row))
                if rows[-1]['a'] is None:
                    print(row, file=sys.stderr)
                    raise RuntimeError()
            if len(rows) == 0:
                raise RuntimeError()
            print(json.dumps({'a': sum(row['a'] for row in rows)}))
            """
        )

        create("file", "//tmp/mapper.py")
        create("file", "//tmp/reducer.py")
        write_file("//tmp/mapper.py", mapper.encode("ascii"))
        write_file("//tmp/reducer.py", reducer.encode("ascii"))

    @authors("coteeq")
    def test_map_remote_and_local_with_mapper(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        data1 = [{"a": 1}, {"a": 2}]
        data2 = [{"a": 10}, {"a": 20}]
        write_table("//tmp/t1", data1, driver=self.remote_driver)
        write_table("//tmp/t1", data2)

        self._upload_mapper_and_reducer()

        map(
            in_=[
                self.to_remote_path("//tmp/t1"),
                "//tmp/t1",
            ],
            out="//tmp/t2",
            mapper_file=["//tmp/mapper.py"],
            mapper_command="python3 mapper.py",
            spec={
                "mapper": {
                    "input_format": "json",
                    "output_format": "json",
                    "enable_input_table_index": False,
                },
            },
        )

        expected = [{"a": row["a"] * 10} for row in data1 + data2]
        assert sorted_dicts(read_table("//tmp/t2")) == sorted_dicts(expected)
        assert not get("//tmp/t2/@sorted")

    @authors("coteeq")
    def test_map_reduce_small_table(self):
        driver = self.remote_driver
        create("table", "//tmp/t1", driver=driver)
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": 1}, {"a": 2}], driver=driver)

        self._upload_mapper_and_reducer()

        map_reduce(
            in_=self.to_remote_path("//tmp/t1"),
            out="//tmp/t2",
            mapper_file=["//tmp/mapper.py"],
            reducer_file=["//tmp/reducer.py"],
            mapper_command="python3 mapper.py",
            reducer_command="python3 reducer.py",
            reduce_by=["a"],
            spec={
                "mapper": {
                    "input_format": "json",
                    "output_format": "json",
                },
                "reducer": {
                    "input_format": "json",
                    "output_format": "json",
                },
            }
        )

        assert read_table("//tmp/t2") == [{"a": 30}]

    @authors("coteeq")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_reduce_cat(self, sort_order):
        create("table", "//tmp/in1")
        rows = [
            {"key": 0, "value": 1},
            {"key": 2, "value": 2},
            {"key": 4, "value": 3},
            {"key": 7, "value": 4},
        ]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table(
            "//tmp/in1",
            rows,
            sorted_by=[{"name": "key", "sort_order": sort_order}],
        )

        create("table", "//tmp/in2", driver=self.remote_driver)
        rows = [
            {"key": -1, "value": 5},
            {"key": 1, "value": 6},
            {"key": 3, "value": 7},
            {"key": 5, "value": 8},
        ]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table(
            "//tmp/in2",
            rows,
            sorted_by=[{"name": "key", "sort_order": sort_order}],
            driver=self.remote_driver,
        )

        create("table", "//tmp/out")

        reduce(
            in_=["//tmp/in1", self.to_remote_path("//tmp/in2")],
            out="<sorted_by=[{{name=key;sort_order={}}}]>//tmp/out".format(sort_order),
            reduce_by=[{"name": "key", "sort_order": sort_order}],
            command="cat",
            spec={"reducer": {"format": "dsv"}},
        )

        expected = [
            {"key": "-1", "value": "5"},
            {"key": "0", "value": "1"},
            {"key": "1", "value": "6"},
            {"key": "2", "value": "2"},
            {"key": "3", "value": "7"},
            {"key": "4", "value": "3"},
            {"key": "5", "value": "8"},
            {"key": "7", "value": "4"},
        ]
        if sort_order == "descending":
            expected = expected[::-1]
        assert read_table("//tmp/out") == expected
        assert get("//tmp/out/@sorted")

    @authors("coteeq")
    @pytest.mark.timeout(30)
    def test_revive_map(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": 1}, {"a": 2}], driver=self.remote_driver)
        write_table("<append=%true>//tmp/t1", [{"a": 3}, {"a": 4}], driver=self.remote_driver)

        self._upload_mapper_and_reducer()

        op = map(
            in_=self.to_remote_path("//tmp/t1"),
            out="//tmp/t2",
            mapper_file=["//tmp/mapper.py"],
            mapper_command=with_breakpoint("BREAKPOINT; python3 mapper.py"),
            spec={
                "mapper": {"input_format": "json", "output_format": "json"},
            },
            track=False,
        )

        wait_breakpoint()

        with Restarter(self.Env, [CONTROLLER_AGENTS_SERVICE]):
            pass

        release_breakpoint()

        op.track()

        assert sorted_dicts(read_table("//tmp/t2")) == [{"a": 10}, {"a": 20}, {"a": 30}, {"a": 40}]
        assert not get("//tmp/t2/@sorted")

    @authors("coteeq")
    def test_table_reuses_chunk(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", [{"a": 1}, {"a": 2}], driver=self.remote_driver)
        copy("//tmp/t1", "//tmp/t2", driver=self.remote_driver)
        assert get("//tmp/t1/@chunk_ids", driver=self.remote_driver) == get("//tmp/t2/@chunk_ids", driver=self.remote_driver)

        create("table", "//tmp/out")

        self._upload_mapper_and_reducer()

        map(
            in_=[self.to_remote_path("//tmp/t1"), self.to_remote_path("//tmp/t2")],
            out="//tmp/out",
            mapper_file=["//tmp/mapper.py"],
            mapper_command="python3 mapper.py",
            spec={
                "mapper": {
                    "input_format": "json",
                    "output_format": "json",
                    "enable_input_table_index": False,
                },
            },
        )

        assert sorted_dicts(read_table("//tmp/out")) == sorted_dicts([{"a": 10}, {"a": 20}] * 2)

    @authors("coteeq")
    def test_sort(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2")

        data = list(range(10))
        Random(123).shuffle(data)
        remote_data = [{"value" : val} for val in data[:5]]
        local_data = [{"value" : val} for val in data[5:]]

        write_table("//tmp/t1", remote_data, driver=self.remote_driver)
        write_table("//tmp/t2", local_data)

        create("table", "//tmp/out")

        sort(
            in_=[
                self.to_remote_path("//tmp/t1"),
                "//tmp/t2",
            ],
            out="//tmp/out",
            sort_by=["value"],
        )

        assert read_table("//tmp/out") == [{"value": val} for val in list(range(10))]

    @authors("coteeq")
    def test_tricky_reduce(self):
        sorted_by = [
            {"name": "key", "sort_order": "ascending"},
            {"name": "value1", "sort_order": "ascending"},
        ]

        remote_data = [
            [
                {"key": 1, "value1":  1, "value2": 2},
                {"key": 2, "value1": 10, "value2": 2},
            ],
            [
                {"key": 3, "value1":  1, "value2": 2},
                {"key": 4, "value1": 10, "value2": 2},
            ],
            [
                {"key": 5, "value1":  1, "value2": 2},
            ],
        ]
        local_data = [
            [
                {"key": 1, "value1": 10, "value2": 20},
                {"key": 2, "value1":  1, "value2": 20},
            ],
            [
                {"key": 3, "value1": 10, "value2": 20},
                {"key": 4, "value1":  1, "value2": 20},
            ],
            [
                {"key": 5, "value1": 10, "value2": 20},
            ],
        ]

        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2")

        for datum in remote_data:
            write_table("<append=%true>//tmp/t1", datum, sorted_by=sorted_by, driver=self.remote_driver)
        for datum in local_data:
            write_table("<append=%true>//tmp/t2", datum, sorted_by=sorted_by)

        reducer = dedent(
            """
            import sys
            from sys import stdin
            from json import loads, dumps

            firsts = dict()
            for line in stdin:
                row = loads(line)
                if "$attributes" in line:
                    continue
                if row["key"] not in firsts:
                    firsts[row["key"]] = row["value2"]

            for key, first in firsts.items():
                print(dumps({"key": key, "first": first}))
            """
        )

        create("file", "//tmp/reducer.py")
        write_file("//tmp/reducer.py", reducer.encode("ascii"))

        create("table", "//tmp/out")

        reduce(
            in_=[
                self.to_remote_path("//tmp/t1"),
                "//tmp/t2",
            ],
            reducer_file=["//tmp/reducer.py"],
            reducer_command="python3 reducer.py",
            out="""<sorted_by=[{name=key; sort_order=ascending}]>//tmp/out""",
            reduce_by=["key"],
            sort_by=["key", "value1"],
            spec={
                "reducer": {"input_format": "json", "output_format": "json"},
                "data_size_per_job": 1,
            },
        )

        assert read_table("//tmp/out") == [
            {"key": 1, "first": 2},
            {"key": 2, "first": 20},
            {"key": 3, "first": 2},
            {"key": 4, "first": 20},
            {"key": 5, "first": 2},
        ]

    @authors("coteeq")
    @pytest.mark.parametrize("mode", ["unordered", "ordered", "sorted"])
    def test_merge_does_not_teleport(self, mode):
        create("table", "//tmp/t_in", driver=self.remote_driver)
        create("table", "//tmp/t_out")

        sorted_by = "<sorted_by=[{name=key; sort_order=ascending}]>"

        data = [{"key": i, "value": i + 100} for i in range(0, 10)]

        write_table(sorted_by + "//tmp/t_in", data, driver=self.remote_driver)

        merge_by = {"merge_by": ["key"]} if mode == "sorted" else {}
        merge(
            in_=[
                self.to_remote_path("//tmp/t_in"),
            ],
            out="//tmp/t_out",
            mode=mode,
            **merge_by,
        )

        if mode == "sorted":
            assert get("//tmp/t_out/@sorted")

        assert read_table("//tmp/t_out") == data

    @authors("coteeq")
    def test_merge_interleave_rows(self):
        create("table", "//tmp/t_in", driver=self.remote_driver)
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        sorted_by = "<sorted_by=[{name=key; sort_order=ascending}]>"

        data1 = [{"key": i, "value": i + 100} for i in range(0, 10, 2)]
        data2 = [{"key": i, "value": i + 100} for i in range(1, 10, 2)]

        write_table(sorted_by + "//tmp/t_in", data1, driver=self.remote_driver)
        write_table(sorted_by + "//tmp/t_in", data2)

        merge(
            in_=[
                self.to_remote_path("//tmp/t_in"),
                "//tmp/t_in"
            ],
            out="//tmp/t_out",
            mode="sorted",
            merge_by=["key"],
        )

        expected = data1 + data2
        expected.sort(key=lambda row: row["key"])

        assert get("//tmp/t_out/@sorted")
        assert read_table("//tmp/t_out") == expected

    @authors("coteeq")
    @pytest.mark.parametrize("remote_primary", [False, True])
    @pytest.mark.parametrize("remote_foreign", [False, True])
    def test_join_reduce(self, remote_primary, remote_foreign):
        def _get_driver(remote):
            if remote:
                return self.remote_driver
            else:
                return get_driver()

        create("table", "//tmp/primary", driver=_get_driver(remote_primary))
        create("table", "//tmp/foreign", driver=_get_driver(remote_foreign))
        create("table", "//tmp/out")

        primary_data = [
            {"key": "1_primary"},
            {"key": "2_primary_foreign"},
        ]

        foreign_data = [
            {"key": "2_primary_foreign"},
            {"key": "3_foreign"},
        ]

        sorted_by = "<sorted_by=[{name=key; sort_order=ascending}]>"
        sorted_by_and_append = "<sorted_by=[{name=key; sort_order=ascending}]; append=%true>"

        for row in primary_data:
            write_table(sorted_by_and_append + "//tmp/primary", [row], driver=_get_driver(remote_primary))
        for row in foreign_data:
            write_table(sorted_by_and_append + "//tmp/foreign", [row], driver=_get_driver(remote_foreign))

        primary_path = "//tmp/primary"
        foreign_path = "<foreign=%true>//tmp/foreign"
        if remote_primary:
            primary_path = self.to_remote_path(primary_path)
        if remote_foreign:
            foreign_path = f"<foreign=%true;cluster=\"{self.REMOTE_CLUSTER_NAME}\">//tmp/foreign"

        join_reduce(
            in_=[
                primary_path,
                foreign_path,
            ],
            out=sorted_by + "//tmp/out",
            reduce_by=["key"],
            join_by=["key"],
            command="cat",
            spec={
                "reducer": {"format": yson.loads(b"<line_prefix=tskv; enable_table_index=true>dsv")},
                "data_size_per_job": 1,
                "enable_key_guarantee": True,
            }
        )

        expected = [
            {"key": "1_primary", "@table_index": "0"},
            {"key": "2_primary_foreign", "@table_index": "0"},
            {"key": "2_primary_foreign", "@table_index": "1"},
        ]
        assert read_table("//tmp/out") == expected

    @authors("coteeq")
    @pytest.mark.parametrize("revive", [False, True])
    def test_with_transactions(self, revive):
        local_tx = start_transaction(timeout=30000)
        remote_tx = start_transaction(timeout=30000, driver=self.remote_driver)

        n_chunks = 2
        create("table", "//tmp/t1", driver=self.remote_driver, tx=remote_tx)
        create("table", "//tmp/t1", tx=local_tx)
        create("table", "//tmp/t_out")
        data1 = [{"a": 1}, {"a": 2}]
        data2 = [{"a": 10}, {"a": 20}]
        for _ in range(n_chunks):
            write_table("<append=%true>//tmp/t1", data1, driver=self.remote_driver, tx=remote_tx)
            write_table("<append=%true>//tmp/t1", data2, tx=local_tx)

        op = map(
            track=False,
            in_=[
                f"<transaction_id=\"{remote_tx}\";cluster=\"{self.REMOTE_CLUSTER_NAME}\">//tmp/t1",
                f"<transaction_id=\"{local_tx}\">//tmp/t1",
            ],
            out="//tmp/t_out",
            command=with_breakpoint("BREAKPOINT; cat"),
            spec={
                "mapper": {
                    "enable_input_table_index": False,
                },
            },
        )

        wait_breakpoint()

        if revive:
            with Restarter(self.Env, [CONTROLLER_AGENTS_SERVICE]):
                pass

        release_breakpoint()
        op.track()

        assert sorted_dicts(read_table("//tmp/t_out")) == sorted_dicts((data1 + data2) * n_chunks)
        assert not get("//tmp/t_out/@sorted")

    @authors("coteeq")
    def test_disallow(self):
        create_user("user-not-allowed")
        with raises_yt_error("not allowed to start operations"):
            map(
                in_=self.to_remote_path("//tmp/t"),
                out_="//tmp/out",
                authenticated_user="user-not-allowed",
                command="cat"
            )

        with raises_yt_error("not allowed to be an input remote cluster"):
            map(
                # NB: Cluster 'not-allowed' does not need to exist
                in_="""<cluster="not-allowed">//tmp/t""",
                out_="//tmp/out",
                command="cat"
            )

    @authors("coteeq")
    def test_max_total_data_weight(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2")
        create("table", "//tmp/t_out")

        write_table("//tmp/t1", [{"value": "x" * 10_000}], driver=self.remote_driver)
        write_table("//tmp/t2", [{"value": "x" * 10_000}])

        def try_run_map():
            map(
                in_=[
                    self.to_remote_path("//tmp/t1"),
                    "//tmp/t2",
                ],
                out="//tmp/t_out",
                command="cat",
                spec={
                    "mapper": {
                        "format": "json",
                        "enable_input_table_index": False,
                    },
                },
            )

        try_run_map()

        update_controller_agent_config("remote_operations/remote_0/max_total_data_weight", 1_000)
        with raises_yt_error("Total estimated input data weight from cluster"):
            try_run_map()

        write_table("//tmp/t1", [{"value": "x" * 10}], driver=self.remote_driver)
        try_run_map()

    @authors("coteeq")
    def test_seed_replicas(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2")

        data = [{"a": 1}, {"a": 2}]
        write_table("<append=%true>//tmp/t1", data, driver=self.remote_driver)

        map(
            in_=self.to_remote_path("//tmp/t1"),
            out="//tmp/t2",
            command="cat",
            spec={
                "job_io": {
                    "table_reader": {
                        # Forbid reader from locating seeds and populating node directory.
                        "retry_count": 1,
                    }
                },
            }
        )

        assert sorted_dicts(read_table("//tmp/t2")) == sorted_dicts(data)
        assert not get("//tmp/t2/@sorted")

    @authors("coteeq")
    @pytest.mark.parametrize("operation_type", ["map", "merge", "map_reduce"])
    @pytest.mark.parametrize("dump_local", [True, False])
    def test_per_cluster_chunk_reader_statistics(self, operation_type, dump_local):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", [{"a": "b"}], driver=self.remote_driver)
        create("table", "//tmp/t1_local")
        write_table("//tmp/t1_local", [{"a": "b"}])

        create("table", "//tmp/t2")

        mapper_spec = {
            "mapper": {
                "input_format": "json",
                "output_format": "json",
                "enable_input_table_index": False,
            },
        }

        run_operation = {
            "map": partial(map, command="cat", spec=mapper_spec),
            "merge": partial(merge, spec={"force_transform": True}),
            "map_reduce": partial(map_reduce, mapper_command="cat", spec=mapper_spec, reducer_command="cat", reduce_by=["a"]),
        }[operation_type]

        job_type = {
            "map": "map",
            "merge": "unordered_merge",
            "map_reduce": "partition_map(0)",
        }[operation_type]

        def run_and_get_statistics(remote_input=True):
            op = run_operation(
                in_=[
                    *([self.to_remote_path("//tmp/t1")] if remote_input else []),
                    "//tmp/t1_local",
                ],
                out="//tmp/t2",
            )

            return get(op.get_path() + "/@progress/job_statistics_v2")

        statistics = run_and_get_statistics()
        assert extract_statistic_v2(statistics, "chunk_reader_statistics.remote_0.block_count", job_type=job_type) is None

        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_proxy": {
                        "enable_per_cluster_chunk_reader_statistics": True,
                        "dump_single_local_cluster_statistics": dump_local,
                    }
                },
            },
        })

        statistics = run_and_get_statistics()

        assert extract_statistic_v2(statistics, "chunk_reader_statistics.block_count", job_type=job_type) == 2
        assert extract_statistic_v2(statistics, "chunk_reader_statistics.remote_0.block_count", job_type=job_type) == 1
        assert extract_statistic_v2(statistics, "chunk_reader_statistics.<local>.block_count", job_type=job_type) == 1

        statistics = run_and_get_statistics(remote_input=False)

        assert extract_statistic_v2(statistics, "chunk_reader_statistics.block_count", job_type=job_type) == 1
        assert extract_statistic_v2(statistics, "chunk_reader_statistics.<local>.block_count", job_type=job_type) == (1 if dump_local else None)

    @authors("coteeq")
    def test_output_table_path(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2", driver=self.remote_driver)

        with raises_yt_error("Output table must be on the same cluster as operation"):
            map(
                in_=self.to_remote_path("//tmp/t1"),
                out=self.to_remote_path("//tmp/t2"),
                command="cat",
            )


@pytest.mark.enabled_multidaemon
class TestSchedulerRemoteOperationNetworks(TestSchedulerRemoteOperationCommandsBase):
    ENABLE_MULTIDAEMON = True

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        if cls.get_cluster_name(cluster_index) == cls.REMOTE_CLUSTER_NAME:
            config["addresses"].append(["custom_network", dict(config["addresses"])["default"]])

    @authors("coteeq")
    @pytest.mark.parametrize("operation_type", ["map", "merge", "map_reduce"])
    def test_custom_network(self, operation_type):
        create("table", "//tmp/t1", driver=self.remote_driver)
        write_table("//tmp/t1", {"a": "b"}, driver=self.remote_driver)

        create("table", "//tmp/t2")

        run_operation = {
            "map": partial(map, command="cat"),
            "merge": merge,
            "map_reduce": partial(map_reduce, mapper_command="cat", reducer_command="cat", reduce_by=["a"]),
        }

        def run_and_assert():
            run_operation[operation_type](
                in_=self.to_remote_path("//tmp/t1"),
                out="//tmp/t2",
            )
            assert read_table("//tmp/t2") == [{"a": "b"}]

        run_and_assert()

        update_controller_agent_config("remote_operations/remote_0/networks", ["custom_network"])

        run_and_assert()

        update_controller_agent_config("remote_operations/remote_0/networks", ["unexisting"])

        with raises_yt_error():
            run_and_assert()


@pytest.mark.enabled_multidaemon
class TestSchedulerRemoteOperationAllowedForEveryoneCluster(TestSchedulerRemoteOperationCommandsBase):
    ENABLE_MULTIDAEMON = True
    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
            "remote_copy_operation_options": {
                "spec_template": {
                    "use_remote_master_caches": True,
                },
            },
            "remote_operations": {
                "remote_0": {
                    "allowed_for_everyone": True,
                },
            },
        },
    }

    @authors("renadeen")
    def test_simple(self):
        create("table", "//tmp/t1", driver=self.remote_driver)
        create("table", "//tmp/t2")

        map(
            in_=self.to_remote_path("//tmp/t1"),
            out="//tmp/t2",
            command="cat",
        )

        assert read_table("//tmp/t2") == []
        assert not get("//tmp/t2/@sorted")


class TestSchedulerRemoteOperationWithClusterThrottlers(TestSchedulerRemoteOperationCommandsBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_DISCOVERY_SERVERS = 1
    NUM_NODES = 2

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1,
            "default_file_replication_factor": 1,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            # Enable job throttler on exe node.
            "job_throttler": {
            },
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operations_update_period": 100,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
            "operations_update_period": 100,
            "operation_alerts_push_period": 100,
            "alert_manager": {
                "period": 100,
                "task_unavailable_network_bandwidth_time_ratio_alert_threshold": 0.01,
            },
            "remote_copy_operation_options": {
                "spec_template": {
                    "use_remote_master_caches": True,
                },
            },
            "remote_operations": {
                "remote_0": {
                    "allowed_users": ["root"],
                },
            },
        },
    }

    CHUNK_COUNT = 32
    BANDWIDTH_LIMIT = 10 ** 7
    THROTTLER_JITTER_MULTIPLIER = 0.5
    DATA_WEIGHT_SIZE_PER_CHUNK = 10 ** 7

    LEASE_TIMEOUT_SECONDS = 1

    def _create_default_cluster_throttlers_config(self):
        return {
            "enabled": True,
            "update_period": 600,
            "cluster_limits": {
                # Limit bandwidth from remote cluster to local cluster.
                self.REMOTE_CLUSTER_NAME: {
                    "bandwidth": {
                        "limit": self.BANDWIDTH_LIMIT,
                    },
                },
            },
            "distributed_throttler": {
                "member_client": {
                    "lease_timeout": self.LEASE_TIMEOUT_SECONDS * 1000,
                },
                "heartbeat_period": 200,
                "attribute_update_period": 600,
                "heartbeat_throttler_count_limit": 2,
                "limit_update_period": 600,
                "leader_update_period": 600,
            },
        }

    # Setup cluster throttlers config on local cluster.
    def _setup_cluster_throttlers_config(self, config=None):
        create('document', '//sys/cluster_throttlers', force=True)
        if config is None:
            config = self._create_default_cluster_throttlers_config()
        set('//sys/cluster_throttlers', config)

    # Restart exe nodes to initialize cluster throttlers after config setup.
    def _restart_nodes(self):
        with Restarter(self.Env, NODES_SERVICE):
            time.sleep(1)

        wait_for_nodes()

    # Create and initialize cluster throttlers config on all exe nodes.
    def _init_cluster_throttlers_config(self, config=None):
        # Create and set cluster throttlers config on local cluster.
        self._setup_cluster_throttlers_config(config=config)

        # Restart exe nodes to pick up new cluster throttlers config.
        self._restart_nodes()

        # Wait for exe nodes to apply new cluster throttlers config.
        self._wait_for_all_remote_cluster_throttlers_group_members()

    # Wait for all exe nodes to register in remote_cluster_throttlers_group.
    def _wait_for_all_remote_cluster_throttlers_group_members(self):
        def has_all_remote_cluster_throttlers_group_members():
            try:
                sys = ls("//sys")
                if len(sys) == 0:
                    return False
                if 'discovery_servers' not in sys:
                    return False
                servers = ls("//sys/discovery_servers")
                if len(servers) == 0:
                    return False
                discovery_server = servers[0]
                groups = ls("//sys/discovery_servers/{}/orchid/discovery_server".format(discovery_server))
                if 'remote_cluster_throttlers_group' not in groups:
                    return False
                group_members = ls("//sys/discovery_servers/{}/orchid/discovery_server/remote_cluster_throttlers_group/@members".format(discovery_server))
                return len(group_members) >= self.NUM_NODES
            except YtResponseError:
                return False

        # Wait for all exe nodes to register in discovery service.
        wait(lambda: has_all_remote_cluster_throttlers_group_members(), timeout=60)

    # Wait for all exe nodes to unregister from remote_cluster_throttlers_group.
    def _wait_for_no_remote_cluster_throttlers_group_members(self):
        def has_no_remote_cluster_throttlers_group_members():
            try:
                sys = ls("//sys")
                if len(sys) == 0:
                    return False
                if 'discovery_servers' not in sys:
                    return False
                servers = ls("//sys/discovery_servers")
                if len(servers) == 0:
                    return False
                discovery_server = servers[0]
                groups = ls("//sys/discovery_servers/{}/orchid/discovery_server".format(discovery_server))
                if 'remote_cluster_throttlers_group' not in groups:
                    return True
                group_members = ls("//sys/discovery_servers/{}/orchid/discovery_server/remote_cluster_throttlers_group/@members".format(discovery_server))
                return len(group_members) == 0
            except YtResponseError:
                return False

        # Wait for all exe nodes to unregister from discovery service.
        wait(lambda: has_no_remote_cluster_throttlers_group_members(), timeout=60)

    # Wait for bandwidth to become unavailable in CA.
    def _wait_for_bandwidth_to_become_unavailable(self, op):
        wait(lambda: exists(op.get_orchid_path() + "/controller/network_bandwidth_availability"))

        def is_not_available(cluster, op):
            value = get(op.get_orchid_path() + "/controller/network_bandwidth_availability")
            return str(value.get(cluster, None)) == "false"

        wait(lambda: is_not_available(self.REMOTE_CLUSTER_NAME, op))

    @authors("yuryalekseev")
    def test_cluster_throttlers(self):
        # Create and initialize default cluster throttlers config on all exe nodes.
        self._init_cluster_throttlers_config()

        # Create table on remote cluster.
        create(
            "table",
            "//tmp/remote_table",
            attributes={"compression_codec": "none"},
            chunk_reader={"enable_local_throttling": True},
            driver=self.remote_driver)

        # Fill up table on remote cluster.
        for c in range(self.CHUNK_COUNT):
            write_table("<append=%true>//tmp/remote_table", {"v": "0" * self.DATA_WEIGHT_SIZE_PER_CHUNK}, driver=self.remote_driver)

        # Create table on local cluster.
        create("table", "//tmp/local_table")

        operation_start_time = time.time()

        op = map(
            in_=self.to_remote_path("//tmp/remote_table"),
            out="//tmp/local_table",
            command="cat",
            spec={
                "job_io": {
                    "table_reader": {
                        "enable_local_throttling": True,
                    },
                },
                "job_count": 1,
                "use_cluster_throttlers": True,
            },
        )

        operation_end_time = time.time()

        # Check result table on local cluster.
        assert read_table("//tmp/local_table") == [{"v": "0" * self.DATA_WEIGHT_SIZE_PER_CHUNK} for c in range(self.CHUNK_COUNT)]
        assert not get("//tmp/local_table/@sorted")

        # Check that throttling has happened.
        assert (operation_end_time - operation_start_time) > (self.CHUNK_COUNT * self.DATA_WEIGHT_SIZE_PER_CHUNK * self.THROTTLER_JITTER_MULTIPLIER / self.BANDWIDTH_LIMIT)

        # Check that solomon counters have showed up.
        for job_id in op.list_jobs():
            job = get_job(op.id, job_id)

            profiler = profiler_factory().at_node(job["address"])
            wait(lambda: profiler.get("exec_node/throttler_manager/distributed_throttler/limit", {"throttler_id": "bandwidth_{}".format(self.REMOTE_CLUSTER_NAME)}) is not None)
            wait(lambda: profiler.get("exec_node/throttler_manager/distributed_throttler/usage", {"throttler_id": "bandwidth_{}".format(self.REMOTE_CLUSTER_NAME)}) is not None)

    @authors("yuryalekseev")
    def test_cluster_throttlers_all_nodes_banned(self):
        # Create and initialize default cluster throttlers config on all exe nodes.
        self._init_cluster_throttlers_config()

        # Ban all nodes on local cluster.
        set_all_nodes_banned(True)

        # Wait for all nodes to disappear from group.
        self._wait_for_no_remote_cluster_throttlers_group_members()

    @authors("yuryalekseev")
    def test_rate_limit_ratio_hard_threshold(self):
        config = self._create_default_cluster_throttlers_config()
        # Set parameters to disable scheduling of operations.
        config["rate_limit_ratio_hard_threshold"] = -1
        config["cluster_limits"][self.REMOTE_CLUSTER_NAME] = {
            "bandwidth": {
                "limit": 0,
            },
        }
        self._init_cluster_throttlers_config(config)

        # Create table on remote cluster.
        create(
            "table",
            "//tmp/remote_table",
            attributes={"compression_codec": "none"},
            chunk_reader={"enable_local_throttling": True},
            driver=self.remote_driver)

        # Fill up table on remote cluster.
        for c in range(self.CHUNK_COUNT):
            write_table("<append=%true>//tmp/remote_table", {"v": "0" * self.DATA_WEIGHT_SIZE_PER_CHUNK}, driver=self.remote_driver)

        # Create table on local cluster.
        create("table", "//tmp/local_table")

        operation_time_limit_seconds = 30

        # Copy table from remote cluster to local cluster.
        op = map(
            track=False,
            in_=self.to_remote_path("//tmp/remote_table"),
            out="//tmp/local_table",
            command="cat",
            spec={
                "job_io": {
                    "table_reader": {
                        "enable_local_throttling": True,
                    },
                },
                "job_count": self.CHUNK_COUNT,
                "use_cluster_throttlers": True,
                "time_limit": operation_time_limit_seconds * 1000,
            },
        )

        # Wait for bandwidth to become unavailable in CA.
        self._wait_for_bandwidth_to_become_unavailable(op)

        # Wait for operation abortion by time limit.
        with pytest.raises(YtError) as err:
            op.track(timeout=datetime.timedelta(seconds=2*operation_time_limit_seconds))

        assert 'has not finished in' in str(err.value) or 'running for too long' in str(err.value)
        assert 'unavailable_network_bandwidth_to_clusters' in op.get_alerts()

    @authors("yuryalekseev")
    def test_map_with_auto_merge_with_remote_bandwidth_control(self):
        # Create and initialize default cluster throttlers config on all exe nodes.
        self._init_cluster_throttlers_config()

        # Create table on remote cluster.
        create(
            "table",
            "//tmp/remote_input",
            attributes={"compression_codec": "none"},
            chunk_reader={"enable_local_throttling": True},
            driver=self.remote_driver)

        # Make a two chunk table.
        data = [{"key": i, "value": f"data_{i}"} for i in range(10)]
        write_table("//tmp/remote_input", data, driver=self.remote_driver)
        write_table("<append=%true>//tmp/remote_input", data, driver=self.remote_driver)

        # Create table on local cluster.
        create("table", "//tmp/local_output")

        # Run map operation with auto merge and breakpoint.
        op = map(
            track=False,
            in_=self.to_remote_path("//tmp/remote_input"),
            out="//tmp/local_output",
            command=with_breakpoint("cat; BREAKPOINT"),
            spec={
                "job_count": 2,
                "use_cluster_throttlers": True,
                "auto_merge": {
                    "mode": "manual",
                    "max_intermediate_chunk_count": 2,
                    "chunk_count_per_merge_job": 1,
                    "single_chunk_teleport_strategy": "disabled",
                    "enable_shallow_merge": False,
                },
                "job_io": {
                    "table_reader": {
                        "enable_local_throttling": True,
                    },
                },
            },
        )

        # Wait for map jobs to reach the breakpoint.
        wait_breakpoint(job_count=2, timeout=datetime.timedelta(seconds=30))

        # Make bandwidth unavailable.
        config = self._create_default_cluster_throttlers_config()
        config["rate_limit_ratio_hard_threshold"] = -1
        config["cluster_limits"][self.REMOTE_CLUSTER_NAME] = {
            "bandwidth": {
                "limit": 0,
            },
        }

        # Update cluster throttlers config.
        set('//sys/cluster_throttlers', config)

        # Wait for bandwidth to become unavailable in CA.
        self._wait_for_bandwidth_to_become_unavailable(op)

        # Resume jobs and complete the operation.
        release_breakpoint()
        op.track(timeout=datetime.timedelta(seconds=30))

        # Verify results.
        result = read_table("//tmp/local_output")
        assert sorted_dicts(result) == sorted_dicts(data + data)

        # Check that auto merge jobs have been run.
        data_flow_graph = get(op.get_path() + "/@progress/data_flow_graph")
        data_weight = data_flow_graph["edges"]["map"]["auto_merge"]["statistics"]["data_weight"]
        assert data_weight > 0
        assert data_flow_graph["edges"]["auto_merge"]["sink"]["statistics"]["data_weight"] == data_weight
