from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, get, set, copy, remove, exists, wait,
    create_account, create_user, assert_statistics, raises_yt_error, sorted_dicts,
    make_ace, start_transaction, commit_transaction, insert_rows, read_table, write_table, sort, erase, get_operation,
    sync_create_cells, sync_mount_table, sync_unmount_table, get_singular_chunk_id, create_dynamic_table)

from yt_helpers import skip_if_no_descending
from yt_type_helpers import make_schema, normalize_schema, normalize_schema_v3, list_type, optional_type

from yt.environment.helpers import assert_items_equal
from yt.common import YtError

import pytest

import random
import builtins
import yt.yson as yson
from copy import deepcopy

##################################################################


def get_operation_job_types(opid):
    progress = get_operation(opid)["progress"]
    job_types = [task["job_type"] for task in progress["tasks"]]
    return job_types


def check_operation_tasks(op, expected):
    data_flow_path = op.get_path() + "/@progress/data_flow"
    data_flow = get(data_flow_path)
    tasks = []
    for direction in data_flow:
        if direction["source_name"] != "input":
            tasks.append(direction["source_name"])
        if direction["target_name"] != "output":
            tasks.append(direction["target_name"])
    return builtins.set(tasks) == builtins.set(expected)


def simple_sort_1_phase(in_, out, sort_by):
    op = sort(in_=in_, out=out, sort_by=sort_by)
    op.track()
    assert builtins.set(get_operation_job_types(op.id)) == {"simple_sort"}
    assert check_operation_tasks(op, ["simple_sort"])
    return op


def simple_sort_2_phase(in_, out, sort_by):
    op = sort(in_=in_, out=out, sort_by=sort_by, spec={"data_weight_per_sort_job": 1})
    op.track()
    assert builtins.set(get_operation_job_types(op.id)) == {
        "simple_sort",
        "sorted_merge",
    }
    assert check_operation_tasks(op, ["simple_sort", "sorted_merge"])
    return op


def sort_2_phase(in_, out, sort_by):
    op = sort(
        in_=in_,
        out=out,
        sort_by=sort_by,
        spec={
            "partition_job_count": 2,
            "partition_count": 2,
        },
    )
    op.track()
    assert builtins.set(get_operation_job_types(op.id)) == {
        "partition",
        "final_sort",
    }
    assert check_operation_tasks(op, ["partition(0)", "final_sort"])
    return op


def sort_2_phase_depth_2(in_, out, sort_by):
    op = sort(
        in_=in_,
        out=out,
        sort_by=sort_by,
        spec={
            "partition_job_count": 2,
            "partition_count": 4,
            "max_partition_factor": 2,
        },
    )
    op.track()
    assert builtins.set(get_operation_job_types(op.id)) == {
        "partition",
        "final_sort",
    }
    assert check_operation_tasks(op, ["partition(0)", "partition(1)", "final_sort"])
    return op


def sort_3_phase(in_, out, sort_by):
    op = sort(
        in_=in_,
        out=out,
        sort_by=sort_by,
        spec={
            "partition_job_count": 10,
            "partition_count": 10,
            "data_weight_per_sort_job": 1,
            "partition_job_io": {
                "table_writer": {
                    "desired_chunk_size": 1,
                    "block_size": 1024,
                }
            },
        },
    )
    op.track()
    assert builtins.set(get_operation_job_types(op.id)) == {
        "intermediate_sort",
        "partition",
        "sorted_merge",
    }
    assert check_operation_tasks(op, ["partition(0)", "intermediate_sort", "sorted_merge"])
    return op


def sort_3_phase_depth_2(in_, out, sort_by):
    op = sort(
        in_=in_,
        out=out,
        sort_by=sort_by,
        spec={
            "partition_job_count": 10,
            "partition_count": 10,
            "max_partition_factor": 4,
            "data_weight_per_sort_job": 1,
            "data_weight_per_intermediate_partition_job": 10,
            "partition_job_io": {
                "table_writer": {
                    "desired_chunk_size": 1,
                    "block_size": 1024,
                }
            },
        },
    )
    op.track()
    assert builtins.set(get_operation_job_types(op.id)) == {
        "intermediate_sort",
        "partition",
        "sorted_merge",
    }
    assert check_operation_tasks(op, ["partition(0)", "partition(1)", "intermediate_sort", "sorted_merge"])
    return op


def sort_maniac(in_, out, sort_by, validate_types=False):
    if isinstance(sort_by, str):
        sort_by = [sort_by]

    def normalize_key(value):
        if isinstance(value, list):
            return tuple(normalize_key(v) for v in value)
        else:
            return value

    def get_key(row):
        return tuple(normalize_key(row[k]) for k in sort_by)

    def count_keys(key_list):
        # NB. The easier solution would be create a set of keys
        # but sometimes keys contain YsonLists that are unhashable
        # so we use sorting.

        prev = ()  # empty tuple is never met in keys
        count = 0
        for k in sorted(key_list):
            if k != prev:
                count += 1
            prev = k
        return count

    key_count = len(builtins.set(get_key(r) for r in read_table(in_)))

    tmp = in_ + ".tmp"
    copy(in_, tmp)
    write_table(tmp, [])
    for r in read_table(in_):
        write_table("<append=%true>" + tmp, [r])

    op = sort(
        in_=tmp,
        out=out,
        sort_by=sort_by,
        spec={
            "partition_job_count": 4,
            "partition_count": key_count * 5,
            "data_weight_per_sort_job": 1,
        },
    )
    op.track()

    if validate_types:
        job_types = {"unordered_merge", "partition"}
        assert builtins.set(get_operation_job_types(op.id)) == job_types
        task_names = ["partition(0)", "unordered_merge"]
        assert check_operation_tasks(op, task_names)
    return op


class TestSchedulerSortCommands(YTEnvSetup):
    NUM_TEST_PARTITIONS = 18
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "sort_operation_options": {
                "min_uncompressed_block_size": 1,
                "min_partition_size": 1,
                "max_value_count_per_simple_sort_job": 100,
                "max_data_slices_per_job": 100,
                "spec_template": {
                    "use_new_sorted_pool": False,
                }
            },
        }
    }

    def skip_if_legacy_sorted_pool(self):
        if not isinstance(self, TestSchedulerSortCommandsNewSortedPool):
            pytest.skip("This test requires new sorted pool")

    @authors("ignat")
    def test_simple(self):
        v1 = {"key": "aaa"}
        v2 = {"key": "bb"}
        v3 = {"key": "bbxx"}
        v4 = {"key": "zfoo"}
        v5 = {"key": "zzz"}

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [v3, v5, v1, v2, v4])  # some random order

        create("table", "//tmp/t_out")

        sort(in_="//tmp/t_in", out="//tmp/t_out", sort_by="key")

        assert read_table("//tmp/t_out") == [v1, v2, v3, v4, v5]
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["key"]

    @authors("psushin")
    def test_megalomaniac_protection(self):
        v1 = {"key": "aaa"}
        v2 = {"key": "bb"}
        v3 = {"key": "bbxx"}
        v4 = {"key": "zfoo"}
        v5 = {"key": "zzz"}

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [v3, v5, v1, v2, v4])  # some random order

        create("table", "//tmp/t_out")

        with pytest.raises(YtError):
            sort(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                sort_by="key",
                spec={"max_input_data_weight": 5},
            )

        with pytest.raises(YtError):
            sort(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                sort_by="key",
                spec={
                    "max_shuffle_data_slice_count": 1,
                    "partition_job_count": 2,
                    "partition_count": 2,
                },
            )

        with pytest.raises(YtError):
            sort(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                sort_by="key",
                spec={
                    "max_shuffle_job_count": 1,
                    "partition_job_count": 2,
                    "partition_count": 2,
                },
            )

        with pytest.raises(YtError):
            sort(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                sort_by="key",
                spec={
                    "max_merge_data_slice_count": 1,
                    "partition_job_count": 2,
                    "partition_count": 2,
                    "data_weight_per_sort_job": 3,
                },
            )

    @authors("max42")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sort_with_sampling(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t_in")

        n = 1003
        write_table("//tmp/t_in", [{"a": (42 * x) % n} for x in range(n)])

        create("table", "//tmp/t_out")

        def check():
            result = read_table("//tmp/t_out")
            assert n * 0.5 - 100 <= len(result) <= n * 0.5 + 100

        sort(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by=[{"name": "a", "sort_order": sort_order}],
            spec={
                "partition_job_io": {"table_reader": {"sampling_rate": 0.5}},
                "partition_count": 10,
            },
        )
        check()

        sort(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by=[{"name": "a", "sort_order": sort_order}],
            spec={
                "partition_job_io": {"table_reader": {"sampling_rate": 0.5}},
                "partition_count": 10,
                "max_partition_factor": 2,
            },
        )
        check()

    @authors("psushin")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_simple_read_limits(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        v1 = {"key": "aaa", "value": "2"}
        v2 = {"key": "bb", "value": "5"}
        v3 = {"key": "bbxx", "value": "1"}
        v4 = {"key": "zfoo", "value": "4"}
        v5 = {"key": "zzz", "value": "3"}

        create("table", "//tmp/t_in", attributes={
            "schema": make_schema([
                {"name": "key", "type": "string", "sort_order": sort_order},
                {"name": "value", "type": "string"},
            ])})

        rows = [v1, v2, v3, v4, v5]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table("//tmp/t_in", rows)

        create("table", "//tmp/t_out")

        if sort_order == "ascending":
            t_in = "<lower_limit={key=[b]}; upper_limit={key=[z]}>//tmp/t_in"
        else:
            t_in = "<lower_limit={key=[bbxx]}; upper_limit={key=[ad]}>//tmp/t_in"

        sort(
            in_=t_in,
            out="//tmp/t_out",
            sort_by=[{"name": "value", "sort_order": sort_order}],
        )

        expected = [v3, v2] if sort_order == "ascending" else [v2, v3]
        assert read_table("//tmp/t_out") == expected
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["value"]

    @authors("psushin")
    def test_key_weight_limit(self):
        v1 = {"key": "aaa"}
        v2 = {"key": "bb"}

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [v2, v1])

        create("table", "//tmp/t_out")

        with pytest.raises(YtError):
            sort(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                sort_by="key",
                spec={"merge_job_io": {"table_writer": {"max_key_weight": 2}}},
            )

    @authors("psushin")
    def test_max_value_count_per_simple_sort_job(self):
        schema = make_schema(
            [{"name": "key", "type": "string"}] +
            [{"name": "value{}".format(i), "type": "string"} for i in range(200)]
        )

        create("table", "//tmp/t_in", attributes={"schema": schema})
        data = [{"key" : str(i)} for i in range(10)]
        shuffled_data = data[:]
        random.shuffle(shuffled_data)
        write_table("//tmp/t_in", shuffled_data)

        create("table", "//tmp/t_out")

        op = sort(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by="key",
            spec={"use_new_partitions_heuristic": True})

        assert read_table("<columns=[key]>//tmp/t_out") == data
        assert check_operation_tasks(op, {"partition(0)", "final_sort"})

    @authors("psushin")
    def test_foreign(self):
        v1 = {"key": "aaa"}

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [v1])

        create("table", "//tmp/t_out")

        with pytest.raises(YtError):
            sort(in_="<foreign=true>//tmp/t_in", out="//tmp/t_out", sort_by="key")

    @authors("psushin")
    def test_large_values(self):
        a = "".join(["a"] * 10 * 1024)
        b = "".join(["b"] * 100 * 1024)
        v1 = {"key": b, "subkey": b}
        v2 = {"key": a, "subkey": a}

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", v1)
        write_table("<append=true>//tmp/t_in", v2)

        create("table", "//tmp/t_out")

        sort(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by=["key", "subkey"],
            spec={"merge_job_io": {"table_writer": {"max_key_weight": 250 * 1024}}},
        )

        assert read_table("//tmp/t_out") == [v2, v1]
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["key", "subkey"]

    # the same as test_simple but within transaction
    @authors("babenko", "ignat")
    def test_simple_transacted(self):
        tx = start_transaction()

        v1 = {"key": "aaa"}
        v2 = {"key": "bb"}
        v3 = {"key": "bbxx"}
        v4 = {"key": "zfoo"}
        v5 = {"key": "zzz"}

        create("table", "//tmp/t_in", tx=tx)
        write_table("//tmp/t_in", [v3, v5, v1, v2, v4], tx=tx)  # some random order

        create("table", "//tmp/t_out", tx=tx)

        sort(in_="//tmp/t_in", out="//tmp/t_out", sort_by="key", tx=tx)

        commit_transaction(tx)

        assert read_table("//tmp/t_out") == [v1, v2, v3, v4, v5]
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["key"]

    @authors("ignat")
    def test_empty_columns(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", {"foo": "bar"})

        with pytest.raises(YtError):
            sort(in_="//tmp/t_in", out="//tmp/t_out", sort_by=[])

    @authors("ignat")
    def test_empty_in(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        sort(in_="//tmp/t_in", out="//tmp/t_out", sort_by="key")

        assert read_table("//tmp/t_out") == []
        assert get("//tmp/t_out/@sorted")

    @authors("panin", "ignat")
    def test_non_empty_out(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", {"foo": "bar"})
        write_table("//tmp/t_out", {"hello": "world"})

        with pytest.raises(YtError):
            sort(in_="//tmp/t_in", out="<append=true>//tmp/t_out", sort_by="foo")

    @authors("ermolovd")
    def test_validate_schema(self):
        create(
            "table",
            "//tmp/t_in",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "field", "type": "int64", "required": False},
                    ]
                )
            },
        )
        create(
            "table",
            "//tmp/t_out",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "field", "type": "int64", "required": True},
                    ]
                )
            },
        )
        write_table("//tmp/t_in", [{"field": 1}])
        sort(in_="//tmp/t_in", out="//tmp/t_out", sort_by="field")

        write_table("<append=%true>//tmp/t_in", [{"field": None}])

        with pytest.raises(YtError):
            sort(in_="//tmp/t_in", out="//tmp/t_out", sort_by="field")

    @authors("dakovalkov")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_append_simple(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t_in")
        create(
            "table",
            "//tmp/t_out",
            attributes={
                "schema": make_schema(
                    [{"name": "key", "type": "int64", "sort_order": sort_order}],
                    unique_keys=False,
                )
            },
        )

        old_key = 1
        new_key = 2 if sort_order == "ascending" else 0

        write_table("//tmp/t_in", {"key": new_key})
        write_table("//tmp/t_out", {"key": old_key})

        sort(
            in_="//tmp/t_in",
            out="<append=true>//tmp/t_out",
            sort_by=[{"name": "key", "sort_order": sort_order}])

        assert read_table("//tmp/t_out") == [{"key": old_key}, {"key": new_key}]
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["key"]

    @authors("dakovalkov")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_append_different_key_columns(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t_in")
        create(
            "table",
            "//tmp/t_out",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "key", "type": "int64", "sort_order": sort_order},
                        {"name": "subkey", "type": "int64"},
                    ],
                    unique_keys=False,
                )
            },
        )

        old_row = {"key": 2, "subkey": 2}
        new_row = {"key": 1, "subkey": 1}
        if sort_order == "descending":
            old_row, new_row = new_row, old_row

        write_table("//tmp/t_in", old_row)
        write_table("//tmp/t_out", new_row)

        with pytest.raises(YtError):
            sort(
                in_="//tmp/t_in",
                out="<append=true>//tmp/t_out",
                sort_by=[
                    {"name": "key", "sort_order": sort_order},
                    {"name": "subkey", "sort_order": sort_order},
                ],
            )

    @authors("dakovalkov")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_append_different_key_columns_2(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t_in")
        create(
            "table",
            "//tmp/t_out",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "key", "type": "int64", "sort_order": sort_order},
                        {"name": "subkey", "type": "int64", "sort_order": sort_order},
                    ],
                    unique_keys=False,
                )
            },
        )

        write_table("//tmp/t_in", {"key": 2, "subkey": 2})
        if sort_order == "ascending":
            write_table("//tmp/t_out", {"key": 1, "subkey": 1})
        else:
            write_table("//tmp/t_out", {"key": 3, "subkey": 3})

        with pytest.raises(YtError):
            sort(
                in_="//tmp/t_in",
                out="<append=true>//tmp/t_out",
                sort_by=[{"name": "key", "sort_order": sort_order}])

    @authors("gritukan", "dakovalkov")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_append_different_sort_order(self, sort_order):
        skip_if_no_descending(self.Env)

        inverted_sort_order = "descending" if sort_order == "ascending" else "ascending"

        create("table", "//tmp/in_0")
        create("table", "//tmp/in_2")

        create(
            "table",
            "//tmp/out",
            attributes={
                "schema": make_schema(
                    [{"name": "key", "type": "int64", "sort_order": sort_order}],
                    unique_keys=False,
                )
            },
        )

        write_table("//tmp/in_0", {"key": 0})
        write_table("//tmp/in_2", {"key": 2})
        write_table("//tmp/out", {"key": 1})

        for in_table in ["//tmp/in_0", "//tmp/in_2"]:
            with pytest.raises(YtError):
                sort(
                    in_=in_table,
                    out="<append=true>//tmp/out",
                    sort_by=[{"name": "key", "sort_order": inverted_sort_order}])

    @authors("ignat")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_maniac(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        v1 = {"key": "aaa"}
        v2 = {"key": "bb"}
        v3 = {"key": "bbxx"}
        v4 = {"key": "zfoo"}
        v5 = {"key": "zzz"}

        create("table", "//tmp/t_in")
        for i in range(0, 10):
            write_table("<append=true>//tmp/t_in", [v3, v5, v1, v2, v4])  # some random order

        create("table", "//tmp/t_out")

        sort(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by=[{"name": "missing_key", "sort_order": sort_order}],
            spec={
                "partition_count": 5,
                "partition_job_count": 2,
                "data_weight_per_sort_job": 1,
            },
        )

        assert len(read_table("//tmp/t_out")) == 50

    @authors("max42")
    def test_incomplete_sample_fetching(self):
        max_sample_size = 64 * 1024
        v1 = {"key": "a" * max_sample_size + "a"}
        v2 = {"key": "a" * max_sample_size + "b"}
        v3 = {"key": "a" * max_sample_size + "c"}
        v4 = {"key": "a" * max_sample_size + "d"}
        v5 = {"key": "a" * max_sample_size + "e"}

        create("table", "//tmp/t_in")
        for i in range(0, 10):
            write_table("<append=true>//tmp/t_in", [v3, v5, v1, v2, v4], verbose=False)  # some random order

        create("table", "//tmp/t_out")

        sort(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by=["key"],
            spec={
                "partition_count": 5,
                "partition_job_count": 2,
                "data_weight_per_sort_job": 1,
                "merge_job_io": {"table_writer": {"max_key_weight": 2 * max_sample_size}},
                "sort_job_io": {"table_writer": {"max_key_weight": 2 * max_sample_size}},
            },
        )

        assert len(read_table("//tmp/t_out", verbose=False)) == 50
        assert sorted_dicts(read_table("//tmp/t_out", verbose=False)) == [v1] * 10 + [v2] * 10 + [v3] * 10 + [v4] * 10 + [v5] * 10

    @authors("psushin", "ignat")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_many_merge(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        v1 = {"key": "aaa"}
        v2 = {"key": "bb"}
        v3 = {"key": "bbxx"}
        v4 = {"key": "zfoo"}
        v5 = {"key": "zzz"}

        create("table", "//tmp/t_in")
        for i in range(0, 10):
            row = [v1, v2, v3, v4, v5]
            random.shuffle(row)
            write_table("<append=true>//tmp/t_in", row)  # some random order

        create("table", "//tmp/t_out")

        sort(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by=[{"name": "key", "sort_order": sort_order}],
            spec={
                "partition_count": 5,
                "partition_job_count": 2,
                "data_weight_per_sort_job": 1,
                "partition_job_io": {"table_writer": {"desired_chunk_size": 1, "block_size": 1024}},
            },
        )

        assert len(read_table("//tmp/t_out")) == 50

    @authors("max42")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_several_merge_jobs_per_partition(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        create("table", "//tmp/t_in")
        rows = [{"key": "k%03d" % (i), "value": "v%03d" % (i)} for i in range(500)]
        if sort_order == "descending":
            rows = rows[::-1]
        shuffled_rows = rows[::]
        random.shuffle(shuffled_rows)
        write_table("//tmp/t_in", shuffled_rows)

        create("table", "//tmp/t_out")

        sort(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by=[{"name": "key", "sort_order": sort_order}],
            spec={
                "partition_count": 2,
                "partition_job_count": 10,
                "data_weight_per_sort_job": 1,
                "partition_job_io": {"table_writer": {"desired_chunk_size": 1, "block_size": 1024}},
            },
        )

        assert read_table("//tmp/t_out") == rows
        assert get("//tmp/t_out/@chunk_count") >= 10

    @authors("dakovalkov")
    def test_sort_with_row_count_limit(self):
        create("table", "//tmp/t_in")
        rows = [{"key": "k%03d" % (i), "value": "v%03d" % (i)} for i in range(500)]
        shuffled_rows = rows[::]
        random.shuffle(shuffled_rows)

        for i in range(10):
            write_table("<append=%true>//tmp/t_in", shuffled_rows[50 * i:50 * (i + 1)])

        create("table", "//tmp/t_out")

        sort(
            in_="//tmp/t_in",
            out="<row_count_limit=10>//tmp/t_out",
            sort_by="key",
            spec={
                "partition_count": 10,
                "partition_job_count": 10,
                "data_weight_per_sort_job": 1000,
            },
        )

        output_rows = read_table("//tmp/t_out")
        assert sorted_dicts(output_rows) == output_rows
        assert len(output_rows) < 500
        assert len(output_rows) >= 10

    @authors("dakovalkov")
    def test_huge_uncompressed_size(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        def generate_row(row_index):
            row = {
                "key": "k%03d" % (row_index)
            }
            for column_index in range(100):
                row["column_%03d" % (column_index)] = False

            return row

        rows = [generate_row(i) for i in range(500)]
        shuffled_rows = rows[::]
        random.shuffle(shuffled_rows)

        write_table("//tmp/t_in", shuffled_rows)

        assert get("//tmp/t_in/@data_weight") < get("//tmp/t_in/@uncompressed_data_size")

        op = sort(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by="key",
            spec={
                "data_weight_per_sort_job": get("//tmp/t_in/@data_weight") * 2,
                "use_new_partitions_heuristic": True,
            },
        )
        op.track()
        assert op.get_job_count("completed", from_orchid=False) >= 2

    @authors("ignat", "klyachin")
    def test_with_intermediate_account(self):
        v1 = {"key": "aaa"}
        v2 = {"key": "bb"}
        v3 = {"key": "bbxx"}
        v4 = {"key": "zfoo"}
        v5 = {"key": "zzz"}

        create("table", "//tmp/t_in")
        for i in range(0, 10):
            write_table("<append=true>//tmp/t_in", [v3, v5, v1, v2, v4])  # some random order

        create("table", "//tmp/t_out")

        create_user("test_user")
        create_account("test_account")

        sort(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by="key",
            authenticated_user="test_user",
        )

        with pytest.raises(YtError):
            sort(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                sort_by="key",
                spec={
                    "partition_count": 5,
                    "partition_job_count": 2,
                    "data_weight_per_sort_job": 1,
                    "intermediate_data_account": "non_existing",
                },
            )

        with pytest.raises(YtError):
            sort(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                sort_by="missing_key",
                spec={"intermediate_data_account": "test_account"},
                authenticated_user="test_user",
            )

        set("//sys/accounts/test_account/@acl", [make_ace("allow", "test_user", "use")])

        sort(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by="key",
            authenticated_user="test_user",
        )

    @authors("panin", "ignat")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_composite_key(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        v1 = {"key": -7, "subkey": "bar", "value": "v1"}
        v2 = {"key": -7, "subkey": "foo", "value": "v2"}
        v3 = {"key": 12, "subkey": "a", "value": "v3"}
        v4 = {"key": 12, "subkey": "z", "value": "v4"}
        v5 = {"key": 500, "subkey": "foo", "value": "v5"}

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [v2, v5, v1, v4, v3])  # some random order

        create("table", "//tmp/t_out")

        sort(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by=[
                {"name": "key", "sort_order": sort_order},
                {"name": "subkey", "sort_order": sort_order}
            ]
        )

        expected = [v1, v2, v3, v4, v5]
        if sort_order == "descending":
            expected = expected[::-1]
        assert read_table("//tmp/t_out") == expected

        create("table", "//tmp/t_another_out")
        sort(
            in_="//tmp/t_in",
            out="//tmp/t_another_out",
            sort_by=[
                {"name": "subkey", "sort_order": sort_order},
                {"name": "key", "sort_order": sort_order}
            ]
        )

        expected = [v3, v1, v2, v5, v4]
        if sort_order == "descending":
            expected = expected[::-1]
        assert read_table("//tmp/t_another_out") == expected

    @authors("ignat")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_many_inputs(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        v1 = {"key": -7, "value": "v1"}
        v2 = {"key": -3, "value": "v2"}
        v3 = {"key": 0, "value": "v3"}
        v4 = {"key": 12, "value": "v4"}
        v5 = {"key": 500, "value": "v5"}
        v6 = {"key": 100500, "value": "v6"}

        create("table", "//tmp/in1")
        create("table", "//tmp/in2")

        write_table("//tmp/in1", [v5, v1, v4])  # some random order
        write_table("//tmp/in2", [v3, v6, v2])  # some random order

        create("table", "//tmp/t_out")
        sort(
            in_=["//tmp/in1", "//tmp/in2"],
            out="//tmp/t_out",
            sort_by=[
                {"name": "key", "sort_order": sort_order},
            ]
        )

        expected = [v1, v2, v3, v4, v5, v6]
        if sort_order == "descending":
            expected = expected[::-1]
        assert read_table("//tmp/t_out") == expected

    def sort_with_options(self, optimize_for, sort_order, **kwargs):
        input = "//tmp/in"
        output = "//tmp/out"
        create("table", input, attributes={"optimize_for": optimize_for})
        create("table", output)
        for i in range(20, 0, -1):
            write_table("<append=true>" + input, [{"key": i, "value": [1, 2]}])

        args = {
            "in_": [input],
            "out": output,
            "sort_by": [{"name": "key", "sort_order": sort_order}],
        }
        args.update(kwargs)

        sort(**args)
        assert get("//tmp/out/@sorted")
        expected = [{"key": i} for i in range(1, 21)]
        if sort_order == "descending":
            expected = expected[::-1]
        assert read_table(output + "{key}") == expected

    @authors("ignat", "babenko", "psushin")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_one_partition_no_merge(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        self.sort_with_options("lookup", sort_order)

    @authors("psushin")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_one_partition_with_merge(self, optimize_for, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        self.sort_with_options(optimize_for, sort_order, spec={"data_weight_per_sort_job": 1})

    @authors("psushin")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_two_partitions_no_merge(self, optimize_for, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        self.sort_with_options(optimize_for, sort_order, spec={"partition_count": 2})

    @authors("psushin")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_ten_partitions_no_merge(self, optimize_for, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        self.sort_with_options(optimize_for, sort_order, spec={"partition_count": 10})

    @authors("psushin")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_two_partitions_with_merge(self, optimize_for, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        self.sort_with_options(
            optimize_for,
            sort_order,
            spec={
                "partition_count": 2,
                "partition_data_size": 1,
                "data_weight_per_sort_job": 1,
            },
        )

    @authors("ignat")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_inplace_sort(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t")
        write_table("//tmp/t", [{"key": "b"}, {"key": "a"}])

        sort(
            in_="//tmp/t",
            out="//tmp/t",
            sort_by=[{"name": "key", "sort_order": sort_order}]
        )

        assert get("//tmp/t/@sorted")
        expected = [{"key": "a"}, {"key": "b"}]
        if sort_order == "descending":
            expected = expected[::-1]
        assert read_table("//tmp/t") == expected

    @authors("ignat")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_inplace_sort_with_schema(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create(
            "table",
            "//tmp/t",
            attributes={"schema": [{"name": "key", "type": "string"}]},
        )
        write_table("//tmp/t", [{"key": "b"}, {"key": "a"}])

        sort(
            in_="//tmp/t",
            out="//tmp/t",
            sort_by=[{"name": "key", "sort_order": sort_order}]
        )

        assert get("//tmp/t/@sorted")
        assert normalize_schema(get("//tmp/t/@schema")) == make_schema(
            [
                {
                    "name": "key",
                    "type": "string",
                    "required": False,
                    "sort_order": sort_order,
                }
            ],
            strict=True,
            unique_keys=False,
        )
        expected = [{"key": "a"}, {"key": "b"}]
        if sort_order == "descending":
            expected = expected[::-1]
        assert read_table("//tmp/t") == expected

    @authors("psushin")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_auto_schema_inference(self, optimize_for, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        loose_schema = make_schema([{"name": "key", "type": "int64"}], strict=False)
        strict_schema = make_schema([{"name": "key", "type": "int64"}])

        create("table", "//tmp/input_loose", attributes={"schema": loose_schema})
        create("table", "//tmp/input_weak")
        create("table", "//tmp/output_weak", attributes={"optimize_for": optimize_for})
        create(
            "table",
            "//tmp/output_loose",
            attributes={"optimize_for": optimize_for, "schema": loose_schema},
        )
        create(
            "table",
            "//tmp/output_strict",
            attributes={"optimize_for": optimize_for, "schema": strict_schema},
        )

        write_table("<append=true>//tmp/input_loose", {"key": 1, "value": "foo"})
        write_table("<append=true>//tmp/input_weak", {"key": 1, "value": "foo"})

        sort_by = [{"name": "key", "sort_order": sort_order}]

        # input weak
        sort(in_="//tmp/input_weak", out="//tmp/output_loose", sort_by=sort_by)

        assert get("//tmp/output_loose/@schema_mode") == "strong"
        assert get("//tmp/output_loose/@sorted")

        sort(in_="//tmp/input_weak", out="//tmp/output_weak", sort_by=sort_by)

        assert get("//tmp/output_weak/@schema_mode") == "weak"
        assert get("//tmp/output_weak/@sorted")

        with pytest.raises(YtError):
            sort(in_="//tmp/input_weak", out="//tmp/output_strict", sort_by=sort_by)

        # input loose
        sort(in_="//tmp/input_loose", out="//tmp/output_loose", sort_by=sort_by)

        assert get("//tmp/output_loose/@schema_mode") == "strong"
        assert get("//tmp/output_loose/@sorted")

        sort(in_="//tmp/input_loose", out="//tmp/output_weak", sort_by=sort_by)

        assert get("//tmp/output_weak/@schema_mode") == "strong"
        assert get("//tmp/output_weak/@sorted")

        with pytest.raises(YtError):
            sort(in_="//tmp/input_loose", out="//tmp/output_strict", sort_by=sort_by)

    @authors("savrus")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_unique_keys_inference(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        schema_in = make_schema(
            [
                {"name": "key1", "type": "string", "sort_order": sort_order},
                {"name": "key2", "type": "string", "sort_order": sort_order},
                {"name": "key3", "type": "string"},
            ],
            strict=True,
            unique_keys=True,
        )

        create("table", "//tmp/t_in", attributes={"schema": schema_in})
        create("table", "//tmp/t_out")

        row1 = {"key1": "a", "key2": "b", "key3": "c"}
        row2 = {"key1": "b", "key2": "a", "key3": "d"}
        if sort_order == "descending":
            row1, row2 = row2, row1
        write_table("//tmp/t_in", [row1, row2])

        def _do(out_table, sort_by, unique_keys, result):
            sort_by = [{"name": col, "sort_order": sort_order} for col in sort_by]
            sort(
                in_="//tmp/t_in",
                out=out_table,
                sort_by=sort_by,
                spec={"schema_inference_mode": "from_input"},
            )

            assert get(out_table + "/@sorted_by") == [col["name"] for col in sort_by]
            assert get(out_table + "/@schema/@strict")
            assert get(out_table + "/@schema/@unique_keys") == unique_keys
            assert read_table(out_table) == result

        _do("//tmp/t_out", ["key2", "key1"], True, [row2, row1])
        _do("//tmp/t_out", ["key2"], False, [row2, row1])
        _do("//tmp/t_out", ["key3", "key2", "key1"], True, [row1, row2])
        _do("//tmp/t_out", ["key3", "key1"], False, [row1, row2])
        _do("//tmp/t_in", ["key2", "key1"], True, [row2, row1])

    @authors("savrus")
    def test_schema_validation(self):
        create("table", "//tmp/input")
        create(
            "table",
            "//tmp/output",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "key", "type": "int64"},
                        {"name": "value", "type": "string"},
                    ]
                )
            },
        )

        for i in range(10, 0, -2):
            write_table(
                "<append=true>//tmp/input",
                [{"key": i, "value": "foo"}, {"key": i - 1, "value": "foo"}],
            )

        sort(
            in_="//tmp/input",
            out="//tmp/output",
            sort_by="key",
            spec={"schema_inference_mode": "from_output"},
        )

        assert get("//tmp/output/@schema_mode") == "strong"
        assert get("//tmp/output/@schema/@strict")
        assert read_table("//tmp/output") == [{"key": i, "value": "foo"} for i in range(1, 11)]

        write_table("<sorted_by=[key]>//tmp/input", {"key": "1", "value": "foo"})
        assert get("//tmp/input/@sorted_by") == ["key"]

        with pytest.raises(YtError):
            sort(in_="//tmp/input", out="//tmp/output", sort_by="key")

    @authors("ermolovd")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_complex_types_schema_validation(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        input_schema = make_schema(
            [
                {"name": "index", "type_v3": "int64"},
                {"name": "value", "type_v3": optional_type(optional_type("string"))},
            ],
            unique_keys=False,
            strict=True,
        )
        output_schema = make_schema(
            [
                {"name": "index", "type_v3": "int64", "sort_order": sort_order},
                {"name": "value", "type_v3": list_type(optional_type("string"))},
            ],
            unique_keys=False,
            strict=True,
        )

        create("table", "//tmp/input", attributes={"schema": input_schema})
        create("table", "//tmp/output", attributes={"schema": output_schema})

        input_rows = [
            {"index": 1, "value": [None]},
            {"index": 2, "value": ["foo"]},
        ]
        if sort_order == "descending":
            input_rows = input_rows[::-1]

        write_table("//tmp/input", input_rows)

        # We check that yson representation of types are compatible with each other
        write_table("//tmp/output", read_table("//tmp/input"))

        sort_by = [{"name": "index", "sort_order": sort_order}]

        with pytest.raises(YtError):
            sort(
                in_="//tmp/input",
                out="//tmp/output",
                sort_by=sort_by,
                spec={"schema_inference_mode": "auto"},
            )
        sort(
            in_="//tmp/input",
            out="//tmp/output",
            sort_by=sort_by,
            spec={"schema_inference_mode": "from_output"},
        )
        assert normalize_schema_v3(output_schema) == normalize_schema_v3(get("//tmp/output/@schema"))
        sort(
            in_="//tmp/input",
            out="//tmp/output",
            sort_by=sort_by,
            spec={"schema_inference_mode": "from_input"},
        )
        input_sorted_schema = deepcopy(input_schema)
        input_sorted_schema[0]["sort_order"] = sort_order
        assert normalize_schema_v3(input_sorted_schema) == normalize_schema_v3(get("//tmp/output/@schema"))

    @authors("savrus")
    def test_unique_keys_validation(self):
        create("table", "//tmp/input")
        create(
            "table",
            "//tmp/output",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "key", "type": "int64", "sort_order": "ascending"},
                        {"name": "value", "type": "string"},
                    ],
                    unique_keys=True,
                )
            },
        )

        for i in range(10, 0, -2):
            write_table(
                "<append=true>//tmp/input",
                [{"key": i, "value": "foo"}, {"key": i - 1, "value": "foo"}],
            )

        sort(
            in_="//tmp/input",
            out="//tmp/output",
            sort_by="key",
            spec={"schema_inference_mode": "from_output"},
        )

        assert get("//tmp/output/@schema/@strict")
        assert get("//tmp/output/@schema/@unique_keys")
        assert read_table("//tmp/output") == [{"key": i, "value": "foo"} for i in range(1, 11)]

        write_table(
            "<sorted_by=[key]>//tmp/input",
            [{"key": 1, "value": "foo"} for i in range(2)],
        )

        with pytest.raises(YtError):
            sort(
                in_="//tmp/input",
                out="//tmp/output",
                sort_by="key",
                spec={"schema_inference_mode": "from_output", "max_failed_job_count": 1},
            )

        erase("//tmp/input")

        for i in range(2):
            write_table("<append=%true; sorted_by=[key]>//tmp/input", {"key": 1, "value": "foo"})

        with pytest.raises(YtError):
            sort(
                in_="//tmp/input",
                out="//tmp/output",
                sort_by="key",
                spec={"schema_inference_mode": "from_output", "max_failed_job_count": 1},
            )

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("sort_order", [None, "ascending"])
    def test_sort_on_dynamic_table(self, sort_order, optimize_for):
        # TODO(ifsmirnov): drop when 20.3 artifacts are updated to the version
        # with ordered dynamic store reader.
        if sort_order is None and self.Env.get_component_version("ytserver-master").abi <= (20, 3):
            return

        schema = [
            {"name": "key1", "type": "int64", "sort_order": sort_order},
            {"name": "key2", "type": "int64", "sort_order": sort_order},
            {"name": "value", "type": "string"},
        ]

        sync_create_cells(1)
        create_dynamic_table("//tmp/t", schema=schema, optimize_for=optimize_for)

        create("table", "//tmp/t_out")

        rows = [{"key1": None, "key2": i, "value": str(i)} for i in range(2)]
        rows += [{"key1": i, "key2": i, "value": str(i)} for i in range(6)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")

        sort(in_="//tmp/t", out="//tmp/t_out", sort_by=["key1", "key2"])
        assert read_table("//tmp/t_out") == rows

        rows1 = [{"key1": i, "key2": i, "value": str(i + 1)} for i in range(3, 10)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows1)
        sync_unmount_table("//tmp/t")

        def update(new):
            def update_row(row):
                if sort_order == "ascending":
                    for r in rows:
                        if all(r[k] == row[k] for k in ("key1", "key2")):
                            r["value"] = row["value"]
                            return
                rows.append(row)

            for row in new:
                update_row(row)

        update(rows1)

        def verify_sort(sort_by):
            sort(in_="//tmp/t", out="//tmp/t_out", sort_by=sort_by)
            actual = read_table("//tmp/t_out")

            # Equivalent for null which is comparable with YsonInt64 and int.
            comparable_null = yson.YsonInt64(-2**63)

            # Oh Yson
            def fix_null(x):
                return comparable_null if x == yson.YsonEntity() else x

            key = lambda r: [fix_null(r[k]) for k in sort_by]  # noqa
            for i in range(1, len(actual)):
                assert key(actual[i - 1]) <= key(actual[i])

            wide_by = sort_by + [c["name"] for c in schema if c["name"] not in sort_by]
            key = lambda r: [fix_null(r[k]) for k in wide_by]  # noqa
            assert sorted(actual, key=key) == sorted(rows, key=key)

        verify_sort(["key1"])
        verify_sort(["key2"])
        verify_sort(["key2", "key1"])
        verify_sort(["key1", "key2", "value"])
        verify_sort(["value", "key2", "key1"])

    @authors("savrus", "psushin")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_computed_columns(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "k1", "type": "int64", "expression": "k2 * 2"},
                    {"name": "k2", "type": "int64"},
                ]
            },
        )

        write_table("//tmp/t", [{"k2": i} for i in range(2)])
        assert read_table("//tmp/t") == [{"k1": i * 2, "k2": i} for i in range(2)]

        sort_by = [{"name": "k1", "sort_order": sort_order}]
        sort(in_="//tmp/t", out="//tmp/t", sort_by=sort_by)

        assert normalize_schema(get("//tmp/t/@schema")) == make_schema(
            [
                {
                    "name": "k1",
                    "type": "int64",
                    "expression": "k2 * 2",
                    "sort_order": sort_order,
                    "required": False,
                },
                {"name": "k2", "type": "int64", "required": False},
            ],
            unique_keys=False,
            strict=True,
        )

        expected = [{"k1": i * 2, "k2": i} for i in range(2)]
        if sort_order == "descending":
            expected = expected[::-1]
        assert read_table("//tmp/t") == expected

        create("table", "//tmp/t2")
        for i in range(5):
            write_table("//tmp/t2", {"k2": i})

        for schema_inference_mode in ("auto", "from_output"):
            with pytest.raises(YtError):
                # sort table with weak schema into table with computed column
                sort(
                    in_="//tmp/t2",
                    out="//tmp/t",
                    sort_by=sort_order,
                    spec={"schema_inference_mode": schema_inference_mode},
                )

    @authors("ifsmirnov")
    @pytest.mark.parametrize("schema_inference_mode", ["auto", "from_output"])
    def test_computed_column_schema_validation(self, schema_inference_mode):
        schema = [
            {"name": "expr", "type": "int64", "expression": "key * 2"},
            {"name": "key", "type": "int64"},
            {"name": "value", "type": "string"},
        ]
        create("table", "//tmp/output", attributes={"schema": schema})

        # Referenced column is not present.
        create("table", "//tmp/input1", attributes={"schema": schema})
        write_table("//tmp/input1", [{"key": 1, "value": "1"}, {"key": 2, "value": "2"}])

        with pytest.raises(YtError):
            sort(
                in_="//tmp/input1{expr,value}",
                out="//tmp/output",
                sort_by=["expr"],
                spec={"schema_inference_mode": schema_inference_mode},
            )

        # Expression is different.
        schema[0]["expression"] = "key * 3"
        create("table", "//tmp/input2", attributes={"schema": schema})
        write_table("//tmp/input2", [{"key": 1, "value": "1"}, {"key": 2, "value": "2"}])

        with pytest.raises(YtError):
            sort(
                in_="//tmp/input2",
                out="//tmp/output",
                sort_by=["expr"],
                spec={"schema_inference_mode": schema_inference_mode},
            )

    @authors("savrus")
    def test_writer_config(self):
        create("table", "//tmp/t_in")
        create(
            "table",
            "//tmp/t_out",
            attributes={
                "chunk_writer": {"block_size": 1024},
                "compression_codec": "none",
            },
        )

        write_table("//tmp/t_in", [{"value": "A" * 1024} for i in range(10)])

        sort(in_="//tmp/t_in", out="//tmp/t_out", sort_by="value", spec={"job_count": 1})

        chunk_id = get_singular_chunk_id("//tmp/t_out")
        assert get("#" + chunk_id + "/@compressed_data_size") > 1024 * 10
        assert get("#" + chunk_id + "/@max_block_size") < 1024 * 2

    @authors("savrus")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_column_selectors_schema_inference(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "k1", "type": "int64", "sort_order": sort_order},
                        {"name": "k2", "type": "int64", "sort_order": sort_order},
                        {"name": "v1", "type": "int64"},
                        {"name": "v2", "type": "int64"},
                    ],
                    unique_keys=True,
                )
            },
        )
        create("table", "//tmp/t_out")
        rows = [{"k1": i, "k2": i + 1, "v1": i + 2, "v2": i + 3} for i in range(2)]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table("//tmp/t", rows)

        sort(
            in_="//tmp/t{k1,v1}",
            out="//tmp/t_out",
            sort_by=[{"name": "k1", "sort_order": sort_order}]
        )

        assert_items_equal(read_table("//tmp/t_out"), [{k: r[k] for k in ("k1", "v1")} for r in rows])

        schema = make_schema(
            [
                {
                    "name": "k1",
                    "type": "int64",
                    "required": False,
                    "sort_order": sort_order,
                },
                {"name": "v1", "type": "int64", "required": False},
            ],
            unique_keys=False,
            strict=True,
        )

        assert normalize_schema(get("//tmp/t_out/@schema")) == schema

        remove("//tmp/t_out")
        create("table", "//tmp/t_out")

        sort(
            in_="//tmp/t{k1,k2,v2}",
            out="//tmp/t_out",
            sort_by=[
                {"name": "k1", "sort_order": sort_order},
                {"name": "k2", "sort_order": sort_order},
            ],
        )

        assert_items_equal(
            read_table("//tmp/t_out"),
            [{k: r[k] for k in ("k1", "k2", "v2")} for r in rows],
        )

        schema = make_schema(
            [
                {
                    "name": "k1",
                    "type": "int64",
                    "required": False,
                    "sort_order": sort_order,
                },
                {
                    "name": "k2",
                    "type": "int64",
                    "required": False,
                    "sort_order": sort_order,
                },
                {"name": "v2", "type": "int64", "required": False},
            ],
            unique_keys=True,
            strict=True,
        )
        assert normalize_schema(get("//tmp/t_out/@schema")) == schema

    @authors("savrus")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_column_selectors_output_schema_validation(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": sort_order},
                    {"name": "value", "type": "string"},
                ]
            },
        )
        create(
            "table",
            "//tmp/t_out",
            attributes={"schema": [{"name": "key", "type": "int64", "sort_order": sort_order}]},
        )
        rows = [{"key": i, "value": str(i)} for i in range(2)]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table("//tmp/t", rows)

        sort(
            in_="//tmp/t{key}",
            out="//tmp/t_out",
            sort_by=[{"name": "key", "sort_order": sort_order}],
        )

        assert_items_equal(read_table("//tmp/t_out"), [{"key": r["key"]} for r in rows])

    @authors("savrus")
    def test_query_filtering(self):
        create("table", "//tmp/t1", attributes={"schema": [{"name": "a", "type": "int64"}]})
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": i} for i in range(2)])

        with pytest.raises(YtError):
            sort(in_="//tmp/t1", out="//tmp/t2", spec={"input_query": "a where a > 0"})

    @authors("gritukan")
    def test_pivot_keys(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        rows = [{"key": "%02d" % key} for key in range(50)]
        random.shuffle(rows)
        write_table("//tmp/t1", rows)

        sort(
            in_="//tmp/t1",
            out="//tmp/t2",
            sort_by="key",
            spec={"pivot_keys": [["01"], ["43"]]},
        )
        assert_items_equal(read_table("//tmp/t2"), sorted_dicts(rows))
        chunk_ids = get("//tmp/t2/@chunk_ids")
        assert sorted([get("#" + chunk_id + "/@row_count") for chunk_id in chunk_ids]) == [1, 7, 42]

    @authors("gritukan")
    def test_pivot_keys_incorrect_options(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        rows = [{"key": "%02d" % i, "value": i} for i in range(50)]
        write_table("//tmp/t1", rows)

        with pytest.raises(YtError):
            sort(
                in_="//tmp/t1",
                out="//tmp/t2",
                sort_by="key",
                spec={"pivot_keys": [["73"], ["37"]]},
            )

    @authors("gritukan")
    def test_pivot_keys_hierarchical_partitions(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        rows = [{"key": "%02d" % key} for key in range(100)]
        random.shuffle(rows)
        write_table("//tmp/t1", rows)

        op = sort(
            in_="//tmp/t1",
            out="//tmp/t2",
            sort_by="key",
            spec={
                "pivot_keys": [["01"], ["25"], ["52"], ["77"]],
                "max_partition_factor": 2,
            },
        )
        op.track()
        assert_items_equal(read_table("//tmp/t2"), sorted_dicts(rows))
        chunk_ids = get("//tmp/t2/@chunk_ids")
        assert sorted([get("#" + chunk_id + "/@row_count") for chunk_id in chunk_ids]) == [1, 23, 24, 25, 27]
        assert check_operation_tasks(op, {"partition(0)", "partition(1)", "partition(2)", "final_sort"})

    @authors("gritukan")
    def test_pivot_keys_descending(self):
        skip_if_no_descending(self.Env)

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        rows = [{"key": "%02d" % key} for key in range(50)]
        random.shuffle(rows)
        write_table("//tmp/t1", rows)

        sort(
            in_="//tmp/t1",
            out="//tmp/t2",
            sort_by=[{"name": "key", "sort_order": "descending"}],
            spec={"pivot_keys": [["43"], ["01"]]},
        )
        assert_items_equal(read_table("//tmp/t2"), sorted_dicts(rows)[::-1])
        chunk_ids = get("//tmp/t2/@chunk_ids")
        # Partitions are (+oo, 43), [43, 01), [01, -oo).
        assert sorted([get("#" + chunk_id + "/@row_count") for chunk_id in chunk_ids]) == [2, 6, 42]

    @authors("gritukan")
    def test_non_existent_sort_by_column(self):
        create("table", "//tmp/in", attributes={"schema": [{"name": "x", "type": "int64"}]})
        create("table", "//tmp/out")
        write_table("//tmp/in", [{"x": 1}])

        with pytest.raises(YtError):
            sort(in_="//tmp/in", out="//tmp/out", sort_by="foo")

    @authors("gritukan")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_non_strict_schema(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        write_table(
            "<schema=<strict=false>[{name=value;type=string}]>//tmp/in",
            [{"key": 2, "value": "a"}, {"key": 1, "value": "b"}],
        )

        sort(
            in_="//tmp/in",
            out="//tmp/out1",
            sort_by=[{"name": "key", "sort_order": sort_order}],
        )
        expected = [
            {"key": 1, "value": "b"},
            {"key": 2, "value": "a"},
        ]
        if sort_order == "descending":
            expected = expected[::-1]
        assert read_table("//tmp/out1") == expected
        schema = make_schema(
            [
                {
                    "name": "key",
                    "type": "any",
                    "required": False,
                    "sort_order": sort_order,
                },
                {"name": "value", "type": "string", "required": False},
            ],
            unique_keys=False,
            strict=False,
        )
        assert normalize_schema(get("//tmp/out1/@schema")) == schema

        sort(
            in_="//tmp/in",
            out="//tmp/out2",
            sort_by=[{"name": "foo", "sort_order": sort_order}],
        )
        assert sorted_dicts(read_table("//tmp/out1")) == [
            {"key": 1, "value": "b"},
            {"key": 2, "value": "a"},
        ]
        schema = make_schema(
            [
                {
                    "name": "foo",
                    "type": "any",
                    "required": False,
                    "sort_order": sort_order,
                },
                {"name": "value", "type": "string", "required": False},
            ],
            unique_keys=False,
            strict=False,
        )
        assert normalize_schema(get("//tmp/out2/@schema")) == schema

    @authors("gritukan")
    def test_data_flow_graph(self):
        partition_vertex = "partition(0)"

        create("table", "//tmp/in")
        create("table", "//tmp/in2")
        create("table", "//tmp/out")

        n = 47
        write_table("//tmp/in", [{"x": (42 * x) % n} for x in range(n)])
        write_table("//tmp/in2", [{"x": 42} for x in range(n)])

        def count_edges(data_flow_graph):
            edges = data_flow_graph["edges"]
            result = 0
            for source in edges:
                result += len(edges[source])
            return result

        op = simple_sort_1_phase("//tmp/in", "//tmp/out", "x")
        data_flow_graph = get(op.get_path() + "/@progress/data_flow_graph")
        data_weight = data_flow_graph["edges"]["source"]["simple_sort"]["statistics"]["data_weight"]
        assert data_weight > 0
        assert count_edges(data_flow_graph) == 2
        assert data_flow_graph["edges"]["simple_sort"]["sink"]["statistics"]["data_weight"] == data_weight

        op = simple_sort_2_phase("//tmp/in", "//tmp/out", "x")
        data_flow_graph = get(op.get_path() + "/@progress/data_flow_graph")
        data_weight = data_flow_graph["edges"]["source"]["simple_sort"]["statistics"]["data_weight"]
        assert data_weight > 0
        assert count_edges(data_flow_graph) == 3
        assert data_flow_graph["edges"]["simple_sort"]["sorted_merge"]["statistics"]["data_weight"] == data_weight
        assert data_flow_graph["edges"]["sorted_merge"]["sink"]["statistics"]["data_weight"] == data_weight

        op = sort_2_phase("//tmp/in", "//tmp/out", "x")
        data_flow_graph = get(op.get_path() + "/@progress/data_flow_graph")
        data_weight = data_flow_graph["edges"]["source"][partition_vertex]["statistics"]["data_weight"]
        assert data_weight > 0
        assert count_edges(data_flow_graph) == 3
        assert data_flow_graph["edges"][partition_vertex]["final_sort"]["statistics"]["data_weight"] == data_weight
        assert data_flow_graph["edges"]["final_sort"]["sink"]["statistics"]["data_weight"] == data_weight

        op = sort_3_phase("//tmp/in", "//tmp/out", "x")
        data_flow_graph = get(op.get_path() + "/@progress/data_flow_graph")
        data_weight = data_flow_graph["edges"]["source"][partition_vertex]["statistics"]["data_weight"]
        assert data_weight > 0
        assert count_edges(data_flow_graph) == 4
        assert (
            data_flow_graph["edges"][partition_vertex]["intermediate_sort"]["statistics"]["data_weight"] == data_weight
        )
        assert data_flow_graph["edges"]["intermediate_sort"]["sorted_merge"]["statistics"]["data_weight"] == data_weight
        assert data_flow_graph["edges"]["sorted_merge"]["sink"]["statistics"]["data_weight"] == data_weight

        op = sort_maniac("//tmp/in2", "//tmp/out", "x")
        data_flow_graph = get(op.get_path() + "/@progress/data_flow_graph")
        data_weight = data_flow_graph["edges"]["source"][partition_vertex]["statistics"]["data_weight"]
        assert data_weight > 0
        assert count_edges(data_flow_graph) == 3
        assert data_flow_graph["edges"][partition_vertex]["unordered_merge"]["statistics"]["data_weight"] == data_weight
        assert data_flow_graph["edges"]["unordered_merge"]["sink"]["statistics"]["data_weight"] == data_weight

    @authors("max42")
    def test_proper_key_comparison(self):
        # YT-13530.
        create(
            "table",
            "//tmp/in",
            attributes={
                "schema": [
                    {"name": "x", "type": "int64"},
                    {"name": "d", "type": "double"},
                ]
            },
        )
        create("table", "//tmp/out")

        n = 47
        nan = float("nan")
        rows = [{"x": x, "d": nan} for x in ([20, 60] + [42] * (n - 2))]
        write_table("//tmp/in", rows)

        sort_maniac("//tmp/in", "//tmp/out", "x", validate_types=False)
        assert len(read_table("//tmp/out")) == n

    @authors("ermolovd")
    @pytest.mark.timeout(200)
    @pytest.mark.parametrize(
        "sort_func",
        [
            simple_sort_1_phase,
            simple_sort_2_phase,
            sort_2_phase,
            sort_2_phase_depth_2,
            sort_3_phase,
            sort_3_phase_depth_2,
            sort_maniac,
        ],
    )
    def test_sort_nonkey_complex_type(self, sort_func):
        create(
            "table",
            "//tmp/in",
            attributes={
                "schema": [
                    {"name": "key", "type_v3": "int64"},
                    {"name": "value", "type_v3": list_type("int64")},
                ]
            },
        )
        data = [{"key": i % 10, "value": [1] * i} for i in range(100, 1, -1)]
        for d in data:
            write_table("<append=%true>//tmp/in", [d])
        create("table", "//tmp/out")
        sort_func(in_="//tmp/in", out="//tmp/out", sort_by="key")

    def run_test_complex_sort(self, sort_func, type_v3, data, expected_data):
        create(
            "table",
            "//tmp/in",
            attributes={
                "schema": [{"name": "key", "type_v3": type_v3}],
            },
        )
        write_table("//tmp/in", [{"key": d} for d in data])

        create("table", "//tmp/out")
        sort_func("//tmp/in", "//tmp/out", sort_by="key")

        out_data = [r["key"] for r in read_table("//tmp/out")]
        assert out_data == expected_data

    @authors("ermolovd")
    @pytest.mark.parametrize("sort_func", [simple_sort_1_phase, simple_sort_2_phase, sort_2_phase])
    def test_sort_key_complex_type_list(self, sort_func):
        self.run_test_complex_sort(
            sort_func,
            list_type(optional_type("int64")),
            [
                [0, 4, 8],
                [0],
                [1, 2, 3, 5],
                [2, 2, 3, 5],
                [2, None, 3, 5],
                [5],
                [5],
                [5],
                [5],
                [5],
                [5],
            ],
            [
                [0],
                [0, 4, 8],
                [1, 2, 3, 5],
                [2, None, 3, 5],
                [2, 2, 3, 5],
                [5],
                [5],
                [5],
                [5],
                [5],
                [5],
            ],
        )

    @authors("ermolovd")
    @pytest.mark.timeout(150)
    @pytest.mark.parametrize(
        "sort_func",
        [sort_maniac, sort_2_phase_depth_2, sort_3_phase_depth_2, sort_3_phase],
    )
    def test_sort_key_complex_type_large_input(self, sort_func):
        create(
            "table",
            "//tmp/in",
            attributes={
                "schema": [{"name": "key", "type_v3": list_type("int64")}],
            },
        )

        data = [{"key": [i % 10]} for i in range(100, 1, -1)]
        for d in data:
            write_table("<append=%true>//tmp/in", [d])

        create("table", "//tmp/out")
        sort_func("//tmp/in", "//tmp/out", sort_by="key")

    @authors("gritukan")
    def test_simple_sort_with_many_data_slices(self):
        # N here is max_data_slices_per_job + 1.
        N = 101
        p = [x for x in range(N)]
        random.shuffle(p)
        create("table", "//tmp/t_in")
        for x in p:
            write_table("<append=%true>//tmp/t_in", [{"x": x}])
        create("table", "//tmp/t_out")

        # Despite of large data weight per sort job we can't create one
        # sort job, since number of data slices is too large.
        op = sort(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by="x",
            spec={
                "partition_count": 1,
                "data_weight_per_sort_job": 10 ** 8,
            },
        )
        op.track()
        assert builtins.set(get_operation_job_types(op.id)) == {
            "simple_sort",
            "sorted_merge",
        }

        assert read_table("//tmp/t_out") == [{"x": x} for x in range(N)]
        assert get("//tmp/t_out/@chunk_count") == 1

    @authors("babenko")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("schemaful", [False, True])
    def test_simple_sort_any_values(self, optimize_for, schemaful):
        N = 100
        p = [x for x in range(N)]
        random.shuffle(p)
        attributes = {
            "optimize_for": optimize_for,
        }
        if schemaful:
            attributes["schema"] = [{"name": "x", "type": "any"}]
        create("table", "//tmp/t_in", attributes=attributes)
        write_table("//tmp/t_in", [{"x": x} for x in p])
        create("table", "//tmp/t_out")

        sort(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by="x",
        )

        assert read_table("//tmp/t_out") == [{"x": x} for x in range(N)]

    @authors("gritukan")
    @pytest.mark.timeout(300)
    def test_hierarchical_partitions(self):
        p = [x for x in range(100)]
        random.shuffle(p)
        create("table", "//tmp/t_in")
        for x in p:
            write_table("<append=%true>//tmp/t_in", [{"x": x, "y": "A" * (10 ** 4)}])
        create("table", "//tmp/t_out")

        def get_directions(op):
            data_flow = get(op.get_path() + "/@progress/data_flow")
            directions = {}
            for direction in data_flow:
                directions[(direction["source_name"], direction["target_name"])] = direction
            return directions

        for mode in ["3-phase", "2-phase"]:
            for partition_count, max_partition_factor, partition_tree_depth in [
                [4, 2, 2],
            ]:
                op = sort(
                    in_="//tmp/t_in",
                    out="//tmp/t_out",
                    sort_by="x",
                    spec={
                        "partition_count": partition_count,
                        "max_partition_factor": max_partition_factor,
                        "data_weight_per_sort_job": 1 if mode == "3-phase" else 10 ** 8,
                        "partition_job_io": {
                            "table_writer": {
                                "desired_chunk_size": 1,
                                "block_size": 1024,
                            }
                        },
                    },
                )
                op.track()

                expected_vertices = ["input"] + ["partition({})".format(x) for x in range(partition_tree_depth)]

                if mode == "2-phase":
                    expected_vertices += ["final_sort", "output"]
                else:
                    expected_vertices += ["intermediate_sort", "sorted_merge", "output"]

                directions = get_directions(op)
                assert len(directions) == len(expected_vertices) - 1
                for i in range(len(expected_vertices) - 1):
                    from_, to = expected_vertices[i], expected_vertices[i + 1]
                    job_row_count = 0 if from_ == "input" else 100
                    teleport_row_count = 100 if from_ == "input" else 0
                    assert directions[(from_, to)]["job_data_statistics"]["row_count"] == job_row_count
                    assert directions[(from_, to)]["teleport_data_statistics"]["row_count"] == teleport_row_count

                assert read_table("//tmp/t_out", verbose=False) == [{"x": x, "y": "A" * (10 ** 4)} for x in range(100)]
                if mode == "2-phase":
                    assert get("//tmp/t_out/@chunk_count") == partition_count

        for partition_count, max_partition_factor, partition_tree_depth in [
            [7, 2, 3],
        ]:
            if exists("//tmp/t_in"):
                remove("//tmp/t_in")
            create("table", "//tmp/t_in")

            p = [x for x in range(partition_count - 2)]
            random.shuffle(p)
            for x in p:
                for _ in range(10):
                    write_table("<append=%true>//tmp/t_in", [{"x": x}])

            op = sort(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                sort_by="x",
                spec={
                    "partition_count": partition_count,
                    "max_partition_factor": max_partition_factor,
                },
            )
            op.track()

            row_count = 10 * (partition_count - 2)

            directions = get_directions(op)
            assert directions[("input", "partition(0)")]["job_data_statistics"]["row_count"] == 0
            assert directions[("input", "partition(0)")]["teleport_data_statistics"]["row_count"] == row_count
            assert directions[("partition(0)", "partition(1)")]["job_data_statistics"]["row_count"] == row_count
            assert directions[("partition(0)", "partition(1)")]["teleport_data_statistics"]["row_count"] == 0
            assert directions[("partition(1)", "partition(2)")]["job_data_statistics"]["row_count"] == row_count
            assert directions[("partition(1)", "partition(2)")]["teleport_data_statistics"]["row_count"] == 0
            unordered_merge_rows = directions[("partition(2)", "unordered_merge")]["job_data_statistics"]["row_count"]
            assert unordered_merge_rows > 0
            assert directions[("partition(2)", "unordered_merge")]["teleport_data_statistics"]["row_count"] == 0
            final_sort_rows = row_count - unordered_merge_rows
            assert directions[("partition(2)", "final_sort")]["job_data_statistics"]["row_count"] == final_sort_rows
            assert directions[("partition(2)", "final_sort")]["teleport_data_statistics"]["row_count"] == 0
            assert directions[("unordered_merge", "output")]["job_data_statistics"]["row_count"] == unordered_merge_rows
            assert directions[("unordered_merge", "output")]["teleport_data_statistics"]["row_count"] == 0
            assert directions[("final_sort", "output")]["job_data_statistics"]["row_count"] == final_sort_rows
            assert directions[("final_sort", "output")]["teleport_data_statistics"]["row_count"] == 0

            expected = []
            for i in range(partition_count - 2):
                expected += [{"x": i}] * 10
            assert read_table("//tmp/t_out") == expected

    @authors("gritukan")
    @pytest.mark.parametrize("sort_type", [
        "simple_sort_1_phase",
        "simple_sort_2_phase",
        "2_phase",
        "2_phase_hierarchical",
        "3_phase",
        "3_phase_hierarchical"])
    def test_descending_sort_order(self, sort_type):
        skip_if_no_descending(self.Env)

        rows = [{"x": str(x), "y": str(y), "z": "A" * 1000} for x in range(7, 0, -1) for y in range(8)]
        expected = deepcopy(rows)
        random.shuffle(rows)

        create("table", "//tmp/in")
        for row in rows:
            write_table("<append=%true>//tmp/in", [row])

        create("table", "//tmp/out")

        sort_by = [
            {"name": "x", "sort_order": "descending"},
            {"name": "y", "sort_order": "ascending"},
        ]

        if sort_type == "simple_sort_1_phase":
            op = sort(in_="//tmp/in", out="//tmp/out", sort_by=sort_by)
            op.track()
            assert check_operation_tasks(op, ["simple_sort"])
        elif sort_type == "simple_sort_2_phase":
            self.skip_if_legacy_sorted_pool()
            op = sort(in_="//tmp/in", out="//tmp/out", sort_by=sort_by, spec={
                "data_weight_per_sort_job": 5000,
            })
            op.track()
            assert check_operation_tasks(op, ["simple_sort", "sorted_merge"])
        elif sort_type == "2_phase":
            self.skip_if_legacy_sorted_pool()
            op = sort(in_="//tmp/in", out="//tmp/out", sort_by=sort_by, spec={
                "partition_count": 3,
                "data_weight_per_sort_job": 10 ** 8,
            })
            op.track()
            assert check_operation_tasks(op, ["partition(0)", "final_sort"])
        elif sort_type == "2_phase_hierarchical":
            op = sort(in_="//tmp/in", out="//tmp/out", sort_by=sort_by, spec={
                "partition_count": 3,
                "max_partition_factor": 2,
                "data_weight_per_sort_job": 10 ** 8,
            })
            op.track()
            assert check_operation_tasks(op, ["partition(0)", "partition(1)", "final_sort"])
        elif sort_type == "3_phase":
            self.skip_if_legacy_sorted_pool()
            op = sort(in_="//tmp/in", out="//tmp/out", sort_by=sort_by, spec={
                "partition_count": 2,
                "data_weight_per_sort_job": 5000,
                "partition_job_io": {
                    "table_writer": {
                        "desired_chunk_size": 1,
                        "block_size": 1024,
                    }
                },
            })
            op.track()
            assert check_operation_tasks(op, ["partition(0)", "intermediate_sort", "sorted_merge"])
        elif sort_type == "3_phase_hierarchical":
            self.skip_if_legacy_sorted_pool()
            op = sort(in_="//tmp/in", out="//tmp/out", sort_by=sort_by, spec={
                "partition_count": 3,
                "max_partition_factor": 2,
                "data_weight_per_sort_job": 5000,
                "partition_job_io": {
                    "table_writer": {
                        "desired_chunk_size": 1,
                        "block_size": 1024,
                    }
                },
            })
            op.track()
            assert check_operation_tasks(op, ["partition(0)", "partition(1)", "intermediate_sort", "sorted_merge"])

        output_schema = make_schema(
            [
                {"name": "x", "sort_order": "descending", "type": "any", "required": False},
                {"name": "y", "sort_order": "ascending", "type": "any", "required": False},
            ],
            unique_keys=False,
            strict=False,
        )

        assert normalize_schema(output_schema) == normalize_schema(get("//tmp/out/@schema"))
        assert read_table("//tmp/out") == expected

    @authors("alexkolodezny")
    def test_chunk_reader_timing_statistics(self):
        if self.Env.get_component_version("ytserver-job-proxy").abi < (21, 3):
            pytest.skip()
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        write_table("//tmp/t_in", [{"x": i, "y": "A" * 10000} for i in range(100)])

        op = sort(in_="//tmp/t_in", out="//tmp/t_out", sort_by="x")

        wait(lambda: assert_statistics(
            op,
            key="chunk_reader_statistics.wait_time",
            assertion=lambda wait_time: wait_time > 0,
            job_type="simple_sort"))

        wait(lambda: assert_statistics(
            op,
            key="chunk_reader_statistics.idle_time",
            assertion=lambda idle_time: idle_time > 0,
            job_type="simple_sort"))

    @authors("max42")
    def test_column_selector_contradicting_sort_columns(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        row_count = 23
        for i in range(row_count):
            write_table("<append=%true>//tmp/t_in", [{"x": (i * 7) % row_count, "z": "A" * 65536}], verbose=False)

        with raises_yt_error('Sort column "y" is discarded by input column selectors'):
            sort(in_="//tmp/t_in{x}", out="//tmp/t_out", sort_by=["y"], spec={
                "partition_count": 10,
                "data_weight_per_sort_job": 10 ** 8,
                "schema_inference_mode": "auto",
            })


##################################################################


class TestSchedulerSortCommandsMulticell(TestSchedulerSortCommands):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestSchedulerSortCommandsNewSortedPool(TestSchedulerSortCommands):
    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "sort_operation_options": {
                "min_uncompressed_block_size": 1,
                "min_partition_size": 1,
                "max_value_count_per_simple_sort_job": 100,
                "max_data_slices_per_job": 100,
                "spec_template": {
                    "use_new_sorted_pool": True,
                },
            },
        }
    }
