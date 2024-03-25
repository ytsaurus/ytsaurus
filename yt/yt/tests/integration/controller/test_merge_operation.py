from yt_env_setup import YTEnvSetup, parametrize_external

from yt_commands import (
    authors, wait, create, ls, get, set, copy,
    remove, exists, sorted_dicts,
    start_transaction, abort_transaction, insert_rows, trim_rows, read_table, write_table, merge, sort, interrupt_job,
    sync_create_cells, sync_mount_table, sync_unmount_table, sync_freeze_table, sync_unfreeze_table,
    get_singular_chunk_id, get_chunk_replication_factor,
    create_dynamic_table, get_driver, alter_table)

from yt_type_helpers import (
    make_schema, normalize_schema, normalize_schema_v3, optional_type, list_type)

from yt_helpers import skip_if_no_descending, skip_if_renaming_disabled

from yt.environment.helpers import assert_items_equal
from yt.common import YtError
import yt.yson as yson

import pytest

from time import sleep

##################################################################


class TestSchedulerMergeCommands(YTEnvSetup):
    NUM_TEST_PARTITIONS = 6

    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_allocations_update_period": 10,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 10,
            "max_chunks_per_fetch": 10,
            "sorted_merge_operation_options": {
                "job_splitter": {
                    "min_job_time": 3000,
                    "min_total_data_size": 1024,
                    "update_period": 100,
                    "candidate_percentile": 0.8,
                    "max_jobs_per_split": 3,
                },
                "spec_template": {
                    "use_new_sorted_pool": False,
                },
            },
            "ordered_merge_operation_options": {
                "job_splitter": {
                    "min_job_time": 3000,
                    "min_total_data_size": 1024,
                    "update_period": 100,
                    "candidate_percentile": 0.8,
                    "max_jobs_per_split": 3,
                },
            },
        }
    }

    def skip_if_legacy_sorted_pool(self):
        if not isinstance(self, TestSchedulerMergeCommandsNewSortedPool):
            pytest.skip("This test requires new sorted pool")

    def _prepare_tables(self):
        t1 = "//tmp/t1"
        create("table", t1)
        v1 = [{"key" + str(i): "value" + str(i)} for i in range(3)]
        for v in v1:
            write_table("<append=true>" + t1, v)

        t2 = "//tmp/t2"
        create("table", t2)
        v2 = [{"another_key" + str(i): "another_value" + str(i)} for i in range(4)]
        for v in v2:
            write_table("<append=true>" + t2, v)

        self.t1 = t1
        self.t2 = t2

        self.v1 = v1
        self.v2 = v2

        create("table", "//tmp/t_out")

    def _create_simple_dynamic_table(self, path, **attributes):
        if "schema" not in attributes:
            attributes.update(
                {
                    "schema": [
                        {"name": "key", "type": "int64", "sort_order": "ascending"},
                        {"name": "value", "type": "string"},
                    ]
                }
            )
        create_dynamic_table(path, **attributes)

    # usual cases
    @authors("panin", "ignat")
    def test_unordered(self):
        self._prepare_tables()

        merge(mode="unordered", in_=[self.t1, self.t2], out="//tmp/t_out")

        assert_items_equal(read_table("//tmp/t_out"), self.v1 + self.v2)
        assert get("//tmp/t_out/@chunk_count") == 7

    @authors("panin", "ignat")
    def test_unordered_combine(self):
        self._prepare_tables()

        merge(
            combine_chunks=True,
            mode="unordered",
            in_=[self.t1, self.t2],
            out="//tmp/t_out",
        )

        assert_items_equal(read_table("//tmp/t_out"), self.v1 + self.v2)
        assert get("//tmp/t_out/@chunk_count") == 1

    @authors("klyachin")
    def test_unordered_with_mixed_chunks(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")

        write_table("//tmp/t1", [{"a": 4}, {"a": 5}, {"a": 6}])
        write_table("//tmp/t2", [{"a": 7}, {"a": 8}, {"a": 9}])
        write_table("//tmp/t3", [{"a": 1}, {"a": 2}, {"a": 3}])

        create("table", "//tmp/t_out")
        merge(
            mode="unordered",
            in_=["//tmp/t1", "//tmp/t2[:#2]", "//tmp/t3[#1:]"],
            out="//tmp/t_out",
            spec={"data_size_per_job": 1000},
        )

        assert get("//tmp/t_out/@row_count") == 7
        assert get("//tmp/t_out/@chunk_count") == 2
        assert sorted_dicts(read_table("//tmp/t_out")) == [{"a": i} for i in range(2, 9)]

    @authors("dakovalkov")
    @pytest.mark.parametrize("merge_mode", ["unordered", "ordered", "sorted"])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_rename_columns(self, merge_mode, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        if sort_order == "descending" and merge_mode != "sorted":
            pytest.skip("Descending sort order is interesting only with sorted merge")

        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "a", "type": "int64", "sort_order": sort_order}]},
        )
        create(
            "table",
            "//tmp/t2",
            attributes={"schema": [{"name": "a2", "type": "int64", "sort_order": sort_order}]},
        )

        rows = [{"a": 1}, {"a": 2}]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table("//tmp/t1", rows)
        rows = [{"a2": 3}, {"a2": 4}]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table("//tmp/t2", rows)

        create("table", "//tmp/t_out")
        merge(
            mode=merge_mode,
            in_=["//tmp/t1", "<rename_columns={a2=a}>//tmp/t2"],
            out="//tmp/t_out",
        )

        assert sorted_dicts(read_table("//tmp/t_out")) == [
            {"a": 1},
            {"a": 2},
            {"a": 3},
            {"a": 4},
        ]

    @authors("levysotsky")
    @pytest.mark.parametrize("merge_mode", ["unordered", "ordered", "sorted"])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_rename_columns_alter_table(self, merge_mode, sort_order):
        skip_if_renaming_disabled(self.Env)
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()
        if sort_order == "descending" and merge_mode != "sorted":
            pytest.skip("Descending sort order is interesting only with sorted merge")

        order_slice = slice(None) if sort_order == "ascending" else slice(None, None, -1)

        table1 = "//tmp/t1"
        table2 = "//tmp/t2"
        output = "//tmp/tout"

        schema1_original = [{"name": "a", "type": "int64", "sort_order": sort_order}]
        schema1_new = [{"name": "a_new", "stable_name": "a", "type": "int64", "sort_order": sort_order}]
        create("table", table1, attributes={"schema": schema1_original})

        schema2 = [{"name": "a2", "type": "int64", "sort_order": sort_order}]
        create("table", table2, attributes={"schema": schema2})

        def rename(rows, schema):
            result = []
            for row in rows:
                renamed_row = {}
                for column in schema:
                    name = column["name"]
                    stable_name = column.get("stable_name", name)
                    if stable_name in row:
                        renamed_row[name] = row[stable_name]
                result.append(renamed_row)
            return result

        rows1 = [{"a": 1}, {"a": 2}][order_slice]

        write_table(table1, rows1[:1])
        alter_table(table1, schema=schema1_new)
        write_table("<append=%true>" + table1, rename(rows1[1:], schema1_new))

        rows = [{"a2": 3}, {"a2": 4}][order_slice]
        write_table(table2, rows)

        create("table", output)
        merge(
            mode=merge_mode,
            in_=["<rename_columns={a_new=a3}>" + table1, "<rename_columns={a2=a3}>" + table2],
            out=output,
        )

        assert sorted(read_table(output), key=lambda r: r["a3"]) == [
            {"a3": 1},
            {"a3": 2},
            {"a3": 3},
            {"a3": 4},
        ]

    @authors("levysotsky")
    @pytest.mark.parametrize("merge_mode", ["unordered", "ordered", "sorted"])
    def test_rename_columns_stable_names(self, merge_mode):
        table1 = "//tmp/t1"
        table2 = "//tmp/t2"
        output = "//tmp/tout"

        sort_order = "ascending"

        schema1_original = [{"name": "a1", "stable_name": "a2", "type": "int64", "sort_order": sort_order}]
        create("table", table1, attributes={"schema": schema1_original})
        write_table(table1, [{"a1": 1}, {"a1": 2}])

        schema2 = [{"name": "b1", "stable_name": "b2", "type": "int64", "sort_order": sort_order}]
        create("table", table2, attributes={"schema": schema2})
        write_table(table2, [{"b1": 3}, {"b1": 4}])

        create("table", output)
        merge(
            mode=merge_mode,
            in_=["<rename_columns={a1=c}>" + table1, "<rename_columns={b1=c}>" + table2],
            out=output,
        )

        assert sorted(read_table(output), key=lambda r: r["c"]) == [
            {"c": 1},
            {"c": 2},
            {"c": 3},
            {"c": 4},
        ]

    @authors("panin", "ignat")
    def test_ordered(self):
        self._prepare_tables()

        merge(mode="ordered", in_=[self.t1, self.t2], out="//tmp/t_out")

        assert read_table("//tmp/t_out") == self.v1 + self.v2
        assert get("//tmp/t_out/@chunk_count") == 7

    @authors("panin", "ignat")
    def test_ordered_combine(self):
        self._prepare_tables()

        merge(
            combine_chunks=True,
            mode="ordered",
            in_=[self.t1, self.t2],
            out="//tmp/t_out",
        )

        assert read_table("//tmp/t_out") == self.v1 + self.v2
        assert get("//tmp/t_out/@chunk_count") == 1

    @authors("achulkov2")
    def test_ordered_batch_row_count(self):
        # TODO(achulkov2): Lower/remove after cherry-picks.
        if self.Env.get_component_version("ytserver-controller-agent").abi <= (24, 1):
            pytest.skip()

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        batch_row_count = 33

        input_rows = []
        input_rows += [[{"value": "smol"} for i in range(3)] for i in range(15)]
        input_rows += [[{"value": "bighumongouslargefatenormousobeseoverweightoverflowing"} for i in range(11)] for i in range(43)]
        input_rows += [[{"value": "w.e.f.a.latfnltih.tecwtfna"} for i in range(5)] for i in range(2)]

        for row_batch in input_rows:
            write_table("<append=true>//tmp/t_in", row_batch)

        merge(
            combine_chunks=False,
            mode="ordered",
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            spec={"data_size_per_job": 26, "batch_row_count": batch_row_count}
        )

        assert read_table("//tmp/t_out") == sum(input_rows, start=[])

        out_chunk_ids = get("//tmp/t_out/@chunk_ids")
        for out_chunk in out_chunk_ids:
            assert get(f"#{out_chunk}/@row_count") % batch_row_count == 0

    @authors("achulkov2")
    def test_ordered_batch_row_count_disables_teleports_and_sampling(self):
        # TODO(achulkov2): Lower/remove after cherry-picks.
        if self.Env.get_component_version("ytserver-controller-agent").abi <= (24, 1):
            pytest.skip()

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        chunk_count = 10

        rows = [{"value": i} for i in range(3)]
        for i in range(chunk_count):
            write_table("<append=true>//tmp/t_in", rows)

        with pytest.raises(YtError):
            merge(
                combine_chunks=False,
                mode="ordered",
                in_=["//tmp/t_in"],
                out="//tmp/t_out",
                spec={"data_size_per_job": 1, "batch_row_count": 3, "sampling": {"sampling_rate": 0.2}}
            )

        merge(
            combine_chunks=False,
            mode="ordered",
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            spec={"data_size_per_job": 1, "batch_row_count": 3}
        )

        assert read_table("//tmp/t_out") == rows * chunk_count

        in_chunk_ids = get("//tmp/t_in/@chunk_ids")
        out_chunk_ids = get("//tmp/t_out/@chunk_ids")

        assert len(in_chunk_ids) == len(out_chunk_ids)
        for in_chunk, out_chunk in zip(in_chunk_ids, out_chunk_ids):
            # Rows should be the same, but none of the chunks should have been teleported!
            assert get(f"#{in_chunk}/@row_count") == get(f"#{out_chunk}/@row_count")
            assert in_chunk != out_chunk

    @authors("ignat")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    @pytest.mark.parametrize("combine_chunks", [False, True])
    def test_sorted(self, sort_order, combine_chunks):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        rows = [{"a": 1}, {"a": 10}, {"a": 100}]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table("//tmp/t1", rows, sorted_by=[{"name": "a", "sort_order": sort_order}])
        rows = [{"a": 2}, {"a": 3}, {"a": 15}]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table("//tmp/t2", rows, sorted_by=[{"name": "a", "sort_order": sort_order}])

        create("table", "//tmp/t_out")
        merge(
            combine_chunks=combine_chunks,
            mode="sorted",
            in_=["//tmp/t1", "//tmp/t2"],
            out="//tmp/t_out")

        expected = [
            {"a": 1},
            {"a": 2},
            {"a": 3},
            {"a": 10},
            {"a": 15},
            {"a": 100},
        ]
        if sort_order == "descending":
            expected = expected[::-1]
        assert read_table("//tmp/t_out") == expected
        assert (
            get("//tmp/t_out/@chunk_count") == 1
        )  # resulting number of chunks is always equal to 1 (as long as they are small)
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["a"]

    @authors("dakovalkov")
    def test_sorted_different_types(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "key", "type": "int64", "sort_order": "ascending"}]},
        )
        create(
            "table",
            "//tmp/t2",
            attributes={"schema": [{"name": "key", "type": "string", "sort_order": "ascending"}]},
        )
        create("table", "//tmp/out")

        write_table("//tmp/t1", [{"key": 1}])
        write_table("//tmp/t2", [{"key": "1"}])

        with pytest.raises(YtError):
            merge(mode="sorted", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/out")

    @authors("gritukan")
    def test_sorted_different_directions(self):
        skip_if_no_descending(self.Env)
        self.skip_if_legacy_sorted_pool()

        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "key", "type": "int64", "sort_order": "ascending"}]},
        )
        create(
            "table",
            "//tmp/t2",
            attributes={"schema": [{"name": "key", "type": "int64", "sort_order": "descending"}]},
        )
        create("table", "//tmp/out")

        write_table("//tmp/t1", [{"key": 1}])
        write_table("//tmp/t2", [{"key": 1}])

        with pytest.raises(YtError):
            merge(mode="sorted", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/out")

    @authors("psushin")
    def test_sorted_column_filter(self):
        create("table", "//tmp/t")
        write_table(
            "//tmp/t",
            [{"a": 1, "b": 3}, {"a": 10, "b": 2}, {"a": 100, "b": 1}],
            sorted_by="a",
        )

        create("table", "//tmp/t_out")
        with pytest.raises(YtError):
            merge(mode="sorted", in_=["<columns=[b]>//tmp/t"], out="//tmp/t_out")

    @authors("klyachin")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sorted_merge_result_is_sorted(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        create("table", "//tmp/t1")

        count = 100

        chunks = []
        chunks.append([
            {
                "key": "%05d" % (i if i < count / 2 else count / 2),
                "value": "%05d" % i,
            }
            for i in range(count)
        ])
        chunks.append([{"key": "%05d" % (count / 2), "value": "%05d" % (i + count)} for i in range(count)])
        chunks.append([{"key": "%05d" % (count / 2), "value": "%05d" % (i + 2 * count)} for i in range(count)])
        chunks.append([
            {
                "key": "%05d" % (count / 2 if i < count / 2 else i),
                "value": "%05d" % (i + 3 * count),
            }
            for i in range(count)
        ])

        if sort_order == "descending":
            chunks = chunks[::-1]
        for chunk in chunks:
            if sort_order == "descending":
                chunk = chunk[::-1]
            write_table(
                "<append=%true>//tmp/t1",
                chunk,
                sorted_by=[{"name": "key", "sort_order": sort_order}],
                table_writer={"block_size": 1024},
            )

        create("table", "//tmp/t_out")
        merge(
            mode="sorted",
            in_=["//tmp/t1"],
            out="//tmp/t_out",
            spec={"data_size_per_job": 100},
        )

        result = read_table("//tmp/t_out")
        assert len(result) == 4 * count
        for i in range(len(result) - 1):
            if sort_order == "ascending":
                assert result[i]["key"] <= result[i + 1]["key"]
            else:
                assert result[i]["key"] >= result[i + 1]["key"]

    @authors("psushin", "ignat")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sorted_trivial(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        create("table", "//tmp/t1")

        rows = [{"a": 1}, {"a": 10}, {"a": 100}]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table("//tmp/t1", rows, sorted_by=[{"name": "a", "sort_order": sort_order}])

        create("table", "//tmp/t_out")
        merge(combine_chunks=True, mode="sorted", in_=["//tmp/t1"], out="//tmp/t_out")

        assert read_table("//tmp/t_out") == rows
        assert (
            get("//tmp/t_out/@chunk_count") == 1
        )  # resulting number of chunks is always equal to 1 (as long as they are small)
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["a"]

    @authors("monster")
    def test_append_not_sorted(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_out", [{"a": 1}, {"a": 2}, {"a": 3}], sorted_by="a")
        write_table("//tmp/t_in", [{"a": 0}])

        merge(mode="unordered", in_=["//tmp/t_in"], out="<append=true>//tmp/t_out")

        assert not get("//tmp/t_out/@sorted")

    @authors("ignat")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sorted_with_same_chunks(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        t1 = "//tmp/t1"
        t2 = "//tmp/t2"
        v = [{"key1": "value1"}]

        create("table", t1)
        write_table(t1, v[0])
        sort(in_=t1, out=t1, sort_by=[{"name": "key1", "sort_order": sort_order}])
        copy(t1, t2)

        create("table", "//tmp/t_out")
        merge(mode="sorted", in_=[t1, t2], out="//tmp/t_out")
        assert_items_equal(read_table("//tmp/t_out"), v + v)

        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["key1"]

    @authors("dakovalkov")
    def test_sorted_row_count_limit(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")

        write_table("//tmp/t1", [{"k": "a", "s": 0}, {"k": "b", "s": 1}], sorted_by=["k", "s"])
        write_table("//tmp/t2", [{"k": "b", "s": 2}, {"k": "c", "s": 0}], sorted_by=["k", "s"])
        write_table("//tmp/t3", [{"k": "b", "s": 1}, {"k": "b", "s": 2}], sorted_by=["k", "s"])

        create("table", "//tmp/out")
        merge(
            mode="sorted",
            in_=["//tmp/t1", "//tmp/t2", "//tmp/t3"],
            out="<row_count_limit=2>//tmp/out",
            merge_by=["k", "s"],
        )

        assert read_table("//tmp/out") == [
            {"k": "a", "s": 0},
            {"k": "b", "s": 1},
            {"k": "b", "s": 1},
            {"k": "b", "s": 2},
            {"k": "b", "s": 2},
            {"k": "c", "s": 0},
        ]

    @authors("dakovalkov")
    def test_sorted_row_count_limit_2(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")

        write_table("//tmp/t1", [{"k": "a", "s": 0}, {"k": "b", "s": 1}], sorted_by=["k", "s"])
        write_table("//tmp/t2", [{"k": "b", "s": 3}, {"k": "c", "s": 0}], sorted_by=["k", "s"])
        write_table("//tmp/t3", [{"k": "b", "s": 2}, {"k": "b", "s": 4}], sorted_by=["k", "s"])

        create("table", "//tmp/out")
        merge(
            mode="sorted",
            in_=["//tmp/t1", "//tmp/t2", "//tmp/t3"],
            out="<row_count_limit=2>//tmp/out",
            merge_by=["k", "s"],
        )

        assert read_table("//tmp/out") == [{"k": "a", "s": 0}, {"k": "b", "s": 1}]

    @authors("ignat")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sorted_teleport(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")

        sorted_by = [
            {"name": "k", "sort_order": sort_order},
            {"name": "s", "sort_order": sort_order},
        ]
        rows = [{"k": "a", "s": 0}, {"k": "b", "s": 1}]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table("//tmp/t1", rows, sorted_by=sorted_by)
        rows = [{"k": "b", "s": 2}, {"k": "c", "s": 0}]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table("//tmp/t2", rows, sorted_by=sorted_by)
        rows = [{"k": "b", "s": 0}, {"k": "b", "s": 3}]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table("//tmp/t3", rows, sorted_by=sorted_by)

        create("table", "//tmp/t_out")
        if sort_order == "ascending":
            merge(
                mode="sorted",
                in_=["//tmp/t1", "//tmp/t2", "//tmp/t3", "//tmp/t2[(b, 3) : (b, 7)]"],
                out="//tmp/t_out",
                merge_by="k",
            )
        else:
            merge(
                mode="sorted",
                in_=["//tmp/t1", "//tmp/t2", "//tmp/t3"],
                out="//tmp/t_out",
                merge_by=[{"name": "k", "sort_order": "descending"}]
            )

        res = read_table("//tmp/t_out")
        expected = [
            {"k": "a", "s": 0},
            {"k": "b", "s": 1},
            {"k": "b", "s": 0},
            {"k": "b", "s": 3},
            {"k": "b", "s": 2},
            {"k": "c", "s": 0},
        ]
        if sort_order == "descending":
            expected = expected[::-1]

        assert_items_equal(res, expected)

        assert get("//tmp/t_out/@chunk_count") == 3
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["k"]

        merge(
            mode="sorted",
            in_=["//tmp/t1", "//tmp/t2", "//tmp/t3"],
            out="//tmp/t_out",
            merge_by=[{"name": "k", "sort_order": sort_order}],
        )

        res = read_table("//tmp/t_out")
        assert_items_equal(res, expected)

        assert get("//tmp/t_out/@chunk_count") == 3
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["k"]

        merge(
            mode="sorted",
            in_=["//tmp/t1", "//tmp/t2", "//tmp/t3"],
            out="//tmp/t_out",
            merge_by=[
                {"name": "k", "sort_order": sort_order},
                {"name": "s", "sort_order": sort_order}
            ],
        )

        res = read_table("//tmp/t_out")
        expected = [
            {"k": "a", "s": 0},
            {"k": "b", "s": 0},
            {"k": "b", "s": 1},
            {"k": "b", "s": 2},
            {"k": "b", "s": 3},
            {"k": "c", "s": 0},
        ]
        if sort_order == "descending":
            expected = expected[::-1]

        for i, j in zip(res, expected):
            assert i == j

        assert get("//tmp/t_out/@chunk_count") == 1
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["k", "s"]

    @authors("ignat")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sorted_with_maniacs(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")

        def write(table, rows):
            if sort_order == "descending":
                rows = rows[::-1]
            write_table(table, rows, sorted_by=[{"name": "a", "sort_order": sort_order}])

        write("//tmp/t1", [{"a": 3}, {"a": 3}, {"a": 3}])
        write("//tmp/t2", [{"a": 2}, {"a": 3}, {"a": 15}])
        write("//tmp/t3", [{"a": 1}, {"a": 3}])

        create("table", "//tmp/t_out")
        merge(
            combine_chunks=True,
            mode="sorted",
            in_=["//tmp/t1", "//tmp/t2", "//tmp/t3"],
            out="//tmp/t_out",
            spec={"data_size_per_job": 1},
        )

        expected = [
            {"a": 1},
            {"a": 2},
            {"a": 3},
            {"a": 3},
            {"a": 3},
            {"a": 3},
            {"a": 3},
            {"a": 15},
        ]
        if sort_order == "descending":
            expected = expected[::-1]
        assert read_table("//tmp/t_out") == expected
        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["a"]

    @authors("psushin")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sorted_with_row_limits(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        create("table", "//tmp/t1")

        rows = [{"a": 2}, {"a": 3}, {"a": 15}]
        if sort_order == "descending":
            rows = rows[::-1]

        write_table("//tmp/t1", rows, sorted_by=[{"name": "a", "sort_order": sort_order}])

        create("table", "//tmp/t_out")
        merge(combine_chunks=False, mode="sorted", in_="//tmp/t1[:#2]", out="//tmp/t_out")

        assert read_table("//tmp/t_out") == rows[:2]
        assert get("//tmp/t_out/@chunk_count") == 1

    @authors("ignat")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sorted_by(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        mul = 1 if sort_order == "ascending" else -1

        a1 = {"a": 1 * mul, "b": 20 * mul}
        a2 = {"a": 10 * mul, "b": 1 * mul}
        a3 = {"a": 10 * mul, "b": 2 * mul}

        b1 = {"a": 2 * mul, "c": 10 * mul}
        b2 = {"a": 10 * mul, "c": 0 * mul}
        b3 = {"a": 15 * mul, "c": 5 * mul}

        write_table("//tmp/t1", [a1, a2, a3], sorted_by=[
            {"name": "a", "sort_order": sort_order},
            {"name": "b", "sort_order": sort_order}])
        write_table("//tmp/t2", [b1, b2, b3], sorted_by=[
            {"name": "a", "sort_order": sort_order},
            {"name": "c", "sort_order": sort_order}])

        create("table", "//tmp/t_out")

        # error when sorted_by of input tables are different and merge_by is not set
        with pytest.raises(YtError):
            merge(mode="sorted", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t_out")

        # now merge_by is set
        merge(
            mode="sorted",
            in_=["//tmp/t1", "//tmp/t2"],
            out="//tmp/t_out",
            merge_by=[{"name": "a", "sort_order": sort_order}])

        result = read_table("//tmp/t_out")
        assert result[:2] == [a1, b1]
        assert_items_equal(result[2:5], [a2, a3, b2])
        assert result[5] == b3

        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["a"]

    @authors("babenko")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sorted_unique_simple(self, optimize_for, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")
        create(
            "table",
            "//tmp/t_out",
            attributes={
                "optimize_for": optimize_for,
                "schema": make_schema(
                    [
                        {"name": "a", "type": "int64", "sort_order": sort_order},
                        {"name": "b", "type": "int64"},
                    ],
                    unique_keys=True,
                ),
            },
        )

        mul = 1 if sort_order == "ascending" else -1
        a1 = {"a": 1 * mul, "b": 1 * mul}
        a2 = {"a": 2 * mul, "b": 2 * mul}
        a3 = {"a": 3 * mul, "b": 3 * mul}

        write_table("//tmp/t1", [a1, a2], sorted_by=[
            {"name": "a", "sort_order": sort_order},
            {"name": "b", "sort_order": sort_order}
        ])
        write_table("//tmp/t2", [a3], sorted_by=[
            {"name": "a", "sort_order": sort_order},
        ])
        write_table("//tmp/t3", [a3, a3], sorted_by=[
            {"name": "a", "sort_order": sort_order},
            {"name": "b", "sort_order": sort_order}
        ])

        with pytest.raises(YtError):
            merge(
                mode="sorted",
                in_="//tmp/t3",
                out="//tmp/t_out",
                merge_by=[{"name": "a", "sort_order": sort_order}],
                spec={"schema_inference_mode": "from_output"},
            )

        merge(
            mode="sorted",
            in_=["//tmp/t1", "//tmp/t2"],
            out="//tmp/t_out",
            merge_by=[{"name": "a", "sort_order": sort_order}],
            spec={"schema_inference_mode": "from_output"},
        )

        result = read_table("//tmp/t_out")
        assert result == [a1, a2, a3]

        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["a"]
        assert get("//tmp/t_out/@schema/@unique_keys")

    @authors("psushin")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sorted_unique_teleport(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "a", "type": "int64", "sort_order": sort_order},
                        {"name": "b", "type": "int64"},
                    ],
                    unique_keys=True,
                )
            },
        )
        create(
            "table",
            "//tmp/t_out",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "a", "type": "int64", "sort_order": sort_order},
                        {"name": "b", "type": "int64"},
                    ],
                    unique_keys=True,
                )
            },
        )

        rows = [{"a": 1, "b": 1}, {"a": 2, "b": 2}]
        if sort_order == "descending":
            rows = rows[::-1]

        write_table("//tmp/t1", rows)

        merge(
            mode="sorted",
            in_="//tmp/t1",
            out="//tmp/t_out",
            merge_by=[{"name": "a", "sort_order": sort_order}])

        assert read_table("//tmp/t_out") == rows
        assert get("//tmp/t_out/@schema/@unique_keys")

    @authors("babenko")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sorted_unique_with_wider_key_columns(self, optimize_for, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        create("table", "//tmp/t1")
        create(
            "table",
            "//tmp/t_out",
            attributes={
                "optimize_for": optimize_for,
                "schema": make_schema(
                    [
                        {"name": "key1", "type": "int64", "sort_order": sort_order},
                        {"name": "key2", "type": "int64", "sort_order": sort_order},
                    ],
                    unique_keys=True,
                ),
            },
        )

        rows = [{"key1": 1, "key2": 1}, {"key1": 1, "key2": 2}]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table(
            "//tmp/t1",
            rows,
            sorted_by=[
                {"name": "key1", "sort_order": sort_order},
                {"name": "key2", "sort_order": sort_order}
            ],
        )

        with pytest.raises(YtError):
            merge(
                mode="sorted",
                in_="//tmp/t1",
                out="//tmp/t_out",
                merge_by=[{"name": "key1", "sort_order": sort_order}],
                spec={"schema_inference_mode": "from_output"},
            )

        merge(
            mode="sorted",
            in_="//tmp/t1",
            out="//tmp/t_out",
            merge_by=[
                {"name": "key1", "sort_order": sort_order},
                {"name": "key2", "sort_order": sort_order}
            ],
            spec={"schema_inference_mode": "from_output"},
        )

        assert get("//tmp/t_out/@sorted")
        assert get("//tmp/t_out/@sorted_by") == ["key1", "key2"]
        assert get("//tmp/t_out/@schema/@unique_keys")

    @authors("ignat")
    def test_empty_in_ordered(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t_out")

        v = {"foo": "bar"}
        write_table("//tmp/t1", v)

        merge(mode="ordered", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t_out")

        assert read_table("//tmp/t_out") == [v]

    @authors("psushin")
    def test_empty_in_sorted(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}]},
        )
        create("table", "//tmp/t_out")

        merge(mode="sorted", in_="//tmp/t1", out="//tmp/t_out")

        assert read_table("//tmp/t_out") == []

    @authors("ignat")
    def test_non_empty_out(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t_out")

        v1 = {"value": 1}
        v2 = {"value": 2}
        v3 = {"value": 3}

        write_table("//tmp/t1", v1)
        write_table("//tmp/t2", v2)
        write_table("//tmp/t_out", v3)

        merge(mode="ordered", in_=["//tmp/t1", "//tmp/t2"], out="<append=true>//tmp/t_out")

        assert read_table("//tmp/t_out") == [v3, v1, v2]

    @authors("panin", "ignat")
    def test_multiple_in(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        v = {"foo": "bar"}

        write_table("//tmp/t_in", v)

        merge(
            mode="ordered",
            in_=["//tmp/t_in", "//tmp/t_in", "//tmp/t_in"],
            out="//tmp/t_out",
        )

        assert read_table("//tmp/t_out") == [v, v, v]

    @authors("panin", "ignat")
    def test_in_equal_to_out(self):
        create("table", "//tmp/t_in")

        v = {"foo": "bar"}

        write_table("<append=true>//tmp/t_in", v)
        write_table("<append=true>//tmp/t_in", v)

        merge(
            combine_chunks=True,
            mode="ordered",
            in_="//tmp/t_in",
            out="<append=true>//tmp/t_in",
        )

        assert read_table("//tmp/t_in") == [v, v, v, v]
        assert get("//tmp/t_in/@chunk_count") == 3  # only result of merge is combined

    @authors("ignat")
    def test_selectors(self):
        self._prepare_tables()

        merge(
            mode="unordered",
            in_=[self.t1 + "[:#1]", self.t2 + "[#1:#2]"],
            out="//tmp/t_out",
        )

        assert_items_equal(read_table("//tmp/t_out"), self.v1[:1] + self.v2[1:2])
        assert get("//tmp/t_out/@chunk_count") == 2

    @authors("ignat")
    def test_column_selectors(self):
        self._prepare_tables()

        merge(mode="unordered", in_=[self.t1 + "{key1}"], out="//tmp/t_out")

        assert_items_equal(read_table("//tmp/t_out"), [self.v1[1], {}, {}])
        assert get("//tmp/t_out/@chunk_count") == 1

    @authors("savrus", "ermolovd")
    @pytest.mark.parametrize("mode", ["ordered", "unordered", "sorted"])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_column_selectors_schema_inference(self, mode, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

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

        if mode != "sorted":
            merge(mode=mode, in_="//tmp/t{k1,v1}", out="//tmp/t_out")

            assert_items_equal(
                read_table("//tmp/t_out"),
                [{k: r[k] for k in ("k1", "v1")} for r in rows],
            )

            schema = make_schema(
                [
                    {"name": "k1", "type": "int64", "required": False},
                    {"name": "v1", "type": "int64", "required": False},
                ],
                unique_keys=False,
                strict=True,
            )
            if mode != "unordered":
                schema[0]["sort_order"] = sort_order
            assert normalize_schema(get("//tmp/t_out/@schema")) == schema

            remove("//tmp/t_out")
            create("table", "//tmp/t_out")

            merge(mode=mode, in_="//tmp/t{k2,v2}", out="//tmp/t_out")

            assert_items_equal(
                read_table("//tmp/t_out"),
                [{k: r[k] for k in ("k2", "v2")} for r in rows],
            )

            schema = make_schema(
                [
                    {"name": "k2", "type": "int64", "required": False},
                    {"name": "v2", "type": "int64", "required": False},
                ],
                unique_keys=False,
                strict=True,
            )
            assert normalize_schema(get("//tmp/t_out/@schema")) == schema

            remove("//tmp/t_out")
            create("table", "//tmp/t_out")

        merge(mode=mode, in_="//tmp/t{k1,k2,v2}", out="//tmp/t_out")

        assert_items_equal(
            read_table("//tmp/t_out"),
            [{k: r[k] for k in ("k1", "k2", "v2")} for r in rows],
        )

        schema = make_schema(
            [
                {"name": "k1", "type": "int64", "required": False},
                {"name": "k2", "type": "int64", "required": False},
                {"name": "v2", "type": "int64", "required": False},
            ],
            unique_keys=False,
            strict=True,
        )
        if mode != "unordered":
            schema.attributes["unique_keys"] = True
            schema[0]["sort_order"] = sort_order
            schema[1]["sort_order"] = sort_order
        assert normalize_schema(get("//tmp/t_out/@schema")) == schema

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["ordered", "sorted"])
    def test_column_selectors_output_schema_validation(self, mode):
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ]
            },
        )
        create(
            "table",
            "//tmp/t_out",
            attributes={"schema": [{"name": "key", "type": "int64", "sort_order": "ascending"}]},
        )
        rows = [{"key": i, "value": str(i)} for i in range(2)]
        write_table("//tmp/t", rows)

        merge(mode=mode, in_="//tmp/t{key}", out="//tmp/t_out")

        assert_items_equal(read_table("//tmp/t_out"), [{"key": r["key"]} for r in rows])

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["ordered", "unordered"])
    def test_query_filtering(self, mode):
        create("table", "//tmp/t1", attributes={"schema": [{"name": "a", "type": "int64"}]})
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": i} for i in range(2)])

        merge(
            mode=mode,
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"input_query": "a where a > 0"},
        )

        assert read_table("//tmp/t2") == [{"a": 1}]
        assert get("//tmp/t2/@schema") == get("//tmp/t1/@schema")

        remove("//tmp/t2")
        create("table", "//tmp/t2")
        merge(
            mode=mode,
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"input_query": "a + 1 as b where a > 0"},
        )

        assert read_table("//tmp/t2") == [{"b": 2}]
        schema = get("//tmp/t1/@schema")
        schema[0]["name"] = "b"
        assert get("//tmp/t2/@schema") == schema

    @authors("savrus")
    def test_sorted_merge_query_filtering(self):
        create("table", "//tmp/t1", attributes={"schema": [{"name": "a", "type": "int64"}]})
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": i} for i in range(2)])

        with pytest.raises(YtError):
            merge(
                mode="sorted",
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={"input_query": "a where a > 0"},
            )

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["ordered", "unordered"])
    def test_query_filtering_output_schema_validation(self, mode):
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ]
            },
        )
        sort_order = "ascending" if mode == "ordered" else None
        create(
            "table",
            "//tmp/t_out",
            attributes={"schema": [{"name": "k", "type": "int64", "sort_order": sort_order}]},
        )
        rows = [{"key": i, "value": str(i)} for i in range(2)]
        write_table("//tmp/t", rows)

        merge(
            mode=mode,
            in_="//tmp/t",
            out="//tmp/t_out",
            spec={"input_query": "key as k"},
        )

        assert_items_equal(read_table("//tmp/t_out"), [{"k": r["key"]} for r in rows])

    @authors("babenko")
    def test_merge_chunk_properties(self):
        create("table", "//tmp/t1", attributes={"replication_factor": 1, "vital": False})
        write_table("//tmp/t1", [{"a": 1}])
        chunk_id = get_singular_chunk_id("//tmp/t1")

        assert get_chunk_replication_factor(chunk_id) == 1
        assert not get("#" + chunk_id + "/@vital")

        create("table", "//tmp/t2", attributes={"replication_factor": 3, "vital": True})
        merge(mode="ordered", in_=["//tmp/t1"], out="//tmp/t2")

        wait(lambda: get_chunk_replication_factor(chunk_id) == 3 and get("#" + chunk_id + "/@vital"))

    @authors("ignat")
    def test_chunk_indices(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        for i in range(5):
            write_table("<sorted_by=[a];append=%true>//tmp/t1", [{"a": i}])

        merge(
            mode="sorted",
            in_=yson.to_yson_type(
                "//tmp/t1",
                attributes={
                    "ranges": [
                        {
                            "lower_limit": {"chunk_index": 1},
                            "upper_limit": {"chunk_index": 3},
                        }
                    ]
                },
            ),
            out="//tmp/t2",
        )

        assert read_table("//tmp/t2") == [{"a": i} for i in range(1, 3)]

    @authors("psushin")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_auto_schema_inference_unordered(self, optimize_for):
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

        assert get("//tmp/input_loose/@schema_mode") == "strong"
        assert get("//tmp/output_weak/@schema_mode") == "weak"
        assert get("//tmp/output_loose/@schema_mode") == "strong"

        write_table("<append=true>//tmp/input_loose", {"key": 1, "value": "foo"})
        write_table("<append=true>//tmp/input_weak", {"key": 1, "value": "foo"})

        merge(in_="//tmp/input_loose", out="//tmp/output_weak")

        assert get("//tmp/output_weak/@schema_mode") == "strong"
        assert not get("//tmp/output_weak/@schema/@strict")

        merge(in_="//tmp/input_loose", out="//tmp/output_loose")
        assert get("//tmp/output_loose/@schema_mode") == "strong"
        assert not get("//tmp/output_loose/@schema/@strict")

        with pytest.raises(YtError):
            # changing from strict schema to nonstrict is not allowed
            merge(in_="//tmp/input_loose", out="//tmp/output_strict")

        # schema validation must pass
        merge(in_="//tmp/input_weak", out="//tmp/output_loose")
        assert get("//tmp/output_loose/@schema_mode") == "strong"
        assert not get("//tmp/output_loose/@schema/@strict")

        merge(in_=["//tmp/input_weak", "//tmp/input_loose"], out="//tmp/output_loose")

        create(
            "table",
            "//tmp/output_sorted",
            attributes={
                "optimize_for": optimize_for,
                "schema": make_schema(
                    [{"name": "key", "type": "int64", "sort_order": "ascending"}],
                    strict=False,
                ),
            },
        )

        with pytest.raises(YtError):
            # cannot do unordered merge to sorted output
            merge(in_="//tmp/input_loose", out="//tmp/output_sorted")

        with pytest.raises(YtError):
            # even in user insists
            merge(
                in_="//tmp/input_loose",
                out="//tmp/output_sorted",
                spec={"schema_inference_mode": "from_output"},
            )

    @authors("psushin")
    def test_auto_schema_inference_ordered(self):
        output_schema = make_schema([{"name": "key", "type": "int64"}, {"name": "value", "type": "string"}])
        good_schema = make_schema([{"name": "key", "type": "int64"}])
        bad_schema = make_schema([{"name": "key", "type": "int64"}, {"name": "bad", "type": "int64"}])

        create("table", "//tmp/input_good", attributes={"schema": good_schema})
        create("table", "//tmp/input_bad", attributes={"schema": bad_schema})
        create("table", "//tmp/input_weak")
        create("table", "//tmp/output_strong", attributes={"schema": output_schema})
        create("table", "//tmp/output_weak")

        merge(in_=["//tmp/input_weak", "//tmp/input_good"], out="//tmp/output_strong")

        with pytest.raises(YtError):
            merge(in_=["//tmp/input_weak", "//tmp/input_bad"], out="//tmp/output_strong")

        with pytest.raises(YtError):
            merge(in_=["//tmp/input_weak", "//tmp/input_good"], out="//tmp/output_weak")

        with pytest.raises(YtError):
            merge(in_=["//tmp/input_bad", "//tmp/input_good"], out="//tmp/output_weak")

    @authors("babenko")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_schema_validation_unordered(self, optimize_for):
        create("table", "//tmp/input")
        create(
            "table",
            "//tmp/output",
            attributes={
                "optimize_for": optimize_for,
                "schema": make_schema(
                    [
                        {"name": "key", "type": "int64"},
                        {"name": "value", "type": "string"},
                    ]
                ),
            },
        )

        for i in range(10):
            write_table("<append=true>//tmp/input", {"key": i, "value": "foo"})

        merge(
            in_="//tmp/input",
            out="//tmp/output",
            spec={"schema_inference_mode": "from_output"},
        )

        assert get("//tmp/output/@schema_mode") == "strong"
        assert get("//tmp/output/@schema/@strict")
        assert_items_equal(read_table("//tmp/output"), [{"key": i, "value": "foo"} for i in range(10)])

        write_table("//tmp/input", {"key": "1", "value": "foo"})

        with pytest.raises(YtError):
            merge(
                in_="//tmp/input",
                out="//tmp/output",
                spec={"schema_inference_mode": "from_output"},
            )

    @authors("babenko")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_schema_validation_ordered(self, optimize_for):
        create("table", "//tmp/input")
        create(
            "table",
            "//tmp/output",
            attributes={
                "optimize_for": optimize_for,
                "schema": make_schema(
                    [
                        {"name": "key", "type": "int64"},
                        {"name": "value", "type": "string"},
                    ]
                ),
            },
        )

        for i in range(10):
            write_table("<append=true>//tmp/input", {"key": i, "value": "foo"})

        merge(
            mode="ordered",
            in_="//tmp/input",
            out="//tmp/output",
            spec={"schema_inference_mode": "from_output"},
        )

        assert get("//tmp/output/@schema_mode") == "strong"
        assert get("//tmp/output/@schema/@strict")
        assert read_table("//tmp/output") == [{"key": i, "value": "foo"} for i in range(10)]

        write_table("//tmp/input", {"key": "1", "value": "foo"})

        with pytest.raises(YtError):
            merge(
                mode="ordered",
                in_="//tmp/input",
                out="//tmp/output",
                spec={"schema_inference_mode": "from_output"},
            )

    @authors("babenko")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_schema_validation_sorted(self, optimize_for):
        create("table", "//tmp/input")
        create(
            "table",
            "//tmp/output",
            attributes={
                "optimize_for": optimize_for,
                "schema": make_schema(
                    [
                        {"name": "key", "type": "int64", "sort_order": "ascending"},
                        {"name": "value", "type": "string"},
                    ]
                ),
            },
        )

        for i in range(10):
            write_table("<append=true; sorted_by=[key]>//tmp/input", {"key": i, "value": "foo"})

        assert get("//tmp/input/@sorted_by") == ["key"]

        merge(
            mode="sorted",
            in_="//tmp/input",
            out="//tmp/output",
            spec={"schema_inference_mode": "from_output"},
        )

        assert get("//tmp/output/@schema_mode") == "strong"
        assert get("//tmp/output/@schema/@strict")
        assert read_table("//tmp/output") == [{"key": i, "value": "foo"} for i in range(10)]

        write_table("<sorted_by=[key]>//tmp/input", {"key": "1", "value": "foo"})
        assert get("//tmp/input/@sorted_by") == ["key"]

        with pytest.raises(YtError):
            merge(
                mode="sorted",
                in_="//tmp/input",
                out="//tmp/output",
                spec={"schema_inference_mode": "from_output"},
            )

    @authors("ermolovd")
    @pytest.mark.parametrize("mode", ["unordered", "ordered", "sorted"])
    def test_schema_validation_complex_types(self, mode):
        first_column = {"name": "index", "type_v3": "int64"}
        if mode == "sorted":
            first_column["sort_order"] = "ascending"

        input_schema = make_schema(
            [
                first_column,
                {"name": "value", "type_v3": optional_type(optional_type("string"))},
            ],
            unique_keys=False,
            strict=True,
        )
        output_schema = make_schema(
            [
                first_column,
                {"name": "value", "type_v3": list_type(optional_type("string"))},
            ],
            unique_keys=False,
            strict=True,
        )

        create("table", "//tmp/input", attributes={"schema": input_schema})
        create("table", "//tmp/output", attributes={"schema": output_schema})
        write_table(
            "//tmp/input",
            [
                {"index": 1, "value": [None]},
                {"index": 2, "value": ["foo"]},
            ],
        )

        # We check that yson representation of types are compatible with each other
        write_table("//tmp/output", read_table("//tmp/input"))

        merge_by_args = {}
        if mode == "sorted":
            merge_by_args["merge_by"] = "index"

        with pytest.raises(YtError):
            merge(
                mode=mode,
                in_="//tmp/input",
                out="//tmp/output",
                spec={"schema_inference_mode": "auto"},
                **merge_by_args
            )
        merge(
            mode=mode,
            in_="//tmp/input",
            out="//tmp/output",
            spec={"schema_inference_mode": "from_output"},
            **merge_by_args
        )
        assert normalize_schema_v3(output_schema) == normalize_schema_v3(get("//tmp/output/@schema"))
        merge(
            mode=mode,
            in_="//tmp/input",
            out="//tmp/output",
            spec={"schema_inference_mode": "from_input"},
            **merge_by_args
        )
        assert normalize_schema_v3(input_schema) == normalize_schema_v3(get("//tmp/output/@schema"))

    @authors("savrus")
    @parametrize_external
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_sorted_merge_on_dynamic_table(self, external, optimize_for):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t", optimize_for=optimize_for, external=external)

        create("table", "//tmp/t_out")

        rows1 = [{"key": i, "value": str(i)} for i in range(10)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows1)
        sync_unmount_table("//tmp/t")

        merge(mode="sorted", in_="//tmp/t", out="//tmp/t_out", merge_by="key")

        assert_items_equal(read_table("//tmp/t_out"), rows1)

        rows2 = [{"key": i, "value": str(i + 1)} for i in range(5, 15)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows2)
        sync_unmount_table("//tmp/t")

        merge(mode="sorted", in_="//tmp/t", out="//tmp/t_out", merge_by="key")

        assert_items_equal(read_table("//tmp/t_out"), rows1[:5] + rows2)

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["unordered", "ordered", "sorted"])
    def test_computed_columns(self, mode):
        create("table", "//tmp/t1")
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "k1", "type": "int64", "expression": "k2 * 2"},
                    {"name": "k2", "type": "int64"},
                ]
            },
        )

        write_table("<sorted_by=[k2]>//tmp/t1", [{"k2": i} for i in range(2)])

        merge(mode=mode, in_="//tmp/t1", out="//tmp/t2")

        assert get("//tmp/t2/@schema_mode") == "strong"
        assert read_table("//tmp/t2") == [{"k1": i * 2, "k2": i} for i in range(2)]

    @authors("psushin")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sort_order_validation_failure(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        create("table", "//tmp/input")
        create(
            "table",
            "//tmp/output",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "key", "type": "int64", "sort_order": sort_order},
                        {"name": "value", "type": "string"},
                    ]
                )
            },
        )

        for i in range(10):
            write_table("<append=true;>//tmp/input", {"key": i % 3, "value": "foo"})

        with pytest.raises(YtError):
            merge(
                mode="unordered",
                in_="//tmp/input",
                out="//tmp/output",
                spec={"schema_inference_mode": "from_output"},
            )

        with pytest.raises(YtError):
            merge(
                mode="ordered",
                in_="//tmp/input",
                out="//tmp/output",
                spec={"schema_inference_mode": "from_output"},
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

        merge(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"force_transform": "true", "job_count": 1},
        )

        chunk_id = get_singular_chunk_id("//tmp/t_out")
        assert get("#" + chunk_id + "/@compressed_data_size") > 1024 * 10
        assert get("#" + chunk_id + "/@max_block_size") < 1024 * 2

    @authors("max42")
    @pytest.mark.parametrize("mode", ["sorted", "ordered"])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_merge_interrupt(self, mode, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        create(
            "table",
            "//tmp/t_in",
            attributes={"schema": [{"name": "a", "type": "int64", "sort_order": sort_order}]},
        )
        create("table", "//tmp/t_out")

        rows = [{"a": i} for i in range(25)]
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
                    "testing_options": {"pipe_delay": 250},
                    "buffer_row_count": 1,
                },
                "enable_job_splitting": False,
            },
        )
        wait(lambda: len(op.get_running_jobs()) > 0)
        jobs = list(op.get_running_jobs())
        assert len(jobs) == 1
        job_id = jobs[0]
        wait(
            lambda: get(
                op.get_path() + "/controller_orchid/running_jobs/{}/progress".format(job_id),
                default=0,
            )
            >= 0.1
        )
        interrupt_job(job_id)
        op.track()
        assert read_table("//tmp/t_out") == rows
        assert get(op.get_path() + "/@progress/jobs/completed/total") == 2

    @authors("max42")
    @pytest.mark.parametrize("mode", ["sorted", "ordered"])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_merge_job_splitter(self, mode, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

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
            },
        )

        wait(lambda: exists(op.get_path() + "/controller_orchid/progress/tasks/0/job_splitter"))

        op.track()

        completed = get(op.get_path() + "/@progress/jobs/completed")
        interrupted = completed["interrupted"]
        assert completed["total"] >= 2
        assert interrupted["job_split"] >= 1
        assert read_table("//tmp/t_out", verbose=False) == rows

    @authors("max42")
    @pytest.mark.parametrize("mode", ["sorted", "ordered", "unordered"])
    def test_sampling(self, mode):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "key", "type": "string", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ]
            },
        )
        create("table", "//tmp/t2")
        write_table(
            "//tmp/t1",
            [{"key": ("%02d" % (i // 100)), "value": "x" * 10 ** 2} for i in range(10000)],
            table_writer={"block_size": 1024},
        )

        op = merge(
            in_="//tmp/t1",
            out="//tmp/t2",
            mode=mode,
            spec={
                "sampling": {"sampling_rate": 0, "io_block_size": 10 ** 5},
                "data_weight_per_job": 10 ** 9,
                "enable_job_splitting": False,
            },
        )
        assert get("//tmp/t2/@row_count") == 0
        assert get("//tmp/t2/@row_count") == 0
        assert get(op.get_path() + "/@progress/jobs/total") == get(op.get_path() + "/@progress/jobs/completed/total")

        op = merge(
            in_="//tmp/t1",
            out="//tmp/t2",
            mode=mode,
            spec={
                "sampling": {"sampling_rate": 0.5, "io_block_size": 10 ** 5},
                "combine_chunks": True,
                "data_weight_per_job": 10 ** 9,
                "enable_job_splitting": False,
            },
        )
        assert 0.25 * 10000 <= get("//tmp/t2/@row_count") <= 0.75 * 10000
        assert get("//tmp/t2/@chunk_count") == 1
        assert get(op.get_path() + "/@progress/jobs/total") == get(op.get_path() + "/@progress/jobs/completed/total")

        op = merge(
            in_="//tmp/t1",
            out="//tmp/t2",
            mode=mode,
            spec={
                "sampling": {"sampling_rate": 0.5, "io_block_size": 10 ** 5},
                "job_count": 10,
                "combine_chunks": True,
                "enable_job_splitting": False,
            },
        )
        assert 0.25 * 10000 <= get("//tmp/t2/@row_count") <= 0.75 * 10000
        assert get("//tmp/t2/@chunk_count") > 1
        assert get(op.get_path() + "/@progress/jobs/total") == get(op.get_path() + "/@progress/jobs/completed/total")

        if mode != "unordered":
            op = merge(
                in_="//tmp/t1",
                out="//tmp/t2",
                mode=mode,
                spec={
                    "sampling": {"sampling_rate": 0.5, "io_block_size": 10 ** 7},
                    "job_count": 10,
                    "combine_chunks": True,
                    "data_weight_per_job": 10 ** 9,
                    "enable_job_splitting": False,
                },
            )
            assert get("//tmp/t2/@row_count") in [0, 10000]
            assert get("//tmp/t2/@chunk_count") in [0, 1]
            assert get(op.get_path() + "/@progress/jobs/total") == get(
                op.get_path() + "/@progress/jobs/completed/total"
            )

            op = merge(
                in_="//tmp/t1",
                out="//tmp/t2",
                mode=mode,
                spec={
                    "sampling": {
                        "sampling_rate": 0.5,
                        "io_block_size": 1,
                        "max_total_slice_count": 1,
                    },
                    "job_count": 10,
                    "combine_chunks": True,
                    "enable_job_splitting": False,
                },
            )
            assert get("//tmp/t2/@row_count") in [0, 10000]
            assert get("//tmp/t2/@chunk_count") in [0, 1]
            assert get(op.get_path() + "/@progress/jobs/total") == get(
                op.get_path() + "/@progress/jobs/completed/total"
            )

    @authors("max42")
    @pytest.mark.parametrize("mode", ["ordered", "unordered"])
    def test_zero_sampling_rate(self, mode):
        # YT-11640
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "key", "type": "string"}]},
        )
        write_table(
            "//tmp/t1",
            [{"key": "0"} for i in range(1000000)],
            table_writer={"block_size": 1024},
        )
        for i in range(12):
            merge(in_=["//tmp/t1", "//tmp/t1"], out="//tmp/t1")
        create("table", "//tmp/t2")
        op = merge(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            mode=mode,
            spec={
                "sampling": {"sampling_rate": 0, "io_block_size": 10 ** 5},
                "data_weight_per_job": 10 ** 9,
                "enable_job_splitting": False,
                "combine_chunks": True,
            },
        )
        wait(lambda: op.get_state().endswith("ed"))
        op.track()

        assert get("//tmp/t2/@row_count") == 0
        assert get("//tmp/t2/@row_count") == 0
        assert get(op.get_path() + "/@progress/jobs/total") == get(op.get_path() + "/@progress/jobs/completed/total")

    @authors("max42")
    @pytest.mark.parametrize("mode", ["sorted", "ordered", "unordered"])
    def test_sampling_teleport(self, mode):
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "key", "type": "string", "sort_order": "ascending"}]},
        )
        create("table", "//tmp/t2")
        for i in range(100):
            write_table("<append=%true>//tmp/t1", [{"key": ("%02d" % i)}])

        merge(
            in_="//tmp/t1",
            out="//tmp/t2",
            mode=mode,
            spec={
                "sampling": {
                    "sampling_rate": 0.1,
                    "user_limits": {"resource_limits": {"user_slots": 0}},
                },
                "enable_job_splitting": False,
            },
        )
        assert 0 <= get("//tmp/t2/@chunk_count") <= 20

    @authors("max42", "psushin")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_overlapping_ranges_in_sorted_merge(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "key", "type": "int64", "sort_order": sort_order}]},
        )
        create("table", "//tmp/t2")
        if sort_order == "ascending":
            write_table("//tmp/t1", [{"key": 0}, {"key": 1}])
        else:
            write_table("//tmp/t1", [{"key": 1}, {"key": 0}])

        merge(in_="<ranges=[{};{}]>//tmp/t1", out="//tmp/t2", mode="sorted")

        expected = [
            {"key": 0},
            {"key": 0},
            {"key": 1},
            {"key": 1},
        ]
        if sort_order == "descending":
            expected = expected[::-1]

        assert read_table("//tmp/t2") == expected

    @authors("max42")
    def test_overlapping_ranges_in_sorted_merge_multiple_jobs(self):
        create("table", "//tmp/t1", attributes={"schema": [{"name": "k", "type": "int64", "sort_order": "ascending"}]})
        create("table", "//tmp/t2", attributes={"schema": [{"name": "k", "type": "int64", "sort_order": "ascending"}]})
        create("table", "//tmp/d", attributes={"schema": [{"name": "k", "type": "int64", "sort_order": "ascending"}]})

        rows = [{"k": i} for i in range(20)]
        write_table("//tmp/t1", rows)
        write_table("//tmp/t2", rows)

        merge(in_=["//tmp/t1[2:6,4:8]", "//tmp/t2[16:18,12:14]"],
              out="//tmp/d",
              mode="sorted",
              spec={"data_weight_per_job": 1})

        assert get("//tmp/d/@chunk_count") > 1

        assert [row["k"] for row in read_table("//tmp/d")] == [2, 3, 4, 4, 5, 5, 6, 7, 12, 13, 16, 17]

    @authors("ermolovd")
    def test_schema_compatibility(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "key", "type": "int64"}]},
        )
        write_table("//tmp/t1", [{"key": None}])
        with pytest.raises(YtError):
            merge(
                in_="//tmp/t1",
                out="<schema=[{name=key;type=int64;required=true}]>//tmp/t2",
            )

    @authors("ermolovd")
    @pytest.mark.parametrize("mode", ["unordered", "ordered"])
    def test_infer_output_yt_8661_first(self, mode):
        schema = make_schema([{"name": "x", "type": "uint64"}], strict=False)
        schemaful_table = "//tmp/schemaful_table"
        schemaless_table = "//tmp/schemaless_table"

        create(
            "table",
            schemaful_table,
            attributes={
                "schema": schema,
                "optimize_for": "scan",
            },
        )
        create("table", schemaless_table)

        write_table(schemaful_table, [{"x": i} for i in range(100)])
        write_table(schemaless_table, [{"x": str(i)} for i in range(100, 200)])

        # merging non-strict table with strict table
        with pytest.raises(YtError):
            merge(
                mode=mode,
                in_=[schemaless_table, schemaful_table],
                out=schemaful_table,
                spec={"schema_inference_mode": "from_output"},
            )

    @authors("ermolovd")
    @pytest.mark.parametrize("mode", ["unordered", "ordered"])
    def test_infer_output_yt_8661_second(self, mode):
        schema1 = make_schema(
            [
                {"name": "x", "type": "uint64", "required": True},
                {"name": "y", "type": "uint64", "required": True},
            ]
        )
        schema2 = make_schema(
            [
                {"name": "y", "type": "uint64", "required": True},
            ]
        )
        table1 = "//tmp/table1"
        table2 = "//tmp/table2"

        create(
            "table",
            table1,
            attributes={
                "schema": schema1,
            },
        )
        create(
            "table",
            table2,
            attributes={
                "schema": schema2,
            },
        )

        write_table(table1, [{"x": i, "y": i} for i in range(100)])
        write_table(table2, [{"y": i} for i in range(100, 200)])

        with pytest.raises(YtError):
            merge(
                mode=mode,
                in_=[table2, table1],
                out=table1,
                spec={"schema_inference_mode": "from_output"},
            )

    @authors("ermolovd")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_merge_lose_composite_type(self, optimize_for):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "a", "type_v3": list_type("int64")},
                ]
            },
        )
        write_table("//tmp/t1", [{"a": [1, 2, 3]}, {"a": [4, 5, 6]}])

        create(
            "table",
            "//tmp/t_out",
            attributes={
                "schema": [
                    {"name": "a", "type_v3": optional_type("yson")},
                ],
                "optimize_for": optimize_for,
            },
        )
        merge(
            in_=["//tmp/t1"],
            out="//tmp/t_out",
            spec={"schema_inference_mode": "from_output"},
        )

    @authors("gritukan")
    @pytest.mark.parametrize("mode", ["sorted", "ordered", "unordered"])
    def test_empty_input_with_explicit_job_count(self, mode):
        create(
            "table",
            "//tmp/t_in",
            attributes={
                "schema": [
                    {"name": "key", "type": "string", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ]
            },
        )
        create("table", "//tmp/t_out")

        merge(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            mode=mode,
            spec={
                "job_count": 2,
            })

    @authors("max42")
    def test_teleport_to_narrow_schema_and_merge(self):
        # YT-14536.
        create("table", "//tmp/t_wide", attributes={
            "schema": [
                {"name": "k1", "type": "int64", "sort_order": "ascending"},
                {"name": "k2", "type": "int64", "sort_order": "ascending"},
            ],
            "chunk_writer": {"block_size": 1},
            "compression_codec": "none",
        })
        create("table", "//tmp/t_narrow", attributes={
            "schema": [
                {"name": "k1", "type": "int64", "sort_order": "ascending"},
                {"name": "k2", "type": "int64"},
            ],
            "chunk_writer": {"block_size": 1},
            "compression_codec": "none",
        })

        write_table("<append=%true>//tmp/t_wide", [{"k1": 1, "k2": 10}])
        merge(in_=["//tmp/t_wide"],
              out="//tmp/t_narrow",
              mode="sorted",
              merge_by=["k1"])

        merge(in_=["//tmp/t_narrow"],
              out="//tmp/t_narrow",
              mode="sorted",
              merge_by=["k1"],
              spec={"force_transform": True})

        assert read_table("//tmp/t_narrow") == [{"k1": 1, "k2": 10}]

    @authors("gepardo")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_sorted_merge_with_alter_table(self, optimize_for):
        if self.Env.get_component_version("ytserver-job-proxy").abi <= (22, 1):
            pytest.skip("Job proxy does not contain fix for the bug yet")

        create("table", "//tmp/table1", attributes={
            "schema": [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
            ],
            "optimize_for": optimize_for,
        })
        create("table", "//tmp/table2", attributes={
            "schema": [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64", "sort_order": "ascending"},
            ],
            "optimize_for": optimize_for,
        })
        create("table", "//tmp/table3", attributes={
            "schema": [
                {"name": "a", "type": "int64", "sort_order": "ascending"},
                {"name": "b", "type": "int64", "sort_order": "ascending"},
            ],
            "optimize_for": optimize_for,
        })
        write_table("//tmp/table1", [{"a": 1}])
        write_table("//tmp/table2", [{"a": 1}])
        write_table("//tmp/table3", [{"a": 1, "b": 2}])
        alter_table("//tmp/table1", schema=[
            {"name": "a", "type": "int64", "sort_order": "ascending"},
            {"name": "b", "type": "int64", "sort_order": "ascending"},
        ])
        create("table", "//tmp/table0")

        merge(
            in_=["//tmp/table3", "//tmp/table2", "//tmp/table1"],
            out="//tmp/table0",
            mode="sorted",
            merge_by=["a", "b"],
            spec={"force_transform": True},
        )

        expected = [
            {"a": 1, "b": yson.YsonEntity()},
            {"a": 1, "b": yson.YsonEntity()},
            {"a": 1, "b": 2},
        ]
        assert expected == read_table("//tmp/table0")

    @authors("akozhikhov")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_mr_any_value_with_hunks(self, optimize_for):
        # TODO(babenko): remove this once 23.1 binaries are updated.
        if self.__class__.__name__ == "TestMergeCommandsCompatNewCA":
            pytest.skip("Compat test is currently disabled")

        create("table", "//tmp/t", attributes={
            "optimize_for": optimize_for,
            "schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "any", "max_inline_hunk_size": 10},
            ],
        })

        rows = [{"key": 0, "value": "0"}, {"key": 1, "value": [{}, {}]}, {"key": 2, "value": "z" * 100}]
        write_table("//tmp/t", rows)

        merge(
            in_="//tmp/t",
            out="//tmp/t",
            spec={
                "force_transform": True,
                "mode": "sorted",
            }
        )

        assert read_table("//tmp/t") == rows

##################################################################


class TestSchedulerMergeCommandsSliceSize(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_allocations_update_period": 10,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 10,
            "sorted_merge_operation_options": {
                "min_slice_data_weight": 1,
            },
        }
    }

    @authors("gritukan")
    @pytest.mark.skipif("True", reason="YT-13230")
    @pytest.mark.parametrize("tables_intersect", [False, True])
    def test_chunk_slice_size(self, tables_intersect):
        for i in range(10):
            create(
                "table",
                "//tmp/in{}".format(i),
                attributes={
                    "schema": [{"name": "key", "type": "string", "sort_order": "ascending"}],
                    "chunk_writer": {"block_size": 1},  # Each block should have exactly one row to make precise slices.
                    "compression_codec": "none",
                },
            )
            if tables_intersect:
                write_table(
                    "//tmp/in{}".format(i),
                    [{"key": ("%04d" % (10 * x + i))} for x in range(10)],
                )
            else:
                # Last row ensures that chunk won't be teleported.
                write_table(
                    "//tmp/in{}".format(i),
                    [{"key": ("%04d" % (10 * i + x))} for x in range(9)] + [{"key": ("%04d" % (9000 + i))}],
                )
        create("table", "//tmp/out")

        op = merge(
            mode="sorted",
            in_=["//tmp/in{}".format(i) for i in range(10)],
            out="//tmp/out",
            spec={"job_count": 10, "enable_job_splitting": False},
        )
        op.track()
        for chunk_id in get("//tmp/out/@chunk_ids"):
            assert 5 <= get("#" + chunk_id + "/@row_count") <= 15


##################################################################


class TestSchedulerMergeCommandsMulticell(TestSchedulerMergeCommands):
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("babenko")
    def test_multicell_merge_teleport(self):
        create("table", "//tmp/t1", attributes={"external_cell_tag": 11})
        write_table("//tmp/t1", [{"a": 1}])
        chunk_id1 = get_singular_chunk_id("//tmp/t1")

        create("table", "//tmp/t2", attributes={"external_cell_tag": 12})
        write_table("//tmp/t2", [{"a": 2}])
        chunk_id2 = get_singular_chunk_id("//tmp/t2")

        assert get("#" + chunk_id1 + "/@ref_counter") == 1
        assert get("#" + chunk_id2 + "/@ref_counter") == 1
        assert_items_equal(get("#" + chunk_id1 + "/@exports"), {})
        assert_items_equal(get("#" + chunk_id2 + "/@exports"), {})

        create("table", "//tmp/t", attributes={"external": False})
        merge(mode="ordered", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t")

        assert get("//tmp/t/@chunk_ids") == [chunk_id1, chunk_id2]
        assert get("#" + chunk_id1 + "/@ref_counter") == 2
        assert get("#" + chunk_id2 + "/@ref_counter") == 2
        wait(
            lambda: get("#" + chunk_id1 + "/@exports")
            == {
                "10": {
                    "ref_counter": 1,
                    "vital": True,
                    "media": {"default": {"replication_factor": 3, "data_parts_only": False}},
                }
            }
        )
        wait(
            lambda: get("#" + chunk_id2 + "/@exports")
            == {
                "10": {
                    "ref_counter": 1,
                    "vital": True,
                    "media": {"default": {"replication_factor": 3, "data_parts_only": False}},
                }
            }
        )
        assert_items_equal(ls("//sys/foreign_chunks", driver=get_driver(0)), [chunk_id1, chunk_id2])

        assert read_table("//tmp/t") == [{"a": 1}, {"a": 2}]

        remove("//tmp/t")

        wait(
            lambda: get("#" + chunk_id1 + "/@ref_counter") == 1
            and get("#" + chunk_id2 + "/@ref_counter") == 1
            and get("#" + chunk_id1 + "/@exports") == {}
            and get("#" + chunk_id2 + "/@exports") == {}
            and ls("//sys/foreign_chunks", driver=get_driver(0)) == []
        )

    @authors("babenko")
    def test_multicell_merge_multi_teleport(self):
        create("table", "//tmp/t1", attributes={"external_cell_tag": 11})
        write_table("//tmp/t1", [{"a": 1}])
        chunk_id = get_singular_chunk_id("//tmp/t1")

        assert get("#" + chunk_id + "/@ref_counter") == 1
        assert get("#" + chunk_id + "/@exports") == {}
        assert not get("#" + chunk_id + "/@foreign")
        assert not exists("#" + chunk_id, driver=get_driver(2))

        create("table", "//tmp/t2", attributes={"external_cell_tag": 12})
        merge(mode="ordered", in_=["//tmp/t1", "//tmp/t1"], out="//tmp/t2")

        assert get("//tmp/t2/@chunk_ids") == [chunk_id, chunk_id]
        assert get("#" + chunk_id + "/@ref_counter") == 3
        wait(
            lambda: get("#" + chunk_id + "/@exports")
            == {
                "12": {
                    "ref_counter": 2,
                    "vital": True,
                    "media": {"default": {"replication_factor": 3, "data_parts_only": False}},
                }
            }
        )
        assert_items_equal(ls("//sys/foreign_chunks", driver=get_driver(2)), [chunk_id])
        assert get("#" + chunk_id + "/@import_ref_counter", driver=get_driver(2)) == 2

        assert read_table("//tmp/t2") == [{"a": 1}, {"a": 1}]

        create("table", "//tmp/t3", attributes={"external_cell_tag": 12})
        merge(mode="ordered", in_=["//tmp/t1", "//tmp/t1"], out="//tmp/t3")

        assert get("//tmp/t3/@chunk_ids") == [chunk_id, chunk_id]
        assert get("#" + chunk_id + "/@ref_counter") == 5
        wait(
            lambda: get("#" + chunk_id + "/@exports")
            == {
                "12": {
                    "ref_counter": 4,
                    "vital": True,
                    "media": {"default": {"replication_factor": 3, "data_parts_only": False}},
                }
            }
        )
        assert_items_equal(ls("//sys/foreign_chunks", driver=get_driver(2)), [chunk_id])
        assert get("#" + chunk_id + "/@import_ref_counter", driver=get_driver(2)) == 4

        assert read_table("//tmp/t3") == [{"a": 1}, {"a": 1}]

        remove("//tmp/t2")

        wait(
            lambda: get("#" + chunk_id + "/@ref_counter") == 5
            and get("#" + chunk_id + "/@exports")
            == {
                "12": {
                    "ref_counter": 4,
                    "vital": True,
                    "media": {"default": {"replication_factor": 3, "data_parts_only": False}},
                }
            }
            and ls("//sys/foreign_chunks", driver=get_driver(2)) == [chunk_id]
        )

        remove("//tmp/t3")

        wait(
            lambda: get("#" + chunk_id + "/@ref_counter") == 1
            and get("#" + chunk_id + "/@exports") == {}
            and ls("//sys/foreign_chunks", driver=get_driver(2)) == []
        )

        remove("//tmp/t1")

        wait(lambda: not exists("#" + chunk_id))

    @authors("babenko")
    def test_multicell_merge_chunk_properties(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "replication_factor": 1,
                "vital": False,
                "external_cell_tag": 11,
            },
        )
        write_table("//tmp/t1", [{"a": 1}])
        chunk_id = get_singular_chunk_id("//tmp/t1")

        assert get_chunk_replication_factor(chunk_id) == 1
        assert not get("#" + chunk_id + "/@vital")

        create(
            "table",
            "//tmp/t2",
            attributes={
                "replication_factor": 3,
                "vital": False,
                "external_cell_tag": 12,
            },
        )
        merge(mode="ordered", in_=["//tmp/t1"], out="//tmp/t2")

        wait(lambda: get_chunk_replication_factor(chunk_id) == 3 and not get("#" + chunk_id + "/@vital"))

        set("//tmp/t2/@replication_factor", 2)

        wait(lambda: get_chunk_replication_factor(chunk_id) == 2)

        set("//tmp/t2/@replication_factor", 3)

        wait(lambda: get_chunk_replication_factor(chunk_id) == 3)

        set("//tmp/t2/@vital", True)

        wait(lambda: get("#" + chunk_id + "/@vital"))

        set("//tmp/t1/@replication_factor", 4)

        wait(lambda: get_chunk_replication_factor(chunk_id) == 4)

        set("//tmp/t1/@replication_factor", 1)

        wait(lambda: get_chunk_replication_factor(chunk_id) == 3)

        remove("//tmp/t2")

        wait(lambda: get_chunk_replication_factor(chunk_id) == 1 and not get("#" + chunk_id + "/@vital"))

    @authors("babenko")
    def test_yt_4259(self):
        create("table", "//tmp/t", attributes={"external": False})
        create("table", "//tmp/t1", attributes={"external_cell_tag": 11})
        create("table", "//tmp/t2", attributes={"external_cell_tag": 12})

        write_table("//tmp/t", [{"a": 1}])
        chunk_id = get_singular_chunk_id("//tmp/t")

        merge(mode="ordered", in_=["//tmp/t"], out="//tmp/t1")
        merge(mode="ordered", in_=["//tmp/t"], out="//tmp/t2")

        wait(
            lambda: get("#" + chunk_id + "/@exports")
            == {
                "11": {
                    "ref_counter": 1,
                    "vital": True,
                    "media": {"default": {"replication_factor": 3, "data_parts_only": False}},
                },
                "12": {
                    "ref_counter": 1,
                    "vital": True,
                    "media": {"default": {"replication_factor": 3, "data_parts_only": False}},
                },
            }
        )

    @authors("shakurov")
    def test_teleporting_chunks_dont_disappear(self):
        create("table", "//tmp/t1", attributes={"external_cell_tag": 11})
        write_table("//tmp/t1", [{"a": 1}])
        chunk_id = get_singular_chunk_id("//tmp/t1")

        create("table", "//tmp/t2", attributes={"external_cell_tag": 12})

        tx = start_transaction()
        merge(mode="ordered", in_=["//tmp/t1"], out="//tmp/t2", tx=tx)

        assert get("//tmp/t2/@chunk_count", tx=tx) == 1
        assert get_singular_chunk_id("//tmp/t2", tx=tx) == chunk_id

        assert get("#{0}/@exports/12/ref_counter".format(chunk_id)) == 1

        # The point of this test is to make sure snatching chunks from
        # under an uncommitted transaction interoperates with
        # multicell well. Replacing the following two lines with this:
        #     copy("//tmp/t2", "//tmp/t2_copy", source_transaction_id=tx)
        # used to produce (it's no longer supported) a horrific situation
        # when a chunk is destroyed in its cell yet is still
        # referenced from another cell.
        create("table", "//tmp/t2_copy", attributes={"external_cell_tag": 12})
        merge(
            mode="ordered",
            in_=['<transaction_id="{0}">//tmp/t2'.format(tx)],
            out="//tmp/t2_copy",
        )

        abort_transaction(tx)

        remove("//tmp/t1")

        # Give replicator a chance to remove a chunk (in case there's a bug).
        # NB: This sleep cannot be replaced with wait.
        sleep(1.0)

        assert exists("//tmp/t2")
        assert get("//tmp/t2/@chunk_count") == 0
        assert exists("//tmp/t2_copy")
        assert get("//tmp/t2_copy/@chunk_count") == 1
        assert get_singular_chunk_id("//tmp/t2_copy") == chunk_id
        assert exists("#{0}".format(chunk_id))

    @authors("babenko")
    def test_fetch_trimmed_ordered_table_yt_11825(self):
        sync_create_cells(1)
        create(
            "table",
            "//tmp/in",
            attributes={
                "dynamic": True,
                "schema": [{"name": "value", "type": "int64"}],
            },
        )
        sync_mount_table("//tmp/in")

        n = 20
        for i in range(n):
            insert_rows("//tmp/in", [{"value": i}])
            sync_freeze_table("//tmp/in")
            sync_unfreeze_table("//tmp/in")

        assert get("//tmp/in/@chunk_count") == n

        m = 15
        trim_rows("//tmp/in", 0, m)

        wait(lambda: get("//tmp/in/@chunk_count") == n - m)

        create("table", "//tmp/out")

        merge(mode="ordered", in_=["//tmp/in"], out="//tmp/out")

        assert read_table("//tmp/out") == [{"value": i} for i in range(m, n)]


class TestSchedulerMergeCommandsNewSortedPool(TestSchedulerMergeCommands):
    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_allocations_update_period": 10,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 10,
            "max_chunks_per_fetch": 10,
            "sorted_merge_operation_options": {
                "job_splitter": {
                    "min_job_time": 3000,
                    "min_total_data_size": 1024,
                    "update_period": 100,
                    "candidate_percentile": 0.8,
                    "max_jobs_per_split": 3,
                },
                "spec_template": {
                    "use_new_sorted_pool": True,
                },
            },
            "ordered_merge_operation_options": {
                "job_splitter": {
                    "min_job_time": 3000,
                    "min_total_data_size": 1024,
                    "update_period": 100,
                    "candidate_percentile": 0.8,
                    "max_jobs_per_split": 3,
                },
            },
        }
    }

    @authors("max42")
    def test_tricky_teleport(self):
        # YT-14485.
        # This test fails in legacy implementation of sorted pool.

        create("table", "//tmp/t_in", attributes={"schema": [
            {"name": "k", "type": "int64", "sort_order": "ascending"}
        ]})
        create("table", "//tmp/t_out", attributes={"schema": [
            {"name": "k", "type": "int64", "sort_order": "ascending"}
        ]})
        write_table("<append=%true>//tmp/t_in", [{"k": 0}, {"k": 2}])
        write_table("<append=%true>//tmp/t_in", [{"k": 2}, {"k": 2}])
        write_table("<append=%true>//tmp/t_in", [{"k": 2}, {"k": 4}])

        merge(
            in_=["//tmp/t_in", "//tmp/t_in"],
            out="//tmp/t_out",
            mode="sorted"
        )
        assert read_table("//tmp/t_out") == [{"k": 0}] * 2 + [{"k": 2}] * 8 + [{"k": 4}] * 2
