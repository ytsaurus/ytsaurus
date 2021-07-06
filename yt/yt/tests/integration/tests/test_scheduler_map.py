from yt_env_setup import (
    YTEnvSetup,
    wait,
    skip_if_porto,
    is_asan_build,
)

from yt_commands import (  # noqa
    authors, print_debug, wait, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists,
    create_account, create_network_project, create_tmpdir, create_user, create_group,
    create_pool, create_pool_tree,
    create_data_center, create_rack,
    make_ace, check_permission, add_member,
    make_batch_request, execute_batch, get_batch_error,
    start_transaction, abort_transaction, lock,
    insert_rows, select_rows, delete_rows, alter_table,
    read_file, write_file, read_table, write_table, write_local_file,
    map, reduce, map_reduce, join_reduce, merge, vanilla, sort, erase,
    run_test_vanilla, run_sleeping_vanilla,
    abort_job, list_jobs, get_job, abandon_job, interrupt_job,
    get_job_fail_context, get_job_input, get_job_stderr, get_job_spec,
    dump_job_context, poll_job_shell,
    abort_op, complete_op, suspend_op, resume_op,
    get_operation, list_operations, clean_operations,
    get_operation_cypress_path, scheduler_orchid_pool_path,
    scheduler_orchid_default_pool_tree_path, scheduler_orchid_operation_path,
    scheduler_orchid_default_pool_tree_config_path, scheduler_orchid_path,
    scheduler_orchid_node_path, scheduler_orchid_pool_tree_config_path,
    sync_create_cells, sync_mount_table, sync_unmount_table,
    get_first_chunk_id, get_singular_chunk_id, multicell_sleep,
    update_nodes_dynamic_config, update_controller_agent_config,
    update_op_parameters, enable_op_detailed_logs,
    set_node_banned, set_banned_flag, set_account_disk_space_limit,
    check_all_stderrs,
    create_test_tables, PrepareTables,
    get_statistics,
    make_random_string, raises_yt_error)

from yt_type_helpers import make_schema, normalize_schema

from yt_helpers import skip_if_no_descending

import yt.yson as yson
from yt.test_helpers import assert_items_equal
from yt.common import YtError

from flaky import flaky

import pytest
import random
import string
import base64

##################################################################


class TestSchedulerMapCommands(YTEnvSetup):
    NUM_TEST_PARTITIONS = 12
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_jobs_update_period": 10,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 10,
            "map_operation_options": {
                "job_splitter": {
                    "min_job_time": 5000,
                    "min_total_data_size": 1024,
                    "update_period": 100,
                    "candidate_percentile": 0.8,
                    "max_jobs_per_split": 3,
                    "max_input_table_count": 5,
                },
            },
            # COMPAT(shakurov): change the default to false and remove
            # this delta once masters are up to date.
            "enable_prerequisites_for_starting_completion_transactions": False,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "resource_limits": {
                    "user_slots": 5,
                    "cpu": 5,
                    "memory": 5 * 1024 ** 3,
                }
            }
        }
    }

    @authors("ignat")
    def test_empty_table(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        map(in_="//tmp/t1", out="//tmp/t2", command="cat")

        assert read_table("//tmp/t2") == []

    @authors("babenko")
    def test_no_outputs(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", [{"key": "value"}])
        op = map(in_="//tmp/t1", command="cat > /dev/null; echo stderr>&2")
        check_all_stderrs(op, "stderr\n", 1)

    @authors("acid", "ignat")
    def test_empty_range(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        original_data = [{"index": i} for i in xrange(10)]
        write_table("//tmp/t1", original_data)

        command = "cat"
        map(
            in_="<ranges=[{lower_limit={row_index=1}; upper_limit={row_index=1}}]>//tmp/t1",
            out="//tmp/t2",
            command=command,
        )

        assert [] == read_table("//tmp/t2", verbose=False)

    @authors("ignat")
    def test_one_chunk(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})
        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=r'cat; echo "{v1=\"$V1\"};{v2=\"$TMPDIR\"}"',
            spec={
                "mapper": {"environment": {"V1": "Some data", "TMPDIR": "$(SandboxPath)/mytmp"}},
                "title": "MyTitle",
            },
        )

        get(op.get_path() + "/@spec")
        op.track()

        res = read_table("//tmp/t2")
        assert len(res) == 3
        assert res[0] == {"a": "b"}
        assert res[1] == {"v1": "Some data"}
        assert "v2" in res[2]
        assert res[2]["v2"].endswith("/mytmp")
        assert res[2]["v2"].startswith("/")

    @authors("ignat")
    @pytest.mark.skipif(is_asan_build(), reason="Test is too slow to fit into timeout")
    def test_big_input(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        count = 1000 * 1000
        original_data = [{"index": i} for i in xrange(count)]
        write_table("//tmp/t1", original_data)

        command = "cat"
        map(in_="//tmp/t1", out="//tmp/t2", command=command)

        new_data = read_table("//tmp/t2", verbose=False)
        assert sorted(row.items() for row in new_data) == [[("index", i)] for i in xrange(count)]

    @authors("ignat")
    def test_two_outputs_at_the_same_time(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output1")
        create("table", "//tmp/t_output2")

        count = 1000
        original_data = [{"index": i} for i in xrange(count)]
        write_table("//tmp/t_input", original_data)

        file = "//tmp/some_file.txt"
        create("file", file)
        write_file(file, "{value=42};\n")

        command = 'bash -c "cat <&0 & sleep 0.1; cat some_file.txt >&4; wait;"'
        map(
            in_="//tmp/t_input",
            out=["//tmp/t_output1", "//tmp/t_output2"],
            command=command,
            file=[file],
            verbose=True,
        )

        assert read_table("//tmp/t_output2") == [{"value": 42}]
        assert sorted([row.items() for row in read_table("//tmp/t_output1")]) == [[("index", i)] for i in xrange(count)]

    @authors("ignat")
    def test_write_two_outputs_consistently(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output1")
        create("table", "//tmp/t_output2")

        count = 10000
        original_data = [{"index": i} for i in xrange(count)]
        write_table("//tmp/t_input", original_data)

        file1 = "//tmp/some_file.txt"
        create("file", file1)
        write_file(file1, "}}}}};\n")

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_input",
                out=["//tmp/t_output1", "//tmp/t_output2"],
                command='cat some_file.txt >&4; cat >&4; echo "{value=42}"',
                file=[file1],
                verbose=True,
            )

    @authors("psushin")
    def test_in_equal_to_out(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"foo": "bar"})

        map(in_="//tmp/t1", out="<append=true>//tmp/t1", command="cat")

        assert read_table("//tmp/t1") == [{"foo": "bar"}, {"foo": "bar"}]

    @authors("psushin")
    def test_input_row_count(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"key": i} for i in xrange(5)])

        sort(in_="//tmp/t1", out="//tmp/t1", sort_by="key")
        op = map(command="cat", in_="//tmp/t1[:1]", out="//tmp/t2")

        assert get("//tmp/t2/@row_count") == 1

        row_count = get(op.get_path() + "/@progress/job_statistics/data/input/row_count/$/completed/map/sum")
        assert row_count == 1

    @authors("psushin")
    def test_multiple_output_row_count(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")
        write_table("//tmp/t1", [{"key": i} for i in xrange(5)])

        op = map(
            command="cat; echo {hello=world} >&4",
            in_="//tmp/t1",
            out=["//tmp/t2", "//tmp/t3"],
        )
        assert get("//tmp/t2/@row_count") == 5
        row_count = get(op.get_path() + "/@progress/job_statistics/data/output/0/row_count/$/completed/map/sum")
        assert row_count == 5
        row_count = get(op.get_path() + "/@progress/job_statistics/data/output/1/row_count/$/completed/map/sum")
        assert row_count == 1

    @authors("renadeen")
    def test_codec_statistics(self):
        create("table", "//tmp/t1", attributes={"compression_codec": "lzma_9"})
        create("table", "//tmp/t2", attributes={"compression_codec": "lzma_1"})

        def random_string(n):
            return "".join(random.choice(string.printable) for _ in xrange(n))

        write_table(
            "//tmp/t1", [{str(i): random_string(1000)} for i in xrange(100)]
        )  # so much to see non-zero decode CPU usage in release mode

        op = map(command="cat", in_="//tmp/t1", out="//tmp/t2")
        decode_time = get(op.get_path() + "/@progress/job_statistics/codec/cpu/decode/lzma_9/$/completed/map/sum")
        encode_time = get(op.get_path() + "/@progress/job_statistics/codec/cpu/encode/0/lzma_1/$/completed/map/sum")
        assert decode_time > 0
        assert encode_time > 0

    @authors("psushin")
    @pytest.mark.parametrize("sort_kind", ["sorted_by", "ascending", "descending"])
    def test_sorted_output(self, sort_kind):
        if sort_kind == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t1")
        for i in xrange(2):
            write_table("<append=true>//tmp/t1", {"key": "foo", "value": "ninja"})

        echoed_rows = ["{key=$k1; value=one}", "{key=$k2; value=two}"]
        if sort_kind == "descending":
            echoed_rows = echoed_rows[::-1]

        command = """cat >/dev/null;
           if [ "$YT_JOB_INDEX" = "0" ]; then
               k1=0; k2=1;
           else
               k1=0; k2=0;
           fi
           echo "{}"
        """.format("; ".join(echoed_rows))

        attributes = {}
        if sort_kind == "sorted_by":
            out_table = "<sorted_by=[key];append=true>//tmp/t2"
        else:
            out_table = "<append=true>//tmp/t2"
            attributes["schema"] = [{"name": "key", "sort_order": sort_kind, "type": "int64"},
                                    {"name": "value", "type": "string"}]
        create("table", "//tmp/t2", attributes=attributes)

        map(
            in_="//tmp/t1",
            out=out_table,
            command=command,
            spec={"job_count": 2},
        )

        expected_rows = [
            {"key": 0, "value": "one"},
            {"key": 0, "value": "two"},
            {"key": 0, "value": "one"},
            {"key": 1, "value": "two"},
        ]

        if sort_kind == "descending":
            expected_rows = expected_rows[::-1]

        assert get("//tmp/t2/@sorted")
        assert get("//tmp/t2/@sorted_by") == ["key"]
        assert read_table("//tmp/t2") == expected_rows

    @authors("psushin")
    @pytest.mark.parametrize("sort_kind", ["sorted_by", "ascending", "descending"])
    def test_sorted_output_overlap(self, sort_kind):
        if sort_kind == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t1")
        for i in xrange(2):
            write_table("<append=true>//tmp/t1", {"key": "foo", "value": "ninja"})

        echoed_rows = ["{key=1; value=one}", "{key=2; value=two}"]
        if sort_kind == "descending":
            echoed_rows = echoed_rows[::-1]

        command = 'cat >/dev/null; echo "{}"'.format("; ".join(echoed_rows))

        attributes = {}
        if sort_kind == "sorted_by":
            out_table = "<sorted_by=[key];append=true>//tmp/t2"
        else:
            out_table = "<append=true>//tmp/t2"
            attributes["schema"] = [{"name": "key", "sort_order": sort_kind, "type": "int64"},
                                    {"name": "value", "type": "string"}]
        create("table", "//tmp/t2", attributes=attributes)

        with pytest.raises(YtError):
            map(
                in_="//tmp/t1",
                out=out_table,
                command=command,
                spec={"job_count": 2},
            )

    @authors("psushin")
    @pytest.mark.parametrize("sort_kind", ["sorted_by", "ascending", "descending"])
    def test_sorted_output_job_failure(self, sort_kind):
        if sort_kind == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t1")
        for i in xrange(2):
            write_table("<append=true>//tmp/t1", {"key": "foo", "value": "ninja"})

        echoed_rows = ["{key=2; value=one}", "{key=1; value=two}"]
        if sort_kind == "descending":
            echoed_rows = echoed_rows[::-1]

        command = 'cat >/dev/null; echo "{}"'.format("; ".join(echoed_rows))

        attributes = {}
        if sort_kind == "sorted_by":
            out_table = "<sorted_by=[key];append=true>//tmp/t2"
        else:
            out_table = "<append=true>//tmp/t2"
            attributes["schema"] = [{"name": "key", "sort_order": sort_kind, "type": "int64"},
                                    {"name": "value", "type": "string"}]
        create("table", "//tmp/t2", attributes=attributes)

        with pytest.raises(YtError):
            map(
                in_="//tmp/t1",
                out=out_table,
                command=command,
                spec={"job_count": 2},
            )

    @authors("psushin")
    def test_job_count(self):
        create("table", "//tmp/t1")
        for i in xrange(5):
            write_table("<append=true>//tmp/t1", {"foo": "bar"})

        command = "cat > /dev/null; echo {hello=world}"

        def check(table_name, job_count, expected_num_records):
            create("table", table_name)
            map(
                in_="//tmp/t1",
                out=table_name,
                command=command,
                spec={"job_count": job_count},
            )
            assert read_table(table_name) == [{"hello": "world"} for _ in range(expected_num_records)]

        check("//tmp/t2", 3, 3)
        check("//tmp/t3", 10, 5)  # number of jobs cannot be more than number of rows.

    @authors("max42")
    def test_skewed_rows(self):
        create("table", "//tmp/t1")
        # 5 small rows
        write_table("<append=true>//tmp/t1", [{"foo": "bar"}] * 5)
        # and one large row
        write_table("<append=true>//tmp/t1", {"foo": "".join(["r"] * 1024)})

        create("table", "//tmp/t2")
        map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat > /dev/null; echo {hello=world}",
            spec={"job_count": 6},
        )
        assert read_table("//tmp/t2") == [{"hello": "world"} for _ in xrange(6)]

    # We skip this one in Porto because it requires a lot of interaction with Porto
    # (since there are a lot of operations with large number of jobs).
    # There is completely nothing Porto-specific here.
    @authors("ignat")
    @skip_if_porto
    def test_job_per_row(self):
        create("table", "//tmp/input")

        job_count = 976
        original_data = [{"index": str(i)} for i in xrange(job_count)]
        write_table("//tmp/input", original_data)

        create("table", "//tmp/output", ignore_existing=True)

        for job_count in xrange(976, 950, -1):
            op = map(
                track=False,
                in_="//tmp/input",
                out="//tmp/output",
                command="sleep 100",
                spec={"job_count": job_count},
            )

            wait(lambda: len(op.get_running_jobs()) > 1)

            assert op.get_job_count("total") == job_count
            op.abort()

    def run_many_output_tables(self, yamr_mode=False):
        output_tables = ["//tmp/t%d" % i for i in range(3)]

        create("table", "//tmp/t_in")
        for table_path in output_tables:
            create("table", table_path)

        write_table("//tmp/t_in", {"a": "b"})

        if yamr_mode:
            mapper = "cat  > /dev/null; echo {v = 0} >&3; echo {v = 1} >&4; echo {v = 2} >&5"
        else:
            mapper = "cat  > /dev/null; echo {v = 0} >&1; echo {v = 1} >&4; echo {v = 2} >&7"

        create("file", "//tmp/mapper.sh")
        write_file("//tmp/mapper.sh", mapper)

        map(
            in_="//tmp/t_in",
            out=output_tables,
            command="bash mapper.sh",
            file="//tmp/mapper.sh",
            spec={"mapper": {"use_yamr_descriptors": yamr_mode}},
        )

        assert read_table(output_tables[0]) == [{"v": 0}]
        assert read_table(output_tables[1]) == [{"v": 1}]
        assert read_table(output_tables[2]) == [{"v": 2}]

    @authors("ignat")
    def test_many_output_yt(self):
        self.run_many_output_tables()

    @authors("ignat")
    def test_many_output_yamr(self):
        self.run_many_output_tables(True)

    @authors("ignat")
    def test_output_tables_switch(self):
        output_tables = ["//tmp/t%d" % i for i in range(3)]

        create("table", "//tmp/t_in")
        for table_path in output_tables:
            create("table", table_path)

        write_table("//tmp/t_in", {"a": "b"})
        mapper = 'cat  > /dev/null; echo "<table_index=2>#;{v = 0};{v = 1};<table_index=0>#;{v = 2}"'

        create("file", "//tmp/mapper.sh")
        write_file("//tmp/mapper.sh", mapper)

        map(
            in_="//tmp/t_in",
            out=output_tables,
            command="bash mapper.sh",
            file="//tmp/mapper.sh",
        )

        assert read_table(output_tables[0]) == [{"v": 2}]
        assert read_table(output_tables[1]) == []
        assert read_table(output_tables[2]) == [{"v": 0}, {"v": 1}]

    @authors("ignat")
    def test_executable_mapper(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        mapper = """
#!/bin/bash
cat > /dev/null; echo {hello=world}
"""

        create("file", "//tmp/mapper.sh")
        write_file("//tmp/mapper.sh", mapper)

        set("//tmp/mapper.sh/@executable", True)

        create("table", "//tmp/t_out")
        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="./mapper.sh",
            file="//tmp/mapper.sh",
        )

        assert read_table("//tmp/t_out") == [{"hello": "world"}]

    @authors("ignat")
    def test_table_index(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/out")

        write_table("//tmp/t1", {"key": "a", "value": "value"})
        write_table("//tmp/t2", {"key": "b", "value": "value"})

        mapper = """
import sys
table_index = sys.stdin.readline().strip()
row = sys.stdin.readline().strip()
print row + table_index

table_index = sys.stdin.readline().strip()
row = sys.stdin.readline().strip()
print row + table_index
"""

        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        map(
            in_=["//tmp/t1", "//tmp/t2"],
            out="//tmp/out",
            command="python mapper.py",
            file="//tmp/mapper.py",
            spec={"mapper": {"format": yson.loads("<enable_table_index=true>yamr")}},
        )

        expected = [{"key": "a", "value": "value0"}, {"key": "b", "value": "value1"}]
        assert_items_equal(read_table("//tmp/out"), expected)

    @authors("ignat")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_range_index(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t_in", attributes={
            "schema": make_schema([
                {"name": "key", "type": "string", "sort_order": sort_order},
                {"name": "value", "type": "string", "sort_order": sort_order},
            ])})
        create("table", "//tmp/out")

        rows = [0, 1, 2]
        if sort_order == "descending":
            rows = rows[::-1]
        for row in rows:
            write_table(
                "<append=true>//tmp/t_in",
                [
                    {"key": "%05d" % row, "value": "value"},
                ],
            )

        if sort_order == "ascending":
            t_in = '<ranges=['\
                '{lower_limit={key=["00002"]};upper_limit={key=["00003"]}};'\
                '{lower_limit={key=["00002"]};upper_limit={key=["00003"]}}'\
                ']>//tmp/t_in'
        else:
            t_in = '<ranges=['\
                '{lower_limit={key=["00002"]};upper_limit={key=["00001"]}};'\
                '{lower_limit={key=["00002"]};upper_limit={key=["00001"]}}'\
                ']>//tmp/t_in'

        op = map(
            track=False,
            in_=[t_in],
            out="//tmp/out",
            command="cat >& 2",
            spec={
                "job_io": {
                    "control_attributes": {
                        "enable_range_index": True,
                        "enable_row_index": True,
                    }
                },
                "mapper": {
                    "input_format": yson.loads("<format=text>yson"),
                    "output_format": "dsv",
                },
            },
        )

        op.track()
        check_all_stderrs(op, '"range_index"=0;', 1, substring=True)
        check_all_stderrs(op, '"range_index"=1;', 1, substring=True)

    @authors("ogorod")
    def test_insane_demand(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", {"cool": "stuff"})

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                command="cat",
                spec={"mapper": {"memory_limit": 1000000000000}},
            )

    def check_input_fully_consumed_statistics_base(
        self, cmd, throw_on_failure=False, expected_value=1, expected_output=None
    ):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"foo": "bar"} for _ in xrange(10000)])

        op = map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            track=False,
            command=cmd,
            spec={
                "mapper": {
                    "input_format": "dsv",
                    "output_format": "dsv",
                    "max_failed_job_count": 1,
                    "check_input_fully_consumed": throw_on_failure,
                }
            },
        )

        statistics_path = op.get_path() + "/@progress/job_statistics/data/input/not_fully_consumed/$/{}/map/max".format(
            "failed" if throw_on_failure else "completed"
        )
        if throw_on_failure:
            with pytest.raises(YtError):
                op.track()
        else:
            op.track()

        if expected_output is not None:
            assert read_table("//tmp/t_out") == expected_output
        assert get(statistics_path) == expected_value

    @authors("ogorod")
    def test_check_input_fully_consumed_statistics_simple(self):
        self.check_input_fully_consumed_statistics_base("python3 -c 'print(input())'", expected_output=[{"foo": "bar"}])

    @authors("acid", "ogorod")
    def test_check_input_fully_consumed_statistics_all_consumed(self):
        self.check_input_fully_consumed_statistics_base("cat", expected_value=0)

    @authors("tramsmm", "ogorod")
    def test_check_input_fully_consumed_statistics_throw_on_failure(self):
        self.check_input_fully_consumed_statistics_base("exit 0", throw_on_failure=True, expected_output=[])

    @authors("max42")
    def test_live_preview(self):
        create_user("u")

        data = [{"foo": i} for i in range(5)]

        create("table", "//tmp/t1")
        write_table("//tmp/t1", data)

        create("table", "//tmp/t2")
        set("//tmp/t2/@acl", [make_ace("allow", "u", "write")])
        effective_acl = get("//tmp/t2/@effective_acl")

        schema = make_schema(
            [{"name": "foo", "type": "int64", "required": False}],
            strict=True,
            unique_keys=False,
        )
        alter_table("//tmp/t2", schema=schema)

        op = map(
            track=False,
            command=with_breakpoint("cat && BREAKPOINT"),
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"data_size_per_job": 1},
        )
        jobs = wait_breakpoint(job_count=2)

        operation_path = op.get_path()
        async_transaction_id = get(operation_path + "/@async_scheduler_transaction_id")
        assert exists(operation_path + "/output_0", tx=async_transaction_id)
        assert effective_acl == get(operation_path + "/output_0/@acl", tx=async_transaction_id)
        assert schema == normalize_schema(get(operation_path + "/output_0/@schema", tx=async_transaction_id))

        for job_id in jobs[:2]:
            release_breakpoint(job_id=job_id)

        wait(lambda: op.get_job_count("completed") >= 2)

        def check():
            live_preview_data = read_table(operation_path + "/output_0", tx=async_transaction_id)
            return len(live_preview_data) == 2 and all(record in data for record in live_preview_data)

        wait(check)

        release_breakpoint()
        op.track()
        assert sorted(read_table("//tmp/t2")) == sorted(data)

    @authors("max42")
    def test_new_live_preview_multiple_output_tables(self):
        create_user("u")

        data = [{"foo": i} for i in range(5)]

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", data)

        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        op = map(
            track=False,
            command=with_breakpoint('echo "{a=$YT_JOB_INDEX}" >&1; echo "{b=$YT_JOB_INDEX}" >&4; BREAKPOINT'),
            in_="//tmp/t_in",
            out=["//tmp/t_out1", "//tmp/t_out2"],
            spec={"data_size_per_job": 1},
        )

        jobs = wait_breakpoint(job_count=2)
        operation_path = op.get_path()
        for job_id in jobs[:2]:
            release_breakpoint(job_id=job_id)

        wait(lambda: op.get_job_count("completed") == 2)
        wait(lambda: exists(operation_path + "/controller_orchid"))

        live_preview_data1 = read_table(
            operation_path + "/controller_orchid/data_flow_graph/vertices/map/live_previews/0"
        )
        live_preview_data2 = read_table(
            operation_path + "/controller_orchid/data_flow_graph/vertices/map/live_previews/1"
        )
        live_preview_data1 = [d["a"] for d in live_preview_data1]
        live_preview_data2 = [d["b"] for d in live_preview_data2]
        assert sorted(live_preview_data1) == sorted(live_preview_data2)
        assert len(live_preview_data1) == 2

        release_breakpoint()
        op.track()

    @authors("max42", "ignat")
    def test_row_sampling(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")

        count = 1000
        original_data = [{"index": i} for i in xrange(count)]
        write_table("//tmp/t1", original_data)

        command = "cat"
        sampling_rate = 0.5
        spec = {"job_io": {"table_reader": {"sampling_seed": 42, "sampling_rate": sampling_rate}}}

        map(in_="//tmp/t1", out="//tmp/t2", command=command, spec=spec)
        map(in_="//tmp/t1", out="//tmp/t3", command=command, spec=spec)

        new_data_t2 = read_table("//tmp/t2", verbose=False)
        new_data_t3 = read_table("//tmp/t3", verbose=False)

        assert sorted(row.items() for row in new_data_t2) == sorted(row.items() for row in new_data_t3)

        actual_rate = len(new_data_t2) * 1.0 / len(original_data)
        variation = sampling_rate * (1 - sampling_rate)
        assert sampling_rate - variation <= actual_rate <= sampling_rate + variation

    @authors("psushin")
    @pytest.mark.parametrize("ordered", [False, True])
    def test_map_row_count_limit(self, ordered):
        create("table", "//tmp/input")
        for i in xrange(5):
            write_table("<append=true>//tmp/input", {"key": "%05d" % i, "value": "foo"})

        create("table", "//tmp/output")
        op = map(
            track=False,
            in_="//tmp/input",
            out="<row_count_limit=3>//tmp/output",
            command=with_breakpoint("cat ; BREAKPOINT"),
            ordered=ordered,
            spec={"data_size_per_job": 1, "max_failed_job_count": 1},
        )
        jobs = wait_breakpoint(job_count=5)
        assert len(jobs) == 5

        for job_id in jobs[:3]:
            release_breakpoint(job_id=job_id)

        op.track()
        assert len(read_table("//tmp/output")) == 3

    @authors("psushin")
    def test_job_controller_orchid(self):
        create("table", "//tmp/input")
        for i in xrange(5):
            write_table("<append=true>//tmp/input", {"key": "%05d" % i, "value": "foo"})

        create("table", "//tmp/output")
        op = map(
            track=False,
            in_="//tmp/input",
            out="//tmp/output",
            command=with_breakpoint("cat && BREAKPOINT"),
            spec={"data_size_per_job": 1, "max_failed_job_count": 1},
        )
        wait_breakpoint(job_count=5)

        for n in get("//sys/cluster_nodes"):
            scheduler_jobs = get("//sys/cluster_nodes/{0}/orchid/job_controller/active_jobs/scheduler".format(n))
            for job_id, values in scheduler_jobs.items():
                assert "start_time" in values
                assert "operation_id" in values
                assert "statistics" in values
                assert "job_type" in values
                assert "duration" in values

            slot_manager = get("//sys/cluster_nodes/{0}/orchid/job_controller/slot_manager".format(n))
            assert "free_slot_count" in slot_manager
            assert "slot_count" in slot_manager

        op.abort()

    @authors("psushin")
    def test_map_row_count_limit_second_output(self):
        create("table", "//tmp/input")
        for i in xrange(5):
            write_table("<append=true>//tmp/input", {"key": "%05d" % i, "value": "foo"})

        create("table", "//tmp/out_1")
        create("table", "//tmp/out_2")
        op = map(
            track=False,
            in_="//tmp/input",
            out=["//tmp/out_1", "<row_count_limit=3>//tmp/out_2"],
            command=with_breakpoint("cat >&4 ; BREAKPOINT"),
            spec={"data_size_per_job": 1, "max_failed_job_count": 1},
        )

        jobs = wait_breakpoint(job_count=5)
        for job_id in jobs[:3]:
            release_breakpoint(job_id=job_id)

        op.track()
        assert len(read_table("//tmp/out_1")) == 0
        assert len(read_table("//tmp/out_2")) == 3

    @authors("psushin")
    def test_multiple_row_count_limit(self):
        create("table", "//tmp/input")
        create("table", "//tmp/out_1")
        create("table", "//tmp/out_2")
        with pytest.raises(YtError):
            map(
                in_="//tmp/input",
                out=[
                    "<row_count_limit=1>//tmp/out_1",
                    "<row_count_limit=1>//tmp/out_2",
                ],
                command="cat",
            )

    @authors("gritukan")
    def test_negative_row_count_limit(self):
        create("table", "//tmp/input")
        create("table", "//tmp/output")
        with pytest.raises(YtError):
            map(
                in_="//tmp/input",
                out=[
                    "<row_count_limit=-1>//tmp/output",
                ],
                command="cat",
            )

    @authors("psushin")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_schema_validation(self, optimize_for):
        schema = make_schema(
            [
                {"name": "key", "type": "int64", "required": False},
                {"name": "value", "type": "string", "required": False},
            ],
            strict=True,
            unique_keys=False,
        )
        create("table", "//tmp/input")
        create(
            "table",
            "//tmp/output",
            attributes={"optimize_for": optimize_for, "schema": schema},
        )
        create("table", "//tmp/output2")

        for i in xrange(10):
            write_table("<append=true>//tmp/input", {"key": i, "value": "foo"})

        map(in_="//tmp/input", out="//tmp/output", command="cat")

        assert get("//tmp/output/@schema_mode") == "strong"
        assert get("//tmp/output/@schema/@strict")
        assert normalize_schema(get("//tmp/output/@schema")) == schema
        assert_items_equal(read_table("//tmp/output"), [{"key": i, "value": "foo"} for i in xrange(10)])

        map(
            in_="//tmp/input",
            out="<schema=%s>//tmp/output2" % yson.dumps(schema),
            command="cat",
        )

        assert get("//tmp/output2/@schema_mode") == "strong"
        assert get("//tmp/output2/@schema/@strict")
        assert normalize_schema(get("//tmp/output2/@schema")) == schema
        assert_items_equal(
            read_table("//tmp/output2"),
            [{"key": i, "value": "foo"} for i in xrange(10)],
        )

        write_table("//tmp/input", {"key": "1", "value": "foo"})

        with pytest.raises(YtError):
            map(in_="//tmp/input", out="//tmp/output", command="cat")

    @authors("dakovalkov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_rename_columns_simple(seld, optimize_for):
        create(
            "table",
            "//tmp/tin",
            attributes={
                "schema": [{"name": "a", "type": "int64"}],
                "optimize_for": optimize_for,
            },
        )
        create("table", "//tmp/tout")
        write_table("//tmp/tin", [{"a": 42}])

        map(in_="<rename_columns={a=b}>//tmp/tin", out="//tmp/tout", command="cat")
        assert read_table("//tmp/tout") == [{"b": 42}]

    @authors("dakovalkov")
    def test_rename_columns_without_schema(self):
        create("table", "//tmp/tin")
        create("table", "//tmp/tout")
        write_table("//tmp/tin", [{"a": 42}])

        with pytest.raises(YtError):
            map(in_="<rename_columns={a=b}>//tmp/tin", out="//tmp/tout", command="cat")

    @authors("dakovalkov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_raname_columns_overlapping_names_in_schema(self, optimize_for):
        create(
            "table",
            "//tmp/tin",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "b", "type": "int64"},
                ],
                "optimize_for": optimize_for,
            },
        )
        create("table", "//tmp/tout")
        write_table("//tmp/tin", [{"a": 42, "b": 34}])

        with pytest.raises(YtError):
            map(in_="<rename_columns={a=b}>//tmp/tin", out="//tmp/tout", command="cat")

    @authors("dakovalkov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_rename_columns_overlapping_names_in_chunk(self, optimize_for):
        create("table", "//tmp/tin", attributes={"optimize_for": optimize_for})
        create("table", "//tmp/tout")
        write_table("//tmp/tin", [{"a": 42, "b": 34}])

        # Set weak schema
        sort(in_="//tmp/tin", out="//tmp/tin", sort_by="a")

        with pytest.raises(YtError):
            map(in_="<rename_columns={a=b}>//tmp/tin", out="//tmp/tout", command="cat")

    @authors("dakovalkov")
    def test_rename_columns_wrong_name(self):
        create(
            "table",
            "//tmp/tin",
            attributes={
                "schema": [{"name": "a", "type": "int64"}],
            },
        )
        create("table", "//tmp/tout")
        write_table("//tmp/tin", [{"a": 42}])
        with pytest.raises(YtError):
            map(
                in_='<rename_columns={a="$wrong_name"}>//tmp/tin',
                out="//tmp/tout",
                command="cat",
            )

        with pytest.raises(YtError):
            map(in_='<rename_columns={a=""}>//tmp/tin', out="//tmp/tout", command="cat")

        with pytest.raises(YtError):
            map(
                in_="<rename_columns={a=" + "b" * 1000 + "}>//tmp/tin",
                out="//tmp/tout",
                command="cat",
            )

    @authors("dakovalkov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_rename_columns_swap(self, optimize_for):
        create(
            "table",
            "//tmp/tin",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "b", "type": "int64"},
                ],
                "optimize_for": optimize_for,
            },
        )
        create("table", "//tmp/tout")
        write_table("//tmp/tin", [{"a": 42, "b": 34}])

        map(in_="<rename_columns={a=b;b=a}>//tmp/tin", out="//tmp/tout", command="cat")

        assert read_table("//tmp/tout") == [{"b": 42, "a": 34}]

    @authors("dakovalkov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_rename_columns_filter(self, optimize_for):
        create(
            "table",
            "//tmp/tin",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "b", "type": "int64"},
                ],
                "optimize_for": optimize_for,
            },
        )
        create("table", "//tmp/tout")
        write_table("//tmp/tin", [{"a": 42, "b": 34}])

        map(in_="<rename_columns={a=d}>//tmp/tin{d}", out="//tmp/tout", command="cat")

        assert read_table("//tmp/tout") == [{"d": 42}]

    @authors("evgenstf")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_two_tables_with_column_filter(self, optimize_for):
        create(
            "table",
            "//tmp/input_table_1",
            attributes={
                "schema": [
                    {"name": "first_column_1", "type": "int64"},
                    {"name": "second_column_1", "type": "int64"},
                    {"name": "third_column_1", "type": "int64"},
                ],
                "optimize_for": optimize_for,
            },
        )
        write_table(
            "//tmp/input_table_1",
            [{"first_column_1": 1001, "second_column_1": 2001, "third_column_1": 3001}],
        )

        create(
            "table",
            "//tmp/input_table_2",
            attributes={
                "schema": [
                    {"name": "first_column_2", "type": "int64"},
                    {"name": "second_column_2", "type": "int64"},
                    {"name": "third_column_2", "type": "int64"},
                ],
                "optimize_for": optimize_for,
            },
        )
        write_table(
            "//tmp/input_table_2",
            [{"first_column_2": 1002, "second_column_2": 2002, "third_column_2": 3002}],
        )

        create(
            "table",
            "//tmp/input_table_3",
            attributes={
                "schema": [
                    {"name": "first_column_2", "type": "int64"},
                    {"name": "second_column_2", "type": "int64"},
                    {"name": "third_column_2", "type": "int64"},
                ],
                "optimize_for": optimize_for,
            },
        )
        write_table(
            "//tmp/input_table_3",
            [{"first_column_2": 1002, "second_column_2": 2002, "third_column_2": 3002}],
        )

        create("table", "//tmp/output_table_1")
        create("table", "//tmp/output_table_2")
        create("table", "//tmp/output_table_3")

        map(
            in_=[
                '<columns=["first_column_1";"third_column_1"]>//tmp/input_table_1',
                '<columns=["first_column_2";"second_column_2"]>//tmp/input_table_2',
                '<columns=["first_column_2";"second_column_2"]>//tmp/input_table_3',
            ],
            out=[
                "//tmp/output_table_1",
                "//tmp/output_table_2",
                "//tmp/output_table_3",
            ],
            command="cat",
        )

        assert read_table("//tmp/output_table_1") == [{"first_column_1": 1001, "third_column_1": 3001}]
        assert read_table("//tmp/output_table_2") == [{"first_column_2": 1002, "second_column_2": 2002}]
        assert read_table("//tmp/output_table_3") == [{"first_column_2": 1002, "second_column_2": 2002}]

    @authors("lukyan")
    @pytest.mark.parametrize("mode", ["unordered", "ordered"])
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

        write_table("//tmp/t1", [{"k2": i} for i in xrange(2)])

        map(mode=mode, in_="//tmp/t1", out="//tmp/t2", command="cat")

        assert get("//tmp/t2/@schema_mode") == "strong"
        assert read_table("//tmp/t2") == [{"k1": i * 2, "k2": i} for i in xrange(2)]

    @authors("psushin", "ignat")
    def test_map_max_data_size_per_job(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("//tmp/t_input", {"foo": "bar"})

        op = map(
            track=False,
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="cat",
            spec={"max_data_size_per_job": 1},
        )

        with pytest.raises(YtError):
            op.track()

    @authors("psushin")
    def test_ordered_map_multiple_ranges(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        rows = [
            {"field": 1},
            {"field": 42},
            {"field": 63},
            {"field": 100500},
        ]
        write_table("<sorted_by=[field]>//tmp/t_input", rows)

        map(
            ordered=True,
            in_="//tmp/t_input[1,42,63,100500]",
            out="<sorted_by=[field]>//tmp/t_output",
            command="cat",
        )

        assert read_table("//tmp/t_output") == rows

    @authors("klyachin")
    @pytest.mark.parametrize("ordered", [False, True])
    def test_map_interrupt_job(self, ordered):
        create("table", "//tmp/in_1")
        write_table(
            "//tmp/in_1",
            [{"key": "%08d" % i, "value": "(t_1)", "data": "a" * (2 * 1024 * 1024)} for i in range(3)],
            table_writer={"block_size": 1024, "desired_chunk_size": 1024},
        )

        output = "//tmp/output"
        job_type = "map"
        if ordered:
            output = "<sorted_by=[key]>" + output
            job_type = "ordered_map"
        create("table", output)

        op = map(
            ordered=ordered,
            track=False,
            label="interrupt_job",
            in_="//tmp/in_1",
            out=output,
            command=with_breakpoint("""read; echo "${REPLY/(???)/(job)}"; echo "$REPLY" ; BREAKPOINT ; cat"""),
            spec={
                "mapper": {"format": "dsv"},
                "max_failed_job_count": 1,
                "job_io": {
                    "buffer_row_count": 1,
                },
                "enable_job_splitting": False,
            },
        )

        jobs = wait_breakpoint()
        interrupt_job(jobs[0])
        release_breakpoint()
        op.track()

        result = read_table("//tmp/output", verbose=False)
        for row in result:
            print_debug("key:", row["key"], "value:", row["value"])
        assert len(result) == 5
        if not ordered:
            result.sort()
        row_index = 0
        job_indexes = []
        for row in result:
            assert row["key"] == "%08d" % row_index
            if row["value"] == "(job)":
                job_indexes.append(int(row["key"]))
            else:
                row_index += 1
        assert 0 < job_indexes[1] < 99999
        assert (
            get(op.get_path() + "/@progress/job_statistics/data/input/row_count/$/completed/{}/sum".format(job_type))
            == len(result) - 2
        )

    @authors("dakovalkov", "gritukan")
    @pytest.mark.xfail(run=False, reason="YT-14467")
    @flaky(max_runs=3)
    def test_map_soft_interrupt_job(self):
        create_test_tables(row_count=1)

        command = """(trap "echo '{interrupt=42}'; exit 0" SIGINT; BREAKPOINT;)"""

        op = map(
            track=False,
            command=with_breakpoint(command),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "mapper": {
                    "interruption_signal": "SIGINT",
                },
            },
        )

        jobs = wait_breakpoint()
        interrupt_job(jobs[0])
        op.track()

        assert read_table("//tmp/t_out") == [{"interrupt": 42}]

    # YT-6324: false job interrupt when it does not consume any input data.
    @authors("klyachin")
    @pytest.mark.parametrize("ordered", [False, True])
    def test_map_no_consumption(self, ordered):
        create("table", "//tmp/in_1")
        write_table(
            "//tmp/in_1",
            [{"key": "%08d" % i, "value": "(t_1)", "data": "a" * (2 * 1024 * 1024)} for i in range(3)],
            table_writer={"block_size": 1024, "desired_chunk_size": 1024},
        )

        output = "//tmp/output"
        if ordered:
            output = "<sorted_by=[key]>" + output
        create("table", output)

        op = map(
            ordered=ordered,
            track=False,
            label="interrupt_job",
            in_="//tmp/in_1",
            out=output,
            command="true",
            spec={
                "mapper": {"format": "dsv"},
                "max_failed_job_count": 1,
                "job_io": {
                    "buffer_row_count": 1,
                },
                "enable_job_splitting": False,
            },
        )
        op.track()

        assert get(op.get_path() + "/@progress/jobs/completed/total") == 1
        assert get(op.get_path() + "/@progress/jobs/completed/non-interrupted") == 1

    @authors("savrus")
    def test_ordered_map_many_jobs(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        original_data = [{"index": i} for i in xrange(10)]
        for row in original_data:
            write_table("<append=true>//tmp/t_input", row)

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="cat; echo stderr 1>&2",
            ordered=True,
            spec={"data_size_per_job": 1},
        )

        assert get(op.get_path() + "/jobs/@count") == 10
        assert read_table("//tmp/t_output") == original_data

    @authors("max42", "savrus")
    @pytest.mark.parametrize("with_output_schema", [False, True])
    def test_ordered_map_remains_sorted(self, with_output_schema):
        create(
            "table",
            "//tmp/t_input",
            attributes={"schema": [{"name": "key", "sort_order": "ascending", "type": "int64"}]},
        )
        create("table", "//tmp/t_output")
        original_data = [{"key": i} for i in xrange(1000)]
        for i in xrange(10):
            write_table("<append=true>//tmp/t_input", original_data[100 * i:100 * (i + 1)])

        map(
            in_="//tmp/t_input",
            out="<sorted_by=[key]>//tmp/t_output" if with_output_schema else "//tmp/t_output",
            command="cat; sleep $(($RANDOM % 5)); echo stderr 1>&2",
            ordered=True,
            spec={"job_count": 5},
        )

        if with_output_schema:
            assert get("//tmp/t_output/@sorted")
            assert get("//tmp/t_output/@sorted_by") == ["key"]
        assert read_table("//tmp/t_output") == original_data

    # This is a really strange case that was added after YT-7507.
    @authors("max42")
    def test_ordered_map_job_count_consider_only_primary_size(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        for i in xrange(20):
            write_table("<append=true>//tmp/t_input", [{"a": "x" * 1024 * 1024}])

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            ordered=True,
            command="cat >/dev/null; echo stderr 1>&2",
            spec={"job_count": 10, "consider_only_primary_size": True},
        )

        jobs = get(op.get_path() + "/jobs/@count")
        assert jobs == 10

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
            label="split_job",
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
        assert sorted(got) == sorted(expected)

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
            in_=[input_] * 10,
            out=output,
            command="sleep 5; echo '{a=1}'")
        op.track()

        completed = get(op.get_path() + "/@progress/jobs/completed")
        interrupted = completed["interrupted"]
        assert interrupted["job_split"] == 0

    @authors("ifsmirnov")
    def test_disallow_partially_sorted_output(self):
        create(
            "table",
            "//tmp/t",
            attributes={"schema": [{"name": "key", "type": "int64", "sort_order": "ascending"}]},
        )
        write_table("//tmp/t", [{"key": 1}])

        with pytest.raises(YtError):
            map(in_="//tmp/t", out="<partially_sorted=%true>//tmp/t", command="cat")

    @authors("gritukan")
    def test_data_flow(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        def get_directions(op):
            data_flow = get(op.get_path() + "/@progress/data_flow")
            directions = {}
            for direction in data_flow:
                directions[(direction["source_name"], direction["target_name"])] = direction
            return directions

        op = map(track=False, in_="//tmp/t1", out="//tmp/t2", command="cat")
        op.track()

        directions = get_directions(op)
        assert len(directions) == 2
        assert directions[("input", "map")]["job_data_statistics"]["data_weight"] == 0
        assert directions[("input", "map")]["teleport_data_statistics"]["data_weight"] == 2
        assert directions[("map", "output")]["job_data_statistics"]["data_weight"] == 2
        assert directions[("map", "output")]["teleport_data_statistics"]["data_weight"] == 0

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat",
            spec={"auto_merge": {"mode": "relaxed"}},
        )
        op.track()
        assert read_table("//tmp/t2") == [{"a": "b"}]

        directions = get_directions(op)
        assert len(directions) == 3
        assert directions[("input", "map")]["job_data_statistics"]["data_weight"] == 0
        assert directions[("input", "map")]["teleport_data_statistics"]["data_weight"] == 2
        assert directions[("map", "auto_merge")]["job_data_statistics"]["data_weight"] == 2
        assert directions[("map", "auto_merge")]["teleport_data_statistics"]["data_weight"] == 0
        assert directions[("auto_merge", "output")]["job_data_statistics"]["data_weight"] == 2
        assert directions[("auto_merge", "output")]["teleport_data_statistics"]["data_weight"] == 0

    @authors("gritukan")
    def test_data_flow_graph(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        def get_edges(graph):
            result = {}
            for from_ in graph["edges"].keys():
                for to in graph["edges"][from_].keys():
                    result[(from_, to)] = graph["edges"][from_][to]

            return result

        op = map(track=False, in_="//tmp/t1", out="//tmp/t2", command="cat")
        op.track()

        data_flow_graph = get(op.get_path() + "/@progress/data_flow_graph")
        assert data_flow_graph["topological_ordering"] == ["source", "map", "sink"]

        edges = get_edges(data_flow_graph)
        assert len(edges) == 2
        assert edges[("source", "map")]["statistics"]["row_count"] == 1
        assert edges[("map", "sink")]["statistics"]["row_count"] == 1

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat",
            spec={"auto_merge": {"mode": "relaxed"}},
        )
        op.track()

        data_flow_graph = get(op.get_path() + "/@progress/data_flow_graph")
        assert data_flow_graph["topological_ordering"] == [
            "source",
            "map",
            "auto_merge",
            "sink",
        ]

        edges = get_edges(data_flow_graph)
        assert len(edges) == 3
        assert edges[("source", "map")]["statistics"]["row_count"] == 1
        assert edges[("map", "auto_merge")]["statistics"]["row_count"] == 1
        assert edges[("auto_merge", "sink")]["statistics"]["row_count"] == 1

    @authors("gritukan")
    def test_end_of_stream(self):
        create("table", "//tmp/in1")
        create("table", "//tmp/in2")
        create("table", "//tmp/out")

        write_table("//tmp/in1", [{"x": 1}])
        write_table("//tmp/in2", [{"x": 2}])

        op = map(
            ordered=True,
            in_=["//tmp/in1", "//tmp/in2"],
            out="//tmp/out",
            command="cat 1>&2",
            spec={
                "job_io": {"control_attributes": {"enable_end_of_stream": True}},
                "mapper": {"format": yson.loads("<format=text>yson")},
                "job_count": 1,
            },
        )

        jobs_path = op.get_path() + "/jobs"
        job_ids = ls(jobs_path)
        assert len(job_ids) == 1
        stderr_bytes = read_file("{0}/{1}/stderr".format(jobs_path, job_ids[0]))

        assert (
            stderr_bytes
            == """<"table_index"=0;>#;
{"x"=1;};
<"table_index"=1;>#;
{"x"=2;};
<"end_of_stream"=%true;>#;
"""
        )

    @authors("gritukan")
    def test_block_cache(self):
        if self.Env.get_component_version("ytserver-job-proxy").abi <= (20, 3):
            pytest.skip()
        if self.Env.get_component_version("ytserver-controller-agent").abi <= (20, 3):
            pytest.skip()

        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", [{"x": i, "y": "A" * 10000} for i in range(100)])

        input_tables = ["//tmp/in[#{}:#{}]".format(i, i + 1) for i in range(100)]

        op = map(
            in_=input_tables,
            out="//tmp/out",
            command="grep xx || true",
            spec={
                "job_count": 1,
                "job_io": {
                    "table_reader": {
                        "use_async_block_cache": True,
                    },
                    "block_cache": {
                        "compressed_data": {
                            "capacity": 1024 ** 2
                        },
                    },
                },
            },
        )
        op.track()

        statistics = get(op.get_path() + "/@progress/job_statistics")
        read_from_disk = get_statistics(
            statistics,
            "chunk_reader_statistics.data_bytes_read_from_disk.$.completed.map.sum")
        read_from_cache = get_statistics(
            statistics,
            "chunk_reader_statistics.data_bytes_read_from_cache.$.completed.map.sum")
        assert read_from_cache == 99 * read_from_disk

##################################################################


class TestSchedulerMapCommandsPorto(TestSchedulerMapCommands):
    USE_PORTO = True


##################################################################


class TestSchedulerMapCommandsMulticell(TestSchedulerMapCommands):
    NUM_TEST_PARTITIONS = 15
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("babenko")
    def test_multicell_input_fetch(self):
        create("table", "//tmp/t1", attributes={"external_cell_tag": 1})
        write_table("//tmp/t1", [{"a": 1}])
        create("table", "//tmp/t2", attributes={"external_cell_tag": 2})
        write_table("//tmp/t2", [{"a": 2}])

        create("table", "//tmp/t_in", attributes={"external": False})
        merge(mode="ordered", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t_in")

        create("table", "//tmp/t_out")
        map(in_="//tmp/t_in", out="//tmp/t_out", command="cat")

        assert_items_equal(read_table("//tmp/t_out"), [{"a": 1}, {"a": 2}])


##################################################################


class TestSchedulerMapCommandsPortal(TestSchedulerMapCommandsMulticell):
    ENABLE_TMP_PORTAL = True


class TestSchedulerMapCommandsShardedTx(TestSchedulerMapCommandsPortal):
    NUM_SECONDARY_MASTER_CELLS = 5
    MASTER_CELL_ROLES = {
        "0": ["cypress_node_host"],
        "1": ["cypress_node_host"],
        "2": ["chunk_host"],
        "3": ["cypress_node_host"],
        "4": ["transaction_coordinator"],
        "5": ["transaction_coordinator"],
    }


class TestSchedulerMapCommandsShardedTxNoBoomerangs(TestSchedulerMapCommandsShardedTx):
    def setup_method(self, method):
        super(TestSchedulerMapCommandsShardedTxNoBoomerangs, self).setup_method(method)
        set("//sys/@config/object_service/enable_mutation_boomerangs", False)
        set("//sys/@config/chunk_service/enable_mutation_boomerangs", False)


##################################################################


class TestJobSizeAdjuster(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {"controller_agent": {"map_operation_options": {"data_size_per_job": 1}}}

    @authors("max42")
    @pytest.mark.skipif("True", reason="YT-8228")
    def test_map_job_size_adjuster_boost(self):
        create("table", "//tmp/t_input")
        original_data = [{"index": "%05d" % i} for i in xrange(31)]
        for row in original_data:
            write_table("<append=true>//tmp/t_input", row, verbose=False)

        create("table", "//tmp/t_output")

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="echo lines=`wc -l`",
            spec={"mapper": {"format": "dsv"}, "resource_limits": {"user_slots": 1}},
        )

        expected = [{"lines": str(2 ** i)} for i in xrange(5)]
        actual = read_table("//tmp/t_output")
        assert_items_equal(actual, expected)
        estimated = get(op.get_path() + "/@progress/tasks/0/estimated_input_data_weight_histogram")
        histogram = get(op.get_path() + "/@progress/tasks/0/input_data_weight_histogram")
        assert estimated == histogram
        assert histogram["max"] / histogram["min"] == 16
        assert histogram["count"][0] == 1
        assert sum(histogram["count"]) == 5

    @authors("max42")
    def test_map_job_size_adjuster_max_limit(self):
        create("table", "//tmp/t_input")
        original_data = [{"index": "%05d" % i} for i in xrange(31)]
        for row in original_data:
            write_table("<append=true>//tmp/t_input", row, verbose=False)
        chunk_id = get_first_chunk_id("//tmp/t_input")
        chunk_size = get("#{0}/@data_weight".format(chunk_id))
        create("table", "//tmp/t_output")

        map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="echo lines=`wc -l`",
            spec={
                "mapper": {"format": "dsv"},
                "max_data_size_per_job": chunk_size * 4,
                "resource_limits": {"user_slots": 3},
            },
        )

        for row in read_table("//tmp/t_output"):
            assert int(row["lines"]) < 5

    @authors("ignat")
    def test_map_unavailable_chunk(self):
        create("table", "//tmp/t_input", attributes={"replication_factor": 1})
        original_data = [{"index": "%05d" % i} for i in xrange(20)]
        write_table("<append=true>//tmp/t_input", original_data[0], verbose=False)
        chunk_id = get_singular_chunk_id("//tmp/t_input")

        chunk_size = get("#{0}/@uncompressed_data_size".format(chunk_id))
        replicas = get("#{0}/@stored_replicas".format(chunk_id))
        assert len(replicas) == 1
        replica_to_ban = str(replicas[0])  # str() is for attribute stripping.

        banned = False
        for node in ls("//sys/cluster_nodes"):
            if node == replica_to_ban:
                set("//sys/cluster_nodes/{0}/@banned".format(node), True)
                banned = True
        assert banned

        wait(lambda: get("#{0}/@replication_status/default/lost".format(chunk_id)))

        for row in original_data[1:]:
            write_table("<append=true>//tmp/t_input", row, verbose=False)
        chunk_ids = get("//tmp/t_input/@chunk_ids")
        assert len(chunk_ids) == len(original_data)

        create("table", "//tmp/t_output")
        op = map(
            track=False,
            command="sleep $YT_JOB_INDEX; cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={"data_size_per_job": chunk_size * 2},
        )

        wait(lambda: op.get_job_count("completed") > 3)

        unbanned = False
        for node in ls("//sys/cluster_nodes"):
            if node == replica_to_ban:
                set("//sys/cluster_nodes/{0}/@banned".format(node), False)
                unbanned = True
        assert unbanned

        op.track()
        assert op.get_state() == "completed"


##################################################################


class TestInputOutputFormats(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_jobs_update_period": 10,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 10,
            "map_operation_options": {
                "job_splitter": {
                    "min_job_time": 5000,
                    "min_total_data_size": 1024,
                    "update_period": 100,
                    "candidate_percentile": 0.8,
                    "max_jobs_per_split": 3,
                },
            },
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "resource_limits": {
                    "user_slots": 5,
                    "cpu": 5,
                    "memory": 5 * 1024 ** 3,
                }
            }
        }
    }

    @authors("ignat")
    def test_tskv_input_format(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        mapper = """
import sys
input = sys.stdin.readline().strip('\\n').split('\\t')
assert input == ['tskv', 'foo=bar']
print '{hello=world}'

"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        create("table", "//tmp/t_out")
        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python mapper.py",
            file="//tmp/mapper.py",
            spec={"mapper": {"input_format": yson.loads("<line_prefix=tskv>dsv")}},
        )

        assert read_table("//tmp/t_out") == [{"hello": "world"}]

    @authors("ignat")
    def test_tskv_output_format(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        mapper = """
import sys
input = sys.stdin.readline().strip('\\n')
assert input == '<"table_index"=0;>#;'
input = sys.stdin.readline().strip('\\n')
assert input == '{"foo"="bar";};'
print "tskv" + "\\t" + "hello=world"
"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        create("table", "//tmp/t_out")
        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python mapper.py",
            file="//tmp/mapper.py",
            spec={
                "mapper": {
                    "enable_input_table_index": True,
                    "input_format": yson.loads("<format=text>yson"),
                    "output_format": yson.loads("<line_prefix=tskv>dsv"),
                }
            },
        )

        assert read_table("//tmp/t_out") == [{"hello": "world"}]

    @authors("ignat")
    def test_yamr_output_format(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        mapper = """
import sys
input = sys.stdin.readline().strip('\\n')
assert input == '{"foo"="bar";};'
print "key\\tsubkey\\tvalue"

"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        create("table", "//tmp/t_out")
        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python mapper.py",
            file="//tmp/mapper.py",
            spec={
                "mapper": {
                    "input_format": yson.loads("<format=text>yson"),
                    "output_format": yson.loads("<has_subkey=true>yamr"),
                }
            },
        )

        assert read_table("//tmp/t_out") == [{"key": "key", "subkey": "subkey", "value": "value"}]

    @authors("ignat")
    def test_yamr_input_format(self):
        create("table", "//tmp/t_in")
        write_table(
            "//tmp/t_in",
            {"value": "value", "subkey": "subkey", "key": "key", "a": "another"},
        )

        mapper = """
import sys
input = sys.stdin.readline().strip('\\n').split('\\t')
assert input == ['key', 'subkey', 'value']
print '{hello=world}'

"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        create("table", "//tmp/t_out")
        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="python mapper.py",
            file="//tmp/mapper.py",
            spec={"mapper": {"input_format": yson.loads("<has_subkey=true>yamr")}},
        )

        assert read_table("//tmp/t_out") == [{"hello": "world"}]

    @authors("ignat")
    def test_type_conversion(self):
        create("table", "//tmp/s")
        write_table("//tmp/s", {"foo": "42"})

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "int64", "type": "int64", "sort_order": "ascending"},
                        {"name": "uint64", "type": "uint64"},
                        {"name": "boolean", "type": "boolean"},
                        {"name": "double", "type": "double"},
                        {"name": "any", "type": "any"},
                    ],
                    strict=False,
                )
            },
        )

        row = '{int64=3u; uint64=42; boolean="false"; double=18; any={}; extra=qwe}'

        with pytest.raises(YtError):
            map(
                in_="//tmp/s",
                out="//tmp/t",
                command="echo '{0}'".format(row),
                spec={"max_failed_job_count": 1},
            )

        yson_with_type_conversion = yson.loads("<enable_type_conversion=%true>yson")
        map(
            in_="//tmp/s",
            out="//tmp/t",
            command="echo '{0}'".format(row),
            format=yson_with_type_conversion,
            spec={
                "max_failed_job_count": 1,
                "mapper": {"output_format": yson_with_type_conversion},
            },
        )

    @authors("max42")
    def test_invalid_row_indices(self):
        create("table", "//tmp/t_in")
        write_table("<append=%true>//tmp/t_in", [{"a": i} for i in range(10)])
        write_table("<append=%true>//tmp/t_in", [{"a": i} for i in range(10, 20)])

        create("table", "//tmp/t_out")

        # None of this operations should fail.

        map(in_="//tmp/t_in[#18:#2]", out="//tmp/t_out", command="cat")

        map(in_="//tmp/t_in[#8:#2]", out="//tmp/t_out", command="cat")

        map(in_="//tmp/t_in[#18:#12]", out="//tmp/t_out", command="cat")

        map(in_="//tmp/t_in[#8:#8]", out="//tmp/t_out", command="cat")

        map(in_="//tmp/t_in[#10:#10]", out="//tmp/t_out", command="cat")

        map(in_="//tmp/t_in[#12:#12]", out="//tmp/t_out", command="cat")

    @authors("max42")
    def test_ordered_several_ranges(self):
        # YT-11322
        create(
            "table",
            "//tmp/t_in",
            attributes={"schema": [{"name": "key", "type": "int64", "sort_order": "ascending"}]},
        )
        create("table", "//tmp/t_out")
        write_table("//tmp/t_in", [{"key": 0}, {"key": 1}, {"key": 2}])

        script = "\n".join(
            [
                "#!/usr/bin/python",
                "import sys",
                "import base64",
                "print '{out=\"' + base64.standard_b64encode(sys.stdin.read()) + '\"}'",
            ]
        )

        create("file", "//tmp/script.py", attributes={"executable": True})
        write_file("//tmp/script.py", script)

        expected_content = [
            {"key": 0, "table_index": 0, "row_index": 0, "range_index": 0},
            {"key": 1, "table_index": 0, "row_index": 1, "range_index": 1},
            {"key": 2, "table_index": 1, "row_index": 2, "range_index": 0},
        ]

        map(
            in_=[
                "<ranges=[{exact={row_index=0}};{exact={row_index=1}}]>//tmp/t_in",
                "<ranges=[{exact={row_index=2}}]>//tmp/t_in",
            ],
            out="//tmp/t_out",
            ordered=True,
            spec={
                "job_count": 1,
                "mapper": {
                    "file_paths": ["//tmp/script.py"],
                    "format": yson.loads("<format=text>yson"),
                },
                "max_failed_job_count": 1,
                "job_io": {
                    "control_attributes": {
                        "enable_range_index": True,
                        "enable_row_index": True,
                        "enable_table_index": True,
                    }
                },
            },
            command="./script.py",
        )

        job_input = read_table("//tmp/t_out")[0]["out"]
        job_input = base64.standard_b64decode(job_input)
        rows = yson.loads(job_input, yson_type="list_fragment")
        actual_content = []
        current_attrs = {}
        for row in rows:
            if type(row) == yson.yson_types.YsonEntity:
                for key, value in row.attributes.iteritems():
                    current_attrs[key] = value
            else:
                new_row = dict(row)
                for key, value in current_attrs.iteritems():
                    new_row[key] = value
                actual_content.append(new_row)

        assert actual_content == expected_content


##################################################################


class TestInputOutputFormatsMulticell(TestInputOutputFormats):
    NUM_SECONDARY_MASTER_CELLS = 2
