import pytest
import yt.yson as yson

from yt_env_setup import YTEnvSetup, unix_only
from yt_commands import *

from time import sleep

##################################################################

class TestSchedulerAutoMerge(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 16
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_jobs_update_period": 10,
            "job_revival_abort_timeout": 2000,
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 3000,
            "operations_update_period": 10,
            "chunk_unstage_period": 10,
        }
    }

    DELTA_MASTER_CONFIG = {
        "object_manager": {
            "gc_sweep_period": 10
        }
    }

    def _create_account(self, chunk_count):
        create_account("acc")
        set("//sys/accounts/acc/@resource_limits/chunk_count", chunk_count)

    def _track_and_report_peak_chunk_count(self, op, with_revive=False):
        peak_chunk_count = 0
        i = 0
        while True:
            state = op.get_state()
            if state == "completed":
                break
            if op.get_state() == "failed":
                op.track() # this should raise an exception
            current_chunk_count = get("//sys/accounts/acc/@resource_usage/chunk_count")
            peak_chunk_count = max(peak_chunk_count, current_chunk_count)
            sleep(0.5)
            if with_revive:
                i += 1
                if i == 20:
                    self.Env.kill_schedulers()
                    self.Env.start_schedulers()
                    i = 0
        print >>sys.stderr, "peak_chunk_count =", peak_chunk_count

    # Bugs in auto-merge usually lead to the operation being stuck without scheduling any new jobs.
    # This is why we use the pytest timeout decorator.
    @pytest.mark.timeout(480)
    @pytest.mark.parametrize("op_type", ["map", "reduce"])
    def test_auto_merge_does_not_stuck(self, op_type):
        create("table", "//tmp/t_in", attributes={"schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}]})
        create("table", "//tmp/t_out")

        parameters = [
            [1, 1, 1],
            [9, 1, 1],
            [9, 9, 1],
            [9, 9, 9],
            [9, 4, 2],
            [16, 5, 3],
            [100, 28, 14],
            [9, 20, 15],
            [9, 20, 3],
        ]

        run_op = map if op_type == "map" else reduce

        for row_count, max_intermediate_chunk_count, chunk_count_per_merge_job in parameters:
            write_table("//tmp/t_in", [{"a" : i} for i in range(row_count)],
                        max_row_buffer_size=1,
                        table_writer={"desired_chunk_size": 1})

            assert get("//tmp/t_in/@chunk_count") == row_count

            op = run_op(
                dont_track=True,
                in_="//tmp/t_in",
                out="//tmp/t_out",
                command="cat",
                reduce_by=["a"],
                spec={
                    "auto_merge": {
                        "mode": "manual",
                        "max_intermediate_chunk_count": max_intermediate_chunk_count,
                        "chunk_count_per_merge_job": chunk_count_per_merge_job
                    },
                    "data_size_per_job": 1
                })
            op.track()
            assert get("//tmp/t_out/@chunk_count") == \
                   (row_count - 1) // min(chunk_count_per_merge_job, max_intermediate_chunk_count) + 1
            assert get("//tmp/t_out/@row_count") == row_count

    @pytest.mark.timeout(480)
    @pytest.mark.parametrize("op_type", ["map", "reduce"])
    def test_account_chunk_limit(self, op_type):
        self._create_account(60)

        create("table", "//tmp/t_in", attributes={"schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}]})
        create("table", "//tmp/t_out", attributes={"account": "acc"})

        row_count = 300
        write_table("//tmp/t_in", [{"a" : i} for i in range(row_count)],
                    max_row_buffer_size=1,
                    table_writer={"desired_chunk_size": 1})

        assert get("//tmp/t_in/@chunk_count") == row_count

        run_op = map if op_type == "map" else reduce
        op = run_op(
            dont_track=True,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            reduce_by=["a"], # ignored for maps
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "max_intermediate_chunk_count": 35,
                    "chunk_count_per_merge_job": 20,
                },
                "data_size_per_job": 1
            })

        self._track_and_report_peak_chunk_count(op)

        assert get("//tmp/t_out/@row_count") == row_count

    def test_several_auto_merge_output_tables(self):
        self._create_account(35)

        create("table", "//tmp/t_in", attributes={"account": "acc"})
        create("table", "//tmp/t_out1", attributes={"account": "acc"})
        create("table", "//tmp/t_out2", attributes={"account": "acc"})

        row_count = 100
        write_table("//tmp/t_in", [{"a" : i} for i in range(row_count)])

        op = map(
            dont_track=True,
            in_="//tmp/t_in",
            out=["//tmp/t_out1", "//tmp/t_out2"],
            command="read x; echo $x >&$(($x % 2 * 3 + 1))",
            format="<columns=[a]>schemaful_dsv",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "max_intermediate_chunk_count": 20,
                    "chunk_count_per_merge_job": 15,
                },
                "mapper": {
                    "format": yson.loads("<columns=[a]>schemaful_dsv")
                },
                "data_size_per_job": 1
            })

        self._track_and_report_peak_chunk_count(op)

        assert get("//tmp/t_out1/@row_count") == row_count // 2
        assert get("//tmp/t_out2/@row_count") == row_count // 2

    @pytest.mark.timeout(480)
    @pytest.mark.parametrize("with_revive", [True, False])
    def test_only_auto_merge_output_table(self, with_revive):
        chunk_limit = 1000 if with_revive else 40
        self._create_account(chunk_limit)

        create("table", "//tmp/t_in", attributes={"account": "acc"})
        create("table", "//tmp/t_out1", attributes={"account": "acc"})
        create("table", "//tmp/t_out2", attributes={"account": "acc"})

        row_count = 100
        write_table("//tmp/t_in", [{"a" : i} for i in range(row_count)])

        op = map(
            dont_track=True,
            in_="//tmp/t_in",
            out=["<auto_merge=%false>//tmp/t_out1", "//tmp/t_out2"],
            command="read x; if [[ $(($x % 10)) == 0 ]]; then echo $x >&1; else echo $x >&4; fi",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "max_intermediate_chunk_count": 20,
                    "chunk_count_per_merge_job": 15,
                },
                "mapper": {
                    "format": yson.loads("<columns=[a]>schemaful_dsv")
                },
                "data_size_per_job": 1
            })

        self._track_and_report_peak_chunk_count(op, with_revive=with_revive)

        assert get("//tmp/t_out1/@row_count") == row_count // 10
        assert get("//tmp/t_out2/@row_count") == row_count * 9 // 10


    @pytest.mark.timeout(240)
    def test_auto_merge_with_schema_and_append(self):
        schema_in = make_schema([
            {
                "name": "a",
                "type": "int64",
                "sort_order": "ascending",
            },
            {
                "name": "b",
                "type": "string"
            }
        ], unique_keys=False, strict=True)
        create("table", "//tmp/t_in", attributes={"schema": schema_in})

        schema_out = make_schema([
            {
                "name": "a",
                "type": "int64",
            },
            {
                "name": "b",
                "type": "string"
            }
        ], unique_keys=False, strict=True)
        create("table", "//tmp/t_out", attributes={"schema": schema_out})

        data = [{"a": i, "b" : str(i * i)} for i in range(10)];
        write_table("<append=%true>//tmp/t_in", data)

        init_content = [{"a": -1, "b": "1"}]
        write_table("<append=%true>//tmp/t_out", init_content)

        op = map(
            in_="//tmp/t_in",
            out=["<append=%true>//tmp/t_out"],
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "max_intermediate_chunk_count": 4,
                    "chunk_count_per_merge_job": 2,
                },
                "data_size_per_job": 1
            })

        assert get("//tmp/t_out/@row_count") == 11
        assert get("//tmp/t_out/@chunk_count") == 6
        content = read_table("//tmp/t_out")
        assert sorted(content[1:]) == sorted(data)
        assert content[:1] == init_content
        assert get("//tmp/t_out/@schema") == schema_out

    @pytest.mark.timeout(60)
    def test_teleport_large_chunks(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        for i in range(10):
            write_table("<append=%true>//tmp/t_in", [{"a": i}])

        # For even lines output a long random string, for odd lines output a single character.
        op = map(
            in_="//tmp/t_in",
            out=["//tmp/t_out"],
            command="read x; if [[ $(($x % 2)) == 0 ]]; then head -c 1000000 /dev/urandom | base64 -w 0; echo -ne '\n'; else echo $x; fi",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "max_intermediate_chunk_count": 50,
                    "chunk_count_per_merge_job": 50,
                    "chunk_size_threshold": 100 * 1024
                },
                "data_size_per_job": 1,
                "mapper": {
                    "format": yson.loads("<columns=[a]>schemaful_dsv")
                },
            })
        assert get("//tmp/t_out/@chunk_count") == 6
        chunk_ids = get("//tmp/t_out/@chunk_ids")
        row_counts = []
        for chunk_id in chunk_ids:
            row_counts.append(get("#{0}/@row_count".format(chunk_id)))
        row_counts = sorted(row_counts)
        assert row_counts == [1, 1, 1, 1, 1, 5]

    @pytest.mark.timeout(60)
    def test_erasure_output(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        set("//tmp/t_out/@erasure_codec", "lrc_12_2_2")
        for i in range(10):
            write_table("<append=%true>//tmp/t_in", [{"a": i}])

        op = map(
            in_="//tmp/t_in",
            out=["//tmp/t_out"],
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "chunk_count_per_merge_job": 2,
                    "max_intermediate_chunk_count" : 100
                },
                "data_size_per_job": 1,
                "mapper": {
                    "format": yson.loads("<columns=[a]>schemaful_dsv")
                },
            })
        assert get("//tmp/t_out/@chunk_count") == 5
        chunk_ids = get("//tmp/t_out/@chunk_ids")
        for chunk_id in chunk_ids:
            assert get("#{0}/@erasure_codec".format(chunk_id)) == "lrc_12_2_2"

    @pytest.mark.timeout(60)
    def test_replicated_and_compressed_output(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        set("//tmp/t_out/@replication_factor", 5)
        set("//tmp/t_out/@compression_codec", "zstd_17")
        for i in range(10):
            write_table("<append=%true>//tmp/t_in", [{"a": i}])

        op = map(
            in_="//tmp/t_in",
            out=["//tmp/t_out"],
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "chunk_count_per_merge_job": 2,
                    "max_intermediate_chunk_count" : 100
                },
                "data_size_per_job": 1,
                "mapper": {
                    "format": yson.loads("<columns=[a]>schemaful_dsv")
                },
            })
        assert get("//tmp/t_out/@chunk_count") == 5
        chunk_ids = get("//tmp/t_out/@chunk_ids")
        for chunk_id in chunk_ids:
            assert get("#{0}/@media/default/replication_factor".format(chunk_id)) == 5
            assert get("#{0}/@compression_codec".format(chunk_id)) == "zstd_17"

    @pytest.mark.timeout(60)
    def test_row_count_limit_disables_auto_merge(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        for i in range(10):
            write_table("<append=%true>//tmp/t_in", [{"a": i}])

        op = map(
            in_="//tmp/t_in",
            out=["<row_count_limit=5>//tmp/t_out"],
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "chunk_count_per_merge_job": 4,
                    "max_intermediate_chunk_count" : 100
                },
                "data_size_per_job": 1,
                "mapper": {
                    "format": yson.loads("<columns=[a]>schemaful_dsv")
                },
            })
        assert get("//tmp/t_out/@chunk_count") >= 5

    @pytest.mark.timeout(60)
    def test_sorted_output_disables_auto_merge(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        schema_out = make_schema([
            {
                "name": "a",
                "type": "string",
                "sort_order": "ascending",
            },
        ], unique_keys=False, strict=True)
        create("table", "//tmp/t_out_with_schema", attributes={"schema": schema_out})

        for i in range(10):
            write_table("<append=%true>//tmp/t_in", [{"a": i}])

        op = map(
            in_="//tmp/t_in",
            out=["<sorted_by=[a]>//tmp/t_out"],
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "chunk_count_per_merge_job": 4,
                    "max_intermediate_chunk_count" : 100
                },
                "data_size_per_job": 1,
                "mapper": {
                    "format": yson.loads("<columns=[a]>schemaful_dsv")
                },
            })
        assert get("//tmp/t_out/@chunk_count") >= 5

        op = map(
            in_="//tmp/t_in",
            out=["//tmp/t_out_with_schema"],
            command="cat",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "chunk_count_per_merge_job": 4,
                    "max_intermediate_chunk_count" : 100
                },
                "data_size_per_job": 1,
                "mapper": {
                    "format": yson.loads("<columns=[a]>schemaful_dsv")
                },
            })
        assert get("//tmp/t_out_with_schema/@chunk_count") >= 5

    @pytest.mark.timeout(60)
    def test_live_preview(self):
        self._create_account(35)

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        row_count = 50
        write_table("//tmp/t_in", [{"a" : i} for i in range(row_count)])

        op = map(
            dont_track=True,
            in_="//tmp/t_in",
            out=["//tmp/t_out1", "//tmp/t_out2"],
            command="read x; echo $x >&$(($x % 2 * 3 + 1))",
            format="<columns=[a]>schemaful_dsv",
            spec={
                "auto_merge": {
                    "mode": "manual",
                    "max_intermediate_chunk_count": 20,
                    "chunk_count_per_merge_job": 15,
                },
                "mapper": {
                    "format": yson.loads("<columns=[a]>schemaful_dsv")
                },
                "data_size_per_job": 1
            })

        # We check that each of 4 possible live previews was non-empty at some moment.
        live_preview_appeared = {"map": [False, False], "auto_merge": [False, False]}

        while True:
            state = op.get_state(verbose=False)
            if state == "completed":
                break
            elif state == "failed":
                op.track() # this should raise an exception
            for i in range(2):
                for vertex in live_preview_appeared:
                    path = op.get_path() + "/orchid/data_flow_graph/vertices/{0}/live_previews/{1}".format(vertex, i)
                    try:
                        if not exists(path, verbose=False):
                            continue
                        data = read_table(path, verbose=False, table_reader={"unavailable_chunk_strategy": "skip"})
                        if len(data) > 0 and not live_preview_appeared[vertex][i]:
                            print >>sys.stderr, "Live preview of type {0} and index {1} appeared".format(vertex, i)
                        live_preview_appeared[vertex][i] = True
                    except YtError:
                        pass
            time.sleep(0.5)
            print >>sys.stderr, "{0} jobs completed".format(op.get_job_count("completed"))

            if all(live_preview_appeared["map"]) and all(live_preview_appeared["auto_merge"]):
                break

        op.track()
