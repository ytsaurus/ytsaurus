from yt_env_setup import YTEnvSetup, make_schema, unix_only, wait

from yt_commands import *

import pytest

import itertools
import time


def get_stderr_spec(stderr_file):
    return {
        "stderr_table_path": stderr_file,
    }


def get_stderr_dict_from_cypress(operation_id):
    jobs_path = "//sys/operations/{0}/jobs".format(operation_id)
    result = {}
    for job_id, job_content in get(jobs_path).iteritems():
        if "stderr" in job_content:
            stderr_path = "{0}/{1}/stderr".format(jobs_path, job_id)
            result[job_id] = read_file(stderr_path)
    return result


def get_stderr_dict_from_table(table_path):
    result = {}
    stderr_rows = read_table("//tmp/t_stderr")
    for job_id, part_iter in itertools.groupby(stderr_rows, key=lambda x: x["job_id"]):
        job_stderr = ""
        for row in part_iter:
            job_stderr += row["data"]
        result[job_id] = job_stderr
    return result


def compare_stderr_table_and_files(stderr_table_path, operation_id):
    assert get_stderr_dict_from_table("//tmp/t_stderr") == get_stderr_dict_from_cypress(operation_id)


def expect_to_find_in_stderr_table(stderr_table_path, content):
    assert get("{0}/@sorted".format(stderr_table_path))
    assert get("{0}/@sorted_by".format(stderr_table_path)) == ["job_id", "part_index"]
    table_row_list = list(read_table(stderr_table_path))
    assert sorted(row["data"] for row in table_row_list) == sorted(content)
    job_id_list = [row['job_id'] for row in table_row_list]
    assert sorted(job_id_list) == job_id_list


class TestStderrTable(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    @unix_only
    def test_map(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("//tmp/t_input", [{"key": i} for i in xrange(3)])

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="echo GG >&2 ; cat",
            spec=get_stderr_spec("//tmp/t_stderr"),
        )

        expect_to_find_in_stderr_table("//tmp/t_stderr", ["GG\n"])
        compare_stderr_table_and_files("//tmp/t_stderr", op.id)

    @unix_only
    def test_ordered_map(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("""<sorted_by=["key"]>//tmp/t_input""", [{"key": i} for i in xrange(3)])

        op = map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="echo GG >&2 ; cat",
            spec=get_stderr_spec("//tmp/t_stderr"),
            ordered=True
        )

        expect_to_find_in_stderr_table("//tmp/t_stderr", ["GG\n"])
        compare_stderr_table_and_files("//tmp/t_stderr", op.id)

    @unix_only
    def test_reduce(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("""<sorted_by=["key"]>//tmp/t_input""", [{"key": i} for i in xrange(3)])

        op = reduce(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="echo REDUCE > /dev/stderr ; cat",
            reduce_by=["key"],
            spec=get_stderr_spec("//tmp/t_stderr"),
        )

        expect_to_find_in_stderr_table("//tmp/t_stderr", ["REDUCE\n"])
        compare_stderr_table_and_files("//tmp/t_stderr", op.id)

    @unix_only
    def test_join_reduce(self):
        create("table", "//tmp/t_foreign")
        create("table", "//tmp/t_primary")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("""<sorted_by=["host"]>//tmp/t_foreign""",
                    [{"host": "bar"},
                     {"host": "baz"},
                     {"host": "foo"}])

        write_table("""<sorted_by=["host";"path"]>//tmp/t_primary""",
                    [{"host": "bar", "path": "/"},
                     {"host": "bar", "path": "/1"},
                     {"host": "bar", "path": "/2"},
                     {"host": "baz", "path": "/"},
                     {"host": "baz", "path": "/1"},
                     {"host": "foo", "path": "/"}])

        op = reduce(
            in_=["<foreign=true>//tmp/t_foreign", "//tmp/t_primary"],
            out="//tmp/t_output",
            command="echo REDUCE >&2 ; cat > /dev/null",
            join_by=["host"],
            reduce_by=["host", "path"],
            spec=get_stderr_spec("//tmp/t_stderr"),
        )

        expect_to_find_in_stderr_table("//tmp/t_stderr", ["REDUCE\n"])
        compare_stderr_table_and_files("//tmp/t_stderr", op.id)

    @unix_only
    def test_map_reduce(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("//tmp/t_input", [{"key": i} for i in xrange(3)])

        op = map_reduce(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            mapper_command="echo FOO >&2 ; cat",
            reducer_command="echo BAR >&2 ; cat",
            sort_by=["key"],
            spec=get_stderr_spec("//tmp/t_stderr")
        )

        expect_to_find_in_stderr_table("//tmp/t_stderr", ["FOO\n", "BAR\n"])
        compare_stderr_table_and_files("//tmp/t_stderr", op.id)

    @unix_only
    def test_map_reduce_only_reduce(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("//tmp/t_input", [{"key": i} for i in xrange(3)])

        op = map_reduce(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            reducer_command="echo BAZ >&2 ; cat",
            sort_by=["key"],
            spec=get_stderr_spec("//tmp/t_stderr")
        )

        expect_to_find_in_stderr_table("//tmp/t_stderr", ["BAZ\n"])
        compare_stderr_table_and_files("//tmp/t_stderr", op.id)

    @unix_only
    def test_map_combine_reduce(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("//tmp/t_input", [{"key": i} for i in xrange(100)])

        op = map_reduce(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            mapper_command="echo MAPPER >&2 ; cat",
            reducer_command="echo REDUCER >&2 ; cat",
            reduce_combiner_command="echo COMBINER >&2 ; cat",
            sort_by=["key"],
            spec={
                "stderr_table_path": "//tmp/t_stderr",
                "partition_count": 2,
                "map_job_count": 2,
                "data_size_per_sort_job": 10,
            }
        )

        expect_to_find_in_stderr_table(
            "//tmp/t_stderr", [
                "MAPPER\n", "MAPPER\n",
                "COMBINER\n", "COMBINER\n",
                "REDUCER\n"])
        compare_stderr_table_and_files("//tmp/t_stderr", op.id)

    @unix_only
    def test_failed_jobs(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("""<sorted_by=["key"]>//tmp/t_input""", [{"key": i} for i in xrange(3)])

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_input",
                out="//tmp/t_output",
                command="echo EPIC_FAIL >&2 ; exit 1",
                spec={
                    "stderr_table_path": "//tmp/t_stderr",
                    "max_failed_job_count": 2,
                }
            )

        stderr_rows = read_table("//tmp/t_stderr")
        assert [row["data"] for row in stderr_rows] == ["EPIC_FAIL\n"] * 2
        assert get("//tmp/t_stderr/@sorted")
        assert get("//tmp/t_stderr/@sorted_by") == ["job_id", "part_index"]

    @unix_only
    def test_append_stderr_prohibited(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("""<sorted_by=["key"]>//tmp/t_input""", [{"key": i} for i in xrange(3)])

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_input",
                out="//tmp/t_output",
                command="echo EPIC_FAIL >&2 ; cat",
                spec={
                    "stderr_table_path": "<append=true>//tmp/t_stderr",
                    "max_failed_job_count": 2,
                }
            )

    @unix_only
    def test_failing_write(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("""<sorted_by=["key"]>//tmp/t_input""", [{"key": i} for i in xrange(3)])

        with pytest.raises(YtError):
            # We set max_part_size to 10MB and max_row_weight to 5MB and write 20MB of stderr.
            map(
                in_="//tmp/t_input",
                out="//tmp/t_output",
                command="""python -c 'import sys; s = "x" * (20 * 1024 * 1024) ; sys.stderr.write(s)'""",
                spec={
                    "stderr_table_path": "//tmp/t_stderr",
                    "stderr_table_writer_config": {
                        "max_row_weight": 5 * 1024 * 1024,
                        "max_part_size": 10 * 1024 * 1024,
                    },
                }
            )


    @unix_only
    def test_max_part_size(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("""<sorted_by=["key"]>//tmp/t_input""", [{"key": i} for i in xrange(1)])

        map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="""python -c 'import sys; s = "x" * (30 * 1024 * 1024) ; sys.stderr.write(s)'""",
            spec={
                "stderr_table_path": "//tmp/t_stderr",
                "stderr_table_writer_config": {
                    "max_row_weight": 128 * 1024 * 1024,
                    "max_part_size": 40 * 1024 * 1024,
                },
            }
        )

    @unix_only
    def test_big_stderr(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")
        write_table("""<sorted_by=["key"]>//tmp/t_input""", [{"key": 0}])

        map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="""python -c 'import sys; s = "x " * (30 * 1024 * 1024) ; sys.stderr.write(s)'""",
            spec=get_stderr_spec("//tmp/t_stderr"),
        )
        stderr_rows = read_table("//tmp/t_stderr", verbose=False)
        assert len(stderr_rows) > 1

        for item in stderr_rows:
            assert item["job_id"] == stderr_rows[0]["job_id"]

        assert str("".join(item["data"] for item in stderr_rows)) == str("x " * (30 * 1024 * 1024))

    @unix_only
    def test_scheduler_revive(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        create("table", "//tmp/t_stderr")

        # NOTE all values are of same size so our chunks are also of the same size so our
        # scheduler can split them evenly
        write_table("//tmp/t_input",               [{"key": "complete_before_scheduler_dies  "}])
        write_table("<append=%true>//tmp/t_input", [{"key": "complete_after_scheduler_restart"}])
        write_table("<append=%true>//tmp/t_input", [{"key": "complete_while_scheduler_dead   "}])

        events = EventsOnFs()

        op = map(
            dont_track=True,
            waiting_jobs=True,
            command=(
                # one job completes before scheduler is dead
                # second job completes while scheduler is dead
                # third one completes after scheduler restart
                "cat > input\n"
                     "grep complete_while_scheduler_dead input >/dev/null "
                     "  && {wait_scheduler_dead} "
                     "  && echo complete_while_scheduler_dead >&2\n"
                     "grep complete_after_scheduler_restart input >/dev/null "
                     "  && {wait_scheduler_restart} "
                     "  && echo complete_after_scheduler_restart >&2\n"
                     "grep complete_before_scheduler_dies input >/dev/null "
                     "  && echo complete_before_scheduler_dies >&2\n"
                     "cat input"
            ).format(
                wait_scheduler_dead=events.wait_event_cmd("scheduler_dead"),
                wait_scheduler_restart=events.wait_event_cmd("scheduler_restart")),
            format="dsv",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "job_count": 3,
                "data_size_per_job": 1,
                "max_failed_job_count": 1,
                "stderr_table_path": "//tmp/t_stderr",
            }
        )
        assert op.get_job_count("total") == 3
        op.resume_jobs()

        wait(lambda: op.get_job_count("running") == 2);

        self.Env.kill_schedulers()

        events.notify_event("scheduler_dead")

        # Wait some time to give `complete_while_scheduler_dead'-job time to complete.
        time.sleep(1)

        self.Env.start_schedulers()
        events.notify_event("scheduler_restart")
        op.track()

        stderr_rows = read_table("//tmp/t_stderr")
        assert sorted(row["data"] for row in stderr_rows) == ["complete_after_scheduler_restart\n",
                                                              "complete_before_scheduler_dies\n",
                                                              "complete_while_scheduler_dead\n"]
        assert get("//tmp/t_stderr/@sorted")
        assert get("//tmp/t_stderr/@sorted_by") == ["job_id", "part_index"]
