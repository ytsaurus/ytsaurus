from yt_env_setup import YTEnvSetup

from yt_commands import (
    assert_statistics, authors, extract_deprecated_statistic, extract_statistic_v2, wait,
    get, create, write_table, map,
    with_breakpoint, wait_breakpoint, release_breakpoint)

from yt.common import YtError

import pytest

##################################################################


class TestSchedulerUserStatistics(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_PORTO = True

    @authors("tramsmm")
    def test_job_statistics(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})
        op = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"mapper": {"custom_statistics_count_limit": 3}},
            command='cat; echo "{ cpu={ k1=4; k3=7 }}; {k2=-7};{k2=1};" >&5',
        )

        def check(statistics, extract_statistic):
            assert extract_statistic(
                statistics,
                key="custom.cpu.k1",
                job_state="completed",
                job_type="map",
                summary_type="max") == 4
            assert extract_statistic(
                statistics,
                key="custom.k2",
                job_state="completed",
                job_type="map",
                summary_type="count") == 2
            assert extract_statistic(
                statistics,
                key="custom.k2",
                job_state="completed",
                job_type="map",
                summary_type="max") == 1

        statistics_v2 = get(op.get_path() + "/@progress/job_statistics_v2")
        check(statistics_v2, extract_statistic_v2)

        deprecated_statistics = get(op.get_path() + "/@progress/job_statistics")
        check(deprecated_statistics, extract_deprecated_statistic)

    @authors("max42")
    def test_tricky_names(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        # Keys with special symbols not allowed inside YPath are ok (they are represented as is).
        op = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"max_failed_job_count": 1},
            command='cat; echo "{\\"name/with/slashes\\"={\\"@table_index\\"=42}}">&5',
        )

        statistics_v2 = get(op.get_path() + "/@progress/job_statistics_v2")
        assert extract_statistic_v2(
            statistics_v2,
            key="custom.name/with/slashes.@table_index",
            job_state="completed",
            job_type="map",
            summary_type="max") == 42

        deprecated_statistics = get(op.get_path() + "/@progress/job_statistics")
        assert extract_deprecated_statistic(
            deprecated_statistics,
            key="custom.name/with/slashes.@table_index",
            job_state="completed",
            job_type="map",
            summary_type="max") == 42

        # But the empty keys are not ok (as well as for any other map nodes).
        with pytest.raises(YtError):
            op = map(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={"max_failed_job_count": 1},
                command='cat; echo "{\\"\\"=42}">&5',
            )

    @authors("tramsmm", "acid")
    def test_name_is_too_long(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        long_name = "a" * 2048

        with pytest.raises(YtError):
            map(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={"max_failed_job_count": 1},
                command='cat; echo "{ ' + long_name + '=42};">&5',
            )

    @authors("tramsmm")
    def test_too_many_custom_statistics(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        custom_statistics_count_limit = 16
        write_line = ""
        for i in range(custom_statistics_count_limit + 1):
            write_line += 'echo "{ name' + str(i) + '=42};">&5;'

        with pytest.raises(YtError):
            map(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={
                    "max_failed_job_count": 1,
                    "mapper": {"custom_statistics_count_limit": custom_statistics_count_limit},
                },
                command="cat; " + write_line,
            )

    @authors("tramsmm")
    def test_multiple_job_statistics(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"} for i in range(2)])

        op = map(in_="//tmp/t1", out="//tmp/t2", command="cat", spec={"job_count": 2})

        assert_statistics(op, "data.input.unmerged_data_weight", lambda weight: weight == 2, summary_type="count")

    @authors("babenko")
    def test_job_statistics_progress(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"} for i in range(2)])

        op = map(
            track=False,
            label="job_statistics_progress",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat > /dev/null ; BREAKPOINT ;"),
            spec={"max_failed_job_count": 1, "job_count": 2},
        )

        jobs = wait_breakpoint()
        release_breakpoint(job_id=jobs[0])

        def get_counter():
            statistics = get(op.get_path() + "/@progress/job_statistics_v2")
            return extract_statistic_v2(statistics, "data.input.unmerged_data_weight", summary_type="count")

        wait(lambda: get_counter() == 1)

        release_breakpoint()
        op.track()

        assert get_counter() == 2
