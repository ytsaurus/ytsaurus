import pytest

from yt_env_setup import YTEnvSetup, patch_porto_env_only, require_ytserver_root_privileges
from yt_commands import *

##################################################################

porto_delta_node_config = {
    "exec_agent": {
        "slot_manager": {
            "enforce_job_control": True,                              # <= 18.4
            "job_environment" : {
                "type" : "porto",                                     # >= 19.2
            },
        }
    }
}

class TestSchedulerUserStatistics(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "slot_manager": {
                "job_environment" : {
                    "type" : "cgroups",
                    "supported_cgroups": [
                        "cpuacct",
                        "blkio",
                        "cpu"],
                },
            }
        }
    }

    @require_ytserver_root_privileges
    def test_job_statistics(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})
        op = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"mapper":{"custom_statistics_count_limit": 3}},
            command='cat; echo "{ cpu={ k1=4; k3=7 }}; {k2=-7};{k2=1};" >&5')

        statistics = get(op.get_path() + "/@progress/job_statistics")
        assert get_statistics(statistics, "custom.cpu.k1.$.completed.map.max") == 4
        assert get_statistics(statistics, "custom.k2.$.completed.map.count") == 2
        assert get_statistics(statistics, "custom.k2.$.completed.map.max") == 1

    @require_ytserver_root_privileges
    def test_tricky_names(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        # Keys with special symbols not allowed inside YPath are ok (they are represented as is).
        op = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"max_failed_job_count": 1},
            command='cat; echo "{\\"name/with/slashes\\"={\\"@table_index\\"=42}}">&5')
        statistics = get(op.get_path() + "/@progress/job_statistics")
        assert get_statistics(statistics, "custom.name/with/slashes.@table_index.$.completed.map.max") == 42

        # But the empty keys are not ok (as well as for any other map nodes).
        with pytest.raises(YtError):
            op = map(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={"max_failed_job_count": 1},
                command='cat; echo "{\\"\\"=42}">&5')

    @require_ytserver_root_privileges
    def test_name_is_too_long(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        long_name = 'a'*2048;

        with pytest.raises(YtError):
            op = map(
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={"max_failed_job_count": 1},
                command='cat; echo "{ ' + long_name + '=42};">&5')

    @require_ytserver_root_privileges
    def test_too_many_custom_statistics(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        custom_statistics_count_limit = 16
        write_line = ""
        for i in range(custom_statistics_count_limit + 1):
            write_line += 'echo "{ name' + str(i) + '=42};">&5;'

        with pytest.raises(YtError):
            map(in_="//tmp/t1",
                out="//tmp/t2",
                spec={"max_failed_job_count": 1, "mapper":{"custom_statistics_count_limit": custom_statistics_count_limit}},
                command="cat; " + write_line)

    @require_ytserver_root_privileges
    def test_multiple_job_statistics(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"} for i in range(2)])

        op = map(in_="//tmp/t1", out="//tmp/t2", command="cat", spec={"job_count": 2})
        statistics = get(op.get_path() + "/@progress/job_statistics")
        assert get_statistics(statistics, "user_job.cpu.user.$.completed.map.count") == 2

    @require_ytserver_root_privileges
    def test_job_statistics_progress(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"} for i in xrange(2)])

        op = map(
            dont_track=True,
            label="job_statistics_progress",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat > /dev/null ; BREAKPOINT ;"),
            spec={"max_failed_job_count": 1, "job_count": 2})

        jobs = wait_breakpoint()
        release_breakpoint(job_id=jobs[0])

        tries = 0

        counter_name = "user_job.cpu.user.$.completed.map.count"
        count = None

        while count is None and tries <= 100:
            statistics = get(op.get_path() + "/@progress")
            tries += 1
            try:
                count = get_statistics(statistics["job_statistics"], counter_name)
            except KeyError:
                time.sleep(0.1)

        assert count == 1

        release_breakpoint()
        op.track()

        statistics = get(op.get_path() + "/@progress")
        count = get_statistics(statistics["job_statistics"], counter_name)
        assert count == 2

@patch_porto_env_only(TestSchedulerUserStatistics)
class TestSchedulerUserStatisticsPorto(YTEnvSetup):
    DELTA_NODE_CONFIG = porto_delta_node_config
    USE_PORTO_FOR_SERVERS = True
