import pytest
import os

from yt_env_setup import YTEnvSetup, require_ytserver_root_privileges
from yt_commands import *

##################################################################

def get_statistics(statistics, complex_key):
    result = statistics
    for part in complex_key.split("."):
        if part:
            result = result[part]
    return result

##################################################################

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
                        "memory",
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

        statistics = get("//sys/operations/{0}/@progress/job_statistics".format(op.id))
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
        statistics = get("//sys/operations/{0}/@progress/job_statistics".format(op.id))
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
        statistics = get("//sys/operations/{0}/@progress/job_statistics".format(op.id))
        assert get_statistics(statistics, "user_job.cpu.user.$.completed.map.count") == 2

    @require_ytserver_root_privileges
    def test_job_statistics_progress(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"} for i in xrange(2)])

        op = map(
            dont_track=True,
            waiting_jobs=True,
            label="job_statistics_progress",
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat > /dev/null",
            spec={"max_failed_job_count": 1, "job_count": 2})

        op.resume_job(op.jobs[0])

        tries = 0
        statistics = {}

        counter_name = "user_job.cpu.user.$.completed.map.count"
        count = None

        while count is None and tries <= 10:
            statistics = get("//sys/operations/{0}/@progress".format(op.id))
            tries += 1
            try:
                count = get_statistics(statistics["job_statistics"], counter_name)
            except KeyError:
                time.sleep(1)

        assert count == 1

        op.resume_jobs()
        op.track()

        statistics = get("//sys/operations/{0}/@progress".format(op.id))
        count = get_statistics(statistics["job_statistics"], counter_name)
        assert count == 2
