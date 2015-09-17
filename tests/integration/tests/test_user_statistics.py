import pytest
import os
import tempfile
import contextlib

from yt_env_setup import YTEnvSetup
from yt_commands import *


def get_statistics(statistics, complex_key):
    result = statistics
    for part in complex_key.split("."):
        if part:
            result = result[part]
    return result


@contextlib.contextmanager
def tempfolder(prefix):
    tmpdir = tempfile.mkdtemp(prefix=prefix)
    yield tmpdir
    try:
        os.unlink(tmpdir)
    except:
        pass


class TestUserStatistics(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "exec_agent" : {
            "enable_cgroups" : True,
            "supported_cgroups" : [ "cpuacct", "blkio", "memory" ]
        }
    }

    def test_job_statistics(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})
        op_id = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"mapper":{"custom_statistics_count_limit": 3}},
            command='cat; echo "{ cpu={ k1=4; k3=7 }}; {k2=-7};{k2=1};" >&5')

        statistics = get("//sys/operations/{0}/@progress/job_statistics".format(op_id))
        assert get_statistics(statistics, "custom.cpu.k1.$.completed.map.max") == 4
        assert get_statistics(statistics, "custom.k2.$.completed.map.count") == 2
        assert get_statistics(statistics, "custom.k2.$.completed.map.max") == 1

    def test_name_is_too_long(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        long_name = 'a'*2048;
        op_id = map(
            dont_track=True,
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"max_failed_job_count": 1},
            command='cat; echo "{ ' + long_name + '=42};">&5')

        with pytest.raises(YtError):
            track_op(op_id)

    def test_too_many_custom_statistics(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        custom_statistics_count_limit = 16
        write_line = ""
        for i in range(custom_statistics_count_limit + 1):
            write_line += 'echo "{ name' + str(i) + '=42};">&5;'

        op_id = map(
            dont_track=True,
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={"max_failed_job_count": 1, "mapper":{"custom_statistics_count_limit": custom_statistics_count_limit}},
            command="cat; " + write_line)

        with pytest.raises(YtError):
            track_op(op_id)

    def test_multiple_job_statistics(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"} for i in range(2)])

        op_id = map(in_="//tmp/t1", out="//tmp/t2", command="cat", spec={"job_count": 2})
        statistics = get("//sys/operations/{0}/@progress/job_statistics".format(op_id))
        assert get_statistics(statistics, "user_job.cpu.user.$.completed.map.count") == 2

    def test_job_statistics_progress(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"} for i in xrange(2)])

        with tempfolder("job_statistics_progress") as tmpdir:
            to_delete = []

            keeper_filename = os.path.join(tmpdir, "keep")
            with open(keeper_filename, "w") as f:
                f.close()
            to_delete.append(keeper_filename)

            command = '''cat > /dev/null;
                DIR={0}
                if [ "$YT_START_ROW_INDEX" = "0" ]; then
                  until rmdir $DIR 2>/dev/null; do sleep 1; done;
                fi
                exit 0;
                '''.format(tmpdir)

            try:
                op_id = map(dont_track=True, in_="//tmp/t1", out="//tmp/t2", command=command,
                            spec={"max_failed_job_count": 1, "job_count": 2})

                tries = 0
                statistics = {}

                counter_name = "user_job.cpu.user.$.completed.map.count"
                count = None

                while count is None and tries <= 10:
                    statistics = get("//sys/operations/{0}/@progress".format(op_id))
                    tries += 1
                    try:
                        count = get_statistics(statistics["job_statistics"], counter_name)
                    except KeyError:
                        time.sleep(1)

                assert count == 1

                os.unlink(keeper_filename)
                track_op(op_id)

                statistics = get("//sys/operations/{0}/@progress".format(op_id))
                count = get_statistics(statistics["job_statistics"], counter_name)
                assert count == 2
            finally:
                to_delete.reverse()
                for filename in to_delete:
                    try:
                        os.unlink(filename)
                    except:
                        pass
