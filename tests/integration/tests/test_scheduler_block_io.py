import pytest
import subprocess

from yt_env_setup import YTEnvSetup
from yt_commands import *


##################################################################

def can_perform_block_io_tests():
    try:
        subprocess.check_call(["ls", "-l", "/dev/sda"])
        return subprocess.check_output(["sudo", "-n", "-l", "dd"]).strip() == "/bin/dd"
    except AttributeError:
        # python 2.6 subprocess module does not have check_output function
        return False
    except subprocess.CalledProcessError:
        return False


block_io_mark = pytest.mark.skipif("not can_perform_block_io_tests()")


def get_statistics(statistics, complex_key):
    result = statistics
    for part in complex_key.split("."):
        if part:
            result = result[part]
    return result


##################################################################

class TestSchedulerBlockIO(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler" : {
            "user_job_blkio_weight" : 10
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent" : {
            'enable_cgroups' : 'true',
            "enable_iops_throttling" : 'true',
            "block_io_watchdog_period" : 8000,
            "supported_cgroups" :  [ "blkio" ]
        }
    }

    FAIL_IF_NOT_HIT_LIMIT="""
sleep 10

CURRENT_BLKIO_CGROUP=/sys/fs/cgroup/blkio`grep blkio /proc/self/cgroup | cut -d: -f 3`
echo Current blkio cgroup: $CURRENT_BLKIO_CGROUP >&2

echo "blkio.io_serviced content:" >&2
cat $CURRENT_BLKIO_CGROUP/blkio.io_serviced >&2
echo '===' >&2

echo "blkio.throttle.read_iops_device content:" >&2
CONTENT=`cat $CURRENT_BLKIO_CGROUP/blkio.throttle.read_iops_device`
echo $CONTENT >&2
echo $CONTENT | grep ' 3' 1>/dev/null
"""

    @block_io_mark
    def test_hitlimit(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"}])
        command="""cat
sudo -n dd if=/dev/sda of=/dev/null bs=16K count=100 iflag=direct 1>/dev/null
"""
        command += self.FAIL_IF_NOT_HIT_LIMIT
        map(in_="//tmp/t1", out="//tmp/t2", command=command, spec={"max_failed_job_count": 1})

    @block_io_mark
    def test_do_not_hitlimit(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"}])
        command="""
cat
sudo -n dd if=/dev/sda of=/dev/null bs=1600K count=1 iflag=direct 1>/dev/null
"""
        command += self.FAIL_IF_NOT_HIT_LIMIT

        with pytest.raises(YtError):
            map(in_="//tmp/t1",
                out="//tmp/t2",
                command=command,
                spec={"max_failed_job_count": 1})

    @block_io_mark
    def test_block_io_accounting(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"}])
        op = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat; sudo -n dd if=/dev/sda of=/dev/null bs=160K count=50 iflag=direct 1>/dev/null;")

        stats = get("//sys/operations/{0}/@progress/job_statistics".format(op.id))
        bytes_read = get_statistics(stats, "user_job.block_io.bytes_read.$.completed.map.sum")
        io_read = get_statistics(stats, "user_job.block_io.io_read.$.completed.map.sum")
        assert bytes_read >= 160*1024*50
        assert io_read >= 50
