from yt_env_setup import (
    require_ytserver_root_privileges
)
from yt_commands import *

from quota_mixin import QuotaMixin

class TestDiskQuota(QuotaMixin):
    NUM_SCHEDULERS = 1
    NUM_MASTERS = 1
    NUM_NODES = 3

    def _init_tables(self):
        tables = ["//tmp/t1", "//tmp/t2"]
        for table in tables:
            create("table", table)
        write_table(tables[0], [{"foo": "bar"} for _ in xrange(200)])
        return tables

    @require_ytserver_root_privileges
    def test_disk_usage(self):
        tables = self._init_tables()
        try:
            map(
                in_=tables[0],
                out=tables[1],
                command="/bin/bash -c 'dd if=/dev/zero of=zeros.txt count=20'",
                spec={"mapper": {"disk_space_limit": 2 * 1024}, "max_failed_job_count": 1}
            )
        except YtError as err:
            message = str(err)
            if not "quota exceeded" in message:
                raise
        else:
            assert False, "Operation expected to fail, but completed successfully"

    @require_ytserver_root_privileges
    def test_inodes_count(self):
        tables = self._init_tables()
        try:
            map(
                in_=tables[0],
                out=tables[1],
                command="/bin/bash -c 'touch {1..200}.txt'",
                spec={"mapper": {"inode_limit": 100}, "max_failed_job_count": 1}
            )
        except YtError as err:
            message = str(err)
            if not "quota exceeded" in message:
                raise
        else:
            assert False, "Operation expected to fail, but completed successfully"
