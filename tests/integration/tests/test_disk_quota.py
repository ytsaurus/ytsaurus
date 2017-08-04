import pytest

from yt_env_setup import (
    YTEnvSetup,
    SANDBOX_ROOTDIR,
    require_ytserver_root_privileges
)
from yt_commands import *

import os
import os.path
import uuid
import subprocess
import getpass
import sys

SCRIPT_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
PREPARE_SCRIPT = os.path.join(SCRIPT_DIR, "prepare_quota.sh")

def debug_log(message):
    print >>sys.stderr, "[DEBUG QUOTA LOG] {0}".format(message)

def check_fs(path):
    result = os.statvfs(path)
    debug_log("fs free blocks {0}, free nodes {1}, total blocks {2}".format(
        result.f_bfree,
        result.f_ffree,
        result.f_blocks))
    subprocess.check_call("ls -l {0} && df -h".format(path), shell=True)

class TestDiskQuota(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_MASTERS = 1
    VIRTUAL_FS_PATH = SCRIPT_DIR
    VIRTUAL_FS_FILE = "disk-quota"
    VIRTUAL_FS_TYPE = "ext4"
    BLOCKS = 20960 * 5

    @classmethod
    def _fs_path(cls):
        return os.path.join(
            cls.VIRTUAL_FS_PATH,
            "{0}.{1}".format(cls.VIRTUAL_FS_FILE, cls.VIRTUAL_FS_TYPE)
        )

    @classmethod
    def setup_class(cls):
        path_to_test = os.path.join(SANDBOX_ROOTDIR, cls.__name__)
        run_id = "run_" + uuid.uuid4().hex[:8]
        cls.path_to_run = os.path.join(path_to_test, run_id)
        fs_path = cls._fs_path()
        mount_path = cls.path_to_run
        os.makedirs(cls.path_to_run)
        username = getpass.getuser()
        debug_log("Ready to setup: path to run {0}, image path {1}, user {2}".format(
            cls.path_to_run,
            fs_path,
            username))
        try:
            subprocess.check_call(["sudo", PREPARE_SCRIPT, fs_path, cls.VIRTUAL_FS_TYPE,
                                   str(cls.BLOCKS), mount_path, username])
        except subprocess.CalledProcessError:
            cls.clear(True)
            raise
        debug_log("Finished quota setup, run default")
        check_fs(cls.path_to_run)
        super(TestDiskQuota, cls).setup_class(run_id=run_id)
        debug_log("Finished setup")
        check_fs(cls.path_to_run)

    @classmethod
    def clear(cls, ignore_errors):
        fs_path = cls._fs_path()
        debug_log("Clear")
        check_fs(cls.path_to_run)
        try:
            subprocess.check_call(["sudo", "repquota", "-vs", cls.path_to_run])
            subprocess.check_call(["sudo", "umount", cls.path_to_run])
            subprocess.check_call(["sudo", "rm", fs_path])
        except subprocess.CalledProcessError:
            debug_log("Fail on clear")
            if not ignore_errors:
                raise

    @classmethod
    def teardown_class(cls):
        debug_log("Teardown")
        cls.clear(False)
        debug_log("Default teardown")
        super(TestDiskQuota, cls).teardown_class()

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
            operation = map(
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
            operation = map(
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
