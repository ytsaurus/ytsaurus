from yt_env_setup import (
    YTEnvSetup,
    SANDBOX_ROOTDIR,
)
from yt_commands import *

import os
import os.path
import uuid
import subprocess
import getpass
import errno
import pytest

SCRIPT_DIR = os.path.normpath(os.path.dirname(os.path.realpath(__file__)))
PREPARE_SCRIPT = os.path.join(SCRIPT_DIR, "prepare_quota.sh")

class QuotaMixin(YTEnvSetup):
    VIRTUAL_FS_FILE = "disk-quota"
    VIRTUAL_FS_TYPE = "ext4"
    VIRTUAL_FS_SIZE = 256 * 1024 * 1024
    VIRTUAL_FS_BLOCK_SIZE = 4096

    @classmethod
    def setup_class(cls):
        path_to_test = os.path.join(SANDBOX_ROOTDIR, cls.__name__)
        run_id = "run_" + uuid.uuid4().hex[:8]
        cls.path_to_run = os.path.join(path_to_test, run_id)
        cls.fs_path = os.path.join(SANDBOX_ROOTDIR, "{0}-{1}.{2}".format(cls.__name__, cls.VIRTUAL_FS_FILE, cls.VIRTUAL_FS_TYPE))

        # Node slots should be placed to new mounted filesystem.
        cls.mount_path = os.path.join(cls.path_to_run, "runtime_data", "node")

        try:
            os.makedirs(cls.mount_path)
        except OSError as err:
            if err.errno != errno.EEXIST:
                raise

        username = getpass.getuser()

        try:
            subprocess.check_call(["sudo", PREPARE_SCRIPT, cls.fs_path, cls.VIRTUAL_FS_TYPE,
                                   str(cls.VIRTUAL_FS_SIZE // cls.VIRTUAL_FS_BLOCK_SIZE),
                                   str(cls.VIRTUAL_FS_BLOCK_SIZE), cls.mount_path, username])
        except subprocess.CalledProcessError:
            cls.clear(True)
            raise

        super(QuotaMixin, cls).setup_class(run_id=run_id)

    @classmethod
    def clear(cls, ignore_errors):
        try:
            subprocess.check_call(["sudo", "umount", "-ld", cls.mount_path])
            subprocess.check_call(["sudo", "rm", "-f", cls.fs_path])
        except subprocess.CalledProcessError:
            if not ignore_errors:
                raise
