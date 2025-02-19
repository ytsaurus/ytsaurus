from yt.common import update

from yt.test_helpers import get_tests_sandbox, prepare_yt_environment, get_build_root

from yt.environment import arcadia_interop

import yt.local as yt_local

import pytest

import os
import subprocess
import sys
import uuid


def run_clear_tmp(proxy_address, args=None):
    if args is None:
        args = []

    env = update(os.environ, {"YT_PROXY": proxy_address})
    binary = arcadia_interop.search_binary_path(
        "clear_tmp",
        binary_root=os.path.join(get_build_root(), "yt/cron/clear_tmp"))

    if arcadia_interop.yatest_common is not None:
        arcadia_interop.yatest_common.execute(
            [binary] + args,
            env=env,
            check_exit_code=True,
            stderr=sys.stderr)
    else:
        subprocess.check_call(
            [binary] + args,
            env=env,
            stderr=sys.stderr)


def generate_unique_id(prefix):
    return f"{prefix}_" + uuid.uuid4().hex[:8]


class YtTestEnvironment:
    def __init__(self):
        path = os.path.join(get_tests_sandbox(), "TestClearTmp")
        self.yt_instance_id = generate_unique_id("yt_env")
        self.yt_instance_path = os.path.join(path, self.yt_instance_id)

        self.yt_instance = yt_local.start(
            secondary_master_cell_count=1,
            path=path,
            id=self.yt_instance_id,
            fqdn="localhost",
            enable_debug_logging=True)
        self.yt_client = self.yt_instance.create_client()

    def cleanup(self):
        self.yt_instance.stop()


@pytest.fixture(scope="session", autouse=True)
def prepare_yt():
    prepare_yt_environment()


@pytest.fixture(scope="function")
def yt_env(request):
    environment = YtTestEnvironment()
    request.addfinalizer(lambda: environment.cleanup())
    return environment
