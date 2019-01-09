from __future__ import print_function

from yp.common import YtResponseError, wait
from yp.local import YpInstance, ACTUAL_DB_VERSION
from yp.logger import logger

from yt.wrapper.common import generate_uuid
from yt.wrapper.retries import run_with_retries

from yt.common import update

import yt.subprocess_wrapper as subprocess

# TODO(ignat): avoid this hacks
try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

if yatest_common is not None:
    from yt.environment import arcadia_interop
else:
    arcadia_interop = None

import pytest

import copy
import logging
import os
import sys
import time
import shutil


if yatest_common is None:
    sys.path.insert(0, os.path.abspath('../../python'))
    pytest_plugins = "yt.test_runner.plugin"

TESTS_LOCATION = os.path.dirname(os.path.abspath(__file__))
TESTS_SANDBOX = os.environ.get("TESTS_SANDBOX", TESTS_LOCATION + ".sandbox")

OBJECT_TYPES = [
    "pod",
    "pod_set",
    "resource",
    "network_project",
    "node",
    "endpoint",
    "endpoint_set",
    "node_segment",
    "user",
    "group",
    "internet_address",
    "account",
    "replica_set",
]

ZERO_RESOURCE_REQUESTS = {
    "vcpu_guarantee": 0,
    "vcpu_limit": 0,
    "memory_guarantee": 0,
    "memory_limit": 0
}

logger.setLevel(logging.DEBUG)


class Cli(object):
    def __init__(self, directory_path, yamake_subdirectory_name, binary_name):
        if yatest_common is not None:
            binary_path = os.path.join("yp", directory_path, yamake_subdirectory_name, binary_name)
            self._cli_execute = [yatest_common.binary_path(binary_path)]
        else:
            binary_path = os.path.join(TESTS_LOCATION, "..", directory_path, binary_name)
            self._cli_execute = [
                os.environ.get("PYTHON_BINARY", sys.executable),
                os.path.normpath(binary_path),
            ]
        self._env = None

    def update_env(self, env):
        self._env = update(copy.deepcopy(os.environ), env)

    def _check_output(self, *args, **kwargs):
        return subprocess.check_output(*args, env=self._env, stderr=sys.stderr, **kwargs)

    def __call__(self, *args):
        return self._check_output(self._cli_execute + list(args)).strip()


class YpTestEnvironment(object):
    def __init__(self,
                 yp_master_config=None,
                 enable_ssl=False,
                 start=True,
                 db_version=ACTUAL_DB_VERSION,
                 local_yt_options=None):
        if yatest_common is not None:
            destination = os.path.join(yatest_common.work_path(), "yt_build_" + generate_uuid())
            os.makedirs(destination)
            path = arcadia_interop.prepare_yt_environment(destination)
            os.environ["PATH"] = os.pathsep.join([path, os.environ.get("PATH", "")])

            self.ram_drive_path = yatest_common.get_param("ram_drive_path")
            if self.ram_drive_path is None:
                self.test_sandbox_base_path = yatest_common.output_path()
            else:
                self.test_sandbox_base_path = self.ram_drive_path
            self.test_sandbox_path = os.path.join(self.test_sandbox_base_path, "yp_" + generate_uuid())
        else:
            self.test_sandbox_base_path = TESTS_SANDBOX
            self.test_sandbox_path = os.path.join(TESTS_SANDBOX, "yp_" + generate_uuid())

        self.yp_instance = YpInstance(self.test_sandbox_path,
                                      yp_master_config=yp_master_config,
                                      enable_ssl=enable_ssl,
                                      db_version=db_version,
                                      port_locks_path=os.path.join(self.test_sandbox_base_path, "ports"),
                                      local_yt_options=local_yt_options)
        if start:
            self._start()
        else:
            self.yp_instance.prepare()
        self.yt_client = self.yp_instance.create_yt_client()
        self.sync_access_control()

    def _start(self):
        try:
            self.yp_instance.start()
            self.yp_client = self.yp_instance.create_client()

            def touch_pod_set():
                try:
                    pod_set_id = self.yp_client.create_object("pod_set")
                    self.yp_client.remove_object("pod_set", pod_set_id)
                except YtResponseError:
                    return False
                return True

            wait(touch_pod_set)
        except:
            self._save_yatest_working_files()
            raise

    def sync_access_control(self):
        # TODO(babenko): improve
        time.sleep(1.0)

    def cleanup(self):
        try:
            self.yp_instance.stop()
            if yatest_common is not None:
                self._save_yatest_working_files()
        except:
            # Additional logging added due to https://github.com/pytest-dev/pytest/issues/2237
            logger.exception("YpTestEnvironment cleanup failed")
            raise

    def _save_yatest_working_files(self):
        if self.ram_drive_path is not None:
            shutil.copytree(
                self.test_sandbox_path,
                os.path.join(yatest_common.output_path(), os.path.basename(self.test_sandbox_path)))

def test_method_setup(yp_env):
    print("\n", file=sys.stderr)

def test_method_teardown(yp_env):
    def cleanup_objects(yp_client):
        for object_type in OBJECT_TYPES:
            if object_type == "schema":
                continue

            # Occasionally we may run into conflicts with the scheduler, see YP-284
            def do():
                object_ids = yp_client.select_objects(object_type, selectors=["/meta/id"])
                for object_id_list in object_ids:
                    object_id = object_id_list[0]
                    if object_type == "user" and object_id == "root":
                        continue
                    if object_type == "group" and object_id == "superusers":
                        continue
                    if object_type == "account" and object_id == "tmp":
                        continue
                    if object_type == "node_segment" and object_id == "default":
                        continue
                    yp_client.remove_object(object_type, object_id)
                yp_client.update_object("group", "superusers", set_updates=[
                    {"path": "/spec/members", "value": ["root"]}
                ])

            run_with_retries(do, exceptions=(YtResponseError,))

    print("\n", file=sys.stderr)
    try:
        cleanup_objects(yp_env.yp_client)
    except:
        # Additional logging added due to https://github.com/pytest-dev/pytest/issues/2237
        logger.exception("test_method_teardown failed")
        raise


@pytest.fixture(scope="session")
def test_environment(request):
    environment = YpTestEnvironment()
    request.addfinalizer(lambda: environment.cleanup())
    return environment

@pytest.fixture(scope="function")
def yp_env(request, test_environment):
    test_method_setup(test_environment)
    request.addfinalizer(lambda: test_method_teardown(test_environment))
    return test_environment

@pytest.fixture(scope="class")
def test_environment_configurable(request):
    environment = YpTestEnvironment(
        yp_master_config=getattr(request.cls, "YP_MASTER_CONFIG", None),
        enable_ssl=getattr(request.cls, "ENABLE_SSL", False),
        local_yt_options=getattr(request.cls, "LOCAL_YT_OPTIONS", None),
        start=getattr(request.cls, "START", True),
    )
    request.addfinalizer(lambda: environment.cleanup())
    return environment

@pytest.fixture(scope="function")
def yp_env_configurable(request, test_environment_configurable):
    test_method_setup(test_environment_configurable)
    request.addfinalizer(lambda: test_method_teardown(test_environment_configurable))
    return test_environment_configurable
