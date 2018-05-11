from yp.local import YpInstance, ACTUAL_DB_VERSION
from yp.logger import logger

from yt.wrapper.common import generate_uuid

import pytest

import os
import sys
import logging

# TODO(ignat): avoid this hacks
try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

if yatest_common is not None:
    from yt.environment import arcadia_interop
else:
    arcadia_interop = None

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
    "node_segment"
]

NODE_CONFIG = {
    "tablet_node": {
        "resource_limits": {
            "tablet_static_memory": 100 * 1024 * 1024,
        }
    }
}

logger.setLevel(logging.DEBUG)

class YpTestEnvironment(object):
    def __init__(self, yp_master_config=None, enable_ssl=False, start=True, db_version=ACTUAL_DB_VERSION):
        if yatest_common is not None:
            destination = os.path.join(yatest_common.work_path(), "build")
            os.makedirs(destination)
            path, node_path = arcadia_interop.prepare_yt_environment(destination)
            os.environ["PATH"] = os.pathsep.join([path, os.environ.get("PATH", "")])
            os.environ["NODE_PATH"] = node_path

        self.test_sandbox_path = os.path.join(TESTS_SANDBOX, "yp_" + generate_uuid())
        self.yp_instance = YpInstance(self.test_sandbox_path,
                                      yp_master_config=yp_master_config,
                                      local_yt_options=dict(enable_debug_logging=True, node_config=NODE_CONFIG),
                                      enable_ssl=enable_ssl,
                                      db_version=db_version)
        if start:
            self.yp_instance.start()
            self.yp_client = self.yp_instance.create_client()
        else:
            self.yp_instance.prepare()
        self.yt_client = self.yp_instance.create_yt_client()

    def cleanup(self):
        self.yp_instance.stop()

def test_method_setup():
    print >>sys.stderr, "\n"

def test_method_teardown(yp_client):
    print >>sys.stderr, "\n"
    for object_type in OBJECT_TYPES:
        object_ids = yp_client.select_objects(object_type, selectors=["/meta/id"])
        for object_id in object_ids:
            yp_client.remove_object(object_type, object_id[0])


@pytest.fixture(scope="session")
def test_environment(request):
    environment = YpTestEnvironment()
    request.addfinalizer(lambda: environment.cleanup())
    return environment

@pytest.fixture(scope="function")
def yp_env(request, test_environment):
    test_method_setup()
    request.addfinalizer(lambda: test_method_teardown(test_environment.yp_client))
    return test_environment

@pytest.fixture(scope="class")
def test_environment_configurable(request):
    environment = YpTestEnvironment(
        yp_master_config=getattr(request.cls, "YP_MASTER_CONFIG"),
        enable_ssl=getattr(request.cls, "ENABLE_SSL", False))
    request.addfinalizer(lambda: environment.cleanup())
    return environment

@pytest.fixture(scope="function")
def yp_env_configurable(request, test_environment_configurable):
    test_method_setup()
    request.addfinalizer(lambda: test_method_teardown(test_environment_configurable.yp_client))
    return test_environment_configurable

@pytest.fixture(scope="function")
def yp_env_migration(request):
    environment = YpTestEnvironment(start=False, db_version=1)
    return environment
