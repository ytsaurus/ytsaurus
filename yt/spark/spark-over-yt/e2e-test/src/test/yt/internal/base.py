from yt_commands import (sync_create_cells, print_debug)
from yt_env_setup import YTEnvSetup

from spyt.testing.common.cluster import SpytCluster
import spyt.testing.common.cypress as cypress_helpers
from spyt.testing.common.helpers import get_python_path, get_java_home
import yt.wrapper

import os


class SpytInternalTestBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    NUM_DISCOVERY_SERVERS = 1
    USE_DYNAMIC_TABLES = True

    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True

    DELTA_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1,
            "default_file_replication_factor": 1,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_controller": {
                "resource_limits": {
                    "cpu": 2,
                    "memory": 4 * 2 ** 30,
                },
            },
        }
    }

    PYTHON_PATH = get_python_path()
    JAVA_HOME = get_java_home()
    YT = None

    @classmethod
    def get_proxy_address(cls):
        return "http://" + cls.Env.get_http_proxy_address()

    @staticmethod
    def _signal_instance(pid, signal_number):
        print_debug("Killing instance with with os.kill({}, {})".format(pid, signal_number))
        os.kill(pid, signal_number)

    @classmethod
    def _create_yt_client(cls):
        cls.YT = yt.wrapper.YtClient(proxy=cls.get_proxy_address())

    @classmethod
    def setup_class(cls, test_name=None, run_id=None):
        super().setup_class(test_name=test_name, run_id=run_id)

        if not cls.YT:
            cls._create_yt_client()
        cypress_helpers.quick_setup(cls.YT, cls.PYTHON_PATH, cls.JAVA_HOME)

    def setup_method(self, method):
        super().setup_method(method)
        sync_create_cells(1)

    def spyt_cluster(self):
        return SpytCluster(proxy=self.get_proxy_address(), java_home=self.JAVA_HOME)
