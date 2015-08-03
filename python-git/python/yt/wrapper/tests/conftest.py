from yt.environment import YTEnv
from yt.wrapper.default_config import get_default_config
from yt.wrapper.common import update
import yt.logger as logger
import yt.wrapper as yt

from helpers import TEST_DIR

import os
from copy import deepcopy
import shutil
import logging
import pytest

def _pytest_finalize_func(environment, process_call_args):
    pytest.exit('Process run by command "{0}" is dead! Tests terminated.' \
                .format(" ".join(process_call_args)))
    environment.clear_environment(safe=False)

class YtTestEnvironment(object):
    def __init__(self, test_name, config=None):
        self.test_name = test_name

        if config is None:
            config = {}

        logging.basicConfig(level=logging.WARNING)
        logger.LOGGER.setLevel(logging.WARNING)

        dir = os.path.join(os.environ.get("TESTS_SANDBOX", "tests/sandbox"), self.test_name)

        self.env = YTEnv()
        self.env.NUM_MASTERS = 1
        self.env.NUM_NODES = 5
        self.env.NUM_SCHEDULERS = 1
        self.env.START_PROXY = True

        self.env.DELTA_NODE_CONFIG = {
            "exec_agent" : {
                "enable_cgroups" : "false"
            },
            "data_node": {
                "store_locations": [
                    {
                        "max_trash_ttl": 2000
                    }
                ]
            }
        }

        self.env.start(dir, os.path.join(dir, "pids.txt"), supress_yt_output=True)

        reload(yt)
        reload(yt.config)

        yt._cleanup_http_session()

        # For debug purpose
        #from yt.wrapper.client import Yt
        #yt.config.CLIENT = Yt("localhost:%d" % cls.env._ports["proxy"][0])

        self.config = update(get_default_config(), config)
        self.config["operation_tracker"]["poll_period"] = 100
        self.config.DEFAULT_STRATEGY = yt.WaitStrategy(print_progress=False)
        self.config["proxy"]["url"] = "localhost:%d" % self.env._ports["proxy"][0]
        self.config["enable_token"] = False
        self.config["clear_local_temp_files"] = True
        self.config["pickling"]["module_filter"] = lambda module: hasattr(module, "__file__") and not "driver_lib" in module.__file__
        self.config["driver_config"] = self.env.configs["console_driver"][0]["driver"]
        self.config["driver_config_path"] = self.env.config_paths["console_driver"][0]
        update(yt.config.config, self.config)

        os.environ["PATH"] = ".:" + os.environ["PATH"]

    def cleanup(self):
        self.env.clear_environment()
        for node_config in self.env.configs["node"]:
            shutil.rmtree(node_config["data_node"]["store_locations"][0]["path"])
            shutil.rmtree(node_config["data_node"]["cache_locations"][0]["path"])

    def check_liveness(self):
        self.env.check_liveness(callback_func=_pytest_finalize_func)

def init_environment_for_test_session(mode):
    if mode == "v2" or mode == "yamr":
        config = {"api_version": "v2"}
    elif mode == "v3":
        config = {"api_version": "v3", "proxy": {"header_format": "yson"}}
    else:
        config = {"backend": "native", "api_version": "v3"}

    environment = YtTestEnvironment("TestYtWrapper" + mode.capitalize(), config)

    if mode == "native":
        import yt_driver_bindings
        yt_driver_bindings.configure_logging(environment.env.driver_logging_config)
    else:
        yt.config.COMMANDS = None

    return environment

@pytest.fixture(scope="session", params=["v2", "v3", "native"])
def test_environment(request):
    environment = init_environment_for_test_session(request.param)
    request.addfinalizer(lambda: environment.cleanup())
    return environment

@pytest.fixture(scope="session")
def test_environment_for_yamr(request):
    environment = init_environment_for_test_session("yamr")
    request.addfinalizer(lambda: environment.cleanup())

    yt.set_yamr_mode()
    yt.config["yamr_mode"]["treat_unexisting_as_empty"] = False
    if not yt.exists("//sys/empty_yamr_table"):
        yt.create("table", "//sys/empty_yamr_table", recursive=True)
    if not yt.is_sorted("//sys/empty_yamr_table"):
        yt.run_sort("//sys/empty_yamr_table", "//sys/empty_yamr_table", sort_by=["key", "subkey"])
    yt.config["yamr_mode"]["treat_unexisting_as_empty"] = True

    return environment

@pytest.fixture(scope="function")
def yt_env(request, test_environment):
    """ YT cluster fixture.
        Uses test_environment fixture.
        Starts YT cluster once per session but checks its health before each test function.
    """
    test_environment.check_liveness()
    yt.mkdir(TEST_DIR, recursive=True)
    request.addfinalizer(lambda: yt.remove(TEST_DIR, recursive=True, force=True))
    return test_environment

@pytest.fixture(scope="function")
def config(yt_env):
    """ Test environment startup config fixture
        Used in tests to restore config after changes.
    """
    return deepcopy(yt_env.config)

@pytest.fixture(scope="function")
def yt_env_for_yamr(request, test_environment_for_yamr):
    """ YT cluster fixture for Yamr mode tests.
        Uses test_environment_for_yamr fixture.
        Starts YT cluster (with api_version v2) once per session but checks its health
        before each test function.
    """
    test_environment_for_yamr.check_liveness()
    yt.mkdir(TEST_DIR, recursive=True)
    request.addfinalizer(lambda: yt.remove(TEST_DIR, recursive=True, force=True))
    return test_environment_for_yamr

