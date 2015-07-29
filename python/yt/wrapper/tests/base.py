from yt.environment import YTEnv

from yt.wrapper.default_config import get_default_config

from yt.wrapper.common import update
import yt.logger as logger
import yt.wrapper as yt

import os
import shutil
import logging

TEST_DIR = "//home/wrapper_tests"

class YtTestBase(object):
    @classmethod
    def setup_class(cls, config=None):
        if config is None:
            config = {}

        logging.basicConfig(level=logging.WARNING)
        logger.LOGGER.setLevel(logging.WARNING)

        dir = os.path.join(os.environ.get("TESTS_SANDBOX", "tests/sandbox"), cls.__name__)

        cls.env = YTEnv()
        cls.env.NUM_MASTERS = 1
        cls.env.NUM_NODES = 5
        cls.env.NUM_SCHEDULERS = 1
        cls.env.START_PROXY = True

        cls.env.DELTA_NODE_CONFIG = {
            "exec_agent" : {
                "enable_cgroups": "false"
            },
            "data_node": {
                "store_locations": [
                    {
                        "max_trash_ttl": 2000
                    }
                ]
            }
        }

        cls.env.set_environment(dir, os.path.join(dir, "pids.txt"), supress_yt_output=True)

        reload(yt)
        reload(yt.config)

        yt._cleanup_http_session()

        # For debug purpose
        #from yt.wrapper.client import Yt
        #yt.config.CLIENT = Yt("localhost:%d" % cls.env._ports["proxy"][0])
        
        cls.config = update(get_default_config(), config)
        cls.config["operation_tracker"]["poll_period"] = 100
        cls.config["proxy"]["url"] = "localhost:%d" % cls.env._ports["proxy"][0]
        cls.config["enable_token"] = False
        cls.config["clear_local_temp_files"] = True
        cls.config["pickling"]["module_filter"] = lambda module: hasattr(module, "__file__") and not "driver_lib" in module.__file__
        cls.config["driver_config"] = cls.env.configs["console_driver"][0]["driver"]
        cls.config["driver_config_path"] = cls.env.config_paths["console_driver"][0]
        update(yt.config.config, cls.config)

    @classmethod
    def teardown_class(cls):
        cls.env.clear_environment()
        for node_config in cls.env.configs["node"]:
            shutil.rmtree(node_config["data_node"]["store_locations"][0]["path"])
            shutil.rmtree(node_config["data_node"]["cache_locations"][0]["path"])

    def setup(self):
        os.environ["PATH"] = ".:" + os.environ["PATH"]
        yt.mkdir(TEST_DIR, recursive=True)

        yt.config.OPERATION_STATE_UPDATE_PERIOD = 0.2
        yt.config.DEFAULT_STRATEGY = yt.WaitStrategy(print_progress=False)

    def teardown(self):
        self.env.check_liveness()
        yt.remove(TEST_DIR, recursive=True, force=True)

    def get_environment(self):
        env = {"PYTHONPATH": os.environ["PYTHONPATH"],
              "YT_USE_TOKEN": "0",
              "YT_VERSION": yt.config.VERSION}
        if yt.config.http.PROXY is not None:
            env["YT_PROXY"] = yt.config.http.PROXY
        if yt.config.DRIVER_CONFIG_PATH is not None:
            env["YT_DRIVER_CONFIG_PATH"] = yt.config.DRIVER_CONFIG_PATH
        return env

