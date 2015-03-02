from yt.environment import YTEnv

import yt.logger as logger
import yt.wrapper as yt

import os
import logging

TEST_DIR = "//home/wrapper_tests"

class YtTestBase(object):
    @classmethod
    def setup_class(cls):
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
                "slot_manager" : {
                    "enable_cgroups" : "false"
                }
            }
        }

        cls.env.set_environment(dir, os.path.join(dir, "pids.txt"), supress_yt_output=True)

        yt.config.http.PROXY = None
        reload(yt.config)
        reload(yt)

        # For debug purpose
        #from yt.wrapper.client import Yt
        #yt.config.CLIENT = Yt("localhost:%d" % cls.env._ports["proxy"][0])
        yt.config.set_proxy("localhost:%d" % cls.env._ports["proxy"][0])
        yt.config.http.USE_TOKEN = False
        yt.config.http.RETRY_VOLATILE_COMMANDS = True
        yt.config.OPERATION_STATE_UPDATE_PERIOD = 100

    @classmethod
    def teardown_class(cls):
        cls.env.clear_environment()

    def setup(self):
        os.environ["PATH"] = ".:" + os.environ["PATH"]
        yt.mkdir(TEST_DIR, recursive=True)

        yt.config.OPERATION_STATE_UPDATE_PERIOD = 0.2
        yt.config.DEFAULT_STRATEGY = yt.WaitStrategy(print_progress=False)

    def teardown(self):
        self.env.check_liveness()
        yt.remove(TEST_DIR, recursive=True, force=True)

    def get_environment(self):
        return {"PYTHONPATH": os.environ["PYTHONPATH"],
                "YT_USE_TOKEN": "0",
                "YT_PROXY": yt.config.http.PROXY,
                "YT_VERSION": yt.config.VERSION}
