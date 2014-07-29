import yt.logger as logger
import yt.wrapper as yt

import os
import logging

TEST_DIR = "//home/wrapper_tests"

class YtTestBase(object):
    @classmethod
    def _setup_class(cls, test_class):
        logging.basicConfig(level=logging.WARNING)
        logger.LOGGER.setLevel(logging.WARNING)

        test_class.NUM_MASTERS = 1
        test_class.NUM_NODES = 5
        test_class.NUM_SCHEDULERS = 1
        test_class.START_PROXY = True

        # (TODO): remake this strange stuff.
        cls.env = test_class()

        dir = os.environ.get("TESTS_SANDBOX", "tests/sandbox")
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
    def _teardown_class(cls):
        cls.env.clear_environment()

    def setup(self):
        os.environ["PATH"] = ".:" + os.environ["PATH"]
        yt.mkdir(TEST_DIR, recursive=True)

        yt.config.OPERATION_STATE_UPDATE_PERIOD = 0.2
        yt.config.DEFAULT_STRATEGY = yt.WaitStrategy(print_progress=False)

    def teardown(self):
        self.env.check_liveness()
        yt.remove(TEST_DIR, recursive=True, force=True)

