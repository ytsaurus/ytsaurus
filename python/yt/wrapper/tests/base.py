import yt.wrapper as yt
import yt.wrapper.config as config

import os
import logging

TEST_DIR = "//home/wrapper_tests"

class YtTestBase(object):
    @classmethod
    def _setup_class(cls, test_class):
        reload(yt)
        reload(config)

        logging.basicConfig(level=logging.WARNING)

        test_class.NUM_MASTERS = 1
        test_class.NUM_NODES = 5
        test_class.START_SCHEDULER = True
        test_class.START_PROXY = True

        ports = {
            "master": 18001,
            "node": 17101,
            "scheduler": 18101,
            "proxy": 18080,
            "proxy_log": 18081}
        # (TODO): remake this strange stuff.
        cls.env = test_class()
        dir = os.environ.get("TESTS_SANDBOX", "tests/sandbox")
        cls.env.set_environment(dir, os.path.join(dir, "pids.txt"), ports, supress_yt_output=True)

        config.PROXY = "localhost:%d" % ports["proxy"]
        config.USE_TOKEN = False
        config.RETRY_VOLATILE_COMMANDS = True

    @classmethod
    def _teardown_class(cls):
        cls.env.clear_environment()

    def setup(self):
        os.environ["PATH"] = ".:" + os.environ["PATH"]
        yt.mkdir(TEST_DIR, recursive=True)

        config.WAIT_TIMEOUT = 0.2
        config.DEFAULT_STRATEGY = yt.WaitStrategy(print_progress=False)

    def teardown(self):
        try:
            yt.remove(TEST_DIR, recursive=True, force=True)
        except:
            pass

