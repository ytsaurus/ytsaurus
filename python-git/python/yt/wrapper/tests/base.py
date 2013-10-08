import yt.wrapper as yt

import os
import logging

TEST_DIR = "//home/wrapper_tests"

class YtTestBase(object):
    @classmethod
    def _setup_class(cls, test_class):
        logging.basicConfig(level=logging.WARNING)

        test_class.NUM_MASTERS = 1
        test_class.NUM_NODES = 5
        test_class.NUM_SCHEDULERS = 1
        test_class.START_PROXY = True

        ports = {
            "master": 18001,
            "node": 17101,
            "scheduler": 18101,
            "proxy": 18080}
        # (TODO): remake this strange stuff.
        cls.env = test_class()
        
        dir = os.environ.get("TESTS_SANDBOX", "tests/sandbox")
        cls.env.set_environment(dir, os.path.join(dir, "pids.txt"), ports, supress_yt_output=True)
        
        reload(yt)
        reload(yt.config)

        yt.config.set_proxy("localhost:%d" % ports["proxy"])
        yt.config.http.USE_TOKEN = False
        yt.config.http.RETRY_VOLATILE_COMMANDS = True

    @classmethod
    def _teardown_class(cls):
        cls.env.clear_environment()

    def setup(self):
        os.environ["PATH"] = ".:" + os.environ["PATH"]
        yt.mkdir(TEST_DIR, recursive=True)

        yt.config.WAIT_TIMEOUT = 0.2
        yt.config.DEFAULT_STRATEGY = yt.WaitStrategy(print_progress=False)

    def teardown(self):
        try:
            yt.remove(TEST_DIR, recursive=True, force=True)
        except:
            pass

