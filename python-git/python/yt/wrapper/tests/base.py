import yt.wrapper as yt
import yt.wrapper.config as config

import os
import logging

TEST_DIR = "//home/tests"

class YtTestBase(object):
    @classmethod
    def setUpClass(cls, test_class):
        logging.basicConfig(level=logging.WARNING)

        test_class.NUM_MASTERS = 1
        test_class.NUM_NODES = 5
        test_class.START_SCHEDULER = True
        test_class.START_PROXY = True

        ports = {
            "master": 18001,
            "node": 17001,
            "scheduler": 18101,
            "proxy": 18080,
            "proxy_log": 18081}
        # (TODO): remake this strange stuff.
        cls.env = test_class()
        cls.env.set_environment("tests/sandbox", "tests/sandbox/pids.txt", ports, supress_yt_output=True)

        config.PROXY = "localhost:%d" % ports["proxy"]

    @classmethod
    def tearDownClass(cls):
        cls.env.clear_environment()

    def setUp(self):
        os.environ["PATH"] = ".:" + os.environ["PATH"]
        yt.mkdir(TEST_DIR, recursive=True)

        config.WAIT_TIMEOUT = 0.2
        config.DEFAULT_STRATEGY = yt.WaitStrategy(print_progress=False)

    def tearDown(self):
        yt.remove(TEST_DIR, recursive=True)

