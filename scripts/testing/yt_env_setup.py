import os

from yt_env import YTEnv, SANDBOX_ROOTDIR

class YTEnvSetup(YTEnv):

    @classmethod
    def setup_class(cls):
        print 'setting up', cls
        path_to_test = os.path.join(SANDBOX_ROOTDIR, cls.__name__)
        path_to_run = os.path.join(path_to_test, "run")
        cls.Env = cls()
        cls.Env.setUp(path_to_run)

    @classmethod
    def teardown_class(cls):
        print 'tearingdown', cls
        cls.Env.tearDown()
