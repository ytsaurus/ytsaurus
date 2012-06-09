import os

from yt_env import YTEnv, SANDBOX_ROOTDIR
from functools import wraps

def _working_dir(test_name):
    path_to_test = os.path.join(SANDBOX_ROOTDIR, test_name)
    return os.path.join(path_to_test, "run")

class YTEnvSetup(YTEnv):

    @classmethod
    def setup_class(cls):
        #print 'setting up', cls
        path_to_run = _working_dir(test_name=cls.__name__)
        cls.Env = cls()
        cls.Env.setUp(path_to_run)
        #print '=' * 70

    @classmethod
    def teardown_class(cls):
        #print 'tearingdown', cls
        cls.Env.tearDown()

# decorator form
ATTRS = [
    'NUM_MASTERS',
    'NUM_HOLDERS',
    'NUM_SCHEDULERS',
    'DELTA_MASTER_CONFIG',
    'DELTA_HOLDER_CONFIG',
    'DELTA_SCHEDULER_CONFIG',
    ]
def ytenv(**attrs):
    def make_decorator(f):
        @wraps(f)
        def wrapped(*args, **kw):
            env = YTEnv()
            for i in ATTRS:
                if i in attrs:
                    setattr(env, i, attrs.get(i))
            working_dir = _working_dir(f.__name__)
            env.setUp(working_dir)
            f(*args, **kw)
            env.tearDown()
        return wrapped
    return make_decorator