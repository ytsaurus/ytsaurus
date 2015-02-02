import config

if config.PYTHON_USE_DILL:
    assert not config.PYTHON_USE_CLOUDPICKLE, \
        "PYTHON_USE_DILL and PYTHON_USE_CLOUDPICKLE cannot be set simultaneously"
    from yt.packages.dill import *
elif config.PYTHON_USE_CLOUDPICKLE:
    from yt.packages.cloudpickle import *
else:
    from cPickle import *
