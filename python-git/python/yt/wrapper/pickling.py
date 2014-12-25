import config

if config.PYTHON_USE_DILL:
    from yt.packages.dill import *
else:
    from cPickle import *
