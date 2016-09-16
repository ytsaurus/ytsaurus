try:
    from importlib import import_module
except ImportError:
    from yt.packages.importlib import import_module

import yt

FRAMEWORKS = {
    "dill": "yt.packages.dill",
    "cloudpickle": "yt.packages.cloudpickle",
    "pickle": "yt.packages.six.moves.cPickle"
}

def import_framework_module(framework):
    if framework not in FRAMEWORKS:
        raise yt.YtError("Cannot find pickling framework {0}. Available frameworks: {1}."
                         .format(framework, list(FRAMEWORKS)))
    return import_module(FRAMEWORKS[framework])

class Pickler(object):
    def __init__(self, framework):
        framework_module = import_framework_module(framework)
        self.dump, self.dumps = framework_module.dump, framework_module.dumps

class Unpickler(object):
    def __init__(self, framework):
        framework_module = import_framework_module(framework)
        self.load, self.loads = framework_module.load, framework_module.loads
