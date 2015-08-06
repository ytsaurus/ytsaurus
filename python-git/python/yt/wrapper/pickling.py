from yt.packages.importlib import import_module

import yt

frameworks = {
    "dill": "yt.packages.dill",
    "cloudpickle": "yt.packages.cloudpickle",
    "pickle": "cPickle",
}


def import_framework_module(framework):
    if framework not in frameworks:
        raise yt.YtError("Cannot find pickling framework {}. Available frameworks: {}.".format(
            framework, frameworks.keys()))
    return import_module(frameworks[framework])


class Pickler(object):
    def __init__(self, framework):
        framework_module = import_framework_module(framework)
        self.dump, self.dumps = framework_module.dump, framework_module.dumps


class Unpickler(object):
    def __init__(self, framework):
        framework_module = import_framework_module(framework)
        self.load, self.loads = framework_module.load, framework_module.loads
