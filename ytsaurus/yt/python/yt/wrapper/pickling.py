try:
    from importlib import import_module
except ImportError:
    from yt.packages.importlib import import_module

try:
    from yt.packages.six import PY3
except ImportError:
    from six import PY3

import yt

FRAMEWORKS = {
    "dill": ("yt.packages.dill",),
    "cloudpickle": ("yt.packages.cloudpickle", "cloudpickle"),
    "pickle": ("yt.packages.six.moves.cPickle", "six.moves.cPickle"),
}


def import_framework_module(framework):
    if framework not in FRAMEWORKS:
        raise yt.YtError("Cannot find pickling framework {0}. Available frameworks: {1}."
                         .format(framework, list(FRAMEWORKS)))
    result_module = None
    modules = FRAMEWORKS[framework]
    for module in modules:
        try:
            result_module = import_module(module)
        except ImportError:
            pass

    if framework == "dill" and PY3:
        # NB: python3.8 has changes DEFAULT_PROTOCTOL to 4.
        # We set protocol implicitly for client<->server compatibility.
        result_module.settings["protocol"] = 3
        result_module.settings["byref"] = True

    if result_module is None:
        raise RuntimeError("Failed to find module for framework '{}', tried modules {}".format(framework, modules))

    return result_module


class Pickler(object):
    def __init__(self, framework):
        self.framework_module = import_framework_module(framework)
        self.dump, self.dumps = self.framework_module.dump, self.framework_module.dumps

    def __getattr__(self, name):
        return getattr(self.framework_module, name)


class Unpickler(object):
    def __init__(self, framework):
        self.framework_module = import_framework_module(framework)
        self.load, self.loads = self.framework_module.load, self.framework_module.loads

    def __getattr__(self, name):
        return getattr(self.framework_module, name)
