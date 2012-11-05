import collections
from itertools import chain, imap
import types
import os

class YtError(Exception):
    pass

def which(name, flags=os.X_OK):
    """
    Returns list of files in system paths with given name.
    """
    # TODO: check behavior when dealing with symlinks
    result = []
    for dir in os.environ.get("PATH", "").split(os.pathsep):
        path = os.path.join(dir, name)
        if os.access(path, flags):
            result.append(path)
    return result

def require(condition, exception):
    if not condition: raise exception

def update(d, u):
    for k, v in u.iteritems():
        if isinstance(v, collections.Mapping):
            r = update(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d

def flatten(obj, list_types=(list, tuple, set, types.GeneratorType)):
    """ Creates flat list from all elements.  """
    if isinstance(obj, list_types):
        return list(chain(*map(flatten, obj)))
    return [obj]



