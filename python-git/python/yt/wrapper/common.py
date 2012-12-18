from yt.common import require, flatten, update, which, YtError
import yt.yson as yson

from functools import partial
import simplejson as json


EMPTY_GENERATOR = (i for i in [])

class YtOperationFailedError(YtError):
    """
    Represents error that occurs when we synchronously wait operation that fails.
    """
    pass

class YtResponseError(YtError):
    """
    Represents error that occurs when we have error in http response.
    """
    pass

def compose(*args):
    def compose_two(f, g):
        return lambda x: f(g(x))
    return reduce(compose_two, args)

def unlist(l):
    return l[0] if len(l) == 1 else l

def parse_bool(word):
    word = word.lower()
    if word == "true":
        return True
    elif word == "false":
        return False
    else:
        raise YtError("Cannot parse word %s to boolean type" % word)

def bool_to_string(bool_value):
    if bool_value:
        return "true"
    else:
        return "false"

def is_prefix(list_a, list_b):
    if len(list_a) > len(list_b):
        return False
    for i in xrange(len(list_a)):
        if list_a[i] != list_b[i]:
            return False
    return True

def prefix(iterable, n):
    counter = 0
    for value in iterable:
        if counter == n:
            break
        counter += 1
        yield value

def dict_depth(obj):
    if not isinstance(obj, dict):
        return 0
    else:
        return 1 + max(map(dict_depth, obj.values()))

# Remove attributes from json response
def remove_attributes(tree):
    if isinstance(tree, dict):
        if "$attributes" in tree:
            return remove_attributes(tree["$value"])
        else:
            return dict([(k, remove_attributes(v)) for k, v in tree.iteritems()])
    elif isinstance(tree, list):
        return map(remove_attributes, tree)
    else:
        return tree

def first_not_none(iter):
    return filter(None, iter)[0]

def get_value(value, default):
    if value is None:
        return default
    else:
        return value

def dump_to_json(obj):
    return json.dumps(yson.simplify(obj), indent=2)
