import collections
import copy
import types

class YtError(Exception):
    pass

def partial(func, *args, **keywords):
    def newfunc(*fargs, **fkeywords):
        newkeywords = keywords.copy()
        newkeywords.update(fkeywords)
        return func(*(args + fargs), **newkeywords)
    newfunc.func = func
    newfunc.args = args
    newfunc.keywords = keywords
    return newfunc

def flatten(obj, list_types=(list, tuple, set, types.GeneratorType)):
    """
    Creates flat list from all elements.
    """
    if isinstance(obj, list_types):
        res = []
        for x in obj:
            res += flatten(x)
        return res
    else:
        return [obj]

def update(d, u):
    for k, v in u.iteritems():
        if isinstance(v, collections.Mapping):
            r = update(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d

def compose(f, g):
    return lambda x: f(g(x))

def require(condition, exception):
    if not condition: raise exception

def unlist(l):
    return l[0] if len(l) == 1 else l

def add_quotes(name):
    return '"%s"' % name

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
