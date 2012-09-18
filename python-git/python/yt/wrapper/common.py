from yt.common import require, flatten, update, which

EMPTY_GENERATOR = (i for i in [])

class YtError(Exception):
    pass

def compose(f, g):
    return lambda x: f(g(x))

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

