from yt.common import require, flatten, update, which, YtError, update_from_env
import yt.yson as yson

import os
import sys
import random
from functools import partial
from itertools import ifilter
import simplejson as json

EMPTY_GENERATOR = (i for i in [])

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
        raise YtError("Cannot parse boolean from %s" % word)

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
    return ifilter(None, iter).next()

def get_value(value, default):
    if value is None:
        return default
    else:
        return value

def dump_to_json(obj):
    return json.dumps(yson.yson_to_json(obj), indent=2)

def execute_handling_sigint(action, except_action):
    try:
        return action()
    except KeyboardInterrupt:
        while True:
            try:
                except_action()
            except KeyboardInterrupt:
                continue
            break
        raise
    except:
        raise

def chunk_iter(stream, chunk_size):
    while True:
        chunk = stream.read(chunk_size)
        if not chunk:
            break
        yield chunk

def chunk_iter_lines(lines, chunk_size):
    size = 0
    chunk = []
    for line in lines:
        size += len(line)
        chunk.append(line)
        if size >= chunk_size:
            yield chunk
            size = 0
            chunk = []
    yield chunk

def die(message=None, return_code=1):
    if message is not None:
        print >>sys.stderr, message
    sys.exit(return_code)

def generate_uuid():
    def get_int():
        return hex(random.randint(0, 2**32 - 1))[2:].rstrip("L")
    return "-".join([get_int() for _ in xrange(4)])
