"""Some common useful misc"""

from yt.common import require, flatten, update, which, YtError, update_from_env, unlist, get_value, filter_dict, date_string_to_timestamp
import yt.yson as yson

import os
import sys
import inspect
import random
import time
import functools
from datetime import datetime
from itertools import ifilter, chain

EMPTY_GENERATOR = (i for i in [])

MB = 1024 * 1024

def compose(*args):
    def compose_two(f, g):
        return lambda x: f(g(x))
    return reduce(compose_two, args)

def parse_bool(word):
    """convert 'true' and 'false' and something like this to Python bool

    Raise `YtError` if input word is incorrect."""

    # Compatibility with api/v3
    if word is False or word is True or isinstance(word, yson.YsonBoolean):
        return word

    word = word.lower()
    if word == "true":
        return True
    elif word == "false":
        return False
    else:
        raise YtError("Cannot parse boolean from %s" % word)

def bool_to_string(bool_value):
    """convert Python bool value to 'true' or 'false' string

    Raise `YtError` if value is incorrect.
    """
    if bool_value in ["false", "true"]:
        return bool_value
    if bool_value not in [False, True]:
        raise YtError("Incorrect bool value '{0}'".format(bool_value))
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

def first_not_none(iter):
    return ifilter(None, iter).next()

def merge_dicts(*dicts):
    return dict(chain(*[d.iteritems() for d in dicts]))

def chunk_iter_blobs(lines, chunk_size):
    """ Unite lines into large chunks """
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

def chunk_iter_stream(stream, chunk_size):
    while True:
        chunk = stream.read(chunk_size)
        if not chunk:
            break
        yield chunk

def chunk_iter_string(string, chunk_size):
    index = 0
    while True:
        lower_index = index * chunk_size
        upper_index = min((index + 1) * chunk_size, len(string))
        if lower_index >= len(string):
            return
        yield string[lower_index:upper_index]
        index += 1

def total_seconds(td):
    return float(td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6) / 10 ** 6

def get_backoff(timeout, start_time):
    return max(0.0, (timeout / 1000.0) - total_seconds(datetime.now() - start_time))

def generate_uuid(generator=None):
    if generator is None:
        generator = random
    def get_int():
        return hex(generator.randint(0, 2**32 - 1))[2:].rstrip("L")
    return "-".join([get_int() for _ in xrange(4)])

def get_version():
    """ Returns python wrapper version """
    try:
        from version import VERSION
        return VERSION
    except:
        return "unknown"

def get_python_version():
    return sys.version_info[:3]

def run_with_retries(action, retry_count=6, backoff=20.0, exceptions=(YtError,), except_action=None,
                     backoff_action=None):
    start_time = datetime.now()
    for iter in xrange(retry_count):
        try:
            return action()
        except exceptions as err:
            if iter + 1 == retry_count:
                raise

            if except_action:
                if len(inspect.getargspec(except_action).args) == 0:
                    except_action()
                else:
                    except_action(err)

            sleep_backoff = max(0.0, backoff * iter - total_seconds(datetime.now() - start_time))

            if backoff_action:
                backoff_action(err, iter, sleep_backoff)

            time.sleep(sleep_backoff)

def is_inside_job():
    """Returns True if the code is currently being run in the context of a YT job."""
    return bool(int(os.environ.get("YT_WRAPPER_IS_INSIDE_JOB", "0")))

def forbidden_inside_job(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        flag = os.environ.get("YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB", "0")
        if is_inside_job() and not bool(int(flag)):
            raise YtError('HTTP requests to YT are forbidden inside jobs by default. '
                          'Did you forget to surround code that runs YT operations with '
                          'if __name__ == "__main__"? If not and you need to make requests '
                          'you can override this behaviour by turning on '
                          '"allow_http_requests_to_yt_from_job" option in config.')
        return func(*args, **kwargs)
    return wrapper
