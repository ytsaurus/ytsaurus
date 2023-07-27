import gc
import os
import atexit

if 1:
    gc.set_threshold(0)
    atexit.register(os._exit, 0)

import sys
import simplejson
import time
import ujson
import json

import library.python.json as lpj

if len(sys.argv) == 2:
    with open(sys.argv[1], 'r') as f:
        d = f.read()

        for i in range(0, 10000):
            lpj.loads(d)
            # ujson.loads(d)

        sys.exit(0)


def timeit(n, f, number=100):
    t = time.time()

    for i in range(0, number):
        f()

    print n, 'elapsed', time.time() - t


def do1(data, n):
    prefix = 'len(data) = %s, times = %s, ' % (len(data), n)

    timeit(prefix + 'arcadia', lambda: lpj.loads(data), number=n)
    timeit(prefix + 'simplejson', lambda: simplejson.loads(data), number=n)
    timeit(prefix + 'ujson', lambda: ujson.loads(data), number=n)
    timeit(prefix + 'json', lambda: json.loads(data), number=n)


# do1('{}', 2000000)
# do1('{"1": 2, "3": "45", "4": [1, 2, 3, "qw"]}', 1000000)


with open(sys.argv[1], 'r') as f:
    do1(f.read(), int(sys.argv[2]))
