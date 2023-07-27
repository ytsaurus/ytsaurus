import library.python.codecs as lpc

import pickle
import random
import sys


def gen_string(n):
    res = ''

    for i in range(0, n):
        if res and random.random() < 0.3:
            res += res[-1]
        else:
            res += chr(random.randint(0, 128))

    return res


strings = [gen_string(n) for n in (10, 100, 1000, 10000)]
res = []

for c in lpc.list_all_codecs():
    for s in strings:
        res.append((c, s, lpc.dumps(c, s)))

with open(sys.argv[1], 'w') as f:
    f.write(lpc.dumps('zstd08_10', pickle.dumps(res)))
