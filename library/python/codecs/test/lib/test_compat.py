import library.python.codecs as lpc
import library.python.resource as lpr

import pickle
import functools


data = lpr.find(b'/data')
data = lpc.loads('zstd08_10', data)


try:
    data = pickle.loads(data, encoding='bytes')
except TypeError:
    data = pickle.loads(data)


def do_run(codec, orig, compr):
    assert lpc.loads(codec, compr) == orig


for codec, orig, compr in data:
    ucodec = codec.decode('utf-8')

    globals()['test_' + ucodec.replace('-', '_') + '_' + str(len(orig))] = functools.partial(do_run, ucodec, orig, compr)
