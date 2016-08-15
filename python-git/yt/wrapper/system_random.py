from random import Random, RECIP_BPF
from binascii import hexlify
from os import urandom

class SystemRandom(Random):
    """ Alternative implementation of SystemRandom that supports pickle and deepcopy """

    def random(self):
        return (long(hexlify(urandom(7)), 16) >> 3) * RECIP_BPF

    def getrandbits(self, k):
        if k <= 0:
            raise ValueError('number of bits must be greater than zero')
        if k != int(k):
            raise TypeError('number of bits should be an integer')
        bytes = (k + 7) // 8
        x = long(hexlify(urandom(bytes)), 16)
        return x >> (bytes * 8 - k)

    def _stub(self, *args, **kwds):
        return None

    seed = jumpahead = getstate = setstate = _stub

