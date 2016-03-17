from random import Random, _hexlify, _urandom, RECIP_BPF

class SystemRandom(Random):
    """ Alternative implementation of SystemRandom that supports pickle and deepcopy """

    def random(self):
        return (long(_hexlify(_urandom(7)), 16) >> 3) * RECIP_BPF

    def getrandbits(self, k):
        if k <= 0:
            raise ValueError('number of bits must be greater than zero')
        if k != int(k):
            raise TypeError('number of bits should be an integer')
        bytes = (k + 7) // 8
        x = long(_hexlify(_urandom(bytes)), 16)
        return x >> (bytes * 8 - k)

    def _stub(self, *args, **kwds):
        return None

    seed = jumpahead = getstate = setstate = _stub

