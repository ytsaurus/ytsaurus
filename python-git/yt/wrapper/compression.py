from yt.packages.six import binary_type

import sys

try:
    import zlib_fork_safe as zlib
except ImportError:
    import zlib

def create_zlib_generator(obj):
    zlib_obj = zlib.compressobj()
    if isinstance(obj, binary_type):
        yield zlib_obj.compress(obj)
    else:
        for chunk in ["".join(obj)]:
            c = zlib_obj.compress(chunk)
            if c:
                print >>sys.stderr, "YIELDING", len(c)
                yield c
                print >>sys.stderr, "AFTER YIELD"
    c = zlib_obj.flush()
    if c:
        print >>sys.stderr, "YIELDING", len(c)
        yield c
