import zlib

import sys

def create_zlib_generator(stream):
    zlib_obj = zlib.compressobj()
    for chunk in stream:
        c = zlib_obj.compress(chunk)
        if c:
            yield c
    c = zlib_obj.flush()
    if c:
        yield c
