import zlib

def create_zlib_generator(obj):
    zlib_obj = zlib.compressobj()
    if isinstance(obj, str):
        yield zlib_obj.compress(obj)
    else:
        for chunk in obj:
            c = zlib_obj.compress(chunk)
            if c:
                yield c
        c = zlib_obj.flush()
        if c:
            yield c
