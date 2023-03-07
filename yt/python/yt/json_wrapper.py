from yt.packages.six import iteritems

try:
    from simplejson import *
except ImportError:
    # This version of simplejson has no compliled speedup module.
    from yt.packages.simplejson import *

def loads_as_bytes(*args, **kwargs):
    def encode(value):
        if isinstance(value, dict):
            return dict([(encode(k), encode(v)) for k, v in iteritems(value)])
        elif isinstance(value, list):
            return [encode(item) for item in value]
        elif isinstance(value, unicode):
            return value.encode("utf-8")
        else:
            return value

    return encode(loads(*args, **kwargs))
