from http import get_api_version

import yt.yson
from yt.yson import *
from yt.common import YtError

def _fix_boolean_as_string(kwargs):
    if "boolean_as_string" in kwargs:
        return

    try:
        version = get_api_version(client=kwargs.get("client", None))
        kwargs["boolean_as_string"] = (version == "v2")
    except YtError:
        pass

def load(*args, **kwargs):
    return yt.yson.load(*args, **kwargs)

def loads(*args, **kwargs):
    return yt.yson.loads(*args, **kwargs)

def dump(*args, **kwargs):
    _fix_boolean_as_string(kwargs)
    return yt.yson.dump(*args, **kwargs)

def dumps(*args, **kwargs):
    _fix_boolean_as_string(kwargs)
    return yt.yson.dumps(*args, **kwargs)
