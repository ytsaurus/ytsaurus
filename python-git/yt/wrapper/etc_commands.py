from config import get_config
from driver import make_request
from format import YsonFormat

try:
    from yt_yson_bindings import parse_ypath as native_parse_ypath
except ImportError:
    native_parse_ypath = None

from yt.common import update
from yt.yson import loads, YsonString

import copy

def parse_ypath(path, client=None):
    attributes = {}
    if isinstance(path, YsonString):
        attributes = copy.deepcopy(path.attributes)

    if get_config(client)["enable_native_parse_ypath"] and native_parse_ypath is not None:
        result = native_parse_ypath(path)
    else:
        result = loads(make_request("parse_ypath", {"path": path, "output_format": YsonFormat().to_yson_type()}, client=client))

    result.attributes = update(attributes, result.attributes)

    return result


