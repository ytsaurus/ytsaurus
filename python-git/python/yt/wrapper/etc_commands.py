from driver import make_request
from format import YsonFormat

from yt.common import update
from yt.yson import loads, YsonString

def parse_ypath(path):
    attributes = {}
    if isinstance(path, YsonString):
        attributes = path.attributes

    result = loads(make_request("parse_ypath", {"path": path, "output_format": YsonFormat().json()}))
    result.attributes = update(attributes, result.attributes)

    return result

