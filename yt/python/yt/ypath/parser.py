from .rich import RichYPath

from yt.yson import YsonString, YsonUnicode, convert
from yt.common import update

from copy import deepcopy

def parse_ypath(path, client=None):
    path_attributes = {}
    if isinstance(path, (YsonString, YsonUnicode)):
        path_attributes = deepcopy(path.attributes)

    parser = RichYPath()
    path, attributes = parser.parse(str(path))
    return convert.to_yson_type(path, update(path_attributes, attributes))
