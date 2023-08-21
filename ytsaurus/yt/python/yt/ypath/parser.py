from .rich import RichYPath

from yt.yson import YsonString, YsonUnicode, convert
from yt.common import update

try:
    from yt.packages.six import text_type, binary_type
except ImportError:
    from six import text_type, binary_type

from copy import deepcopy


def parse_ypath(path, client=None):
    path_attributes = {}

    base_type = None
    if isinstance(path, YsonString):
        base_type = binary_type
    if isinstance(path, YsonUnicode):
        base_type = text_type

    if base_type is not None:
        path_attributes = deepcopy(path.attributes)
        path = base_type(path)

    parser = RichYPath()
    path, attributes = parser.parse(path)
    return convert.to_yson_type(path, update(path_attributes, attributes))
