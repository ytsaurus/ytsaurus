from driver import make_request
from format import YsonFormat

from yt.yson import loads

def parse_ypath(path):
    return loads(make_request("parse_ypath", {"path": path, "output_format": YsonFormat().json()}))
    
