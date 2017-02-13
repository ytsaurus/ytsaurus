from .config import get_config
from .driver import make_request, make_formatted_request
from .format import YsonFormat

try:
    from yt_yson_bindings import parse_ypath as native_parse_ypath
except ImportError:
    native_parse_ypath = None

from yt.common import update
from yt.yson import loads, YsonString, YsonUnicode

import copy

def parse_ypath(path, client=None):
    attributes = {}
    if isinstance(path, (YsonString, YsonUnicode)):
        attributes = copy.deepcopy(path.attributes)

    if get_config(client)["enable_native_parse_ypath"] and native_parse_ypath is not None:
        result = native_parse_ypath(path)
    else:
        result = loads(make_request(
            "parse_ypath",
            {"path": path, "output_format": YsonFormat(require_yson_bindings=False).to_yson_type()},
            client=client,
            decode_content=False))

    result.attributes = update(attributes, result.attributes)

    return result

def execute_batch(requests, client=None):
    """Executes `requests` in parallel as one batch request."""
    return make_formatted_request("execute_batch", params={"requests": requests}, format=None, client=client)

def dump_job_context(job_id, path, client=None):
    """Dumps job input context to specified path."""
    return make_request("dump_job_context", {"job_id": job_id, "path": path}, client=client)
