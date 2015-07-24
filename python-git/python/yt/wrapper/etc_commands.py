from driver import make_request
from format import YsonFormat
from http import make_request_with_retries, get_proxy_url, get_api_version

from yt.common import update
from yt.yson import loads, YsonString

import copy

def parse_ypath(path, client=None):
    attributes = {}
    if isinstance(path, YsonString):
        attributes = copy.deepcopy(path.attributes)

    result = loads(make_request("parse_ypath", {"path": path, "output_format": YsonFormat().to_yson_type()}, client=client))
    result.attributes = update(attributes, result.attributes)

    return result

def get_user_name(token=None, headers=None, client=None):
    version = get_api_version(client=client)
    proxy = get_proxy_url(None, client=client)

    if version == "v3":
        if headers is None:
            headers = {}
        if token is not None:
            headers["Authorization"] = "OAuth " + token.strip()
        data = None
        verb = "whoami"
    else:
        if not token:
            return None
        data = "token=" + token.strip()
        verb = "login"

    response = make_request_with_retries(
        "post",
        "http://{0}/auth/{1}".format(proxy, verb),
        headers=headers,
        data=data)
    login = response.json()["login"]
    if not login:
        return None
    return login

