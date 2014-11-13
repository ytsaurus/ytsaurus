from driver import make_request
from format import YsonFormat
from http import make_request_with_retries, get_proxy_url

from yt.common import update
from yt.yson import loads, YsonString

import copy

def parse_ypath(path, client=None):
    attributes = {}
    if isinstance(path, YsonString):
        attributes = copy.deepcopy(path.attributes)

    result = loads(make_request("parse_ypath", {"path": path, "output_format": YsonFormat().json()}, client=client))
    result.attributes = update(attributes, result.attributes)

    return result

def get_user_name(token, proxy=None, client=None):
    if not token.strip():
        return None

    proxy = get_proxy_url(proxy, client)
    response = make_request_with_retries(
        "post",
        "http://{0}/auth/login".format(proxy),
        data="token=" + token,
        response_should_be_json=True)
    login = response.json()["login"]
    if not login:
        return None
    return login

