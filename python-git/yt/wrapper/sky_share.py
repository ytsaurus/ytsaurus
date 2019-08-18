from .common import YtError
from .config import get_config
from .http_helpers import make_request_with_retries, get_proxy_url
from .ypath import TablePath

import yt.yson as yson

import time

def sky_share(path, cluster=None, key_columns=[], client=None):
    """Shares table on cluster via skynet
    :param path: path to table
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param cluster: cluster name [by default it is derived from proxy url]
    :type path: str

    .. seealso:: `get on wiki <https://wiki.yandex-team.ru/yt/userdoc/blob_tables/#skynet>`_
    """
    if cluster is None:
        cluster = get_proxy_url(client=client).split(".")[0]

    params = {"path": TablePath(path).to_yson_string(), "cluster": cluster, "key_columns": key_columns}
    headers = {"X-YT-Parameters": yson.dumps(params, yson_format="text")}

    make_request = lambda: make_request_with_retries(
        method="POST",
        url=get_config(client)["skynet_manager_url"] + "/share",
        headers=headers,
        response_format=None,
        error_format="yson",
        client=client)

    while True:
        rsp = make_request()
        if rsp.status_code == 202:
            time.sleep(1.0)
        elif rsp.status_code == 200:
            return rsp.content
        else:
            if rsp.is_ok():
                raise YtError("Unknown status code from sky share " + str(rsp.status_code))
            raise rsp.error()

