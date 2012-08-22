import config
from common import YtError

import sys
import requests
import simplejson as json

def make_request(http_method, request_type, params,
                 data=None, format=None, verbose=False, proxy=None, check_errors=True,
                 raw_response=False, files=None):
    """ Makes request to yt proxy.
        http_method may be equal to GET, POST or PUT
        type may be equal to  get, read, write, create ...
        Returns response content, raw_response option force
        to return request.Response instance"""


    # Prepare request url.
    if proxy is None:
        proxy = config.PROXY
    url = "http://{0}/api/{1}".format(proxy, request_type)

    # Prepare headers
    if format is None:
        mime_type = "application/json"
    else:
        mime_type = format.to_mime_type()
    headers = {"User-Agent": "maps yt library",
               "Content-Type": mime_type,
               "Accept": mime_type}

    if verbose:
        print >>sys.stderr, "Request url:", url
        print >>sys.stderr, "Params:", params
        print >>sys.stderr, "Headers:", headers
        if http_method != "PUT" and data is not None:
            print >>sys.stderr, data

    # Some hack from @sandello
    #request_config = requests.defaults.defaults
    #if "Accept-Encoding" in request_config["base_headers"]:
    #    del request_config["base_headers"]["Accept-Encoding"]
    response = requests.request(
        url=url,
        method=http_method,
        headers=headers,
        #config=request_config,
        prefetch=False,
        params=params,
        data=data,
        files=files)

    if verbose:
        print >>sys.stderr, "Response header", response.headers

    if response.headers["content-type"] == "application/json":
        if verbose:
            print >>sys.stderr, "Response body", response.content
        if response.content:
            result = json.loads(response.content)
        else:
            result = None

        if check_errors and isinstance(result, dict) and "error" in result:
            raise YtError(
                "Response to request {0} with headers {1} contains error: {2}".
                format(url, headers, result["error"]))
    else:
        if raw_response:
            result = response
        else:
            result = response.content

    return result

