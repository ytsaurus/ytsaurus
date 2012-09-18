import config
from common import YtError
from format import RawFormat

import requests

import sys
import logger
import urllib

def make_request(http_method, request_type, params,
                 data=None, format=None, verbose=False, proxy=None, check_errors=True,
                 raw_response=False, files=None):
    """ Makes request to yt proxy.
        http_method may be equal to GET, POST or PUT
        type may be equal to  get, read, write, create ...
        Returns response content, raw_response option force
        to return request.Response instance"""

    def print_info(msg, *args, **kwargs):
        if verbose:
            print >>sys.stderr, msg % args % kwargs
        logger.debug(msg, *args, **kwargs)

    # Prepare request url.
    if proxy is None:
        proxy = config.PROXY
    url = "http://{0}/api/{1}".format(proxy, request_type)

    # Prepare headers
    if format is None:
        mime_type = "application/json"
    else:
        mime_type = format.to_mime_type()
        # In this case we cannot write arbitrary params to body,
        # so we should encode it into url. But standard urlencode
        # support only one level dict as params, therefore we use special 
        # recursive encoding method.
        url = "{0}?{1}".format(url, urlencode(params))
        params = {}

    if isinstance(format, RawFormat):
        input_format_key = "X-YT-Input-Format"
        output_format_key = "X-YT-Output-Format"
    else:
        input_format_key = "Content-Type"
        output_format_key = "Accept"


    headers = {"User-Agent": "Python wrapper",
               input_format_key: mime_type,
               output_format_key: mime_type,
               "Accept-Encoding": config.ACCEPT_ENCODING}


    print_info("Request url: %r", url)
    print_info("Params: %r", params)
    print_info("Headers: %r", headers)
    if http_method != "PUT" and data is not None:
        print_info(data)

    response = requests.request(
        url=url,
        method=http_method,
        headers=headers,
        prefetch=False,
        params=params,
        data=data,
        files=files)

    print_info("Response header %r", response.headers)
    if response.headers["content-type"] == "application/json":
        # In this case we load json and try to detect error response from server
        print_info("Response body %r", response.content)
        result = response.json if response.content else None

        # TODO(ignat): improve method to detect errors from server
        if check_errors and isinstance(result, dict) and "error" in result:
            message = "Response to request {0} with headers {1} contains error: {2}".\
                      format(url, headers, result["error"])
            if config.EXIT_WITHOUT_TRACEBACK:
                print >>sys.stderr, "Error:", message
                sys.exit(1)
            else:
                raise YtError(message)
    else:
        result = response if raw_response else response.content
    
    return result

def urlencode(params):
    """ Be careful, such urlencoding is not a part of W3C standard """
    urlencode.flat_params = {}
    def recursive_urlencode(params, prefix="", depth=0):
        for key, value in params.iteritems():
            if depth > 0:
                key = "[%s]" % key
            key = prefix + key
            if isinstance(value, dict):
                recursive_urlencode(value, key, depth + 1)
            else:
                urlencode.flat_params[key] = value
    recursive_urlencode(params)
    return urllib.urlencode(urlencode.flat_params)

