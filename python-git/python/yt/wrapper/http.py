import config
from common import YtError, dict_depth, require
from format import JsonFormat

import requests

import sys
import logger
import urllib
import simplejson as json

def iter_lines(response):
    """
    Iterates over the response data, one line at a time.  This avoids reading
    the content at once into memory for large responses. It is get from
    requests, but improved to ignore \r line breaks.
    """
    def add_eoln(str):
        return str + "\n"

    pending = None
    for chunk in response.iter_content(chunk_size=config.READ_BUFFER_SIZE):
        if pending is not None:
            chunk = pending + chunk
        lines = chunk.split('\n')
        pending = lines.pop()
        for line in lines:
            yield add_eoln(line)

    if pending is not None and pending:
        yield add_eoln(pending)

def read_content(response, type):
    if type == "iter_lines":
        return iter_lines(response)
    elif type == "iter_content":
        return response.iter_content(chunk_size=config.HTTP_CHUNK_SIZE)
    else:
        raise YtError("Incorrent response type: " + type)


def make_request(command_name, params,
                 data=None, format=None, verbose=False, proxy=None, check_errors=True,
                 raw_response=False, files=None):
    """ Makes request to yt proxy.
        http_method may be equal to GET, POST or PUT,
        command_name is type of driver command, it may be equal
        to get, read, write, create ...
        Returns response content, raw_response option force
        to return request.Response instance"""
    def print_info(msg, *args, **kwargs):
        # Verbose option is used for debugging because it is more
        # selective than logging
        if verbose:
            # We don't use kwargs because python doesn't support such kind of formatting
            print >>sys.stderr, msg % args
        logger.debug(msg, *args, **kwargs)

    # Prepare request url.
    if proxy is None:
        proxy = config.PROXY

    # prepare commands description for given proxy
    if not hasattr(make_request, "proxy_api"):
        make_request.proxy_api = {}
    if proxy not in make_request.proxy_api:
        api = requests.get(
            "http://{}/api".format(proxy),
            headers= {
                "User-Agent": "Python wrapper",
                "Accept": "application/json"
            }
        ).json
        make_request.proxy_api[proxy] = dict(map(lambda d: (d["name"], d), api))

    # calculate http_method
    is_volatile = make_request.proxy_api[proxy][command_name]["is_volatile"]
    #is_heavy = make_request.proxy_api[proxy][command_name]["is_heavy"]
    require(data is None or is_volatile,
            YtError("Not volatile request should not have body"))
    if not is_volatile:
        http_method = "GET"
    elif data is None:
        http_method = "POST"
    else:
        http_method = "PUT"


    # prepare url
    url = "http://{0}/api/{1}".format(proxy, command_name)
    print_info("Request url: %r", url)

    # prepare params, format and headers
    print_info("Params: %r", params)
    headers = {"User-Agent": "Python wrapper",
               "Accept-Encoding": config.ACCEPT_ENCODING}
    if data is None and is_volatile:
        require(format is None,
                YtError("Format has no meaning if data is not specified and command is volatile"))
        headers.update(JsonFormat().to_input_http_header())
        data = json.dumps(params)
        params = {}
    elif dict_depth(params) > 1:
        # In this case we need encode params to the url.
        # But standard urlencode support only one level dict as params,
        # therefore we use special recursive encoding method.
        url = "{0}?{1}".format(url, urlencode(params))
        params = {}
    if format is not None:
        headers.update(format.to_input_http_header())
        headers.update(format.to_output_http_header())
    else:
        headers.update(JsonFormat().to_output_http_header())
    print_info("Headers: %r", headers)
    print_info("Params: %r", params)
    print_info("Body: %r", data)

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

