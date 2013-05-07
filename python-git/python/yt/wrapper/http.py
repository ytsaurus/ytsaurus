import config
import logger
from common import require
from errors import YtError, YtResponseError, YtTokenError, format_error
from format import JsonFormat
from version import VERSION

import yt.yson as yson

import requests

import os
import sys
import string
import uuid
import time
import httplib
import simplejson as json

# We cannot use requests.HTTPError in module namespace because of conflict with python3 http library
from requests import HTTPError, ConnectionError, Timeout
NETWORK_ERRORS = (HTTPError, ConnectionError, Timeout, httplib.IncompleteRead)

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
    elif type == "string":
        return response.text
    else:
        raise YtError("Incorrent response type: " + type)

class Response(object):
    def __init__(self, http_response):
        self.http_response = http_response
        if not str(http_response.status_code).startswith("2"):
            # 401 is case of incorrect token
            if http_response.status_code == 401:
                raise YtTokenError(
                    "Your authentication token was rejected by the server (X-YT-Request-ID: %s).\n"
                    "Please refer to http://proxy.yt.yandex.net/auth/ for obtaining a valid token or contact us at yt@yandex-team.ru." %
                    http_response.headers.get("X-YT-Request-ID", "absent"))
            self._error = format_error(http_response.json())
        elif int(http_response.headers.get("x-yt-response-code", 0)) != 0:
            self._error = format_error(json.loads(http_response.headers["x-yt-error"]))

    def error(self):
        return self._error

    def is_ok(self):
        return not hasattr(self, "_error")

    def is_json(self):
        return self.http_response.headers["content-type"] == "application/json"

    def is_yson(self):
        content_type = self.http_response.headers["content-type"]
        return isinstance(content_type, str) and content_type.startswith("application/x-yt-yson")

    def json(self):
        return self.http_response.json()

    def yson(self):
        return yson.loads(self.content())

    def content(self):
        return self.http_response.content

def get_token():
    token = config.TOKEN
    if token is None:
        token_path = os.path.join(os.path.expanduser("~"), ".yt/token")
        if os.path.isfile(token_path):
            token = open(token_path).read().strip()
    if token is not None:
        require(all(c in string.hexdigits for c in token),
                YtTokenError("You have an improper authentication token in ~/.yt_token.\n"
                             "Please refer to http://proxy.yt.yandex.net/auth/ for obtaining a valid token."))
    if not token:
        token = None
    return token

def get_hosts(proxy=None):
    if proxy is None:
        proxy = config.PROXY
    return requests.get("http://{0}/hosts".format(proxy)).json()

def get_host_for_heavy_operation():
    if config.USE_HOSTS:
        hosts = get_hosts()
        if hosts:
            return hosts[0]
    return config.PROXY

def make_request(command_name, params,
                 data=None, format=None, verbose=False, proxy=None,
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

    # Trying to set http retries in requests
    requests.adapters.DEFAULT_RETRIES = 10

    http_method = {
        "start_tx": "POST",
        "ping_tx": "POST",
        "commit_tx": "POST",
        "abort_tx": "POST",
        "create": "POST",
        "remove": "POST",
        "set": "PUT",
        "get": "GET",
        "list": "GET",
        "lock": "POST",
        "copy": "POST",
        "move": "POST",
        "link": "POST",
        "exists": "GET",
        "parse_ypath": "GET",
        "check_permission": "GET",
        "upload": "PUT",
        "download": "GET",
        "write": "PUT",
        "read": "GET",
        "merge": "POST",
        "erase": "POST",
        "map": "POST",
        "reduce": "POST",
        "map_reduce": "POST",
        "sort": "POST",
        "abort_op": "POST"
    }

    nonvolatile_commands = [
        "get",
        "list",
        "exists",
        "parse_ypath",
        "check_permission"
    ]

    heavy_commands = [
        "read",
        "write",
        "upload",
        "download"
    ]
    
    make_retry = command_name in nonvolatile_commands or \
            (config.RETRY_VOLATILE_COMMANDS and command_name not in heavy_commands)
    if make_retry and command_name not in nonvolatile_commands and "mutation_id" not in params:
        params["mutation_id"] = str(uuid.uuid4())

    # Prepare request url.
    if proxy is None:
        proxy = config.PROXY
    require(proxy, YtError("You should specify proxy"))

    # prepare url
    url = "http://{0}/api/{1}".format(proxy, command_name)
    print_info("Request url: %r", url)

    # prepare params, format and headers
    headers = {"User-Agent": "Python wrapper " + VERSION,
               "Accept-Encoding": config.ACCEPT_ENCODING}
    if http_method[command_name] == "POST":
        require(data is None and format is None,
                YtError("Format and data should not be specified in POST methods"))
        headers.update(JsonFormat().to_input_http_header())
        data = json.dumps(params)
        params = {}
    if params:
        headers.update({"X-YT-Parameters": json.dumps(params)})
    if format is not None:
        headers.update(format.to_input_http_header())
        headers.update(format.to_output_http_header())
    else:
        headers.update(JsonFormat().to_output_http_header())

    if config.USE_TOKEN:
        token = get_token()
        if token is None:
            # TODO(ignat): use YtError
            print >>sys.stderr, "Authentication token is missing. Please obtain it as soon as possible."
            print >>sys.stderr, "Refer to http://proxy.yt.yandex.net/auth/ for instructions."
            sys.exit(1)
        else:
            headers["Authorization"] = "OAuth " + token

    print_info("Headers: %r", headers)
    print_info("Params: %r", params)
    if http_method[command_name] != "PUT":
        print_info("Body: %r", data)

    stream = False
    if command_name in ["read", "download"]:
        stream = True

    
    for r in xrange(config.HTTP_RETRIES_COUNT):
        try:
            response = Response(
                requests.request(
                    url=url,
                    method=http_method[command_name],
                    headers=headers,
                    data=data,
                    files=files,
                    timeout=config.CONNECTION_TIMEOUT,
                    stream=stream))
            break
        except NETWORK_ERRORS:
            if make_retry:
                logger.warning("Retrying http request for command " + command_name)
                time.sleep(config.HTTP_RETRY_TIMEOUT)
            else:
                raise

    if config.USE_TOKEN and "Authorization" in headers:
        headers["Authorization"] = "x" * 32

    print_info("Response header %r", response.http_response.headers)
    if response.is_ok():
        if raw_response:
            return response.http_response
        elif response.is_json():
            return response.json()
        elif response.is_yson():
            return response.yson()
        else:
            return response.content
    else:
        message = "Response to request {0} with headers {1} contains error:\n{2}".\
                  format(url, headers, response.error())
        raise YtResponseError(message)

