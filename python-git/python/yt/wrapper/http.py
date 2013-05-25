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
NETWORK_ERRORS = (HTTPError, ConnectionError, Timeout, httplib.IncompleteRead, YtResponseError)

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
        self._return_code_processed = False

    def error(self):
        self._process_return_code()
        return self._error

    def is_ok(self):
        self._process_return_code()
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

    def _process_return_code(self):
        if self._return_code_processed:
            return

        if not str(self.http_response.status_code).startswith("2"):
            # 401 is case of incorrect token
            if self.http_response.status_code == 401:
                raise YtTokenError(
                    "Your authentication token was rejected by the server (X-YT-Request-ID: %s).\n"
                    "Please refer to http://proxy.yt.yandex.net/auth/ for obtaining a valid token or contact us at yt@yandex-team.ru." %
                    self.http_response.headers.get("X-YT-Request-ID", "absent"))
            self._error = format_error(self.http_response.json())
        elif int(self.http_response.headers.get("x-yt-response-code", 0)) != 0:
            self._error = format_error(json.loads(self.http_response.headers["x-yt-error"]))
        self._return_code_processed = True


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

def make_request_with_retries(request, make_retries=False, url="", return_raw_response=False):
    for attempt in xrange(config.HTTP_RETRIES_COUNT):
        try:
            response = request()
            is_json = response.is_json() or not str(response.http_response.status_code).startswith("2")
            if not return_raw_response and is_json and not response.content():
                raise YtResponseError("Content is json but body is empty")
            return response
        except NETWORK_ERRORS as error:
            if make_retries:
                logger.warning("Http request (%s) has failed with error '%s'. Retrying...", url, str(error))
                time.sleep(config.HTTP_RETRY_TIMEOUT)
            else:
                raise

def get_hosts(proxy=None):
    if proxy is None:
        proxy = config.PROXY
    url = "http://{0}/hosts".format(proxy)
    return make_request_with_retries(
        lambda: Response(requests.get(url)),
        True,
        url).json()

def get_host_for_heavy_operation():
    if config.USE_HOSTS:
        hosts = get_hosts()
        if hosts:
            return hosts[0]
    return config.PROXY


class Command(object):
    def __init__(self, input_data_format, output_data_format, is_volatile, is_heavy):
        self.input_data_format = input_data_format
        self.output_data_format = output_data_format
        self.is_volatile = is_volatile
        self.is_heavy = is_heavy

    def http_method(self):
        if self.input_data_format is not None:
            return "PUT"
        elif self.is_volatile:
            return "POST"
        else:
            return "GET"

def make_request(command_name, params,
                 data=None, format=None, verbose=False, proxy=None,
                 return_raw_response=False, files=None):
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

    commands = {
        "start_tx":      Command(None,         "structured", True,  False),
        "ping_tx":       Command(None,         None,         True,  False),
        "commit_tx":     Command(None,         None,         True,  False),
        "abort_tx":      Command(None,         None,         True,  False),

        "create":        Command(None,         "structured", True,  False),
        "remove":        Command(None,         None,         True,  False),
        "set":           Command("structured", None,         True,  False),
        "get":           Command(None,         "structured", False, False),
        "list":          Command(None,         "structured", False, False),
        "lock":          Command(None,         None,         True,  False),
        "copy":          Command(None,         "structured", True,  False),
        "move":          Command(None,         None,         True,  False),
        "link":          Command(None,         "structured", True,  False),
        "exists":        Command(None,         "structured", False, False),

        "upload":        Command("binary",     None,         True,  True ),
        "download":      Command(None,         "binary",     False, True ),

        "write":         Command("tabular",    None,         True,  True ),
        "read":          Command(None,         "tabular",    False, True ),

        "merge":         Command(None,         "structured", True,  False),
        "erase":         Command(None,         "structured", True,  False),
        "map":           Command(None,         "structured", True,  False),
        "sort":          Command(None,         "structured", True,  False),
        "reduce":        Command(None,         "structured", True,  False),
        "map_reduce":    Command(None,         "structured", True,  False),
        "abort_op":      Command(None,         None,         True,  False),

        "parse_ypath":   Command(None,         "structured", False, False),

        "add_member":    Command(None,         None,         True,  False),
        "remove_member": Command(None,         None,         True,  False)
    }

    command = commands[command_name]

    if command.is_volatile and config.MUTATION_ID is not None:
        params["mutation_id"] = config.MUTATION_ID

    make_retries = not command.is_volatile or \
            (config.RETRY_VOLATILE_COMMANDS and not command.is_heavy)
    if make_retries and command.is_volatile and "mutation_id" not in params:
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
    # TODO(ignat) stop using http method for detection command properties
    if command.http_method() == "POST":
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
    if command.http_method() != "PUT":
        print_info("Body: %r", data)

    stream = False
    if command_name in ["read", "download"]:
        stream = True

    response = make_request_with_retries(
        lambda: Response(
            requests.request(
                url=url,
                method=command.http_method(),
                headers=headers,
                data=data,
                files=files,
                timeout=config.CONNECTION_TIMEOUT,
                stream=stream)),
        make_retries,
        url,
        return_raw_response)

    if config.USE_TOKEN and "Authorization" in headers:
        headers["Authorization"] = "x" * 32

    print_info("Response header %r", response.http_response.headers)
    if response.is_ok():
        if return_raw_response:
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

