"""YT requests misc"""
import config
import yt.logger as logger
from compression_wrapper import create_zlib_generator
from common import require, generate_uuid, bool_to_string, get_value
from errors import YtError, YtResponseError
from version import VERSION
from http import make_get_request_with_retries, make_request_with_retries, get_token, get_api, get_proxy_url
from command import parse_commands

from yt.yson.convert import json_to_yson

import yt.packages.requests as requests

import sys
import simplejson as json

def escape_utf8(obj):
    def escape_symbol(sym):
        if ord(sym) < 128:
            return sym
        else:
            return chr(ord('\xC0') | (ord(sym) >> 6)) + chr(ord('\x80') | (ord(sym) & ~ord('\xC0')))
    def escape_str(str):
        return "".join(map(escape_symbol, str))

    if isinstance(obj, unicode):
        obj = escape_str(str(bytearray(obj, 'utf-8')))
    elif isinstance(obj, str):
        obj = escape_str(obj)
    elif isinstance(obj, list):
        obj = map(escape_utf8, obj)
    elif isinstance(obj, dict):
        obj = dict((escape_str(k), escape_utf8(v)) for k, v in obj.iteritems())
    return obj

class ResponseStream(object):
    """Iterator over response"""
    def __init__(self, response, iter_type):
        self.response = response.raw_response
        self.iter_type = iter_type
        self._buffer = ""
        self._pos = 0
        self._iter_content = self.response.iter_content(config.READ_BUFFER_SIZE)

    def read(self, length=None):
        if length is None:
            length = 2 ** 32

        result = []
        while length > len(self._buffer) - self._pos:
            right = min(len(self._buffer), self._pos + length)
            result.append(self._buffer[self._pos:right])
            length -= right - self._pos
            self._pos = right
            if length == 0 or not self._fetch():
                break
        return "".join(result)

    def readline(self):
        result = []
        while True:
            index = self._buffer.find("\n", self._pos)
            if index != -1:
                result.append(self._buffer[self._pos: index + 1])
                self._pos = index + 1
                break

            result.append(self._buffer[self._pos:])
            if not self._fetch():
                break
        return "".join(result)

    def _fetch(self):
        try:
            self._buffer = self._iter_content.next()
            self._pos = 0
            if not self._buffer:
                return False
            return True
        except StopIteration:
            return False

    def __iter__(self):
        return self

    def next(self):
        if self.iter_type == "iter_lines":
            line = self.readline()
            if not line:
                raise StopIteration()
            return line
        elif self.iter_type == "iter_content":
            if self._pos == len(self._buffer) and not self._fetch():
                raise StopIteration()
            result = self._buffer[self._pos:]
            self._pos = len(self._buffer)
            return result
        else:
            raise YtError("Incorrect iter type: " + str(self.iter_type))


def read_content(response, raw, format, response_type):
    if raw:
        if response_type == "string":
            return response.raw_response.text
        elif response_type == "raw":
            return response.raw_response
        else:
            return ResponseStream(response, response_type)
    else:
        return format.load_rows(ResponseStream(response, response_type))


def get_hosts(client=None):
    proxy = get_proxy_url(None, client)
    return make_get_request_with_retries("http://{0}/{1}".format(proxy, config.HOSTS))

def get_host_for_heavy_operation(client=None):
    client = get_value(client, config.CLIENT)
    if config.USE_HOSTS:
        hosts = get_hosts(client=client)
        if hosts:
            return hosts[0]
    if client is not None:
        return client.proxy
    else:
        return config.http.PROXY


def make_request(command_name, params,
                 data=None, proxy=None,
                 return_content=True, verbose=False,
                 retry_unavailable_proxy=True, client=None):
    """
    Makes request to yt proxy. Command name is the name of command in YT API.
    """
    def print_info(msg, *args, **kwargs):
        # Verbose option is used for debugging because it is more
        # selective than logging
        if verbose:
            # We don't use kwargs because python doesn't support such kind of formatting
            print >>sys.stderr, msg % args
        logger.debug(msg, *args, **kwargs)

    # Prepare request url.
    proxy = get_proxy_url(proxy, client)

    if client is None:
        client_provider = config
    else:
        client_provider = client

    if not hasattr(client_provider, "COMMANDS"):
        require("v2" in get_api(proxy), "Old versions of API are not supported")
        client_provider.COMMANDS = parse_commands(get_api(proxy, version="v2"))
        client_provider.API_PATH = "api/v2"
    commands = client_provider.COMMANDS
    api_path = client_provider.API_PATH

    # Get command description
    command = commands[command_name]

    # Determine make retries or not and set mutation if needed
    allow_retries = \
            not command.is_volatile or \
            (config.http.RETRY_VOLATILE_COMMANDS and not command.is_heavy)
    if command.is_volatile and allow_retries:
        if config.MUTATION_ID is not None:
            params["mutation_id"] = config.MUTATION_ID
        else:
            params["mutation_id"] = generate_uuid()

    if config.TRACE is not None and config.TRACE:
        params["trace"] = bool_to_string(config.TRACE)

    # prepare url
    url = "http://{0}/{1}/{2}".format(proxy, api_path, command_name)
    print_info("Request url: %r", url)

    # prepare params, format and headers
    headers = {"User-Agent": "Python wrapper " + VERSION,
               "Accept-Encoding": config.http.ACCEPT_ENCODING,
               "X-YT-Correlation-Id": generate_uuid()}

    if command.input_type is None:
        # Should we also check that command is volatile?
        require(data is None, YtError("Body should be empty in commands without input type"))
        if command.is_volatile:
            headers["Content-Type"] = "application/json"
            data = json.dumps(escape_utf8(params))
            params = {}

    if params:
        headers.update({"X-YT-Parameters": json.dumps(escape_utf8(params))})

    token = get_token(client=client)
    if token is not None:
        headers["Authorization"] = "OAuth " + token

    if command.input_type in ["binary", "tabular"]:
        content_encoding = config.http.CONTENT_ENCODING
        headers["Content-Encoding"] = content_encoding
        if content_encoding == "identity":
            pass
        elif content_encoding == "gzip":
            data = create_zlib_generator(data)
        else:
            raise YtError("Content encoding '%s' is not supported" % config.http.CONTENT_ENCODING)

    # Debug information
    print_info("Headers: %r", headers)
    if command.input_type is None:
        print_info("Body: %r", data)

    stream = (command.output_type in ["binary", "tabular"])

    try:
        response = make_request_with_retries(
            command.http_method(),
            url,
            make_retries=allow_retries,
            retry_unavailable_proxy=retry_unavailable_proxy,
            headers=headers,
            data=data,
            stream=stream)
    except requests.ConnectionError as error:
        if hasattr(error, "response"):
            # Reponse has trailing, it is ususally error response from yt in case of heavy request.
            response = error.response
        else:
            raise

    # Hide token for security reasons
    if "Authorization" in headers:
        headers["Authorization"] = "x" * 32
    print_info("Response header %r", response.headers())

    # Determine type of response data and return it
    if response.is_ok():
        if return_content:
            return response.content()
        else:
            return response
    else:
        raise YtResponseError(url, headers, response.error())

def make_formatted_request(command_name, params, format, **kwargs):
    # None format means that we want parsed output (as yson structure) instead of string.
    # Yson parser is too slow, so we request result in JsonFormat and then convert it to yson structure.
    if format is None:
        params["output_format"] = "json"
    else:
        params["output_format"] = format.json()

    result = make_request(command_name, params, **kwargs)

    if format is None:
        return json_to_yson(json.loads(result))
    else:
        return result
