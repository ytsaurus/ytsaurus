import config
import yt.logger as logger
from compression_wrapper import create_zlib_generator
from common import require, generate_uuid
from errors import YtError, YtResponseError
from version import VERSION
from http import make_get_request_with_retries, make_request_with_retries, Response, get_token, get_proxy

from yt.yson.convert import json_to_yson

import yt.packages.requests as requests

import sys
import socket
import simplejson as json
from simplejson import JSONEncoder

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
    elif type == "raw":
        return response
    else:
        raise YtError("Incorrent response type: " + type)

def get_hosts():
    return make_get_request_with_retries("http://{0}/{1}".format(get_proxy(config.http.PROXY), config.HOSTS))

def get_host_for_heavy_operation():
    if config.USE_HOSTS:
        hosts = get_hosts()
        if hosts:
            return hosts[0]
    return config.http.PROXY


def make_request(command_name, params,
                 data=None, proxy=None,
                 return_raw_response=False, verbose=False,
                 retry_unavailable_proxy=True):
    """
    Makes request to yt proxy. Command name is the name of command in YT API.
    Option return_raw_response forces returning response of requests library
    without extracting data field.
    """
    def print_info(msg, *args, **kwargs):
        # Verbose option is used for debugging because it is more
        # selective than logging
        if verbose:
            # We don't use kwargs because python doesn't support such kind of formatting
            print >>sys.stderr, msg % args
        logger.debug(msg, *args, **kwargs)

    # Trying to set http retries in requests
    requests.adapters.DEFAULT_RETRIES = config.http.REQUESTS_RETRIES
    
    # Set timeout for requests. Unfortunately, requests param timeout works incorrectly
    # when data is a generator.
    socket.setdefaulttimeout(config.http.CONNECTION_TIMEOUT)

    # Prepare request url.
    if proxy is None:
        proxy = config.http.PROXY
    require(proxy, YtError("You should specify proxy"))

    # Get command description
    command = config.COMMANDS[command_name]

    # Determine make retries or not and set mutation if needed
    allow_retries = \
            not command.is_volatile or \
            (config.http.RETRY_VOLATILE_COMMANDS and not command.is_heavy)
    if command.is_volatile and allow_retries:
        if config.MUTATION_ID is not None:
            params["mutation_id"] = config.MUTATION_ID
        else:
            params["mutation_id"] = generate_uuid()

    # prepare url
    url = "http://{0}/{1}/{2}".format(proxy, config.API_PATH, command_name)
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

    if config.API_PATH == "api":
        if "input_format" in params:
            headers["X-YT-Input-Format"] = json.dumps(params["input_format"])
            del params["input_format"]
        if "output_format" in params:
            headers["X-YT-Output-Format"] = json.dumps(params["output_format"])
            del params["output_format"]

    if params:
        headers.update({"X-YT-Parameters": json.dumps(escape_utf8(params))})

    token = get_token()
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

    def request():
        try:
            rsp = requests.request(
                url=url,
                method=command.http_method(),
                headers=headers,
                data=data,
                stream=stream)
        except requests.ConnectionError as error:
            print >>sys.stderr, type(error)
            if hasattr(error, "response"):
                rsp = error.response
            else:
                raise
        return Response(rsp)

    response = make_request_with_retries(
        request,
        allow_retries,
        retry_unavailable_proxy=retry_unavailable_proxy,
        description=url,
        return_raw_response=return_raw_response)

    # Hide token for security reasons
    if "Authorization" in headers:
        headers["Authorization"] = "x" * 32
    print_info("Response header %r", response.http_response.headers)

    # Determine type of response data and return it
    if response.is_ok():
        if return_raw_response:
            return response.http_response
        else:
            return response.content()
    else:
        raise YtResponseError(url, headers, response.error())

def make_formatted_request(command_name, params, format, **kwargs):
    # None format means that we want parsed output (as yson structure) instead of string.
    # Yson parser is too slow, so we request result in JsonFormat and then convert it to yson structure.
    if format is None:
        params["output_format"] = "json"
    else:
        params["output_format"] = format.json()

    result = make_request(command_name, params)

    if format is None:
        return json_to_yson(json.loads(result))
    else:
        return result
