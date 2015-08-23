from config import get_config, get_option
import yt.logger as logger
import yt.yson as yson
import yt.packages.simplejson as json
from compression_wrapper import create_zlib_generator
from common import require, generate_uuid, bool_to_string, get_value, get_version
from errors import YtError, YtHttpResponseError, YtProxyUnavailable
from http import make_get_request_with_retries, make_request_with_retries, get_token, get_api_version, get_api_commands, get_proxy_url, parse_error_from_headers
from response_stream import ResponseStream

import sys
from copy import deepcopy
from datetime import datetime

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

def get_hosts(client=None):
    proxy = get_proxy_url(None, client=client)
    hosts = get_config(client)["proxy"]["proxy_discovery_url"]
    return make_get_request_with_retries("http://{0}/{1}".format(proxy, hosts))

def get_heavy_proxy(client):
    def total_seconds(td):
        return float(td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6) / 10 ** 6

    banned_hosts = get_option("_banned_proxies", client)

    now = datetime.now()
    for host in banned_hosts.keys():
        time = banned_hosts[host]
        if total_seconds(now - time) * 1000 > get_config(client)["proxy"]["proxy_ban_timeout"]:
            del banned_hosts[host]
    if get_config(client)["proxy"]["enable_proxy_discovery"]:
        hosts = get_hosts(client=client)
        for host in hosts:
            if host not in banned_hosts:
                return host
        if hosts:
            return hosts[0]

    return get_config(client)["proxy"]["url"]

def ban_host(host, client):
    get_option("_banned_proxies", client)[host] = datetime.now()

def make_request(command_name, params,
                 data=None, proxy=None,
                 return_content=True, verbose=False,
                 retry_unavailable_proxy=True,
                 response_should_be_json=False,
                 use_heavy_proxy=False,
                 client=None):
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
    if use_heavy_proxy:
        proxy = get_heavy_proxy(client)
    else:
        proxy = get_proxy_url(proxy, client=client)

    commands = get_api_commands(client)
    api_path = "api/" + get_api_version(client)

    # Get command description
    require(command_name in commands,
            YtError("Command {0} is not supported by {1}".format(command_name, api_path)))
    command = commands[command_name]

    # Determine make retries or not and set mutation if needed
    allow_retries = not command.is_heavy

    if command.is_volatile and allow_retries:
        if "mutation_id" not in params:
            params["mutation_id"] = generate_uuid()
        if "retry" not in params:
            params["retry"] = bool_to_string(False)

    if command.is_volatile and allow_retries:
        def set_retry(command, params, arguments):
            if command.is_volatile:
                params["retry"] = bool_to_string(True)
                if command.input_type is None:
                    arguments["data"] = dumps(params)
                else:
                    arguments["headers"].update({"X-YT-Parameters": dumps(params)})
        copy_params = deepcopy(params)
        retry_action = lambda arguments: set_retry(command, copy_params, arguments)
    else:
        retry_action = None

    # prepare url
    url = "http://{0}/{1}/{2}".format(proxy, api_path, command_name)
    print_info("Request url: %r", url)

    # prepare params, format and headers
    headers = {"User-Agent": "Python wrapper " + get_version(),
               "Accept-Encoding": get_config(client)["proxy"]["accept_encoding"],
               "X-YT-Correlation-Id": generate_uuid()}

    header_format = get_value(get_config(client)["proxy"]["header_format"], "json")
    if header_format not in ["json", "yson"]:
        raise YtError("Incorrect headers format: " + str(header_format))
    def dumps(obj):
        if header_format == "json":
            return json.dumps(escape_utf8(yson.yson_to_json(obj)))
        if header_format == "yson":
            return yson.dumps(obj, yson_format="text")
        assert False

    header_format_header = header_format
    if header_format == "yson":
        header_format_header = "<format=text>yson"

    headers["X-YT-Header-Format"] = header_format_header
    if command.input_type is None:
        # Should we also check that command is volatile?
        require(data is None, YtError("Body should be empty in commands without input type"))
        if command.is_volatile:
            headers["Content-Type"] = "application/x-yt-yson-text" if header_format == "yson" else "application/json"
            data = dumps(params)
            params = {}

    if params:
        headers.update({"X-YT-Parameters": dumps(params)})

    token = get_token(client=client)
    if token is not None:
        headers["Authorization"] = "OAuth " + token

    if command.input_type in ["binary", "tabular"]:
        content_encoding =  get_config(client)["proxy"]["content_encoding"]
        headers["Content-Encoding"] = content_encoding
        if content_encoding == "identity":
            pass
        elif content_encoding == "gzip":
            data = create_zlib_generator(data)
        else:
            raise YtError("Content encoding '%s' is not supported" % get_config(client)["proxy"]["content_encoding"])

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
            retry_action=retry_action,
            headers=headers,
            data=data,
            stream=stream,
            response_should_be_json=response_should_be_json,
            client=client)
    except YtProxyUnavailable:
        ban_host(proxy, client=client)
        raise

    print_info("Response headers %r", response.headers)

    # Determine type of response data and return it
    if return_content:
        return response.content
    else:
        def process_error(request):
            trailers = response.trailers()
            error = parse_error_from_headers(trailers)
            if error is not None:
                raise YtHttpResponseError(response.url, response.headers, error)

        return ResponseStream(
            lambda: response,
            response.iter_content(get_config(client)["read_buffer_size"]),
            lambda: response.close(),
            process_error,
            lambda: response.headers.get("X-YT-Response-Parameters", None))

