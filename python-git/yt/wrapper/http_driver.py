import yson
from config import get_config, get_option
from compression_wrapper import create_zlib_generator
from common import require, generate_uuid, bool_to_string, get_version, total_seconds, forbidden_inside_job
from errors import YtError, YtHttpResponseError, YtProxyUnavailable, YtConcurrentOperationsLimitExceeded, YtRequestTimedOut
from http import make_get_request_with_retries, make_request_with_retries, get_token, get_api_version, get_api_commands, get_proxy_url, parse_error_from_headers, get_header_format
from response_stream import ResponseStream

from yt.packages.requests.auth import AuthBase

import yt.logger as logger
import yt.json as json

from copy import deepcopy
from datetime import datetime

def get_proxy_ban_errors():
    from yt.packages.requests import ConnectionError
    from httplib import BadStatusLine
    from socket import error as SocketError
    return (ConnectionError, BadStatusLine, SocketError, YtRequestTimedOut, YtProxyUnavailable)

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
    banned_hosts = get_option("_banned_proxies", client)

    now = datetime.now()
    for host in banned_hosts.keys():
        time = banned_hosts[host]
        if total_seconds(now - time) * 1000 > get_config(client)["proxy"]["proxy_ban_timeout"]:
            logger.info("Host %s unbanned", host)
            del banned_hosts[host]
    if get_config(client)["proxy"]["enable_proxy_discovery"]:
        hosts = get_hosts(client=client)
        for host in hosts:
            if host not in banned_hosts:
                return host
        logger.warning("All hosts are banned, use %s as first in the hosts list", hosts[0])
        if hosts:
            return hosts[0]

    return get_config(client)["proxy"]["url"]

def ban_host(host, client):
    logger.info("Host %s banned", host)
    get_option("_banned_proxies", client)[host] = datetime.now()

class TokenAuth(AuthBase):
    def __init__(self, token):
        self.token = token

    def set_token(self, request):
        if self.token is not None:
            request.headers["Authorization"] =  "OAuth " + self.token

    def handle_redirect(self, request, **kwargs):
        self.set_token(request)
        return request

    def __call__(self, request):
        self.set_token(request)
        request.register_hook('response', self.handle_redirect)
        return request

@forbidden_inside_job
def make_request(command_name, params,
                 data=None,
                 is_data_compressed=None,
                 proxy=None,
                 return_content=True,
                 retry_unavailable_proxy=True,
                 response_should_be_json=False,
                 use_heavy_proxy=False,
                 timeout=None,
                 client=None,
                 allow_retries=None):
    """
    Makes request to yt proxy. Command name is the name of command in YT API.
    """
    # Prepare request url.
    if use_heavy_proxy:
        proxy = get_heavy_proxy(client)
    else:
        proxy = get_proxy_url(proxy, client=client)

    commands = get_api_commands(client)
    api_path = "api/" + get_api_version(client)

    # Get command description
    require(command_name in commands,
            lambda: YtError("Command {0} is not supported by {1}".format(command_name, api_path)))
    command = commands[command_name]

    # Determine make retries or not and set mutation if needed
    if allow_retries is None:
        allow_retries = not command.is_heavy and \
            command_name not in ["concatenate"] and \
            get_config(client)["proxy"]["request_retry_enable"]

    if timeout is None:
        if command.is_heavy:
            timeout = get_config(client)["proxy"]["heavy_request_retry_timeout"] / 1000.0
        else:
            timeout = get_config(client)["proxy"]["request_retry_timeout"] / 1000.0

    if command.is_volatile and allow_retries:
        if "mutation_id" not in params:
            params["mutation_id"] = generate_uuid(get_option("_random_generator", client))
        if "retry" not in params:
            params["retry"] = bool_to_string(False)

    if command.is_volatile and allow_retries:
        def set_retry(error, command, params, arguments):
            if command.is_volatile:
                if isinstance(error, YtConcurrentOperationsLimitExceeded):
                    # NB: initially specified mutation id is ignored.
                    # Wihtput new mutation id, scheduler always reply with this error.
                    params["retry"] = bool_to_string(False)
                    params["mutation_id"] = generate_uuid(get_option("_random_generator", client))
                else:
                    params["retry"] = bool_to_string(True)
                if command.input_type is None:
                    arguments["data"] = dumps(params)
                else:
                    arguments["headers"].update({"X-YT-Parameters": dumps(params)})
        copy_params = deepcopy(params)
        retry_action = lambda error, arguments: set_retry(error, command, copy_params, arguments)
    else:
        retry_action = None

    # prepare url
    url = "http://{0}/{1}/{2}".format(proxy, api_path, command_name)

    # prepare params, format and headers
    headers = {"User-Agent": "Python wrapper " + get_version(),
               "Accept-Encoding": get_config(client)["proxy"]["accept_encoding"]}

    header_format = get_header_format(client)
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

    write_params_to_header = True
    headers["X-YT-Header-Format"] = header_format_header
    if command.input_type is None:
        # Should we also check that command is volatile?
        require(data is None, lambda: YtError("Body should be empty in commands without input type"))
        if command.is_volatile:
            headers["Content-Type"] = "application/x-yt-yson-text" if header_format == "yson" else "application/json"
            data = dumps(params)
            write_params_to_header = False

    if write_params_to_header and params:
        headers.update({"X-YT-Parameters": dumps(params)})

    auth = TokenAuth(get_token(client=client))

    if command.input_type in ["binary", "tabular"]:
        content_encoding =  get_config(client)["proxy"]["content_encoding"]
        headers["Content-Encoding"] = content_encoding

        require(content_encoding in ["gzip", "identity"],
                lambda: YtError("Content encoding '{0}' is not supported".format(content_encoding)))

        if content_encoding == "gzip" and not is_data_compressed:
            data = create_zlib_generator(data)

    stream = (command.output_type in ["binary", "tabular"])
    try:
        response = make_request_with_retries(
            command.http_method(),
            url,
            make_retries=allow_retries,
            retry_unavailable_proxy=retry_unavailable_proxy,
            retry_action=retry_action,
            log_body=(command.input_type is None),
            headers=headers,
            data=data,
            params=params,
            stream=stream,
            response_should_be_json=response_should_be_json,
            timeout=timeout,
            auth=auth,
            client=client)
    except get_proxy_ban_errors():
        ban_host(proxy, client=client)
        raise

    # Determine type of response data and return it
    if return_content:
        return response.content
    else:
        def process_error(response):
            trailers = response.trailers()
            error = parse_error_from_headers(trailers)
            if error is not None:
                raise YtHttpResponseError(error=error, **response.request_info)

        return ResponseStream(
            lambda: response,
            response.iter_content(get_config(client)["read_buffer_size"]),
            lambda: response.close(),
            process_error,
            lambda: response.headers.get("X-YT-Response-Parameters", None))

