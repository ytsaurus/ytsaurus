from . import yson
from .config import get_config, get_option, set_option
from .compression import get_compressor
from .common import require, generate_uuid, get_version, total_seconds, forbidden_inside_job, get_started_by_short
from .errors import (YtError, YtHttpResponseError, YtProxyUnavailable,
                     YtConcurrentOperationsLimitExceeded, YtRequestTimedOut)
from .http_helpers import (make_request_with_retries, get_token, get_api_version, get_api_commands, get_proxy_url,
                           get_error_from_headers, get_header_format, ProxyProvider)
from .response_stream import ResponseStream

import yt.logger as logger
import yt.json_wrapper as json

from yt.packages.requests.auth import AuthBase
from yt.packages.six import iteritems, text_type, binary_type, iterbytes, int2byte, byte2int, PY3
from yt.packages.six.moves import map as imap

import random
from copy import deepcopy
from datetime import datetime

import os

def escape_utf8(obj):
    def escape_symbol(sym):
        # NOTE: This transformation is equivalent to converting byte char to unicode char
        # with identity encoding (latin-1) and then encoding this unicode char
        # to bytes with utf-8 encoding.
        xC0 = byte2int(b"\xC0")
        x80 = byte2int(b"\x80")
        if sym < 128:
            return int2byte(sym)
        else:
            return int2byte(xC0 | (sym >> 6)) + int2byte(x80 | (sym & ~xC0))

    def escape_str(str):
        return b"".join(imap(escape_symbol, iterbytes(str)))

    if isinstance(obj, text_type):
        # XXX(asaitgalin): Remove escape_str here since string after encode
        # is already correct utf-8 string?
        obj = escape_str(obj.encode("utf-8"))
    elif isinstance(obj, binary_type):
        obj = escape_str(obj)
    elif isinstance(obj, list):
        obj = list(imap(escape_utf8, obj))
    elif isinstance(obj, dict):
        obj = dict((escape_utf8(k), escape_utf8(v)) for k, v in iteritems(obj))
    return obj

def dump_params(obj, header_format):
    if header_format == "json":
        return json.dumps(escape_utf8(yson.yson_to_json(obj)))
    elif header_format == "yson":
        return yson.dumps(obj, yson_format="text")
    else:
        assert False, "Invalid header format"

# NB: It is necessary to avoid reference loop.
# We cannot store proxy provider in client and client in proxy provider.
class HeavyProxyProviderState(object):
    def __init__(self):
        self.banned_proxies = {}
        self.last_provided_proxy = None

class HeavyProxyProvider(ProxyProvider):
    def __init__(self, client, state=None):
        self.client = client
        if state is None:
            self.state = HeavyProxyProviderState()
        else:
            self.state = state

        from yt.packages.requests import ConnectionError
        from yt.packages.six.moves.http_client import BadStatusLine
        from socket import error as SocketError
        self.ban_errors = (ConnectionError, BadStatusLine, SocketError, YtRequestTimedOut, YtProxyUnavailable)

    def _get_light_proxy(self):
        return get_proxy_url(client=self.client)

    def __call__(self):
        now = datetime.now()
        for proxy in list(self.state.banned_proxies):
            time = self.state.banned_proxies[proxy]
            if total_seconds(now - time) * 1000 > get_config(self.client)["proxy"]["proxy_ban_timeout"]:
                logger.info("Proxy %s unbanned", proxy)
                del self.state.banned_proxies[proxy]

        if get_config(self.client)["proxy"]["enable_proxy_discovery"]:
            limit = get_config(self.client)["proxy"]["number_of_top_proxies_for_random_choice"]
            unbanned_proxies = []
            heavy_proxies = self._discover_heavy_proxies()
            if not heavy_proxies:
                return self._get_light_proxy()

            for proxy in heavy_proxies:
                if proxy not in self.state.banned_proxies:
                    unbanned_proxies.append(proxy)

            result_proxy = None
            if unbanned_proxies:
                upper_bound = min(limit, len(unbanned_proxies))
                result_proxy = unbanned_proxies[random.randint(0, upper_bound - 1)]
            else:
                upper_bound = min(limit, len(heavy_proxies))
                logger.warning(
                    "All proxies are banned, use random proxy from top %d of discovered proxies",
                    upper_bound)
                result_proxy = heavy_proxies[random.randint(0, upper_bound - 1)]

            self.state.last_provided_proxy = result_proxy
            return result_proxy

        return self._get_light_proxy()

    def on_error_occured(self, error):
        if isinstance(error, self.ban_errors) and self.state.last_provided_proxy is not None:
            proxy = self.state.last_provided_proxy
            logger.info("Proxy %s banned", proxy)
            self.state.banned_proxies[proxy] = datetime.now()

    def _discover_heavy_proxies(self):
        discovery_url = get_config(self.client)["proxy"]["proxy_discovery_url"]
        return make_request_with_retries(
            "get",
            "http://{0}/{1}".format(self._get_light_proxy(), discovery_url),
            client=self.client).json()

class TokenAuth(AuthBase):
    def __init__(self, token):
        self.token = token

    def set_token(self, request):
        if self.token is not None:
            request.headers["Authorization"] = "OAuth " + self.token

    def handle_redirect(self, request, **kwargs):
        self.set_token(request)
        return request

    def __call__(self, request):
        self.set_token(request)
        request.register_hook("response", self.handle_redirect)
        return request

@forbidden_inside_job
def make_request(command_name,
                 params,
                 data=None,
                 is_data_compressed=None,
                 return_content=True,
                 response_format=None,
                 use_heavy_proxy=False,
                 timeout=None,
                 client=None,
                 allow_retries=None,
                 decode_content=True):
    """Makes request to yt proxy. Command name is the name of command in YT API."""

    if "master_cell_id" in params:
        raise YtError('Option "master_cell_id" is not supported for HTTP backend')

    commands = get_api_commands(client)
    api_path = "api/" + get_api_version(client)

    # Get command description
    require(command_name in commands,
            lambda: YtError("Command {0} is not supported by {1}".format(command_name, api_path)))
    command = commands[command_name]

    # Determine make retries or not and set mutation if needed
    if allow_retries is None:
        allow_retries = not command.is_heavy and \
            command_name not in ["concatenate"]

    if timeout is None:
        if command.is_heavy:
            request_timeout = get_config(client)["proxy"]["heavy_request_timeout"]
        else:
            request_timeout = get_config(client)["proxy"]["request_timeout"]
        connect_timeout = get_config(client)["proxy"]["connect_timeout"]

        timeout = (connect_timeout, request_timeout)

    if command.is_volatile and allow_retries:
        if "mutation_id" not in params:
            params["mutation_id"] = generate_uuid(get_option("_random_generator", client))
        if "retry" not in params:
            params["retry"] = False

    if command.is_volatile and allow_retries:
        def set_retry(error, command, params, arguments):
            if command.is_volatile:
                if isinstance(error, YtConcurrentOperationsLimitExceeded):
                    # NB: initially specified mutation id is ignored.
                    # Without new mutation id, scheduler always reply with this error.
                    params["retry"] = False
                    params["mutation_id"] = generate_uuid(get_option("_random_generator", client))
                else:
                    params["retry"] = True
                if command.input_type is None:
                    arguments["data"] = dump_params(params, header_format)
                else:
                    arguments["headers"].update({"X-YT-Parameters": dump_params(params, header_format)})
        copy_params = deepcopy(params)
        retry_action = lambda error, arguments: set_retry(error, command, copy_params, arguments)
    else:
        retry_action = None

    # prepare url.
    url_pattern = "http://{proxy}/{api}/{command}"
    if use_heavy_proxy:
        proxy_provider_state = get_option("_heavy_proxy_provider_state", client)
        if proxy_provider_state is None:
            proxy_provider_state = HeavyProxyProviderState()
            set_option("_heavy_proxy_provider_state", proxy_provider_state, client)
        proxy_provider = HeavyProxyProvider(client, proxy_provider_state)
        url = url_pattern.format(proxy="{proxy}", api=api_path, command=command_name)
    else:
        proxy_provider = None
        url = url_pattern.format(proxy=get_proxy_url(client=client), api=api_path, command=command_name)

    # prepare params, format and headers
    user_agent = "Python wrapper " + get_version()
    if "_ARGCOMPLETE" in os.environ:
        user_agent += " [argcomplete mode]"

    header_format = get_header_format(client)
    if header_format not in ["json", "yson"]:
        raise YtError("Incorrect headers format: " + str(header_format))

    header_format_header = header_format
    if header_format == "yson":
        header_format_header = "<format=text>yson"

    headers = {"User-Agent": user_agent,
               "Accept-Encoding": get_config(client)["proxy"]["accept_encoding"],
               "X-Started-By": dump_params(get_started_by_short(), header_format)}

    write_params_to_header = True
    headers["X-YT-Header-Format"] = header_format_header
    if command.input_type is None:
        # Should we also check that command is volatile?
        require(data is None, lambda: YtError("Body should be empty in commands without input type"))
        if command.is_volatile:
            headers["Content-Type"] = "application/x-yt-yson-text" if header_format == "yson" else "application/json"
            data = dump_params(params, header_format)
            write_params_to_header = False

    if write_params_to_header and params:
        headers.update({"X-YT-Parameters": dump_params(params, header_format)})

    auth = TokenAuth(get_token(client=client))

    if command.input_type in ["binary", "tabular"]:
        content_encoding = get_config(client)["proxy"]["content_encoding"]
        headers["Content-Encoding"] = content_encoding

        require(content_encoding in ["gzip", "identity", "br"],
                lambda: YtError("Content encoding '{0}' is not supported".format(content_encoding)))

        if content_encoding in ["br", "gzip"] and not is_data_compressed:
            data = get_compressor(content_encoding)(data)

    stream = (command.output_type in ["binary", "tabular"])
    response = make_request_with_retries(
        command.http_method(),
        url,
        make_retries=allow_retries,
        retry_action=retry_action,
        log_body=(command.input_type is None),
        headers=headers,
        data=data,
        params=params,
        stream=stream,
        response_format=response_format,
        timeout=timeout,
        auth=auth,
        # TODO(ignat): Refactor retrying logic to avoid this hack.
        is_ping=(command_name == "ping_tx"),
        proxy_provider=proxy_provider,
        client=client)

    def process_error(response):
        trailers = response.trailers()
        if trailers is None:
            return

        error = get_error_from_headers(trailers)
        if error is not None:
            raise YtHttpResponseError(error=json.loads(error), **response.request_info)

    # Determine type of response data and return it
    if return_content:
        response_content = response.text if (decode_content and PY3) else response.content
        # NOTE: Should be called after accessing "text" or "content" attribute
        # to ensure that all response is read and trailers are processed and can be accessed.
        process_error(response)
        return response_content
    else:
        return ResponseStream(
            lambda: response,
            response.iter_content(get_config(client)["read_buffer_size"]),
            lambda: response.close(),
            process_error,
            lambda: response.headers.get("X-YT-Response-Parameters", None))
