from . import yson
from .config import get_config, get_option, set_option
from .compression import get_compressor, has_compressor
from .common import (require, get_user_agent, total_seconds, forbidden_inside_job, get_started_by_short,
                     generate_uuid, hide_secure_vault, hide_auth_headers)
from .errors import (YtError, YtProxyUnavailable, YtConcurrentOperationsLimitExceeded, YtRequestTimedOut,
                     create_http_response_error)
from .format import JsonFormat
from .http_helpers import (make_request_with_retries, get_token, get_http_api_version, get_http_api_commands,
                           get_proxy_address_url, get_error_from_headers, get_header_format, ProxyProvider)
from .response_stream import ResponseStream

import yt.logger as logger
import yt.json_wrapper as json

from yt.packages.requests.auth import AuthBase

import collections
import importlib
import random
from copy import deepcopy
from datetime import datetime


def dump_params(obj, header_format):
    if header_format == "json":
        return JsonFormat().dumps_node(obj)
    elif header_format == "yson":
        return yson.dumps(obj, yson_format="text")
    else:
        assert False, "Invalid header format"


class PatientLogger:
    def __init__(self, patience):
        self.patience = patience

    def silence(self):
        self.patience = 0

    def has_fired(self):
        return self.patience <= 0

    def pester(self, message):
        self.patience -= 1
        if self.patience == 0:
            logger.warning(message)


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
        from http.client import BadStatusLine
        from socket import error as SocketError

        self.ban_errors = (ConnectionError, BadStatusLine, SocketError, YtRequestTimedOut, YtProxyUnavailable)

    def _get_light_proxy(self):
        return get_proxy_address_url(client=self.client)

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
                if get_config(self.client)["proxy"]["allow_light_proxy_for_heavy_requests"]:
                    return self._get_light_proxy()
                else:
                    raise YtError("There are no heavy proxies and using light proxy is forbidden")

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

    def on_error_occurred(self, error):
        if isinstance(error, self.ban_errors) and self.state.last_provided_proxy is not None:
            proxy = self.state.last_provided_proxy
            logger.info("Proxy %s considered unavailable and temporarily banned", proxy)
            self.state.banned_proxies[proxy] = datetime.now()

    def _configure_proxy(self, proxy):
        return get_proxy_address_url(client=self.client, replace_host=proxy)

    def _discover_heavy_proxies(self):
        proxy_config = get_config(self.client)["proxy"]
        proxy_role = proxy_config["http_proxy_role"]
        discovery_url = str(proxy_config["proxy_discovery_url"])
        network_name = proxy_config["network_name"]

        def _get_address_type(proxy_config):
            tvm_only = proxy_config["tvm_only"]
            prefer_https = proxy_config["prefer_https"]

            address_type = ""
            if tvm_only:
                address_type = "tvm_only_"
            if prefer_https:
                address_type += "https"
            else:
                address_type += "http"
            return address_type

        # `/hosts` ignores it, `/api/v4/discover_proxies` uses it and returns addresses with appropriate ports.
        query_separator = "&" if "?" in discovery_url else "?"
        discovery_url += f"{query_separator}address_type={_get_address_type(proxy_config)}"

        if "role" not in discovery_url and proxy_role is not None:
            discovery_url += f"&role={proxy_role}"

        if network_name is not None:
            discovery_url += f"&network_name={network_name}"

        heavy_proxies_response = make_request_with_retries(
            "get",
            "{0}/{1}".format(self._get_light_proxy(), discovery_url),
            client=self.client).json()

        if isinstance(heavy_proxies_response, collections.abc.Mapping):
            heavy_proxies = heavy_proxies_response.get("proxies", None)
            if heavy_proxies is None:
                logger.error("Discover proxies handler returned unexpected response: %s", heavy_proxies_response)
                raise YtError("Unexpected server response for \"discover_proxies\": missing key \"proxies\"")
        else:
            heavy_proxies = heavy_proxies_response

        return list(map(self._configure_proxy, heavy_proxies))


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
                 allow_retries=None,
                 retry_config=None,
                 mutation_id=None,
                 client=None):
    """Makes request to yt proxy. Command name is the name of command in YT API."""

    if not hasattr(make_request, "patient_logger"):
        make_request.patient_logger = PatientLogger(1000)

    if (not make_request.patient_logger.has_fired()) and command_name.endswith("_rows"):
        make_request.patient_logger.pester("PRC proxies are heavily recommended for dynamic tables RPC calls")

    if "master_cell_id" in params:
        raise YtError('Option "master_cell_id" is not supported for HTTP backend')

    commands = get_http_api_commands(client)
    api_path = "api/" + get_http_api_version(client)

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

    if mutation_id is None:
        generate_mutation_id = get_option("_generate_mutation_id", client)
        if generate_mutation_id is None:
            random_generator = get_option("_random_generator", client)
            generate_mutation_id = lambda command_descriptor: generate_uuid(random_generator)  # noqa
    else:
        generate_mutation_id = lambda command_descriptor: mutation_id  # noqa

    if command.is_volatile and allow_retries:
        if "mutation_id" not in params:
            params["mutation_id"] = generate_mutation_id(command)
        if mutation_id is not None:
            params["retry"] = True
        else:
            if "retry" not in params:
                params["retry"] = False

    if command.is_volatile and allow_retries:
        def set_retry(error, command, params, arguments):
            if command.is_volatile:
                if isinstance(error, YtConcurrentOperationsLimitExceeded):
                    # NB: initially specified mutation id is ignored.
                    # Without new mutation id, scheduler always reply with this error.
                    params["retry"] = False
                    params["mutation_id"] = generate_mutation_id(command)
                else:
                    params["retry"] = True
                if command.input_type is None:
                    arguments["data"] = dump_params(params, header_format)
                else:
                    arguments["headers"].update({"X-YT-Parameters": dump_params(params, header_format)})
        copy_params = deepcopy(params)
        retry_action = lambda error, arguments: set_retry(error, command, copy_params, arguments)  # noqa
    else:
        retry_action = None

    # prepare url.
    url_pattern = "{proxy}/{api}/{command}"
    if use_heavy_proxy:
        proxy_provider_state = get_option("_heavy_proxy_provider_state", client)
        if proxy_provider_state is None:
            proxy_provider_state = HeavyProxyProviderState()
            set_option("_heavy_proxy_provider_state", proxy_provider_state, client)
        proxy_provider = HeavyProxyProvider(client, proxy_provider_state)
        url = url_pattern.format(proxy="{proxy}", api=api_path, command=command_name)
    else:
        proxy_provider = None
        url = url_pattern.format(proxy=get_proxy_address_url(client=client), api=api_path, command=command_name)

    # prepare params, format and headers
    header_format = get_header_format(client)
    if header_format not in ["json", "yson"]:
        raise YtError("Incorrect headers format: " + str(header_format))

    header_format_header = header_format
    if header_format == "yson":
        header_format_header = "<format=text>yson"

    accept_encoding = get_config(client)["proxy"]["accept_encoding"]
    if accept_encoding is None:
        # Enable after urllib3 and requests update.
        if has_compressor("br"):
            accept_encoding = "br, gzip, identity"
        else:
            accept_encoding = "gzip, identity"
            if command_name in ["read_table", "read_file"]:
                logger.tip("To improve performance, install the \"brotli\" library")

    headers = {"User-Agent": get_user_agent(),
               "Accept-Encoding": accept_encoding,
               "X-Started-By": dump_params(get_started_by_short(), header_format)}

    data_log = ""
    write_params_to_header = True
    headers["X-YT-Header-Format"] = header_format_header
    if command.input_type is None:
        # Should we also check that command is volatile?
        require(data is None, lambda: YtError("Body should be empty in commands without input type"))
        if command.is_volatile:
            headers["Content-Type"] = "application/x-yt-yson-text" if header_format == "yson" else "application/json"
            data = dump_params(params, header_format)
            data_log = dump_params(hide_secure_vault(params), header_format)
            write_params_to_header = False

    if write_params_to_header and params:
        headers.update({"X-YT-Parameters": dump_params(params, header_format)})

    use_framing = command_name in get_config(client)["proxy"]["commands_with_framing"]
    if use_framing:
        headers["X-YT-Accept-Framing"] = "1"

    result_auth = TokenAuth(get_token(client=client))

    tvm_auth = get_config(client)["tvm_auth"]
    auth = get_config(client)["auth"]
    auth_class = get_config(client)["auth_class"]

    if auth and tvm_auth:
        raise YtError("Only one of `auth` and `tvm_auth` should be specified in the config, use `auth`")

    if auth_class and auth_class["module_name"] and auth_class["class_name"]:
        module = None
        module_name = auth_class["module_name"]
        class_name = auth_class["class_name"]
        auth_config = auth_class.get("config", None)
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            raise YtError(f"Failed to import {module_name}, install it if you want to use it as auth class")

        if auth_config:
            result_auth = getattr(module, class_name)(auth_config)
        else:
            result_auth = getattr(module, class_name)()

    if tvm_auth:
        result_auth = tvm_auth

    if auth:
        result_auth = auth

    if command.input_type in ["binary", "tabular"]:
        content_encoding = get_config(client)["proxy"]["content_encoding"]
        if content_encoding is None:
            if has_compressor("br"):
                content_encoding = "br"
            else:
                content_encoding = "gzip"
                if command_name in ["write_table", "write_file"]:
                    logger.tip("To improve performance, install the \"brotli\" library")

        headers["Content-Encoding"] = content_encoding

        require(content_encoding in ["gzip", "identity", "br", "z-lz4"],
                lambda: YtError("Content encoding '{0}' is not supported".format(content_encoding)))

        if content_encoding != "identity" and not is_data_compressed:
            data = get_compressor(content_encoding)(data)

    impersonation_user = get_config(client)["impersonation_user"]
    if impersonation_user is not None:
        headers["X-YT-User-Name"] = impersonation_user

    stream = use_framing or (command.output_type in ["binary", "tabular"])
    response = make_request_with_retries(
        command.http_method(),
        url,
        make_retries=allow_retries,
        retry_action=retry_action,
        data_log=data_log,
        headers=headers,
        data=data,
        params=params,
        stream=stream,
        response_format=response_format,
        timeout=timeout,
        auth=result_auth,
        # TODO(ignat): Refactor retrying logic to avoid this hack.
        is_ping=(command_name in ("ping_tx", "ping_transaction")),
        proxy_provider=proxy_provider,
        retry_config=retry_config,
        client=client)

    def process_trailers(response):
        trailers = response.trailers()
        if trailers is not None and trailers:
            logger.info(
                "HTTP response has non-empty trailers (request_id: %s, trailers: %s)",
                response.request_id,
                hide_auth_headers(trailers))
            error = get_error_from_headers(trailers)
            if error is not None:
                error_exception = create_http_response_error(
                    json.loads(error),
                    url=response.request_info["url"],
                    request_headers=response.request_info["headers"],
                    response_headers=trailers,
                    params=response.request_info["params"])
                raise error_exception

        if response.framing_error is not None:
            logger.info(
                "HTTP response has framing error (request_id: %s, error: %s)",
                response.request_id,
                repr(response.framing_error))
            raise response.framing_error

    if return_content:
        response_content = response.content
        # NOTE: Should be called after accessing "text" or "content" attribute
        # to ensure that all response is read and trailers are processed and can be accessed.
        process_trailers(response)
        return response_content
    else:
        return ResponseStream(
            lambda: response,
            response.iter_content(get_config(client)["read_buffer_size"]),
            lambda from_delete: response.close(),
            process_trailers,
            lambda: response.headers.get("X-YT-Response-Parameters", None))
