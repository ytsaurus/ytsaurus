import yt.logger as logger
from config import get_config, get_option, set_option, get_backend_type
from common import require, get_backoff, get_value
from errors import YtError, YtTokenError, YtProxyUnavailable, YtIncorrectResponse, build_http_response_error, YtRequestRateLimitExceeded
from command import parse_commands

import yt.yson as yson
import yt.packages.simplejson as json

import os
import random
import string
import time
import types
import yt.packages.requests
from datetime import datetime
from socket import error as SocketError

# We cannot use requests.HTTPError in module namespace because of conflict with python3 http library
from yt.packages.requests import HTTPError, ConnectionError, Timeout
from yt.packages.requests.packages.urllib3.packages.httplib import BadStatusLine, IncompleteRead
RETRIABLE_ERRORS = (HTTPError, ConnectionError, Timeout, IncompleteRead, SocketError, BadStatusLine, YtRequestRateLimitExceeded, YtIncorrectResponse, YtProxyUnavailable)

session_ = yt.packages.requests.Session()
def get_session():
    return session_

def _cleanup_http_session():
    global session_
    session_ = yt.packages.requests.Session()

def configure_ip(client):
    if get_option("_ip_configured", client):
        return

    set_option("_ip_configured", True, client)

    force_ipv4 = get_config(client)["proxy"]["force_ipv4"]
    force_ipv6 = get_config(client)["proxy"]["force_ipv6"]

    if force_ipv4 or force_ipv6:
        import socket
        origGetAddrInfo = socket.getaddrinfo

        protocol = socket.AF_INET if force_ipv4 else socket.AF_INET6

        def getAddrInfoWrapper(host, port, family=0, socktype=0, proto=0, flags=0):
            return origGetAddrInfo(host, port, protocol, socktype, proto, flags)

        # replace the original socket.getaddrinfo by our version
        socket.getaddrinfo = getAddrInfoWrapper

def parse_error_from_headers(headers):
    if int(headers.get("x-yt-response-code", 0)) != 0:
        return json.loads(headers["x-yt-error"])
    return None

def create_response(response, request_headers, client):
    header_format = get_value(get_config(client)["proxy"]["header_format"], "json")
    def loads(str):
        if header_format == "json":
            return yson.json_to_yson(json.loads(str))
        if header_format == "yson":
            return yson.loads(str)
        raise YtError("Incorrect header format: {0}".format(header_format))

    def get_error():
        if not str(response.status_code).startswith("2"):
            # 401 is case of incorrect token
            if response.status_code == 401:
                url_base = "/".join(response.url.split("/")[:3])
                raise YtTokenError(
                    "Your authentication token was rejected by the server (X-YT-Request-ID: {0}).\n"
                    "Please refer to {1}/auth/ for obtaining a valid token or submit your request to https://st.yandex-team.ru/createTicket?queue=YTADMIN"\
                        .format(
                            response.headers.get("X-YT-Request-ID", "missing"),
                            url_base))

                try:
                    response.json()
                except json.JSONDecodeError:
                    raise YtIncorrectResponse("Response body can not be decoded from JSON (bug in proxy)", response)
            return response.json()
        else:
            error = parse_error_from_headers(response.headers)
            return error

    def error(self):
        return self._error

    def is_ok(self):
        return self._error is None

    if "X-YT-Response-Parameters" in response.headers:
        response.headers["X-YT-Response-Parameters"] = loads(response.headers["X-YT-Response-Parameters"])
    response.request_headers = request_headers
    response._error = get_error()
    response.error = types.MethodType(error, response)
    response.is_ok = types.MethodType(is_ok, response)
    return response

def _process_request_backoff(current_time, client):
    backoff = get_config(client)["proxy"]["request_backoff_time"]
    if backoff is not None:
        last_request_time = getattr(get_session(), "last_request_time", 0)
        now_seconds = (current_time - datetime(1970, 1, 1)).total_seconds()
        diff = now_seconds - last_request_time
        if diff * 1000.0 < float(backoff):
            time.sleep(float(backoff) / 1000.0 - diff)
        get_session().last_request_time = now_seconds

def make_request_with_retries(method, url, make_retries=True, retry_unavailable_proxy=True, response_should_be_json=False, timeout=None, retry_action=None, client=None, **kwargs):
    configure_ip(client)

    if timeout is None:
        timeout = get_config(client)["proxy"]["request_retry_timeout"] / 1000.0

    retriable_errors = list(RETRIABLE_ERRORS)
    if not retry_unavailable_proxy:
        retriable_errors.remove(YtProxyUnavailable)

    for attempt in xrange(get_config(client)["proxy"]["request_retry_count"]):
        current_time = datetime.now()
        _process_request_backoff(current_time, client=client)
        headers = kwargs.get("headers", {})
        try:
            try:
                response = create_response(get_session().request(method, url, timeout=timeout, **kwargs), headers, client)
                if get_option("_ENABLE_HTTP_CHAOS_MONKEY", client) and random.randint(1, 5) == 1:
                    raise YtIncorrectResponse("", response)
            except ConnectionError as error:
                if hasattr(error, "response"):
                    raise build_http_response_error(url, headers, create_response(error.response, headers, client).error())
                else:
                    raise

            # Sometimes (quite often) we obtain incomplete response with body expected to be JSON.
            # So we should retry such requests.
            if response_should_be_json:
                try:
                    response.json()
                except json.JSONDecodeError:
                    raise YtIncorrectResponse("Response body can not be decoded from JSON (bug in proxy)", response)
            if response.status_code == 503:
                raise YtProxyUnavailable(response)
            if not response.is_ok():
                raise build_http_response_error(url, headers, response.error())

            return response
        except tuple(retriable_errors) as error:
            message =  "HTTP %s request %s has failed with error %s, message: '%s', headers: %s" % (method, url, type(error), error.message, headers)
            if make_retries and attempt + 1 < get_config(client)["proxy"]["request_retry_count"]:
                if retry_action is not None:
                    retry_action(kwargs)
                backoff = get_backoff(get_config(client)["proxy"]["request_retry_timeout"], current_time)
                logger.warning(message)
                if backoff:
                    logger.warning("Sleep for %.2lf seconds before next retry", backoff)
                    time.sleep(backoff)
                logger.warning("New retry (%d) ...", attempt + 2)
            else:
                raise

def make_get_request_with_retries(url, **kwargs):
    response = make_request_with_retries("get", url, **kwargs)
    return response.json()

def get_proxy_url(proxy=None, check=True, client=None):
    if proxy is None:
        proxy = get_config(client=client)["proxy"]["url"]

    if proxy is not None and "." not in proxy and "localhost" not in proxy:
        proxy = proxy + get_config(client=client)["proxy"]["default_suffix"]

    if check:
        require(proxy, YtError("You should specify proxy"))

    return proxy

def _request_api(proxy, version=None, client=None):
    proxy = get_proxy_url(proxy, client=client)
    location = "api" if version is None else "api/" + version
    return make_get_request_with_retries("http://{0}/{1}".format(proxy, location))

def get_api_version(client=None):
    api_version_option = get_option("_api_version", client)
    if api_version_option is not None:
        return api_version_option

    api_version_from_config = get_config(client)["api_version"]
    if api_version_from_config is not None:
        set_option("_api_version", api_version_from_config, client)
        return api_version_from_config

    require(get_backend_type(client) == "http",
            YtError("Cannot automatically detect api version for non-proxy backend"))

    api_versions = _request_api(get_config(client)["proxy"]["url"])
    if "v3" in api_versions:
        api_version = "v3"
    else:
        api_version = "v2"
    require(api_version in api_versions, YtError("API {0} is not supported".format(api_version)))

    set_option("_api_version", api_version, client)

    return api_version

def get_api_commands(client=None):
    if get_option("_commands", client):
        return get_option("_commands", client)

    commands = parse_commands(
        _request_api(
            get_config(client)["proxy"]["url"],
            version=get_api_version(client),
            client=client))
    set_option("_commands", commands, client)

    return commands

def get_token(client=None):
    if not get_config(client)["enable_token"]:
        return None

    token = get_config(client)["token"]
    if token is None:
        token_path = get_config(client=client)["token_path"]
        if token_path is None:
            token_path = os.path.join(os.path.expanduser("~"), ".yt/token")
        if os.path.isfile(token_path):
            token = open(token_path).read().strip()
            logger.debug("Token got from %s", token_path)
    else:
        logger.debug("Token got from environment variable")
    if token is not None:
        require(all(c in string.hexdigits for c in token),
                YtTokenError("You have an improper authentication token"))
    if not token:
        token = None
    return token

def get_user_name(token=None, headers=None, client=None):
    if token is None and headers is None:
        token = get_token(client)

    version = get_api_version(client=client)
    proxy = get_proxy_url(None, client=client)

    if version == "v3":
        if headers is None:
            headers = {}
        if token is not None:
            headers["Authorization"] = "OAuth " + token.strip()
        data = None
        verb = "whoami"
    else:
        if not token:
            return None
        data = "token=" + token.strip()
        verb = "login"

    response = make_request_with_retries(
        "post",
        "http://{0}/auth/{1}".format(proxy, verb),
        headers=headers,
        data=data)
    login = response.json()["login"]
    if not login:
        return None
    return login
