import http_config
import config
import yt.logger as logger
from common import require, get_backoff, get_value
from errors import YtError, YtTokenError, YtProxyUnavailable, YtIncorrectResponse, YtResponseError, YtRequestRateLimitExceeded

import os
import string
import time
import httplib
import yt.packages.requests
import simplejson as json
from datetime import datetime
from socket import error as SocketError
from httplib import BadStatusLine

# We cannot use requests.HTTPError in module namespace because of conflict with python3 http library
from yt.packages.requests import HTTPError, ConnectionError, Timeout
RETRIABLE_ERRORS = (HTTPError, ConnectionError, Timeout, httplib.IncompleteRead, SocketError, BadStatusLine, YtRequestRateLimitExceeded, YtIncorrectResponse)

session_ = yt.packages.requests.Session()
def get_session():
    return session_

if http_config.FORCE_IPV4 or http_config.FORCE_IPV6:
    import socket
    origGetAddrInfo = socket.getaddrinfo

    protocol = socket.AF_INET if http_config.FORCE_IPV4 else socket.AF_INET6

    def getAddrInfoWrapper(host, port, family=0, socktype=0, proto=0, flags=0):
        return origGetAddrInfo(host, port, protocol, socktype, proto, flags)

    # replace the original socket.getaddrinfo by our version
    socket.getaddrinfo = getAddrInfoWrapper

class Response(object):
    def __init__(self, raw_response):
        self.raw_response = raw_response
        self._return_code_processed = False

    def error(self):
        self._process_return_code()
        if hasattr(self, "_error"):
            return self._error
        return None

    def is_ok(self):
        self._process_return_code()
        return not hasattr(self, "_error")

    def json(self):
        return self.raw_response.json()

    def content(self):
        return self.raw_response.content

    def headers(self):
        return self.raw_response.headers

    def _process_return_code(self):
        if self._return_code_processed:
            return

        if not str(self.raw_response.status_code).startswith("2"):
            # 401 is case of incorrect token
            if self.raw_response.status_code == 401:
                url_base = "/".join(self.raw_response.url.split("/")[:3])
                raise YtTokenError(
                    "Your authentication token was rejected by the server (X-YT-Request-ID: {0}).\n"
                    "Please refer to {1}/auth/ for obtaining a valid token or contact us at yt@yandex-team.ru."\
                        .format(
                            self.raw_response.headers.get("X-YT-Request-ID", "absent"),
                            url_base))
            self._error = self.raw_response.json()
        elif int(self.raw_response.headers.get("x-yt-response-code", 0)) != 0:
            self._error = json.loads(self.raw_response.headers["x-yt-error"])
        self._return_code_processed = True

def _process_request_backoff(current_time):
    if http_config.REQUEST_BACKOFF is not None:
        last_request_time = getattr(get_session(), "last_request_time", 0)
        now_seconds = (current_time - datetime(1970, 1, 1)).total_seconds()
        diff = now_seconds - last_request_time
        if diff * 1000.0 < float(http_config.REQUEST_BACKOFF):
            time.sleep(float(http_config.REQUEST_BACKOFF) / 1000.0 - diff)
        get_session().last_request_time = now_seconds

def make_request_with_retries(method, url, make_retries=True, retry_unavailable_proxy=True, response_should_be_json=False, timeout=None, **kwargs):
    if timeout is not None:
        timeout = http_config.REQUEST_RETRY_TIMEOUT / 1000.0

    retriable_errors = list(RETRIABLE_ERRORS)
    if retry_unavailable_proxy:
        retriable_errors.append(YtProxyUnavailable)

    for attempt in xrange(http_config.REQUEST_RETRY_COUNT):
        current_time = datetime.now()
        _process_request_backoff(current_time)
        try:
            try:
                response = Response(get_session().request(method, url, timeout=timeout, **kwargs))
            except ConnectionError as error:
                if hasattr(error, "response"):
                    raise YtResponseError(url, kwargs.get("headers", {}), Response(error.response).error())
                else:
                    raise
            except YtResponseError as error:
                if error.is_request_rate_limit_exceeded():
                    raise YtRequestRateLimitExceeded(error)
                raise

            # Sometimes (quite often) we obtain incomplete response with body expected to be JSON.
            # So we should retry such requests.
            if response_should_be_json:
                try:
                    json.loads(response.content())
                except json.JSONDecodeError:
                    raise YtIncorrectResponse("Response body can not be decoded from JSON (bug in proxy)")
            if response.raw_response.status_code == 503:
                raise YtProxyUnavailable("Retrying response with code 503 and body %s" % response.content())
            if not response.is_ok():
                raise YtResponseError(url, kwargs.get("headers", {}), response.error())

            return response

        except tuple(retriable_errors) as error:
            message =  "HTTP %s request %s has failed with error %s, message: '%s', headers: %s" % (method, url, type(error), str(error), kwargs.get("headers", {}))
            if make_retries and attempt + 1 < http_config.REQUEST_RETRY_COUNT:
                backoff = get_backoff(http_config.REQUEST_RETRY_TIMEOUT, current_time)
                if backoff:
                    logger.warning("%s. Sleep for %.2lf seconds...", message, backoff)
                    time.sleep(backoff)
                logger.warning("New retry (%d) ...", attempt + 2)
            else:
                raise

def make_get_request_with_retries(url, **kwargs):
    response = make_request_with_retries("get", url, **kwargs)
    return response.json()

def get_proxy_url(proxy=None, client=None):
    client = get_value(client, config.CLIENT)
    if proxy is None:
        if client is None:
            proxy = config.http.PROXY
        else:
            proxy = client.proxy
    require(proxy, YtError("You should specify proxy"))
    return proxy

def get_api(proxy, version=None, client=None):
    proxy = get_proxy_url(proxy)
    location = "api" if version is None else "api/" + version
    return make_get_request_with_retries("http://{0}/{1}".format(proxy, location))

def get_token(client=None):
    token = None
    if client is not None:
        token = client.token
    if token is None:
        token = http_config.TOKEN
    if token is None:
        token_path = http_config.TOKEN_PATH
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

