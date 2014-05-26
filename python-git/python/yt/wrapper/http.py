import http_config
import yt.logger as logger
from common import require, get_backoff
from errors import YtError, YtNetworkError, YtTokenError, YtProxyUnavailable, YtIncorrectResponse

import os
import string
import time
import httplib
import yt.packages.requests
import simplejson as json
from datetime import datetime

# We cannot use requests.HTTPError in module namespace because of conflict with python3 http library
from yt.packages.requests import HTTPError, ConnectionError, Timeout
NETWORK_ERRORS = (HTTPError, ConnectionError, Timeout, httplib.IncompleteRead)

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
        return self.http_response.headers.get("content-type") == "application/json"

    def json(self):
        return self.http_response.json()

    def content(self):
        return self.http_response.content

    def _process_return_code(self):
        if self._return_code_processed:
            return

        if not str(self.http_response.status_code).startswith("2"):
            # 401 is case of incorrect token
            if self.http_response.status_code == 401:
                url_base = "/".join(self.http_response.url.split("/")[:2])
                raise YtTokenError(
                    "Your authentication token was rejected by the server (X-YT-Request-ID: {0}).\n"
                    "Please refer to {1}/auth/ for obtaining a valid token or contact us at yt@yandex-team.ru."\
                        .format(
                            self.http_response.headers.get("X-YT-Request-ID", "absent"),
                            url_base))
            self._error = self.http_response.json()
        elif int(self.http_response.headers.get("x-yt-response-code", 0)) != 0:
            self._error = json.loads(self.http_response.headers["x-yt-error"])
        self._return_code_processed = True

def make_request_with_retries(request, make_retries=False, retry_unavailable_proxy=True,
                              description="", return_raw_response=False):
    yt.packages.requests.adapters.DEFAULT_TIMEOUT = http_config.REQUEST_TIMEOUT

    network_errors = list(NETWORK_ERRORS)
    network_errors.append(YtIncorrectResponse)
    if retry_unavailable_proxy:
        network_errors.append(YtProxyUnavailable)

    for attempt in xrange(http_config.REQUEST_RETRY_COUNT):
        current_time = datetime.now()
        try:
            response = request()
            # Sometimes (quite often) we obtain incomplete response with empty body where expected to be JSON.
            # So we should retry this request.
            is_json = response.is_json()
            if not return_raw_response and is_json and not response.content():
                raise YtIncorrectResponse(
                        "Response has empty body and JSON content type (Headers: %s)" %
                        repr(response.http_response.headers))
            if response.http_response.status_code == 503:
                raise YtProxyUnavailable("Retrying response with code 503 and body %s" % response.content())
            return response
        except tuple(network_errors) as error:
            message =  "HTTP request (%s) has failed with error '%s'" % (description, str(error))
            if make_retries and attempt + 1 < http_config.REQUEST_RETRY_COUNT:
                backoff = get_backoff(http_config.REQUEST_RETRY_TIMEOUT, current_time)
                if backoff:
                    logger.warning("%s. Sleep for %.2lf seconds...", message, backoff)
                    time.sleep(backoff)
                logger.warning("New retry (%d) ...", attempt + 2)
            elif not isinstance(error, YtError):
                # We wrapping network errors to simplify catching such errors later.
                raise YtNetworkError(message)
            else:
                raise

def make_get_request_with_retries(url):
    response = make_request_with_retries(
        lambda: Response(session_.get(url)),
        make_retries=True,
        description=url)
    return response.json()

def check_proxy(proxy):
    require(proxy, YtError("You should specify proxy"))

def get_api(proxy, version=None):
    check_proxy(proxy)
    location = "api" if version is None else "api/" + version
    return make_get_request_with_retries("http://{0}/{1}".format(proxy, location))

def get_token():
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

