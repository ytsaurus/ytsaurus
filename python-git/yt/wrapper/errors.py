"""YT usage errors"""

from yt.common import YtError, YtResponseError
import yt.json as json

from copy import deepcopy

def hide_token(headers):
    if "Authorization" in headers:
        headers = deepcopy(headers)
        headers["Authorization"] = "x" * 32
    return headers

class YtOperationFailedError(YtError):
    """Operation failed during WaitStrategy.process_operation."""
    def __init__(self, id, state, error, stderrs, url):
        message = "Operation {0} {1}".format(id, state)
        attributes = {
            "id": id,
            "state": state,
            "stderrs": stderrs,
            "url": url}

        inner_errors = []
        if error is not None:
            inner_errors.append(error)

        super(YtOperationFailedError, self).__init__(message, attributes=attributes, inner_errors=inner_errors)

class YtTimeoutError(YtError):
    """WaitStrategy timeout expired."""
    pass

class YtHttpResponseError(YtResponseError):
    def __init__(self, error, url, headers, params):
        def dumps(obj):
            return json.dumps(hide_token(obj), indent=4, sort_keys=True)

        super(YtHttpResponseError, self).__init__(error)
        self.url = url
        self.headers = deepcopy(headers)
        self.params = params
        self.message = "Received response with error.\n"\
                       "Url: {0}\n"\
                       "Headers: {1}\n"\
                       "Params: {2}"\
            .format(url, dumps(self.headers), dumps(self.params))
        if self.is_request_rate_limit_exceeded():
            self.__class__ = YtRequestRateLimitExceeded

class YtRequestRateLimitExceeded(YtHttpResponseError):
    """ Request rate limit exceeded error. """
    """ It is used in retries. """
    pass

class YtProxyUnavailable(YtError):
    """Proxy is under heavy load."""
    def __init__(self, response):
        self.response = response
        attributes = {
            "url": response.url,
            "request_info": hide_token(response.request_info)}
        super(YtProxyUnavailable, self).__init__(message="Proxy is unavailable", attributes=attributes, inner_errors=[response.json()])

class YtIncorrectResponse(YtError):
    """Incorrect proxy response."""
    def __init__(self, message, response):
        self.response = response
        attributes = {
            "url": response.url,
            "request_info": hide_token(response.request_info)}
        super(YtIncorrectResponse, self).__init__(message, attributes=attributes)

class YtTokenError(YtError):
    """Some problem occurred with authentication token."""
    pass

class YtRetriableError(Exception):
    """Just simple retriable error for test purposes. """
    pass


