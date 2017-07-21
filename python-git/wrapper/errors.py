"""YT usage errors"""

import yt.common
from yt.common import YtError, PrettyPrintableDict
import yt.json as json

from copy import deepcopy

def hide_token(headers):
    if "Authorization" in headers:
        headers = deepcopy(headers)
        headers["Authorization"] = "x" * 32
    return headers

class YtOperationFailedError(YtError):
    """Operation failed during waiting completion."""
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

        # TODO(ignat): Add all stderr as suberrors?
        if stderrs:
            failed_job = stderrs[0]
            failed_job_error = failed_job["error"]
            if "stderr" in failed_job:
                failed_job_error["attributes"]["stderr"] = failed_job["stderr"]
            inner_errors.append(failed_job_error)

        super(YtOperationFailedError, self).__init__(message, attributes=attributes, inner_errors=inner_errors)

class YtResponseError(yt.common.YtResponseError):
    """Another incarnation of YtResponseError."""
    def __init__(self, *args, **kwargs):
        super(YtResponseError, self).__init__(*args, **kwargs)
        if self.is_request_rate_limit_exceeded():
            self.__class__ = YtRequestRateLimitExceeded
        if self.is_request_queue_size_limit_exceeded():
            self.__class__ = YtRequestQueueSizeLimitExceeded
        if self.is_concurrent_operations_limit_reached():
            self.__class__ = YtConcurrentOperationsLimitExceeded
        if self.is_request_timed_out():
            self.__class__ = YtRequestTimedOut
        if self.is_no_such_transaction():
            self.__class__ = YtNoSuchTransaction
        if self.is_master_communication_error():
            self.__class__ = YtMasterCommunicationError

class YtHttpResponseError(YtResponseError):
    """Reponse error recieved from http proxy with additional http request information."""
    def __init__(self, error, url, headers, params):
        def dumps(obj):
            return json.dumps(hide_token(obj), indent=4, sort_keys=True)

        super(YtHttpResponseError, self).__init__(error)
        self.url = url
        self.headers = deepcopy(headers)
        self.params = params
        self.message = "Received HTTP response with error"
        self.attributes.update({
            "url": url,
            "headers": PrettyPrintableDict(self.headers),
            "params": PrettyPrintableDict(self.params),
            "transparent": True})

    def __reduce__(self):
        return (YtHttpResponseError, (self.error, self.url, self.headers, self.params))

class YtRequestRateLimitExceeded(YtHttpResponseError):
    """Request rate limit exceeded error.
       It is used in retries."""
    pass

class YtRequestQueueSizeLimitExceeded(YtHttpResponseError):
    """Request queue size limit exceeded error.
       It is used in retries.
    """
    pass

class YtConcurrentOperationsLimitExceeded(YtHttpResponseError):
    """Concurrent operations limit exceeded.
       It is used in retries."""
    pass

class YtRequestTimedOut(YtHttpResponseError):
    """Request timed out.
       It is used in retries."""
    pass

class YtNoSuchTransaction(YtHttpResponseError):
    """No such transaction.
       It is used in retries."""
    pass

class YtMasterCommunicationError(YtHttpResponseError):
    """Master communication error.
       It is used in retries."""
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
            "headers": response.headers,
            "request_info": hide_token(response.request_info),
            "body": self.truncate(response.text)}
        super(YtIncorrectResponse, self).__init__(message, attributes=attributes)

    def truncate(self, str):
        if len(str) > 100:
            return str[:100] + "...truncated"
        return str

class YtTokenError(YtError):
    """Some problem occurred with authentication token."""
    pass

class YtRetriableError(Exception):
    """Just simple retriable error for test purposes."""
    pass

class YtTransactionPingError(BaseException):
    """Raised in signal handler when thread was unable to ping transaction."""
    pass

