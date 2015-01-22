"""YT usage errors"""

import errors_config
from yt.common import YtError

from copy import deepcopy
import simplejson as json

def hide_token(headers):
    if "Authorization" in headers:
        headers["Authorization"] = "x" * 32
    return headers

class YtOperationFailedError(YtError):
    """Operation failed during WaitStrategy.process_operation."""
    pass

class YtTimeoutError(YtError):
    """WaitStrategy timeout expired."""
    pass

class YtResponseError(YtError):
    """Error in HTTP response."""
    def __init__(self, url, headers, error):
        super(YtResponseError, self).__init__(repr(error))
        self.url = url
        self.headers = deepcopy(headers)
        self.error = error
        self.message = "Received an error while requesting {0}. Request headers are {1}"\
            .format(url, json.dumps(hide_token(self.headers), indent=4, sort_keys=True))
        self.inner_errors = [self.error]

    def __str__(self):
        return format_error(self)

    def is_resolve_error(self):
        """Resolving error."""
        return int(self.error["code"]) == 500

    def is_access_denied(self):
        """Access denied."""
        return int(self.error["code"]) == 901

    def is_concurrent_transaction_lock_conflict(self):
        """Transaction lock conflict."""
        return int(self.error["code"]) == 402

    def is_request_rate_limit_exceeded(self):
        """Request rate limit exceeded."""
        return YtResponseError._contains_code(self.error, 904)

    @staticmethod
    def _contains_code(error, code):
        if int(error["code"]) == code:
            return True
        for inner_error in error["inner_errors"]:
            if YtResponseError._contains_code(inner_error, code):
                return True
        return False


class YtRequestRateLimitExceeded(YtResponseError):
    pass

def build_response_error(url, headers, error):
    if YtResponseError._contains_code(error, 904):
        return YtRequestRateLimitExceeded(url, headers, error)
    return YtResponseError(url, headers, error)

class YtProxyUnavailable(YtError):
    """Proxy is under heavy load."""
    pass

class YtIncorrectResponse(YtError):
    """Incorrect proxy response."""
    pass

class YtTokenError(YtError):
    """Some problem occurred with authentication token."""
    pass

class YtFormatError(YtError):
    """Wrong format"""
    pass

def format_error(error, indent=0):
    if isinstance(error, YtError):
        error = error.simplify()
    elif isinstance(error, Exception):
        error = {"code": 1, "message": str(error)}

    if errors_config.ERROR_FORMAT == "json":
        return json.dumps(error)
    elif errors_config.ERROR_FORMAT == "json_pretty":
        return json.dumps(error, indent=2)
    elif errors_config.ERROR_FORMAT == "text":
        return pretty_format(error)
    else:
        raise YtError("Incorrect error format: " + errors_config.ERROR_FORMAT)

def pretty_format(error, indent=0):
    def format_attribute(name, value):
        return (" " * (indent + 4)) + "%-15s %s" % (name, value)

    lines = []
    if "message" in error:
        lines.append(error["message"])

    if "code" in error and int(error["code"]) != 1:
        lines.append(format_attribute("code", error["code"]))

    attributes = error.get("attributes", {})

    origin_keys = ["host", "datetime", "pid", "tid", "fid"]
    if all(key in attributes for key in origin_keys):
        lines.append(
            format_attribute(
                "origin",
                "%s in %s (pid %d, tid %x, fid %x)" % (
                    attributes["host"],
                    attributes["datetime"],
                    attributes["pid"],
                    attributes["tid"],
                    attributes["fid"])))

    location_keys = ["file", "line"]
    if all(key in attributes for key in location_keys):
        lines.append(format_attribute("location", "%s:%d" % (attributes["file"], attributes["line"])))

    for key, value in attributes.items():
        if key in origin_keys or key in location_keys:
            continue
        lines.append(format_attribute(key, str(value)))

    result = " " * indent + (" " * (indent + 4) + "\n").join(lines)
    if "inner_errors" in error:
        for inner_error in error["inner_errors"]:
            result += "\n" + format_error(inner_error, indent + 2)

    return result
