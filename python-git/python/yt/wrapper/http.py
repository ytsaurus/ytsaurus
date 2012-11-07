import config
from common import YtError, YtResponseError, require
from format import JsonFormat

import requests

import sys
import logger
import simplejson as json

def iter_lines(response):
    """
    Iterates over the response data, one line at a time.  This avoids reading
    the content at once into memory for large responses. It is get from
    requests, but improved to ignore \r line breaks.
    """
    def add_eoln(str):
        return str + "\n"

    pending = None
    for chunk in response.iter_content(chunk_size=config.READ_BUFFER_SIZE):
        if pending is not None:
            chunk = pending + chunk
        lines = chunk.split('\n')
        pending = lines.pop()
        for line in lines:
            yield add_eoln(line)

    if pending is not None and pending:
        yield add_eoln(pending)

def read_content(response, type):
    if type == "iter_lines":
        return iter_lines(response)
    elif type == "iter_content":
        return response.iter_content(chunk_size=config.HTTP_CHUNK_SIZE)
    else:
        raise YtError("Incorrent response type: " + type)

class Response(object):
    def __init__(self, http_response):
        self.http_response = http_response
        if not str(http_response.status_code).startswith("2"):
            self._error = http_response.json
        elif http_response.headers.get("x-yt-response-code", 0) != 0:
            self._error = http_response.headers["x-yt-error"].json

    def error(self):
        return self._error

    def is_ok(self):
        return not hasattr(self, "_error")

    def is_json(self):
        return self.http_response["content-type"] == "application/json"

    def json(self):
        return self.http_response.json

    def content(self):
        return self.http_response.content


def make_request(command_name, params,
                 data=None, format=None, verbose=False, proxy=None,
                 raw_response=False, files=None):
    """ Makes request to yt proxy.
        http_method may be equal to GET, POST or PUT,
        command_name is type of driver command, it may be equal
        to get, read, write, create ...
        Returns response content, raw_response option force
        to return request.Response instance"""
    def print_info(msg, *args, **kwargs):
        # Verbose option is used for debugging because it is more
        # selective than logging
        if verbose:
            # We don't use kwargs because python doesn't support such kind of formatting
            print >>sys.stderr, msg % args
        logger.debug(msg, *args, **kwargs)

    http_method = {
        "start_tx": "POST",
        "renew_tx": "POST",
        "commit_tx": "POST",
        "abort_tx": "POST",
        "create": "POST",
        "remove": "POST",
        "set": "PUT",
        "get": "GET",
        "list": "GET",
        "lock": "POST",
        "copy": "POST",
        "exists": "GET",
        "upload": "PUT",
        "download": "GET",
        "write": "PUT",
        "read": "GET",
        "merge": "POST",
        "erase": "POST",
        "map": "POST",
        "reduce": "POST",
        "map_reduce": "POST",
        "sort": "POST",
        "abort_op": "POST"
    }

    # Prepare request url.
    if proxy is None:
        proxy = config.PROXY

    # prepare url
    url = "http://{0}/api/{1}".format(proxy, command_name)
    print_info("Request url: %r", url)

    # prepare params, format and headers
    headers = {"User-Agent": "Python wrapper",
               "Accept-Encoding": config.ACCEPT_ENCODING}
    if http_method[command_name] == "POST":
        require(data is None and format is None,
                YtError("Format and data should not be specified in POST methods"))
        headers.update(JsonFormat().to_input_http_header())
        data = json.dumps(params)
        params = {}
    if params:
        headers.update({"X-YT-Parameters": json.dumps(params)})
    if format is not None:
        headers.update(format.to_input_http_header())
        headers.update(format.to_output_http_header())
    else:
        headers.update(JsonFormat().to_output_http_header())
    print_info("Headers: %r", headers)
    print_info("Params: %r", params)
    if http_method[command_name] != "PUT":
        print_info("Body: %r", data)

    response = Response(
        requests.request(
            url=url,
            method=http_method[command_name],
            headers=headers,
            prefetch=False,
            data=data,
            files=files))

    print_info("Response header %r", response.http_response.headers)
    if response.is_ok():
        if raw_response:
            return response.http_response
        elif response.is_json:
            return response.json()
        else:
            response.content()
    else:
        message = "Response to request {0} with headers {1} contains error: {2}".\
                  format(url, headers, response.error())
        raise YtResponseError(message)

