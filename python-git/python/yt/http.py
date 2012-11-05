from common import YtError, require

import requests

import sys
import logger
import simplejson as json

HTTP_METHOD_DICT = {
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


def make_request(proxy, command_name, arguments,
                 data=None,
                 input_format=None, output_format=None,
                 headers=None, verbose=False):
    def print_info(msg, *args, **kwargs):
        # Verbose option is used for debugging because it is more
        # selective than logging
        if verbose:
            # We don't use kwargs because python doesn't support such kind of formatting
            print >>sys.stderr, msg % args
        logger.debug(msg, *args, **kwargs)
    
    http_method = HTTP_METHOD_DICT[command_name]

    # prepare url
    url = "http://{0}/api/{1}".format(proxy, command_name)
    print_info("Request url: %r", url)

    # prepare arguments, format and headers
    if headers is None: headers = {}

    headers.update({"User-Agent": "Python wrapper"})
    if http_method[command_name] == "POST":
        require(data is None and input_format is None,
                YtError("Input format and data should not be specified in POST methods"))
        headers.update({"Accept": "application/json"})
        data = json.dumps(arguments)
        arguments = {}
    if arguments:
        headers.update({"X-YT-Parameters": json.dumps(arguments)})
    if input_format is not None:
        headers.update({"X-YT-Input-Format": json.dumps(input_format)})
    if output_format is not None:
        headers.update({"X-YT-Output-Format": json.dumps(output_format)})

    print_info("Headers: %r", headers)
    print_info("Arguments: %r", arguments)
    print_info("Data(body): %r", data)

    return requests.request(
            url=url,
            method=http_method[command_name],
            headers=headers,
            prefetch=False,
            data=data)

def execute(proxy, command_name, arguments,
            input_stream=None, output_stream=None,
            input_format=None, output_format=None,
            headers=None, verbose=False):
    data = None
    if input_stream is not None:
        data = input_stream.read()

    response = make_request(proxy, command_name, arguments,
                            data, input_format, output_format,
                            headers, verbose)

    error = None
    if not str(response.status_code).startswith("2"):
        error = response.json
    elif response.headers.get("x-yt-response-code", 0) != 0:
        error = response.headers["x-yt-error"].json

    if error is not None:
        return error

    for chunk in response.iter_content():
        output_stream.write(chunk)

    #TODO(ignat): finish method and syncronize with bindings and wrapper
