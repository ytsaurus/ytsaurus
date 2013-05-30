import config
import logger
from common import require
from errors import YtError, YtResponseError
from format import JsonFormat
from version import VERSION
from http import make_get_request_with_retries, make_request_with_retries, Response, get_token, get_proxy

import requests

import sys
import uuid
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
    elif type == "string":
        return response.text
    else:
        raise YtError("Incorrent response type: " + type)

def get_hosts():
    return make_get_request_with_retries("http://{0}/hosts".format(get_proxy(config.http.PROXY)))

def get_host_for_heavy_operation():
    if config.USE_HOSTS:
        hosts = get_hosts()
        if hosts:
            return hosts[0]
    return config.http.PROXY

def make_request(command_name, params,
                 data=None, format=None, proxy=None, return_raw_response=False, verbose=False):
    """
    Makes request to yt proxy. Command name is the name of command in YT API.
    Option return_raw_response forces returning response of requests library without extracting data field.
    """
    def print_info(msg, *args, **kwargs):
        # Verbose option is used for debugging because it is more
        # selective than logging
        if verbose:
            # We don't use kwargs because python doesn't support such kind of formatting
            print >>sys.stderr, msg % args
        logger.debug(msg, *args, **kwargs)

    # Trying to set http retries in requests
    requests.adapters.DEFAULT_RETRIES = config.http.REQUESTS_RETRIES

    # Get command description
    command = config.COMMANDS[command_name]
    
    # Determine make retries or not and set mutation if needed
    allow_retries = \
            not command.is_volatile or \
            (config.http.RETRY_VOLATILE_COMMANDS and not command.is_heavy)
    if command.is_volatile and allow_retries:
        if config.MUTATION_ID is not None:
            params["mutation_id"] = config.MUTATION_ID
        else:
            params["mutation_id"] = str(uuid.uuid4())

    # Prepare request url.
    if proxy is None:
        proxy = config.http.PROXY
    require(proxy, YtError("You should specify proxy"))

    # prepare url
    url = "http://{0}/{1}/{2}".format(proxy, config.API_PATH, command_name)
    print_info("Request url: %r", url)

    # prepare params, format and headers
    headers = {"User-Agent": "Python wrapper " + VERSION,
               "Accept-Encoding": config.http.ACCEPT_ENCODING,
               "X-YT-Correlation-Id": str(uuid.uuid4())}
    # TODO(ignat) stop using http method for detection command properties
    if command.http_method() == "POST":
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

    token = get_token()
    if token is not None:
        headers["Authorization"] = "OAuth " + token

    # Debug information
    print_info("Headers: %r", headers)
    print_info("Params: %r", params)
    if command.http_method() != "PUT":
        print_info("Body: %r", data)

    stream = (command.output_type in ["binary", "tabular"])

    response = make_request_with_retries(
        lambda: Response(
            requests.request(
                url=url,
                method=command.http_method(),
                headers=headers,
                data=data,
                timeout=config.http.CONNECTION_TIMEOUT,
                stream=stream)),
        allow_retries,
        url,
        return_raw_response)

    # Hide token for security reasons 
    if "Authorization" in headers:
        headers["Authorization"] = "x" * 32
    print_info("Response header %r", response.http_response.headers)

    # Determine type of response data and return it
    if response.is_ok():
        if return_raw_response:
            return response.http_response
        elif response.is_json():
            return response.json()
        elif response.is_yson():
            return response.yson()
        else:
            return response.content
    else:
        message = "Response to request {0} with headers {1} contains error:\n{2}".\
                  format(url, headers, response.error())
        raise YtResponseError(message)

