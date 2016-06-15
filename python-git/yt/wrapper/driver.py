"""YT requests misc"""
import http_driver
import native_driver
from common import bool_to_string, YtError
from config import get_option, get_config, get_backend_type
from format import create_format

import yt.logger as logger
import yt.yson as yson
from yt.yson.convert import json_to_yson
import yt.json as json

def make_request(command_name, params,
                 data=None,
                 is_data_compressed=False,
                 proxy=None,
                 return_content=True,
                 retry_unavailable_proxy=True,
                 response_should_be_json=False,
                 use_heavy_proxy=False,
                 timeout=None,
                 allow_retries=None,
                 client=None):
    backend = get_backend_type(client)

    if get_option("MUTATION_ID", client) is not None:
        params["mutation_id"] = get_option("MUTATION_ID", client)
    if get_option("TRACE", client) is not None and get_option("TRACE", client):
        params["trace"] = bool_to_string(get_option("TRACE", client))
    if get_option("RETRY", client) is not None:
        params["retry"] = bool_to_string(get_option("RETRY", client))

    enable_request_logging = get_config(client)["enable_request_logging"]

    if enable_request_logging:
        logger.info("Executing %s (params: %r)", command_name, params)

    if backend == "native":
        if is_data_compressed:
            raise YtError("Native driver does not support compressed input for file and tabular data")
        result = native_driver.make_request(command_name, params, data, return_content=return_content, client=client)
    elif backend == "http":
        result = http_driver.make_request(
            command_name,
            params,
            data=data,
            is_data_compressed=is_data_compressed,
            proxy=proxy,
            return_content=return_content,
            retry_unavailable_proxy=retry_unavailable_proxy,
            response_should_be_json=response_should_be_json,
            use_heavy_proxy=use_heavy_proxy,
            timeout=timeout,
            allow_retries=allow_retries,
            client=client)
    else:
        raise YtError("Incorrect backend type: " + backend)

    if enable_request_logging:
        result_string = ""
        if result:
            result_string = " (result: %r)" % result
        logger.info("Command executed" + result_string)

    return result


def make_formatted_request(command_name, params, format, ignore_result=False, **kwargs):
    # None format means that we want parsed output (as YSON structure) instead of string.
    # Yson parser is too slow, so we request result in JsonFormat and then convert it to YSON structure.

    client = kwargs.get("client", None)

    has_yson_bindings = (yson.TYPE == "BINARY")
    response_should_be_json = format is None and not has_yson_bindings
    if format is None:
        if get_config(client)["force_using_yson_for_formatted_requests"] or has_yson_bindings:
            params["output_format"] = yson.to_yson_type("yson", attributes={"format": "text"})
        else:
            params["output_format"] = "json"
    else:
        if isinstance(format, str):
            format = create_format(format)
        params["output_format"] = format.to_yson_type()

    result = make_request(command_name, params, response_should_be_json=response_should_be_json, **kwargs)

    if ignore_result:
        return
    if format is None:
        if has_yson_bindings:
            return yson.loads(result)
        else:
            return json_to_yson(json.loads(result))
    else:
        return result
