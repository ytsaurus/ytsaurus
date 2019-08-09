from . import http_driver

from . import native_driver
from .batch_response import apply_function_to_result
from .common import YtError, update, simplify_structure, set_param
from .config import get_option, get_config, get_backend_type
from .format import create_format
from .http_helpers import get_api_commands

from yt.common import YT_NULL_TRANSACTION_ID

import yt.logger as logger
import yt.yson as yson
import yt.json_wrapper as json
from yt.yson.convert import json_to_yson

from yt.packages.six import string_types

import os
from copy import deepcopy

_DEFAULT_COMMAND_PARAMS = {
    "transaction_id": YT_NULL_TRANSACTION_ID,
    "ping_ancestor_transactions": False
}

# TODO(ignat): move it to more appropriate place.
def _create_http_client_from_rpc(client, command_name):
    from .client import YtClient
    config = get_config(client)
    command_params = get_option("COMMAND_PARAMS", client)
    if config["proxy"]["url"] is None:
        raise YtError("For rpc proxy url must be specified in command '{}'".format(command_name))
    new_config = deepcopy(config)
    new_config["backend"] = "http"
    new_client = YtClient(config=new_config)
    new_client.COMMAND_PARAMS = command_params
    return new_client

def get_command_list(client=None):
    if get_option("_client_type", client) == "batch":
        raise YtError("Command \"get_command_list\" is not supported in batch mode")

    backend = get_backend_type(client)
    if backend in ("native", "rpc"):
        return list(native_driver.get_command_descriptors(client))
    else:  # backend == "http"
        return list(get_api_commands(client))

def set_read_from_params(params, read_from, cache_sticky_group_size):
    set_param(params, "read_from", read_from)
    set_param(params, "cache_sticky_group_size", cache_sticky_group_size)

def make_request(command_name,
                 params,
                 data=None,
                 is_data_compressed=False,
                 return_content=True,
                 response_format=None,
                 use_heavy_proxy=False,
                 timeout=None,
                 allow_retries=None,
                 decode_content=True,
                 batch_yson_dumps=True,
                 client=None):
    backend = get_backend_type(client)

    enable_params_logging = get_config(client)["enable_logging_for_params_changes"]
    if enable_params_logging:
        logger.debug("Start deepcopy (pid: %d)", os.getpid())
    command_params = deepcopy(get_option("COMMAND_PARAMS", client))
    if enable_params_logging:
        logger.debug("Finish deepcopy (pid: %d)", os.getpid())

    for key in _DEFAULT_COMMAND_PARAMS:
        if key in command_params and command_params[key] == _DEFAULT_COMMAND_PARAMS[key]:
            del command_params[key]

    params = update(command_params, params)

    params = simplify_structure(params)

    enable_request_logging = get_config(client)["enable_request_logging"]

    if enable_request_logging:
        logger.info("Executing %s (params: %r)", command_name, params)

    if get_option("_client_type", client) == "batch":
        result = client._batch_executor.add_task(command_name, params, data)
        if batch_yson_dumps:
            result = apply_function_to_result(lambda output: yson.dumps(output), result)
        return result

    if backend in ("native", "rpc"):
        if is_data_compressed:
            raise YtError("Native driver does not support compressed input for file and tabular data")
        result = native_driver.make_request(
            command_name,
            params,
            data,
            return_content=return_content,
            decode_content=decode_content,
            client=client)
    elif backend == "http":
        result = http_driver.make_request(
            command_name,
            params,
            data=data,
            is_data_compressed=is_data_compressed,
            return_content=return_content,
            response_format=response_format,
            use_heavy_proxy=use_heavy_proxy,
            timeout=timeout,
            allow_retries=allow_retries,
            decode_content=decode_content,
            client=client)
    else:
        raise YtError("Incorrect backend type: " + backend)

    if enable_request_logging:
        result_string = ""
        try:
            if result:
                debug_result = result
                if "output_format" in params and str(params["output_format"]) == "yson" and isinstance(result, bytes):
                    debug_result = yson.dumps(yson.loads(result), yson_format="text")
                result_string = " (result: %r)" % debug_result
        except:
            result_string = ""
        logger.info("Command executed" + result_string)

    return result


def make_formatted_request(command_name, params, format, **kwargs):
    # None format means that we want parsed output (as YSON structure) instead of string.
    # Yson parser is too slow, so we request result in JsonFormat and then convert it to YSON structure.

    client = kwargs.get("client", None)

    response_format = None

    has_yson_bindings = (yson.TYPE == "BINARY")
    use_yson = get_config(client)["force_using_yson_for_formatted_requests"] or has_yson_bindings

    is_batch = get_option("_client_type", client) == "batch"
    if is_batch:
        # NB: batch executor always use YSON format.
        if format is not None:
            raise YtError("Batch request is not supported for formatted requests")
    else:
        if format is None:
            if use_yson:
                params["output_format"] = "yson"
                response_format = "yson"
            else:
                params["output_format"] = "json"
                response_format = "json"
        else:
            if isinstance(format, string_types):
                format = create_format(format)
            params["output_format"] = format.to_yson_type()

    decode_content = format is not None
    result = make_request(command_name, params,
                          response_format=response_format,
                          decode_content=decode_content,
                          batch_yson_dumps=not is_batch,
                          **kwargs)

    if is_batch:
        return result

    if format is None:
        if use_yson:
            return yson.loads(result)
        else:
            return json_to_yson(json.loads(result), encoding="latin-1")
    else:
        return format.postprocess(result)
