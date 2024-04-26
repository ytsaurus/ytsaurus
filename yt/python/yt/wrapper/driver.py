from . import http_driver

from . import native_driver
from .batch_response import apply_function_to_result
from .common import YtError, update, simplify_structure, set_param
from .config import get_option, set_option, get_config, get_backend_type
from .format import create_format, JsonFormat, YsonFormat, YtFormatError
from .http_helpers import get_http_api_version, get_http_api_commands

from yt.common import YT_NULL_TRANSACTION_ID

import yt.logger as logger
import yt.yson as yson

from copy import deepcopy

_DEFAULT_COMMAND_PARAMS = {
    "transaction_id": YT_NULL_TRANSACTION_ID,
    "ping_ancestor_transactions": False
}


def get_command_list(client=None):
    if get_option("_client_type", client) == "batch":
        raise YtError("Command \"get_command_list\" is not supported in batch mode")

    backend = get_backend_type(client)
    if backend in ("native", "rpc"):
        return list(native_driver.get_command_descriptors(client))
    else:  # backend == "http"
        return list(get_http_api_commands(client))


def set_master_read_params(params, read_from, cache_sticky_group_size):
    set_param(params, "read_from", read_from)
    set_param(params, "cache_sticky_group_size", cache_sticky_group_size)


def get_api_version(client=None):
    api_version_option = get_option("_api_version", client)
    if api_version_option:
        return api_version_option

    api_version_from_config = get_config(client)["api_version"]
    if api_version_from_config:
        set_option("_api_version", api_version_from_config, client)
        return api_version_from_config

    if get_backend_type(client) == "http":
        api_version = get_http_api_version(client=client)
    else:
        driver = native_driver.get_driver_instance(client=client)
        if driver is None or "api_version" not in driver.get_config():
            api_version = "v3"
        else:
            api_version = "v" + str(driver.get_config()["api_version"])

    set_option("_api_version", api_version, client)

    return api_version


def make_request(command_name,
                 params,
                 data=None,
                 is_data_compressed=False,
                 return_content=True,
                 response_format=None,
                 use_heavy_proxy=False,
                 timeout=None,
                 allow_retries=None,
                 retry_config=None,
                 batch_yson_dumps=True,
                 mutation_id=None,
                 client=None):
    backend = get_backend_type(client)

    command_params = deepcopy(get_option("COMMAND_PARAMS", client))

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
            retry_config=retry_config,
            mutation_id=mutation_id,
            client=client)
    else:
        raise YtError("Incorrect backend type: " + backend)

    # For testing purposes only.
    if enable_request_logging:
        result_string = ""
        try:
            if result:
                if "output_format" in params and str(params["output_format"]) == "yson":
                    debug_result = yson.dumps(yson.loads(result), yson_format="text").decode("ascii")
                else:
                    debug_result = result.decode("ascii")
                result_string = " (result: %r)" % debug_result
        except Exception:
            result_string = ""
        logger.info("Command executed" + result_string)

    return result


def get_structured_format(format, client):
    if format is None:
        format = get_config(client)["structured_data_format"]
    if format is None:
        has_yson_bindings = (yson.TYPE == "BINARY")
        use_yson = get_config(client)["force_using_yson_for_formatted_requests"] or has_yson_bindings
        if use_yson:
            format = YsonFormat()
        else:
            format = JsonFormat()
    if isinstance(format, str):
        format = create_format(format)
    return format


def make_formatted_request(command_name, params, format, **kwargs):
    # None format means that we want to return parsed output (as YSON structure)
    # instead of string.

    client = kwargs.get("client", None)

    is_format_specified = format is not None

    is_batch = get_option("_client_type", client) == "batch"
    if is_batch:
        # NB: batch executor always use YSON format.
        if is_format_specified:
            raise YtError("Batch request is not supported for formatted requests")
    else:
        format = get_structured_format(format, client=client)
        params["output_format"] = format.to_yson_type()

    result = make_request(command_name, params,
                          response_format=format,
                          batch_yson_dumps=not is_batch,
                          **kwargs)

    if is_batch:
        return result

    if not is_format_specified:
        try:
            structured_result = format.loads_node(result)
        except UnicodeDecodeError:
            logger.exception("Failed to decode string")
            error = YtFormatError("Failed to decode string, it usually means that "
                                  "you are using python3 and non-unicode data in YT; "
                                  "try to specify structured_data_format with none encoding")
            raise error from None
        return structured_result
    else:
        return format.postprocess(result)
