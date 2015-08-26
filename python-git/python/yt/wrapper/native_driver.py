from config import get_config
from common import require
from errors import YtResponseError, YtError
from string_iter_io import StringIterIO
from response_stream import ResponseStream

import yt.yson as yson
try:
    import yt_driver_bindings
    from yt_driver_bindings import Request, Driver, BufferedStream
except ImportError:
    Driver = None

from cStringIO import StringIO

driver_ = None

def read_config(path):
    driver_config = yson.load(open(path))
    if "logging" in driver_config:
        yt_driver_bindings.configure_logging(driver_config["logging"])
    if "tracing" in driver_config:
        yt_driver_bindings.configure_tracing(driver_config["tracing"])
    return driver_config["driver"]

def get_driver_instance(client):
    config = get_config(client)
    if config["driver_config"] is not None:
        driver_config = config["driver_config"]
    elif config["driver_config_path"] is not None:
        driver_config = read_config(config["driver_config_path"])
    else:
        raise YtError("Driver config is not specified")

    if client is not None:
        if client._driver is None:
            if Driver is None:
                raise YtError("Driver class not found, install yt driver bindings.")
            client._driver = Driver(driver_config)
        return client._driver
    else:
        global driver_
        if driver_ is None:
            driver_ = Driver(driver_config)
        return driver_

def convert_to_stream(data):
    if data is None:
        return data
    elif hasattr(data, "read"):
        return data
    elif isinstance(data, str):
        return StringIO(data)
    elif isinstance(data, list):
        return StringIterIO(iter(data))
    else:
        return StringIterIO(data)

def make_request(command_name, params,
                 data=None,
                 return_content=True,
                 client=None):
    driver = get_driver_instance(client)

    require(command_name in driver.get_command_descriptors(),
            YtError("Command {0} is not supported".format(command_name)))

    description = driver.get_command_descriptor(command_name)

    input_stream = convert_to_stream(data)

    output_stream = None
    if description.output_type() != "null":
        if "output_format" not in params:
            params["output_format"] = "json"
        if return_content:
            output_stream = StringIO()
        else:
            output_stream = BufferedStream(size=1024 * 1024)

    response = driver.execute(
        Request(command_name=command_name,
                parameters=params,
                input_stream=input_stream,
                output_stream=output_stream))

    if return_content:
        response.wait()
        if not response.is_ok():
            raise YtResponseError(response.error())
        return output_stream.getvalue()
    else:
        def process_error(request):
            if not response.is_ok():
                raise YtResponseError(response.error())

        if response.is_set() and not response.is_ok():
            raise YtResponseError(response.error())

        return ResponseStream(
            lambda: response,
            yt_driver_bindings.chunk_iter(output_stream, response, get_config(client)["read_buffer_size"]),
            lambda: None,
            process_error,
            lambda: response.response_parameters())
