from .config import get_config, get_option, set_option
from .common import require, generate_int64
from .errors import YtResponseError, YtError
from .string_iter_io import StringIterIO
from .response_stream import ResponseStream

import yt.logger as logger
import yt.yson as yson

from yt.packages.six import binary_type, PY3

try:
    from cStringIO import StringIO as BytesIO
except ImportError:  # Python 3
    from io import BytesIO

driver_bindings = None
def lazy_import_driver_bindings():
    global driver_bindings
    if driver_bindings is None:
        try:
            import yt_driver_bindings
            driver_bindings = yt_driver_bindings
        except ImportError:
            pass

def read_config(path):
    lazy_import_driver_bindings()

    driver_config = yson.load(open(path, "rb"))
    if not hasattr(read_config, "logging_and_tracing_initialized"):
        if "logging" in driver_config:
            driver_bindings.configure_logging(driver_config["logging"])
        if "tracing" in driver_config:
            driver_bindings.configure_tracing(driver_config["tracing"])
        setattr(read_config, "logging_and_tracing_initialized", True)
    return driver_config["driver"]

def get_driver_instance(client):
    lazy_import_driver_bindings()

    driver = get_option("_driver", client=client)
    if driver is None:
        if driver_bindings is None:
            raise YtError("Driver class not found, install yt driver bindings.")

        config = get_config(client)
        if config["driver_config"] is not None:
            driver_config = config["driver_config"]
        elif config["driver_config_path"] is not None:
            driver_config = read_config(config["driver_config_path"])
        else:
            raise YtError("Driver config is not specified")

        if config["backend"] == "rpc":
            if driver_config.get("backend") is None:
                driver_config["backend"] = "rpc"
            else:
                raise YtError("Driver and client backend mismatch (driver_backend: {0}, client_backend: {1})"
                    .format(driver_config["backend"], config["backend"]))

        set_option("_driver", driver_bindings.Driver(driver_config), client=client)
        driver = get_option("_driver", client=client)
    return driver

def create_driver_for_cell(driver, cell_id):
    config = driver.get_config()
    if config["primary_master"]["cell_id"] == cell_id:
        return driver

    new_primary_master_config = None
    for secondary_master in config["secondary_masters"]:
        if secondary_master["cell_id"] == cell_id:
            new_primary_master_config = secondary_master
            break

    if new_primary_master_config is None:
        raise YtError("Cell id {0} is not found in driver config".format(cell_id))

    config["primary_master"] = new_primary_master_config
    #config["master_cache"] = {"addresses": []}
    if "master_cache" in config:
        del config["master_cache"]
    #config["timestamp_provider"] = {"addresses": []}
    if "timestamp_provider" in config:
        del config["timestamp_provider"]

    del config["secondary_masters"]

    return driver_bindings.Driver(config)

def convert_to_stream(data):
    if data is None:
        return data
    elif hasattr(data, "read"):
        return data
    elif isinstance(data, binary_type):
        return BytesIO(data)
    elif isinstance(data, list):
        return StringIterIO(iter(data))
    else:
        return StringIterIO(data)

def get_command_descriptors(client=None):
    driver = get_driver_instance(client)
    return driver.get_command_descriptors()

def chunk_iter(stream, response, size):
    while True:
        if response.is_set():
            if not response.is_ok():
                raise YtResponseError(response.error())
            else:
                break
        yield stream.read(size)

    while not stream.empty():
        yield stream.read(size)

def make_request(command_name, params,
                 data=None,
                 return_content=True,
                 decode_content=True,
                 client=None):
    driver = get_driver_instance(client)

    cell_id = params.get("master_cell_id")
    if cell_id is not None:
        driver = create_driver_for_cell(driver, cell_id)

    require(command_name in driver.get_command_descriptors(),
            lambda: YtError("Command {0} is not supported".format(command_name)))

    description = driver.get_command_descriptor(command_name)

    input_stream = convert_to_stream(data)

    output_stream = None
    if description.output_type() != b"Null":
        if "output_format" not in params and description.output_type() != b"Binary":
            raise YtError("Inner error: output format is not specified for native driver command '{0}'".format(command_name))
        if return_content:
            output_stream = BytesIO()
        else:
            output_stream = driver_bindings.BufferedStream(size=get_config(client)["read_buffer_size"])

    request_id = generate_int64(get_option("_random_generator", client))

    logger.debug("Executing command %s with parameters %s and id %s", command_name, repr(params), hex(request_id)[2:])

    driver_user_name = get_config(client)["driver_user_name"]
    if driver_user_name is not None:
        driver_user_name = str(driver_user_name)

    request = driver_bindings.Request(
        command_name=command_name,
        parameters=params,
        input_stream=input_stream,
        output_stream=output_stream,
        user=driver_user_name)

    if get_config(client)["enable_passing_request_id_to_driver"]:
        request.id = request_id

    response = driver.execute(request)

    if return_content:
        response.wait()
        if not response.is_ok():
            error = YtResponseError(response.error())
            error.message = "Received driver response with error"
            raise error
        if output_stream is not None:
            value = output_stream.getvalue()
            if decode_content and PY3:
                return value.decode("utf-8")
            return value
    else:
        def process_error(request):
            if not response.is_ok():
                raise YtResponseError(response.error())

        if response.is_set() and not response.is_ok():
            raise YtResponseError(response.error())

        return ResponseStream(
            lambda: response,
            chunk_iter(output_stream, response, get_config(client)["read_buffer_size"]),
            lambda: None,
            process_error,
            lambda: response.response_parameters())
